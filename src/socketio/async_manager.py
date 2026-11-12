import asyncio
import inspect

from engineio import packet as eio_packet
from socketio import packet
from .base_manager import BaseManager


class AsyncManager(BaseManager):
    """Manage a client list for an asyncio server."""
    async def can_disconnect(self, sid, namespace):
        return self.is_connected(sid, namespace)

    def _get_send_lock(self, eio_sid):
        """Return the asyncio lock that serializes emits on an Engine.IO
        connection, creating it on first use."""
        lock = self._send_locks.get(eio_sid)
        if lock is None:
            lock = asyncio.Lock()
            self._send_locks[eio_sid] = lock
        return lock

    async def emit(self, event, data, namespace, room=None, skip_sid=None,
                   callback=None, to=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace.

        Note: this method is a coroutine.
        """
        room = to or room
        if namespace not in self.rooms:
            return
        if isinstance(data, tuple):
            # tuples are expanded to multiple arguments, everything else is
            # sent as a single argument
            data = list(data)
        elif data is not None:
            data = [data]
        else:
            data = []
        if not isinstance(skip_sid, list):
            skip_sid = [skip_sid]
        if getattr(self.server, 'safe_emits', False) is True:
            await self._safe_emit(event, data, namespace, room, skip_sid,
                                  callback)
            return
        tasks = []
        if not callback:
            # when callbacks aren't used the packets sent to each recipient are
            # identical, so they can be generated once and reused
            pkt = self.server.packet_class(
                packet.EVENT, namespace=namespace, data=[event] + data)
            encoded_packet = pkt.encode()
            if not isinstance(encoded_packet, list):
                encoded_packet = [encoded_packet]
            eio_pkt = [eio_packet.Packet(eio_packet.MESSAGE, p)
                       for p in encoded_packet]
            for sid, eio_sid in self.get_participants(namespace, room):
                if sid not in skip_sid:
                    for p in eio_pkt:
                        tasks.append(asyncio.create_task(
                            self.server._send_eio_packet(eio_sid, p)))
        else:
            # callbacks are used, so each recipient must be sent a packet that
            # contains a unique callback id
            # note that callbacks when addressing a group of people are
            # implemented but not tested or supported
            for sid, eio_sid in self.get_participants(namespace, room):
                if sid not in skip_sid:  # pragma: no branch
                    id = self._generate_ack_id(sid, callback)
                    pkt = self.server.packet_class(
                        packet.EVENT, namespace=namespace, data=[event] + data,
                        id=id)
                    tasks.append(asyncio.create_task(
                        self.server._send_packet(eio_sid, pkt)))
        if tasks == []:  # pragma: no cover
            return
        await asyncio.wait(tasks)

    async def _safe_emit(self, event, data, namespace, room, skip_sid,
                         callback):
        """Emit an event while keeping the packets of each event contiguous on
        every recipient connection.

        One lock per Engine.IO connection is acquired for the whole duration
        of the emit. Locks are taken in a deterministic order (sorted eio_sid)
        so two broadcasts whose recipient sets overlap cannot deadlock and
        cannot interleave on the shared connections. Connections that are not
        shared keep emitting in parallel.
        """
        recipients = [(sid, eio_sid)
                      for sid, eio_sid in self.get_participants(namespace, room)
                      if sid not in skip_sid]
        if not recipients:
            return
        eio_sids = sorted({eio_sid for _, eio_sid in recipients})
        locks = [self._get_send_lock(eio_sid) for eio_sid in eio_sids]
        # waiting for a lock is cancellation clean: nothing has been sent yet,
        # so a cancellation here leaves no partial event on any connection
        for lock in locks:
            await lock.acquire()
        try:
            tasks = [
                asyncio.ensure_future(
                    self._emit_to_recipient(sid, eio_sid, event, data,
                                            namespace, callback))
                for sid, eio_sid in recipients
            ]
            # let every recipient task start and reach its first send before
            # suspending; otherwise a cancellation arriving at this coroutine
            # could find tasks that never ran
            await asyncio.sleep(0)
            try:
                # shield the wait: if the emit is cancelled, the recipient
                # tasks must keep running to completion instead of being
                # abandoned mid-event
                await asyncio.shield(self._await_recipient_tasks(tasks))
            except asyncio.CancelledError:
                # the emit was cancelled after at least one event header was
                # enqueued. Drain every recipient send so no connection is
                # left expecting attachments, then surface the cancellation.
                await self._drain_recipient_tasks(tasks)
                raise
        finally:
            for lock in locks:
                lock.release()

    async def _emit_to_recipient(self, sid, eio_sid, event, data, namespace,
                                 callback):
        """Build and send the complete event to a single recipient. Runs with
        the recipient connection's send lock held, so all the Engine.IO
        packets of the event are enqueued back to back."""
        if callback is None:
            pkt = self.server.packet_class(
                packet.EVENT, namespace=namespace, data=[event] + data)
            encoded_packet = pkt.encode()
            if not isinstance(encoded_packet, list):
                encoded_packet = [encoded_packet]
            for encoded in encoded_packet:
                await self.server._send_eio_packet(
                    eio_sid,
                    eio_packet.Packet(eio_packet.MESSAGE, encoded))
        else:
            # callbacks require a unique ack id per recipient
            id = self._generate_ack_id(sid, callback)
            pkt = self.server.packet_class(
                packet.EVENT, namespace=namespace, data=[event] + data,
                id=id)
            await self.server._send_packet(eio_sid, pkt)

    async def _await_recipient_tasks(self, tasks):
        """Wait for every recipient send to finish and surface the first
        exception. All tasks run to completion while the connection locks are
        held, so a failure on one connection cannot leave a half-sent event on
        another."""
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_EXCEPTION)
        if pending:
            await asyncio.wait(pending)
        for task in tasks:
            exc = task.exception()
            if exc is not None:
                raise exc

    async def _drain_recipient_tasks(self, tasks):
        """Wait for in-flight recipient sends to finish. The recipient tasks
        are independent tasks and are not cancelled when the emitting
        coroutine is, so awaiting them here completes every event that already
        started, preventing half-sent messages."""
        # shield so a second cancellation of this coroutine still lets the
        # recipient tasks run to completion
        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))

    async def pre_connect(self, eio_sid, namespace):
        """Register a client connection to a namespace.

        The client is initially marked as pre-connected and later updated to
        fully connected once it passes authentication in the connect handler.

        Note: this method is a coroutine.
        """
        return super().pre_connect(eio_sid, namespace)

    async def connect(self, sid, namespace):
        """Register a connected client as fully connected.

        Note: this method is a coroutine.
        """
        if not self.is_pre_connected(sid, namespace):
            sid = await self.pre_connect(sid, namespace)
        return super().connect(sid, namespace)

    async def disconnect(self, sid, namespace, **kwargs):
        """Disconnect a client.

        Note: this method is a coroutine.
        """
        return self.basic_disconnect(sid, namespace, **kwargs)

    async def enter_room(self, sid, namespace, room, eio_sid=None):
        """Add a client to a room.

        Note: this method is a coroutine.
        """
        return self.basic_enter_room(sid, namespace, room, eio_sid=eio_sid)

    async def leave_room(self, sid, namespace, room):
        """Remove a client from a room.

        Note: this method is a coroutine.
        """
        return self.basic_leave_room(sid, namespace, room)

    async def close_room(self, room, namespace):
        """Remove all participants from a room.

        Note: this method is a coroutine.
        """
        return self.basic_close_room(room, namespace)

    async def trigger_callback(self, sid, id, data):
        """Invoke an application callback.

        Note: this method is a coroutine.
        """
        callback = None
        try:
            callback = self.callbacks[sid][id]
        except KeyError:
            # if we get an unknown callback we just ignore it
            self._get_logger().warning('Unknown callback received, ignoring.')
        else:
            del self.callbacks[sid][id]
        if callback is not None:
            ret = callback(*data)
            if inspect.iscoroutine(ret):
                try:
                    await ret
                except asyncio.CancelledError:  # pragma: no cover
                    pass
