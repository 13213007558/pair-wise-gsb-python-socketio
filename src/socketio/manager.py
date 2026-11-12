import logging
import threading

from engineio import packet as eio_packet
from . import base_manager
from . import packet

default_logger = logging.getLogger('socketio')


class Manager(base_manager.BaseManager):
    """Manage client connections.

    This class keeps track of all the clients and the rooms they are in, to
    support the broadcasting of messages. The data used by this class is
    stored in a memory structure, making it appropriate only for single process
    services. More sophisticated storage backends can be implemented by
    subclasses.
    """
    def can_disconnect(self, sid, namespace):
        return self.is_connected(sid, namespace)

    def _get_send_lock(self, eio_sid):
        """Return the lock that serializes emits on an Engine.IO connection,
        creating it on first use."""
        lock = self._send_locks.get(eio_sid)
        if lock is None:
            lock = threading.Lock()
            self._send_locks[eio_sid] = lock
        return lock

    def emit(self, event, data, namespace, room=None, skip_sid=None,
             callback=None, to=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace."""
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
            self._safe_emit(event, data, namespace, room, skip_sid, callback)
            return
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
                        self.server._send_eio_packet(eio_sid, p)
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
                    self.server._send_packet(eio_sid, pkt)

    def _safe_emit(self, event, data, namespace, room, skip_sid, callback):
        """Emit an event while keeping the packets of each event contiguous on
        every recipient connection.

        Locks are acquired in a deterministic order (sorted eio_sid) so two
        broadcasts whose recipient sets overlap cannot deadlock and cannot
        interleave on the shared connections.
        """
        recipients = [(sid, eio_sid)
                      for sid, eio_sid in self.get_participants(namespace, room)
                      if sid not in skip_sid]
        if not recipients:
            return
        eio_sids = sorted({eio_sid for _, eio_sid in recipients})
        locks = [self._get_send_lock(eio_sid) for eio_sid in eio_sids]
        acquired = 0
        try:
            for lock in locks:
                lock.acquire()
                acquired += 1
            for sid, eio_sid in recipients:
                if callback is None:
                    pkt = self.server.packet_class(
                        packet.EVENT, namespace=namespace,
                        data=[event] + data)
                    encoded_packet = pkt.encode()
                    if not isinstance(encoded_packet, list):
                        encoded_packet = [encoded_packet]
                    for encoded in encoded_packet:
                        self.server._send_eio_packet(
                            eio_sid,
                            eio_packet.Packet(eio_packet.MESSAGE, encoded))
                else:
                    id = self._generate_ack_id(sid, callback)
                    pkt = self.server.packet_class(
                        packet.EVENT, namespace=namespace,
                        data=[event] + data, id=id)
                    self.server._send_packet(eio_sid, pkt)
        finally:
            for lock in reversed(locks[:acquired]):
                lock.release()

    def disconnect(self, sid, namespace, **kwargs):
        """Register a client disconnect from a namespace."""
        return self.basic_disconnect(sid, namespace)

    def enter_room(self, sid, namespace, room, eio_sid=None):
        """Add a client to a room."""
        return self.basic_enter_room(sid, namespace, room, eio_sid=eio_sid)

    def leave_room(self, sid, namespace, room):
        """Remove a client from a room."""
        return self.basic_leave_room(sid, namespace, room)

    def close_room(self, room, namespace):
        """Remove all participants from a room."""
        return self.basic_close_room(room, namespace)

    def trigger_callback(self, sid, id, data):
        """Invoke an application callback."""
        callback = None
        try:
            callback = self.callbacks[sid][id]
        except KeyError:
            # if we get an unknown callback we just ignore it
            self._get_logger().warning('Unknown callback received, ignoring.')
        else:
            del self.callbacks[sid][id]
        if callback is not None:
            callback(*data)
