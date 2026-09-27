import asyncio
import base64
from functools import partial
import uuid

from .async_manager import AsyncManager
from .packet import Packet
from .pubsub_manager import ClusterOperationResult, ClusterQueryResult


class AsyncClusterQueryResult(ClusterQueryResult):
    """Async version of :class:`ClusterQueryResult`, returned by
    `AsyncPubSubManager.get_cluster_participants`."""
    async def join(self, room, timeout=1.0):
        """Join all the participants in this result to the given room,
        including participants connected to other hosts.

        :param room: the room to join.
        :param timeout: how long to wait for the remote hosts to acknowledge
                        the operation.

        Returns a :class:`ClusterOperationResult` instance.
        Participants whose connection changed since the query was made are
        reported as stale and are not affected.

        Note: this method is a coroutine.
        """
        if room is None:
            raise ValueError('A room is required for this operation')
        return await self._manager._cluster_operation(
            'join', self._targets(), room=room, timeout=timeout)

    async def leave(self, room, timeout=1.0):
        """Remove all the participants in this result from the given room,
        including participants connected to other hosts.

        :param room: the room to leave.
        :param timeout: how long to wait for the remote hosts to acknowledge
                        the operation.

        Returns a :class:`ClusterOperationResult` instance.
        Participants whose connection changed since the query was made are
        reported as stale and are not affected.

        Note: this method is a coroutine.
        """
        if room is None:
            raise ValueError('A room is required for this operation')
        return await self._manager._cluster_operation(
            'leave', self._targets(), room=room, timeout=timeout)

    async def disconnect(self, timeout=1.0):
        """Disconnect all the participants in this result, including
        participants connected to other hosts.

        :param timeout: how long to wait for the remote hosts to acknowledge
                        the operation.

        Returns a :class:`ClusterOperationResult` instance.
        Participants whose connection changed since the query was made are
        reported as stale and are not affected.

        Note: this method is a coroutine.
        """
        return await self._manager._cluster_operation(
            'disconnect', self._targets(), timeout=timeout)


class AsyncPubSubManager(AsyncManager):
    """Manage a client list attached to a pub/sub backend under asyncio.

    This is a base class that enables multiple servers to share the list of
    clients, with the servers communicating events through a pub/sub backend.
    The use of a pub/sub backend also allows any client connected to the
    backend to emit events addressed to Socket.IO clients.

    The actual backends must be implemented by subclasses, this class only
    provides a pub/sub generic framework for asyncio applications.

    :param channel: The channel name on which the server sends and receives
                    notifications.
    :param write_only: If set to ``True``, only initialize to emit events. The
                       default of ``False`` initializes the class for emitting
                       and receiving. A write-only instance can be used
                       independently of the server to emit to clients from an
                       external process.
    :param logger: a custom logger to log it. If not given, the server logger
                   is used.
    :param json: An alternative JSON module to use for encoding and decoding
                 packets. Custom json modules must have ``dumps`` and ``loads``
                 functions that are compatible with the standard library
                 versions. This setting is only used when ``write_only`` is set
                 to ``True``. Otherwise the JSON module configured in the
                 server is used.
    """
    name = 'asyncpubsub'

    def __init__(self, channel='socketio', write_only=False, logger=None,
                 json=None):
        super().__init__()
        self.channel = channel
        self.write_only = write_only
        self.host_id = uuid.uuid4().hex
        self.logger = logger
        self._known_hosts = set()
        self._pending_requests = {}
        if json is not None:
            self.json = json

    def initialize(self):
        super().initialize()
        if not self.write_only:
            self.thread = self.server.start_background_task(self._thread)
        self._get_logger().info(self.name + ' backend initialized.')

    async def emit(self, event, data, namespace=None, room=None, skip_sid=None,
                   callback=None, to=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace.

        This method takes care or propagating the message to all the servers
        that are connected through the message queue.

        The parameters are the same as in :meth:`.Server.emit`.

        Note: this method is a coroutine.
        """
        room = to or room
        if kwargs.get('ignore_queue'):
            return await super().emit(
                event, data, namespace=namespace, room=room, skip_sid=skip_sid,
                callback=callback)
        namespace = namespace or '/'
        if callback is not None:
            if self.server is None:
                raise RuntimeError('Callbacks can only be issued from the '
                                   'context of a server.')
            if room is None:
                raise ValueError('Cannot use callback without a room set.')
            id = self._generate_ack_id(room, callback)
            callback = (room, namespace, id)
        else:
            callback = None
        if isinstance(data, tuple):
            data = list(data)
        else:
            data = [data]
        binary = Packet.data_is_binary(data)
        if binary:
            data, attachments = Packet.deconstruct_binary(data)
            data = [data, *[base64.b64encode(a).decode() for a in attachments]]
        message = {'method': 'emit', 'event': event, 'data': data,
                   'binary': binary, 'namespace': namespace, 'room': room,
                   'skip_sid': skip_sid, 'callback': callback,
                   'host_id': self.host_id}
        await self._handle_emit(message)  # handle in this host
        await self._publish(message)  # notify other hosts

    async def can_disconnect(self, sid, namespace):
        if self.is_connected(sid, namespace):
            # client is in this server, so we can disconnect directly
            return await super().can_disconnect(sid, namespace)
        else:
            # client is in another server, so we post request to the queue
            await self._publish({'method': 'disconnect', 'sid': sid,
                                 'namespace': namespace or '/',
                                 'host_id': self.host_id})

    async def disconnect(self, sid, namespace, **kwargs):
        if kwargs.get('ignore_queue'):
            return await super().disconnect(
                sid, namespace=namespace)
        message = {'method': 'disconnect', 'sid': sid,
                   'namespace': namespace or '/', 'host_id': self.host_id}
        await self._handle_disconnect(message)  # handle in this host
        await self._publish(message)  # notify other hosts

    async def enter_room(self, sid, namespace, room, eio_sid=None):
        if self.is_connected(sid, namespace):
            # client is in this server, so we can disconnect directly
            return await super().enter_room(sid, namespace, room,
                                            eio_sid=eio_sid)
        else:
            message = {'method': 'enter_room', 'sid': sid, 'room': room,
                       'namespace': namespace or '/', 'host_id': self.host_id}
            await self._publish(message)  # notify other hosts

    async def leave_room(self, sid, namespace, room):
        if self.is_connected(sid, namespace):
            # client is in this server, so we can disconnect directly
            return await super().leave_room(sid, namespace, room)
        else:
            message = {'method': 'leave_room', 'sid': sid, 'room': room,
                       'namespace': namespace or '/', 'host_id': self.host_id}
            await self._publish(message)  # notify other hosts

    async def close_room(self, room, namespace=None):
        message = {'method': 'close_room', 'room': room,
                   'namespace': namespace or '/', 'host_id': self.host_id}
        await self._handle_close_room(message)  # handle in this host
        await self._publish(message)  # notify other hosts

    async def get_cluster_participants(self, namespace, room=None,
                                       session_keys=None, timeout=1.0):
        """Return the participants of a room across all the servers in the
        cluster.

        This method sends a query to all the hosts that are known to be part
        of the cluster and assembles a combined participant list from their
        responses. The result is a point-in-time observation and not a
        globally consistent snapshot. Hosts are discovered from previously
        observed traffic, so a host that has not sent any messages since this
        server started is not known and cannot be awaited or reported as
        missing.

        :param namespace: the namespace to query.
        :param room: the room to query. If omitted, all the clients connected
                     to the namespace are returned.
        :param session_keys: an optional list of session keys to include in
                             the result. Only the given keys are read from
                             each client's session. The request environment
                             is never included.
        :param timeout: how long to wait for the known hosts to respond.

        Returns an :class:`AsyncClusterQueryResult` instance. Hosts
        that do not respond within the timeout are reported in the result's
        `missing_hosts` attribute, and are never assumed to have no
        connections.

        Note: this method is a coroutine. It requires the manager to be
        initialized with `write_only` set to `False`,
        since it needs to receive responses from the other hosts.
        """
        namespace = namespace or '/'
        request_id = uuid.uuid4().hex
        expected = set(self._known_hosts)
        pending = {
            'expected': expected,
            'responses': {
                self.host_id: await self._local_participants(
                    namespace, room, session_keys),
            },
            'event': asyncio.Event(),
        }
        self._pending_requests[request_id] = pending
        try:
            await self._publish({
                'method': 'cluster_query',
                'request_id': request_id,
                'namespace': namespace,
                'room': room,
                'session_keys': list(session_keys) if session_keys else [],
                'host_id': self.host_id,
            })
            if expected:
                try:
                    await asyncio.wait_for(pending['event'].wait(), timeout)
                except asyncio.TimeoutError:
                    pass
        finally:
            self._pending_requests.pop(request_id, None)
        participants = []
        tokens = {}
        for host_id, host_participants in pending['responses'].items():
            for participant in host_participants:
                tokens[(host_id, participant['sid'])] = \
                    participant.get('token')
                participants.append({
                    'sid': participant['sid'],
                    'namespace': participant['namespace'],
                    'rooms': participant['rooms'],
                    'session': participant.get('session', {}),
                    'host_id': host_id,
                })
        missing_hosts = sorted(expected - set(pending['responses']))
        return AsyncClusterQueryResult(self, participants, tokens,
                                       missing_hosts)

    async def _local_participants(self, namespace, room, session_keys):
        participants = []
        for sid, eio_sid in self.get_participants(namespace, room):
            participant = {
                'sid': sid,
                'namespace': namespace,
                'rooms': self.get_rooms(sid, namespace),
                'token': eio_sid,
            }
            if session_keys:
                participant['session'] = await self._get_session_data(
                    sid, namespace, session_keys)
            participants.append(participant)
        return participants

    async def _get_session_data(self, sid, namespace, session_keys):
        try:
            session = await self.server.get_session(sid, namespace=namespace)
        except Exception:  # pragma: no cover
            return {}
        data = {}
        for key in session_keys:
            if key in session:
                try:
                    self.json.dumps(session[key])
                except (TypeError, ValueError):
                    # do not include values that cannot be serialized, such
                    # as the raw request environment
                    continue
                data[key] = session[key]
        return data

    async def _cluster_operation(self, operation, targets, room=None,
                                 timeout=1.0):
        request_id = uuid.uuid4().hex
        expected = {host_id for host_id in targets
                    if host_id != self.host_id}
        results = {}
        if self.host_id in targets:
            results.update(await self._apply_cluster_operation(
                operation, targets[self.host_id], room))
        pending = {
            'expected': expected,
            'responses': {},
            'event': asyncio.Event(),
        }
        self._pending_requests[request_id] = pending
        try:
            if expected:
                await self._publish({
                    'method': 'cluster_' + operation,
                    'request_id': request_id,
                    'targets': {host_id: entries for host_id, entries
                                in targets.items()
                                if host_id != self.host_id},
                    'room': room,
                    'host_id': self.host_id,
                })
                try:
                    await asyncio.wait_for(pending['event'].wait(), timeout)
                except asyncio.TimeoutError:
                    pass
        finally:
            self._pending_requests.pop(request_id, None)
        for response in pending['responses'].values():
            results.update(response)
        missing_hosts = expected - set(pending['responses'])
        return ClusterOperationResult(
            ok=sorted(sid for sid, result in results.items()
                      if result == 'ok'),
            stale=sorted(sid for sid, result in results.items()
                         if result == 'stale'),
            unknown=sorted({entry['sid'] for host_id in missing_hosts
                            for entry in targets.get(host_id, [])}),
            missing_hosts=sorted(missing_hosts))

    async def _apply_cluster_operation(self, operation, entries, room):
        results = {}
        for entry in entries:
            sid = entry.get('sid')
            namespace = entry.get('namespace') or '/'
            if not self.is_connected(sid, namespace) or \
                    self.eio_sid_from_sid(sid, namespace) != \
                    entry.get('token'):
                # the connection observed at query time is gone, so the
                # operation is not applied to avoid affecting a different
                # connection that may have reused the session id
                results[sid] = 'stale'
                continue
            if operation == 'join':
                await super().enter_room(sid, namespace, room)
            elif operation == 'leave':
                await super().leave_room(sid, namespace, room)
            else:  # disconnect
                await self.server.disconnect(sid, namespace,
                                             ignore_queue=True)
            results[sid] = 'ok'
        return results

    async def _publish(self, data):
        """Publish a message on the Socket.IO channel.

        This method needs to be implemented by the different subclasses that
        support pub/sub backends.
        """
        raise NotImplementedError('This method must be implemented in a '
                                  'subclass.')  # pragma: no cover

    async def _listen(self):
        """Return the next message published on the Socket.IO channel,
        blocking until a message is available.

        This method needs to be implemented by the different subclasses that
        support pub/sub backends.
        """
        raise NotImplementedError('This method must be implemented in a '
                                  'subclass.')  # pragma: no cover

    async def _handle_emit(self, message):
        # Events with callbacks are very tricky to handle across hosts
        # Here in the receiving end we set up a local callback that preserves
        # the callback host and id from the sender
        remote_callback = message.get('callback')
        remote_host_id = message.get('host_id')
        if remote_callback is not None and len(remote_callback) == 3:
            callback = partial(self._return_callback, remote_host_id,
                               *remote_callback)
        else:
            callback = None
        data = message['data']
        if message.get('binary'):
            attachments = [base64.b64decode(a) for a in data[1:]]
            data = Packet.reconstruct_binary(data[0], attachments)
        if isinstance(data, list):
            if len(data) == 1:
                data = data[0]
            else:
                data = tuple(data)
        await super().emit(message['event'], data,
                           namespace=message.get('namespace'),
                           room=message.get('room'),
                           skip_sid=message.get('skip_sid'),
                           callback=callback)

    async def _handle_callback(self, message):
        if self.host_id == message.get('host_id'):
            try:
                sid = message['sid']
                id = message['id']
                args = message['args']
            except KeyError:
                return
            await self.trigger_callback(sid, id, args)

    async def _return_callback(self, host_id, sid, namespace, callback_id,
                               *args):
        # When an event callback is received, the callback is returned back
        # the sender, which is identified by the host_id
        if host_id == self.host_id:
            await self.trigger_callback(sid, callback_id, args)
        else:
            await self._publish({'method': 'callback', 'host_id': host_id,
                                 'sid': sid, 'namespace': namespace,
                                 'id': callback_id, 'args': args})

    async def _handle_disconnect(self, message):
        await self.server.disconnect(sid=message.get('sid'),
                                     namespace=message.get('namespace'),
                                     ignore_queue=True)

    async def _handle_enter_room(self, message):
        sid = message.get('sid')
        namespace = message.get('namespace')
        if self.is_connected(sid, namespace):
            await super().enter_room(sid, namespace, message.get('room'))

    async def _handle_leave_room(self, message):
        sid = message.get('sid')
        namespace = message.get('namespace')
        if self.is_connected(sid, namespace):
            await super().leave_room(sid, namespace, message.get('room'))

    async def _handle_close_room(self, message):
        await super().close_room(room=message.get('room'),
                                 namespace=message.get('namespace'))

    async def _handle_cluster_query(self, message):
        await self._publish({
            'method': 'cluster_query_response',
            'request_id': message.get('request_id'),
            'host_id': self.host_id,
            'participants': await self._local_participants(
                message.get('namespace') or '/', message.get('room'),
                message.get('session_keys')),
        })

    async def _handle_cluster_query_response(self, message):
        self._record_response(message.get('request_id'),
                              message.get('host_id'),
                              message.get('participants') or [])

    async def _handle_cluster_operation(self, message):
        entries = message.get('targets', {}).get(self.host_id)
        if not entries:
            # this host has no clients targeted by the operation
            return
        operation = message['method'][len('cluster_'):]
        await self._publish({
            'method': 'cluster_operation_response',
            'request_id': message.get('request_id'),
            'host_id': self.host_id,
            'results': await self._apply_cluster_operation(
                operation, entries, message.get('room')),
        })

    async def _handle_cluster_operation_response(self, message):
        self._record_response(message.get('request_id'),
                              message.get('host_id'),
                              message.get('results') or {})

    def _record_response(self, request_id, host_id, payload):
        pending = self._pending_requests.get(request_id)
        if pending is None:
            # the response arrived after the request completed, so it is
            # too late to use it
            return
        if host_id in pending['responses']:
            # a duplicate response from a host that already responded
            return
        pending['responses'][host_id] = payload
        if pending['expected'] <= set(pending['responses']):
            pending['event'].set()

    async def _process_message(self, data):
        host_id = data.get('host_id')
        if host_id is not None and host_id != self.host_id:
            self._known_hosts.add(host_id)
        try:
            if data['method'] == 'callback':
                await self._handle_callback(data)
            elif host_id != self.host_id:
                if data['method'] == 'emit':
                    await self._handle_emit(data)
                elif data['method'] == 'disconnect':
                    await self._handle_disconnect(data)
                elif data['method'] == 'enter_room':
                    await self._handle_enter_room(data)
                elif data['method'] == 'leave_room':
                    await self._handle_leave_room(data)
                elif data['method'] == 'close_room':
                    await self._handle_close_room(data)
                elif data['method'] == 'cluster_query':
                    await self._handle_cluster_query(data)
                elif data['method'] == 'cluster_query_response':
                    await self._handle_cluster_query_response(data)
                elif data['method'] in ('cluster_join', 'cluster_leave',
                                        'cluster_disconnect'):
                    await self._handle_cluster_operation(data)
                elif data['method'] == 'cluster_operation_response':
                    await self._handle_cluster_operation_response(data)
        except asyncio.CancelledError:
            raise  # let the outer try/except handle it
        except Exception:
            self.server.logger.exception(
                'Handler error in pubsub listening thread')

    async def _thread(self):
        while True:
            try:
                async for message in self._listen():  # pragma: no branch
                    data = None
                    if isinstance(message, dict):
                        data = message
                    else:
                        try:
                            data = self.json.loads(message)
                        except:
                            pass
                    if data and 'method' in data:
                        self._get_logger().debug('pubsub message: {}'.format(
                            data['method']))
                        await self._process_message(data)
                self.server.logger.error('pubsub listen() exited unexpectedly')
                break  # loop should never exit except in unit tests!
            except asyncio.CancelledError:  # pragma: no cover
                break
            except Exception:  # pragma: no cover
                self.server.logger.exception('Unexpected Error in pubsub '
                                             'listening thread')
