import asyncio
import base64
from functools import partial
import uuid

from .async_manager import AsyncManager
from .packet import Packet
from .pubsub_manager import ClusterConnection, ClusterConnections


class AsyncClusterConnection(ClusterConnection):
    """A connection observed on a node of the cluster.

    The asyncio version of :class:`socketio.pubsub_manager.ClusterConnection`.
    The `join()`, `leave()` and `disconnect()` methods are coroutines.
    """
    async def join(self, room, timeout=5):
        """Add this connection to a room on the node that owns it.

        Note: this method is a coroutine.
        """
        return await self.manager._remote_op('join', self, room=room,
                                             timeout=timeout)

    async def leave(self, room, timeout=5):
        """Remove this connection from a room on the node that owns it.

        Note: this method is a coroutine.
        """
        return await self.manager._remote_op('leave', self, room=room,
                                             timeout=timeout)

    async def disconnect(self, timeout=5):
        """Disconnect this connection on the node that owns it.

        Note: this method is a coroutine.
        """
        return await self.manager._remote_op('disconnect', self,
                                             timeout=timeout)


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
        self.known_hosts = set()
        self._pending_queries = {}
        self._pending_ops = {}
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

    async def query_connections(self, namespace=None, room=None,
                                session_keys=None, timeout=5):
        """Query the cluster for connections.

        This method asks all the nodes in the cluster to report their
        connections, and returns the aggregated result. Unlike
        :meth:`get_participants`, which only reports the connections that
        are local to this process, this method includes connections from all
        the nodes that respond.

        The result is a point-in-time observation of the cluster assembled
        from the responses received while the query runs. It is not a
        globally consistent snapshot. Nodes that are known to the cluster
        but fail to respond in time are reported in the result's
        `missing_hosts` attribute; a missing node must not be interpreted
        as a node without connections.

        :param namespace: The Socket.IO namespace to query. If omitted, the
                          default namespace is used.
        :param room: If given, only connections that are in this room are
                     returned. If omitted, all the connections in the
                     namespace are returned.
        :param session_keys: An optional list of session keys to include in
                             the result. Only the given keys are read from
                             each connection's session. If omitted, no
                             session data is collected, so sensitive data
                             stored in the session (such as the request
                             environment) is never serialized onto the
                             pub/sub channel unless explicitly requested.
        :param timeout: How long to wait for responses from the nodes of
                        the cluster, in seconds.

        The return value is a
        :class:`socketio.pubsub_manager.ClusterConnections` object.

        Note: this method is a coroutine.
        """
        if self.server is None:
            raise RuntimeError('Connections can only be queried from the '
                               'context of a server.')
        namespace = namespace or '/'
        query_id = uuid.uuid4().hex
        pending = {'responses': {}, 'event': asyncio.Event()}
        self._pending_queries[query_id] = pending
        try:
            message = {'method': 'query_connections', 'query_id': query_id,
                       'namespace': namespace, 'room': room,
                       'session_keys': session_keys, 'host_id': self.host_id}
            await self._handle_query_connections(message)  # this host
            await self._publish(message)  # ask the other hosts
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                expected = self.known_hosts | {self.host_id}
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0 or \
                        expected.issubset(pending['responses'].keys()):
                    break
                try:
                    await asyncio.wait_for(pending['event'].wait(), remaining)
                except asyncio.TimeoutError:
                    pass
        finally:
            self._pending_queries.pop(query_id, None)
        connections = []
        for host_id, host_connections in pending['responses'].items():
            for c in host_connections:
                connections.append(AsyncClusterConnection(
                    self, host_id=host_id, sid=c['sid'],
                    namespace=c['namespace'], rooms=c['rooms'],
                    session=c['session'], token=c['token']))
        missing_hosts = list(
            (self.known_hosts | {self.host_id}) - pending['responses'].keys())
        return ClusterConnections(connections, missing_hosts)

    async def _remote_op(self, op, connection, room=None, timeout=5):
        """Perform a room or disconnect operation on the node that owns a
        connection returned by :meth:`query_connections`.

        Returns `'ok'` if the operation was applied, `'stale'` if the
        connection no longer matches the observed state, or `'timeout'` if
        the node did not respond in time.
        """
        if self.server is None:
            raise RuntimeError('Remote operations can only be issued from '
                               'the context of a server.')
        request_id = uuid.uuid4().hex
        pending = {'status': None, 'event': asyncio.Event()}
        self._pending_ops[request_id] = pending
        try:
            message = {'method': 'remote_op', 'op': op,
                       'request_id': request_id,
                       'target_host': connection.host_id,
                       'sid': connection.sid,
                       'namespace': connection.namespace, 'room': room,
                       'token': connection.token, 'host_id': self.host_id}
            if connection.host_id == self.host_id:
                await self._handle_remote_op(message)  # handle in this host
            else:
                await self._publish(message)  # send to the owning host
            try:
                await asyncio.wait_for(pending['event'].wait(), timeout)
            except asyncio.TimeoutError:
                pass
            return pending['status'] or 'timeout'
        finally:
            self._pending_ops.pop(request_id, None)

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

    async def _get_connection_info(self, sid, namespace, session_keys):
        session = {}
        if session_keys:
            try:
                s = await self.server.get_session(sid, namespace=namespace)
            except KeyError:
                s = {}
            session = {k: s[k] for k in session_keys if k in s}
        return {'sid': sid, 'namespace': namespace,
                'rooms': self.get_rooms(sid, namespace),
                'session': session,
                'token': self.eio_sid_from_sid(sid, namespace)}

    async def _handle_query_connections(self, message):
        namespace = message.get('namespace') or '/'
        room = message.get('room')
        if room is None:
            sids = [sid for sid in self.rooms.get(namespace, {}).get(None, {})
                    if self.is_connected(sid, namespace)]
        else:
            sids = [sid for sid, _ in self.get_participants(namespace, room)]
        connections = [
            await self._get_connection_info(sid, namespace,
                                            message.get('session_keys'))
            for sid in sids
        ]
        response = {'method': 'query_connections_response',
                    'query_id': message.get('query_id'),
                    'host_id': self.host_id,
                    'target_host': message.get('host_id'),
                    'connections': connections}
        if message.get('host_id') == self.host_id:
            await self._handle_query_connections_response(response)
        else:
            await self._publish(response)

    async def _handle_query_connections_response(self, message):
        if message.get('target_host') != self.host_id:
            return
        pending = self._pending_queries.get(message.get('query_id'))
        if pending is None:
            # the query already finished, so this is a late response
            return
        host_id = message.get('host_id')
        if host_id not in pending['responses']:
            pending['responses'][host_id] = message.get('connections') or []
            pending['event'].set()

    async def _handle_remote_op(self, message):
        if message.get('target_host') != self.host_id:
            return
        status = await self._execute_remote_op(message)
        response = {'method': 'remote_op_response',
                    'request_id': message.get('request_id'),
                    'status': status, 'host_id': self.host_id,
                    'target_host': message.get('host_id')}
        if message.get('host_id') == self.host_id:
            await self._handle_remote_op_response(response)
        else:
            await self._publish(response)

    async def _execute_remote_op(self, message):
        sid = message.get('sid')
        namespace = message.get('namespace') or '/'
        if not self.is_connected(sid, namespace) or \
                self.eio_sid_from_sid(sid, namespace) != message.get('token'):
            # the connection went away or was replaced by a new session
            # since the query ran, so the operation must not be applied
            return 'stale'
        op = message.get('op')
        if op == 'join':
            await super().enter_room(sid, namespace, message.get('room'))
        elif op == 'leave':
            await super().leave_room(sid, namespace, message.get('room'))
        elif op == 'disconnect':
            await self.server.disconnect(sid=sid, namespace=namespace,
                                         ignore_queue=True)
        else:
            return 'error'
        return 'ok'

    async def _handle_remote_op_response(self, message):
        if message.get('target_host') != self.host_id:
            return
        pending = self._pending_ops.get(message.get('request_id'))
        if pending is None or pending['status'] is not None:
            # late or duplicate response
            return
        pending['status'] = message.get('status')
        pending['event'].set()

    async def _handle_message(self, data):
        host_id = data.get('host_id')
        if host_id is not None and host_id != self.host_id:
            self.known_hosts.add(host_id)
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
            elif data['method'] == 'query_connections':
                await self._handle_query_connections(data)
            elif data['method'] == 'query_connections_response':
                await self._handle_query_connections_response(data)
            elif data['method'] == 'remote_op':
                await self._handle_remote_op(data)
            elif data['method'] == 'remote_op_response':
                await self._handle_remote_op_response(data)

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
                        try:
                            await self._handle_message(data)
                        except asyncio.CancelledError:
                            raise  # let the outer try/except handle it
                        except Exception:
                            self.server.logger.exception(
                                'Handler error in pubsub listening thread')
                self.server.logger.error('pubsub listen() exited unexpectedly')
                break  # loop should never exit except in unit tests!
            except asyncio.CancelledError:  # pragma: no cover
                break
            except Exception:  # pragma: no cover
                self.server.logger.exception('Unexpected Error in pubsub '
                                             'listening thread')
