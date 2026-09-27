import base64
from functools import partial
import threading
import time
import uuid

from .manager import Manager
from .packet import Packet


class ClusterConnection:
    """A connection observed on a node of the cluster.

    Instances of this class are returned by
    :meth:`PubSubManager.query_connections` and represent the state of a
    connection at the time the query ran. The ``join()``, ``leave()`` and
    ``disconnect()`` methods issue operations against the node that owns the
    connection. If the connection went away or was replaced by a new session
    since the query ran, these operations report ``'stale'`` and have no
    effect.

    :param manager: the manager that produced this result.
    :param host_id: the id of the node that owns the connection.
    :param sid: the Socket.IO session id of the connection.
    :param namespace: the namespace of the connection.
    :param rooms: the rooms the connection was in when the query ran.
    :param session: the session data that was explicitly requested in the
                    query.
    :param token: an opaque token that identifies this exact connection
                  instance, used to detect stale operations.
    """
    def __init__(self, manager, host_id, sid, namespace, rooms, session,
                 token):
        self.manager = manager
        self.host_id = host_id
        self.sid = sid
        self.namespace = namespace
        self.rooms = rooms
        self.session = session
        self.token = token

    def join(self, room, timeout=5):
        """Add this connection to a room on the node that owns it.

        :param room: the room to join.
        :param timeout: how long to wait for a response from the node.

        Returns ``'ok'`` if the operation was applied, ``'stale'`` if the
        connection no longer matches the observed state, or ``'timeout'`` if
        the node did not respond in time.
        """
        return self.manager._remote_op('join', self, room=room,
                                       timeout=timeout)

    def leave(self, room, timeout=5):
        """Remove this connection from a room on the node that owns it.

        :param room: the room to leave.
        :param timeout: how long to wait for a response from the node.

        The return value is the same as in :meth:`join`.
        """
        return self.manager._remote_op('leave', self, room=room,
                                       timeout=timeout)

    def disconnect(self, timeout=5):
        """Disconnect this connection on the node that owns it.

        :param timeout: how long to wait for a response from the node.

        The return value is the same as in :meth:`join`.
        """
        return self.manager._remote_op('disconnect', self, timeout=timeout)

    def __repr__(self):
        return '<ClusterConnection sid={} namespace={} host={}>'.format(
            self.sid, self.namespace, self.host_id)


class ClusterConnections:
    """Result of a cluster-wide connection query.

    This is a point-in-time observation of the cluster, assembled from the
    responses received while the query ran. It is not a globally consistent
    snapshot.

    :param connections: a list of :class:`ClusterConnection` objects.
    :param missing_hosts: a list of host ids that were known to the cluster
                          but did not respond to the query in time. A host
                          listed here may still have connections, so its
                          absence from ``connections`` must not be
                          interpreted as a lack of connections.
    """
    def __init__(self, connections, missing_hosts):
        self.connections = connections
        self.missing_hosts = missing_hosts

    @property
    def complete(self):
        """``True`` if all the known hosts responded to the query."""
        return len(self.missing_hosts) == 0

    def __iter__(self):
        return iter(self.connections)

    def __len__(self):
        return len(self.connections)


class PubSubManager(Manager):
    """Manage a client list attached to a pub/sub backend.

    This is a base class that enables multiple servers to share the list of
    clients, with the servers communicating events through a pub/sub backend.
    The use of a pub/sub backend also allows any client connected to the
    backend to emit events addressed to Socket.IO clients.

    The actual backends must be implemented by subclasses, this class only
    provides a pub/sub generic framework.

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
    name = 'pubsub'

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

    def emit(self, event, data, namespace=None, room=None, skip_sid=None,
             callback=None, to=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace.

        This method takes care or propagating the message to all the servers
        that are connected through the message queue.

        The parameters are the same as in :meth:`.Server.emit`.
        """
        room = to or room
        if kwargs.get('ignore_queue'):
            return super().emit(
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
        self._handle_emit(message)  # handle in this host
        self._publish(message)  # notify other hosts

    def can_disconnect(self, sid, namespace):
        if self.is_connected(sid, namespace):
            # client is in this server, so we can disconnect directly
            return super().can_disconnect(sid, namespace)
        else:
            # client is in another server, so we post request to the queue
            message = {'method': 'disconnect', 'sid': sid,
                       'namespace': namespace or '/', 'host_id': self.host_id}
            self._handle_disconnect(message)  # handle in this host
            self._publish(message)  # notify other hosts

    def disconnect(self, sid, namespace=None, **kwargs):
        if kwargs.get('ignore_queue'):
            return super().disconnect(sid, namespace=namespace)
        message = {'method': 'disconnect', 'sid': sid,
                   'namespace': namespace or '/', 'host_id': self.host_id}
        self._handle_disconnect(message)  # handle in this host
        self._publish(message)  # notify other hosts

    def enter_room(self, sid, namespace, room, eio_sid=None):
        if self.is_connected(sid, namespace):
            # client is in this server, so we can add to the room directly
            return super().enter_room(sid, namespace, room, eio_sid=eio_sid)
        else:
            message = {'method': 'enter_room', 'sid': sid, 'room': room,
                       'namespace': namespace or '/', 'host_id': self.host_id}
            self._publish(message)  # notify other hosts

    def leave_room(self, sid, namespace, room):
        if self.is_connected(sid, namespace):
            # client is in this server, so we can remove from the room directly
            return super().leave_room(sid, namespace, room)
        else:
            message = {'method': 'leave_room', 'sid': sid, 'room': room,
                       'namespace': namespace or '/', 'host_id': self.host_id}
            self._publish(message)  # notify other hosts

    def close_room(self, room, namespace=None):
        message = {'method': 'close_room', 'room': room,
                   'namespace': namespace or '/', 'host_id': self.host_id}
        self._handle_close_room(message)  # handle in this host
        self._publish(message)  # notify other hosts

    def query_connections(self, namespace=None, room=None, session_keys=None,
                          timeout=5):
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
        ``missing_hosts`` attribute; a missing node must not be interpreted
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

        The return value is a :class:`ClusterConnections` object.
        """
        if self.server is None:
            raise RuntimeError('Connections can only be queried from the '
                               'context of a server.')
        namespace = namespace or '/'
        query_id = uuid.uuid4().hex
        pending = {'responses': {}, 'event': threading.Event()}
        self._pending_queries[query_id] = pending
        try:
            message = {'method': 'query_connections', 'query_id': query_id,
                       'namespace': namespace, 'room': room,
                       'session_keys': session_keys, 'host_id': self.host_id}
            self._handle_query_connections(message)  # handle in this host
            self._publish(message)  # ask the other hosts
            deadline = time.monotonic() + timeout
            while True:
                expected = self.known_hosts | {self.host_id}
                remaining = deadline - time.monotonic()
                if remaining <= 0 or \
                        expected.issubset(pending['responses'].keys()):
                    break
                pending['event'].wait(remaining)
        finally:
            self._pending_queries.pop(query_id, None)
        connections = []
        for host_id, host_connections in pending['responses'].items():
            for c in host_connections:
                connections.append(ClusterConnection(
                    self, host_id=host_id, sid=c['sid'],
                    namespace=c['namespace'], rooms=c['rooms'],
                    session=c['session'], token=c['token']))
        missing_hosts = list(
            (self.known_hosts | {self.host_id}) - pending['responses'].keys())
        return ClusterConnections(connections, missing_hosts)

    def _remote_op(self, op, connection, room=None, timeout=5):
        """Perform a room or disconnect operation on the node that owns a
        connection returned by :meth:`query_connections`.

        Returns ``'ok'`` if the operation was applied, ``'stale'`` if the
        connection no longer matches the observed state, or ``'timeout'`` if
        the node did not respond in time.
        """
        if self.server is None:
            raise RuntimeError('Remote operations can only be issued from '
                               'the context of a server.')
        request_id = uuid.uuid4().hex
        pending = {'status': None, 'event': threading.Event()}
        self._pending_ops[request_id] = pending
        try:
            message = {'method': 'remote_op', 'op': op,
                       'request_id': request_id,
                       'target_host': connection.host_id,
                       'sid': connection.sid,
                       'namespace': connection.namespace, 'room': room,
                       'token': connection.token, 'host_id': self.host_id}
            if connection.host_id == self.host_id:
                self._handle_remote_op(message)  # handle in this host
            else:
                self._publish(message)  # send to the owning host
            pending['event'].wait(timeout)
            return pending['status'] or 'timeout'
        finally:
            self._pending_ops.pop(request_id, None)

    def _publish(self, data):
        """Publish a message on the Socket.IO channel.

        This method needs to be implemented by the different subclasses that
        support pub/sub backends.
        """
        raise NotImplementedError('This method must be implemented in a '
                                  'subclass.')  # pragma: no cover

    def _listen(self):
        """Return the next message published on the Socket.IO channel,
        blocking until a message is available.

        This method needs to be implemented by the different subclasses that
        support pub/sub backends.
        """
        raise NotImplementedError('This method must be implemented in a '
                                  'subclass.')  # pragma: no cover

    def _handle_emit(self, message):
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
        super().emit(message['event'], data,
                     namespace=message.get('namespace'),
                     room=message.get('room'),
                     skip_sid=message.get('skip_sid'), callback=callback)

    def _handle_callback(self, message):
        if self.host_id == message.get('host_id'):
            try:
                sid = message['sid']
                id = message['id']
                args = message['args']
            except KeyError:
                return
            self.trigger_callback(sid, id, args)

    def _return_callback(self, host_id, sid, namespace, callback_id, *args):
        # When an event callback is received, the callback is returned back
        # to the sender, which is identified by the host_id
        if host_id == self.host_id:
            self.trigger_callback(sid, callback_id, args)
        else:
            self._publish({'method': 'callback', 'host_id': host_id,
                           'sid': sid, 'namespace': namespace,
                           'id': callback_id, 'args': args})

    def _handle_disconnect(self, message):
        self.server.disconnect(sid=message.get('sid'),
                               namespace=message.get('namespace'),
                               ignore_queue=True)

    def _handle_enter_room(self, message):
        sid = message.get('sid')
        namespace = message.get('namespace')
        if self.is_connected(sid, namespace):
            super().enter_room(sid, namespace, message.get('room'))

    def _handle_leave_room(self, message):
        sid = message.get('sid')
        namespace = message.get('namespace')
        if self.is_connected(sid, namespace):
            super().leave_room(sid, namespace, message.get('room'))

    def _handle_close_room(self, message):
        super().close_room(room=message.get('room'),
                           namespace=message.get('namespace'))

    def _get_connection_info(self, sid, namespace, session_keys):
        session = {}
        if session_keys:
            try:
                s = self.server.get_session(sid, namespace=namespace)
            except KeyError:
                s = {}
            session = {k: s[k] for k in session_keys if k in s}
        return {'sid': sid, 'namespace': namespace,
                'rooms': self.get_rooms(sid, namespace),
                'session': session,
                'token': self.eio_sid_from_sid(sid, namespace)}

    def _handle_query_connections(self, message):
        namespace = message.get('namespace') or '/'
        room = message.get('room')
        if room is None:
            sids = [sid for sid in self.rooms.get(namespace, {}).get(None, {})
                    if self.is_connected(sid, namespace)]
        else:
            sids = [sid for sid, _ in self.get_participants(namespace, room)]
        connections = [
            self._get_connection_info(sid, namespace,
                                      message.get('session_keys'))
            for sid in sids
        ]
        response = {'method': 'query_connections_response',
                    'query_id': message.get('query_id'),
                    'host_id': self.host_id,
                    'target_host': message.get('host_id'),
                    'connections': connections}
        if message.get('host_id') == self.host_id:
            self._handle_query_connections_response(response)
        else:
            self._publish(response)

    def _handle_query_connections_response(self, message):
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

    def _handle_remote_op(self, message):
        if message.get('target_host') != self.host_id:
            return
        status = self._execute_remote_op(message)
        response = {'method': 'remote_op_response',
                    'request_id': message.get('request_id'),
                    'status': status, 'host_id': self.host_id,
                    'target_host': message.get('host_id')}
        if message.get('host_id') == self.host_id:
            self._handle_remote_op_response(response)
        else:
            self._publish(response)

    def _execute_remote_op(self, message):
        sid = message.get('sid')
        namespace = message.get('namespace') or '/'
        if not self.is_connected(sid, namespace) or \
                self.eio_sid_from_sid(sid, namespace) != message.get('token'):
            # the connection went away or was replaced by a new session
            # since the query ran, so the operation must not be applied
            return 'stale'
        op = message.get('op')
        if op == 'join':
            super().enter_room(sid, namespace, message.get('room'))
        elif op == 'leave':
            super().leave_room(sid, namespace, message.get('room'))
        elif op == 'disconnect':
            self.server.disconnect(sid=sid, namespace=namespace,
                                   ignore_queue=True)
        else:
            return 'error'
        return 'ok'

    def _handle_remote_op_response(self, message):
        if message.get('target_host') != self.host_id:
            return
        pending = self._pending_ops.get(message.get('request_id'))
        if pending is None or pending['status'] is not None:
            # late or duplicate response
            return
        pending['status'] = message.get('status')
        pending['event'].set()

    def _handle_message(self, data):
        host_id = data.get('host_id')
        if host_id is not None and host_id != self.host_id:
            self.known_hosts.add(host_id)
        if data['method'] == 'callback':
            self._handle_callback(data)
        elif host_id != self.host_id:
            if data['method'] == 'emit':
                self._handle_emit(data)
            elif data['method'] == 'disconnect':
                self._handle_disconnect(data)
            elif data['method'] == 'enter_room':
                self._handle_enter_room(data)
            elif data['method'] == 'leave_room':
                self._handle_leave_room(data)
            elif data['method'] == 'close_room':
                self._handle_close_room(data)
            elif data['method'] == 'query_connections':
                self._handle_query_connections(data)
            elif data['method'] == 'query_connections_response':
                self._handle_query_connections_response(data)
            elif data['method'] == 'remote_op':
                self._handle_remote_op(data)
            elif data['method'] == 'remote_op_response':
                self._handle_remote_op_response(data)

    def _thread(self):
        while True:
            try:
                for message in self._listen():
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
                            self._handle_message(data)
                        except Exception:
                            self.server.logger.exception(
                                'Handler error in pubsub listening thread')
                self.server.logger.error('pubsub listen() exited unexpectedly')
                break  # loop should never exit except in unit tests!
            except Exception:  # pragma: no cover
                self.server.logger.exception('Unexpected Error in pubsub '
                                             'listening thread')
