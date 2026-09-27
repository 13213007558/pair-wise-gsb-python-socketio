import time
from urllib.parse import urlparse

try:
    import msgpack
except ImportError:  # pragma: no cover
    msgpack = None

try:
    import redis
    from redis.exceptions import RedisError
except ImportError:  # pragma: no cover
    redis = None
    RedisError = None

try:
    import valkey
    from valkey.exceptions import ValkeyError
except ImportError:  # pragma: no cover
    valkey = None
    ValkeyError = None

from .pubsub_manager import PubSubManager
from .packet import EVENT


def parse_redis_sentinel_url(url):
    """Parse a Redis Sentinel URL with the format:
    redis+sentinel://[:password]@host1:port1,host2:port2,.../db/service_name
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme not in {'redis+sentinel', 'valkey+sentinel'}:
        raise ValueError('Invalid Redis Sentinel URL')
    sentinels = []
    for host_port in parsed_url.netloc.split('@')[-1].split(','):
        host, port = host_port.rsplit(':', 1)
        sentinels.append((host, int(port)))
    kwargs = {}
    if parsed_url.username:
        kwargs['username'] = parsed_url.username
    if parsed_url.password:
        kwargs['password'] = parsed_url.password
    service_name = None
    if parsed_url.path:
        parts = parsed_url.path.split('/')
        if len(parts) >= 2 and parts[1] != '':
            kwargs['db'] = int(parts[1])
        if len(parts) >= 3 and parts[2] != '':
            service_name = parts[2]
    return sentinels, service_name, kwargs


class RedisManager(PubSubManager):
    """Redis based client manager.

    This class implements a Redis backend for event sharing across multiple
    processes. Only kept here as one more example of how to build a custom
    backend, since the kombu backend is perfectly adequate to support a Redis
    message queue.

    To use a Redis backend, initialize the :class:`Server` instance as
    follows::

        url = 'redis://hostname:port/0'
        server = socketio.Server(client_manager=socketio.RedisManager(url))

    :param url: The connection URL for the Redis server. For a default Redis
                store running on the same host, use ``redis://``.  To use a
                TLS connection, use ``rediss://``. To use Redis Sentinel, use
                ``redis+sentinel://`` with a comma-separated list of hosts
                and the service name after the db in the URL path. Example:
                ``redis+sentinel://user:pw@host1:1234,host2:2345/0/myredis``.
    :param channel: The channel name on which the server sends and receives
                    notifications. Must be the same in all the servers.
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
    :param redis_options: additional keyword arguments to be passed to
                          ``Redis.from_url()`` or ``Sentinel()``.
    """
    name = 'redis'

    def __init__(self, url='redis://localhost:6379/0', channel='socketio',
                 write_only=False, logger=None, json=None, redis_options=None):
        super().__init__(channel=channel, write_only=write_only, logger=logger,
                         json=json)
        self.redis_url = url
        self.redis_options = redis_options or {}
        self.connected = False
        self.redis = None
        self.pubsub = None

    def initialize(self):  # pragma: no cover
        super().initialize()

        monkey_patched = True
        if self.server.async_mode == 'eventlet':
            from eventlet.patcher import is_monkey_patched
            monkey_patched = is_monkey_patched('socket')
        elif 'gevent' in self.server.async_mode:
            from gevent.monkey import is_module_patched
            monkey_patched = is_module_patched('socket')
        if not monkey_patched:
            raise RuntimeError(
                'Redis requires a monkey patched socket library to work '
                'with ' + self.server.async_mode)

    def _get_redis_module(self):
        parsed_url = urlparse(self.redis_url)
        scheme = parsed_url.scheme.split('+', 1)[0].lower()
        if scheme in ['redis', 'rediss']:
            if redis is None or RedisError is None:
                raise RuntimeError('Redis package is not installed '
                                   '(Run "pip install redis" '
                                   'in your virtualenv).')
            return redis
        if scheme in ['valkey', 'valkeys']:
            if valkey is None or ValkeyError is None:
                raise RuntimeError('Valkey package is not installed '
                                   '(Run "pip install valkey" '
                                   'in your virtualenv).')
            return valkey
        if scheme == 'unix':
            if redis is None or RedisError is None:
                if valkey is None or ValkeyError is None:
                    raise RuntimeError('Redis package is not installed '
                                       '(Run "pip install redis" '
                                       'or "pip install valkey" '
                                       'in your virtualenv).')
                else:
                    return valkey
            else:
                return redis
        error_msg = f'Unsupported Redis URL scheme: {scheme}'
        raise ValueError(error_msg)

    def _redis_connect(self):
        module = self._get_redis_module()
        parsed_url = urlparse(self.redis_url)
        if parsed_url.scheme in {"redis+sentinel", "valkey+sentinel"}:
            sentinels, service_name, connection_kwargs = \
                parse_redis_sentinel_url(self.redis_url)
            kwargs = self.redis_options
            kwargs.update(connection_kwargs)
            sentinel = module.sentinel.Sentinel(sentinels, **kwargs)
            self.redis = sentinel.master_for(service_name or self.channel)
        else:
            self.redis = module.Redis.from_url(self.redis_url,
                                               **self.redis_options)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.connected = True

    def _publish(self, data):  # pragma: no cover
        self._publish_message(self.channel, self.json.dumps(data))

    def _publish_message(self, channel, data):  # pragma: no cover
        for retries_left in range(1, -1, -1):  # 2 attempts
            try:
                if not self.connected:
                    self._redis_connect()
                return self.redis.publish(channel, data)
            except Exception as exc:
                if retries_left > 0:
                    self._get_logger().error(
                        'Cannot publish to redis... retrying',
                        extra={"redis_exception": str(exc)}
                    )
                    self.connected = False
                else:
                    self._get_logger().error(
                        'Cannot publish to redis... giving up',
                        extra={"redis_exception": str(exc)}
                    )
                    break

    def _redis_listen_with_retries(self):  # pragma: no cover
        retry_sleep = 1
        subscribed = False
        while True:
            try:
                if not subscribed:
                    self._redis_connect()
                    self._redis_subscribe()
                    retry_sleep = 1
                yield from self.pubsub.listen()
            except Exception as exc:
                self._get_logger().error(
                    'Cannot receive from redis... '
                    f'retrying in {retry_sleep} secs',
                    extra={"redis_exception": str(exc)})
                subscribed = False
                time.sleep(retry_sleep)
                retry_sleep *= 2
                if retry_sleep > 60:
                    retry_sleep = 60

    def _redis_subscribe(self):  # pragma: no cover
        self.pubsub.subscribe(self.channel)

    def _listen(self):  # pragma: no cover
        channel = self.channel.encode('utf-8')
        for message in self._redis_listen_with_retries():
            if message['channel'] == channel and \
                    message['type'] == 'message' and 'data' in message:
                yield message['data']
        self.pubsub.unsubscribe(self.channel)


class RedisNodeManager(RedisManager):
    """Redis based client manager that interoperates with Node.js servers.

    This class implements the plain broadcast protocol used by the
    official ``@socket.io/redis-adapter`` package for Node.js (version
    8.x), so that Python and Node.js Socket.IO servers connected to the
    same Redis instance can broadcast events to each other's clients.

    The message format used by this class (a MessagePack encoded
    ``[uid, packet, opts]`` envelope published on per-namespace and
    per-room channels) is not compatible with the format used by
    :class:`RedisManager`. The two formats are never mixed: a
    ``RedisNodeManager`` instance only produces and consumes Node.js
    adapter messages, and a :class:`RedisManager` instance only produces
    and consumes its own JSON messages.

    To use this backend, initialize the :class:`Server` instance as
    follows::

        url = 'redis://hostname:port/0'
        server = socketio.Server(
            client_manager=socketio.RedisNodeManager(url))

    Notes and limitations:

    * The ``channel`` argument must match the ``key`` option of the
      Node.js adapter (``'socket.io'`` by default on both sides).
    * Emitting with a callback is not supported, since the broadcast
      protocol has no provision for returning acknowledgements across
      nodes. A ``ValueError`` is raised if a callback is given.
    * Room management (``enter_room``, ``leave_room``, ``close_room``)
      and client disconnection only apply to clients connected to this
      server, as the plain broadcast protocol does not forward these
      operations to other nodes.

    :param url: The connection URL for the Redis server. For a default
                Redis store running on the same host, use ``redis://``.
                To use a TLS connection, use ``rediss://``. To use Redis
                Sentinel, use ``redis+sentinel://`` with a comma-separated
                list of hosts and the service name after the db in the
                URL path.
    :param channel: The channel prefix on which the server sends and
                    receives notifications. Must match the ``key`` option
                    of the Node.js adapter. Defaults to ``'socket.io'``.
    :param write_only: If set to ``True``, only initialize to emit events.
                       The default of ``False`` initializes the class for
                       emitting and receiving. A write-only instance can
                       be used independently of the server to emit to
                       clients from an external process.
    :param logger: a custom logger to log it. If not given, the server
                   logger is used.
    :param redis_options: additional keyword arguments to be passed to
                          ``Redis.from_url()`` or ``Sentinel()``.
    """
    name = 'redisnode'

    def __init__(self, url='redis://localhost:6379/0', channel='socket.io',
                 write_only=False, logger=None, redis_options=None):
        if msgpack is None:  # pragma: no cover
            raise RuntimeError('msgpack package is not installed '
                               '(Run "pip install msgpack" '
                               'in your virtualenv).')
        super().__init__(url=url, channel=channel, write_only=write_only,
                         logger=logger, redis_options=redis_options)

    def emit(self, event, data, namespace=None, room=None, skip_sid=None,
             callback=None, to=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace.

        This method propagates the message to all the Python and Node.js
        servers that are connected through the message queue.

        The parameters are the same as in :meth:`.Server.emit`, except
        that ``callback`` is not supported and raises a ``ValueError``.
        """
        room = to or room
        if kwargs.get('ignore_queue'):
            return super(PubSubManager, self).emit(
                event, data, namespace=namespace, room=room,
                skip_sid=skip_sid, callback=callback)
        if callback is not None:
            raise ValueError('Callbacks are not supported by the Node.js '
                             'Redis adapter broadcast protocol.')
        namespace = namespace or '/'
        if isinstance(data, tuple):
            data = list(data)
        else:
            data = [data]
        rooms = list(room) if isinstance(room, (list, tuple, set)) else \
            ([room] if room is not None else [])
        if skip_sid is None:
            except_sids = []
        elif isinstance(skip_sid, (list, tuple, set)):
            except_sids = list(skip_sid)
        else:
            except_sids = [skip_sid]
        packet = {'type': EVENT, 'data': [event] + data, 'nsp': namespace}
        opts = {'rooms': rooms, 'except': except_sids, 'flags': {}}
        message = msgpack.packb([self.host_id, packet, opts],
                                use_bin_type=True)
        if len(rooms) == 1:
            channel = self._node_channel(namespace, rooms[0])
        else:
            channel = self._node_channel(namespace)
        self._publish_message(channel, message)
        # deliver to the clients connected to this host
        local_data = data[0] if len(data) == 1 else tuple(data)
        super(PubSubManager, self).emit(
            event, local_data, namespace=namespace,
            room=rooms or None, skip_sid=except_sids or None)

    def can_disconnect(self, sid, namespace):
        # remote disconnects are not part of the broadcast protocol, so
        # only clients connected to this server can be disconnected
        return super(PubSubManager, self).can_disconnect(sid, namespace)

    def disconnect(self, sid, namespace=None, **kwargs):
        return super(PubSubManager, self).disconnect(sid, namespace or '/')

    def enter_room(self, sid, namespace, room, eio_sid=None):
        if self.is_connected(sid, namespace):
            return super(PubSubManager, self).enter_room(
                sid, namespace, room, eio_sid=eio_sid)

    def leave_room(self, sid, namespace, room):
        if self.is_connected(sid, namespace):
            return super(PubSubManager, self).leave_room(
                sid, namespace, room)

    def close_room(self, room, namespace=None):
        return super(PubSubManager, self).close_room(room,
                                                     namespace or '/')

    def _node_channel(self, namespace, room=None):
        channel = '{}#{}#'.format(self.channel, namespace)
        if room is not None:
            channel += '{}#'.format(room)
        return channel

    def _redis_subscribe(self):  # pragma: no cover
        self.pubsub.psubscribe(self.channel + '#*')

    def _listen(self):  # pragma: no cover
        prefix = (self.channel + '#').encode('utf-8')
        for message in self._redis_listen_with_retries():
            if message['type'] in ('message', 'pmessage') and \
                    message['channel'].startswith(prefix) and \
                    'data' in message:
                yield message['data']
        self.pubsub.punsubscribe(self.channel + '#*')

    def _thread(self):
        while True:
            try:
                for message in self._listen():
                    try:
                        data = msgpack.unpackb(message, raw=False)
                    except Exception:
                        # not a MessagePack message, so it cannot come
                        # from the Node.js adapter
                        continue
                    if not isinstance(data, list) or len(data) != 3:
                        continue
                    uid, packet, opts = data
                    if uid == self.host_id:
                        # this is our own message coming back to us
                        continue
                    if not isinstance(packet, dict) or \
                            packet.get('type') != EVENT:
                        continue
                    self._get_logger().debug('pubsub message: emit')
                    try:
                        self._handle_node_emit(packet, opts)
                    except Exception:
                        self.server.logger.exception(
                            'Handler error in pubsub listening thread')
                self.server.logger.error('pubsub listen() exited '
                                         'unexpectedly')
                break  # loop should never exit except in unit tests!
            except Exception:  # pragma: no cover
                self.server.logger.exception('Unexpected Error in pubsub '
                                             'listening thread')

    def _handle_node_emit(self, packet, opts):
        namespace = packet.get('nsp') or '/'
        data = packet.get('data')
        if not isinstance(data, list) or len(data) == 0:
            return
        event = data[0]
        args = data[1:]
        if not isinstance(opts, dict):
            opts = {}
        rooms = list(opts.get('rooms') or [])
        except_sids = list(opts.get('except') or [])
        local_data = args[0] if len(args) == 1 else tuple(args)
        # note: Manager.emit() delivers the event only once to clients
        # that belong to more than one of the target rooms
        super(PubSubManager, self).emit(
            event, local_data, namespace=namespace,
            room=rooms or None, skip_sid=except_sids or None)
