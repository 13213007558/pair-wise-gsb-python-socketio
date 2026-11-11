import asyncio
from datetime import datetime, timezone
import functools
import inspect
import os
import socket
import time
from urllib.parse import parse_qs
from .admin import EventBuffer
from .exceptions import ConnectionRefusedError

HOSTNAME = socket.gethostname()
PID = os.getpid()


class InstrumentedAsyncServer:
    # Registry of the Engine.IO servers that are currently instrumented in
    # this process, as a dict of Engine.IO server instance to a list of
    # InstrumentedAsyncServer instances. The Engine.IO socket class is
    # patched only once (when the first server is instrumented) and restored
    # when the last one is uninstrumented. The shared patches use this
    # registry to route metrics to the instrumented server(s) that own each
    # socket, so that multiple instrumented servers can coexist in the
    # process without interfering with each other.
    _instrumented_eio_servers = {}

    def __init__(self, sio, auth=None, namespace='/admin', read_only=False,
                 server_id=None, mode='development', server_stats_interval=2):
        """Instrument the Socket.IO server for monitoring with the `Socket.IO
        Admin UI <https://socket.io/docs/v4/admin-ui/>`_.
        """
        if auth is None:
            raise ValueError('auth must be specified')
        self.sio = sio
        self.auth = auth
        self.admin_namespace = namespace
        self.read_only = read_only
        self.server_id = server_id or (
            self.sio.manager.host_id if hasattr(self.sio.manager, 'host_id')
            else HOSTNAME
        )
        self.mode = mode
        self.server_stats_interval = server_stats_interval
        self.admin_queue = []
        self.event_buffer = EventBuffer()

        # task that emits "server_stats" every 2 seconds
        self.stop_stats_event = None
        self.stats_task = None

        # whether the Engine.IO socket class patches are registered
        self._instrumented = False

        # monkey-patch the server to report metrics to the admin UI
        self.instrument()

    def instrument(self):
        if self._instrumented:
            # instrumenting an already instrumented server is a no-op
            return
        self._instrumented = True

        self.sio.on('connect', self.admin_connect,
                    namespace=self.admin_namespace)

        if self.mode == 'development':
            if not self.read_only:  # pragma: no branch
                self.sio.on('emit', self.admin_emit,
                            namespace=self.admin_namespace)
                self.sio.on('join', self.admin_enter_room,
                            namespace=self.admin_namespace)
                self.sio.on('leave', self.admin_leave_room,
                            namespace=self.admin_namespace)
                self.sio.on('_disconnect', self.admin_disconnect,
                            namespace=self.admin_namespace)

            # track socket connection times
            self.sio.manager._timestamps = {}

            # The following monkey-patches are installed only once per
            # Socket.IO server, no matter how many instrumented servers
            # monitor it. The shared patches dispatch to the instrumented
            # server(s) registered for the server, so that repeated
            # instrumentation does not stack wrappers.
            if not hasattr(self.sio, '_socketio_trigger_event'):
                # report socket.io connections, disconnections and events
                self.sio._socketio_trigger_event = self.sio._trigger_event
                self.sio._trigger_event = functools.partial(
                    InstrumentedAsyncServer._trigger_event, self.sio)

                # report join rooms
                self.sio.manager._socketio_basic_enter_room = \
                    self.sio.manager.basic_enter_room
                self.sio.manager.basic_enter_room = functools.partial(
                    InstrumentedAsyncServer._basic_enter_room, self.sio)

                # report leave rooms
                self.sio.manager._socketio_basic_leave_room = \
                    self.sio.manager.basic_leave_room
                self.sio.manager.basic_leave_room = functools.partial(
                    InstrumentedAsyncServer._basic_leave_room, self.sio)

                # report emit events
                self.sio.manager._socketio_emit = self.sio.manager.emit
                self.sio.manager.emit = functools.partial(
                    InstrumentedAsyncServer._emit, self.sio)

        if self.sio.eio not in self._instrumented_eio_servers:
            # report engine.io connections
            self.sio.eio.on('connect', functools.partial(
                InstrumentedAsyncServer._handle_eio_connect, self.sio))
            self.sio.eio.on('disconnect', functools.partial(
                InstrumentedAsyncServer._handle_eio_disconnect, self.sio))

            # report polling packets
            self.sio.eio._socketio_ok = self.sio.eio._ok
            self.sio.eio._ok = functools.partial(
                InstrumentedAsyncServer._eio_http_response, self.sio)

        # The Engine.IO socket class is patched only once, no matter how
        # many servers are instrumented in this process. The shared patches
        # dispatch to the instrumented server(s) registered for the
        # Engine.IO server that owns each socket.
        from engineio.async_socket import AsyncSocket
        if not self._instrumented_eio_servers:
            AsyncSocket._socketio_handle_post_request = \
                AsyncSocket.handle_post_request
            AsyncSocket.handle_post_request = \
                InstrumentedAsyncServer._eio_handle_post_request
            AsyncSocket._socketio_websocket_handler = \
                AsyncSocket._websocket_handler
            AsyncSocket._websocket_handler = \
                InstrumentedAsyncServer._eio_websocket_handler
            AsyncSocket._socketio_send_ping = AsyncSocket._send_ping
            AsyncSocket._send_ping = InstrumentedAsyncServer._eio_send_ping
        if self.sio.eio not in self._instrumented_eio_servers:
            self._instrumented_eio_servers[self.sio.eio] = []
        self._instrumented_eio_servers[self.sio.eio].append(self)

    def uninstrument(self):  # pragma: no cover
        if not self._instrumented:
            # uninstrumenting a server that is not instrumented is a no-op
            return
        self._instrumented = False

        # remove this server from the registry
        eio = self.sio.eio
        if eio in self._instrumented_eio_servers and \
                self in self._instrumented_eio_servers[eio]:
            self._instrumented_eio_servers[eio].remove(self)
            if not self._instrumented_eio_servers[eio]:
                del self._instrumented_eio_servers[eio]

        if eio not in self._instrumented_eio_servers:
            # this was the last instrumented server monitoring this
            # Socket.IO server, so the monkey-patches are no longer needed
            if hasattr(self.sio, '_socketio_trigger_event'):
                self.sio._trigger_event = self.sio._socketio_trigger_event
                del self.sio._socketio_trigger_event
                self.sio.manager.basic_enter_room = \
                    self.sio.manager._socketio_basic_enter_room
                del self.sio.manager._socketio_basic_enter_room
                self.sio.manager.basic_leave_room = \
                    self.sio.manager._socketio_basic_leave_room
                del self.sio.manager._socketio_basic_leave_room
                self.sio.manager.emit = self.sio.manager._socketio_emit
                del self.sio.manager._socketio_emit
            if hasattr(eio, '_socketio_ok'):
                eio._ok = eio._socketio_ok
                del eio._socketio_ok
            eio.on('connect', self.sio._handle_eio_connect)
            eio.on('disconnect', self.sio._handle_eio_disconnect)

        if not self._instrumented_eio_servers:
            # this was the last instrumented server in the process, so the
            # original Engine.IO socket methods can be restored
            from engineio.async_socket import AsyncSocket
            AsyncSocket.handle_post_request = \
                AsyncSocket._socketio_handle_post_request
            AsyncSocket._websocket_handler = \
                AsyncSocket._socketio_websocket_handler
            AsyncSocket._send_ping = AsyncSocket._socketio_send_ping
            del AsyncSocket._socketio_handle_post_request
            del AsyncSocket._socketio_websocket_handler
            del AsyncSocket._socketio_send_ping

    async def admin_connect(self, sid, environ, client_auth):
        authenticated = True
        if self.auth:
            authenticated = False
            if isinstance(self.auth, dict):
                authenticated = client_auth == self.auth
            elif isinstance(self.auth, list):
                authenticated = client_auth in self.auth
            else:
                if inspect.iscoroutinefunction(self.auth):
                    authenticated = await self.auth(client_auth)
                else:
                    authenticated = self.auth(client_auth)
            if not authenticated:
                raise ConnectionRefusedError('authentication failed')

        async def config(sid):
            await self.sio.sleep(0.1)

            # supported features
            features = ['AGGREGATED_EVENTS']
            if not self.read_only:
                features += ['EMIT', 'JOIN', 'LEAVE', 'DISCONNECT', 'MJOIN',
                             'MLEAVE', 'MDISCONNECT']
            if self.mode == 'development':
                features.append('ALL_EVENTS')
            await self.sio.emit('config', {'supportedFeatures': features},
                                to=sid, namespace=self.admin_namespace)

            # send current sockets
            if self.mode == 'development':
                all_sockets = []
                for nsp in self.sio.manager.get_namespaces():
                    for sid, eio_sid in self.sio.manager.get_participants(
                            nsp, None):
                        all_sockets.append(
                            self.serialize_socket(sid, nsp, eio_sid))
                await self.sio.emit('all_sockets', all_sockets, to=sid,
                                    namespace=self.admin_namespace)

        self.sio.start_background_task(config, sid)

    async def admin_emit(self, _, namespace, room_filter, event, *data):
        await self.sio.emit(event, data, to=room_filter, namespace=namespace)

    async def admin_enter_room(self, _, namespace, room, room_filter=None):
        for sid, _ in self.sio.manager.get_participants(
                namespace, room_filter):
            await self.sio.enter_room(sid, room, namespace=namespace)

    async def admin_leave_room(self, _, namespace, room, room_filter=None):
        for sid, _ in self.sio.manager.get_participants(
                namespace, room_filter):
            await self.sio.leave_room(sid, room, namespace=namespace)

    async def admin_disconnect(self, _, namespace, close, room_filter=None):
        for sid, _ in self.sio.manager.get_participants(
                namespace, room_filter):
            await self.sio.disconnect(sid, namespace=namespace)

    async def shutdown(self):
        if self.stats_task:  # pragma: no branch
            self.stop_stats_event.set()
            await asyncio.gather(self.stats_task)
            self.stats_task = None
            self.stop_stats_event.clear()

    @staticmethod
    async def _trigger_event(sio, event, namespace, *args):
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                sio.eio, []):
            if isvr.mode == 'development':
                await isvr._report_event(event, namespace, *args)
        return await sio._socketio_trigger_event(event, namespace, *args)

    async def _report_event(self, event, namespace, *args):
        t = time.time()
        sid = args[0]
        if event == 'connect':
            eio_sid = self.sio.manager.eio_sid_from_sid(sid, namespace)
            self.sio.manager._timestamps[sid] = t
            serialized_socket = self.serialize_socket(sid, namespace, eio_sid)
            await self.sio.emit('socket_connected', (
                serialized_socket,
                datetime.fromtimestamp(t, timezone.utc).isoformat(),
            ), namespace=self.admin_namespace)
            if not self.sio.eio._get_socket(eio_sid).upgraded:
                self.sio.start_background_task(
                    self._check_for_upgrade, eio_sid, sid, namespace)
        elif event == 'disconnect':
            del self.sio.manager._timestamps[sid]
            reason = args[1]
            await self.sio.emit('socket_disconnected', (
                namespace,
                sid,
                reason,
                datetime.fromtimestamp(t, timezone.utc).isoformat(),
            ), namespace=self.admin_namespace)
        else:
            await self.sio.emit('event_received', (
                namespace,
                sid,
                (event, *args[1:]),
                datetime.fromtimestamp(t, timezone.utc).isoformat(),
            ), namespace=self.admin_namespace)

    async def _check_for_upgrade(self, eio_sid, sid,
                                 namespace):  # pragma: no cover
        for _ in range(5):
            await self.sio.sleep(5)
            try:
                if self.sio.eio._get_socket(eio_sid).upgraded:
                    await self.sio.emit('socket_updated', {
                        'id': sid,
                        'nsp': namespace,
                        'transport': 'websocket',
                    }, namespace=self.admin_namespace)
                    break
            except KeyError:
                pass

    @staticmethod
    def _basic_enter_room(sio, sid, namespace, room, eio_sid=None):
        ret = sio.manager._socketio_basic_enter_room(sid, namespace, room,
                                                     eio_sid)
        if room:
            for isvr in InstrumentedAsyncServer._instrumented_eio_servers \
                    .get(sio.eio, []):
                if isvr.mode == 'development':
                    isvr.admin_queue.append(('room_joined', (
                        namespace,
                        room,
                        sid,
                        datetime.now(timezone.utc).isoformat(),
                    )))
        return ret

    @staticmethod
    def _basic_leave_room(sio, sid, namespace, room):
        if room:
            for isvr in InstrumentedAsyncServer._instrumented_eio_servers \
                    .get(sio.eio, []):
                if isvr.mode == 'development':
                    isvr.admin_queue.append(('room_left', (
                        namespace,
                        room,
                        sid,
                        datetime.now(timezone.utc).isoformat(),
                    )))
        return sio.manager._socketio_basic_leave_room(sid, namespace, room)

    @staticmethod
    async def _emit(sio, event, data, namespace, room=None, skip_sid=None,
                    callback=None, **kwargs):
        ret = await sio.manager._socketio_emit(
            event, data, namespace, room=room, skip_sid=skip_sid,
            callback=callback, **kwargs)
        if not isinstance(skip_sid, list):  # pragma: no branch
            skip_sid = [skip_sid]
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                sio.eio, []):
            if isvr.mode != 'development' or \
                    namespace == isvr.admin_namespace:
                continue
            event_data = [event] + list(data) if isinstance(data, tuple) \
                else [event, data]
            for sid, _ in isvr.sio.manager.get_participants(namespace, room):
                if sid not in skip_sid:
                    await isvr.sio.emit('event_sent', (
                        namespace,
                        sid,
                        event_data,
                        datetime.now(timezone.utc).isoformat(),
                    ), namespace=isvr.admin_namespace)
        return ret

    @staticmethod
    async def _handle_eio_connect(sio, eio_sid, environ):
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                sio.eio, []):
            isvr._report_eio_connect(eio_sid, environ)
        return await sio._handle_eio_connect(eio_sid, environ)

    def _report_eio_connect(self, eio_sid, environ):
        if self.stop_stats_event is None:
            self.stop_stats_event = self.sio.eio.create_event()
        if self.stats_task is None:
            self.stats_task = self.sio.start_background_task(
                self._emit_server_stats)

        self.event_buffer.push('rawConnection')

    @staticmethod
    async def _handle_eio_disconnect(sio, eio_sid, reason):
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                sio.eio, []):
            isvr.event_buffer.push('rawDisconnection')
        return await sio._handle_eio_disconnect(eio_sid, reason)

    @staticmethod
    def _eio_http_response(sio, packets=None, headers=None, jsonp_index=None):
        ret = sio.eio._socketio_ok(packets=packets, headers=headers,
                                   jsonp_index=jsonp_index)
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                sio.eio, []):
            isvr.event_buffer.push('packetsOut')
            isvr.event_buffer.push('bytesOut', len(ret['response']))
        return ret

    @staticmethod
    async def _eio_handle_post_request(socket, environ):
        ret = await socket._socketio_handle_post_request(environ)
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                socket.server, []):
            isvr.event_buffer.push('packetsIn')
            isvr.event_buffer.push(
                'bytesIn', int(environ.get('CONTENT_LENGTH', 0)))
        return ret

    @staticmethod
    async def _eio_websocket_handler(socket, ws):
        isvrs = InstrumentedAsyncServer._instrumented_eio_servers.get(
            socket.server, [])

        async def _send(ws, data):
            for isvr in isvrs:
                isvr.event_buffer.push('packetsOut')
                isvr.event_buffer.push('bytesOut', len(data))
            return await ws._socketio_send(data)

        async def _wait(ws):
            ret = await ws._socketio_wait()
            for isvr in isvrs:
                isvr.event_buffer.push('packetsIn')
                isvr.event_buffer.push('bytesIn', len(ret or ''))
            return ret

        ws._socketio_send = ws.send
        ws.send = functools.partial(_send, ws)
        ws._socketio_wait = ws.wait
        ws.wait = functools.partial(_wait, ws)
        return await socket._socketio_websocket_handler(ws)

    @staticmethod
    async def _eio_send_ping(socket):  # pragma: no cover
        eio_sid = socket.sid
        t = time.time()
        for isvr in InstrumentedAsyncServer._instrumented_eio_servers.get(
                socket.server, []):
            if isvr.mode != 'development':
                continue
            for namespace in isvr.sio.manager.get_namespaces():
                sid = isvr.sio.manager.sid_from_eio_sid(eio_sid, namespace)
                if sid:
                    serialized_socket = isvr.serialize_socket(
                        sid, namespace, eio_sid)
                    await isvr.sio.emit('socket_connected', (
                        serialized_socket,
                        datetime.fromtimestamp(t, timezone.utc).isoformat(),
                    ), namespace=isvr.admin_namespace)
        return await socket._socketio_send_ping()

    async def _emit_server_stats(self):
        start_time = time.time()
        namespaces = list(self.sio.handlers.keys())
        namespaces.sort()
        while not self.stop_stats_event.is_set():
            await self.sio.sleep(self.server_stats_interval)
            await self.sio.emit('server_stats', {
                'serverId': self.server_id,
                'hostname': HOSTNAME,
                'pid': PID,
                'uptime': time.time() - start_time,
                'clientsCount': len(self.sio.eio.sockets),
                'pollingClientsCount': len(
                    [s for s in self.sio.eio.sockets.values()
                     if not s.upgraded]),
                'aggregatedEvents': self.event_buffer.get_and_clear(),
                'namespaces': [{
                    'name': nsp,
                    'socketsCount': len(self.sio.manager.rooms.get(
                        nsp, {None: []}).get(None, []))
                } for nsp in namespaces],
            }, namespace=self.admin_namespace)
            while self.admin_queue:
                event, args = self.admin_queue.pop(0)
                await self.sio.emit(event, args,
                                    namespace=self.admin_namespace)

    def serialize_socket(self, sid, namespace, eio_sid=None):
        if eio_sid is None:  # pragma: no cover
            eio_sid = self.sio.manager.eio_sid_from_sid(sid)
        socket = self.sio.eio._get_socket(eio_sid)
        environ = self.sio.environ.get(eio_sid, {})
        tm = self.sio.manager._timestamps[sid] if sid in \
            self.sio.manager._timestamps else 0
        return {
            'id': sid,
            'clientId': eio_sid,
            'transport': 'websocket' if socket.upgraded else 'polling',
            'nsp': namespace,
            'data': {},
            'handshake': {
                'address': environ.get('REMOTE_ADDR', ''),
                'headers': {k[5:].lower(): v for k, v in environ.items()
                            if k.startswith('HTTP_')},
                'query': {k: v[0] if len(v) == 1 else v for k, v in parse_qs(
                    environ.get('QUERY_STRING', '')).items()},
                'secure': environ.get('wsgi.url_scheme', '') == 'https',
                'url': environ.get('PATH_INFO', ''),
                'issued': tm * 1000,
                'time': datetime.fromtimestamp(tm, timezone.utc).isoformat()
                if tm else '',
            },
            'rooms': self.sio.manager.get_rooms(sid, namespace),
        }
