"""Regression tests for running two Admin UI instrumented Socket.IO servers
in the same process.

Each instrumented server must only report the traffic of its own Socket.IO
server, and uninstrumenting one server must not remove the monitoring
capabilities of the other.

The tests exercise the servers in-process through the WSGI interface, so no
network access is required. An additional end-to-end test over real HTTP and
WebSocket connections is included and is automatically skipped when local
port binding is not permitted.

This module can also be run directly:

    python tests/common/test_admin_isolation.py
"""
import functools
import io
import json
import socket
import threading
import time
import urllib.request
from socketserver import ThreadingMixIn
from wsgiref.simple_server import make_server, WSGIServer, WSGIRequestHandler
from wsgiref.util import setup_testing_defaults

import socketio
from engineio.socket import Socket

try:
    import pytest
except ImportError:  # pragma: no cover
    pytest = None

# pristine Engine.IO socket methods, captured before any instrumentation
PRISTINE_SOCKET_METHODS = {
    name: Socket.__dict__[name]
    for name in ['handle_post_request', '_websocket_handler', '_send_ping']
}


def describe_socket_patches():
    """Human-readable description of the current Engine.IO socket patches,
    used to log the call chain before/after wrapping."""
    lines = []
    for name in ['handle_post_request', '_websocket_handler', '_send_ping']:
        current = Socket.__dict__.get(name)
        if current is PRISTINE_SOCKET_METHODS[name]:
            desc = 'original'
        elif isinstance(current, functools.partialmethod):
            owner = getattr(current.args[0], 'server_id', '?') \
                if current.args else '?'
            desc = f'partialmethod bound to InstrumentedServer({owner})'
        else:
            desc = getattr(current, '__qualname__', repr(current))
        saved = []
        for attr in ('_InstrumentedServer__' + name.lstrip('_'),
                     '_socketio_' + name.lstrip('_')):
            if attr in Socket.__dict__:
                saved_attr = Socket.__dict__[attr]
                if isinstance(saved_attr, functools.partialmethod):
                    owner = getattr(saved_attr.args[0], 'server_id', '?') \
                        if saved_attr.args else '?'
                    saved.append(f'{attr}=partialmethod({owner})')
                elif saved_attr is PRISTINE_SOCKET_METHODS[name]:
                    saved.append(f'{attr}=original')
                else:
                    saved.append(
                        f'{attr}='
                        f'{getattr(saved_attr, "__qualname__", saved_attr)}')
        lines.append(f'Socket.{name} -> {desc}'
                     + (f'  [saved: {", ".join(saved)}]' if saved else ''))
    return lines


def assert_socket_methods_restored():
    for name, original in PRISTINE_SOCKET_METHODS.items():
        assert Socket.__dict__[name] is original, \
            f'Socket.{name} was not restored after uninstrument()'


def wait_for(predicate, timeout=5, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def parse_packets(payload):
    """Parse an Engine.IO payload into (namespace, socket.io packet) tuples
    for Socket.IO packets, or (None, packet) for raw Engine.IO packets."""
    for pkt in payload.split('\x1e'):
        if not pkt:
            continue
        if pkt[0] != '4':
            yield None, pkt
            continue
        nsp = '/'
        rest = pkt[2:]
        if rest.startswith('/'):
            nsp, rest = rest.split(',', 1)
        yield nsp, (pkt[1], json.loads(rest) if rest else None)


def connect_packet(namespace='/', auth=None):
    pkt = '40'
    if namespace != '/':
        pkt = f'40{namespace},'
        if auth is not None:
            pkt += json.dumps(auth)
    elif auth is not None:
        pkt = '40' + json.dumps(auth)
    return pkt


def event_packet(event, data, namespace='/'):
    pkt = '42'
    if namespace != '/':
        pkt += f'{namespace},'
    return pkt + json.dumps([event, data])


def disconnect_packet(namespace='/'):
    return '41' if namespace == '/' else f'41{namespace},'


class InProcessPollingClient:
    """Minimal Engine.IO long-polling client that talks directly to a WSGI
    app, so no network access is needed."""

    def __init__(self, app):
        self.app = app
        payload, _ = self._request(
            'GET', '/socket.io/?EIO=4&transport=polling')
        pkt = payload.split('\x1e')[0]
        assert pkt[0] == '0'
        self.eio_sid = json.loads(pkt[1:])['sid']

    def _request(self, method, path, body=None):
        environ = {}
        setup_testing_defaults(environ)
        if '?' in path:
            environ['PATH_INFO'], environ['QUERY_STRING'] = path.split('?', 1)
        else:
            environ['PATH_INFO'] = path
            environ['QUERY_STRING'] = ''
        environ['REQUEST_METHOD'] = method
        body_bytes = body.encode() if body else b''
        environ['wsgi.input'] = io.BytesIO(body_bytes)
        environ['CONTENT_LENGTH'] = str(len(body_bytes))
        result = {}

        def start_response(status, headers, exc_info=None):
            result['status'] = status

        response = b''.join(self.app(environ, start_response))
        return response.decode(), result['status']

    def post(self, data):
        return self._request(
            'POST',
            f'/socket.io/?EIO=4&transport=polling&sid={self.eio_sid}', data)

    def poll(self):
        return self._request(
            'GET',
            f'/socket.io/?EIO=4&transport=polling&sid={self.eio_sid}')[0]

    def connect_namespace(self, namespace='/', auth=None):
        self.post(connect_packet(namespace, auth))
        deadline = time.time() + 5
        while time.time() < deadline:
            for nsp, packet in parse_packets(self.poll()):
                if packet and nsp == namespace and packet[0] == '0':
                    return packet[1]['sid'] if packet[1] else None
        raise RuntimeError(f'connect to namespace {namespace} failed')

    def emit(self, event, data, namespace='/'):
        self.post(event_packet(event, data, namespace))

    def disconnect_namespace(self, namespace='/'):
        self.post(disconnect_packet(namespace))


class AdminClient:
    """Admin UI client that collects the events sent to the /admin
    namespace over long-polling."""

    def __init__(self, make_client, token):
        self.transport = make_client()
        self.transport.connect_namespace(
            '/admin', auth={'token': token})
        self.events = []
        self.lock = threading.Lock()
        self.running = True
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def _poll_loop(self):
        while self.running:
            try:
                payload = self.transport.poll()
            except Exception:
                if self.running:
                    time.sleep(0.1)
                continue
            for nsp, packet in parse_packets(payload):
                if packet and nsp == '/admin' and packet[0] == '2':
                    with self.lock:
                        self.events.append((packet[1][0], packet[1][1:]))

    def get_events(self, name=None):
        with self.lock:
            return [(event, args) for event, args in self.events
                    if name is None or event == name]

    def close(self):
        self.running = False


def make_instrumented_server(tag, stats_interval=0.25):
    """Create an instrumented Socket.IO server with a distinct identity.

    Returns (sio, instrumented_server, wsgi_app, received_events).
    """
    received = []
    sio = socketio.Server(async_mode='threading')

    @sio.event
    def hello(sid, data):
        received.append((sid, data))

    isvr = sio.instrument(auth={'token': f'secret-{tag}'},
                          server_id=f'server-{tag}',
                          server_stats_interval=stats_interval)

    def http_app(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b'OK']

    app = socketio.WSGIApp(sio, http_app)
    return sio, isvr, app, received


def stop_instrumented_server(sio, isvr):
    sio.shutdown()
    isvr.shutdown()
    isvr.uninstrument()


def check_two_servers_isolation(make_client_a, make_client_b):
    """Shared scenario: polling and disconnect traffic of two instrumented
    servers must be reported only to the admin endpoint that owns the
    connection. ``make_client_*`` are zero-argument factories returning a
    polling client for each server."""
    admin_a = AdminClient(make_client_a, 'secret-A')
    admin_b = AdminClient(make_client_b, 'secret-B')
    try:
        # wait for the admin namespace connections to be established
        assert wait_for(lambda: admin_a.get_events('config'), timeout=5)
        assert wait_for(lambda: admin_b.get_events('config'), timeout=5)

        # a long-polling client connects to server A and emits an event
        client_a = make_client_a()
        sid_a = client_a.connect_namespace()
        client_a.emit('hello', {'from': 'polling'})

        # the business event must be delivered exactly once
        return admin_a, admin_b, client_a, sid_a
    except Exception:
        admin_a.close()
        admin_b.close()
        raise


def test_two_servers_do_not_share_admin_events():
    """Two instrumented servers in one process: polling and disconnect
    traffic must be reported only to the admin endpoint of the server that
    owns the connection."""
    print('\n--- call chain before instrumentation ---')
    for line in describe_socket_patches():
        print(' ', line)

    sio_a, isvr_a, app_a, received_a = make_instrumented_server('A')
    sio_b, isvr_b, app_b, received_b = make_instrumented_server('B')
    print('--- call chain after instrumenting server-A and server-B ---')
    for line in describe_socket_patches():
        print(' ', line)

    try:
        admin_a, admin_b, client_a, sid_a = check_two_servers_isolation(
            lambda: InProcessPollingClient(app_a),
            lambda: InProcessPollingClient(app_b))
        try:
            assert wait_for(lambda: len(received_a) == 1, timeout=5)
            assert received_a[0][1] == {'from': 'polling'}
            assert received_b == []

            # admin A must see the polling client of server A
            assert wait_for(
                lambda: any(
                    args[0]['id'] == sid_a
                    for _, args in admin_a.get_events('socket_connected')
                    if args and isinstance(args[0], dict)), timeout=5)
            assert wait_for(
                lambda: len(admin_a.get_events('event_received')) == 1,
                timeout=5)

            # no cross-attribution: admin B must not see server A's socket
            assert admin_b.get_events('socket_connected') == []
            assert admin_b.get_events('event_received') == []

            # each admin reports its own server id in the stats
            assert wait_for(lambda: admin_a.get_events('server_stats'),
                            timeout=5)
            assert wait_for(lambda: admin_b.get_events('server_stats'),
                            timeout=5)
            assert all(args[0]['serverId'] == 'server-A'
                       for _, args in admin_a.get_events('server_stats'))
            assert all(args[0]['serverId'] == 'server-B'
                       for _, args in admin_b.get_events('server_stats'))

            # disconnect the polling client of server A
            client_a.disconnect_namespace()
            assert wait_for(
                lambda: len(admin_a.get_events('socket_disconnected')) == 1,
                timeout=5)
            assert admin_b.get_events('socket_disconnected') == []

            print('--- events received by admin A ---')
            for event, _ in admin_a.get_events():
                print(' ', event)
            print('--- events received by admin B ---')
            for event, _ in admin_b.get_events():
                print(' ', event)
        finally:
            admin_a.close()
            admin_b.close()
    finally:
        stop_instrumented_server(sio_a, isvr_a)
        stop_instrumented_server(sio_b, isvr_b)

    print('--- call chain after uninstrumenting both servers ---')
    for line in describe_socket_patches():
        print(' ', line)
    assert_socket_methods_restored()


def test_uninstrument_one_server_keeps_the_other_working():
    """Uninstrumenting one server (in either order) must not break the
    monitoring of the other server."""
    for first in ['A', 'B']:
        sio_a, isvr_a, app_a, _ = make_instrumented_server('A')
        sio_b, isvr_b, app_b, _ = make_instrumented_server('B')
        servers = {'A': (sio_a, isvr_a, app_a),
                   'B': (sio_b, isvr_b, app_b)}
        try:
            # uninstrument one of the servers
            gone = servers[first]
            gone[1].shutdown()
            gone[1].uninstrument()

            # the other server must still report events to its admin
            other_tag = 'B' if first == 'A' else 'A'
            other = servers[other_tag]
            admin = AdminClient(
                lambda: InProcessPollingClient(other[2]),
                f'secret-{other_tag}')
            try:
                assert wait_for(lambda: admin.get_events('config'), timeout=5)
                client = InProcessPollingClient(other[2])
                sid = client.connect_namespace()
                client.emit('hello', 'still monitored')
                assert wait_for(
                    lambda: any(
                        args[0]['id'] == sid
                        for _, args in admin.get_events('socket_connected')
                        if args and isinstance(args[0], dict)), timeout=5)
                assert wait_for(
                    lambda: len(admin.get_events('event_received')) == 1,
                    timeout=5)
                client.disconnect_namespace()
            finally:
                admin.close()
        finally:
            for sio, isvr, _ in servers.values():
                sio.shutdown()
                isvr.shutdown()
                isvr.uninstrument()

        assert_socket_methods_restored()


def test_repeated_instrumentation_same_server():
    """Instrumenting the same server twice (and re-instrumenting an
    instrumented server) must not stack wrappers; both instrumented
    servers must keep reporting."""
    sio = socketio.Server(async_mode='threading')
    isvr1 = sio.instrument(auth=False, server_id='first',
                           server_stats_interval=0.25)
    isvr2 = sio.instrument(auth=False, server_id='second',
                           server_stats_interval=0.25)
    isvr1.instrument()  # repeated instrument() must be a no-op

    def http_app(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return [b'OK']

    app = socketio.WSGIApp(sio, http_app)
    admin = AdminClient(lambda: InProcessPollingClient(app), token=None)
    try:
        client = InProcessPollingClient(app)
        client.connect_namespace()
        # this would recurse forever if the wrappers were stacked
        client.emit('hello', 'x')

        # both instrumented servers monitor the same Socket.IO server, so
        # both must report the polling packets in their stats
        def packet_types_seen():
            seen = {}
            for _, args in admin.get_events('server_stats'):
                types = seen.setdefault(args[0]['serverId'], set())
                types.update(e['type'] for e in args[0]['aggregatedEvents'])
            return seen

        assert wait_for(
            lambda: len(packet_types_seen()) == 2 and all(
                'packetsIn' in types
                for types in packet_types_seen().values()), timeout=5)
        client.disconnect_namespace()
    finally:
        admin.close()
        sio.shutdown()
        isvr1.shutdown()
        isvr2.shutdown()
        isvr1.uninstrument()
        isvr2.uninstrument()
        isvr1.uninstrument()  # repeated uninstrument() must be a no-op

    assert_socket_methods_restored()


def test_post_request_metrics_routed_to_owning_server():
    """Unit-level check: the shared Engine.IO socket patch must attribute
    metrics to the instrumented server that owns the Engine.IO socket."""
    sio1 = socketio.Server(async_mode='threading')
    sio2 = socketio.Server(async_mode='threading')
    isvr1 = sio1.instrument(auth=False, server_id='one')
    isvr2 = sio2.instrument(auth=False, server_id='two')
    try:
        class FakeSocket:
            pass

        fake = FakeSocket()
        fake.server = sio1.eio  # this socket belongs to server one
        fake._socketio_handle_post_request = \
            lambda environ: {'response': b''}
        Socket.handle_post_request(fake, {'CONTENT_LENGTH': '5'})

        buffer1 = [e['type'] for e in isvr1.event_buffer.get_and_clear()]
        buffer2 = [e['type'] for e in isvr2.event_buffer.get_and_clear()]
        assert 'packetsIn' in buffer1
        assert 'packetsIn' not in buffer2
    finally:
        isvr1.uninstrument()
        isvr2.uninstrument()


def test_websocket_handler_metrics_routed_to_owning_server():
    """Unit-level check: the shared WebSocket handler patch must attribute
    metrics to the instrumented server that owns the Engine.IO socket, and
    must wrap the websocket send/wait methods only once."""
    sio1 = socketio.Server(async_mode='threading')
    sio2 = socketio.Server(async_mode='threading')
    isvr1 = sio1.instrument(auth=False, server_id='one')
    isvr2 = sio2.instrument(auth=False, server_id='two')
    try:
        class FakeWebSocket:
            def __init__(self):
                self.sent = []

            def send(self, data, *args, **kwargs):
                self.sent.append(data)
                return 'sent'

            def wait(self):
                return 'incoming'

        class FakeSocket:
            pass

        fake = FakeSocket()
        fake.server = sio1.eio  # this socket belongs to server one

        def original_websocket_handler(ws):
            # emulate the Engine.IO websocket loop using the wrapped socket
            ws.send('out')
            ws.wait()
            return 'done'

        fake._socketio_websocket_handler = original_websocket_handler
        ws = FakeWebSocket()
        assert Socket._websocket_handler(fake, ws) == 'done'
        assert ws.sent == ['out']

        buffer1 = [e['type'] for e in isvr1.event_buffer.get_and_clear()]
        buffer2 = [e['type'] for e in isvr2.event_buffer.get_and_clear()]
        assert buffer1.count('packetsOut') == 1
        assert buffer1.count('packetsIn') == 1
        assert buffer2 == []
    finally:
        isvr1.uninstrument()
        isvr2.uninstrument()


# --- end-to-end test over real HTTP and WebSocket connections ---
# (skipped automatically when local port binding is not permitted)


def local_binding_allowed():
    try:
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        s.close()
        return True
    except OSError:
        return False


NETWORK_AVAILABLE = local_binding_allowed()


class WebServer:
    """Minimal threading WSGI web server with WebSocket support."""

    def __init__(self, sio):
        def http_app(environ, start_response):
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [b'OK']

        self.sio = sio
        self.app = socketio.WSGIApp(sio, http_app)
        self.httpd = None
        self.thread = None

    def start(self, port):
        class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
            daemon_threads = True

        class WebSocketRequestHandler(WSGIRequestHandler):
            def get_environ(self):
                env = super().get_environ()
                # pass the raw socket to the WSGI app so that it can be
                # used by WebSocket connections (hack copied from gunicorn)
                env['gunicorn.socket'] = self.connection
                return env

        self.httpd = make_server('localhost', port, self._app_wrapper,
                                 ThreadingWSGIServer, WebSocketRequestHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        assert wait_for(self._ready, timeout=5)

    def _ready(self):
        try:
            with urllib.request.urlopen(
                    f'http://localhost:{self.httpd.server_port}/') as r:
                return r.read() == b'OK'
        except Exception:
            return False

    def stop(self):
        self.sio.shutdown()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join()
        self.httpd = None
        self.thread = None

    def _app_wrapper(self, environ, start_response):
        try:
            return self.app(environ, start_response)
        except StopIteration:
            # end the WebSocket request without sending a response
            start_response('200 OK', [])
            return []


class HttpPollingClient:
    """Minimal Engine.IO long-polling client over real HTTP."""

    def __init__(self, port):
        self.base = f'http://localhost:{port}/socket.io/'
        pkt = self._get('?EIO=4&transport=polling').split('\x1e')[0]
        assert pkt[0] == '0'
        self.eio_sid = json.loads(pkt[1:])['sid']

    def _get(self, query):
        with urllib.request.urlopen(self.base + query, timeout=30) as r:
            return r.read().decode()

    def post(self, data):
        req = urllib.request.Request(
            f'{self.base}?EIO=4&transport=polling&sid={self.eio_sid}',
            data=data.encode(),
            headers={'Content-Type': 'text/plain;charset=UTF-8'},
            method='POST')
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read().decode()

    def poll(self):
        return self._get(
            f'?EIO=4&transport=polling&sid={self.eio_sid}')

    def connect_namespace(self, namespace='/', auth=None):
        self.post(connect_packet(namespace, auth))
        deadline = time.time() + 5
        while time.time() < deadline:
            for nsp, packet in parse_packets(self.poll()):
                if packet and nsp == namespace and packet[0] == '0':
                    return packet[1]['sid'] if packet[1] else None
        raise RuntimeError(f'connect to namespace {namespace} failed')

    def emit(self, event, data, namespace='/'):
        self.post(event_packet(event, data, namespace))

    def disconnect_namespace(self, namespace='/'):
        self.post(disconnect_packet(namespace))


class WSClient:
    """Minimal Socket.IO client over a direct WebSocket connection."""

    def __init__(self, port, namespace='/', auth=None):
        from simple_websocket import Client as SimpleWSClient
        self.ws = SimpleWSClient.connect(
            f'ws://localhost:{port}/socket.io/?EIO=4&transport=websocket')
        handshake = self.ws.receive(timeout=5)
        assert handshake[0] == '0'
        self.ws.send(connect_packet(namespace, auth))
        for nsp, packet in parse_packets(self.ws.receive(timeout=5)):
            if packet and nsp == namespace and packet[0] == '0':
                return
        raise RuntimeError(f'connect to namespace {namespace} failed')

    def emit(self, event, data, namespace='/'):
        self.ws.send(event_packet(event, data, namespace))

    def disconnect(self, namespace='/'):
        self.ws.send(disconnect_packet(namespace))
        self.ws.close()


def test_two_servers_over_http_and_websocket():
    """End-to-end version of the isolation test over real HTTP long-polling
    and WebSocket connections. Skipped when local port binding is not
    permitted."""
    if not NETWORK_AVAILABLE:
        if pytest:  # pragma: no cover
            pytest.skip('local port binding is not permitted')
        print('SKIP: local port binding is not permitted')
        return

    sio_a, isvr_a, _, received_a = make_instrumented_server('A')
    sio_b, isvr_b, _, received_b = make_instrumented_server('B')
    web_a = WebServer(sio_a)
    web_b = WebServer(sio_b)
    web_a.start(8911)
    web_b.start(8912)
    try:
        admin_a = AdminClient(lambda: HttpPollingClient(8911), 'secret-A')
        admin_b = AdminClient(lambda: HttpPollingClient(8912), 'secret-B')
        try:
            assert wait_for(lambda: admin_a.get_events('config'), timeout=5)
            assert wait_for(lambda: admin_b.get_events('config'), timeout=5)

            # polling client on server A
            client_a = HttpPollingClient(8911)
            sid_a = client_a.connect_namespace()
            client_a.emit('hello', {'from': 'polling'})

            # websocket client on server B, then disconnect
            client_b = WSClient(8912)
            client_b.emit('hello', {'from': 'websocket'})
            client_b.disconnect()

            # each business event must be delivered exactly once
            assert wait_for(lambda: len(received_a) == 1, timeout=5)
            assert wait_for(lambda: len(received_b) == 1, timeout=5)
            assert received_a[0][1] == {'from': 'polling'}
            assert received_b[0][1] == {'from': 'websocket'}

            # each admin only sees the socket of its own server
            assert wait_for(
                lambda: any(
                    args[0]['id'] == sid_a
                    for _, args in admin_a.get_events('socket_connected')
                    if args and isinstance(args[0], dict)), timeout=5)
            assert wait_for(
                lambda: len(admin_b.get_events('socket_connected')) == 1,
                timeout=5)
            assert len(admin_a.get_events('socket_connected')) == 1
            assert admin_b.get_events('socket_connected')[0][1][0][
                'transport'] == 'websocket'

            # the websocket disconnect is reported by admin B only
            assert wait_for(
                lambda: len(admin_b.get_events('socket_disconnected')) == 1,
                timeout=5)
            assert admin_a.get_events('socket_disconnected') == []

            client_a.disconnect_namespace()
            assert wait_for(
                lambda: len(admin_a.get_events('socket_disconnected')) == 1,
                timeout=5)
        finally:
            admin_a.close()
            admin_b.close()
    finally:
        web_a.stop()
        web_b.stop()
        isvr_a.shutdown()
        isvr_b.shutdown()
        isvr_a.uninstrument()
        isvr_b.uninstrument()

    assert_socket_methods_restored()


ALL_TESTS = [
    test_two_servers_do_not_share_admin_events,
    test_uninstrument_one_server_keeps_the_other_working,
    test_repeated_instrumentation_same_server,
    test_post_request_metrics_routed_to_owning_server,
    test_websocket_handler_metrics_routed_to_owning_server,
    test_two_servers_over_http_and_websocket,
]

if __name__ == '__main__':
    failures = 0
    for test in ALL_TESTS:
        print(f'\n===== {test.__name__} =====')
        try:
            test()
            print(f'===== {test.__name__}: PASS =====')
        except Exception as exc:
            failures += 1
            print(f'===== {test.__name__}: FAIL: {exc} =====')
    print(f'\n{len(ALL_TESTS) - failures}/{len(ALL_TESTS)} tests passed')
    raise SystemExit(1 if failures else 0)
