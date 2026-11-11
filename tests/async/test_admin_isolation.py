"""Unit-level regression tests for running two async Admin UI instrumented
Socket.IO servers in the same process.

These tests exercise the instrument/uninstrument mechanics and the metric
routing of the shared Engine.IO socket patches without requiring a running
web server. They can also be run directly:

    python tests/async/test_admin_isolation.py
"""
import asyncio
import time

import socketio
from engineio.async_socket import AsyncSocket
from socketio.async_admin import InstrumentedAsyncServer

# pristine Engine.IO socket methods, captured before any instrumentation
PRISTINE_SOCKET_METHODS = {
    name: AsyncSocket.__dict__[name]
    for name in ['handle_post_request', '_websocket_handler', '_send_ping']
}


def assert_socket_methods_restored():
    for name, original in PRISTINE_SOCKET_METHODS.items():
        assert AsyncSocket.__dict__[name] is original, \
            f'AsyncSocket.{name} was not restored after uninstrument()'


def make_fake_socket(eio):
    class FakeSocket:
        pass

    fake = FakeSocket()
    fake.server = eio

    async def original_handle_post_request(environ):
        return {'response': b''}

    fake._socketio_handle_post_request = original_handle_post_request
    return fake


def test_two_async_servers_share_a_single_class_patch():
    sio1 = socketio.AsyncServer()
    sio2 = socketio.AsyncServer()
    isvr1 = sio1.instrument(auth=False, server_id='one')
    isvr2 = sio2.instrument(auth=False, server_id='two')
    try:
        registry = InstrumentedAsyncServer._instrumented_eio_servers
        assert registry[sio1.eio] == [isvr1]
        assert registry[sio2.eio] == [isvr2]

        # the class is patched once, with the shared dispatcher
        assert AsyncSocket.__dict__['handle_post_request'] is \
            InstrumentedAsyncServer._eio_handle_post_request

        # metrics are routed to the instrumented server that owns the
        # Engine.IO socket
        fake = make_fake_socket(sio2.eio)
        asyncio.run(AsyncSocket.handle_post_request(
            fake, {'CONTENT_LENGTH': '3'}))
        buffer1 = [e['type'] for e in isvr1.event_buffer.get_and_clear()]
        buffer2 = [e['type'] for e in isvr2.event_buffer.get_and_clear()]
        assert 'packetsIn' not in buffer1
        assert 'packetsIn' in buffer2

        # uninstrumenting one server keeps the other one patched
        isvr1.uninstrument()
        assert sio1.eio not in registry
        assert registry[sio2.eio] == [isvr2]
        assert AsyncSocket.__dict__['handle_post_request'] is \
            InstrumentedAsyncServer._eio_handle_post_request

        # uninstrumenting the last server restores the original methods
        isvr2.uninstrument()
        assert not registry
        assert_socket_methods_restored()
    finally:
        isvr1.uninstrument()
        isvr2.uninstrument()


def test_repeated_instrumentation_same_async_server():
    sio = socketio.AsyncServer()
    isvr1 = sio.instrument(auth=False, server_id='first')
    isvr2 = sio.instrument(auth=False, server_id='second')
    isvr1.instrument()  # repeated instrument() must be a no-op
    try:
        registry = InstrumentedAsyncServer._instrumented_eio_servers
        assert registry[sio.eio] == [isvr1, isvr2]

        # both instrumented servers monitor the same Socket.IO server, so
        # both must receive the metrics, exactly once each
        fake = make_fake_socket(sio.eio)
        asyncio.run(AsyncSocket.handle_post_request(
            fake, {'CONTENT_LENGTH': '3'}))
        for isvr in [isvr1, isvr2]:
            buffer = [e['type'] for e in isvr.event_buffer.get_and_clear()]
            assert buffer.count('packetsIn') == 1
    finally:
        isvr1.uninstrument()
        isvr2.uninstrument()
        isvr1.uninstrument()  # repeated uninstrument() must be a no-op
    assert_socket_methods_restored()


def test_async_dispatch_paths_are_isolated():
    """End-to-end check (without network) that the shared dispatchers route
    engine.io connections, socket.io events, room and emit metrics only to
    the instrumented server that owns the connection."""
    sio1 = socketio.AsyncServer()
    sio2 = socketio.AsyncServer()
    isvr1 = sio1.instrument(auth=False, server_id='one',
                            server_stats_interval=0.25)
    isvr2 = sio2.instrument(auth=False, server_id='two',
                            server_stats_interval=0.25)

    results = {}

    async def run():
        # engine.io connect + disconnect on server one only
        await sio1.eio.handlers['connect']('eio-1', {})
        sid = await sio1.manager.connect('eio-1', '/')
        sio1.manager._timestamps[sid] = time.time()
        # socket.io event on server one only
        await sio1._trigger_event('my_event', '/', sid, 'data')
        # room join and emit on server one only
        sio1.manager.basic_enter_room(sid, '/', 'room1')
        await sio1.emit('evt', {'x': 1}, namespace='/')
        await sio1.eio.handlers['disconnect']('eio-1', 'client gone')
        results['sid'] = sid
        # collect the metrics before the stats tasks flush them
        results['buffer1'] = [e['type']
                              for e in isvr1.event_buffer.get_and_clear()]
        results['buffer2'] = [e['type']
                              for e in isvr2.event_buffer.get_and_clear()]
        results['queue1'] = isvr1.admin_queue.copy()
        results['queue2'] = isvr2.admin_queue.copy()
        await isvr1.shutdown()
        await isvr2.shutdown()

    try:
        asyncio.run(run())

        assert 'rawConnection' in results['buffer1']
        assert 'rawDisconnection' in results['buffer1']
        assert results['buffer2'] == []

        # the room joins and leaves (the sid room and room1) were queued
        # for the admin namespace of server one only
        assert [event for event, _ in results['queue1']] == [
            'room_joined', 'room_joined', 'room_left', 'room_left']
        assert [args[1] for _, args in results['queue1']] == [
            results['sid'], 'room1', results['sid'], 'room1']
        assert results['queue2'] == []
    finally:
        isvr1.uninstrument()
        isvr2.uninstrument()
    assert_socket_methods_restored()


if __name__ == '__main__':
    failures = 0
    for test in [test_two_async_servers_share_a_single_class_patch,
                 test_repeated_instrumentation_same_async_server,
                 test_async_dispatch_paths_are_isolated]:
        print(f'\n===== {test.__name__} =====')
        try:
            test()
            print(f'===== {test.__name__}: PASS =====')
        except Exception as exc:
            failures += 1
            print(f'===== {test.__name__}: FAIL: {exc} =====')
    print(f'\n{3 - failures}/3 tests passed')
    raise SystemExit(1 if failures else 0)
