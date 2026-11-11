"""Integration tests for the AsyncServer.call() acknowledgement lifecycle.

These tests use a real client manager and real asyncio events, exercising the
actual registry bookkeeping across completion, timeout, cancellation,
disconnect, reconnect and different namespaces.
"""
import asyncio
from unittest import mock

import pytest
import pytest_asyncio

from socketio import async_server
from socketio import exceptions
from socketio import packet as sio_packet


async def make_server():
    patched = mock.patch(
        'socketio.server.engineio.AsyncServer', **{
            'return_value.generate_id.side_effect':
                [str(i) for i in range(1, 1000)],
        })
    patched.start()
    s = async_server.AsyncServer(async_handlers=True)
    s.eio.create_event = asyncio.Event
    s.eio.send = mock.AsyncMock()
    s.eio.send_packet = mock.AsyncMock()
    s.handlers['/'] = {}
    s.handlers['/foo'] = {}
    await s._handle_eio_connect('eio', 'environ')
    await s._handle_eio_message('eio', '0')
    await s._handle_eio_message('eio', '0/foo,')
    return s, patched


def sid_for(s, namespace):
    return s.manager.sid_from_eio_sid('eio', namespace)


def event_packets(s):
    packets = []
    for call in s.eio.send.call_args_list:
        pkt = sio_packet.Packet(encoded_packet=call[0][1])
        if pkt.id is not None:
            packets.append(pkt)
    return packets


class TestAsyncCallLifecycle:
    @pytest_asyncio.fixture
    async def server(self):
        s, patched = await make_server()
        yield s
        patched.stop()

    async def test_normal_reply_returns_value_and_removes_entry(
            self, server):
        s = server

        async def reply():
            await asyncio.sleep(0.02)
            pkt = event_packets(s)[-1]
            await s._handle_eio_message(
                'eio', f'3{pkt.id}["ok",7]')

        task = asyncio.create_task(reply())
        value = await s.call('ping', sid=sid_for(s, '/'), timeout=2)
        await task
        assert value == ('ok', 7)
        assert s.manager._pending_ack_count() == 0

    async def test_single_and_empty_argument_unpacking(self, server):
        s = server
        for payload, expected in [('["one"]', 'one'), ('[]', None)]:
            async def reply(payload=payload):
                await asyncio.sleep(0.02)
                pkt = event_packets(s)[-1]
                await s._handle_eio_message(
                    'eio', f'3{pkt.id}{payload}')

            task = asyncio.create_task(reply())
            value = await s.call('p', sid=sid_for(s, '/'), timeout=2)
            await task
            assert value == expected
            assert s.manager._pending_ack_count() == 0

    async def test_repeated_timeouts_leave_no_callbacks(self, server):
        s = server
        iterations = 25
        for _ in range(iterations):
            with pytest.raises(exceptions.TimeoutError):
                await s.call('p', sid=sid_for(s, '/'), timeout=0.01)
        assert s.manager._pending_ack_count() == 0
        registry = s.manager.callbacks[sid_for(s, '/')]['/']
        assert set(registry) == {0}
        assert next(registry[0]) == iterations + 1

    async def test_ack_racing_timeout_returns_ack_value(self, server):
        s = server
        original_wait_for = asyncio.wait_for
        state = {'done': False}

        def close_awaitable(awaitable):
            task = asyncio.ensure_future(awaitable)
            task.cancel()

        async def racing_wait_for(awaitable, timeout):
            if not state['done']:
                state['done'] = True
                pkt = event_packets(s)[-1]
                await s._handle_eio_message(
                    'eio', f'3{pkt.id}["late"]')
                close_awaitable(awaitable)
                raise asyncio.TimeoutError()
            return await original_wait_for(awaitable, timeout)

        asyncio.wait_for = racing_wait_for
        try:
            value = await s.call('p', sid=sid_for(s, '/'), timeout=0.1)
        finally:
            asyncio.wait_for = original_wait_for
        assert value == 'late'
        assert s.manager._pending_ack_count() == 0

    async def test_duplicate_ack_invokes_callback_once(self, server):
        s = server

        async def reply():
            await asyncio.sleep(0.02)
            pkt = event_packets(s)[-1]
            await s._handle_eio_message(
                'eio', f'3{pkt.id}["first"]')

        task = asyncio.create_task(reply())
        value = await s.call('p', sid=sid_for(s, '/'), timeout=2)
        await task
        pkt = event_packets(s)[-1]
        await s._handle_eio_message(
            'eio', f'3{pkt.id}["second"]')
        assert value == 'first'
        assert s.manager._pending_ack_count() == 0

    async def test_disconnect_aborts_waiting_call_with_disconnected_error(
            self, server):
        s = server

        async def disconnecter():
            await asyncio.sleep(0.02)
            await s._handle_eio_message('eio', '1')

        asyncio.create_task(disconnecter())
        sid = sid_for(s, '/')
        with pytest.raises(exceptions.DisconnectedError):
            await s.call('p', sid=sid, timeout=30)
        assert s.manager._pending_ack_count() == 0
        assert sid not in s.manager.callbacks

    async def test_disconnect_on_one_namespace_keeps_other_requests(
            self, server):
        s = server
        root_sid = sid_for(s, '/')
        foo_sid = sid_for(s, '/foo')

        async def root_call():
            return await s.call('p', sid=root_sid, namespace='/',
                                timeout=2)

        task = asyncio.create_task(root_call())
        # wait until the event packet (with its ack id) has actually been sent
        while not event_packets(s):
            await asyncio.sleep(0)
        pkt = event_packets(s)[-1]
        assert s.manager._pending_ack_count(root_sid, '/') == 1
        await s._handle_eio_message('eio', '1/foo,')
        assert s.manager._pending_ack_count(root_sid, '/') == 1
        assert foo_sid not in s.manager.callbacks
        await s._handle_eio_message('eio', f'3{pkt.id}["root"]')
        assert await task == 'root'
        assert s.manager._pending_ack_count() == 0

    async def test_two_calls_on_different_namespaces_are_independent(
            self, server):
        s = server
        root_sid = sid_for(s, '/')
        foo_sid = sid_for(s, '/foo')

        root_task = asyncio.create_task(
            s.call('p', sid=root_sid, namespace='/', timeout=2))
        foo_task = asyncio.create_task(
            s.call('p', sid=foo_sid, namespace='/foo', timeout=2))
        while s.manager._pending_ack_count() != 2:
            await asyncio.sleep(0)
        root_ids = [i for i in s.manager.callbacks[root_sid]['/']
                    if i != 0]
        foo_ids = [i for i in s.manager.callbacks[foo_sid]['/foo']
                   if i != 0]
        assert root_ids == foo_ids == [1]
        await s.manager.trigger_callback(
            foo_sid, '/foo', foo_ids[0], ['foo-ok'])
        assert s.manager._pending_ack_count(foo_sid, '/foo') == 0
        assert s.manager._pending_ack_count(root_sid, '/') == 1
        await s.manager.trigger_callback(
            root_sid, '/', root_ids[0], ['root-ok'])
        assert await foo_task == 'foo-ok'
        assert await root_task == 'root-ok'
        assert s.manager._pending_ack_count() == 0

    async def test_reconnect_does_not_deliver_old_ack(self, server):
        s = server
        with pytest.raises(exceptions.TimeoutError):
            await s.call('p', sid=sid_for(s, '/'), timeout=0.01)
        await s._handle_eio_disconnect('eio', 'client')
        assert s.manager.callbacks == {}
        await s._handle_eio_connect('eio2', 'environ')
        await s._handle_eio_message('eio2', '0')
        before = s.manager._pending_ack_count()
        await s._handle_eio_message('eio2', '31["ghost"]')
        assert s.manager._pending_ack_count() == before == 0

    async def test_plain_emit_callback_is_not_given_a_timeout(
            self, server):
        s = server
        cb = mock.MagicMock()
        sid = sid_for(s, '/')
        await s.emit('p', data='x', to=sid, namespace='/', callback=cb)
        assert s.manager._pending_ack_count(sid, '/') == 1
        pkt = event_packets(s)[-1]
        await s._handle_eio_message('eio', f'3{pkt.id}["done"]')
        cb.assert_called_once_with('done')
        assert s.manager._pending_ack_count() == 0
