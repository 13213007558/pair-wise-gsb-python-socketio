import asyncio
import functools
import json
from unittest import mock

import pytest

from engineio.packet import Packet as EIOPacket
from socketio import async_manager
from socketio import async_pubsub_manager
from socketio import async_server
from socketio import packet


class TestAsyncPubSubManager:
    def setup_method(self):
        id = 0

        def generate_id():
            nonlocal id
            id += 1
            return str(id)

        mock_server = mock.MagicMock()
        mock_server.eio.generate_id = generate_id
        mock_server.packet_class = packet.Packet
        mock_server._send_packet = mock.AsyncMock()
        mock_server._send_eio_packet = mock.AsyncMock()
        mock_server.disconnect = mock.AsyncMock()
        self.pm = async_pubsub_manager.AsyncPubSubManager()
        self.pm._publish = mock.AsyncMock()
        self.pm.set_server(mock_server)
        self.pm.host_id = '123456'
        self.pm.initialize()

    async def test_default_init(self):
        assert self.pm.channel == 'socketio'
        self.pm.server.start_background_task.assert_called_once_with(
            self.pm._thread
        )

    async def test_custom_init(self):
        pubsub = async_pubsub_manager.AsyncPubSubManager(channel='foo')
        assert pubsub.channel == 'foo'
        assert len(pubsub.host_id) == 32

    async def test_write_only_init(self):
        mock_server = mock.MagicMock()
        pm = async_pubsub_manager.AsyncPubSubManager(write_only=True)
        pm.set_server(mock_server)
        pm.initialize()
        assert pm.channel == 'socketio'
        assert len(pm.host_id) == 32
        assert pm.server.start_background_task.call_count == 0

    async def test_emit(self):
        await self.pm.emit('foo', 'bar')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_binary(self):
        await self.pm.emit('foo', b'bar')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'_placeholder': True, 'num': 0}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', {'foo': b'bar'})
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'foo': {'_placeholder': True, 'num': 0}}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_bytearray(self):
        await self.pm.emit('foo', bytearray(b'bar'))
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'_placeholder': True, 'num': 0}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', {'foo': bytearray(b'bar')})
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'foo': {'_placeholder': True, 'num': 0}}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_list(self):
        await self.pm.emit('foo', [1, 'two'])
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': [[1, 'two']],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', [1, b'two', 'three'])
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [
                    [[1, {'_placeholder': True, 'num': 0}, 'three']], 'dHdv',
                ],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_no_arguments(self):
        await self.pm.emit('foo', ())
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': [],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_multiple_arguments(self):
        await self.pm.emit('foo', (1, 'two'))
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': [1, 'two'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', (1, b'two', 'three'))
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [
                    [1, {'_placeholder': True, 'num': 0}, 'three'], 'dHdv',
                ],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_to(self):
        sid = 'room-mate'
        await self.pm.emit('foo', 'bar', to=sid)
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': sid,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_namespace(self):
        await self.pm.emit('foo', 'bar', namespace='/baz')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/baz',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_room(self):
        await self.pm.emit('foo', 'bar', room='baz')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': 'baz',
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_skip_sid(self):
        await self.pm.emit('foo', 'bar', skip_sid='baz')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': None,
                'skip_sid': 'baz',
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_callback(self):
        with mock.patch.object(
            self.pm, '_generate_ack_id', return_value='123'
        ):
            await self.pm.emit('foo', 'bar', room='baz', callback='cb')
            self.pm._publish.assert_awaited_once_with(
                {
                    'method': 'emit',
                    'event': 'foo',
                    'binary': False,
                    'data': ['bar'],
                    'namespace': '/',
                    'room': 'baz',
                    'skip_sid': None,
                    'callback': ('baz', '/', '123'),
                    'host_id': '123456',
                }
            )

    async def test_emit_with_callback_without_server(self):
        standalone_pm = async_pubsub_manager.AsyncPubSubManager()
        with pytest.raises(RuntimeError):
            await standalone_pm.emit('foo', 'bar', callback='cb')

    async def test_emit_with_callback_missing_room(self):
        with mock.patch.object(
            self.pm, '_generate_ack_id', return_value='123'
        ):
            with pytest.raises(ValueError):
                await self.pm.emit('foo', 'bar', callback='cb')

    async def test_emit_with_ignore_queue(self):
        sid = await self.pm.connect('123', '/')
        await self.pm.emit(
            'foo', 'bar', room=sid, namespace='/', ignore_queue=True
        )
        self.pm._publish.assert_not_awaited()
        assert self.pm.server._send_eio_packet.await_count == 1
        assert self.pm.server._send_eio_packet.await_args_list[0][0][0] \
            == '123'
        pkt = self.pm.server._send_eio_packet.await_args_list[0][0][1]
        assert pkt.encode() == '42["foo","bar"]'

    async def test_can_disconnect(self):
        sid = await self.pm.connect('123', '/')
        assert await self.pm.can_disconnect(sid, '/') is True
        await self.pm.can_disconnect(sid, '/foo')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'disconnect', 'sid': sid, 'namespace': '/foo',
             'host_id': '123456'}
        )

    async def test_disconnect(self):
        await self.pm.disconnect('foo', '/')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'disconnect', 'sid': 'foo', 'namespace': '/',
             'host_id': '123456'}
        )

    async def test_disconnect_ignore_queue(self):
        sid = await self.pm.connect('123', '/')
        self.pm.pre_disconnect(sid, '/')
        await self.pm.disconnect(sid, '/', ignore_queue=True)
        self.pm._publish.assert_not_awaited()
        assert self.pm.is_connected(sid, '/') is False

    async def test_enter_room(self):
        sid = await self.pm.connect('123', '/')
        await self.pm.enter_room(sid, '/', 'foo')
        await self.pm.enter_room('456', '/', 'foo')
        assert sid in self.pm.rooms['/']['foo']
        assert self.pm.rooms['/']['foo'][sid] == '123'
        self.pm._publish.assert_awaited_once_with(
            {'method': 'enter_room', 'sid': '456', 'room': 'foo',
             'namespace': '/', 'host_id': '123456'}
        )

    async def test_leave_room(self):
        sid = await self.pm.connect('123', '/')
        await self.pm.leave_room(sid, '/', 'foo')
        await self.pm.leave_room('456', '/', 'foo')
        assert 'foo' not in self.pm.rooms['/']
        self.pm._publish.assert_awaited_once_with(
            {'method': 'leave_room', 'sid': '456', 'room': 'foo',
             'namespace': '/', 'host_id': '123456'}
        )

    async def test_close_room(self):
        await self.pm.close_room('foo')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'close_room', 'room': 'foo', 'namespace': '/',
             'host_id': '123456'}
        )

    async def test_close_room_with_namespace(self):
        await self.pm.close_room('foo', '/bar')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'close_room', 'room': 'foo', 'namespace': '/bar',
             'host_id': '123456'}
        )

    async def test_handle_emit(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': ['bar']})
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_legacy_emit(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': 'bar'})
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_binary(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [[{'_placeholder': True, 'num': 0}], 'YmFy'],
            })
            super_emit.assert_awaited_once_with(
                'foo',
                b'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [[{'foo': {'_placeholder': True, 'num': 0}}], 'YmFy'],
            })
            super_emit.assert_awaited_with(
                'foo',
                {'foo': b'bar'},
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_legacy_emit_binary(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [{'_placeholder': True, 'num': 0}, 'YmFy'],
            })
            super_emit.assert_awaited_once_with(
                'foo',
                b'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [{'foo': {'_placeholder': True, 'num': 0}}, 'YmFy'],
            })
            super_emit.assert_awaited_with(
                'foo',
                {'foo': b'bar'},
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_list(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': [[1, 'two']]})
            super_emit.assert_awaited_once_with(
                'foo',
                [1, 'two'],
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [
                    [[1, {'_placeholder': True, 'num': 0}, 'three']], 'dHdv'
                ]
            })
            super_emit.assert_awaited_with(
                'foo',
                [1, b'two', 'three'],
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_no_arguments(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': []})
            super_emit.assert_awaited_once_with(
                'foo',
                (),
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_multiple_arguments(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': [1, 'two']})
            super_emit.assert_awaited_once_with(
                'foo',
                (1, 'two'),
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [
                    [1, {'_placeholder': True, 'num': 0}, 'three'], 'dHdv'
                ]
            })
            super_emit.assert_awaited_with(
                'foo',
                (1, b'two', 'three'),
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_with_namespace(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {'event': 'foo', 'data': 'bar', 'namespace': '/baz'}
            )
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace='/baz',
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_with_room(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {'event': 'foo', 'data': 'bar', 'room': 'baz'}
            )
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room='baz',
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_with_skip_sid(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {'event': 'foo', 'data': 'bar', 'skip_sid': '123'}
            )
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room=None,
                skip_sid='123',
                callback=None,
            )

    async def test_handle_emit_with_remote_callback(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {
                    'event': 'foo',
                    'data': 'bar',
                    'namespace': '/baz',
                    'callback': ('sid', '/baz', 123),
                    'host_id': 'x',
                }
            )
            assert super_emit.await_count == 1
            assert super_emit.await_args[0] == ('foo', 'bar')
            assert super_emit.await_args[1]['namespace'] == '/baz'
            assert super_emit.await_args[1]['room'] is None
            assert super_emit.await_args[1]['skip_sid'] is None
            assert isinstance(
                super_emit.await_args[1]['callback'], functools.partial
            )
            await super_emit.await_args[1]['callback']('one', 2, 'three')
            self.pm._publish.assert_awaited_once_with(
                {
                    'method': 'callback',
                    'host_id': 'x',
                    'sid': 'sid',
                    'namespace': '/baz',
                    'id': 123,
                    'args': ('one', 2, 'three'),
                }
            )

    async def test_handle_emit_with_local_callback(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {
                    'event': 'foo',
                    'data': 'bar',
                    'namespace': '/baz',
                    'callback': ('sid', '/baz', 123),
                    'host_id': self.pm.host_id,
                }
            )
            assert super_emit.await_count == 1
            assert super_emit.await_args[0] == ('foo', 'bar')
            assert super_emit.await_args[1]['namespace'] == '/baz'
            assert super_emit.await_args[1]['room'] is None
            assert super_emit.await_args[1]['skip_sid'] is None
            assert isinstance(
                super_emit.await_args[1]['callback'], functools.partial
            )
            await super_emit.await_args[1]['callback']('one', 2, 'three')
            self.pm._publish.assert_not_awaited()

    async def test_handle_callback(self):
        host_id = self.pm.host_id
        with mock.patch.object(
            self.pm, 'trigger_callback'
        ) as trigger:
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': host_id,
                    'sid': 'sid',
                    'namespace': '/',
                    'id': 123,
                    'args': ('one', 2),
                }
            )
            trigger.assert_awaited_once_with('sid', 123, ('one', 2))

    async def test_handle_callback_bad_host_id(self):
        with mock.patch.object(
            self.pm, 'trigger_callback'
        ) as trigger:
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': 'bad',
                    'sid': 'sid',
                    'namespace': '/',
                    'id': 123,
                    'args': ('one', 2),
                }
            )
            assert trigger.await_count == 0

    async def test_handle_callback_missing_args(self):
        host_id = self.pm.host_id
        with mock.patch.object(
            self.pm, 'trigger_callback'
        ) as trigger:
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': host_id,
                    'sid': 'sid',
                    'namespace': '/',
                    'id': 123,
                }
            )
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': host_id,
                    'sid': 'sid',
                    'namespace': '/',
                }
            )
            await self.pm._handle_callback(
                {'method': 'callback', 'host_id': host_id, 'sid': 'sid'}
            )
            await self.pm._handle_callback(
                {'method': 'callback', 'host_id': host_id}
            )
            assert trigger.await_count == 0

    async def test_handle_disconnect(self):
        await self.pm._handle_disconnect(
            {'method': 'disconnect', 'sid': '123', 'namespace': '/foo'}
        )
        self.pm.server.disconnect.assert_awaited_once_with(
            sid='123', namespace='/foo', ignore_queue=True
        )

    async def test_handle_enter_room(self):
        sid = await self.pm.connect('123', '/')
        with mock.patch.object(
            async_manager.AsyncManager, 'enter_room'
        ) as super_enter_room:
            await self.pm._handle_enter_room(
                {'method': 'enter_room', 'sid': sid, 'namespace': '/',
                 'room': 'foo'}
            )
            await self.pm._handle_enter_room(
                {'method': 'enter_room', 'sid': '456', 'namespace': '/',
                 'room': 'foo'}
            )
            super_enter_room.assert_awaited_once_with(sid, '/', 'foo')

    async def test_handle_leave_room(self):
        sid = await self.pm.connect('123', '/')
        with mock.patch.object(
            async_manager.AsyncManager, 'leave_room'
        ) as super_leave_room:
            await self.pm._handle_leave_room(
                {'method': 'leave_room', 'sid': sid, 'namespace': '/',
                 'room': 'foo'}
            )
            await self.pm._handle_leave_room(
                {'method': 'leave_room', 'sid': '456', 'namespace': '/',
                 'room': 'foo'}
            )
            super_leave_room.assert_awaited_once_with(sid, '/', 'foo')

    async def test_handle_close_room(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'close_room'
        ) as super_close_room:
            await self.pm._handle_close_room(
                {'method': 'close_room', 'room': 'foo'}
            )
            super_close_room.assert_awaited_once_with(
                room='foo', namespace=None
            )

    async def test_handle_close_room_with_namespace(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'close_room'
        ) as super_close_room:
            await self.pm._handle_close_room(
                {
                    'method': 'close_room',
                    'room': 'foo',
                    'namespace': '/bar',
                }
            )
            super_close_room.assert_awaited_once_with(
                room='foo', namespace='/bar'
            )

    async def test_background_thread(self):
        self.pm._handle_emit = mock.AsyncMock()
        self.pm._handle_callback = mock.AsyncMock()
        self.pm._handle_disconnect = mock.AsyncMock()
        self.pm._handle_enter_room = mock.AsyncMock()
        self.pm._handle_leave_room = mock.AsyncMock()
        self.pm._handle_close_room = mock.AsyncMock()
        host_id = self.pm.host_id

        async def messages():
            yield {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
            yield {'missing': 'method', 'host_id': 'x'}
            yield '{"method": "callback", "value": "bar", "host_id": "x"}'
            yield {'method': 'disconnect', 'sid': '123', 'namespace': '/foo',
                   'host_id': 'x'}
            yield {'method': 'bogus', 'host_id': 'x'}
            yield json.dumps({'method': 'close_room', 'value': 'baz',
                              'host_id': 'x'})
            yield {'method': 'enter_room', 'sid': '123', 'namespace': '/foo',
                   'room': 'room', 'host_id': 'x'}
            yield {'method': 'leave_room', 'sid': '123', 'namespace': '/foo',
                   'room': 'room', 'host_id': 'x'}
            yield 'bad json'
            yield b'bad data'

            # these should not publish anything on the queue, as they come from
            # the same host
            yield {'method': 'emit', 'value': 'foo', 'host_id': host_id}
            yield {'method': 'callback', 'value': 'bar', 'host_id': host_id}
            yield {'method': 'disconnect', 'sid': '123', 'namespace': '/foo',
                   'host_id': host_id}
            yield json.dumps({'method': 'close_room', 'value': 'baz',
                              'host_id': host_id})

        self.pm._listen = messages
        await self.pm._thread()

        self.pm._handle_emit.assert_awaited_once_with(
            {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
        )
        self.pm._handle_callback.assert_any_await(
            {'method': 'callback', 'value': 'bar', 'host_id': 'x'}
        )
        self.pm._handle_callback.assert_any_await(
            {'method': 'callback', 'value': 'bar', 'host_id': host_id}
        )
        self.pm._handle_disconnect.assert_awaited_once_with(
            {'method': 'disconnect', 'sid': '123', 'namespace': '/foo',
             'host_id': 'x'}
        )
        self.pm._handle_enter_room.assert_awaited_once_with(
            {'method': 'enter_room', 'sid': '123', 'namespace': '/foo',
             'room': 'room', 'host_id': 'x'}
        )
        self.pm._handle_leave_room.assert_awaited_once_with(
            {'method': 'leave_room', 'sid': '123', 'namespace': '/foo',
             'room': 'room', 'host_id': 'x'}
        )
        self.pm._handle_close_room.assert_awaited_once_with(
            {'method': 'close_room', 'value': 'baz', 'host_id': 'x'}
        )

    async def test_background_thread_exception(self):
        self.pm._handle_emit = mock.AsyncMock(side_effect=[
            ValueError(), asyncio.CancelledError])

        async def messages():
            yield {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
            yield {'method': 'emit', 'value': 'bar', 'host_id': 'x'}

        self.pm._listen = messages
        await self.pm._thread()

        self.pm._handle_emit.assert_any_await(
            {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
        )
        self.pm._handle_emit.assert_awaited_with(
            {'method': 'emit', 'value': 'bar', 'host_id': 'x'}
        )

    def test_custom_json(self):
        saved_json = packet.Packet.json

        cm = async_pubsub_manager.AsyncPubSubManager(json='foo')
        assert cm.json == 'foo'
        async_server.AsyncServer(json='bar', client_manager=cm)
        assert cm.json == 'bar'

        packet.Packet.json = saved_json
        EIOPacket.json = saved_json


class _AsyncClusterBus:
    """An in-memory pub/sub backend that connects any number of managers."""
    def __init__(self):
        self.managers = {}
        self.blocked = set()
        self.published = []

    def add(self, manager):
        self.managers[manager.host_id] = manager

        async def _publish(message, _manager=manager):
            await self.publish(_manager, message)

        manager._publish = _publish

    async def publish(self, sender, message):
        # simulate the serialization that a real backend would perform
        message = json.loads(json.dumps(message))
        self.published.append((sender.host_id, message))
        for host_id, manager in self.managers.items():
            if manager is sender or host_id in self.blocked:
                continue
            await manager._process_message(message)


class TestAsyncPubSubManagerCluster:
    def _make_manager(self, bus, host_id, sessions=None):
        mock_server = mock.MagicMock()
        mock_server.packet_class = packet.Packet
        mock_server._send_packet = mock.AsyncMock()
        mock_server._send_eio_packet = mock.AsyncMock()
        mock_server.disconnect = mock.AsyncMock()
        mock_server.get_session = mock.AsyncMock(
            side_effect=lambda sid, namespace=None: (
                sessions or {}).get(sid, {}))
        counter = [0]

        def generate_id():
            counter[0] += 1
            return host_id + '-sid-' + str(counter[0])

        mock_server.eio.generate_id = generate_id
        pm = async_pubsub_manager.AsyncPubSubManager()
        pm.set_server(mock_server)
        pm.host_id = host_id
        pm.initialize()
        bus.add(pm)
        return pm

    async def _make_cluster(self, sessions=None):
        bus = _AsyncClusterBus()
        managers = [
            self._make_manager(bus, 'host-a', (sessions or {}).get('host-a')),
            self._make_manager(bus, 'host-b', (sessions or {}).get('host-b')),
            self._make_manager(bus, 'host-c', (sessions or {}).get('host-c')),
        ]
        sids = {}
        for pm in managers:
            sid = await pm.connect('eio-' + pm.host_id, '/')
            pm.basic_enter_room(sid, '/', 'room1')
            sids[pm.host_id] = sid
        # generate some traffic so that the hosts discover each other
        for pm in managers:
            await pm.emit('ping', 'x')
        return bus, managers, sids

    async def test_cluster_query_merges_all_hosts(self):
        bus, managers, sids = await self._make_cluster()

        result = await managers[0].get_cluster_participants('/', 'room1')

        assert result.complete
        assert result.missing_hosts == []
        assert len(result) == 3
        by_host = {p['host_id']: p for p in result}
        assert set(by_host) == {'host-a', 'host-b', 'host-c'}
        for host_id, participant in by_host.items():
            assert participant['sid'] == sids[host_id]
            assert participant['namespace'] == '/'
            assert participant['rooms'] == [sids[host_id], 'room1']
            assert participant['session'] == {}

    async def test_cluster_query_single_host(self):
        bus = _AsyncClusterBus()
        pm = self._make_manager(bus, 'host-a')
        sid = await pm.connect('eio-a', '/')
        pm.basic_enter_room(sid, '/', 'room1')

        # with no other known hosts the query returns immediately
        result = await pm.get_cluster_participants('/', 'room1')
        assert result.complete
        assert result.missing_hosts == []
        assert [p['sid'] for p in result] == [sid]

        # operations on a purely local result do not hit the pub/sub backend
        op = await result.join('room2')
        assert op.complete
        assert op.ok == [sid]
        assert sid in pm.rooms['/']['room2']
        assert [m for _, m in bus.published
                if m['method'] == 'cluster_join'] == []

    async def test_cluster_query_missing_host(self):
        bus, managers, sids = await self._make_cluster()
        bus.blocked.add('host-c')

        result = await managers[0].get_cluster_participants(
            '/', 'room1', timeout=0.01)

        # the unresponsive host is reported and never assumed to be empty
        assert not result.complete
        assert result.missing_hosts == ['host-c']
        assert {p['sid'] for p in result} == {sids['host-a'], sids['host-b']}

    async def test_cluster_query_duplicate_response(self):
        bus, managers, sids = await self._make_cluster()
        original_publish = bus.publish

        async def duplicate_publish(sender, message):
            await original_publish(sender, message)
            if message.get('method') == 'cluster_query_response':
                await original_publish(sender, message)  # duplicate delivery

        bus.publish = duplicate_publish

        result = await managers[0].get_cluster_participants('/', 'room1')

        assert result.complete
        assert len(result) == 3
        assert sorted(p['sid'] for p in result) == sorted(sids.values())

    async def test_cluster_query_late_response(self):
        bus, managers, sids = await self._make_cluster()
        bus.blocked.add('host-c')

        result = await managers[0].get_cluster_participants(
            '/', 'room1', timeout=0.01)
        assert result.missing_hosts == ['host-c']

        # a response that arrives after the query completed is ignored
        request_id = [m for _, m in bus.published
                      if m['method'] == 'cluster_query'][-1]['request_id']
        await managers[0]._process_message({
            'method': 'cluster_query_response',
            'request_id': request_id,
            'host_id': 'host-c',
            'participants': [{'sid': 'late-sid', 'namespace': '/',
                              'rooms': [], 'token': 'x'}],
        })

        assert managers[0]._pending_requests == {}
        assert {p['sid'] for p in result} == {sids['host-a'], sids['host-b']}

    async def test_cluster_query_session_keys(self):
        sessions = {}
        for host in ('host-a', 'host-b', 'host-c'):
            sessions[host] = {
                host + '-sid-1': {
                    'user': 'user-of-' + host,
                    'auth_token': 'secret',
                    'environ': {'wsgi.input': object()},
                },
            }
        bus, managers, sids = await self._make_cluster(sessions=sessions)

        # by default no session data is included in the result
        result = await managers[0].get_cluster_participants('/', 'room1')
        for participant in result:
            assert participant['session'] == {}
        for _, message in bus.published:
            if message['method'] == 'cluster_query_response':
                for participant in message['participants']:
                    assert 'session' not in participant

        # only the explicitly allowed keys are returned, and values that
        # cannot be serialized (such as the request environ) are omitted
        result = await managers[0].get_cluster_participants(
            '/', 'room1', session_keys=['user', 'environ', 'missing'])
        for participant in result:
            assert participant['session'] == {
                'user': 'user-of-' + participant['host_id']}

    async def test_cluster_remote_operation_from_observer_host(self):
        bus, managers, sids = await self._make_cluster()
        # host-a has no clients in room1, so it is not a target of the
        # operation it issues
        managers[0].basic_leave_room(sids['host-a'], '/', 'room1')
        # host-d has no clients in room1 either, so it ignores the operation
        pm_d = self._make_manager(bus, 'host-d')
        sid_d = await pm_d.connect('eio-d', '/')
        await pm_d.emit('ping', 'x')

        result = await managers[0].get_cluster_participants('/', 'room1')
        assert result.complete
        assert {p['sid'] for p in result} == {sids['host-b'], sids['host-c']}

        op = await result.join('room2')
        assert op.complete
        assert op.ok == sorted([sids['host-b'], sids['host-c']])
        assert sids['host-b'] in managers[1].rooms['/']['room2']
        assert sids['host-c'] in managers[2].rooms['/']['room2']
        assert sid_d not in pm_d.rooms['/'].get('room2', {})

    async def test_cluster_remote_join_leave_disconnect(self):
        bus, managers, sids = await self._make_cluster()
        result = await managers[0].get_cluster_participants('/', 'room1')

        op = await result.join('room2')
        assert op == async_pubsub_manager.ClusterOperationResult(
            ok=sorted(sids.values()))
        assert repr(op) == ('ClusterOperationResult(ok={!r}, stale=[], '
                            'unknown=[], missing_hosts=[])').format(
                                sorted(sids.values()))
        for pm in managers:
            assert sids[pm.host_id] in pm.rooms['/']['room2']

        op = await result.leave('room2')
        assert op == async_pubsub_manager.ClusterOperationResult(
            ok=sorted(sids.values()))
        for pm in managers:
            assert 'room2' not in pm.rooms['/']

        op = await result.disconnect()
        assert op == async_pubsub_manager.ClusterOperationResult(
            ok=sorted(sids.values()))
        for pm in managers:
            pm.server.disconnect.assert_called_once_with(
                sids[pm.host_id], '/', ignore_queue=True)

    async def test_cluster_remote_operation_missing_host(self):
        bus, managers, sids = await self._make_cluster()
        result = await managers[0].get_cluster_participants('/', 'room1')
        assert result.complete

        # host-c becomes unresponsive after the query was made
        bus.blocked.add('host-c')
        op = await result.join('room2', timeout=0.01)
        assert not op.complete
        assert op.missing_hosts == ['host-c']
        assert op.ok == sorted([sids['host-a'], sids['host-b']])
        assert op.stale == []
        assert op.unknown == [sids['host-c']]
        assert 'room2' not in managers[2].rooms['/']

    async def test_cluster_remote_operation_on_incomplete_query(self):
        bus, managers, sids = await self._make_cluster()
        bus.blocked.add('host-c')
        result = await managers[0].get_cluster_participants(
            '/', 'room1', timeout=0.01)
        assert result.missing_hosts == ['host-c']

        # the operation applies to the observed participants only
        op = await result.join('room2')
        assert op.complete
        assert op.ok == sorted([sids['host-a'], sids['host-b']])
        assert 'room2' not in managers[2].rooms['/']

    async def test_cluster_remote_operation_stale(self):
        bus, managers, sids = await self._make_cluster()
        result = await managers[0].get_cluster_participants('/', 'room1')

        # the client on host-b disconnects and reconnects after the query
        managers[1].basic_disconnect(sids['host-b'], '/')
        new_sid = await managers[1].connect('eio-new-b', '/')
        managers[1].basic_enter_room(new_sid, '/', 'room1')

        # the stale connection is reported and the new one is not affected
        op = await result.join('room2')
        assert op.ok == sorted([sids['host-a'], sids['host-c']])
        assert op.stale == [sids['host-b']]
        assert new_sid not in managers[1].rooms['/'].get('room2', {})

        op = await result.disconnect()
        assert op.ok == sorted([sids['host-a'], sids['host-c']])
        assert op.stale == [sids['host-b']]
        managers[1].server.disconnect.assert_not_called()
        assert managers[1].is_connected(new_sid, '/')

    async def test_cluster_remote_operation_requires_room(self):
        bus, managers, sids = await self._make_cluster()
        result = await managers[0].get_cluster_participants('/', 'room1')
        with pytest.raises(ValueError):
            await result.join(None)
        with pytest.raises(ValueError):
            await result.leave(None)
