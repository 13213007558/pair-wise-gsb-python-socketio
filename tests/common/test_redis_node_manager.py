import json
from unittest import mock

import msgpack
import pytest

from socketio import manager
from socketio import packet
from socketio import pubsub_manager
from socketio.redis_manager import RedisNodeManager


def unpack(data):
    return msgpack.unpackb(data, raw=False)


def pack(message):
    return msgpack.packb(message, use_bin_type=True)


class TestRedisNodeManager:
    def setup_method(self):
        id = 0

        def generate_id():
            nonlocal id
            id += 1
            return str(id)

        mock_server = mock.MagicMock()
        mock_server.eio.generate_id = generate_id
        mock_server.packet_class = packet.Packet
        self.pm = RedisNodeManager('redis://')
        self.pm._publish_message = mock.MagicMock()
        self.pm.set_server(mock_server)
        self.pm.host_id = '123456'
        self.pm.initialize()

    def test_default_init(self):
        assert self.pm.channel == 'socket.io'
        assert self.pm.name == 'redisnode'
        self.pm.server.start_background_task.assert_called_once_with(
            self.pm._thread
        )

    def test_write_only_init(self):
        mock_server = mock.MagicMock()
        pm = RedisNodeManager('redis://', write_only=True)
        pm.set_server(mock_server)
        pm.initialize()
        assert pm.channel == 'socket.io'
        assert mock_server.start_background_task.call_count == 0

    def test_emit(self):
        self.pm.emit('foo', 'bar')
        self.pm._publish_message.assert_called_once()
        channel, message = self.pm._publish_message.call_args[0]
        assert channel == 'socket.io#/#'
        uid, pkt, opts = unpack(message)
        assert uid == '123456'
        assert pkt == {'type': 2, 'data': ['foo', 'bar'], 'nsp': '/'}
        assert opts == {'rooms': [], 'except': [], 'flags': {}}

    def test_emit_with_namespace(self):
        self.pm.emit('foo', 'bar', namespace='/chat')
        channel, message = self.pm._publish_message.call_args[0]
        assert channel == 'socket.io#/chat#'
        assert unpack(message)[1]['nsp'] == '/chat'

    def test_emit_with_room(self):
        self.pm.emit('foo', 'bar', room='room1')
        channel, message = self.pm._publish_message.call_args[0]
        assert channel == 'socket.io#/#room1#'
        assert unpack(message)[2]['rooms'] == ['room1']

    def test_emit_with_to(self):
        self.pm.emit('foo', 'bar', to='room1')
        channel, message = self.pm._publish_message.call_args[0]
        assert channel == 'socket.io#/#room1#'
        assert unpack(message)[2]['rooms'] == ['room1']

    def test_emit_with_multiple_rooms(self):
        self.pm.emit('foo', 'bar', to=['room1', 'room2'])
        channel, message = self.pm._publish_message.call_args[0]
        assert channel == 'socket.io#/#'
        assert unpack(message)[2]['rooms'] == ['room1', 'room2']

    def test_emit_with_skip_sid(self):
        self.pm.emit('foo', 'bar', skip_sid='987')
        channel, message = self.pm._publish_message.call_args[0]
        assert unpack(message)[2]['except'] == ['987']

    def test_emit_with_skip_sid_list(self):
        self.pm.emit('foo', 'bar', skip_sid=['987', '654'])
        channel, message = self.pm._publish_message.call_args[0]
        assert unpack(message)[2]['except'] == ['987', '654']

    def test_emit_multiple_arguments(self):
        self.pm.emit('foo', ('bar', 'baz'))
        channel, message = self.pm._publish_message.call_args[0]
        assert unpack(message)[1]['data'] == ['foo', 'bar', 'baz']

    def test_emit_binary(self):
        self.pm.emit('foo', b'\x01\x02')
        channel, message = self.pm._publish_message.call_args[0]
        assert unpack(message)[1]['data'] == ['foo', b'\x01\x02']

    def test_emit_with_callback(self):
        with pytest.raises(ValueError):
            self.pm.emit('foo', 'bar', room='room1', callback='cb')
        self.pm._publish_message.assert_not_called()

    def test_emit_with_ignore_queue(self):
        sid = self.pm.connect('123', '/')
        self.pm.emit('foo', 'bar', room=sid, namespace='/',
                     ignore_queue=True)
        self.pm._publish_message.assert_not_called()
        assert self.pm.server._send_eio_packet.call_count == 1
        pkt = self.pm.server._send_eio_packet.call_args_list[0][0][1]
        assert pkt.encode() == '42["foo","bar"]'

    def test_emit_delivers_to_local_clients(self):
        self.pm.connect('123', '/')
        self.pm.emit('foo', 'bar')
        assert self.pm.server._send_eio_packet.call_count == 1
        assert self.pm.server._send_eio_packet.call_args_list[0][0][0] == '123'
        pkt = self.pm.server._send_eio_packet.call_args_list[0][0][1]
        assert pkt.encode() == '42["foo","bar"]'

    def test_emit_delivers_to_local_room(self):
        sid1 = self.pm.connect('1', '/')
        self.pm.connect('2', '/')
        self.pm.enter_room(sid1, '/', 'room1')
        self.pm.emit('foo', 'bar', room='room1')
        assert self.pm.server._send_eio_packet.call_count == 1
        assert self.pm.server._send_eio_packet.call_args_list[0][0][0] == '1'

    def test_disconnect_is_local_only(self):
        self.pm.disconnect('foo')
        self.pm._publish_message.assert_not_called()

    def test_enter_room_remote_is_local_only(self):
        self.pm.enter_room('foo', '/', 'room1')
        self.pm._publish_message.assert_not_called()

    def test_leave_room_remote_is_local_only(self):
        self.pm.leave_room('foo', '/', 'room1')
        self.pm._publish_message.assert_not_called()

    def test_close_room_is_local_only(self):
        self.pm.close_room('room1')
        self.pm._publish_message.assert_not_called()

    def test_handle_node_emit(self):
        with mock.patch.object(manager.Manager, 'emit') as super_emit:
            self.pm._handle_node_emit(
                {'type': 2, 'data': ['foo', 'bar'], 'nsp': '/baz'},
                {'rooms': [], 'except': [], 'flags': {}},
            )
            super_emit.assert_called_once_with(
                'foo', 'bar', namespace='/baz', room=None, skip_sid=None)

    def test_handle_node_emit_with_rooms_and_except(self):
        with mock.patch.object(manager.Manager, 'emit') as super_emit:
            self.pm._handle_node_emit(
                {'type': 2, 'data': ['foo', 'bar'], 'nsp': '/'},
                {'rooms': ['a', 'b'], 'except': ['1', '2'], 'flags': {}},
            )
            super_emit.assert_called_once_with(
                'foo', 'bar', namespace='/', room=['a', 'b'],
                skip_sid=['1', '2'])

    def test_handle_node_emit_multiple_arguments(self):
        with mock.patch.object(manager.Manager, 'emit') as super_emit:
            self.pm._handle_node_emit(
                {'type': 2, 'data': ['foo', 'bar', 'baz'], 'nsp': '/'},
                {'rooms': [], 'except': [], 'flags': {}},
            )
            super_emit.assert_called_once_with(
                'foo', ('bar', 'baz'), namespace='/', room=None,
                skip_sid=None)

    def test_handle_node_emit_no_arguments(self):
        with mock.patch.object(manager.Manager, 'emit') as super_emit:
            self.pm._handle_node_emit(
                {'type': 2, 'data': ['foo'], 'nsp': '/'},
                {'rooms': [], 'except': [], 'flags': {}},
            )
            super_emit.assert_called_once_with(
                'foo', (), namespace='/', room=None, skip_sid=None)

    def test_handle_node_emit_missing_nsp(self):
        with mock.patch.object(manager.Manager, 'emit') as super_emit:
            self.pm._handle_node_emit(
                {'type': 2, 'data': ['foo', 'bar']},
                {'rooms': [], 'except': [], 'flags': {}},
            )
            super_emit.assert_called_once_with(
                'foo', 'bar', namespace='/', room=None, skip_sid=None)

    def test_handle_node_emit_binary(self):
        self.pm.connect('123', '/')
        self.pm._handle_node_emit(
            {'type': 2, 'data': ['foo', b'\x01\x02'], 'nsp': '/'},
            {'rooms': [], 'except': [], 'flags': {}},
        )
        packets = self.pm.server._send_eio_packet.call_args_list
        assert len(packets) == 2
        assert packets[0][0][1].encode() == \
            '451-["foo",{"_placeholder":true,"num":0}]'
        assert packets[1][0][1].data == b'\x01\x02'

    def test_handle_node_emit_dedupes_multiple_rooms(self):
        sid = self.pm.connect('123', '/')
        self.pm.enter_room(sid, '/', 'room1')
        self.pm.enter_room(sid, '/', 'room2')
        self.pm._handle_node_emit(
            {'type': 2, 'data': ['foo', 'bar'], 'nsp': '/'},
            {'rooms': ['room1', 'room2'], 'except': [], 'flags': {}},
        )
        assert self.pm.server._send_eio_packet.call_count == 1

    def test_handle_node_emit_except(self):
        sid1 = self.pm.connect('1', '/')
        self.pm.connect('2', '/')
        self.pm._handle_node_emit(
            {'type': 2, 'data': ['foo', 'bar'], 'nsp': '/'},
            {'rooms': [], 'except': [sid1], 'flags': {}},
        )
        assert self.pm.server._send_eio_packet.call_count == 1
        assert self.pm.server._send_eio_packet.call_args_list[0][0][0] == '2'

    def test_background_thread(self):
        self.pm._handle_node_emit = mock.MagicMock()

        def messages():
            yield pack(['abc', {'type': 2, 'data': ['foo', 'bar'],
                                'nsp': '/'},
                        {'rooms': [], 'except': [], 'flags': {}}])
            # own messages are ignored
            yield pack(['123456', {'type': 2, 'data': ['foo', 'bar'],
                                   'nsp': '/'},
                        {'rooms': [], 'except': [], 'flags': {}}])
            # a message in the python RedisManager format is ignored
            yield json.dumps({'method': 'emit', 'event': 'foo',
                              'data': ['bar'], 'namespace': '/',
                              'host_id': 'x'})
            # invalid messages are ignored
            yield b'\x01\x02\x03'
            yield pack(['not', 'a', 'broadcast'])
            yield pack(['abc', {'type': 3, 'data': ['foo'], 'nsp': '/'},
                        {'rooms': [], 'except': [], 'flags': {}}])

        self.pm._listen = mock.MagicMock(side_effect=messages)
        try:
            self.pm._thread()
        except StopIteration:
            pass

        self.pm._handle_node_emit.assert_called_once_with(
            {'type': 2, 'data': ['foo', 'bar'], 'nsp': '/'},
            {'rooms': [], 'except': [], 'flags': {}},
        )

    def test_background_thread_exception(self):
        self.pm._handle_node_emit = mock.MagicMock(
            side_effect=[ValueError(), None])

        def messages():
            yield pack(['abc', {'type': 2, 'data': ['foo'], 'nsp': '/'}, {}])
            yield pack(['abc', {'type': 2, 'data': ['bar'], 'nsp': '/'}, {}])

        self.pm._listen = mock.MagicMock(side_effect=messages)
        try:
            self.pm._thread()
        except StopIteration:
            pass

        assert self.pm._handle_node_emit.call_count == 2

    def test_python_manager_ignores_node_messages(self):
        # the regular RedisManager protocol must not pick up messages
        # encoded for the Node.js adapter
        pm = pubsub_manager.PubSubManager()
        pm._publish = mock.MagicMock()
        mock_server = mock.MagicMock()
        mock_server.packet_class = packet.Packet
        pm.set_server(mock_server)
        pm._handle_emit = mock.MagicMock()

        def messages():
            yield pack(['abc', {'type': 2, 'data': ['foo', 'bar'],
                                'nsp': '/'},
                        {'rooms': [], 'except': [], 'flags': {}}])

        pm._listen = mock.MagicMock(side_effect=messages)
        try:
            pm._thread()
        except StopIteration:
            pass

        pm._handle_emit.assert_not_called()
