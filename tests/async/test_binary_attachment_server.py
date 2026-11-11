"""Async server entry-point tests for malformed binary attachments.

Async counterpart of ``tests/common/test_binary_attachment_server.py``: each
test drives ``AsyncServer._handle_eio_message`` with hand-written frames so
the complete decode -> assemble -> dispatch path is covered for both
BINARY_EVENT and BINARY_ACK.
"""
from unittest import mock

import pytest

from socketio import async_server
from socketio import packet


@mock.patch('socketio.server.engineio.AsyncServer', **{
    'return_value.generate_id.side_effect': [str(i) for i in range(1, 10)],
    'return_value.send_packet': mock.AsyncMock()})
class TestAsyncServerBinaryAttachmentDispatch:
    async def _connect(self):
        s = async_server.AsyncServer(async_handlers=False)
        await s.manager.connect('123', '/')
        return s

    async def test_binary_event_is_dispatched_once_with_full_content(
            self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message(
            '123',
            '52-["my message","a",'
            '{"_placeholder":true,"num":1},'
            '{"_placeholder":true,"num":0}]',
        )
        handler.assert_not_called()
        await s._handle_eio_message('123', b'first')
        handler.assert_not_called()
        await s._handle_eio_message('123', b'second')
        handler.assert_called_once_with(mock.ANY, 'a', b'second', b'first')
        assert '123' not in s._binary_packet

    async def test_binary_ack_invokes_callback_once_with_assembled_args(
            self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        s.manager.trigger_callback = mock.AsyncMock()
        sid = s.manager.sid_from_eio_sid('123', '/')
        await s._handle_eio_message(
            '123',
            '62-321["ack",{"_placeholder":true,"num":0},'
            '{"_placeholder":true,"num":1}]',
        )
        s.manager.trigger_callback.assert_not_called()
        await s._handle_eio_message('123', b'zero')
        s.manager.trigger_callback.assert_not_called()
        await s._handle_eio_message('123', b'one')
        s.manager.trigger_callback.assert_awaited_once_with(
            sid, 321, ['ack', b'zero', b'one'])
        assert '123' not in s._binary_packet

    @pytest.mark.parametrize('encoded,attachments', [
        ('51-["my message",{"_placeholder":true,"num":-1}]', [b'att']),
        ('52-["my message",{"_placeholder":true,"num":2}]',
         [b'att0', b'att1']),
        ('51-["my message",{"_placeholder":true,"num":true}]', [b'att']),
        ('51-["my message",{"_placeholder":true,"num":"0"}]', [b'att']),
    ])
    async def test_invalid_reference_rejects_and_never_calls_handler(
            self, eio, encoded, attachments):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message('123', encoded)
        for attachment in attachments[:-1]:
            await s._handle_eio_message('123', attachment)
            handler.assert_not_called()
        with pytest.raises(ValueError):
            await s._handle_eio_message('123', attachments[-1])
        handler.assert_not_called()
        assert s._binary_packet == {}

    async def test_failed_reconstruction_does_not_poison_following_messages(
            self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message(
            '123', '51-["my message",{"_placeholder":true,"num":-1}]')
        with pytest.raises(ValueError):
            await s._handle_eio_message('123', b'attachment')
        assert s._binary_packet == {}
        await s._handle_eio_message('123', '2["my message","text"]')
        handler.assert_called_once_with(mock.ANY, 'text')
        await s._handle_eio_message(
            '123', '51-["my message",{"_placeholder":true,"num":0}]')
        await s._handle_eio_message('123', b'good')
        assert handler.call_count == 2
        assert handler.call_args == mock.call(mock.ANY, b'good')

    async def test_invalid_binary_ack_rejects_without_callback(self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        s.manager.trigger_callback = mock.AsyncMock()
        await s._handle_eio_message(
            '123', '61-321[{"_placeholder":true,"num":-1}]')
        with pytest.raises(ValueError):
            await s._handle_eio_message('123', b'attachment')
        s.manager.trigger_callback.assert_not_called()
        assert s._binary_packet == {}
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message('123', '2["my message","ok"]')
        handler.assert_called_once_with(mock.ANY, 'ok')

    async def test_business_dict_with_num_field_reaches_handler_unchanged(
            self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        handler = mock.MagicMock()
        s.on('order', handler)
        await s._handle_eio_message(
            '123', '2["order",{"num":3,"state":"open"}]')
        handler.assert_called_once_with(
            mock.ANY, {'num': 3, 'state': 'open'})

    async def test_duplicate_attachment_reference_is_delivered_twice(
            self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message(
            '123',
            '51-["my message",'
            '{"_placeholder":true,"num":0},'
            '{"_placeholder":true,"num":0}]')
        await s._handle_eio_message('123', b'shared')
        handler.assert_called_once_with(mock.ANY, b'shared', b'shared')

    async def test_unreferenced_attachment_still_completes_dispatch(
            self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = await self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message(
            '123',
            '52-["my message",{"_placeholder":true,"num":0}]')
        await s._handle_eio_message('123', b'used')
        await s._handle_eio_message('123', b'orphan')
        handler.assert_called_once_with(mock.ANY, b'used')

    async def test_reconstruct_is_strict_under_a_naive_indexing_implementation(
            self, eio):
        eio.return_value.send = mock.AsyncMock()

        class VulnerablePacket(packet.Packet):
            @classmethod
            def _reconstruct_binary_internal(cls, data, attachments):
                if isinstance(data, list):
                    return [cls._reconstruct_binary_internal(i, attachments)
                            for i in data]
                if isinstance(data, dict):
                    if data.get('_placeholder') and 'num' in data:
                        return attachments[data['num']]
                    return {k: cls._reconstruct_binary_internal(v,
                            attachments) for k, v in data.items()}
                return data

        s = async_server.AsyncServer(async_handlers=False)
        s.packet_class = VulnerablePacket
        await s.manager.connect('123', '/')
        handler = mock.MagicMock()
        s.on('my message', handler)
        await s._handle_eio_message(
            '123', '51-["my message",{"_placeholder":true,"num":-1}]')
        await s._handle_eio_message('123', b'should-not-leak')
        handler.assert_called_once_with(mock.ANY, b'should-not-leak')
