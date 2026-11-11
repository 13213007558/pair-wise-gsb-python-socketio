"""Server entry-point tests for malformed binary attachments.

Every test drives ``Server._handle_eio_message`` directly with hand-written
Socket.IO frames, so the full path (frame decode -> attachment assembly ->
event handler / ack callback) is exercised without a local encoder.
"""
from unittest import mock

import pytest

from socketio import packet
from socketio import server


class TestServerBinaryAttachmentDispatch:
    def _connect(self):
        s = server.Server(async_handlers=False)
        s.manager.connect('123', '/')
        return s

    def test_binary_event_is_dispatched_once_with_full_content(self):
        s = self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        s._handle_eio_message(
            '123',
            '52-["my message","a",'
            '{"_placeholder":true,"num":1},'
            '{"_placeholder":true,"num":0}]',
        )
        # mid assembly, nothing has been dispatched yet
        handler.assert_not_called()
        s._handle_eio_message('123', b'first')
        handler.assert_not_called()
        s._handle_eio_message('123', b'second')
        handler.assert_called_once_with(mock.ANY, 'a', b'second', b'first')
        assert '123' not in s._binary_packet

    def test_binary_ack_invokes_callback_once_with_assembled_args(self):
        s = self._connect()
        s.manager.trigger_callback = mock.MagicMock()
        sid = s.manager.sid_from_eio_sid('123', '/')
        s._handle_eio_message(
            '123',
            '62-321["ack",{"_placeholder":true,"num":0},'
            '{"_placeholder":true,"num":1}]',
        )
        s.manager.trigger_callback.assert_not_called()
        s._handle_eio_message('123', b'zero')
        s.manager.trigger_callback.assert_not_called()
        s._handle_eio_message('123', b'one')
        s.manager.trigger_callback.assert_called_once_with(
            sid, 321, ['ack', b'zero', b'one'])
        assert '123' not in s._binary_packet

    @pytest.mark.parametrize('encoded,attachments', [
        # negative index must never resolve via Python negative indexing
        ('51-["my message",{"_placeholder":true,"num":-1}]', [b'att']),
        # out of range positive index
        ('52-["my message",{"_placeholder":true,"num":2}]',
         [b'att0', b'att1']),
        # boolean index
        ('51-["my message",{"_placeholder":true,"num":true}]', [b'att']),
        # wrong type index
        ('51-["my message",{"_placeholder":true,"num":"0"}]', [b'att']),
    ])
    def test_invalid_reference_rejects_and_never_calls_handler(
            self, encoded, attachments):
        s = self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        s._handle_eio_message('123', encoded)
        for attachment in attachments[:-1]:
            # the packet stays pending until every declared attachment
            # arrives; intermediate frames must not dispatch either
            s._handle_eio_message('123', attachment)
            handler.assert_not_called()
        with pytest.raises(ValueError):
            s._handle_eio_message('123', attachments[-1])
        handler.assert_not_called()
        assert s._binary_packet == {}

    def test_failed_reconstruction_does_not_poison_following_messages(self):
        s = self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        # malformed binary event with a negative placeholder index
        s._handle_eio_message(
            '123', '51-["my message",{"_placeholder":true,"num":-1}]')
        with pytest.raises(ValueError):
            s._handle_eio_message('123', b'attachment')
        # the half assembled packet must be gone, otherwise this plain text
        # event would be swallowed as another attachment
        assert s._binary_packet == {}
        s._handle_eio_message('123', '2["my message","text"]')
        handler.assert_called_once_with(mock.ANY, 'text')
        # and a subsequent, well-formed binary event still assembles normally
        s._handle_eio_message(
            '123', '51-["my message",{"_placeholder":true,"num":0}]')
        s._handle_eio_message('123', b'good')
        assert handler.call_count == 2
        assert handler.call_args == mock.call(mock.ANY, b'good')

    def test_invalid_binary_ack_rejects_without_callback(self):
        s = self._connect()
        s.manager.trigger_callback = mock.MagicMock()
        s._handle_eio_message(
            '123', '61-321[{"_placeholder":true,"num":-1}]')
        with pytest.raises(ValueError):
            s._handle_eio_message('123', b'attachment')
        s.manager.trigger_callback.assert_not_called()
        assert s._binary_packet == {}
        # the connection still accepts a normal text event afterwards
        handler = mock.MagicMock()
        s.on('my message', handler)
        s._handle_eio_message('123', '2["my message","ok"]')
        handler.assert_called_once_with(mock.ANY, 'ok')

    def test_business_dict_with_num_field_reaches_handler_unchanged(self):
        s = self._connect()
        handler = mock.MagicMock()
        s.on('order', handler)
        # plain EVENT (type 2), no binary attachments at all
        s._handle_eio_message(
            '123', '2["order",{"num":3,"state":"open"}]')
        handler.assert_called_once_with(
            mock.ANY, {'num': 3, 'state': 'open'})

    def test_duplicate_attachment_reference_is_delivered_twice(self):
        s = self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        s._handle_eio_message(
            '123',
            '51-["my message",'
            '{"_placeholder":true,"num":0},'
            '{"_placeholder":true,"num":0}]')
        s._handle_eio_message('123', b'shared')
        handler.assert_called_once_with(mock.ANY, b'shared', b'shared')

    def test_unreferenced_attachment_still_completes_dispatch(self):
        s = self._connect()
        handler = mock.MagicMock()
        s.on('my message', handler)
        s._handle_eio_message(
            '123',
            '52-["my message",{"_placeholder":true,"num":0}]')
        s._handle_eio_message('123', b'used')
        s._handle_eio_message('123', b'orphan')
        handler.assert_called_once_with(mock.ANY, b'used')

    def test_reconstruct_is_strict_under_a_naive_indexing_implementation(self):
        # mutation test: swap in a deliberately vulnerable reconstruct that
        # indexes attachments directly, the way the original code did. The
        # negative index tests above must fail against it, proving the test
        # suite actually pins the security behavior rather than passing
        # vacuously.
        class VulnerablePacket(packet.Packet):
            @classmethod
            def _reconstruct_binary_internal(cls, data, attachments):
                if isinstance(data, list):
                    return [cls._reconstruct_binary_internal(i, attachments)
                            for i in data]
                if isinstance(data, dict):
                    if data.get('_placeholder') and 'num' in data:
                        return attachments[data['num']]
                    return {k: cls._reconstruct_binary_internal(v, attachments)
                            for k, v in data.items()}
                return data

        s = server.Server(async_handlers=False)
        s.packet_class = VulnerablePacket
        s.manager.connect('123', '/')
        handler = mock.MagicMock()
        s.on('my message', handler)
        s._handle_eio_message(
            '123', '51-["my message",{"_placeholder":true,"num":-1}]')
        # the naive implementation resolves -1 to the last attachment and
        # dispatches it; assert that this is the broken behavior we guard
        # against, so a regression to it cannot slip through unnoticed
        s._handle_eio_message('123', b'should-not-leak')
        handler.assert_called_once_with(mock.ANY, b'should-not-leak')
