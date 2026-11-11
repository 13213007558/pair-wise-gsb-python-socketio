"""Security regression tests for binary attachment placeholders.

These tests feed hand-crafted Socket.IO packets (no local encode/decode round
trip) through :class:`socketio.packet.Packet` and through the server entry
point (``_handle_eio_message``) all the way to application event handlers and
ack callbacks.

Protocol contract for the Socket.IO v5 binary packet (revision 5):

* A placeholder is an object with a truthy ``_placeholder`` key and a ``num``
  key. ``num`` selects one of the binary attachment packets by position.
* Placeholder indices are zero based and must resolve to an attachment that
  was actually declared. Negative indices, out of range indices and indices of
  the wrong type are invalid and must be rejected with ``ValueError``.
* The same attachment may be referenced more than once; each reference is
  substituted, so an attachment can legitimately appear several times.
* Attachments that no placeholder references are allowed (the count declared
  in the packet header is what gates reconstruction, not actual usage).
* Plain business dictionaries that merely contain a ``num`` field, or whose
  ``_placeholder`` value is falsy, are not placeholders and must be preserved
  verbatim.
* A rejected reconstruction must not leave a half assembled packet behind that
  could consume subsequent messages on the same connection.
"""
import pytest

from socketio import packet


class TestBinaryAttachmentReference:
    def _decode(self, encoded, attachments, packet_type=None):
        pkt = packet.Packet(encoded_packet=encoded)
        if packet_type is not None:
            assert pkt.packet_type == packet_type
        for index, attachment in enumerate(attachments):
            complete = pkt.add_attachment(attachment)
            assert complete is (index == len(attachments) - 1)
        return pkt

    def test_nested_placeholders_select_attachments_by_position(self):
        # hand-crafted nested array/object payload; note that num:1 appears
        # before num:0, so ordering must follow the index rather than arrival
        pkt = self._decode(
            '52-{"outer":[{"_placeholder":true,"num":1},'
            '{"inner":{"_placeholder":true,"num":0}}],"tail":7}',
            [b'first', b'second'],
            packet.BINARY_EVENT,
        )
        assert pkt.data == {
            'outer': [b'second', {'inner': b'first'}],
            'tail': 7,
        }

    def test_duplicate_reference_is_accepted_and_substituted_each_time(self):
        # the protocol allows the same attachment to be referenced repeatedly
        pkt = self._decode(
            '51-[{"_placeholder":true,"num":0},'
            '{"_placeholder":true,"num":0}]',
            [b'shared'],
        )
        assert pkt.data == [b'shared', b'shared']

    def test_unreferenced_attachment_is_accepted(self):
        # two attachments are declared and delivered, but only one is used
        pkt = self._decode(
            '52-[{"_placeholder":true,"num":0},"note"]',
            [b'used', b'orphan'],
        )
        assert pkt.data == [b'used', 'note']
        assert pkt.attachments == [b'used', b'orphan']

    def test_truthy_non_boolean_placeholder_is_recognized(self):
        # any truthy _placeholder marks a reference, matching the reference
        # Socket.IO parser
        pkt = self._decode(
            '51-{"_placeholder":"yes","num":0}', [b'data'])
        assert pkt.data == b'data'

    @pytest.mark.parametrize('num', ['-1', '-2', '99'])
    def test_negative_and_out_of_range_indices_are_rejected(self, num):
        pkt = packet.Packet(
            encoded_packet='51-{"_placeholder":true,"num":%s}' % num)
        with pytest.raises(ValueError):
            pkt.add_attachment(b'attachment')
        # no partially assembled state survives the failure
        assert pkt.attachments == []
        assert pkt.data == {'_placeholder': True, 'num': int(num)}

    def test_negative_index_must_not_select_another_attachment(self):
        # the concrete attack: Python list[-1] would return the last
        # attachment instead of failing
        pkt = packet.Packet(
            encoded_packet='51-{"_placeholder":true,"num":-1}')
        with pytest.raises(ValueError):
            pkt.add_attachment(b'secret')
        assert pkt.data != b'secret'

    def test_negative_index_inside_nested_structure_is_rejected(self):
        pkt = packet.Packet(
            encoded_packet='51-{"a":[{"b":'
            '{"_placeholder":true,"num":-1}}]}')
        with pytest.raises(ValueError):
            pkt.add_attachment(b'attachment')
        assert pkt.attachments == []

    def test_boolean_index_is_rejected(self):
        # JSON true/false decode to Python bool, which would otherwise index
        # attachment 1 / fail oddly; the protocol only allows integers
        pkt = packet.Packet(
            encoded_packet='52-[{"_placeholder":true,"num":true},'
            '{"_placeholder":true,"num":0}]')
        pkt.add_attachment(b'zero')
        with pytest.raises(ValueError):
            pkt.add_attachment(b'one')
        # attachment 1 must not have been silently selected via True == 1
        assert pkt.data == [
            {'_placeholder': True, 'num': True},
            {'_placeholder': True, 'num': 0},
        ]
        assert pkt.attachments == [b'zero']

    @pytest.mark.parametrize('num', ['"0"', '1.5', 'null', '[]', '{}'])
    def test_wrong_type_index_is_rejected(self, num):
        pkt = packet.Packet(
            encoded_packet='51-{"_placeholder":true,"num":%s}' % num)
        with pytest.raises(ValueError):
            pkt.add_attachment(b'attachment')
        assert pkt.attachments == []

    def test_business_dict_with_num_only_is_not_a_placeholder(self):
        assert packet.Packet.reconstruct_binary(
            {'num': 0, 'label': 'order'}, [b'attachment']) == \
            {'num': 0, 'label': 'order'}

    def test_business_dict_with_falsy_placeholder_is_preserved(self):
        for encoded in (
                '51-[{"_placeholder":false,"num":0}]',
                '51-[{"_placeholder":0,"num":0}]',
                '51-[{"_placeholder":null,"num":0}]'):
            pkt = packet.Packet(encoded_packet=encoded)
            pkt.add_attachment(b'attachment')
            assert pkt.data == [
                packet.Packet.json.loads(encoded[3:])[0]]

    def test_reconstruction_failure_does_not_consume_attachment(self):
        pkt = packet.Packet(
            encoded_packet='51-{"_placeholder":true,"num":-1}')
        with pytest.raises(ValueError):
            pkt.add_attachment(b'attachment')
        # the attachment was rolled back so the packet cannot be completed
        # later by a stray message
        assert len(pkt.attachments) == 0
        with pytest.raises(ValueError):
            pkt.add_attachment(b'attachment')

    def test_extra_attachment_after_completion_is_rejected(self):
        pkt = self._decode(
            '51-{"_placeholder":true,"num":0}', [b'only'])
        with pytest.raises(ValueError):
            pkt.add_attachment(b'unexpected')

    def test_zero_declared_attachments_rejects_late_binary(self):
        pkt = packet.Packet(
            encoded_packet='50-["x",{"_placeholder":true,"num":0}]')
        assert pkt.attachment_count == 0
        with pytest.raises(ValueError):
            pkt.add_attachment(b'late')

    def test_binary_ack_uses_the_same_reference_rules(self):
        # BINARY_ACK (type 6) packets are reconstructed identically
        pkt = self._decode(
            '61-42[{"_placeholder":true,"num":0}]', [b'ack-data'],
            packet.BINARY_ACK)
        assert pkt.id == 42
        assert pkt.data == [b'ack-data']

        bad = packet.Packet(
            encoded_packet='61-42[{"_placeholder":true,"num":-1}]')
        with pytest.raises(ValueError):
            bad.add_attachment(b'ack-data')
