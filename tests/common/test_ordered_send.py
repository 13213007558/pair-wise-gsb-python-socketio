import threading
import time
from unittest import mock

import pytest

from socketio import packet
from socketio import server


class ControlledSender:
    """Replacement for eio.send() and eio.send_packet() that records the
    frames it receives and can pause the send of a selected frame, so that
    interleavings between concurrent senders can be reproduced in a
    deterministic way."""
    def __init__(self):
        self.frames = []  # (eio_sid, data) pairs, in the order they were sent
        self.condition = threading.Condition()
        self.pause_marker = None  # frame data whose send should pause
        self.pause_sid = None  # or, an eio_sid whose sends should pause
        self.paused = threading.Event()
        self.resume = threading.Event()

    def __call__(self, eio_sid, data):
        if hasattr(data, 'data'):
            # unwrap Engine.IO packets to their data payload
            data = data.data
        if not self.resume.is_set() and (
                data == self.pause_marker or eio_sid == self.pause_sid):
            self.paused.set()
            self.resume.wait(5)
        with self.condition:
            self.frames.append((eio_sid, data))
            self.condition.notify_all()

    def sent_data(self, eio_sid=None):
        return [data for sid, data in self.frames
                if eio_sid is None or sid == eio_sid]


def receive(frames):
    """Simulate the receiving client. Returns the list of reconstructed
    (event, args) tuples, plus a half-received packet if the stream ended
    in the middle of one."""
    events = []
    pending = None
    for _, data in frames:
        if pending is not None:
            if pending.add_attachment(data):
                events.append((pending.data[0], pending.data[1:]))
                pending = None
        else:
            pkt = packet.Packet(encoded_packet=data)
            if pkt.packet_type in (packet.BINARY_EVENT, packet.BINARY_ACK):
                pending = pkt
            else:
                events.append((pkt.data[0], pkt.data[1:]))
    return events, pending


def encode(event, data, **kwargs):
    frames = packet.Packet(packet.EVENT, data=[event] + data,
                           **kwargs).encode()
    return frames if isinstance(frames, list) else [frames]


def make_server(sender, **kwargs):
    with mock.patch('socketio.server.engineio.Server') as mock_eio:
        mock_eio.return_value.generate_id.side_effect = (
            [str(i) for i in range(1, 100)])
        s = server.Server(**kwargs)
    s.eio.send = sender
    s.eio.send_packet = sender
    return s


class TestOrderedSend:
    def test_concurrent_emits_interleave_without_ordered_send(self):
        # reproduces the interleaving problem when ordered_send is disabled
        sender = ControlledSender()
        s = make_server(sender)
        sid = s.manager.connect('123', '/')
        sender.pause_marker = b'a1'

        t = threading.Thread(target=s.emit,
                             args=('evA', {'bin': b'a1'}), kwargs={'to': sid})
        t.start()
        assert sender.paused.wait(5)
        # the first event is paused with only its header sent; a second
        # event sent now interleaves its frames with it
        s.emit('evB', {'bin': b'b1'}, to=sid)
        sender.resume.set()
        t.join(5)

        expected_a = encode('evA', [{'bin': b'a1'}])
        expected_b = encode('evB', [{'bin': b'b1'}])
        assert sender.sent_data() == [
            expected_a[0], expected_b[0], expected_b[1], expected_a[1]]
        # no send locks were used
        assert s._send_locks == {}

    def test_ordered_send_serializes_concurrent_emits(self):
        sender = ControlledSender()
        s = make_server(sender, ordered_send=True)
        sid = s.manager.connect('123', '/')
        sender.pause_marker = b'a1'

        t_a = threading.Thread(target=s.emit,
                               args=('evA', {'bin': b'a1'}),
                               kwargs={'to': sid})
        t_a.start()
        assert sender.paused.wait(5)
        t_b = threading.Thread(target=s.emit,
                               args=('evB', {'bin': b'b1'}),
                               kwargs={'to': sid})
        t_b.start()
        time.sleep(0.1)
        # the second event cannot start while the first one is half sent
        assert sender.sent_data() == encode('evA', [{'bin': b'a1'}])[:1]
        sender.resume.set()
        t_a.join(5)
        t_b.join(5)

        assert sender.sent_data() == (
            encode('evA', [{'bin': b'a1'}]) + encode('evB', [{'bin': b'b1'}]))
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}]),
                          ('evB', [{'bin': b'b1'}])]

    def test_ordered_send_across_namespaces(self):
        sender = ControlledSender()
        s = make_server(sender, ordered_send=True, namespaces=['/', '/chat'])
        sid = s.manager.connect('123', '/')
        sid_chat = s.manager.connect('123', '/chat')
        sender.pause_marker = b'a1'

        t_a = threading.Thread(target=s.emit,
                               args=('evA', {'bin': b'a1'}),
                               kwargs={'to': sid})
        t_a.start()
        assert sender.paused.wait(5)
        t_c = threading.Thread(target=s.emit,
                               args=('evC', {'bin': b'c1'}),
                               kwargs={'to': sid_chat, 'namespace': '/chat'})
        t_c.start()
        time.sleep(0.1)
        assert sender.sent_data() == encode('evA', [{'bin': b'a1'}])[:1]
        sender.resume.set()
        t_a.join(5)
        t_c.join(5)

        # the lock is per Engine.IO connection, so packets are serialized
        # even when they are sent to different namespaces
        expected = encode('evA', [{'bin': b'a1'}])
        expected += encode('evC', [{'bin': b'c1'}], namespace='/chat')
        assert sender.sent_data() == expected
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}]),
                          ('evC', [{'bin': b'c1'}])]

    def test_ordered_send_broadcast(self):
        sender = ControlledSender()
        s = make_server(sender, ordered_send=True)
        s.manager.connect('123', '/')
        s.manager.connect('456', '/')
        sender.pause_marker = b'a1'

        t_a = threading.Thread(target=s.emit, args=('evA', {'bin': b'a1'}))
        t_a.start()
        assert sender.paused.wait(5)
        t_b = threading.Thread(target=s.emit, args=('evB', {'bin': b'b1'}))
        t_b.start()
        time.sleep(0.1)
        # the second broadcast cannot send its first frame to a client that
        # is in the middle of the first broadcast
        assert sender.sent_data() == encode('evA', [{'bin': b'a1'}])[:1]
        sender.resume.set()
        t_a.join(5)
        t_b.join(5)

        # each client received the frames of each event contiguously
        expected = encode('evA', [{'bin': b'a1'}])
        expected += encode('evB', [{'bin': b'b1'}])
        assert sender.sent_data('123') == expected
        assert sender.sent_data('456') == expected
        for eio_sid in ['123', '456']:
            events, pending = receive(
                [f for f in sender.frames if f[0] == eio_sid])
            assert pending is None
            assert events == [('evA', [{'bin': b'a1'}]),
                              ('evB', [{'bin': b'b1'}])]

    def test_slow_client_does_not_block_other_clients(self):
        sender = ControlledSender()
        s = make_server(sender, ordered_send=True)
        sid_x = s.manager.connect('123', '/')
        sid_y = s.manager.connect('456', '/')
        sender.pause_sid = '123'

        t_a = threading.Thread(target=s.emit,
                               args=('evA', {'bin': b'a1'}),
                               kwargs={'to': sid_x})
        t_a.start()
        assert sender.paused.wait(5)
        # client 123 is stuck mid-packet, but a send to client 456 must
        # still go through
        s.emit('evB', {'bin': b'b1'}, to=sid_y)
        assert sender.sent_data('456') == encode('evB', [{'bin': b'b1'}])
        assert sender.sent_data('123') == []
        sender.resume.set()
        t_a.join(5)
        assert sender.sent_data('123') == encode('evA', [{'bin': b'a1'}])

    def test_ordered_send_binary_ack(self):
        sender = ControlledSender()
        s = make_server(sender, ordered_send=True, async_handlers=False)
        s.manager.connect('123', '/')

        @s.on('get')
        def on_get(sid, data):
            return {'bin': data}

        sender.pause_marker = b'a1'
        t_a = threading.Thread(target=s._handle_event,
                               args=('123', '/', 1, ['get', b'a1']))
        t_a.start()
        assert sender.paused.wait(5)
        t_b = threading.Thread(target=s._handle_event,
                               args=('123', '/', 2, ['get', b'b1']))
        t_b.start()
        time.sleep(0.1)
        sender.resume.set()
        t_a.join(5)
        t_b.join(5)

        expected_a = packet.Packet(
            packet.ACK, namespace='/', id=1, data=[{'bin': b'a1'}]).encode()
        expected_b = packet.Packet(
            packet.ACK, namespace='/', id=2, data=[{'bin': b'b1'}]).encode()
        assert sender.sent_data() == expected_a + expected_b

    def test_send_locks_removed_on_disconnect(self):
        sender = ControlledSender()
        s = make_server(sender, ordered_send=True)
        sid = s.manager.connect('123', '/')
        s.emit('ev', {'bin': b'a1'}, to=sid)
        assert '123' in s._send_locks
        s._handle_eio_disconnect('123', s.reason.CLIENT_DISCONNECT)
        assert s._send_locks == {}

    def test_interrupted_send_completes_packet(self):
        # a send that fails in the middle of a packet still delivers the
        # remaining frames, so that the client is not left with half a
        # packet
        class FailingSender(ControlledSender):
            failed = False

            def __call__(self, eio_sid, data):
                payload = data.data if hasattr(data, 'data') else data
                if payload == b'a1' and not self.failed:
                    self.failed = True
                    raise RuntimeError('boom')
                super().__call__(eio_sid, data)

        sender = FailingSender()
        s = make_server(sender, ordered_send=True)
        s.manager.connect('123', '/')
        pkt = s.packet_class(packet.EVENT, namespace='/',
                             data=['evA', {'bin': [b'a1', b'a2']}])
        with pytest.raises(RuntimeError):
            s._send_packet('123', pkt)

        expected = packet.Packet(
            packet.EVENT, namespace='/',
            data=['evA', {'bin': [b'a1', b'a2']}]).encode()
        assert sender.sent_data() == expected
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': [b'a1', b'a2']}])]
