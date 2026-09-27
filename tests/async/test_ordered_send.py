import asyncio
from unittest import mock

import pytest

from socketio import async_server
from socketio import packet


class AsyncControlledSender:
    """Replacement for eio.send() and eio.send_packet() that records the
    frames it receives and can pause the send of a selected frame, so that
    interleavings between concurrent senders can be reproduced in a
    deterministic way."""
    def __init__(self):
        self.frames = []  # (eio_sid, data) pairs, in the order they were sent
        self.pause_marker = None  # frame data whose send should pause
        self.pause_sid = None  # or, an eio_sid whose sends should pause
        self.pause_once = False  # pause only on the first matching frame
        self.paused = asyncio.Event()
        self.resume = asyncio.Event()
        self._pause_consumed = False

    async def __call__(self, eio_sid, data):
        if hasattr(data, 'data'):
            # unwrap Engine.IO packets to their data payload
            data = data.data
        if not self.resume.is_set() and (
                data == self.pause_marker or eio_sid == self.pause_sid):
            if not self.pause_once or not self._pause_consumed:
                self._pause_consumed = True
                self.paused.set()
                await self.resume.wait()
        self.frames.append((eio_sid, data))

    def sent_data(self, eio_sid=None):
        return [data for sid, data in self.frames
                if eio_sid is None or sid == eio_sid]


async def wait_for_frames(sender, eio_sid, count, timeout=5):
    async def _wait():
        while len(sender.sent_data(eio_sid)) < count:
            await asyncio.sleep(0.001)
    await asyncio.wait_for(_wait(), timeout)


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
    with mock.patch('socketio.async_server.engineio.AsyncServer') as mock_eio:
        mock_eio.return_value.generate_id.side_effect = (
            [str(i) for i in range(1, 100)])
        s = async_server.AsyncServer(**kwargs)
    s.eio.send = sender
    s.eio.send_packet = sender
    return s


class TestAsyncOrderedSend:
    async def test_concurrent_emits_interleave_without_ordered_send(self):
        # reproduces the interleaving problem when ordered_send is disabled
        sender = AsyncControlledSender()
        s = make_server(sender)
        sid = await s.manager.connect('123', '/')
        sender.pause_marker = b'a1'

        task = asyncio.create_task(
            s.emit('evA', {'bin': b'a1'}, to=sid))
        await asyncio.wait_for(sender.paused.wait(), 5)
        # the first event is paused with only its header sent; a second
        # event sent now interleaves its frames with it
        await s.emit('evB', {'bin': b'b1'}, to=sid)
        sender.resume.set()
        await task

        expected_a = encode('evA', [{'bin': b'a1'}])
        expected_b = encode('evB', [{'bin': b'b1'}])
        assert sender.sent_data() == [
            expected_a[0], expected_b[0], expected_b[1], expected_a[1]]
        # no send locks were used
        assert s._send_locks == {}

    async def test_ordered_send_serializes_concurrent_emits(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True)
        sid = await s.manager.connect('123', '/')
        sender.pause_marker = b'a1'

        task_a = asyncio.create_task(s.emit('evA', {'bin': b'a1'}, to=sid))
        await asyncio.wait_for(sender.paused.wait(), 5)
        task_b = asyncio.create_task(s.emit('evB', {'bin': b'b1'}, to=sid))
        await asyncio.sleep(0.1)
        # the second event cannot start while the first one is half sent
        assert sender.sent_data() == encode('evA', [{'bin': b'a1'}])[:1]
        sender.resume.set()
        await task_a
        await task_b

        assert sender.sent_data() == (
            encode('evA', [{'bin': b'a1'}]) + encode('evB', [{'bin': b'b1'}]))
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}]),
                          ('evB', [{'bin': b'b1'}])]

    async def test_ordered_send_across_namespaces(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True, namespaces=['/', '/chat'])
        sid = await s.manager.connect('123', '/')
        sid_chat = await s.manager.connect('123', '/chat')
        sender.pause_marker = b'a1'

        task_a = asyncio.create_task(s.emit('evA', {'bin': b'a1'}, to=sid))
        await asyncio.wait_for(sender.paused.wait(), 5)
        task_c = asyncio.create_task(
            s.emit('evC', {'bin': b'c1'}, to=sid_chat, namespace='/chat'))
        await asyncio.sleep(0.1)
        assert sender.sent_data() == encode('evA', [{'bin': b'a1'}])[:1]
        sender.resume.set()
        await task_a
        await task_c

        # the lock is per Engine.IO connection, so packets are serialized
        # even when they are sent to different namespaces
        expected = encode('evA', [{'bin': b'a1'}])
        expected += encode('evC', [{'bin': b'c1'}], namespace='/chat')
        assert sender.sent_data() == expected
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}]),
                          ('evC', [{'bin': b'c1'}])]

    async def test_slow_client_does_not_block_broadcast(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True)
        await s.manager.connect('123', '/')
        await s.manager.connect('456', '/')
        sender.pause_sid = '123'

        task = asyncio.create_task(s.emit('evA', {'bin': b'a1'}))
        await asyncio.wait_for(sender.paused.wait(), 5)
        # client 123 is stuck mid-packet, but client 456 must still receive
        # the complete event
        await wait_for_frames(sender, '456', 2)
        assert sender.sent_data('456') == encode('evA', [{'bin': b'a1'}])
        assert sender.sent_data('123') == []
        sender.resume.set()
        await task
        assert sender.sent_data('123') == encode('evA', [{'bin': b'a1'}])

    async def test_cancelled_emit_completes_packet(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True)
        await s.manager.connect('123', '/')
        sender.pause_marker = b'a1'
        sender.pause_once = True

        pkt = s.packet_class(packet.EVENT, namespace='/',
                             data=['evA', {'bin': b'a1'}])
        task = asyncio.create_task(s._send_packet('123', pkt))
        await asyncio.wait_for(sender.paused.wait(), 5)
        # cancel the send while it is paused between the header and the
        # binary attachment
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # the attachment was still sent, so the client is not left with
        # half a packet
        assert sender.sent_data() == encode('evA', [{'bin': b'a1'}])
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}])]

        # and the connection can still be used for the next packet
        await s._send_packet('123', s.packet_class(
            packet.EVENT, namespace='/', data=['evB', {'bin': b'b1'}]))
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}]),
                          ('evB', [{'bin': b'b1'}])]

    async def test_ordered_send_binary_ack(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True, async_handlers=False)
        await s.manager.connect('123', '/')

        @s.on('get')
        async def on_get(sid, data):
            return {'bin': data}

        sender.pause_marker = b'a1'
        task_a = asyncio.create_task(
            s._handle_event('123', '/', 1, ['get', b'a1']))
        await asyncio.wait_for(sender.paused.wait(), 5)
        task_b = asyncio.create_task(
            s._handle_event('123', '/', 2, ['get', b'b1']))
        await asyncio.sleep(0.1)
        sender.resume.set()
        await task_a
        await task_b

        expected_a = packet.Packet(
            packet.ACK, namespace='/', id=1, data=[{'bin': b'a1'}]).encode()
        expected_b = packet.Packet(
            packet.ACK, namespace='/', id=2, data=[{'bin': b'b1'}]).encode()
        assert sender.sent_data() == expected_a + expected_b

    async def test_cancelled_broadcast_completes_packet(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True)
        await s.manager.connect('123', '/')
        sender.pause_marker = b'a1'

        task = asyncio.create_task(s.emit('evA', {'bin': b'a1'}))
        await asyncio.wait_for(sender.paused.wait(), 5)
        # cancel the broadcast while it is paused between the header and
        # the binary attachment
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # the in-flight packet is still completed in the background
        sender.resume.set()
        await wait_for_frames(sender, '123', 2)
        assert sender.sent_data('123') == encode('evA', [{'bin': b'a1'}])
        events, pending = receive(sender.frames)
        assert pending is None
        assert events == [('evA', [{'bin': b'a1'}])]

    async def test_send_locks_removed_on_disconnect(self):
        sender = AsyncControlledSender()
        s = make_server(sender, ordered_send=True)
        sid = await s.manager.connect('123', '/')
        await s.emit('ev', {'bin': b'a1'}, to=sid)
        assert '123' in s._send_locks
        await s._handle_eio_disconnect('123', s.reason.CLIENT_DISCONNECT)
        assert s._send_locks == {}
