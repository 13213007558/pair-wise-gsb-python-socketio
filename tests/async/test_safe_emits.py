"""Tests for the opt-in ``safe_emits`` serialization mode.

The tests in this module install a barrier at the Engine.IO ``send_packet``
boundary, i.e. at the point where each individual Engine.IO packet is handed
to the connection. The barrier lets the tests force the packets of two
concurrent binary events to interleave and then verify, on the receiver side,
that every binary attachment is attributed to the event it belongs to.
"""
import asyncio
from unittest import mock

import pytest

from engineio import packet as eio_packet
from socketio import async_manager
from socketio import async_server
from socketio import packet as sio_packet


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

class PacketBarrier:
    """Block every Engine.IO packet until the test explicitly releases it.

    A barrier is installed per Engine.IO connection. Releasing a barrier lets
    *all* the packets currently waiting on it flow through and keeps the gate
    open for any later packet, which gives the test deterministic control over
    the interleaving of two emits without ever stalling a late packet.
    """

    def __init__(self):
        self._gate = asyncio.Event()
        self._gate.set()
        self._held = asyncio.Event()
        self._held.set()
        self._hold_predicate = None
        self.arrived_count = 0

    def block(self):
        # reset the arrival counter so wait_for_arrivals() only observes
        # packets that reach the barrier after blocking starts, ignoring any
        # handshake traffic produced while connecting
        self.arrived_count = 0
        self._gate.clear()

    def hold_if(self, predicate):
        """Hold every packet for which ``predicate(encoded_packet)`` is True.

        Other packets flow through immediately. Used to block only binary
        attachments while letting event headers pass, which deterministically
        reproduces a header/header interleaving.
        """
        # reset the arrival counter so only packets after this point count
        self.arrived_count = 0
        self._held.clear()
        self._hold_predicate = predicate

    async def release(self):
        self._gate.set()
        self._held.set()
        self._hold_predicate = None
        await asyncio.sleep(0)

    async def wait_if_blocked(self, encoded):
        self.arrived_count += 1
        if self._hold_predicate is not None and self._hold_predicate(encoded):
            await self._held.wait()
        else:
            await self._gate.wait()

    async def wait_for_arrivals(self, count):
        # poll with a zero sleep instead of waiting on an Event: this keeps
        # yielding control until the just-scheduled emit coroutine runs deep
        # enough to push the expected number of packets into the barrier,
        # regardless of how many coroutine hops that takes
        while self.arrived_count < count:
            await asyncio.sleep(0)


class Receiver:
    """Minimal Socket.IO client decoder.

    Feeds it the raw Engine.IO MESSAGE packets captured on a connection and it
    reconstructs the events exactly like a real client, pairing each binary
    attachment with the event header that announced it.
    """

    def __init__(self):
        self.events = []
        self._pending = None
        # every ordering rule the receiver had to enforce while decoding is
        # recorded here, instead of raised, so a corrupted stream can still be
        # fully observed by the test
        self.violations = []

    def feed(self, encoded):
        if isinstance(encoded, (bytes, bytearray)):
            if self._pending is None:
                self.violations.append(
                    'attachment without a binary event header')
                return
            complete = self._pending.add_attachment(bytes(encoded))
            if complete:
                self.events.append((self._pending.packet_type,
                                    self._pending.data))
                self._pending = None
        else:
            if self._pending is not None:
                self.violations.append(
                    'new event header while attachments were still expected')
                self._pending = None
            if encoded[0] == '4':
                # strip the Engine.IO MESSAGE packet type prefix
                encoded = encoded[1:]
            pkt = sio_packet.Packet(encoded_packet=encoded)
            if pkt.attachment_count:
                self._pending = pkt
            else:
                self.events.append((pkt.packet_type, pkt.data))

    def names(self):
        return [data[0] for _, data in self.events
                if isinstance(data, list) and data]

    def payload(self, name):
        for _, data in self.events:
            if isinstance(data, list) and data and data[0] == name:
                return data[1] if len(data) > 1 else None
        raise KeyError(name)


class Harness:
    """Wires a real AsyncServer to mocked Engine.IO connections and captures
    every packet that would be written on each connection."""

    def __init__(self, safe_emits, fail_on=None, eio_sids=('e1', 'e2')):
        self.eio_sids = list(eio_sids)
        self.captured = {sid: [] for sid in eio_sids}
        self.receivers = {sid: Receiver() for sid in eio_sids}
        self.barriers = {sid: PacketBarrier() for sid in eio_sids}
        # number of send failures still to inject per connection, so a failure
        # does not poison every subsequent emit on the same connection
        fail_on = fail_on or {}
        if isinstance(fail_on, dict):
            self.fail_remaining = dict(fail_on)
        else:
            self.fail_remaining = {sid: 1 for sid in fail_on}
        self.server = async_server.AsyncServer(safe_emits=safe_emits)
        self._patch_eio()

    def _patch_eio(self):
        eio = self.server.eio

        async def fake_send_packet(sid, pkt):
            barrier = self.barriers[sid]
            encoded = pkt.encode()
            await barrier.wait_if_blocked(encoded)
            if pkt.packet_type != eio_packet.MESSAGE:
                return
            # binary attachments travel as bare bytes with no Socket.IO type
            # prefix; everything else is a string carrying the Socket.IO type
            if isinstance(encoded, (bytes, bytearray)):
                is_event_traffic = True
                sio_type = sio_packet.BINARY_EVENT
            else:
                sio_type = int(encoded[1])
                is_event_traffic = sio_type in (
                    sio_packet.EVENT, sio_packet.ACK,
                    sio_packet.BINARY_EVENT, sio_packet.BINARY_ACK)
            if is_event_traffic and self.fail_remaining.get(sid, 0) > 0:
                self.fail_remaining[sid] -= 1
                raise ConnectionError(f'send failed on {sid}')
            if not is_event_traffic:
                return
            self.captured[sid].append(encoded)
            self.receivers[sid].feed(pkt.data)

        eio.send_packet = mock.AsyncMock(side_effect=fake_send_packet)

        async def fake_send(sid, data):
            await fake_send_packet(
                sid, eio_packet.Packet(eio_packet.MESSAGE, data))

        eio.send = mock.AsyncMock(side_effect=fake_send)
        eio.generate_id.side_effect = (str(i) for i in range(1, 100))
        eio.create_event = asyncio.Event

    async def connect(self, eio_sid, rooms=()):
        await self.server._handle_eio_connect(eio_sid, {})
        await self.server._handle_eio_message(eio_sid, '0')
        sid = self.server.manager.sid_from_eio_sid(eio_sid, '/')
        for room in rooms:
            await self.server.enter_room(sid, room)
        return sid

    def block_all(self):
        for barrier in self.barriers.values():
            barrier.block()

    async def release(self, *sids):
        for sid in sids:
            await self.barriers[sid].release()

    async def wait_arrivals(self, sid, count):
        await self.barriers[sid].wait_for_arrivals(count)

    def assert_no_violations(self):
        for sid, receiver in self.receivers.items():
            assert not receiver.violations, \
                f'{sid}: {receiver.violations}'
            assert receiver._pending is None


SCREENSHOT = b'\x89PNG screenshot-bytes' * 8
AUDIT = b'AUDIT-RESULT-payload' * 8


@pytest.fixture
def patched_eio():
    with mock.patch(
        'socketio.base_server.engineio.AsyncServer'
    ) as eio_cls:
        eio_cls.return_value.generate_id.side_effect = \
            (str(i) for i in range(1, 100))
        yield eio_cls


# ---------------------------------------------------------------------------
# Normal concurrency
# ---------------------------------------------------------------------------

class TestSafeEmitsConcurrency:
    async def test_unsafe_interleaves_binary_events(self, patched_eio):
        """Reproduce the defect: without safe_emits two binary events can be
        interleaved, so an attachment lands on the wrong event."""
        harness = Harness(safe_emits=False)
        await harness.connect('e1')
        # let every event header flow through, but hold both binary
        # attachments at the barrier, forcing the two headers to be adjacent
        barrier = harness.barriers['e1']
        barrier.hold_if(lambda encoded: isinstance(encoded, (bytes, bytearray)))

        first = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT))
        second = asyncio.ensure_future(harness.server.emit(
            'audit', AUDIT))

        # wait until both headers were delivered and both attachments held
        await harness.wait_arrivals('e1', 4)
        # at this point the receiver has two open binary events and no
        # attachment yet: a guaranteed interleaving
        assert len(harness.captured['e1']) == 2
        await harness.release('e1')
        await asyncio.gather(first, second)

        encoded = harness.captured['e1']
        headers = [item for item in encoded if isinstance(item, str)]
        attachments = [item for item in encoded
                       if isinstance(item, (bytes, bytearray))]
        # the two headers were enqueued before either attachment
        assert len(headers) == 2 and len(attachments) == 2
        assert {h.split('["', 1)[1].split('"', 1)[0] for h in headers} == \
            {'screenshot', 'audit'}
        # the receiver flags the malformed stream: a second header arrived
        # while the first event was still waiting for its attachment
        assert harness.receivers['e1'].violations

    async def test_safe_keeps_events_contiguous(self, patched_eio):
        """With safe_emits the header and attachment of each event are always
        contiguous, even with the barrier forcing interleaving."""
        harness = Harness(safe_emits=True)
        await harness.connect('e1')
        harness.block_all()

        first = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT))
        second = asyncio.ensure_future(harness.server.emit(
            'audit', AUDIT))

        # under safe_emits the connection lock lets only one event reach the
        # barrier while blocked; the second is still waiting for the lock
        await harness.wait_arrivals('e1', 1)
        await harness.release('e1')
        await asyncio.gather(first, second)

        encoded = harness.captured['e1']
        # two complete, contiguous events; order between them is not promised
        events = harness.receivers['e1'].events
        event_names = [data[0] for _, data in events
                       if isinstance(data, list) and data]
        event_payloads = {data[0]: data[1] for _, data in events
                          if isinstance(data, list) and len(data) > 1}
        assert event_payloads.get('screenshot') == SCREENSHOT
        assert event_payloads.get('audit') == AUDIT
        assert sorted(event_names) == ['audit', 'screenshot']
        # every attachment sits directly after its own header
        for index, item in enumerate(encoded):
            if isinstance(item, str) and item.startswith('451-'):
                assert isinstance(encoded[index + 1], (bytes, bytearray))

    async def test_different_connections_run_in_parallel(self, patched_eio):
        """Two emits with disjoint recipients must not block on each other."""
        harness = Harness(safe_emits=True, eio_sids=('e1', 'e2'))
        sid1 = await harness.connect('e1')
        sid2 = await harness.connect('e2')

        # block only e1 inside its send; e2's barrier stays open so its emit
        # must complete independently, proving the two connections do not
        # serialize on each other
        harness.barriers['e1'].block()
        task1 = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, to=sid1))
        await harness.wait_arrivals('e1', 1)
        task2 = asyncio.ensure_future(harness.server.emit(
            'audit', AUDIT, to=sid2))
        await asyncio.wait_for(task2, timeout=2)
        assert harness.receivers['e2'].payload('audit') == AUDIT
        await harness.release('e1')
        await asyncio.wait_for(task1, timeout=2)

        assert harness.receivers['e1'].payload('screenshot') == SCREENSHOT
        harness.assert_no_violations()

    async def test_overlapping_broadcasts_no_intersection_interleave(
            self, patched_eio):
        """Two room broadcasts sharing a connection cannot interleave on the
        shared connection, while their exclusive recipients proceed."""
        harness = Harness(safe_emits=True, eio_sids=('e1', 'e2', 'e3'))
        sid1 = await harness.connect('e1', rooms=('room_a', 'room_b'))
        sid2 = await harness.connect('e2', rooms=('room_a',))
        sid3 = await harness.connect('e3', rooms=('room_b',))
        assert sid1 != sid2 != sid3
        harness.block_all()

        broadcast_a = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, room='room_a'))
        broadcast_b = asyncio.ensure_future(harness.server.emit(
            'audit', AUDIT, room='room_b'))

        # both broadcasts now compete for the connections. Wait for the first
        # packet to reach the shared connection e1; whichever broadcast wins
        # its lock holds e1 for the full event, so the two events cannot
        # interleave there. Exclusive recipients e2/e3 are unconstrained.
        await harness.wait_arrivals('e1', 1)
        await harness.release('e1', 'e2', 'e3')
        await asyncio.gather(broadcast_a, broadcast_b)

        # e1 is in both rooms: its two events must each be contiguous
        shared = harness.captured['e1']
        receiver = harness.receivers['e1']
        assert sorted(receiver.names()) == ['audit', 'screenshot']
        assert receiver.payload('screenshot') == SCREENSHOT
        assert receiver.payload('audit') == AUDIT
        for index, item in enumerate(shared):
            if isinstance(item, str) and item.startswith('451-'):
                assert isinstance(shared[index + 1], (bytes, bytearray))
        # exclusive recipients each got exactly their own event
        assert harness.receivers['e2'].names() == ['screenshot']
        assert harness.receivers['e3'].names() == ['audit']

    async def test_ack_event_is_serialized(self, patched_eio):
        """Events requiring an acknowledgement go through the same lock and
        keep their attachments contiguous."""
        harness = Harness(safe_emits=True)
        sid = await harness.connect('e1')
        harness.block_all()

        async def callback(*args):
            pass

        first = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, to=sid, callback=callback))
        second = asyncio.ensure_future(harness.server.emit(
            'audit', AUDIT, to=sid))
        # under safe_emits only one event can reach the blocked connection
        await harness.wait_arrivals('e1', 1)
        await harness.release('e1')
        await asyncio.gather(first, second)

        receiver = harness.receivers['e1']
        assert sorted(receiver.names()) == ['audit', 'screenshot']
        assert receiver.payload('screenshot') == SCREENSHOT
        # the ack event's binary header carries an ack id between the
        # attachment count and the JSON payload, e.g. 451-1["screenshot",...]
        headers = [item for item in harness.captured['e1']
                   if isinstance(item, str) and 'screenshot' in item]
        assert headers and headers[0].startswith('451-')
        after_dash = headers[0].split('-', 1)[1]
        assert after_dash[0].isdigit()
        assert '["screenshot",{"_placeholder":true,"num":0}]' in after_dash
        harness.assert_no_violations()

    async def test_direct_emit_send_and_call(self, patched_eio):
        """emit(), send() and call() all serialize on the connection."""
        harness = Harness(safe_emits=True)
        sid = await harness.connect('e1')

        await harness.server.send(SCREENSHOT, to=sid)
        await harness.server.emit('audit', AUDIT, to=sid)
        receiver = harness.receivers['e1']
        assert receiver.names() == ['message', 'audit']
        assert receiver.payload('message') == SCREENSHOT
        assert receiver.payload('audit') == AUDIT


# ---------------------------------------------------------------------------
# Send errors
# ---------------------------------------------------------------------------

class TestSafeEmitsErrors:
    async def test_send_exception_other_recipients_complete(
            self, patched_eio):
        """A failure on one connection must not strand half events elsewhere;
        the exception propagates after every recipient send settled."""
        harness = Harness(
            safe_emits=True, fail_on={'e2'}, eio_sids=('e1', 'e2'))
        await harness.connect('e1', rooms=('room',))
        await harness.connect('e2', rooms=('room',))

        with pytest.raises(ConnectionError):
            await harness.server.emit('screenshot', SCREENSHOT, room='room')

        # the healthy connection still received a complete event
        assert harness.receivers['e1'].payload('screenshot') == SCREENSHOT
        # the failed connection never accumulated a dangling attachment
        assert len(harness.captured['e2']) == 0

    async def test_exception_does_not_leave_locked(self, patched_eio):
        """After a failed broadcast the connection lock is released, so the
        next emit still works."""
        harness = Harness(safe_emits=True, fail_on={'e1'})
        sid = await harness.connect('e1')
        with pytest.raises(ConnectionError):
            await harness.server.emit('screenshot', SCREENSHOT, to=sid)

        await harness.server.emit('audit', AUDIT, to=sid)
        assert harness.receivers['e1'].payload('audit') == AUDIT
        lock = harness.server.manager._send_locks['e1']
        assert not lock.locked()


# ---------------------------------------------------------------------------
# Disconnects
# ---------------------------------------------------------------------------

class TestSafeEmitsDisconnect:
    async def test_disconnect_during_pending_emit(self, patched_eio):
        """A disconnect that races a queued emit removes the client and its
        lock. The emit either completes before the disconnect or resolves
        against a gone connection, but never leaves a half event behind."""
        harness = Harness(safe_emits=True)
        sid = await harness.connect('e1')

        emit_task = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, to=sid))
        # disconnect concurrently, before the emit necessarily drains
        await asyncio.sleep(0)
        await harness.server.disconnect(sid)
        await asyncio.wait_for(emit_task, timeout=2)

        # the lock for the dead connection is gone and no dangling attachment
        # state remains on either side
        assert 'e1' not in harness.server.manager._send_locks
        assert harness.server._binary_packet == {}
        receiver = harness.receivers['e1']
        for _, data in receiver.events:
            if isinstance(data, list) and data and data[0] == 'screenshot':
                assert data[1] == SCREENSHOT
        assert receiver._pending is None

    async def test_disconnect_clears_lock_after_inflight_event(
            self, patched_eio):
        """Once a queued event drained and the client then disconnects, the
        lock registry does not retain an entry for the dead connection."""
        harness = Harness(safe_emits=True)
        sid = await harness.connect('e1')
        await harness.server.emit('audit', AUDIT, to=sid)
        assert 'e1' in harness.server.manager._send_locks
        await harness.server.disconnect(sid)
        assert 'e1' not in harness.server.manager._send_locks


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------

class TestSafeEmitsCancellation:
    async def test_cancel_while_waiting_for_lock_sends_nothing(
            self, patched_eio):
        """Cancellation before the send lock is acquired must produce zero
        output: no header and no attachment."""
        harness = Harness(safe_emits=True)
        sid = await harness.connect('e1')
        harness.block_all()
        holder = asyncio.ensure_future(harness.server.emit(
            'audit', AUDIT, to=sid))
        await harness.wait_arrivals('e1', 1)

        waiter = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, to=sid))
        await asyncio.sleep(0)
        assert not waiter.done()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        await harness.release('e1')
        await holder
        captured = harness.captured['e1']
        # only the holder's audit event (header + attachment) went out; there
        # is no screenshot header and no screenshot attachment
        assert not any(item == SCREENSHOT for item in captured)
        assert not any(isinstance(item, str) and 'screenshot' in item
                       for item in captured)
        assert harness.receivers['e1'].names() == ['audit']
        assert harness.receivers['e1'].payload('audit') == AUDIT

    async def test_cancel_after_header_drains_full_event(
            self, patched_eio):
        """If the event header was already enqueued when the caller is
        cancelled, the whole event must still be delivered, never a half
        message waiting for the next emit."""
        harness = Harness(safe_emits=True)
        sid = await harness.connect('e1')
        harness.block_all()
        emit_task = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, to=sid))
        await harness.wait_arrivals('e1', 1)

        emit_task.cancel()
        await harness.release('e1')
        with pytest.raises(asyncio.CancelledError):
            await emit_task

        # the receiver got a complete, parseable event despite cancellation
        assert harness.receivers['e1'].events == [
            (sio_packet.BINARY_EVENT, ['screenshot', SCREENSHOT])]
        # a subsequent emit starts from a clean state (no pending attachment)
        await harness.server.emit('audit', AUDIT, to=sid)
        assert harness.receivers['e1'].events[-1] == (
            sio_packet.BINARY_EVENT, ['audit', AUDIT])
        assert harness.server._binary_packet == {}

    async def test_cancel_broadcast_drains_shared_connection(
            self, patched_eio):
        """Cancelling a broadcast after headers went out still completes the
        event on every shared recipient."""
        harness = Harness(safe_emits=True, eio_sids=('e1', 'e2'))
        await harness.connect('e1', rooms=('room',))
        await harness.connect('e2', rooms=('room',))
        harness.block_all()
        broadcast = asyncio.ensure_future(harness.server.emit(
            'screenshot', SCREENSHOT, room='room'))
        await harness.wait_arrivals('e1', 1)
        await harness.wait_arrivals('e2', 1)

        broadcast.cancel()
        await harness.release('e1', 'e2')
        with pytest.raises(asyncio.CancelledError):
            await broadcast

        for sid in ('e1', 'e2'):
            assert harness.receivers[sid].events == [
                (sio_packet.BINARY_EVENT, ['screenshot', SCREENSHOT])]


# ---------------------------------------------------------------------------
# Manager level: lock lifecycle and ordering
# ---------------------------------------------------------------------------

class TestSafeEmitsManager:
    def setup_method(self):
        mock_server = mock.MagicMock()
        mock_server.safe_emits = True
        mock_server._send_packet = mock.AsyncMock()
        mock_server._send_eio_packet = mock.AsyncMock()
        mock_server.eio.generate_id.side_effect = \
            (str(i) for i in range(1, 100))
        mock_server.packet_class = sio_packet.Packet
        self.bm = async_manager.AsyncManager()
        self.bm.set_server(mock_server)
        self.bm.initialize()

    async def _connect(self, eio_sid, namespace='/'):
        return await self.bm.connect(eio_sid, namespace)

    async def test_locks_created_and_removed_on_disconnect(self):
        sid = await self._connect('123')
        await self.bm.emit('evt', b'data', namespace='/', room=sid)
        assert '123' in self.bm._send_locks
        await self.bm.disconnect(sid, '/')
        assert '123' not in self.bm._send_locks

    async def test_safe_emit_uses_send_packet_for_binary(self):
        sid = await self._connect('123')
        await self.bm.emit('evt', b'data', namespace='/', room=sid)
        # header + attachment, both via _send_eio_packet, in order
        calls = self.bm.server._send_eio_packet.await_args_list
        assert len(calls) == 2
        assert calls[0][0][0] == '123'
        assert calls[1][0][0] == '123'
        assert calls[0][0][1].encode().startswith('451-')
        assert calls[1][0][1].encode() == b'data'

    async def test_safe_emit_callback_uses_send_packet(self):
        sid = await self._connect('123')
        await self.bm.emit('evt', {'k': 'v'}, namespace='/', room=sid,
                           callback=lambda *a: None)
        self.bm.server._send_packet.assert_awaited_once()
        args = self.bm.server._send_packet.await_args
        assert args[0][0] == '123'
        assert args[0][1].id is not None

    async def test_legacy_path_unchanged_when_disabled(self):
        self.bm.server.safe_emits = False
        sid = await self._connect('123')
        await self.bm.emit('evt', b'data', namespace='/', room=sid)
        # legacy behavior creates one task per packet, still works
        assert self.bm.server._send_eio_packet.await_count == 2


class TestSafeEmitsOption:
    """The server-level ``safe_emits`` option and its lock lifecycle."""

    async def test_disabled_by_default(self, patched_eio):
        server = async_server.AsyncServer()
        assert server.safe_emits is False
        assert server.manager._send_locks == {}

    async def test_enabled_propagates_to_manager(self, patched_eio):
        server = async_server.AsyncServer(safe_emits=True)
        assert server.safe_emits is True
        assert isinstance(server.manager._send_locks, dict)

    async def test_lock_survives_multiple_namespaces_until_disconnect(
            self, patched_eio):
        harness = Harness(safe_emits=True)
        await harness.connect('e1')
        # connect the same Engine.IO connection to a second namespace
        await harness.server.manager.connect('e1', '/foo')
        sid = harness.server.manager.sid_from_eio_sid('e1', '/')
        await harness.server.emit('audit', AUDIT, to=sid)
        assert 'e1' in harness.server.manager._send_locks
        # disconnecting one namespace must not drop the shared lock while the
        # other namespace is still connected
        await harness.server.manager.disconnect(sid, '/')
        assert 'e1' in harness.server.manager._send_locks
        sid_foo = harness.server.manager.sid_from_eio_sid('e1', '/foo')
        await harness.server.manager.disconnect(sid_foo, '/foo')
        assert 'e1' not in harness.server.manager._send_locks
