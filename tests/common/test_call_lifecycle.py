"""Integration tests for the Server.call() acknowledgement lifecycle.

These tests use a real (non-mocked) client manager together with real
threading events, so they exercise the actual registry bookkeeping: the
number of registered callbacks, the number of times application callbacks are
invoked, and the interaction between completion, timeout, cancellation,
disconnect, reconnect and different namespaces.
"""
import threading
import time
from unittest import mock

import pytest

from socketio import exceptions
from socketio import packet as sio_packet
from socketio import server


def make_server():
    patched = mock.patch(
        'socketio.server.engineio.Server', **{
            'return_value.generate_id.side_effect':
                [str(i) for i in range(1, 1000)],
            'return_value.send_packet': mock.MagicMock(),
        })
    patched.start()
    s = server.Server(async_handlers=True)
    # use a real, controllable event instead of a MagicMock event
    s.eio.create_event = threading.Event
    s.handlers['/'] = {}
    s.handlers['/foo'] = {}
    s._handle_eio_connect('eio', 'environ')
    s._handle_eio_message('eio', '0')
    s._handle_eio_message('eio', '0/foo,')
    return s, patched


def last_ack_id(s):
    encoded = s.eio.send.call_args_list[-1][0][1]
    return sio_packet.Packet(encoded_packet=encoded).id


def sid_for(s, namespace):
    return s.manager.sid_from_eio_sid('eio', namespace)


def ack(ack_id, payload):
    return f'3{ack_id}{payload}'


def wait_until(condition, timeout=2):
    """Block until ``condition()`` is true, polling with a short sleep."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError('condition was not met before the timeout')
        time.sleep(0.005)


class Barrier:
    """A simple thread barrier used to make the tests deterministic."""
    def __init__(self):
        self._event = threading.Event()

    def set(self):
        self._event.set()

    def wait(self, timeout=2):
        assert self._event.wait(timeout)


class TestCallLifecycle:
    def setup_method(self):
        self.s, self._patched = make_server()

    def teardown_method(self):
        self._patched.stop()

    def test_normal_reply_returns_value_and_removes_entry(self):
        result = {}
        barrier = Barrier()

        def worker():
            result['value'] = self.s.call('ping', sid='1', timeout=2)
            barrier.set()

        threading.Thread(target=worker).start()
        # wait until the call has registered its callback
        wait_until(lambda: self.s.manager._pending_ack_count() == 1)
        assert self.s.manager._pending_ack_count('1', '/') == 1
        self.s._handle_eio_message('eio', ack(last_ack_id(self.s),
                                              '["ok",7]'))
        barrier.wait()
        assert result['value'] == ('ok', 7)
        assert self.s.manager._pending_ack_count() == 0

    def test_single_and_empty_argument_unpacking(self):
        for payload, expected in [('["one"]', 'one'), ('[]', None)]:
            result = {}
            barrier = Barrier()

            def worker():
                result['value'] = self.s.call('p', sid='1', timeout=2)
                barrier.set()

            threading.Thread(target=worker).start()
            wait_until(lambda: self.s.manager._pending_ack_count() == 1)
            self.s._handle_eio_message(
                'eio', ack(last_ack_id(self.s), payload))
            barrier.wait()
            assert result['value'] == expected
            assert self.s.manager._pending_ack_count() == 0

    def test_repeated_timeouts_leave_no_callbacks(self):
        iterations = 25
        for _ in range(iterations):
            with pytest.raises(exceptions.TimeoutError):
                self.s.call('p', sid='1', timeout=0.01)
        # stable result across many short timeouts: only the id counters
        # remain, no application callback entries are retained
        assert self.s.manager._pending_ack_count() == 0
        registry = self.s.manager.callbacks['1']['/']
        assert set(registry) == {0}
        assert next(registry[0]) == iterations + 1

    def test_timeout_raises_timeout_error_type(self):
        with pytest.raises(exceptions.TimeoutError):
            self.s.call('p', sid='1', timeout=0.01)

    def test_ack_racing_timeout_returns_ack_value(self):
        original_wait = threading.Event.wait
        state = {'done': False}

        def racing_wait(self_event, timeout=None):
            if not state['done']:
                state['done'] = True
                # the ACK is delivered in the window where the timed wait is
                # expiring, so the timeout and acknowledgement race
                self.s._handle_eio_message(
                    'eio', ack(last_ack_id(self.s), '["late"]'))
                return False
            return original_wait(self_event, timeout)

        threading.Event.wait = racing_wait
        try:
            value = self.s.call('p', sid='1', timeout=0.1)
        finally:
            threading.Event.wait = original_wait
        assert value == 'late'
        assert self.s.manager._pending_ack_count() == 0

    def test_duplicate_ack_invokes_callback_once(self):
        seen = []
        barrier = Barrier()

        def worker():
            seen.append(self.s.call('p', sid='1', timeout=2))
            barrier.set()

        threading.Thread(target=worker).start()
        wait_until(lambda: self.s.manager._pending_ack_count() == 1)
        ack_id = last_ack_id(self.s)
        self.s._handle_eio_message('eio', ack(ack_id, '["first"]'))
        barrier.wait()
        # a repeated / late ACK for the same id must be dropped silently
        self.s._handle_eio_message('eio', ack(ack_id, '["second"]'))
        assert seen == ['first']
        assert self.s.manager._pending_ack_count() == 0

    def test_disconnect_aborts_waiting_call_with_disconnected_error(self):
        result = {}
        barrier = Barrier()

        def worker():
            try:
                self.s.call('p', sid='1', timeout=30)
            except Exception as exc:  # pragma: no branch
                result['exc'] = exc
            barrier.set()

        threading.Thread(target=worker).start()
        wait_until(lambda: self.s.manager._pending_ack_count() == 1)
        self.s._handle_eio_message('eio', '1')  # namespace DISCONNECT
        barrier.wait()
        assert isinstance(result['exc'], exceptions.DisconnectedError)
        assert self.s.manager._pending_ack_count() == 0
        assert '1' not in self.s.manager.callbacks

    def test_disconnect_on_one_namespace_keeps_other_requests(self):
        root_sid = sid_for(self.s, '/')
        foo_sid = sid_for(self.s, '/foo')
        root_result = {}
        barrier = Barrier()

        def worker():
            root_result['value'] = self.s.call(
                'p', sid=root_sid, namespace='/', timeout=2)
            barrier.set()

        threading.Thread(target=worker).start()
        wait_until(
            lambda: self.s.manager._pending_ack_count(root_sid, '/') == 1)
        # disconnecting from /foo must not touch the call waiting on /
        self.s._handle_eio_message('eio', '1/foo,')
        assert self.s.manager._pending_ack_count(root_sid, '/') == 1
        assert foo_sid not in self.s.manager.callbacks
        root_id = last_ack_id(self.s)
        self.s._handle_eio_message('eio', ack(root_id, '["root"]'))
        barrier.wait()
        assert root_result['value'] == 'root'
        assert self.s.manager._pending_ack_count() == 0

    def test_two_concurrent_calls_on_same_sid_are_independent(self):
        root_sid = sid_for(self.s, '/')
        foo_sid = sid_for(self.s, '/foo')
        results = {}
        start = threading.Barrier(3, timeout=2)
        done = threading.Barrier(3, timeout=2)

        def worker(key, sid, namespace):
            start.wait()
            results[key] = self.s.call(
                'p', sid=sid, namespace=namespace, timeout=2)
            done.wait()

        t1 = threading.Thread(
            target=worker, args=('root', root_sid, '/'))
        t2 = threading.Thread(
            target=worker, args=('foo', foo_sid, '/foo'))
        t1.start()
        t2.start()
        start.wait()
        # wait until both requests have registered their callbacks
        wait_until(lambda: self.s.manager._pending_ack_count() == 2)
        # two distinct connections/namespaces, each holding its own ack id;
        # the ids may even coincide without the acks being confused
        root_ids = [i for i in self.s.manager.callbacks[root_sid]['/']
                    if i != 0]
        foo_ids = [i for i in self.s.manager.callbacks[foo_sid]['/foo']
                   if i != 0]
        assert root_ids == foo_ids == [1]
        # resolving the /foo request must not remove the / request
        self.s.manager.trigger_callback(
            foo_sid, '/foo', foo_ids[0], ['foo-ok'])
        assert self.s.manager._pending_ack_count(foo_sid, '/foo') == 0
        assert self.s.manager._pending_ack_count(root_sid, '/') == 1
        self.s.manager.trigger_callback(
            root_sid, '/', root_ids[0], ['root-ok'])
        done.wait()
        assert results == {'root': 'root-ok', 'foo': 'foo-ok'}
        assert self.s.manager._pending_ack_count() == 0

    def test_reconnect_does_not_deliver_old_ack(self):
        with pytest.raises(exceptions.TimeoutError):
            self.s.call('p', sid='1', timeout=0.01)
        # the old connection goes away and a brand new one connects
        self.s._handle_eio_disconnect('eio', 'client')
        assert self.s.manager.callbacks == {}
        self.s._handle_eio_connect('eio2', 'environ')
        self.s._handle_eio_message('eio2', '0')
        # an ACK carrying a recycled id on the new connection is unknown and
        # must not create or resolve any callback entry
        before = self.s.manager._pending_ack_count()
        self.s._handle_eio_message('eio2', '31["ghost"]')
        assert self.s.manager._pending_ack_count() == before == 0

    def test_plain_emit_callback_is_not_given_a_timeout(self):
        # a normal emit(callback=...) registers through the manager and is
        # only resolved by an ACK; call()'s timeout machinery is not involved
        cb = mock.MagicMock()
        self.s.emit('p', data='x', to='1', namespace='/', callback=cb)
        assert self.s.manager._pending_ack_count('1', '/') == 1
        ack_id = last_ack_id(self.s)
        self.s._handle_eio_message('eio', ack(ack_id, '["done"]'))
        cb.assert_called_once_with('done')
        assert self.s.manager._pending_ack_count() == 0
