import base64
import json
import os
import shutil
import subprocess
import tempfile
from unittest import mock

import pytest

from socketio import packet
from socketio.redis_manager import RedisNodeManager

NODE_DIR = os.path.join(os.path.dirname(__file__), '..', 'node_interop')
NODE_SCRIPT = os.path.join(NODE_DIR, 'interop.js')

node_available = shutil.which('node') is not None and os.path.isdir(
    os.path.join(NODE_DIR, 'node_modules'))
pytestmark = pytest.mark.skipif(
    not node_available,
    reason='node.js or the node_interop dependencies are not available')


def run_node(*args):
    subprocess.run(
        ['node', NODE_SCRIPT, *args],
        check=True, cwd=NODE_DIR, capture_output=True, timeout=60)


class TestRedisNodeInterop:
    """Acceptance tests that exchange messages with the pinned Node.js
    @socket.io/redis-adapter 8.x broadcast protocol implementation in
    tests/node_interop."""

    def make_manager(self, connect_clients=True):
        id = 0

        def generate_id():
            nonlocal id
            id += 1
            return str(id)

        mock_server = mock.MagicMock()
        mock_server.eio.generate_id = generate_id
        mock_server.packet_class = packet.Packet
        pm = RedisNodeManager('redis://')
        pm._publish_message = mock.MagicMock()
        pm.set_server(mock_server)
        pm.initialize()
        if connect_clients:
            # client 1 is in room1, client 2 is in room1 and room2 and
            # client 3 is in no rooms
            pm.connect('eio1', '/')
            pm.connect('eio2', '/')
            pm.connect('eio3', '/')
            pm.enter_room('1', '/', 'room1')
            pm.enter_room('2', '/', 'room1')
            pm.enter_room('2', '/', 'room2')
        return pm

    def test_python_to_node(self, tmp_path):
        pm = self.make_manager(connect_clients=False)
        scenarios = []

        def capture(name, nsp='/', own=False):
            channel, message = pm._publish_message.call_args[0]
            scenarios.append({
                'name': name, 'nsp': nsp, 'own': own,
                'messages': [[channel,
                              base64.b64encode(message).decode()]],
            })
            pm._publish_message.reset_mock()

        pm.emit('hello', 'world')
        capture('broadcast-all')
        pm.emit('hello', 'room-only', room='room1')
        capture('single-room')
        pm.emit('hello', 'multi', to=['room1', 'room2'], skip_sid='sidA')
        capture('multi-room-except')
        pm.emit('bin', b'\x01\x02\x03')
        capture('binary')
        pm.emit('ns-event', 42, namespace='/chat')
        capture('namespace', nsp='/chat')
        pm.emit('hello', 'echo')
        capture('echo', own=True)

        in_file = tmp_path / 'in.json'
        out_file = tmp_path / 'out.json'
        in_file.write_text(json.dumps({'scenarios': scenarios}))
        run_node('consume', str(in_file), str(out_file))
        results = {s['name']: s['deliveries']
                   for s in json.loads(out_file.read_text())['scenarios']}

        def received(deliveries, sid):
            return deliveries[sid]

        all_sids = ['sidA', 'sidB', 'sidC']

        # a broadcast to the namespace reaches every client
        for sid in all_sids:
            assert received(results['broadcast-all'], sid) == [{
                'type': 2, 'nsp': '/', 'data': ['hello', 'world']}]

        # a single-room broadcast only reaches the members of the room
        assert received(results['single-room'], 'sidA') == [{
            'type': 2, 'nsp': '/', 'data': ['hello', 'room-only']}]
        assert received(results['single-room'], 'sidB') == [{
            'type': 2, 'nsp': '/', 'data': ['hello', 'room-only']}]
        assert received(results['single-room'], 'sidC') == []

        # a client in several target rooms receives the event only once,
        # and excepted clients do not receive it
        assert received(results['multi-room-except'], 'sidA') == []
        assert received(results['multi-room-except'], 'sidB') == [{
            'type': 2, 'nsp': '/', 'data': ['hello', 'multi']}]
        assert received(results['multi-room-except'], 'sidC') == []

        # binary payloads arrive as binary (the node decoder maps the
        # binary event back to a plain event with the binary attachment)
        for sid in all_sids:
            assert received(results['binary'], sid) == [{
                'type': 2, 'nsp': '/',
                'data': ['bin', {'$binary': 'AQID'}]}]

        # namespaces are honored
        for sid in all_sids:
            assert received(results['namespace'], sid) == [{
                'type': 2, 'nsp': '/chat', 'data': ['ns-event', 42]}]

        # the node ignores its own messages
        for sid in all_sids:
            assert received(results['echo'], sid) == []

    def test_node_to_python(self, tmp_path):
        out_file = tmp_path / 'out.json'
        run_node('publish', str(out_file))
        scenarios = json.loads(out_file.read_text())['scenarios']

        for scenario in scenarios:
            nsp = scenario['nsp']
            pm = self.make_manager()
            if nsp != '/':
                pm.connect('eio4', nsp)

            def messages(scenario=scenario):
                for channel, b64 in scenario['messages']:
                    yield base64.b64decode(b64)

            pm._listen = mock.MagicMock(side_effect=messages)
            try:
                pm._thread()
            except StopIteration:
                pass

            calls = pm.server._send_eio_packet.call_args_list
            by_eio_sid = {}
            for call in calls:
                by_eio_sid.setdefault(call[0][0], []).append(call[0][1])

            name = scenario['name']
            if name == 'broadcast-all':
                assert set(by_eio_sid) == {'eio1', 'eio2', 'eio3'}
                for pkts in by_eio_sid.values():
                    assert [p.encode() for p in pkts] == \
                        ['42["hello","world"]']
            elif name == 'single-room':
                assert set(by_eio_sid) == {'eio1', 'eio2'}
                for pkts in by_eio_sid.values():
                    assert [p.encode() for p in pkts] == \
                        ['42["hello","room-only"]']
            elif name == 'multi-room-except':
                # client 2 is in both rooms but receives the event once
                assert set(by_eio_sid) == {'eio1', 'eio2'}
                for pkts in by_eio_sid.values():
                    assert [p.encode() for p in pkts] == \
                        ['42["hello","multi"]']
            elif name == 'binary':
                assert set(by_eio_sid) == {'eio1', 'eio2', 'eio3'}
                for pkts in by_eio_sid.values():
                    assert len(pkts) == 2
                    assert pkts[0].encode() == \
                        '451-["bin",{"_placeholder":true,"num":0}]'
                    assert pkts[1].data == b'\x01\x02\x03'
            elif name == 'namespace':
                assert set(by_eio_sid) == {'eio4'}
                assert [p.encode() for p in by_eio_sid['eio4']] == \
                    ['42/chat,["ns-event",42]']
            else:  # pragma: no cover
                raise AssertionError('unknown scenario ' + name)
