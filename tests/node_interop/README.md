# Node.js interop test fixtures

This directory contains the Node.js side of the acceptance tests for
`socketio.RedisNodeManager` (see `tests/common/test_redis_node_interop.py`).

- `redis-adapter.js` is a minimal but faithful port of the broadcast path
  of the official `@socket.io/redis-adapter` package for Node.js, pinned to
  the 8.x protocol (msgpack `[uid, packet, opts]` envelopes published on
  `<key>#<namespace>#` and `<key>#<namespace>#<room>#` channels). It extends
  the real `socket.io-adapter@2.5.7` and uses the real `msgpackr@1.11.12`,
  which are the exact dependencies of `@socket.io/redis-adapter@8.x`, so
  encoding, decoding, room/except filtering and multi-room de-duplication
  behave exactly as in the Node.js package.
- `interop.js` generates messages with the adapter (`publish` mode) and
  delivers messages through the adapter (`consume` mode).

## Setup

The `node_modules` directory is not committed. The pinned dependencies can
be installed from the npm registry with:

    npm install

or, in an offline environment where the npm cache already contains the
required tarballs, with:

    node extract.js
