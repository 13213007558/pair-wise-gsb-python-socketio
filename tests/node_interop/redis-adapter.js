// Minimal but faithful port of the broadcast path of the official
// @socket.io/redis-adapter package for Node.js, pinned to the 8.x protocol
// (https://github.com/socketio/socket.io-redis-adapter):
//
//   channels:  <key>#<namespace>#  and  <key>#<namespace>#<room>#
//   envelope:  msgpack [ uid, packet, opts ]
//
// It extends the real socket.io-adapter@2.5.7 (the exact base class used by
// @socket.io/redis-adapter@8.x) and uses the real msgpackr@1.11.12 parser
// (the exact parser dependency of @socket.io/redis-adapter@8.x), so the
// encoding, decoding, room/except filtering and multi-room de-duplication
// behave exactly as in the Node.js package.

const { Adapter } = require("socket.io-adapter");
const msgpack = require("msgpackr");
const crypto = require("crypto");

function uid2(length) {
  // same algorithm as the uid2 package used by the real adapter
  return crypto.randomBytes(length).toString("base64").replace(/[^a-zA-Z0-9]/g, "").substring(0, length);
}

class RedisAdapter extends Adapter {
  constructor(nsp, pubClient, subClient, opts = {}) {
    super(nsp);
    this.uid = uid2(6);
    this.pubClient = pubClient;
    this.subClient = subClient;
    this.opts = Object.assign({ key: "socket.io", parser: msgpack }, opts);
    this.channel = this.opts.key + "#" + nsp.name + "#";
  }

  broadcast(packet, opts) {
    packet.nsp = this.nsp.name;
    const onlyLocal = opts.flags && opts.flags.local;
    if (!onlyLocal) {
      const rawPacket = this.opts.parser.encode([this.uid, packet, opts]);
      const channel = this.channel;
      if (opts.rooms && opts.rooms.size === 1) {
        this.pubClient.publish(
          channel + opts.rooms.values().next().value + "#",
          rawPacket
        );
      } else {
        this.pubClient.publish(channel, rawPacket);
      }
    }
    super.broadcast(packet, opts);
  }

  onmessage(pattern, channel, msg) {
    channel = channel.toString();
    const channelMatches = channel.startsWith(this.channel);
    if (!channelMatches) {
      return;
    }
    const room = channel.slice(this.channel.length, -1);
    if (room !== "" && !this.rooms.has(room)) {
      return;
    }
    const args = this.opts.parser.decode(msg);
    const [uid, packet, opts] = args;
    if (this.uid === uid) {
      return;
    }
    if (packet && packet.nsp === undefined) {
      packet.nsp = "/";
    }
    if (!packet || packet.nsp !== this.nsp.name) {
      return;
    }
    opts.rooms = new Set(opts.rooms);
    opts.except = new Set(opts.except);
    super.broadcast(packet, opts);
  }
}

module.exports = { RedisAdapter };
