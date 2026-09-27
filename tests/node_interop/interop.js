// Driver for the Python <-> Node.js Redis broadcast interop acceptance
// tests. It uses the pinned @socket.io/redis-adapter 8.x protocol
// implementation in redis-adapter.js to generate and consume messages.
//
// Usage:
//   node interop.js publish <out.json>          Node -> Python messages
//   node interop.js consume <in.json> <out.json> Python -> Node messages

const fs = require("fs");
const { RedisAdapter } = require("./redis-adapter");
const { Encoder, Decoder } = require("socket.io-parser");

class FakePubSubClient {
  constructor() {
    this.published = [];
  }
  publish(channel, message) {
    this.published.push([channel, message]);
  }
  psubscribe() {}
}

function makeNsp(name) {
  return { name, server: { encoder: new Encoder() } };
}

function makeSocket(id) {
  return {
    id,
    received: [],
    client: {
      writeToEngine(encodedPackets) {
        const decoder = new Decoder();
        const owner = this;
        decoder.on("decoded", (packet) => owner.socket.received.push(packet));
        for (const pkt of encodedPackets) {
          decoder.add(pkt);
        }
      },
    },
  };
}

function makeAdapter(nspName) {
  const nsp = makeNsp(nspName);
  const pubClient = new FakePubSubClient();
  const adapter = new RedisAdapter(nsp, pubClient, new FakePubSubClient());
  const sockets = {};
  nsp.sockets = { get: (id) => sockets[id] };
  const rooms = { sidA: ["room1"], sidB: ["room1", "room2"], sidC: [] };
  for (const [sid, sids] of Object.entries(rooms)) {
    const socket = makeSocket(sid);
    socket.client.socket = socket;
    sockets[sid] = socket;
    adapter.addAll(sid, new Set([sid, ...sids]));
  }
  return { adapter, pubClient, sockets };
}

function toJsonable(value) {
  if (value instanceof Buffer || value instanceof Uint8Array) {
    return { $binary: Buffer.from(value).toString("base64") };
  }
  if (Array.isArray(value)) {
    return value.map(toJsonable);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const key of Object.keys(value)) {
      out[key] = toJsonable(value[key]);
    }
    return out;
  }
  return value;
}

function publish(outFile) {
  const scenarios = [
    {
      name: "broadcast-all",
      nsp: "/",
      packet: { type: 2, data: ["hello", "world"] },
      opts: { rooms: new Set(), except: new Set(), flags: {} },
    },
    {
      name: "single-room",
      nsp: "/",
      packet: { type: 2, data: ["hello", "room-only"] },
      opts: { rooms: new Set(["room1"]), except: new Set(), flags: {} },
    },
    {
      name: "multi-room-except",
      nsp: "/",
      packet: { type: 2, data: ["hello", "multi"] },
      opts: {
        rooms: new Set(["room1", "room2"]),
        except: new Set(["sidA"]),
        flags: {},
      },
    },
    {
      name: "binary",
      nsp: "/",
      packet: { type: 2, data: ["bin", Buffer.from([1, 2, 3])] },
      opts: { rooms: new Set(), except: new Set(), flags: {} },
    },
    {
      name: "namespace",
      nsp: "/chat",
      packet: { type: 2, data: ["ns-event", 42] },
      opts: { rooms: new Set(), except: new Set(), flags: {} },
    },
  ];
  const out = [];
  for (const scenario of scenarios) {
    const { adapter, pubClient } = makeAdapter(scenario.nsp);
    adapter.broadcast(scenario.packet, scenario.opts);
    out.push({
      name: scenario.name,
      nsp: scenario.nsp,
      messages: pubClient.published.map(([channel, msg]) => [
        channel,
        Buffer.from(msg).toString("base64"),
      ]),
    });
  }
  fs.writeFileSync(outFile, JSON.stringify({ scenarios: out }));
}

function consume(inFile, outFile) {
  const input = JSON.parse(fs.readFileSync(inFile, "utf8"));
  const out = [];
  for (const scenario of input.scenarios) {
    const { adapter, sockets } = makeAdapter(scenario.nsp || "/");
    for (const [channel, b64] of scenario.messages) {
      const message = Buffer.from(b64, "base64");
      if (scenario.own) {
        // pretend this adapter is the publisher, so it must ignore the
        // message as its own echo
        adapter.uid = adapter.opts.parser.decode(message)[0];
      }
      adapter.onmessage(null, channel, message);
    }
    const deliveries = {};
    for (const [sid, socket] of Object.entries(sockets)) {
      deliveries[sid] = socket.received.map((packet) => ({
        type: packet.type,
        nsp: packet.nsp,
        data: toJsonable(packet.data),
      }));
    }
    out.push({ name: scenario.name, deliveries });
  }
  fs.writeFileSync(outFile, JSON.stringify({ scenarios: out }));
}

const [mode, ...args] = process.argv.slice(2);
if (mode === "publish") {
  publish(args[0]);
} else if (mode === "consume") {
  consume(args[0], args[1]);
} else {
  console.error("unknown mode");
  process.exit(1);
}
