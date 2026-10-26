// Cross-language E2E: a Node.js client drives a Python ACNL node.
// Proves the wire contract is language-neutral: framing (u32 BE length +
// JSON), the seven wire kinds, the seven-operation closed vocabulary,
// envelope validation, and the typed error taxonomy all behave identically
// from a second implementation.
//
// Run: python experiments/run_e2e.py

import net from "node:net";

const HOST = "127.0.0.1";
const PORT = parseInt(process.env.ACNL_PORT || "19710", 10);
const AGENT = "acnl:lab/node1/jsclient";
const BROKER = "acnl:lab/node1/broker";

function frame(value) {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(payload.length, 0);
  return Buffer.concat([head, payload]);
}

class Decoder {
  constructor() {
    this.buf = Buffer.alloc(0);
  }
  feed(data) {
    this.buf = Buffer.concat([this.buf, data]);
    const out = [];
    for (;;) {
      if (this.buf.length < 4) break;
      const len = this.buf.readUInt32BE(0);
      if (len === 0) throw new Error("zero-length frame");
      if (this.buf.length < 4 + len) break;
      const raw = this.buf.subarray(4, 4 + len);
      this.buf = this.buf.subarray(4 + len);
      out.push(JSON.parse(raw.toString("utf8")));
    }
    return out;
  }
}

const results = [];
function check(name, ok, detail = "") {
  results.push({ name, ok });
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
}

const sock = net.createConnection({ host: HOST, port: PORT });
const dec = new Decoder();
const pending = []; // reply/error frames waiting to be matched
let waiter = null;

sock.on("data", (d) => {
  for (const v of dec.feed(d)) {
    if (v.kind === "reply" || v.kind === "error") {
      pending.push(v);
      if (waiter) {
        const w = waiter;
        waiter = null;
        w();
      }
    }
  }
});

async function nextReply() {
  if (pending.length > 0) return pending.shift();
  await new Promise((res) => (waiter = res));
  return pending.shift();
}

let idc = 0;
function env() {
  idc += 1;
  return {
    sender: AGENT,
    recipient: BROKER,
    request_id: `js-${idc}`,
    message_id: `js-m-${idc}`,
    ttl: 8,
    route: [],
    version: "1.0",
  };
}

function sendEnvelope(op, body) {
  sock.write(frame({ kind: "request", envelope: env(), body: { op, ...body } }));
}

function sendRawEnvelope(envelope, op, body) {
  sock.write(frame({ kind: "request", envelope, body: { op, ...body } }));
}

// 0. hello first — the broker learns our URN from the sender field
sock.write(frame({ kind: "hello", envelope: env(), body: {} }));

await new Promise((resolve, reject) => {
  sock.on("connect", resolve);
  sock.on("error", (e) => reject(e));
}).catch((e) => {
  console.error("connection failed:", e.message);
  process.exit(1);
});

// 1. ping round-trip
sendEnvelope("ping");
let r = await nextReply();
check("ping round-trip", r.kind === "reply" && r.body.value === "pong", JSON.stringify(r.body));

// 2. advertise then resolve
sendEnvelope("advertise", {
  urn: AGENT,
  reference: { urn: AGENT, endpoints: [], protocol_version: "1.0", capabilities: ["e2e", "json"], attributes: { runtime: "nodejs" } },
  ttl: 60,
});
r = await nextReply();
check("advertise accepted", r.kind === "reply" && r.body.urn === AGENT, JSON.stringify(r.body));

sendEnvelope("resolve", { urn: AGENT });
r = await nextReply();
check(
  "resolve returns the advertised reference",
  r.kind === "reply" && r.body.reference?.capabilities?.includes("e2e"),
  JSON.stringify(r.body).slice(0, 140)
);

// 3. attribute query, both directions
sendEnvelope("query", { filter: 'runtime = "nodejs"' });
r = await nextReply();
check(
  "query finds the node.js agent",
  r.kind === "reply" && (r.body.references ?? []).some((x) => x.urn === AGENT),
  JSON.stringify(r.body).slice(0, 140)
);
sendEnvelope("query", { filter: 'runtime = "python"' });
r = await nextReply();
check(
  "query excludes the node.js agent",
  r.kind === "reply" && (r.body.references ?? []).every((x) => x.urn !== AGENT)
);

// 4. unknown operation refused naming the word and the legal set
sendEnvelope("do_magic");
r = await nextReply();
check(
  "unknown operation refused naming the word",
  r.kind === "error" && r.body.code === "unknown_verb" && r.body.details?.sent === "do_magic" && Array.isArray(r.body.details?.legal),
  JSON.stringify(r.body).slice(0, 160)
);

// 5. unknown service: user refusal, never retryable
sendEnvelope("resolve", { urn: "acnl:lab/node9/ghost" });
r = await nextReply();
check(
  "unknown service: user refusal, not retryable",
  r.kind === "error" && r.body.code === "unknown_service" && r.body.retryable === false,
  JSON.stringify(r.body).slice(0, 140)
);

// 6. a bad envelope is a message error, answered in-protocol, connection stays open
const bad = env();
bad.ttl = 99; // outside 1..16: refused by the contract at one entrance
sendRawEnvelope(bad, "ping");
r = await nextReply();
check("bad ttl refused in-protocol", r.kind === "error" && r.body.kind === "message", JSON.stringify(r.body).slice(0, 140));
sendEnvelope("ping");
r = await nextReply();
check("connection stayed open after a message error", r.kind === "reply" && r.body.value === "pong");

// 7. withdraw then resolve is a refusal
sendEnvelope("withdraw", { urn: AGENT });
r = await nextReply();
check("withdraw accepted", r.kind === "reply");
sendEnvelope("resolve", { urn: AGENT });
r = await nextReply();
check("after withdraw, resolve refuses", r.kind === "error" && r.body.code === "unknown_service");

// 8. clean close
sock.write(frame({ kind: "close", envelope: env() }));
await new Promise((res) => sock.end(res));

const failed = results.filter((x) => !x.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length > 0 ? 1 : 0);
