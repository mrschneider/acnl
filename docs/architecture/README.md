# ACNL Architecture

The concept-to-code map. The canonical vocabulary is in the research doc
(`docs/research/legacy-lan-architectures.md`); the code follows it.

## Layers

```
┌────────────────────────────────────────────────────────┐
│  acnl_engine (node.py)                                 │
│  TCP server, routing table, connection threads,        │
│  directory, service advertising, backpressure          │
├────────────────────────────────────────────────────────┤
│  acnl_core (contract — IO-free)                        │
│  Envelope, operations, query language, state machine,  │
│  codec (length-prefix, MAX_FRAME), validation          │
├────────────────────────────────────────────────────────┤
│  TCP wire                                              │
│  u32 BE length + JSON body, MAX_FRAME cap              │
└────────────────────────────────────────────────────────┘
```

`acnl_core` has **no sockets**. A transport only moves bytes; the
contract is the only place that validates. This is what makes every
binding and every test cheap.

## Message layer vs. operation layer

Two layers, never fused:

- **Message layer** (transport): `ENVELOPE`, `REPLY`, `DIRECTORY_QUERY`,
  `DIRECTORY_BIND`, `ADVERTISE`, `WITHDRAW`, `HOP_LIMIT`,
  `CONNECTION_CLOSE`, `MESSAGE_ERROR`.
- **Operation layer** (the product's vocabulary) carried inside the
  `ENVELOPE` body: `service_addr`, `service_advertise`, `route_hop`,
  `conn_open`, `conn_close`, `conn_state`.

## Invariants

1. **Vocabulary closed.** An unknown verb is refused *naming the word the
   peer sent and the legal set*. There are 9 verbs; no more without a
   major version bump.
2. **Every refusal is typed.** Three kinds — user refusal (business,
   never retryable), system error (infrastructure, `retryable` flag),
   message error (bytes unparsable, answered in-protocol, connection
   stays open).
3. **Validation at one entrance.** `acnl_core.parse` validates version,
   addressing, payload shape; handlers trust the message after that.
4. **Identity is a URN** carrying the runtime and framework: the first
   question on an incident is "which participant is that", and the
   answer is in the address.
5. **Rebind is explicit.** A silent overwrite on bind lets a typo take
   over a production name.
6. **Registration is implicit.** A participant's URN is learned from the
   `sender` of its first message.
7. **Replies route by the envelope's `recipient`**, never by
   `request_id`.
8. **Loop safety**: each hop appends its own id to the `route` service
   context; the router refuses when it finds itself already there.
9. **Dedup key is `(sender, message_id)`** — the sender is part of the
   identity of a message.
10. **Backpressure**: one bounded queue + one writer thread per
    connection; a full queue maps to a retryable `Unavailable`.
11. **Version policy**: a different major is refused, a different minor
    is accepted as additive.
12. **State machine**: `OPEN → ESTABLISHED → CLOSING → CLOSED`, terminal
    `CLOSED`; `OPEN → CLOSED` is illegal.

## Integration hooks

- **Transport adapters**: `acnl_core.contract` is IO-free, so a
  transport (TCP, SSE, Unix socket) only moves bytes. New transports
  plug in without touching the contract.
- **Directory backends**: `acnl_engine.directory` is a small
  `Protocol`-shaped interface; an in-memory adapter ships today, a
  durable backend is a gap list item.
- **Peers**: `acnl_engine.node` is the reference participant. Any
  framework that speaks the wire format can peer with it.
- **Broker federation** (`Node.peer(host, port)`): broadcast federation —
  no routing protocol, no directory merge daemon. A peer link is a
  broker-role hello; the answering side pushes its whole live table as
  notify frames; learned entries propagate hop by hop on the periodic
  table broadcast, excluding the peer that taught the entry. The echo
  guard ignores any event about a locally known agent or about the
  broker's own urn — a reverb never clobbers the local mapping. Resolve
  answers locally first, then from the federated table (marked
  `origin: "federated"`); queries merge and dedupe by urn with the local
  entry winning. Replies route by recipient through the federated table
  when the recipient is not directly connected. A peer going down takes
  its table with it: entries evicted, eviction announced. An explicit
  withdraw of the agent's own name wins over its implicit registration.
  No new wire kind, no new operation — federation rides entirely on
  hello/notify, so it is a minor version.

## Wire format

- **Frame**: `u32 BE length + JSON body`, big-endian, capped at
  `MAX_FRAME` (65536). A zero-length frame and a hostile length are
  refused, not allocated.
- **Decode**: `Ok(None)` for a partial buffer, `Ok(Some((msg, consumed)))`
  otherwise — never conflate "need more bytes" with "corrupt".
- **Fragmentation** is exercised with a small pinned fragment size in the
  E2E, so no megabytes of fixture data are needed.
