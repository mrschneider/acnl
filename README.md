# Agent Communication Network Layer (ACNL)

[![CI](https://github.com/mrschneider/acnl/actions/workflows/ci.yml/badge.svg)](https://github.com/mrschneider/acnl/actions/workflows/ci.yml)
[![Release](https://github.com/mrschneider/acnl/actions/workflows/release.yml/badge.svg)](https://github.com/mrschneider/acnl/actions/workflows/release.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**Status:** v0.2.0 · **Language:** Python 3.10+ (core is stdlib-only; encryption is an optional `[security]` extra) · **Interop driver:** Node.js

## What it is

ACNL is a distributed agent-to-agent communication layer. It is a network
operating system for agents: a network protocol (framing + closed message
vocabulary), a directory service (registration, binding, resolution, attribute
search), a routed broker (relay with hop-count limits, loop detection, and
deduplication), a sequenced reliable exchange between brokers, and **broker
federation** — brokers link as peers, share their directory tables, and route
across each other.

## What it is NOT

- No HTTP, no websockets, no REST. The wire is a binary-framed TCP stream.
- No JSON-RPC, no open message vocabulary. The protocol has a **closed set of
  seven operations**; unknown verbs are refused in-protocol with the exact
  wording of the peer's message and the legal set.

## The protocol

- **Addressing:** `acnl:<zone>/<node>/<agent>` — a hierarchical URN.
- **Reference:** `acnl://<host>:<port>/<urn>` — the short form a human types.
- **Frame:** `u32 BE length + JSON` on TCP. `MAX_FRAME = 1 MiB`. A zero-length
  frame is refused as a bug, a hostile length is refused as a bug.
- **Message layer:** `hello`, `request`, `reply`, `error`, `notify`, `fragment`, `close`.
- **Operation layer:** `advertise`, `withdraw`, `resolve`, `query`, `send`, `bind`, `ping`.
- **Errors:** `user_error` (business refusal, never retryable), `system_error`
  (infrastructure, carries a `retryable` flag), `message_error` (the bytes
  could not be parsed — answered in-protocol, connection stays open).
- **Version policy:** major mismatch is refused, minor mismatch is accepted as
  additive.

## Federation

Brokers link as peers with `Node.peer(host, port)`. Registration of the link
is implicit (a broker-role hello); the answering side pushes its whole live
directory table as notifications. Learned entries propagate hop by hop on the
periodic table broadcast — an echo back to the peer that taught the entry is
not propagation, and events about locally known agents are reverbs that never
clobber the local mapping. Resolve answers locally first, then from the
federated table (marked `origin: "federated"`); queries merge and dedupe by
urn with the local entry winning; replies route by recipient through the
federated table. A peer going down takes its table with it, and the loss is
announced. No new wire kind, no new operation — federation rides entirely on
`hello`/`notify`, which is why it is a minor version.

## Security

Opt-in per participant via `NodeConfig.security` / `Client(security=...)` with
one `SecurityPolicy` per mesh:

- **Authentication** — every outgoing frame carries a per-hop HMAC-SHA256
  `auth` block over the canonical JSON of the frame (stdlib `hmac`). The
  writer signs *after* the hop mutates `route`/`ttl`; the reader verifies
  before parsing and refuses failures in-protocol with a typed
  `auth_failed` user error (connection stays open). Replay of a request is
  bounded by the existing `(sender, message_id)` dedupe window.
- **Encryption** — AES-256-GCM seals the *body* only: the envelope stays
  plaintext because brokers route on it, and a request keeps its `op`
  visible for routing. AAD binds `kind` + origin `sender` (stable across
  hops). Needs the optional `cryptography` package
  (`pip install acnl[security]`); `require_encryption` refuses plaintext
  request/reply bodies.
- **Authorization** — a default-deny ACL of `{sender, ops, recipient}`
  rules (`fnmatch` patterns) checked at the routing entrance on every hop;
  refusal is a typed `not_authorized` user error. `acl=None` keeps the
  historical allow-all behavior.
- **Trust model, honestly**: one PSK keyring shared by the mesh — the
  cipher protects the wire against observers, not against mesh members.
  No PKI, no key rotation, no dynamic policy distribution (see the gap
  list).

## Datagram plane

`acnl_engine.datagram.UdpLink` exercises the sequenced contract end-to-end
over real UDP sockets: data/ack datagrams with retransmission through the
`SendWindow`, ordered delivery through the `ReceiveWindow` (gaps hold,
duplicates are refused and recorded, never double-delivered), the
contract's state machine (`OPEN → ESTABLISHED → CLOSING → CLOSED`), hard
caps refused before the wire, and the same HMAC `auth` block as TCP. The
UDP plane authenticates; it does not re-encrypt (documented scope).

## Async node

`acnl_engine.async_node.AsyncNode` is the same broker on asyncio:
identical wire, refusals, routing, federation, security, and persistence
semantics — one event loop owns the state (no locks), each connection is a
bounded `asyncio.Queue` + writer task, timers are one task. The threaded
`Node` and `AsyncNode` federate with each other; both are exercised
against the same tests. The client is still threaded (gap list).

## Persistence

`NodeConfig.data_path=<path>` gives the node a file-backed
`ShelveDirectory`; `stop()` closes what it opened (an injected directory
stays the caller's). The adapter is thread-safe under the node's
accept/reader/timer threads — per-thread handles (the default shelve
backend on 3.13+ is SQLite, which is thread-affine) serialized by one
lock. Federation tables are deliberately *not* persisted: they are
connection-derived and die with the peer.

## Deliverables

- `acnl_core/` — the contract, IO-free (message layer, addressing, query
  language, directory contract, sequencing).
- `acnl_engine/` — the reference node (directory service + routed broker) and
  a reference client.
- `tests/` — contract, behaviour, parity, and federation tests.
- `experiments/` — end-to-end proofs (the interop driver is in a different
  language than the server).
- `docs/` — research, architecture, and requirements.

## Quickstart

```python
from acnl_core.directory import InMemoryDirectory
from acnl_engine.config import NodeConfig
from acnl_engine.node import Node
from acnl_engine.client import Client

node = Node(NodeConfig(host="127.0.0.1", port=9100, name="node1", zone="lab"),
            InMemoryDirectory())
node.start()

client = Client("acnl:lab/node1/worker")
client.connect("127.0.0.1", 9100)
client.advertise("acnl:lab/node1/worker",
                 {"urn": "acnl:lab/node1/worker", "endpoints": [],
                  "protocol_version": "1.0", "capabilities": ["math"], "attributes": {}})
print(client.resolve("acnl:lab/node1/worker"))

# federate a second broker
node2 = Node(NodeConfig(host="127.0.0.1", port=9101, name="node2", zone="lab"),
             InMemoryDirectory())
node2.start()
node.peer("127.0.0.1", 9101)
```

## Deliberately not implemented

Honest gap list — what exists is listed above; what does not:

- **No PKI.** One shared PSK keyring per mesh: no certificates, no key
  rotation, no per-agent secrets. Encryption hides bodies from the wire,
  not from mesh members. ACLs are static config, not distributed policy.
- **Replay defense is the dedupe window** (`(sender, message_id)` on
  requests) — no per-sender counters or nonces; replies are not
  deduplicated by the client.
- **The datagram plane authenticates, it does not re-encrypt**, has no
  multicast and no fragmentation (payloads are capped instead); it carries
  payloads, not routed ACNL messages.
- **No async client** — `AsyncNode` is the asyncio implementation; the
  reference `Client` remains threaded (it interoperates with both).
- **Persistence is directory-only**; federation tables and dedupe windows
  are intentionally volatile.
- **The Node.js interop driver speaks the unsigned wire**: the `auth`/`enc`
  blocks are additive JSON keys a foreign implementation must ignore or
  implement; there is no reference JS binding for them yet.
