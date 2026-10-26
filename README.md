# Agent Communication Network Layer (ACNL)

[![CI](https://github.com/mrschneider/acnl/actions/workflows/ci.yml/badge.svg)](https://github.com/mrschneider/acnl/actions/workflows/ci.yml)
[![Release](https://github.com/mrschneider/acnl/actions/workflows/release.yml/badge.svg)](https://github.com/mrschneider/acnl/actions/workflows/release.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**Status:** v0.2.0 · **Language:** Python 3.10+ (stdlib only) · **Interop driver:** Node.js

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

- No authentication, no authorization, no encryption, no persistence.
- No datagram transport; the sequenced plane is contract-only, unexercised
  end-to-end over UDP.
- No async (asyncio) implementation; the node is threaded.
