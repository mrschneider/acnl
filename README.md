# Agent Communication Network Layer (ACNL)

[![CI](https://github.com/mrschneider/acnl/actions/workflows/ci.yml/badge.svg)](https://github.com/mrschneider/acnl/actions/workflows/ci.yml)
[![Release](https://github.com/mrschneider/acnl/actions/workflows/release.yml/badge.svg)](https://github.com/mrschneider/acnl/actions/workflows/release.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**Status:** v0.1.0 · **Language:** Python 3.10+ (stdlib only) · **Interop driver:** Node.js

## What it is

ACNL is a distributed agent-to-agent communication layer. It is a network
operating system for agents: a network protocol (framing + closed message
vocabulary), a directory service (registration, binding, resolution, attribute
search), a routed broker (relay with hop-count limits, loop detection, and
deduplication), and a sequenced reliable exchange between brokers.

## What it is NOT

- No HTTP, no websockets, no REST. The wire is a binary-framed TCP stream.
- No JSON-RPC, no open message vocabulary. The protocol has a **closed set of
  seven operations**; unknown verbs are refused in-protocol with the exact
  wording of the peer's message and the legal set.
- No authentication, no authorization, no encryption, no persistence.

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

## Deliverables

- `acnl_core/` — the contract, IO-free (message layer, addressing, query
  language, directory contract, sequencing).
- `acnl_engine/` — the reference node (directory service + routed broker) and
  a reference client.
- `tests/` — contract, behaviour, and parity tests.
- `experiments/` — end-to-end proofs (the interop driver is in a different
  language than the server).
- `docs/` — research, architecture, and requirements.

## Quickstart

```python
from acnl_engine.node import Node
from acnl_engine.client import Client
from acnl_core.directory import InMemoryDirectory

node = Node("localhost", 9100, InMemoryDirectory(), zone="lab")
node.start()

client = Client("acnl:lab/node1/worker")
client.connect("localhost", 9100)
client.advertise(ref=["localhost:9100"], caps=["math"])
print(client.resolve("acnl:lab/node1/worker"))
```
