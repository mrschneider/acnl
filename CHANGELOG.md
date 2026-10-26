# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
the protocol's own policy (R8): a different major is refused on the wire, a
different minor is accepted as additive.

## [0.2.0] — 2026-10-26

### Added

- Broker federation (`Node.peer(host, port)`): broker-to-broker peer links;
  on link the answering side pushes its whole live directory table as
  notifications; learned entries propagate hop by hop on the periodic table
  broadcast (excluding the peer that taught the entry — an echo is not
  propagation); echo guard ignoring events about locally known agents and
  the broker's own urn; federated resolve (`origin: "federated"`) and
  federated query merge deduped by urn with the local entry winning;
  replies routed by recipient through the federated table; eviction of a
  dead peer's entries with the loss announced.
- `Directory.snapshot()`: the live table for a peer that just linked, on
  both adapters.
- Implicit registration now propagates across the federation, so replies
  are routable two hops away.

### Changed

- An explicit `withdraw` of the agent's own name now also removes its
  implicit registration (an explicit withdraw wins over hello-time
  registration); withdrawing a name carried by a peer link does not
  desync the federation.
- A hello is answered exactly once per connection: replying to every
  hello made two broker peers ping-pong hello replies forever.

### Verified

- 64 tests passing (6 new federation tests), 11/11 cross-language E2E.

## [0.1.0] — 2026-10-26

### Added

- Wire contract (`acnl_core`): seven wire kinds (hello, request, reply,
  error, notify, fragment, close); closed seven-operation vocabulary
  (advertise, withdraw, resolve, query, send, bind, ping); typed failure
  taxonomy; length-prefixed framing (u32 BE + JSON, 1 MiB cap).
- Hierarchical addressing: `acnl:zone/node/agent` URNs, short form
  `acnl://host:port/urn`, reference documents with failover-ordered
  endpoints, capabilities and attributes.
- Directory service: bind (explicit rebind), unbind, resolve, attribute
  query (`NOT > AND > OR`, parentheses, six operators, `CONTAINS`),
  TTL advertise, sweep eviction. Two adapters: in-memory and durable
  (shelve), verified by parity tests.
- Sequenced reliable exchange: explicit connection state machine, send
  window with bounded retries, receive window refusing gaps and duplicates.
- Reference node (`acnl_engine.node`): TCP broker + directory with implicit
  registration, recipient-routed replies, loop safety via route appending,
  `(sender, message_id)` dedupe, per-hop TTL, bounded per-connection
  write queues.
- Reference client (`acnl_engine.client`): learns the broker URN from the
  hello reply, reassembles fragments, dispatches inbound requests.
- Cross-language interop proof: Node.js client × Python node, 11 checks.
- Docs: research, architecture (12 invariants), transport-agnostic
  requirements (R1–R9).
