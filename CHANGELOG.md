# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
the protocol's own policy (R8): a different major is refused on the wire, a
different minor is accepted as additive.

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
