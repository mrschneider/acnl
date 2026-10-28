# ACNL Requirements

Generic, transport-agnostic requirements for an agent-to-agent
communication layer. The requirements are stated as *what must hold*, not
*how it is implemented*.

## R1 — Addressing and identity

- **R1.1** A participant identity is a URN carrying at minimum three
  hierarchical components (a zone, a node, and a leaf).
- **R1.2** The short form a human types is URN + endpoint; the full form
  is a reference document. Both are parseable and printable.
- **R1.3** A reference document lists the endpoints in **failover order**,
  the protocol version, and the capabilities.

## R2 — Directory

- **R2.1** A registry supports bind, unbind, resolve, query, advertise,
  and sweep.
- **R2.2** Bind is explicit: overwriting an existing name requires a
  rebind flag; a silent overwrite is a business refusal.
- **R2.3** Advertise carries a TTL; a service that stops advertising
  within the TTL is evicted and the eviction is observable.
- **R2.4** Query is a real attribute language: `NOT > AND > OR`
  precedence, parentheses, `= != > < >= <=`, and `CONTAINS` on text and
  capability lists. A scalar comparison against text is refused at parse
  time.

## R3 — Wire

- **R3.1** The wire is a length-prefixed frame, big-endian, with a hard
  frame cap. Zero-length and hostile lengths are refused, not allocated.
- **R3.2** A decoder returns "need more bytes" and "corrupt" as distinct
  outcomes; a partial buffer is never treated as a parse failure.
- **R3.3** The wire vocabulary is closed: an unknown verb is refused
  naming the word the peer sent and the legal set.

## R4 — Transport semantics

- **R4.1** A connection state machine is explicit and terminal; an
  illegal transition raises.
- **R4.2** Sequenced delivery buffers out-of-order payloads, refuses
  duplicates and gap-then-duplicate runs, and reports the next contiguous
  run.
- **R4.3** A sender window tracks unacked sequences, retransmits after a
  timeout, and drops past a retry cap.

## R5 — Routing

- **R5.1** Loop safety: each hop appends its own id to the route and
  refuses when it finds itself already there.
- **R5.2** A hop count bound refuses further forwarding when exhausted.
- **R5.3** The dedupe key is `(sender, message_id)`; a retried refusal
  replays as a refusal.
- **R5.4** Replies route by the envelope's recipient, not by request id.

## R6 — Backpressure

- **R6.1** One bounded queue + one writer thread per connection.
- **R6.2** A full queue maps to a retryable `Unavailable`.
- **R6.3** A stalled peer never blocks the global broker lock.

## R7 — Failure taxonomy

- **R7.1** Every refusal is typed: user (never retryable), system
  (`retryable` flag), message (bytes unparsable, connection stays open).
- **R7.2** A malformed frame is answered in-protocol; the connection is
  not closed.

## R8 — Versioning

- **R8.1** A different major is refused; a different minor is accepted as
  additive.
- **R8.2** The version field that requires a bump is documented.

## R9 — Packaging

- **R9.1** The core contract is IO-free: no sockets in the library; a
  transport only moves bytes.
- **R9.2** The reference participant (node) ships as a binary; any
  framework that speaks the wire format can peer with it.
- **R9.3** The E2E driver is written in a language different from the
  server: a Python client talking to a Python server proves nothing about
  portability.

## R10 — Federation

- **R10.1** Brokers link as peers with a broker-role hello; registration of
  the link is implicit (no dedicated register message).
- **R10.2** On link, the answering side pushes its whole live directory
  table as notifications (the router's table).
- **R10.3** Learned entries propagate hop by hop on the periodic table
  broadcast, excluding the peer that taught the entry — sending it back is
  an echo, not propagation.
- **R10.4** An event about a locally known agent or about the broker's own
  urn is a reverb: it is ignored, never clobbering the local mapping.
- **R10.5** Resolve answers locally first; on a local miss the federated
  table answers, and the reply marks `origin: "federated"`.
- **R10.6** A federated query merges the learned table into the result,
  deduped by urn, with the local entry winning on conflicts.
- **R10.7** Replies route by the envelope's recipient through the federated
  table when the recipient is not directly connected — never by request id.
- **R10.8** A peer going down takes its table with it: its entries are
  evicted locally and the eviction is broadcast (a router's loss is
  announced, not discovered by timeout alone).
- **R10.9** An explicit withdraw of the agent's own name wins over its
  implicit registration; a withdraw of a name carried by a peer link must
  not desync the federation.

## R11 — Security

- **R11.1** When a policy is configured, every outgoing frame carries a
  per-hop MAC computed *after* the hop's envelope mutations, and the reader
  verifies before parsing anything.
- **R11.2** An authentication or decryption failure is a typed refusal
  answered in-protocol; the connection stays open and the refusal echoes
  the caller's request_id when it is well-formed.
- **R11.3** Encryption covers the body only: routing headers (and a
  request's `op`) stay visible to brokers; the ciphertext is bound to
  `kind` + origin `sender` so it cannot be spliced across messages.
- **R11.4** Authorization is default-deny whenever an ACL exists; a
  matching rule is required for (sender, op, recipient) on every hop.
- **R11.5** Without a configured policy the wire is byte-identical to the
  pre-security protocol (opt-in, additive keys).

## R12 — Datagram plane

- **R12.1** The sequenced contract runs end-to-end over UDP: send window
  retransmits unacked sequences, receive window orders, holds gaps, and
  refuses duplicates; a refused duplicate is recorded and never
  double-delivered.
- **R12.2** Oversized payloads and datagrams are refused before the wire;
  hostile bytes are recorded without killing the link.
- **R12.3** The connection state machine is the contract's, on both
  endpoints, including close.

## R13 — Async implementation

- **R13.1** The asyncio node speaks the identical wire and produces the
  identical refusals as the threaded node for the same scenario.
- **R13.2** Backpressure stays bounded: one queue per connection, a full
  queue maps to a retryable `queue_full`, the loop never blocks on write.
- **R13.3** The two implementations federate with each other without
  special cases.
