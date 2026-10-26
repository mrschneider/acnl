# Research: Legacy LAN directory and reliable transport architectures

## Scope

This is the research deliverable for ACNL. It synthesizes the published
designs of the LAN network operating systems of the 1980s and 90s — the
directory/naming, service-advertising, sequenced reliable transport, and
hop-count routing models — as they apply to a modern agent-to-agent layer.
The protocol vocabulary is **closed**; the framing, refusal taxonomy, and
routing semantics below are taken from that research.

## 1. Directory and naming

Legacy LAN network OSes (X.500-derived, the ancestor of the directory model)
used a **hierarchical name** and stored a **reference document**, not a
bare address. The rules that survive:

- The name is a tree: `zone/node/leaf`. The name is authoritative; the
  address behind it is an implementation detail.
- The reference document is a **URN + endpoints + capabilities**. The
  registry stores the reference, not the address. The short form the human
  types is URN + one endpoint, failover ordered.
- **Bind is explicit**: overwriting a name requires a rebind flag. A silent
  overwrite lets a typo take over a production name. The reference document
  lists the reference document as the *storage* unit of the registry.
- **Resolution is a query** over a **real attribute language**: attribute
  comparisons (`=`, `!=`, `>`, `<`, `>=`, `<=`) and `CONTAINS` on text and
  capability lists. Ordering `NOT > AND > OR`. A comparison against a
  non-numeric type with a scalar operator is refused at parse time.

## 2. Service advertising

The LAN network OSes solved the same problem: a server that starts must
announce itself; a server that stops must announce its disappearance; a
router that goes away must not leave stale entries. Their rules:

- **Advertise on startup, withdraw on shutdown.** A server that cannot
  advertise cannot serve.
- **Periodic broadcast** (default every 60 s) refreshes the registration.
- **TTL expiry**: a service that stops advertising within the TTL is
  presumed down and evicted, and the eviction is broadcast.
- **Routers gather and echo**: a broker's directory is not authoritative
  for the whole internetwork; it is one of several, and the router's
  neighbors are peers, not clients.

## 3. Sequenced reliable transport

The LAN network OSes (SPX in the IPX family; the same idea in the
reliable datagram literature) used **sequence numbers** plus a **retry
window**:

- The sender tracks unacked sequences and retransmits after a timeout.
- The receiver buffers out-of-order payloads and refuses gaps and
  duplicates — an assembled-wrong message is worse than a failed one.
- A **connection state machine** is explicit: `OPEN → ESTABLISHED →
  CLOSING → CLOSED`, with `ESTABLISHED → CLOSED` legal (terminal) and
  `OPEN → CLOSED` illegal (a clean close must close the connection).

## 4. Hop-count routing

The LAN network OSes (RIP in the IPX family; NLSP as the successor) used
**hop-count routing** with:

- **Loop safety**: each hop appends its own id to the route; the router
  refuses when it finds itself already in the route.
- **A bounded TTL**: a frame that exhausts its hops is refused, not
  dropped silently.
- **A deduped route**: `(sender, message_id)` is the dedup key — a
  retried refusal replays as a refusal, not as silence.

## 5. Broker / relay

The LAN network OSes (NCP, the file/print core) handled a broker's core
duties:

- **Registration is implicit**: a participant's URN is learned from the
  `sender` of its first message. A dedicated register message is a second
  source of truth.
- **Route replies by the envelope's `recipient`**, never by `request_id`:
  request ids collide across connections, and a routing table keyed on
  them cross-talks.
- **Backpressure**: one bounded queue + one writer thread per
  connection; a full queue maps to a retryable `Unavailable`; a stalled
  peer never freezes the whole broker.

## 6. Closed vocabulary

The **vocabulary is closed** because an unknown verb is refused *naming
the word the peer sent and the legal set*. A protocol that quietly
accepts any verb needs a translator per pair of frameworks.

## 7. Validation at one entrance

The **contract is IO-free**; the transport layer is where bytes are
moved. The contract is the *only* place that validates: version,
addressing, payload shape. Validation spread across handlers is
validation that is forgotten.

## 8. Version policy

The **version policy**: a different major is refused, a different minor
is accepted as additive. The reference document lists the version field
the protocol bumps.

## 9. Failure taxonomy

The **failure taxonomy** is three kinds: user refusal (business, never
retryable), system error (infrastructure, `retryable` flag), and message
error (the bytes could not even be parsed — answered in-protocol, the
connection stays open).

## Sources

- [RFC 1058 — RIP (hop-count routing)](https://www.rfc-editor.org/rfc/rfc1058)
- [RFC 2165 — Service Location Protocol](https://www.rfc-editor.org/rfc/rfc2165)
- [RFC 2131 — DHCP/TTL semantics](https://www.rfc-editor.org/rfc/rfc2131)
- [X.500 directory model](https://www.itu.int/rec/T-REC-X.500)
- [ISO/OSI 8327-1 — session-layer sequencing](https://www.iso.org/standard/15154.html)
- [Novell NetWare service-advertising (SAP) behavior](https://en.wikipedia.org/wiki/Service_Advertising_Protocol)
