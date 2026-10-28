"""The reference node: directory service + routed broker over TCP.

Design rules carried over from the contract:

- Registration is implicit: a participant's URN is learned from the ``sender``
  of its first message. A dedicated register message is a second source of truth.
- Route replies by the envelope's ``recipient``, never by ``request_id``.
- Loop safety: each hop appends its own id to the route and refuses when it
  finds itself already there.
- Dedupe key is ``(sender, message_id)`` — bounded window.
- TTL per hop: a frame that exhausts its hops is refused.
- Backpressure: one bounded queue + writer thread per connection, enqueue with
  ``try_send``; a full queue maps to a retryable ``Unavailable``. Never block
  on a write while holding the global broker lock.
- Validation at one entrance: every frame is decoded through the same
  ``WireMessage.from_value``; a malformed frame is answered in-protocol and
  the connection stays open.
"""

from __future__ import annotations

import base64
import json
import re
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import acnl_core
from acnl_core.attributes import parse_query
from acnl_core.codec import CodecError, FrameDecoder, encode_frame
from acnl_core.directory import Directory, DuplicateName, InMemoryDirectory
from acnl_core.errors import (
    ErrorBody,
    authentication_failed,
    duplicate_name,
    hops_exhausted,
    loop_detected,
    malformed,
    not_authorized,
    queue_full,
    target_unreachable,
    unavailable,
    unknown_verb,
    unknown_service,
)
from acnl_core.messages import (
    MAX_TTL,
    OPERATIONS,
    REQUEST_ID_RE,
    WIRE_KINDS,
    Envelope,
    MessageError,
    WireMessage,
    make_envelope,
    new_id,
)
from acnl_core.sequencing import ConnectionTracker, ConnectionState
from acnl_core.security import AuthorizationError, SecurityError

from .config import NodeConfig


def _err(conn, to: str, body: ErrorBody, request_id: Optional[str] = None) -> None:
    conn.push(
        WireMessage(
            kind="error",
            envelope=make_envelope(
                sender=conn.node.cfg.self_urn,
                recipient=to,
                request_id=request_id or new_id(),
                message_id=new_id(),
            ),
            body=body.to_wire(),
        )
    )


@dataclass
class Connection:
    """One live TCP connection. A peer is either a local agent (a client that
    talks to this node's directory) or a broker peer (a node that also
    routes)."""

    conn_id: str
    node: "Node"
    sock: socket.socket
    addr: Tuple[str, int]
    queue: deque
    queue_depth: int
    writer: threading.Thread
    tracker: ConnectionTracker = None
    sender: Optional[str] = None  # learned from the first hello
    capabilities: List[str] = None
    is_peer: bool = False  # broker-to-broker link (federation)
    dead: bool = False
    closed_reason: str = None

    def __post_init__(self):
        if self.tracker is None:
            self.tracker = ConnectionTracker()

    def try_send(self, msg: WireMessage) -> bool:
        if self.dead:
            return False
        if len(self.queue) >= self.queue_depth:
            return False
        self.queue.append(msg)
        return True

    def push(self, msg: WireMessage) -> bool:
        return self.try_send(msg)


class Node:
    """A reference participant: a directory service + a routed broker."""

    def __init__(self, config: NodeConfig, directory: Optional[Directory] = None):
        self.cfg = config
        self.sec = config.security
        from .config import resolve_directory

        # shared with AsyncNode so persistence semantics cannot drift
        self.directory, self._owns_directory = resolve_directory(config, directory)
        self.conns: Dict[str, Connection] = {}
        self.remote_agents: Dict[str, str] = {}  # urn -> conn_id that advertised it
        self.remote_directory: Dict[str, Tuple[Dict[str, Any], str]] = {}  # urn -> (reference, conn_id)
        self._seen: deque = deque(maxlen=1024)  # dedupe window
        self._seen_set: set = set()
        self._lock = threading.RLock()
        self._fragments: Dict[Tuple[str, str], Dict[int, bytes]] = {}
        self._fragments_meta: Dict[Tuple[str, str], Tuple[int, float]] = {}
        self._stop = False
        self._listener: Optional[socket.socket] = None
        self._threads: List[threading.Thread] = []

    # ------------------------------------------------------------------
    # lifecycle

    def start(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((self.cfg.host, self.cfg.port))
        self._listener.listen(64)
        self._listener.settimeout(0.2)
        # advertise the node itself as a service
        self.directory.advertise(
            self.cfg.self_urn,
            acnl_core.Reference(
                urn=self.cfg.self_urn,
                endpoints=[f"{self.cfg.host}:{self.cfg.port}"],
                protocol_version=acnl_core.PROTOCOL_VERSION,
                capabilities=["directory", "broker"],
            ),
            ttl=self.cfg.directory_ttl,
        )
        accept = threading.Thread(target=self._accept_loop, daemon=True, name="acnl-accept")
        timer = threading.Thread(target=self._timer_loop, daemon=True, name="acnl-timer")
        accept.start()
        timer.start()
        self._threads.extend([accept, timer])

    def stop(self) -> None:
        self._stop = True
        with self._lock:
            conns = list(self.conns.values())
        for c in conns:
            self._shutdown(c)
        if self._listener:
            try:
                self._listener.close()
            except OSError:
                pass
        closer = getattr(self.directory, "close", None)
        if self._owns_directory and closer is not None:
            try:
                closer()
            except Exception:
                pass

    def _shutdown(self, conn: Connection) -> None:
        try:
            conn.sock.close()
        except OSError:
            pass
        conn.dead = True

    def peer(self, host: str, port: int, timeout: float = 5.0) -> None:
        """Link to another broker as a federation peer. Registration is
        implicit: the link is a hello with role=broker, and the answering
        side pushes its whole directory table as notify frames."""
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(0.5)
        cid = new_id()
        conn = Connection(
            conn_id=cid,
            node=self,
            sock=sock,
            addr=(host, port),
            queue=deque(),
            queue_depth=self.cfg.queue_depth,
            writer=None,
            is_peer=True,
        )
        writer = threading.Thread(target=self._writer_loop, args=(conn,), daemon=True)
        conn.writer = writer
        with self._lock:
            self.conns[cid] = conn
        reader = threading.Thread(target=self._reader_loop, args=(conn,), daemon=True)
        reader.start()
        writer.start()
        conn.push(
            WireMessage(
                kind="hello",
                envelope=make_envelope(sender=self.cfg.self_urn, recipient="acnl:peer/peer/broker"),
                body={"role": "broker", "capabilities": ["directory", "broker"]},
            )
        )

    def _accept_loop(self) -> None:
        while not self._stop:
            try:
                sock, addr = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            sock.settimeout(0.5)
            cid = new_id()
            conn = Connection(
                conn_id=cid,
                node=self,
                sock=sock,
                addr=addr,
                queue=deque(),
                queue_depth=self.cfg.queue_depth,
                writer=None,
            )
            writer = threading.Thread(target=self._writer_loop, args=(conn,), daemon=True)
            conn.writer = writer
            with self._lock:
                self.conns[cid] = conn
            reader = threading.Thread(target=self._reader_loop, args=(conn,), daemon=True)
            reader.start()
            writer.start()

    def _writer_loop(self, conn: Connection) -> None:
        while not conn.dead:
            try:
                msg = conn.queue.popleft()
            except IndexError:
                time.sleep(0.02)
                continue
            try:
                value = msg.to_value()
                if self.sec is not None:
                    # every hop protects afresh: route/ttl mutated on forward
                    value = self.sec.protect(value)
                conn.sock.sendall(encode_frame(value))
            except (OSError, ValueError):
                conn.dead = True
                break
        conn.dead = True

    def _reader_loop(self, conn: Connection) -> None:
        decoder = FrameDecoder(self.cfg.max_frame)
        while not conn.dead and not self._stop:
            try:
                data = conn.sock.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break
            try:
                frames = decoder.feed(data)
            except CodecError as e:
                self._send_error(conn, malformed(str(e)))
                continue
            for value, _ in frames:
                if self.sec is not None:
                    try:
                        value = self.sec.verify(value)
                    except SecurityError as e:
                        # fail closed, in-protocol: the connection stays open
                        # and the refusal is a typed user error
                        self._refuse(conn, value, authentication_failed(str(e)))
                        continue
                self._handle_frame(conn, value)
        conn.dead = True
        evicted: List[str] = []
        with self._lock:
            self.conns.pop(conn.conn_id, None)
            # drop the entries this connection carried (its own agents and
            # everything its side of the federation advertised): a router
            # going down takes its table with it and the loss is announced
            for urn, cid in list(self.remote_agents.items()):
                if cid == conn.conn_id:
                    del self.remote_agents[urn]
            for urn, (_ref, cid) in list(self.remote_directory.items()):
                if cid == conn.conn_id:
                    del self.remote_directory[urn]
                    evicted.append(urn)
        for urn in evicted:
            self._broadcast_to_peers(conn, "withdrawn", urn, None)

    # ------------------------------------------------------------------
    # frame handling

    def _refuse(self, conn: "Connection", value: Any, body: ErrorBody) -> None:
        """Refuse a frame that failed before parsing: echo the sender's
        request_id when it is well-formed, so a waiting caller can match the
        refusal; fall back to a fresh id for hostile garbage."""
        rid = None
        if isinstance(value, dict):
            env = value.get("envelope")
            if isinstance(env, dict) and isinstance(env.get("request_id"), str) and REQUEST_ID_RE.match(
                env["request_id"]
            ):
                rid = env["request_id"]
        _err(conn, conn.sender or "acnl:unknown/unknown/unknown", body, request_id=rid)

    def _handle_frame(self, conn: Connection, value: Any) -> None:
        if self.sec is not None:
            # idempotent: opens a sealed body (also for values reassembled
            # from fragments, whose transport frames authenticated each piece)
            try:
                value = self.sec.open_value(value, require=self.sec.require_encryption)
            except SecurityError as e:
                self._refuse(conn, value, authentication_failed(str(e)))
                return
        try:
            msg = WireMessage.from_value(value)
        except MessageError as e:
            # echo the caller's request_id when the envelope is parseable:
            # a waiting caller must be able to match its own refusal
            self._refuse(conn, value, self._classify_message_error(e))
            return
        if msg.kind == "hello":
            self._handle_hello(conn, msg)
        elif msg.kind == "request":
            self._handle_request(conn, msg)
        elif msg.kind in ("reply", "error"):
            self._handle_reply(conn, msg)
        elif msg.kind == "notify":
            self._handle_notify(conn, msg)
        elif msg.kind == "fragment":
            self._handle_fragment(conn, msg)
        elif msg.kind == "close":
            conn.tracker.transition(ConnectionState.CLOSING)
            conn.tracker.transition(ConnectionState.CLOSED)
            conn.dead = True
            # a peer's close must be answered with our close, or the TCP
            # handle lingers half-open on both sides
            self._shutdown(conn)

    def _classify_message_error(self, e: MessageError) -> ErrorBody:
        """A message error from the contract is a message error; but a refusal
        of the operation layer names the word the peer sent and the legal set."""
        text = str(e)
        m = re.search(r"unknown operation ['\"]([^'\"]+)['\"]", text)
        if m:
            return unknown_verb(m.group(1), list(OPERATIONS))
        return malformed(text)

    def _handle_hello(self, conn: Connection, msg: WireMessage) -> None:
        body = msg.body or {}
        is_new = conn.sender is None
        if is_new:
            conn.sender = msg.envelope.sender
            conn.capabilities = list(body.get("capabilities", []))
            conn.is_peer = body.get("role") == "broker"
            conn.tracker.transition(ConnectionState.ESTABLISHED)
            # answer the greeting exactly once: replying to every hello makes
            # two peers ping-pong hello replies forever
            conn.push(
                WireMessage(
                    kind="hello",
                    envelope=make_envelope(
                        sender=self.cfg.self_urn,
                        recipient=msg.envelope.sender,
                        request_id=msg.envelope.request_id,
                        message_id=new_id(),
                    ),
                    body={
                        "urn": self.cfg.self_urn,
                        "role": "broker",
                        "capabilities": ["directory", "broker"],
                        "protocol_version": acnl_core.PROTOCOL_VERSION,
                    },
                )
            )
        if is_new and conn.is_peer:
            # a router that just linked is told the whole table (the router's
            # table, broadcast federation), including this broker itself
            for ref in self.directory.snapshot():
                conn.push(
                    WireMessage(
                        kind="notify",
                        envelope=make_envelope(
                            sender=self.cfg.self_urn,
                            recipient=conn.sender,
                            request_id=new_id(),
                            message_id=new_id(),
                        ),
                        body={"event": "registered", "urn": ref.urn, "reference": ref.to_dict()},
                    )
                )
        if is_new and not conn.is_peer:
            # implicit registration propagates: a reply addressed to this
            # agent must be routable from a peer broker two hops away
            with self._lock:
                self.remote_agents[conn.sender] = conn.conn_id
                self.remote_directory[conn.sender] = (
                    {
                        "urn": conn.sender,
                        "endpoints": [f"{body.get('host', 'unknown')}:{body.get('port', 0)}"]
                        if body.get("port")
                        else [],
                        "protocol_version": acnl_core.PROTOCOL_VERSION,
                        "capabilities": list(conn.capabilities or []),
                        "attributes": {},
                    },
                    conn.conn_id,
                )
            from acnl_core.addresses import Reference

            self._broadcast_to_peers(
                conn,
                "registered",
                conn.sender,
                Reference(
                    urn=conn.sender,
                    capabilities=list(conn.capabilities or []),
                ),
            )

    def _handle_request(self, conn: Connection, msg: WireMessage) -> None:
        # loop safety: refuse when this node is already in the route
        if self.cfg.self_urn in msg.envelope.route:
            _err(conn, msg.envelope.sender, loop_detected(msg.envelope.route, self.cfg.self_urn), msg.envelope.request_id)
            return
        # dedupe: (sender, message_id) — a retried refusal replays as a refusal
        key = (msg.envelope.sender, msg.envelope.message_id)
        with self._lock:
            if key in self._seen_set:
                return
            self._seen_set.add(key)
            self._seen.append(key)
        # TTL: decrement per hop; refuse when exhausted
        ttl = msg.envelope.ttl
        if ttl <= 1:
            _err(conn, msg.envelope.sender, hops_exhausted(ttl), msg.envelope.request_id)
            return
        op = msg.body.get("op")
        if op not in OPERATIONS:
            _err(conn, msg.envelope.sender, unknown_verb(op, list(OPERATIONS)), msg.envelope.request_id)
            return
        # authorization: default-deny when an ACL is configured, checked at
        # the routing entrance on every hop (forwarded requests included)
        if self.sec is not None and self.sec.acl is not None:
            try:
                self.sec.authorize(msg.envelope.sender, op, msg.envelope.recipient)
            except AuthorizationError as e:
                _err(conn, msg.envelope.sender, not_authorized(str(e)), msg.envelope.request_id)
                return
        # route by the envelope's recipient
        recipient = msg.envelope.recipient
        if recipient == self.cfg.self_urn:
            self._handle_local(conn, msg)
        elif recipient in self.conns_by_sender():
            # the recipient is a directly connected peer: forward as-is
            target = self.conns_by_sender()[recipient]
            self._forward(conn, msg, target)
        elif recipient in self.remote_agents:
            # a remote agent reached via the broker peer that advertised it
            cid = self.remote_agents[recipient]
            target = self.conns.get(cid)
            if target is None or target.dead:
                _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)
            else:
                self._forward(conn, msg, target)
        else:
            _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)

    def _handle_local(self, conn: Connection, msg: WireMessage) -> None:
        op = msg.body.get("op")
        req_id = msg.envelope.request_id
        handler = getattr(self, f"_op_{op}", None)
        if handler is None:
            _err(conn, msg.envelope.sender, unknown_verb(op, list(OPERATIONS)), req_id)
            return
        try:
            handler(conn, msg)
        except DuplicateName as e:
            _err(conn, msg.envelope.sender, duplicate_name(e.urn), req_id)
        except ValueError as e:
            _err(conn, msg.envelope.sender, ErrorBody(code="bad_operation", message=str(e)), req_id)

    # ------------------------------------------------------------------
    # the seven operations

    def _op_ping(self, conn: Connection, msg: WireMessage) -> None:
        self._reply(conn, msg, body={"op": "ping", "value": "pong", "at": time.time()})

    def _op_advertise(self, conn: Connection, msg: WireMessage) -> None:
        from acnl_core.addresses import Reference

        body = msg.body
        ttl = float(body.get("ttl", self.cfg.directory_ttl))
        ref = self._reference_from_body(body)
        self.directory.advertise(ref.urn, ref, ttl)
        # register the remote agent as reachable via this connection
        with self._lock:
            self.remote_agents[ref.urn] = conn.conn_id
        self._reply(conn, msg, body={"op": "advertise", "urn": ref.urn, "ttl": ttl})
        self._broadcast_to_peers(conn, "registered", ref.urn, ref)

    def _op_withdraw(self, conn: Connection, msg: WireMessage) -> None:
        urn = msg.body["urn"]
        self.directory.unbind(urn)
        with self._lock:
            self.remote_agents.pop(urn, None)
            # an explicit withdraw wins over the implicit registration the
            # agent got at hello — but only the owner's own entry: withdrawing
            # a name carried by a peer link must not desync the federation
            entry = self.remote_directory.get(urn)
            if entry is not None and entry[1] == conn.conn_id:
                self.remote_directory.pop(urn, None)
        self._reply(conn, msg, body={"op": "withdraw", "urn": urn})
        self._broadcast_to_peers(conn, "withdrawn", urn, None)

    def _op_bind(self, conn: Connection, msg: WireMessage) -> None:
        body = msg.body
        ref = self._reference_from_body(body)
        self.directory.bind(ref.urn, ref, overwrite=bool(body.get("overwrite", False)))
        self._reply(conn, msg, body={"op": "bind", "urn": ref.urn, "bound": True})

    def _reference_from_body(self, body):
        """Build a Reference from a request body. The body may carry a full
        reference document (``reference``) or flat fields; the document wins
        on conflicts."""
        from acnl_core.addresses import Reference

        doc = body.get("reference") or {}
        return Reference(
            urn=body["urn"],
            endpoints=doc.get("endpoints", list(body.get("endpoints", []))),
            protocol_version=doc.get("protocol_version", acnl_core.PROTOCOL_VERSION),
            capabilities=doc.get("capabilities", list(body.get("capabilities", []))),
            attributes=doc.get("attributes", dict(body.get("attributes", {}))),
        )

    def _op_resolve(self, conn: Connection, msg: WireMessage) -> None:
        urn = msg.body["urn"]
        ref = self.directory.resolve(urn)
        if ref is not None:
            self._reply(conn, msg, body={"op": "resolve", "urn": urn, "reference": ref.to_dict()})
            return
        # local miss: the federated table (learned from peer brokers)
        with self._lock:
            entry = self.remote_directory.get(urn)
        if entry is None:
            _err(conn, msg.envelope.sender, unknown_service(urn), msg.envelope.request_id)
            return
        self._reply(
            conn, msg, body={"op": "resolve", "urn": urn, "reference": entry[0], "origin": "federated"}
        )

    def _op_query(self, conn: Connection, msg: WireMessage) -> None:
        text = msg.body["filter"]
        ast = parse_query(text)
        refs = self.directory.query(ast)
        # federation: merge the remote table into the result, deduped by urn
        # (a local entry always wins — it is the one this broker can answer for)
        seen = {r.urn for r in refs}
        with self._lock:
            remote = [d for (d, _cid) in self.remote_directory.values()]
        from acnl_core.addresses import Reference

        for d in remote:
            if d and d.get("urn") not in seen:
                try:
                    cand = Reference.from_dict(d)
                except Exception:
                    continue
                import acnl_core.attributes as _attrs

                if _attrs._eval(ast, cand.to_dict()):
                    refs.append(cand)
                    seen.add(cand.urn)
        self._reply(
            conn, msg, body={"op": "query", "filter": text, "references": [r.to_dict() for r in refs]}
        )

    def _op_send(self, conn: Connection, msg: WireMessage) -> None:
        # sending to the broker itself is a local echo; the real routing was
        # decided by _handle_request (recipient != self_urn → forwarded)
        self._reply(conn, msg, body={"op": "send", "delivered": True, "urn": self.cfg.self_urn})

    # ------------------------------------------------------------------
    # replies, notifications, fragments

    def _handle_reply(self, conn: Connection, msg: WireMessage) -> None:
        # route by the envelope's recipient, never by request_id. A directly
        # connected recipient goes straight; otherwise the federated table
        # says which peer link carries the recipient's side of the network.
        recipient = msg.envelope.recipient
        target = self.conns_by_sender().get(recipient)
        if target is None:
            with self._lock:
                cid = self.remote_agents.get(recipient)
            target = self.conns.get(cid) if cid else None
        if target is None:
            _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)
            return
        self._forward(conn, msg, target)

    def _handle_notify(self, conn: Connection, msg: WireMessage) -> None:
        # a notification from a peer about an advertised/withdrawn service.
        # Loop guard: an event about something we know locally (our own
        # agents, our own urn) is a reverb of our own broadcast — ignore it
        # entirely, do not clobber the local mapping and do not rebroadcast.
        event = msg.body["event"]
        urn = msg.body["urn"]
        # the echo guard covers only LOCAL agents and our own urn — a peer
        # broker's urn is also a conn sender, but its snapshot entry is the
        # one thing we must import, not swallow
        local_senders = {c.sender for c in self.conns.values() if c.sender and not c.is_peer}
        known = urn == self.cfg.self_urn or urn in local_senders
        if known:
            return
        if event == "registered":
            with self._lock:
                self.remote_agents[urn] = conn.conn_id
                self.remote_directory[urn] = (msg.body.get("reference"), conn.conn_id)
        elif event == "withdrawn":
            with self._lock:
                self.remote_agents.pop(urn, None)
                self.remote_directory.pop(urn, None)
        self._broadcast_to_peers(conn, event, urn, msg.body.get("reference"))

    def _handle_fragment(self, conn: Connection, msg: WireMessage) -> None:
        body = msg.body
        req_id = body["request_id"]
        key = (conn.conn_id, req_id)
        total = body["total"]
        with self._lock:
            self._fragments.setdefault(key, {})
            self._fragments_meta[key] = (total, time.time())
            self._fragments[key][body["index"]] = base64.b64decode(body["data"])
            if len(self._fragments[key]) != total:
                return
            parts = self._fragments.pop(key)
            self._fragments_meta.pop(key, None)
        try:
            payload = json.loads(b"".join(parts[i] for i in range(total)).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            self._send_error(conn, malformed(f"fragment reassembly failed: {e}"))
            return
        self._handle_frame(conn, payload)

    def _send_error(self, conn: Connection, body: ErrorBody) -> None:
        conn.push(
            WireMessage(
                kind="error",
                envelope=make_envelope(
                    sender=self.cfg.self_urn,
                    recipient=conn.sender or "acnl:unknown/unknown/unknown",
                    request_id=new_id(),
                    message_id=new_id(),
                ),
                body=body.to_wire(),
            )
        )

    # ------------------------------------------------------------------
    # routing helpers

    def conns_by_sender(self) -> Dict[str, Connection]:
        with self._lock:
            return {c.sender: c for c in self.conns.values() if c.sender}

    def _forward(self, conn: Connection, msg: WireMessage, target: Connection) -> None:
        # each hop appends its own id to the route
        msg.envelope.route.append(self.cfg.self_urn)
        msg.envelope.ttl -= 1
        if not target.try_send(msg):
            _err(conn, msg.envelope.sender, queue_full(target.conn_id), msg.envelope.request_id)

    def _reply(self, conn: Connection, msg: WireMessage, body: Dict[str, Any]) -> None:
        conn.push(
            WireMessage(
                kind="reply",
                envelope=make_envelope(
                    sender=self.cfg.self_urn,
                    recipient=msg.envelope.sender,
                    request_id=msg.envelope.request_id,
                    message_id=new_id(),
                ),
                body=body,
            )
        )

    def _broadcast_to_peers(
        self, origin: Optional["Connection"], event: str, urn: str, ref, peers_only: bool = False
    ) -> None:
        from acnl_core.addresses import Reference

        origin_id = origin.conn_id if origin is not None else None
        with self._lock:
            targets = [
                c
                for c in self.conns.values()
                if c.conn_id != origin_id and (c.is_peer if peers_only else True)
            ]
        body = {"event": event, "urn": urn, "reference": ref.to_dict() if isinstance(ref, Reference) else ref}
        for peer in targets:
            peer.push(
                WireMessage(
                    kind="notify",
                    envelope=make_envelope(
                        sender=self.cfg.self_urn,
                        recipient=peer.sender or "acnl:unknown/unknown/unknown",
                        request_id=new_id(),
                        message_id=new_id(),
                    ),
                    body=body,
                )
            )

    # ------------------------------------------------------------------
    # timer: sweep evicted services + periodic re-advertise

    def _timer_loop(self) -> None:
        last_sweep = time.time()
        last_bcast = time.time()
        while not self._stop:
            time.sleep(0.5)
            now = time.time()
            if now - last_sweep >= self.cfg.sweep_interval:
                evicted = self.directory.sweep(now)
                for urn in evicted:
                    self._broadcast_to_peers(None, "withdrawn", urn, None)
                last_sweep = now
            if now - last_bcast >= self.cfg.broadcast_interval:
                self._re_advertise()
                last_bcast = now

    def _re_advertise(self) -> None:
        ref = self.directory.resolve(self.cfg.self_urn)
        if ref is None:
            return
        self.directory.advertise(self.cfg.self_urn, ref, self.cfg.directory_ttl)
        self._broadcast_to_peers(None, "registered", self.cfg.self_urn, ref)
        # federation: re-broadcast the learned table so entries propagate
        # hop by hop (a router that linked after the original announcement
        # still converges), excluding the peer that taught us the entry —
        # sending it back is an echo, not propagation
        with self._lock:
            learned = [(urn, d, cid) for urn, (d, cid) in self.remote_directory.items()]
        for urn, d, cid in learned:
            origin = self.conns.get(cid)
            if origin is None:
                continue
            self._broadcast_to_peers(origin, "registered", urn, d, peers_only=True)
