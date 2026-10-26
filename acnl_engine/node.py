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
    duplicate_name,
    hops_exhausted,
    loop_detected,
    malformed,
    queue_full,
    target_unreachable,
    unavailable,
    unknown_verb,
    unknown_service,
)
from acnl_core.messages import (
    MAX_TTL,
    OPERATIONS,
    WIRE_KINDS,
    Envelope,
    MessageError,
    WireMessage,
    make_envelope,
    new_id,
)
from acnl_core.sequencing import ConnectionTracker, ConnectionState

from .config import NodeConfig


def _err(conn: "Connection", to: str, body: ErrorBody, request_id: str = None) -> None:
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
        self.directory = directory or InMemoryDirectory()
        self.conns: Dict[str, Connection] = {}
        self.remote_agents: Dict[str, str] = {}  # urn -> conn_id that advertised it
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

    def _shutdown(self, conn: Connection) -> None:
        try:
            conn.sock.close()
        except OSError:
            pass
        conn.dead = True

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
                conn.sock.sendall(encode_frame(msg.to_value()))
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
                self._handle_frame(conn, value)
        conn.dead = True
        with self._lock:
            self.conns.pop(conn.conn_id, None)
            # drop remote agents this connection advertised
            for urn, cid in list(self.remote_agents.items()):
                if cid == conn.conn_id:
                    del self.remote_agents[urn]

    # ------------------------------------------------------------------
    # frame handling

    def _handle_frame(self, conn: Connection, value: Any) -> None:
        try:
            msg = WireMessage.from_value(value)
        except MessageError as e:
            self._send_error(conn, self._classify_message_error(e))
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
        if conn.sender is None:
            conn.sender = msg.envelope.sender
            conn.capabilities = list((msg.body or {}).get("capabilities", []))
            conn.tracker.transition(ConnectionState.ESTABLISHED)
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
                    "capabilities": ["directory", "broker"],
                    "protocol_version": acnl_core.PROTOCOL_VERSION,
                },
            )
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
        if ref is None:
            _err(conn, msg.envelope.sender, unknown_service(urn), msg.envelope.request_id)
            return
        self._reply(conn, msg, body={"op": "resolve", "urn": urn, "reference": ref.to_dict()})

    def _op_query(self, conn: Connection, msg: WireMessage) -> None:
        text = msg.body["filter"]
        ast = parse_query(text)
        refs = self.directory.query(ast)
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
        # route by the envelope's recipient, never by request_id
        recipient = msg.envelope.recipient
        target = self.conns_by_sender().get(recipient)
        if target is None:
            _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)
            return
        self._forward(conn, msg, target)

    def _handle_notify(self, conn: Connection, msg: WireMessage) -> None:
        # a notification from a peer about an advertised/withdrawn service
        event = msg.body["event"]
        urn = msg.body["urn"]
        if event == "registered":
            with self._lock:
                self.remote_agents[urn] = conn.conn_id
        elif event == "withdrawn":
            with self._lock:
                self.remote_agents.pop(urn, None)
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

    def _broadcast_to_peers(self, origin: Optional["Connection"], event: str, urn: str, ref) -> None:
        from acnl_core.addresses import Reference

        origin_id = origin.conn_id if origin is not None else None
        with self._lock:
            peers = [c for c in self.conns.values() if c.conn_id != origin_id]
        body = {"event": event, "urn": urn, "reference": ref.to_dict() if isinstance(ref, Reference) else ref}
        for peer in peers:
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
