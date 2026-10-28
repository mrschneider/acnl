"""The asyncio reference node: same wire, same contract, one event loop.

``Node`` (threaded) and ``AsyncNode`` speak the identical wire format and
enforce the identical contract — validation at one entrance, route by
recipient, loop safety, ``(sender, message_id)`` dedupe, per-hop TTL,
typed refusals, backpressure, security (verify -> open -> authorize), and
the federation rules. What changes is concurrency:

- one bounded ``asyncio.Queue`` + one writer *task* per connection replaces
  the bounded deque + writer thread (R6 holds: a full queue maps to a
  retryable ``queue_full`` without ever blocking the loop);
- the ``threading.RLock`` disappears — a single event loop owns all state,
  so there are no data races to lock against;
- timers (sweep, periodic re-advertise, table broadcast) are one task with
  ``asyncio.sleep``.

``acnl_core`` stays IO-free: this module is transport, like ``node.py``.
"""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import acnl_core
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
    unknown_verb,
    unknown_service,
)
from acnl_core.messages import (
    OPERATIONS,
    REQUEST_ID_RE,
    MessageError,
    WireMessage,
    make_envelope,
    new_id,
)
from acnl_core.sequencing import ConnectionTracker, ConnectionState
from acnl_core.security import AuthorizationError, SecurityError

from .config import NodeConfig, resolve_directory
from .node import _err  # the typed-refusal builder is shared, not copied


@dataclass
class AsyncConnection:
    """One live TCP connection: a bounded queue and a writer task."""

    conn_id: str
    node: "AsyncNode"
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    queue: asyncio.Queue
    tracker: ConnectionTracker = field(default_factory=ConnectionTracker)
    sender: Optional[str] = None  # learned from the first hello
    capabilities: List[str] = field(default_factory=list)
    is_peer: bool = False
    dead: bool = False
    write_task: Optional[asyncio.Task] = None
    read_task: Optional[asyncio.Task] = None

    def push(self, msg: WireMessage) -> bool:
        if self.dead:
            return False
        try:
            self.queue.put_nowait(msg)
        except asyncio.QueueFull:
            return False
        return True

    try_send = push


class AsyncNode:
    """Directory service + routed broker, asyncio edition."""

    def __init__(self, config: NodeConfig, directory: Optional[Directory] = None):
        self.cfg = config
        self.sec = config.security
        self.directory, self._owns_directory = resolve_directory(config, directory)
        self.conns: Dict[str, AsyncConnection] = {}
        self.remote_agents: Dict[str, str] = {}
        self.remote_directory: Dict[str, Tuple[Dict[str, Any], str]] = {}
        self._seen: deque = deque(maxlen=1024)
        self._seen_set: set = set()
        self._fragments: Dict[Tuple[str, str], Dict[int, bytes]] = {}
        self._fragments_meta: Dict[Tuple[str, str], Tuple[int, float]] = {}
        self._server: Optional[asyncio.Server] = None
        self._tasks: List[asyncio.Task] = []
        self._stop = False
        self.port: Optional[int] = None  # real port after start()

    # -- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._on_client, self.cfg.host, self.cfg.port
        )
        self.port = self._server.sockets[0].getsockname()[1]
        self.directory.advertise(
            self.cfg.self_urn,
            acnl_core.Reference(
                urn=self.cfg.self_urn,
                endpoints=[f"{self.cfg.host}:{self.port}"],
                protocol_version=acnl_core.PROTOCOL_VERSION,
                capabilities=["directory", "broker"],
            ),
            ttl=self.cfg.directory_ttl,
        )
        self._tasks.append(asyncio.create_task(self._timer_loop()))

    async def stop(self) -> None:
        self._stop = True
        for task in self._tasks:
            task.cancel()
        for conn in list(self.conns.values()):
            await self._on_disconnect(conn)
        if self._server is not None:
            self._server.close()
            try:
                # belt and suspenders: a stray handler must not hang stop()
                await asyncio.wait_for(self._server.wait_closed(), timeout=3.0)
            except (asyncio.TimeoutError, Exception):
                pass
        if self._owns_directory:
            closer = getattr(self.directory, "close", None)
            if closer is not None:
                try:
                    closer()
                except Exception:
                    pass

    async def peer(self, host: str, port: int) -> None:
        """Link to another broker (threaded or asyncio — same wire).
        Non-blocking, exactly like the threaded node's peer(): the read
        loop runs as its own task and performs the disconnect cleanup when
        the link dies."""
        reader, writer = await asyncio.open_connection(host, port)
        conn = self._register_conn(reader, writer, is_peer=True)
        conn.push(
            WireMessage(
                kind="hello",
                envelope=make_envelope(sender=self.cfg.self_urn, recipient="acnl:peer/peer/broker"),
                body={"role": "broker", "capabilities": ["directory", "broker"]},
            )
        )
        conn.read_task = asyncio.create_task(self._peer_read(conn))

    async def _peer_read(self, conn: AsyncConnection) -> None:
        try:
            await self._read_loop(conn)
        finally:
            await self._on_disconnect(conn)

    # -- connection plumbing ---------------------------------------------
    def _register_conn(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        is_peer: bool = False,
    ) -> AsyncConnection:
        conn = AsyncConnection(
            conn_id=new_id(),
            node=self,
            reader=reader,
            writer=writer,
            queue=asyncio.Queue(maxsize=self.cfg.queue_depth),
            is_peer=is_peer,
        )
        self.conns[conn.conn_id] = conn
        conn.write_task = asyncio.create_task(self._writer_loop(conn))
        return conn

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = self._register_conn(reader, writer)
        await self._read_loop(conn)
        await self._on_disconnect(conn)

    async def _writer_loop(self, conn: AsyncConnection) -> None:
        try:
            while not conn.dead and not self._stop:
                msg = await conn.queue.get()
                value = msg.to_value()
                if self.sec is not None:
                    value = self.sec.protect(value)
                conn.writer.write(encode_frame(value))
                await conn.writer.drain()
        except (ConnectionError, OSError, SecurityError, asyncio.CancelledError):
            pass
        finally:
            conn.dead = True

    async def _read_loop(self, conn: AsyncConnection) -> None:
        decoder = FrameDecoder(self.cfg.max_frame)
        while not conn.dead and not self._stop:
            try:
                data = await conn.reader.read(65536)
            except (ConnectionError, OSError):
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
                        self._refuse(conn, value, authentication_failed(str(e)))
                        continue
                await self._handle_frame(conn, value)
        conn.dead = True

    async def _on_disconnect(self, conn: AsyncConnection) -> None:
        """A peer going down takes its table with it; the loss is announced."""
        if conn.write_task is not None:
            conn.write_task.cancel()
        if conn.read_task is not None and conn.read_task is not asyncio.current_task():
            conn.read_task.cancel()
        self.conns.pop(conn.conn_id, None)
        evicted: List[str] = []
        for urn, cid in list(self.remote_agents.items()):
            if cid == conn.conn_id:
                del self.remote_agents[urn]
        for urn, (_ref, cid) in list(self.remote_directory.items()):
            if cid == conn.conn_id:
                del self.remote_directory[urn]
                evicted.append(urn)
        for urn in evicted:
            self._broadcast_to_peers(conn, "withdrawn", urn, None)
        try:
            conn.writer.close()
        except Exception:
            pass

    # -- frame handling ---------------------------------------------------
    def _refuse(self, conn: AsyncConnection, value: Any, body: ErrorBody) -> None:
        """Echo a well-formed request_id so a waiting caller can match the
        refusal (same rule as the threaded node)."""
        rid = None
        if isinstance(value, dict):
            env = value.get("envelope")
            if (
                isinstance(env, dict)
                and isinstance(env.get("request_id"), str)
                and REQUEST_ID_RE.match(env["request_id"])
            ):
                rid = env["request_id"]
        _err(conn, conn.sender or "acnl:unknown/unknown/unknown", body, request_id=rid)

    async def _handle_frame(self, conn: AsyncConnection, value: Any) -> None:
        if self.sec is not None:
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
            await self._handle_hello(conn, msg)
        elif msg.kind == "request":
            await self._handle_request(conn, msg)
        elif msg.kind in ("reply", "error"):
            self._handle_reply(conn, msg)
        elif msg.kind == "notify":
            self._handle_notify(conn, msg)
        elif msg.kind == "fragment":
            await self._handle_fragment(conn, msg)
        elif msg.kind == "close":
            conn.tracker.transition(ConnectionState.CLOSING)
            conn.tracker.transition(ConnectionState.CLOSED)
            conn.dead = True

    def _classify_message_error(self, e: MessageError) -> ErrorBody:
        import re

        m = re.search(r"unknown operation ['\"]([^'\"]+)['\"]", str(e))
        if m:
            return unknown_verb(m.group(1), list(OPERATIONS))
        return malformed(str(e))

    async def _handle_hello(self, conn: AsyncConnection, msg: WireMessage) -> None:
        body = msg.body or {}
        sender = msg.envelope.sender  # validated non-None by Envelope
        is_new = conn.sender is None
        if is_new:
            conn.sender = sender
            conn.capabilities = list(body.get("capabilities", []))
            conn.is_peer = body.get("role") == "broker"
            conn.tracker.transition(ConnectionState.ESTABLISHED)
            conn.push(
                WireMessage(
                    kind="hello",
                    envelope=make_envelope(
                        sender=self.cfg.self_urn,
                        recipient=sender,
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
            for ref in self.directory.snapshot():
                conn.push(
                    WireMessage(
                        kind="notify",
                        envelope=make_envelope(
                            sender=self.cfg.self_urn,
                            recipient=sender,
                            request_id=new_id(),
                            message_id=new_id(),
                        ),
                        body={"event": "registered", "urn": ref.urn, "reference": ref.to_dict()},
                    )
                )
        if is_new and not conn.is_peer:
            from acnl_core.addresses import Reference

            self.remote_agents[sender] = conn.conn_id
            self.remote_directory[sender] = (
                {
                    "urn": sender,
                    "endpoints": [f"{body.get('host', 'unknown')}:{body.get('port', 0)}"]
                    if body.get("port")
                    else [],
                    "protocol_version": acnl_core.PROTOCOL_VERSION,
                    "capabilities": list(conn.capabilities or []),
                    "attributes": {},
                },
                conn.conn_id,
            )
            self._broadcast_to_peers(
                conn,
                "registered",
                sender,
                Reference(urn=sender, capabilities=list(conn.capabilities)),
            )

    async def _handle_request(self, conn: AsyncConnection, msg: WireMessage) -> None:
        if self.cfg.self_urn in msg.envelope.route:
            _err(conn, msg.envelope.sender,
                 loop_detected(list(msg.envelope.route or []), self.cfg.self_urn),
                 msg.envelope.request_id)
            return
        key = (msg.envelope.sender, msg.envelope.message_id)
        if key in self._seen_set:
            return
        self._seen_set.add(key)
        self._seen.append(key)
        ttl = msg.envelope.ttl
        if ttl <= 1:
            _err(conn, msg.envelope.sender, hops_exhausted(ttl), msg.envelope.request_id)
            return
        op = msg.body.get("op")
        if op not in OPERATIONS:
            _err(conn, msg.envelope.sender, unknown_verb(op, list(OPERATIONS)), msg.envelope.request_id)
            return
        if self.sec is not None and self.sec.acl is not None:
            try:
                self.sec.authorize(msg.envelope.sender, op, msg.envelope.recipient)
            except AuthorizationError as e:
                _err(conn, msg.envelope.sender, not_authorized(str(e)), msg.envelope.request_id)
                return
        recipient = msg.envelope.recipient
        if recipient == self.cfg.self_urn:
            await self._handle_local(conn, msg)
        elif recipient in self.conns_by_sender():
            self._forward(conn, msg, self.conns_by_sender()[recipient])
        elif recipient in self.remote_agents:
            target = self.conns.get(self.remote_agents[recipient])
            if target is None or target.dead:
                _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)
            else:
                self._forward(conn, msg, target)
        else:
            _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)

    async def _handle_local(self, conn: AsyncConnection, msg: WireMessage) -> None:
        op = msg.body.get("op")
        req_id = msg.envelope.request_id
        handler = getattr(self, f"_op_{op}", None)
        if handler is None:
            _err(conn, msg.envelope.sender, unknown_verb(op, list(OPERATIONS)), req_id)
            return
        try:
            await handler(conn, msg)
        except DuplicateName as e:
            _err(conn, msg.envelope.sender, duplicate_name(e.urn), req_id)
        except ValueError as e:
            _err(conn, msg.envelope.sender, ErrorBody(code="bad_operation", message=str(e)), req_id)

    # -- the seven operations ----------------------------------------------
    async def _op_ping(self, conn: AsyncConnection, msg: WireMessage) -> None:
        self._reply(conn, msg, body={"op": "ping", "value": "pong", "at": time.time()})

    async def _op_advertise(self, conn: AsyncConnection, msg: WireMessage) -> None:
        from acnl_core.addresses import Reference

        body = msg.body
        ttl = float(body.get("ttl", self.cfg.directory_ttl))
        ref = self._reference_from_body(body)
        self.directory.advertise(ref.urn, ref, ttl)
        self.remote_agents[ref.urn] = conn.conn_id
        self._reply(conn, msg, body={"op": "advertise", "urn": ref.urn, "ttl": ttl})
        self._broadcast_to_peers(conn, "registered", ref.urn, ref)

    async def _op_withdraw(self, conn: AsyncConnection, msg: WireMessage) -> None:
        urn = msg.body["urn"]
        self.directory.unbind(urn)
        self.remote_agents.pop(urn, None)
        entry = self.remote_directory.get(urn)
        if entry is not None and entry[1] == conn.conn_id:
            self.remote_directory.pop(urn, None)
        self._reply(conn, msg, body={"op": "withdraw", "urn": urn})
        self._broadcast_to_peers(conn, "withdrawn", urn, None)

    async def _op_bind(self, conn: AsyncConnection, msg: WireMessage) -> None:
        body = msg.body
        ref = self._reference_from_body(body)
        self.directory.bind(ref.urn, ref, overwrite=bool(body.get("overwrite", False)))
        self._reply(conn, msg, body={"op": "bind", "urn": ref.urn, "bound": True})

    def _reference_from_body(self, body):
        from acnl_core.addresses import Reference

        doc = body.get("reference") or {}
        return Reference(
            urn=body["urn"],
            endpoints=doc.get("endpoints", list(body.get("endpoints", []))),
            protocol_version=doc.get("protocol_version", acnl_core.PROTOCOL_VERSION),
            capabilities=doc.get("capabilities", list(body.get("capabilities", []))),
            attributes=doc.get("attributes", dict(body.get("attributes", {}))),
        )

    async def _op_resolve(self, conn: AsyncConnection, msg: WireMessage) -> None:
        urn = msg.body["urn"]
        ref = self.directory.resolve(urn)
        if ref is not None:
            self._reply(conn, msg, body={"op": "resolve", "urn": urn, "reference": ref.to_dict()})
            return
        entry = self.remote_directory.get(urn)
        if entry is None:
            _err(conn, msg.envelope.sender, unknown_service(urn), msg.envelope.request_id)
            return
        self._reply(
            conn, msg, body={"op": "resolve", "urn": urn, "reference": entry[0], "origin": "federated"}
        )

    async def _op_query(self, conn: AsyncConnection, msg: WireMessage) -> None:
        from acnl_core.addresses import Reference
        import acnl_core.attributes as _attrs

        text = msg.body["filter"]
        ast = _attrs.parse_query(text) if hasattr(_attrs, "parse_query") else None
        if ast is None:
            from acnl_core.attributes import parse_query

            ast = parse_query(text)
        refs = self.directory.query(ast)
        seen = {r.urn for r in refs}
        for d in [dr for (dr, _cid) in self.remote_directory.values()]:
            if d and d.get("urn") not in seen:
                try:
                    cand = Reference.from_dict(d)
                except Exception:
                    continue
                if _attrs._eval(ast, cand.to_dict()):
                    refs.append(cand)
                    seen.add(cand.urn)
        self._reply(
            conn, msg, body={"op": "query", "filter": text, "references": [r.to_dict() for r in refs]}
        )

    async def _op_send(self, conn: AsyncConnection, msg: WireMessage) -> None:
        self._reply(conn, msg, body={"op": "send", "delivered": True, "urn": self.cfg.self_urn})

    # -- replies, notifications, fragments ---------------------------------
    def _handle_reply(self, conn: AsyncConnection, msg: WireMessage) -> None:
        recipient = msg.envelope.recipient
        target = self.conns_by_sender().get(recipient)
        if target is None:
            cid = self.remote_agents.get(recipient)
            target = self.conns.get(cid) if cid else None
        if target is None:
            _err(conn, msg.envelope.sender, target_unreachable(recipient), msg.envelope.request_id)
            return
        self._forward(conn, msg, target)

    def _handle_notify(self, conn: AsyncConnection, msg: WireMessage) -> None:
        event = msg.body["event"]
        urn = msg.body["urn"]
        local_senders = {c.sender for c in self.conns.values() if c.sender and not c.is_peer}
        if urn == self.cfg.self_urn or urn in local_senders:
            return  # a reverb of our own broadcast: never clobbers local
        if event == "registered":
            self.remote_agents[urn] = conn.conn_id
            self.remote_directory[urn] = (msg.body.get("reference"), conn.conn_id)
        elif event == "withdrawn":
            self.remote_agents.pop(urn, None)
            self.remote_directory.pop(urn, None)
        self._broadcast_to_peers(conn, event, urn, msg.body.get("reference"))

    async def _handle_fragment(self, conn: AsyncConnection, msg: WireMessage) -> None:
        body = msg.body
        key = (conn.conn_id, body["request_id"])
        total = body["total"]
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
        await self._handle_frame(conn, payload)

    def _send_error(self, conn: AsyncConnection, body: ErrorBody) -> None:
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

    # -- routing helpers ----------------------------------------------------
    def conns_by_sender(self) -> Dict[str, AsyncConnection]:
        return {c.sender: c for c in self.conns.values() if c.sender}

    def _forward(self, conn: AsyncConnection, msg: WireMessage, target: AsyncConnection) -> None:
        msg.envelope.route.append(self.cfg.self_urn)
        msg.envelope.ttl -= 1
        if not target.push(msg):
            _err(conn, msg.envelope.sender, queue_full(target.conn_id), msg.envelope.request_id)

    def _reply(self, conn: AsyncConnection, msg: WireMessage, body: Dict[str, Any]) -> None:
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
        self,
        origin: Optional[AsyncConnection],
        event: str,
        urn: str,
        ref,
        peers_only: bool = False,
    ) -> None:
        from acnl_core.addresses import Reference

        origin_id = origin.conn_id if origin is not None else None
        targets = [
            c
            for c in self.conns.values()
            if c.conn_id != origin_id and (c.is_peer if peers_only else True)
        ]
        body = {
            "event": event,
            "urn": urn,
            "reference": ref.to_dict() if isinstance(ref, Reference) else ref,
        }
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

    # -- timers ---------------------------------------------------------------
    async def _timer_loop(self) -> None:
        last_sweep = time.time()
        last_bcast = time.time()
        while not self._stop:
            await asyncio.sleep(0.5)
            now = time.time()
            if now - last_sweep >= self.cfg.sweep_interval:
                for urn in self.directory.sweep(now):
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
        for urn, d, cid in [(u, dr, c) for u, (dr, c) in self.remote_directory.items()]:
            origin = self.conns.get(cid)
            if origin is None:
                continue
            self._broadcast_to_peers(origin, "registered", urn, d, peers_only=True)
