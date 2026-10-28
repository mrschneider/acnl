"""The reference client.

Harness rules that always break first, encoded:

- A stream event is usually also a Reply-shaped frame: ``wait_reply`` ignores
  notifications and keeps the call open; matching the first reply with the id
  returns the result.
- The server may call the client back on the same connection: the wait loop
  dispatches inbound ``request``s to a handler that replies.
- Fragments are reassembled before parsing, keyed by request id, and gaps and
  reordered runs are refused — an assembled-wrong message is worse than a
  failed one.
"""

from __future__ import annotations

import base64
import json
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from acnl_core.codec import CodecError, FrameDecoder, encode_frame
from acnl_core.errors import ErrorBody
from acnl_core.messages import (
    MAX_TTL,
    Envelope,
    MessageError,
    WireMessage,
    make_envelope,
    new_id,
)
from acnl_core.security import SecurityError

from .config import NodeConfig


class ClientError(Exception):
    """A user error came back from the peer: never retryable."""

    def __init__(self, body: Dict[str, Any]):
        self.body = body
        super().__init__(f"[{body.get('code')}] {body.get('message')}")


class SystemErrorReply(Exception):
    """A system error came back: carries ``retryable``."""

    def __init__(self, body: Dict[str, Any]):
        self.body = body
        super().__init__(f"[{body.get('code')}] {body.get('message')}")


class Client:
    def __init__(self, agent_urn: str, fragment_size: int = 4096, security=None):
        from acnl_core.addresses import parse_urn

        parse_urn(agent_urn)
        self.agent_urn = agent_urn
        self.fragment_size = fragment_size
        self.security = security  # acnl_core.security.SecurityPolicy | None
        self.rejected_auth = 0  # frames refused by the reader (tests + ops)
        self.sock: Optional[socket.socket] = None
        self.decoder = FrameDecoder()
        self.request_id = None
        self.reply: Optional[WireMessage] = None
        self.broker_urn: Optional[str] = None  # learned from the hello reply
        self.notifications: List[WireMessage] = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._handler: Optional[Callable[[WireMessage], Optional[Dict[str, Any]]]] = None
        self._fragments: Dict[str, Dict[int, bytes]] = {}
        self._closed = False
        self._reader: Optional[threading.Thread] = None

    def connect(self, host: str, port: int, timeout: float = 5.0) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(0.5)
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        self._send(
            WireMessage(
                kind="hello",
                envelope=make_envelope(sender=self.agent_urn, recipient="acnl:localhost/localhost/broker"),
                body={"capabilities": [], "host": host, "port": port},
            )
        )
        # the hello reply carries the broker's URN; wait for it before the
        # first call, otherwise the client races its own greeting
        deadline = time.time() + timeout
        while self.broker_urn is None and time.time() < deadline:
            time.sleep(0.02)
        if self.broker_urn is None:
            raise RuntimeError("no hello reply: broker did not greet within timeout")

    def set_handler(self, handler: Callable[[WireMessage], Optional[Dict[str, Any]]]) -> None:
        """Called with inbound requests; the handler returns the reply body
        (or None to not reply)."""
        self._handler = handler

    def on_notification(self, msg: WireMessage) -> None:
        self.notifications.append(msg)

    def close(self) -> None:
        self._closed = True
        if self.sock:
            self._send(
                WireMessage(
                    kind="close",
                    envelope=make_envelope(sender=self.agent_urn, recipient=self.broker_urn or "acnl:localhost/localhost/broker"),
                )
            )
            self.sock.close()

    # ------------------------------------------------------------------
    # operations

    def call(self, op: str, payload: Dict[str, Any], recipient: Optional[str] = None, timeout: float = 10.0) -> Dict[str, Any]:
        if recipient is None:
            recipient = self.broker_urn
        if recipient is None:
            raise RuntimeError("client is not connected: no broker URN learned yet")
        req_id = new_id()
        self.request_id = req_id
        self.reply = None
        self._event.clear()
        self._send(
            WireMessage(
                kind="request",
                envelope=make_envelope(sender=self.agent_urn, recipient=recipient, request_id=req_id, message_id=new_id()),
                body={"op": op, **payload},
            )
        )
        reply = self._wait_reply(req_id, timeout)
        if reply.kind == "error":
            body = reply.body
            if body.get("kind") == "system":
                raise SystemErrorReply(body)
            raise ClientError(body)
        return reply.body

    def ping(self, timeout: float = 5.0) -> Dict[str, Any]:
        return self.call("ping", {}, timeout=timeout)

    def advertise(self, urn: str, ref: Dict[str, Any], ttl: float = 60.0, timeout: float = 5.0) -> Dict[str, Any]:
        return self.call("advertise", {"urn": urn, "reference": ref, "ttl": ttl}, timeout=timeout)

    def withdraw(self, urn: str, timeout: float = 5.0) -> Dict[str, Any]:
        return self.call("withdraw", {"urn": urn}, timeout=timeout)

    def resolve(self, urn: str, timeout: float = 5.0) -> Dict[str, Any]:
        return self.call("resolve", {"urn": urn}, timeout=timeout)

    def query(self, filter: str, timeout: float = 5.0) -> List[Dict[str, Any]]:
        return self.call("query", {"filter": filter}, timeout=timeout)["references"]

    def bind(self, urn: str, ref: Dict[str, Any], overwrite: bool = False, timeout: float = 5.0) -> Dict[str, Any]:
        return self.call("bind", {"urn": urn, "reference": ref, "overwrite": overwrite}, timeout=timeout)

    def send(self, urn: str, payload: Any, timeout: float = 10.0) -> Dict[str, Any]:
        return self.call("send", {"payload": payload}, recipient=urn, timeout=timeout)

    # ------------------------------------------------------------------
    # internals

    def _send(self, msg: WireMessage) -> None:
        sock = self.sock
        if sock is None:
            raise RuntimeError("client is not connected")
        value = msg.to_value()
        if self.security is not None:
            value = self.security.protect(value)
        sock.sendall(encode_frame(value))

    def _wait_reply(self, req_id: str, timeout: float) -> WireMessage:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.reply is not None and self.reply.envelope.request_id == req_id:
                return self.reply
            time.sleep(0.02)
        raise TimeoutError(f"no reply to {req_id} within {timeout}s")

    def _read_loop(self) -> None:
        buf = b""
        sock = self.sock
        while not self._closed:
            try:
                data = sock.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                break
            try:
                frames = self.decoder.feed(data)
            except CodecError:
                break
            for value, _ in frames:
                if self.security is not None:
                    try:
                        value = self.security.unprotect(value)
                    except SecurityError:
                        # no error channel yet: refuse the frame, keep the
                        # connection (mirrors the node's fail-closed reader)
                        self.rejected_auth += 1
                        continue
                try:
                    msg = WireMessage.from_value(value)
                except MessageError:
                    continue
                self._dispatch(msg)

    def _dispatch(self, msg: WireMessage) -> None:
        if msg.kind == "hello":
            # the greeting carries the broker's URN; the client addresses it
            # with that URN from now on
            self.broker_urn = msg.body.get("urn")
            return
        if msg.kind == "notify":
            self.on_notification(msg)
            return
        if msg.kind == "fragment":
            self._reassemble(msg)
            return
        if msg.kind == "close":
            self._closed = True
            return
        if msg.kind == "request":
            if self._handler is not None:
                body = self._handler(msg)
                if body is not None:
                    self._send(
                        WireMessage(
                            kind="reply",
                            envelope=make_envelope(
                                sender=self.agent_urn,
                                recipient=msg.envelope.sender,
                                request_id=msg.envelope.request_id,
                                message_id=new_id(),
                            ),
                            body=body,
                        )
                    )
            return
        if msg.kind in ("reply", "error"):
            self.reply = msg

    def _reassemble(self, msg: WireMessage) -> None:
        body = msg.body
        if self.request_id is None or body["request_id"] != self.request_id:
            return
        self._fragments.setdefault(body["request_id"], {})
        self._fragments[body["request_id"]][body["index"]] = base64.b64decode(body["data"])
        if len(self._fragments[body["request_id"]]) == body["total"]:
            parts = self._fragments.pop(body["request_id"])
            payload = json.loads(b"".join(parts[i] for i in range(body["total"])).decode("utf-8"))
            self._dispatch(WireMessage.from_value(payload))

    _fragments: Dict[str, Dict[int, bytes]]
