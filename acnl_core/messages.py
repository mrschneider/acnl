"""The message layer: a closed vocabulary, validated at one entrance.

Two layers, kept separate:

1. **Message layer** (transport): ``hello``, ``request``, ``reply``, ``error``,
   ``notify``, ``fragment``, ``close`` — seven wire kinds, no more.
2. **Operation layer** (the vocabulary): ``advertise``, ``withdraw``,
   ``resolve``, ``query``, ``send``, ``bind``, ``ping`` — seven verbs, carried
   inside ``request``/``reply`` bodies.

An unknown kind or verb is refused *naming the word the peer sent and listing
the legal set*. That refusal is the feature: a protocol that quietly accepts
anything else needs a translator per pair of frameworks.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .addresses import PROTOCOL_MAJOR, AddressError, parse_urn

WIRE_KINDS = ("hello", "request", "reply", "error", "notify", "fragment", "close")
OPERATIONS = ("advertise", "withdraw", "resolve", "query", "send", "bind", "ping")
MAX_TTL = 16

REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class MessageError(ValueError):
    """The bytes could not be parsed into a message. Answered in-protocol."""

    def __init__(self, message: str, detail: Any = None):
        super().__init__(message)
        self.detail = detail


@dataclass
class Envelope:
    """Routing metadata: who sent, who receives, how many hops are left."""

    sender: str
    recipient: str
    request_id: str
    message_id: str
    ttl: int = MAX_TTL
    route: Optional[List[str]] = None
    version: Optional[str] = None

    def __post_init__(self):
        parse_urn(self.sender)
        parse_urn(self.recipient)
        if not REQUEST_ID_RE.match(self.request_id or ""):
            raise MessageError(f"invalid request_id {self.request_id!r}")
        if not REQUEST_ID_RE.match(self.message_id or ""):
            raise MessageError(f"invalid message_id {self.message_id!r}")
        if not isinstance(self.ttl, int) or self.ttl < 1 or self.ttl > MAX_TTL:
            raise MessageError(f"invalid ttl {self.ttl!r}: must be an int in 1..{MAX_TTL}")
        if self.route is None:
            self.route = []
        else:
            self.route = list(self.route)
        if self.version is None:
            self.version = f"{PROTOCOL_MAJOR}.0"
        else:
            parts = self.version.split(".")
            if len(parts) != 2 or not all(p.isdigit() for p in parts):
                raise MessageError(f"invalid version {self.version!r}")
            if int(parts[0]) != PROTOCOL_MAJOR:
                raise MessageError(
                    f"version mismatch: peer sent {self.version!r}, "
                    f"this protocol speaks major {PROTOCOL_MAJOR}.x "
                    "(a different major is refused; a different minor is accepted as additive)"
                )

    @classmethod
    def from_value(cls, value: Dict[str, Any]) -> "Envelope":
        missing = [k for k in ("sender", "recipient", "request_id", "message_id") if k not in value]
        if missing:
            raise MessageError(f"envelope missing required fields: {missing}")
        try:
            return cls(
                sender=value["sender"],
                recipient=value["recipient"],
                request_id=value["request_id"],
                message_id=value["message_id"],
                ttl=value.get("ttl", MAX_TTL),
                route=list(value.get("route", [])),
                version=value.get("version"),
            )
        except AddressError as e:
            raise MessageError(f"envelope invalid: {e}") from e

    def to_value(self) -> Dict[str, Any]:
        return {
            "sender": self.sender,
            "recipient": self.recipient,
            "request_id": self.request_id,
            "message_id": self.message_id,
            "ttl": self.ttl,
            "route": list(self.route),
            "version": self.version,
        }


@dataclass
class WireMessage:
    """A transport frame: kind + envelope + body."""

    kind: str
    envelope: Envelope
    body: Any = None

    @classmethod
    def from_value(cls, value: Dict[str, Any]) -> "WireMessage":
        if not isinstance(value, dict):
            raise MessageError(f"message must be a dict, got {type(value).__name__}")
        kind = value.get("kind")
        if kind not in WIRE_KINDS:
            raise MessageError(
                f"unknown wire kind {kind!r}: legal set is {', '.join(WIRE_KINDS)}"
            )
        if "envelope" not in value:
            raise MessageError("message missing required field 'envelope'")
        try:
            envelope = Envelope.from_value(value["envelope"])
        except MessageError as e:
            raise MessageError(f"bad envelope: {e}") from e
        body = value.get("body")
        if body is None and kind != "close":
            raise MessageError(f"message kind {kind!r} requires a body")
        # validate at one entrance: the operation layer inside request/reply
        if kind in ("request", "reply"):
            if not isinstance(body, dict):
                raise MessageError(f"{kind} body must be a dict")
            op = body.get("op")
            if op not in OPERATIONS:
                raise MessageError(
                    f"unknown operation {op!r}: legal set is {', '.join(OPERATIONS)}"
                )
        if kind == "error":
            if not isinstance(body, dict):
                raise MessageError("error body must be a dict")
            missing = [k for k in ("code", "message", "retryable") if k not in body]
            if missing:
                raise MessageError(f"error body missing required fields: {missing}")
        if kind == "fragment":
            if not isinstance(body, dict):
                raise MessageError("fragment body must be a dict")
            for k in ("request_id", "index", "total", "data"):
                if k not in body:
                    raise MessageError(f"fragment body missing required field {k!r}")
            if not isinstance(body["index"], int) or not isinstance(body["total"], int):
                raise MessageError("fragment index/total must be ints")
            if not isinstance(body["data"], str):
                raise MessageError("fragment data must be a base64 string")
        # re-validate the envelope embedded in a wire message
        if kind == "hello":
            if not isinstance(body, dict):
                raise MessageError("hello body must be a dict")
        if kind == "notify":
            if not isinstance(body, dict):
                raise MessageError("notify body must be a dict")
            for k in ("event", "urn", "reference"):
                if k not in body:
                    raise MessageError(f"notify body missing required field {k!r}")
        return cls(kind=kind, envelope=envelope, body=body)

    def to_value(self) -> Dict[str, Any]:
        out = {"kind": self.kind, "envelope": self.envelope.to_value(), "body": self.body}
        if self.kind == "close":
            out.pop("body", None)
        return out


def new_id() -> str:
    return uuid.uuid4().hex[:24]


def make_envelope(sender: str, recipient: str, **kwargs) -> Envelope:
    kwargs.setdefault("request_id", new_id())
    kwargs.setdefault("message_id", new_id())
    return Envelope(sender=sender, recipient=recipient, **kwargs)
