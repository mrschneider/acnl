"""The envelope: who is speaking, to whom, how far it may travel, and which
hop introduced it. Identity is a URN carrying runtime and framework — on an
incident the first question is "which participant is that", and the answer
is in the address, not in a registry query."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .errors import MessageError

URN_RE = re.compile(r"^[a-z][a-z0-9]*:[a-z0-9\-_.]+:[a-z0-9\-_.]+$")
URN_PART_RE = re.compile(r"^[a-z0-9\-_.]+$")


def valid_urn(urn: Any) -> bool:
    return isinstance(urn, str) and bool(URN_RE.match(urn))


@dataclass
class Envelope:
    """The routing envelope. `route` is the list of hops that already
    touched this message (loop safety)."""

    sender: str
    recipient: str
    request_id: str
    message_id: str
    op: str
    hops_left: int
    route: List[str] = field(default_factory=list)
    body: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender": self.sender,
            "recipient": self.recipient,
            "request_id": self.request_id,
            "message_id": self.message_id,
            "op": self.op,
            "hops_left": self.hops_left,
            "route": list(self.route),
            "body": dict(self.body),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Envelope":
        if not isinstance(d, dict):
            raise MessageError("envelope must be an object")
        required = ("sender", "recipient", "request_id", "message_id", "op", "hops_left")
        missing = [k for k in required if k not in d]
        if missing:
            raise MessageError(f"envelope missing required fields {missing}")
        for k in ("sender", "recipient"):
            if not valid_urn(d[k]):
                raise MessageError(f"envelope field {k!r} is not a valid urn: {d[k]!r}")
        if not valid_urn(d["request_id"]) and not isinstance(d["request_id"], str):
            raise MessageError("request_id must be a string")
        hops = d["hops_left"]
        if not isinstance(hops, int) or hops < 0:
            raise MessageError(f"hops_left must be a non-negative int, got {hops!r}")
        route = d.get("route", [])
        if not isinstance(route, list):
            raise MessageError("route must be a list of urns")
        for hop in route:
            if not valid_urn(hop):
                raise MessageError(f"route entry is not a valid urn: {hop!r}")
        return cls(
            sender=d["sender"],
            recipient=d["recipient"],
            request_id=d["request_id"],
            message_id=d["message_id"],
            op=d["op"],
            hops_left=hops,
            route=list(route),
            body=dict(d.get("body", {})),
        )


def make_envelope(
    sender: str,
    recipient: str,
    op: str,
    request_id: str,
    message_id: str,
    hops_left: int = 16,
    body: Dict[str, Any] | None = None,
    route: List[str] | None = None,
) -> Envelope:
    return Envelope(
        sender=sender,
        recipient=recipient,
        request_id=request_id,
        message_id=message_id,
        op=op,
        hops_left=hops_left,
        route=list(route or []),
        body=dict(body or {}),
    )


def revalidate_envelope(env: Envelope) -> Envelope:
    """Re-validate after a transport decodes a wire message, so the
    transport layer cannot smuggle an invalid envelope past the contract."""
    return Envelope.from_dict(env.to_dict())
