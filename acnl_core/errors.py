"""The error taxonomy. Every refusal is typed.

Three kinds, all typed:

1. **User error** — the operation ran and refused on business grounds; never
   retryable (same input, same result).
2. **System error** — infrastructure failure; carries a ``retryable`` flag.
   A caller that cannot tell the two apart either retries a poison message
   forever or gives up on a transient blip. That distinction is the whole value.
3. **Message error** — the bytes could not be parsed. Answered in-protocol and
   the connection stays open.

An unknown verb is refused naming the word the peer sent and listing the legal
set — that refusal is the feature.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

USER_CODES = (
    "unknown_verb",
    "unknown_service",
    "bad_operation",
    "duplicate_name",
    "denied",
    "auth_failed",
    "not_authorized",
    "loop_detected",
    "version_mismatch",
    "hops_exhausted",
    "target_unreachable",
)
SYSTEM_CODES = ("unavailable", "timeout", "queue_full", "internal")
MESSAGE_CODES = ("malformed_frame", "bad_json", "bad_frame")


@dataclass
class ErrorBody:
    """A typed error body. ``retryable`` is only meaningful for system errors."""

    code: str
    message: str
    retryable: bool = False
    kind: str = "user"
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.code in USER_CODES:
            self.kind = "user"
            self.retryable = False  # a business refusal is never retryable
        elif self.code in SYSTEM_CODES:
            self.kind = "system"
        elif self.code in MESSAGE_CODES:
            self.kind = "message"
            self.retryable = False
        else:
            raise ValueError(f"unknown error code {self.code!r}")
        if self.details is None:
            self.details = {}

    @classmethod
    def from_value(cls, value: Dict[str, Any]) -> "ErrorBody":
        return cls(
            code=value["code"],
            message=value["message"],
            retryable=bool(value.get("retryable", False)),
            details=value.get("details") or {},
        )

    def to_value(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "kind": self.kind,
            "details": self.details,
        }

    def to_wire(self) -> Dict[str, Any]:
        return self.to_value()


# --- helpers ------------------------------------------------------------

def unknown_verb(word: str, legal: List[str]) -> ErrorBody:
    return ErrorBody(
        code="unknown_verb",
        message=f"unknown operation {word!r}: legal set is {', '.join(legal)}",
        details={"sent": word, "legal": legal},
    )


def unknown_service(urn: str) -> ErrorBody:
    return ErrorBody(code="unknown_service", message=f"no service registered for {urn!r}")


def loop_detected(route: List[str], urn: str) -> ErrorBody:
    return ErrorBody(
        code="loop_detected",
        message=f"refusing to forward: {urn!r} is already in the route "
        f"(loop safety: each hop appends its own id to the route and refuses when it finds itself there)",
        details={"route": route},
    )


def hops_exhausted(ttl: int) -> ErrorBody:
    return ErrorBody(
        code="hops_exhausted",
        message=f"ttl {ttl} exhausted; refusing to forward further",
    )


def unavailable(what: str) -> ErrorBody:
    return ErrorBody(code="unavailable", message=f"unavailable: {what}", retryable=True)


def queue_full(what: str) -> ErrorBody:
    return ErrorBody(code="queue_full", message=f"outbound queue full: {what}", retryable=True)


def target_unreachable(urn: str) -> ErrorBody:
    return ErrorBody(
        code="target_unreachable",
        message=f"no route to {urn!r}: not connected and not a directory peer",
        retryable=False,
    )


def duplicate_name(urn: str) -> ErrorBody:
    return ErrorBody(
        code="duplicate_name",
        message=f"{urn!r} is already bound; a silent overwrite on bind lets a typo "
        "take over a name, so rebind requires overwrite=True",
    )


def malformed(reason: str) -> ErrorBody:
    return ErrorBody(code="malformed_frame", message=f"malformed frame: {reason}")


def authentication_failed(reason: str) -> ErrorBody:
    """The frame did not authenticate (missing/bad MAC, bad ciphertext).
    A user refusal: same input, same result — never retryable."""
    return ErrorBody(code="auth_failed", message=f"authentication failed: {reason}")


def not_authorized(detail: str) -> ErrorBody:
    """The sender authenticated but the ACL refuses this operation."""
    return ErrorBody(code="not_authorized", message=f"not authorized: {detail}")
