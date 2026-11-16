"""Addressing, naming, and reference documents.

An identity is a URN with three hierarchical components:
    acnl:<zone>/<node>/<agent>

A reference document is what a directory stores:
    urn, endpoints (failover order), protocol_version, capabilities, attributes.

A short form is what a human types:
    acnl://<host>:<port>/<urn>
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

PROTOCOL_VERSION = "1.0"
PROTOCOL_MAJOR = 1

URN_RE = re.compile(
    r"^acnl:([a-z0-9][a-z0-9._-]{0,62})/([a-z0-9][a-z0-9._-]{0,62})/([a-z0-9][a-z0-9._-]{0,62})$"
)
ENDPOINT_RE = re.compile(r"^([a-zA-Z0-9.:\-]+):(\d{1,5})$")

# A bounded LRU cache for :func:`parse_urn`. A single URN is parsed twice per
# envelope (sender + recipient) and once per inbound frame, so the regex is
# the single most-called function on a hot node. The cache is keyed by the
# URN string and holds a sentinel for *invalid* URNs too, so a malformed
# address is rejected once and the rejection is cached. Bounded so a workload
# with many distinct URNs cannot grow it without limit.
from collections import OrderedDict as _OrderedDict
import threading as _threading

URNCACHE_MAX = 512
URNCACHE_LOCK = _threading.RLock()
URNCACHE_MISS = object()  # sentinel: invalid URN, stored value is this object
URNCACHE: "OrderedDict[str, object]" = _OrderedDict()


class AddressError(ValueError):
    pass


def parse_urn(text: str) -> Dict[str, str]:
    """Parse a hierarchical URN. Raises AddressError on anything else.

    The common case (a repeated, valid URN) is served from a bounded LRU
    cache; the first call for any given string still runs the regex so the
    cache is correct for malformed input too (the raised error is cached)."""
    if not isinstance(text, str):
        raise AddressError(f"urn must be a string, got {type(text).__name__}")
    with URNCACHE_LOCK:
        cached = URNCACHE.get(text)
        if cached is not None:
            URNCACHE.move_to_end(text)
            return cached
    # Cache miss: run the regex outside the lock, then publish the result.
    m = URN_RE.match(text)
    if not m:
        err = AddressError(
            f"malformed urn {text!r}: expected 'acnl:<zone>/<node>/<agent>' "
            "with lowercase alphanumeric components"
        )
        with URNCACHE_LOCK:
            if len(URNCACHE) >= URNCACHE_MAX:
                URNCACHE.popitem(last=False)
            URNCACHE[text] = URNCACHE_MISS
        raise err
    zone, node, agent = m.group(1), m.group(2), m.group(3)
    result = {"zone": zone, "node": node, "agent": agent}
    with URNCACHE_LOCK:
        if len(URNCACHE) >= URNCACHE_MAX:
            URNCACHE.popitem(last=False)
        URNCACHE[text] = result
    return result


def format_urn(zone: str, node: str, agent: str) -> str:
    return f"acnl:{zone}/{node}/{agent}"


def format_short(urn: str, host: str, port: int) -> str:
    parse_urn(urn)  # validate
    return f"acnl://{host}:{port}/{urn}"


def parse_short(text: str) -> tuple:
    """Parse a short reference. Returns (urn, host, port)."""
    m = re.match(r"^acnl://([a-zA-Z0-9.:\-]+):(\d{1,5})/(.+)$", text)
    if not m:
        raise AddressError(
            f"malformed short reference {text!r}: expected 'acnl://<host>:<port>/<urn>'"
        )
    host, port, urn = m.group(1), int(m.group(2)), m.group(3)
    parse_urn(urn)
    return (urn, host, port)


@dataclass
class Reference:
    """The document a directory stores. A reference is a URN plus the
    endpoints in failover order, the protocol version the peer speaks, the
    capabilities it advertises, and its attributes."""

    urn: str
    endpoints: List[str] = field(default_factory=list)
    protocol_version: str = PROTOCOL_VERSION
    capabilities: List[str] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        parse_urn(self.urn)
        for ep in self.endpoints:
            if not isinstance(ep, str) or not ENDPOINT_RE.match(ep):
                raise AddressError(f"malformed endpoint {ep!r}: expected 'host:port'")
        for cap in self.capabilities:
            if not isinstance(cap, str) or not cap:
                raise AddressError(f"malformed capability {cap!r}: expected a non-empty string")
        if not isinstance(self.attributes, dict):
            raise AddressError("attributes must be a dict")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "urn": self.urn,
            "endpoints": list(self.endpoints),
            "protocol_version": self.protocol_version,
            "capabilities": list(self.capabilities),
            "attributes": dict(self.attributes),
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "Reference":
        if not isinstance(value, dict):
            raise AddressError(f"reference must be a dict, got {type(value).__name__}")
        for k in ("urn", "endpoints", "protocol_version", "capabilities", "attributes"):
            if k not in value:
                raise AddressError(f"reference missing required field {k!r}")
        return cls(
            urn=value["urn"],
            endpoints=list(value["endpoints"]),
            protocol_version=value["protocol_version"],
            capabilities=list(value["capabilities"]),
            attributes=dict(value["attributes"]),
        )
