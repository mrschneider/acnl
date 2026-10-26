"""The directory contract: a small interface, then implementations on top.

Storage/IO boundary as a ``Protocol`` with a small method count. Every record
is a plain dataclass that knows nothing about the storage. Adapters are
deliberately different: ``InMemoryDirectory`` is zero-dependency and
non-durable; ``ShelveDirectory`` is durable and file-backed. The same
scenario must run on every adapter and assert *identical* outcomes — that
assertion is the artifact that justifies the word agnostic.

Naming = hierarchical name → reference, with an **explicit rebind**. A silent
overwrite on bind lets a typo take over a name.
"""

from __future__ import annotations

import shelve
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

from .addresses import Reference
from .attributes import _eval


@dataclass
class Entry:
    urn: str
    reference: Reference
    expires_at: Optional[float] = None
    registered_at: float = field(default_factory=time.time)


class Directory(Protocol):
    def bind(self, urn: str, reference: Reference, overwrite: bool = False) -> None: ...
    def unbind(self, urn: str) -> None: ...
    def resolve(self, urn: str) -> Optional[Reference]: ...
    def query(self, ast: Any) -> List[Reference]: ...
    def advertise(self, urn: str, reference: Reference, ttl: float) -> None: ...
    def sweep(self, now: Optional[float] = None) -> List[str]: ...
    def snapshot(self) -> List[Reference]: ...


class InMemoryDirectory:
    """Zero-dependency, non-durable adapter. TTL expiry is computed by
    ``sweep``; ``sweep`` returns the urns that were evicted."""

    def __init__(self):
        self._entries: Dict[str, Entry] = {}

    def _bind(self, urn: str, reference: Reference, overwrite: bool, expires_at: Optional[float]) -> None:
        if urn in self._entries and not overwrite:
            raise DuplicateName(urn)
        self._entries[urn] = Entry(urn=urn, reference=reference, expires_at=expires_at)

    def bind(self, urn: str, reference: Reference, overwrite: bool = False) -> None:
        self._bind(urn, reference, overwrite, None)

    def advertise(self, urn: str, reference: Reference, ttl: float) -> None:
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl}")
        self._bind(urn, reference, True, time.time() + ttl)

    def unbind(self, urn: str) -> None:
        self._entries.pop(urn, None)

    def resolve(self, urn: str) -> Optional[Reference]:
        e = self._entries.get(urn)
        if e is None:
            return None
        if e.expires_at is not None and e.expires_at <= time.time():
            return None
        return e.reference

    def query(self, ast: Any) -> List[Reference]:
        return [e.reference for e in self._entries.values() if _eval(ast, e.reference.to_dict())]

    def sweep(self, now: Optional[float] = None) -> List[str]:
        now = now if now is not None else time.time()
        evicted = [u for u, e in self._entries.items() if e.expires_at is not None and e.expires_at <= now]
        for u in evicted:
            del self._entries[u]
        return evicted

    def count(self) -> int:
        return len(self._entries)

    def snapshot(self) -> List[Reference]:
        """Every live entry, for a peer that just linked (the router's table)."""
        now = time.time()
        return [e.reference for e in self._entries.values() if e.expires_at is None or e.expires_at > now]


class DuplicateName(ValueError):
    """A business refusal: the name is already bound and overwrite was not
    requested."""

    def __init__(self, urn: str):
        super().__init__(f"{urn!r} is already bound; a silent overwrite on bind lets a "
        "typo take over a name, so rebind requires overwrite=True")
        self.urn = urn


class ShelveDirectory:
    """Durable, file-backed adapter. Same semantics as the in-memory one,
    persisted to a shelve file."""

    def __init__(self, path: str):
        self._path = path
        self._store: shelve.Shelf = shelve.open(path, flag="c", writeback=False)

    def _load(self) -> Dict[str, Any]:
        return dict(self._store)

    def bind(self, urn: str, reference: Reference, overwrite: bool = False) -> None:
        data = self._load()
        if urn in data and not overwrite:
            raise DuplicateName(urn)
        e = data[urn] if urn in data else Entry(urn=urn, reference=reference)
        e.reference = reference
        e.expires_at = None
        data[urn] = e
        self._store.clear()
        self._store.update(data)

    def advertise(self, urn: str, reference: Reference, ttl: float) -> None:
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl}")
        data = self._load()
        data[urn] = Entry(urn=urn, reference=reference, expires_at=time.time() + ttl)
        self._store.clear()
        self._store.update(data)

    def unbind(self, urn: str) -> None:
        data = self._load()
        data.pop(urn, None)
        self._store.clear()
        self._store.update(data)

    def resolve(self, urn: str) -> Optional[Reference]:
        e = self._load().get(urn)
        if e is None:
            return None
        if e.expires_at is not None and e.expires_at <= time.time():
            return None
        return e.reference

    def query(self, ast: Any) -> List[Reference]:
        return [e.reference for e in self._load().values() if _eval(ast, e.reference.to_dict())]

    def sweep(self, now: Optional[float] = None) -> List[str]:
        now = now if now is not None else time.time()
        data = self._load()
        evicted = [u for u, e in data.items() if e.expires_at is not None and e.expires_at <= now]
        for u in evicted:
            del data[u]
        if evicted:
            self._store.clear()
            self._store.update(data)
        return evicted

    def count(self) -> int:
        return len(self._load())

    def snapshot(self) -> List[Reference]:
        now = time.time()
        return [e.reference for e in self._load().values() if e.expires_at is None or e.expires_at > now]

    def close(self) -> None:
        self._store.close()
