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
import threading
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
    persisted to a shelve file.

    Two hazards of the reference node's threading are handled here, not by
    luck:

    - **Thread affinity**: on Python 3.13+ the default shelve backend is
      ``dbm.sqlite3``, and SQLite objects may only be used from the thread
      that created them — the node's reader thread would die with
      ``SQLite objects created in a thread can only be used in that same
      thread``. So each thread gets *its own* handle on first use
      (``threading.local``), and one instance-level ``RLock`` serializes
      every read-modify-write across those handles — a shelve's
      ``clear()`` + ``update()`` cycles are not atomic on their own.
    - **Release on stop**: all opened handles are registered; ``close()``
      closes every one it can (best effort across threads), so the next
      instance can open the file. The lock is what makes the adapter safe
      under the node's accept/reader/timer threads, not just the suite's
      single thread."""

    def __init__(self, path: str):
        self._path = path
        self._lock = threading.RLock()
        self._local = threading.local()
        self._handles: List[shelve.Shelf] = []

    def _db(self) -> shelve.Shelf:
        handle = getattr(self._local, "handle", None)
        if handle is None:
            handle = shelve.open(self._path, flag="c", writeback=False)
            self._local.handle = handle
            with self._lock:
                self._handles.append(handle)
        return handle

    def _load(self) -> Dict[str, Any]:
        return dict(self._db())

    def _flush(self, data: Dict[str, Any]) -> None:
        db = self._db()
        db.clear()
        db.update(data)

    def bind(self, urn: str, reference: Reference, overwrite: bool = False) -> None:
        with self._lock:
            data = self._load()
            if urn in data and not overwrite:
                raise DuplicateName(urn)
            e = data[urn] if urn in data else Entry(urn=urn, reference=reference)
            e.reference = reference
            e.expires_at = None
            data[urn] = e
            self._flush(data)

    def advertise(self, urn: str, reference: Reference, ttl: float) -> None:
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl}")
        with self._lock:
            data = self._load()
            data[urn] = Entry(urn=urn, reference=reference, expires_at=time.time() + ttl)
            self._flush(data)

    def unbind(self, urn: str) -> None:
        with self._lock:
            data = self._load()
            data.pop(urn, None)
            self._flush(data)

    def resolve(self, urn: str) -> Optional[Reference]:
        with self._lock:
            e = self._load().get(urn)
        if e is None:
            return None
        if e.expires_at is not None and e.expires_at <= time.time():
            return None
        return e.reference

    def query(self, ast: Any) -> List[Reference]:
        with self._lock:
            data = self._load()
        return [e.reference for e in data.values() if _eval(ast, e.reference.to_dict())]

    def sweep(self, now: Optional[float] = None) -> List[str]:
        now = now if now is not None else time.time()
        with self._lock:
            data = self._load()
            evicted = [u for u, e in data.items() if e.expires_at is not None and e.expires_at <= now]
            for u in evicted:
                del data[u]
            if evicted:
                self._flush(data)
        return evicted

    def count(self) -> int:
        with self._lock:
            return len(self._load())

    def snapshot(self) -> List[Reference]:
        now = time.time()
        with self._lock:
            data = self._load()
        return [e.reference for e in data.values() if e.expires_at is None or e.expires_at > now]

    def close(self) -> None:
        with self._lock:
            for handle in list(self._handles):
                try:
                    handle.close()
                except Exception:
                    # closing another thread's SQLite handle is refused by
                    # the affinity check; its connection is released when
                    # the owning thread's locals are collected
                    pass
            self._handles.clear()
            self._local = threading.local()
