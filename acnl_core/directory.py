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
from .attributes import And, Cmp, Not, Or, _eval


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
    ``sweep``; ``sweep`` returns the urns that were evicted.

    ``query`` is backed by an inverted index for ``attr = value`` filters:
    the index picks the most selective attribute and evaluates the rest of
    the AST only on its candidates. Without an ``=`` (or when the candidate
    set is nearly the whole table) it degrades to the old full scan, so the
    index only helps, never hurts. The index is maintained incrementally on
    each mutation — O(reference size) per bind/advertise/unbind, never
    O(table).
    """

    def __init__(self):
        self._entries: Dict[str, Entry] = {}
        self._index: Dict[str, Dict[Any, set]] = {}  # attr -> value -> {urns}
        self._contrib: Dict[str, List[Tuple[str, Any]]] = {}  # urn -> (attr, key) it feeds

    @staticmethod
    def _index_key(value: Any) -> Any:
        # lists are unordered for equality: index them as frozensets
        return frozenset(value) if isinstance(value, (list, tuple)) else value

    def _remove_from_index(self, urn: str) -> None:
        for attr, key in self._contrib.pop(urn, []):
            buckets = self._index.get(attr)
            if buckets is None:
                continue
            bucket = buckets.pop(key, None)
            if bucket is not None:
                bucket.discard(urn)

    def _add_to_index(self, urn: str, reference: Reference) -> None:
        doc = reference.to_dict()
        contrib: List[Tuple[str, Any]] = []
        for k, v in doc.items():
            if k == "urn" or isinstance(v, (dict, list, tuple, set)):
                continue
            key = self._index_key(v)
            contrib.append((k, key))
            self._index.setdefault(k, {}).setdefault(key, set()).add(urn)
        for k, v in doc.get("attributes", {}).items():
            if isinstance(v, (dict, list, tuple, set)):
                continue
            key = self._index_key(v)
            contrib.append((k, key))
            self._index.setdefault(k, {}).setdefault(key, set()).add(urn)
        self._contrib[urn] = contrib

    def _bind(self, urn: str, reference: Reference, overwrite: bool, expires_at: Optional[float]) -> None:
        if urn in self._entries and not overwrite:
            raise DuplicateName(urn)
        if urn in self._entries:
            self._remove_from_index(urn)
        self._entries[urn] = Entry(urn=urn, reference=reference, expires_at=expires_at)
        self._add_to_index(urn, reference)

    def bind(self, urn: str, reference: Reference, overwrite: bool = False) -> None:
        self._bind(urn, reference, overwrite, None)

    def advertise(self, urn: str, reference: Reference, ttl: float) -> None:
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl}")
        self._bind(urn, reference, True, time.time() + ttl)

    def unbind(self, urn: str) -> None:
        if self._entries.pop(urn, None) is not None:
            self._remove_from_index(urn)

    def resolve(self, urn: str) -> Optional[Reference]:
        e = self._entries.get(urn)
        if e is None:
            return None
        if e.expires_at is not None and e.expires_at <= time.time():
            return None
        return e.reference

    def query(self, ast: Any) -> List[Reference]:
        cands = self._candidates(ast)
        if cands is None:
            return [e.reference for e in self._entries.values() if _eval(ast, e.reference.to_dict())]
        return [
            e.reference
            for urn in cands
            if (e := self._entries.get(urn)) is not None
            and _eval(ast, e.reference.to_dict())
        ]

    def _candidates(self, ast: Any) -> Optional[frozenset]:
        """Urn set from a selective top-level ``=`` conjunct, or None when no
        such conjunct exists (or the whole table would be the answer).

        The returned set is a guaranteed SUPERSET of the query's true answer,
        so the final ``_eval`` filter over it is always correct — a match is
        never dropped. Only *mandatory* conjuncts qualify: a ``=`` under an
        ``OR`` branch or a ``NOT`` is not satisfied by every answer, so its
        bucket is not a superset and we fall back to a full scan instead."""
        best: Optional[frozenset] = None
        for c in self._top_conjuncts(ast):
            if not (isinstance(c, Cmp) and c.op == "="):
                continue
            if c.attr == "urn":
                # a top-level field, not indexed: direct membership test
                got = frozenset({c.value}) if isinstance(c.value, str) else frozenset()
            else:
                buckets = self._index.get(c.attr)
                if buckets is None:
                    # attribute absent from the index (special attrs like
                    # name/zone/version, or never stored): cannot be used
                    continue
                got = buckets.get(self._index_key(c.value))
                if got is None:
                    got = frozenset()
            if not got:
                return frozenset()  # a mandatory conjunct matches nothing
            if best is None or len(got) < len(best):
                best = got
        if best is None or len(best) * 3 >= len(self._entries):
            return None  # full scan is cheaper than evaluating candidates
        return best

    @staticmethod
    def _top_conjuncts(ast: Any) -> List[Any]:
        """The direct conjuncts of the top-level ``AND`` (a bare atom is a
        1-conjunct). Only these are mandatory — every satisfying entry must
        match each one, so a bucket from one is a valid candidate superset.
        A top-level ``OR``/``NOT`` yields no mandatory conjunct."""
        if isinstance(ast, And):
            return ast.parts
        if isinstance(ast, (Or, Not)):
            return []
        return [ast]

    def sweep(self, now: Optional[float] = None) -> List[str]:
        now = now if now is not None else time.time()
        evicted = [u for u, e in self._entries.items() if e.expires_at is not None and e.expires_at <= now]
        for u in evicted:
            del self._entries[u]
            self._remove_from_index(u)
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

    Persistence is *batched*: the shelf is loaded once at open into an
    in-memory cache that every operation reads and writes, and the file is
    rewritten only when the dirty-entry count reaches ``flush_every`` or a
    background task wakes every ``flush_interval`` seconds — and always on
    ``close()``. The old design (load + full rewrite on *every* call) made a
    5s sweep and a 10s re-advertise rewrite the whole file continuously,
    with cost growing in N.

    Two hazards of the reference node's threading are handled here, not by
    luck:

    - **One handle, one lock**: every thread shares a single shelf
      serialized by an instance ``RLock`` — a shelve's ``clear()`` +
      ``update()`` cycles are not atomic on their own. The backend is
      chosen explicitly: ``dbm.gnu`` (or ``dbm.dumb`` as fallback),
      because 3.13+'s *default* ``dbm.sqlite3`` is thread-affine (a
      cross-thread use dies with ``SQLite objects created in a thread can
      only be used in that same thread``) and gdbm's per-*fd* file lock
      rejects a second same-process opener (``Errno 11``). Neither chosen
      backend has either restriction.
    - **Release on stop**: ``close()`` flushes, releases the handle, so the
      next instance can open the file."""

    def __init__(self, path: str, flush_every: int = 64, flush_interval: float = 2.0):
        self._path = path
        self._lock = threading.RLock()
        self._store: shelve.Shelf = self._open(path)
        self._cache: Dict[str, Any] = dict(self._store)
        # incremental flush: track what changed since the last file write,
        # so a flush is O(dirty), not O(table) — clear()+update() of the
        # whole shelf per batch made bulk loads O(N^2 / flush_every)
        self._dirty_adds: set = set()
        self._dirty_dels: set = set()
        self._dirty = 0
        self._flush_every = flush_every
        self._flush_interval = flush_interval
        self._closed = False
        self._flusher = threading.Thread(
            target=self._flush_loop, daemon=True, name=f"acnl-shelve-{path}"
        )
        self._flusher.start()

    @staticmethod
    def _open(path: str) -> shelve.Shelf:
        try:
            import dbm.gnu

            return shelve.Shelf(dbm.gnu.open(path, "c"))  # type: ignore[arg-type]
        except ImportError:
            import dbm.dumb

            return shelve.Shelf(dbm.dumb.open(path, "c"))  # type: ignore[arg-type]

    def _mark_dirty(self, urn: str) -> None:
        self._dirty += 1
        self._dirty_adds.add(urn)
        self._dirty_dels.discard(urn)
        if self._dirty >= self._flush_every:
            self._flush_locked()

    def _flush_locked(self) -> None:
        """Write the pending changes to the file. Caller holds ``_lock``.
        O(dirty): only the keys that changed since the last flush."""
        if self._dirty <= 0:
            return
        for urn in self._dirty_dels:
            try:
                del self._store[urn]
            except (KeyError, ValueError):
                pass
        for urn in self._dirty_adds:
            e = self._cache.get(urn)
            if e is not None:
                self._store[urn] = e
        self._dirty = 0
        self._dirty_adds = set()
        self._dirty_dels = set()

    def _flush_loop(self) -> None:
        while not self._closed:
            time.sleep(self._flush_interval)
            with self._lock:
                if self._closed:
                    return
                self._flush_locked()

    def bind(self, urn: str, reference: Reference, overwrite: bool = False) -> None:
        with self._lock:
            if urn in self._cache and not overwrite:
                raise DuplicateName(urn)
            e = self._cache[urn] if urn in self._cache else Entry(urn=urn, reference=reference)
            e.reference = reference
            e.expires_at = None
            self._cache[urn] = e
            self._mark_dirty(urn)

    def advertise(self, urn: str, reference: Reference, ttl: float) -> None:
        if ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl}")
        with self._lock:
            self._cache[urn] = Entry(urn=urn, reference=reference, expires_at=time.time() + ttl)
            self._mark_dirty(urn)

    def unbind(self, urn: str) -> None:
        with self._lock:
            if self._cache.pop(urn, None) is not None:
                self._dirty += 1
                self._dirty_dels.add(urn)
                self._dirty_adds.discard(urn)
                if self._dirty >= self._flush_every:
                    self._flush_locked()

    def resolve(self, urn: str) -> Optional[Reference]:
        with self._lock:
            e = self._cache.get(urn)
        if e is None:
            return None
        if e.expires_at is not None and e.expires_at <= time.time():
            return None
        return e.reference

    def query(self, ast: Any) -> List[Reference]:
        with self._lock:
            data = self._cache
        return [e.reference for e in data.values() if _eval(ast, e.reference.to_dict())]

    def sweep(self, now: Optional[float] = None) -> List[str]:
        now = now if now is not None else time.time()
        with self._lock:
            evicted = [
                u
                for u, e in self._cache.items()
                if e.expires_at is not None and e.expires_at <= now
            ]
            for u in evicted:
                del self._cache[u]
                self._dirty_dels.add(u)
                self._dirty_adds.discard(u)
            self._dirty += len(evicted)
            if self._dirty >= self._flush_every:
                self._flush_locked()
        return evicted

    def count(self) -> int:
        with self._lock:
            return len(self._cache)

    def snapshot(self) -> List[Reference]:
        now = time.time()
        with self._lock:
            data = self._cache
        return [e.reference for e in data.values() if e.expires_at is None or e.expires_at > now]

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._flush_locked()
            self._store.close()

    def __del__(self):
        # best effort: an instance that died without close() still flushes;
        # at interpreter shutdown attributes may already be gone
        try:
            if not getattr(self, "_closed", True):
                self.close()
        except Exception:
            pass
