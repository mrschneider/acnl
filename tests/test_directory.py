import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.directory import DuplicateName, InMemoryDirectory, ShelveDirectory
from acnl_core.attributes import parse_query
from acnl_core.addresses import Reference


def _ref(urn="acnl:lab/node1/worker", caps=None, attrs=None):
    return Reference(urn=urn, endpoints=["host:9100"], capabilities=caps or [], attributes=attrs or {})


def _scenarios(dir):
    """The same scenario on every adapter must produce identical outcomes."""
    out = {}
    # explicit rebind: a silent overwrite on bind lets a typo take over a name
    dir.bind("acnl:lab/node1/worker", _ref())
    try:
        dir.bind("acnl:lab/node1/worker", _ref(urn="acnl:lab/node1/worker"))
        out["rebind"] = "accepted"
    except DuplicateName:
        out["rebind"] = "refused"
    dir.bind("acnl:lab/node1/worker", _ref(attrs={"load": 42}), overwrite=True)
    out["after_rebind"] = dir.resolve("acnl:lab/node1/worker").attributes
    # resolve of an unknown urn
    out["unknown"] = dir.resolve("acnl:lab/node9/ghost")
    # query by attribute
    hits = dir.query(parse_query("load = 42"))
    out["query"] = sorted(r.urn for r in hits)
    # advertise with ttl + sweep evicts
    dir.advertise("acnl:lab/node1/clock", _ref(urn="acnl:lab/node1/clock"), ttl=1.0)
    out["advertised_now"] = dir.resolve("acnl:lab/node1/clock") is not None
    time.sleep(1.1)
    evicted = dir.sweep()
    out["evicted"] = sorted(evicted)
    out["after_sweep"] = dir.resolve("acnl:lab/node1/clock")
    return out


def test_parity_memory_vs_shelve(tmp_path):
    shelve = ShelveDirectory(str(tmp_path / "dir.shelf"))
    try:
        a = _scenarios(InMemoryDirectory())
        b = _scenarios(shelve)
        assert a == b
        # and the specific expected values
        assert a["rebind"] == "refused"
        assert a["after_rebind"] == {"load": 42}
        assert a["unknown"] is None
        assert a["query"] == ["acnl:lab/node1/worker"]
        assert a["advertised_now"] is True
        assert a["evicted"] == ["acnl:lab/node1/clock"]
        assert a["after_sweep"] is None
    finally:
        shelve.close()


def test_shelve_persistence_across_instances(tmp_path):
    path = str(tmp_path / "dir2.shelf")
    d1 = ShelveDirectory(path)
    d1.bind("acnl:lab/node1/worker", _ref())
    d1.close()
    d2 = ShelveDirectory(path)
    try:
        assert d2.resolve("acnl:lab/node1/worker") is not None
        assert d2.resolve("acnl:lab/node9/ghost") is None
    finally:
        d2.close()


def test_advertise_zero_ttl_refused():
    dir = InMemoryDirectory()
    with pytest.raises(ValueError):
        dir.advertise("acnl:lab/node1/worker", _ref(), ttl=0)
