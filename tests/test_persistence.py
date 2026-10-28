"""Persistence: the node's directory survives restarts.

The gap was at the *node* level: the durable adapter existed and was tested
in isolation, but NodeConfig had no way to ask for it. Now ``data_path``
wires a file-backed directory into the node, the node closes what it opened
on stop (releasing the shelve lock for the next instance), and an injected
directory stays the caller's — stop() must not close it.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from acnl_core.addresses import Reference
from acnl_core.directory import ShelveDirectory
from acnl_engine.client import Client
from acnl_engine.config import NodeConfig
from acnl_engine.node import Node


def _ref(urn, **kw):
    return Reference(urn=urn, **kw).to_dict()


def _start_node(data_path=None, directory=None):
    cfg = NodeConfig(host="127.0.0.1", port=0, name="node1", zone="lab", data_path=data_path)
    node = Node(cfg, directory)
    node.start()
    return node


def _wait(pred, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


def test_bindings_survive_node_restart(tmp_path):
    shelf = str(tmp_path / "dir.shelf")

    n1 = _start_node(data_path=shelf)
    c1 = Client("acnl:lab/node1/worker")
    try:
        c1.connect("127.0.0.1", n1._listener.getsockname()[1])
        c1.advertise("acnl:lab/node1/worker", _ref("acnl:lab/node1/worker", capabilities=["math"]))
        got = c1.resolve("acnl:lab/node1/worker")
        assert got["reference"]["urn"] == "acnl:lab/node1/worker"
    finally:
        c1.close()
        n1.stop()  # closes the shelve it opened (releases the file lock)

    # a brand-new node instance on the same path: the binding is still there
    n2 = _start_node(data_path=shelf)
    try:
        c2 = Client("acnl:lab/node1/other")
        try:
            c2.connect("127.0.0.1", n2._listener.getsockname()[1])
            got = c2.resolve("acnl:lab/node1/worker")
            assert got["reference"]["urn"] == "acnl:lab/node1/worker"
            assert got["reference"]["capabilities"] == ["math"]
        finally:
            c2.close()
    finally:
        n2.stop()


def test_stop_closes_owned_shelve_releasing_the_lock(tmp_path):
    shelf = str(tmp_path / "dir.shelf")
    n = _start_node(data_path=shelf)
    n.stop()
    # if stop() leaked the handle, this second open would fail on the
    # gdbm lock ("resource temporarily unavailable") or miss entries
    d = ShelveDirectory(shelf)
    try:
        assert d.count() >= 1  # the self-urn advertisement was persisted
        assert d.resolve("acnl:lab/node1/broker") is not None
    finally:
        d.close()


def test_injected_directory_is_not_closed_by_stop(tmp_path):
    shelf = str(tmp_path / "injected.shelf")
    injected = ShelveDirectory(shelf)
    injected.bind("acnl:lab/x/y", Reference(urn="acnl:lab/x/y"))
    n = _start_node(directory=injected)
    n.stop()
    # ownership: the caller opened it, the caller closes it — the handle
    # must still answer after the node is gone
    assert injected.resolve("acnl:lab/x/y") is not None
    injected.close()


def test_data_path_is_ignored_when_directory_is_injected(tmp_path):
    shelf = str(tmp_path / "unused.shelf")
    from acnl_core.directory import InMemoryDirectory

    cfg = NodeConfig(host="127.0.0.1", port=0, name="node1", zone="lab", data_path=shelf)
    node = Node(cfg, InMemoryDirectory())
    assert node._owns_directory is False
    assert not Path(shelf).exists() and not Path(shelf + ".db").exists() and not Path(shelf + ".dat").exists()
