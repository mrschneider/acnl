import sys
import socket
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.directory import InMemoryDirectory
from acnl_engine.config import NodeConfig
from acnl_engine.node import Node
from acnl_engine.client import Client, ClientError


def _node(name="node1", zone="lab", **kw):
    cfg = NodeConfig(host="127.0.0.1", port=0, name=name, zone=zone, **kw)
    node = Node(cfg, InMemoryDirectory())
    node.start()
    port = node._listener.getsockname()[1]
    return node, cfg, port


def _wait(pred, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


def _ref(urn, **kw):
    from acnl_core.addresses import Reference

    return Reference(urn=urn, **kw).to_dict()


@pytest.fixture()
def two_peered():
    n1, cfg1, p1 = _node(name="node1")
    n2, cfg2, p2 = _node(name="node2")
    n1.peer("127.0.0.1", p2)
    ok = _wait(lambda: cfg2.self_urn in n1.remote_directory and cfg1.self_urn in n2.remote_directory)
    assert ok, "peer link did not converge: both sides must learn each other's broker urn"
    yield n1, n2
    n1.stop()
    n2.stop()


def test_snapshot_on_link_federates_resolve(two_peered):
    n1, n2 = two_peered
    # an agent advertised on n2 BEFORE the link: the snapshot must carry it
    c2 = Client("acnl:lab/node2/math")
    try:
        c2.connect("127.0.0.1", n2._listener.getsockname()[1])
        c2.advertise("acnl:lab/node2/math", _ref("acnl:lab/node2/math", capabilities=["math"]))
        c1 = Client("acnl:lab/node1/worker")
        try:
            c1.connect("127.0.0.1", n1._listener.getsockname()[1])
            got = c1.resolve("acnl:lab/node2/math")
            assert got["reference"]["urn"] == "acnl:lab/node2/math"
            assert got["origin"] == "federated"
        finally:
            c1.close()
    finally:
        c2.close()


def test_live_advertise_propagates_through_peer_link(two_peered):
    n1, n2 = two_peered
    c2 = Client("acnl:lab/node2/clock")
    try:
        c2.connect("127.0.0.1", n2._listener.getsockname()[1])
        # advertised AFTER the link: the notify broadcast must reach n1
        c2.advertise("acnl:lab/node2/clock", _ref("acnl:lab/node2/clock", capabilities=["time"]))
        c1 = Client("acnl:lab/node1/worker")
        try:
            c1.connect("127.0.0.1", n1._listener.getsockname()[1])
            assert _wait(lambda: "acnl:lab/node2/clock" in n1.remote_directory)
            got = c1.resolve("acnl:lab/node2/clock")
            assert got["origin"] == "federated"
            assert got["reference"]["capabilities"] == ["time"]
        finally:
            c1.close()
    finally:
        c2.close()


def test_federated_query_merges_and_dedupes(two_peered):
    n1, n2 = two_peered
    c1 = Client("acnl:lab/node1/worker")
    c2 = Client("acnl:lab/node2/math")
    try:
        c1.connect("127.0.0.1", n1._listener.getsockname()[1])
        c2.connect("127.0.0.1", n2._listener.getsockname()[1])
        # the same urn bound on BOTH brokers: a query must return it once,
        # and the local entry wins (this broker can answer for it)
        c1.bind("acnl:lab/shared/svc", _ref("acnl:lab/shared/svc", attributes={"side": "local"}))
        c2.advertise("acnl:lab/shared/svc", _ref("acnl:lab/shared/svc", attributes={"side": "remote"}))
        c2.advertise("acnl:lab/node2/math", _ref("acnl:lab/node2/math", capabilities=["math"]))
        assert _wait(lambda: "acnl:lab/node2/math" in n1.remote_directory)
        hits = c1.query('name CONTAINS "svc" OR capabilities CONTAINS "math"')
        urns = [h["urn"] for h in hits]
        assert urns.count("acnl:lab/shared/svc") == 1
        assert "acnl:lab/node2/math" in urns
        shared = [h for h in hits if h["urn"] == "acnl:lab/shared/svc"][0]
        assert shared["attributes"] == {"side": "local"}  # local wins
    finally:
        c1.close()
        c2.close()


def test_cross_broker_send_roundtrip(two_peered):
    """send crosses the peer link and the reply routes back two hops — the
    reply is routed by the envelope's recipient through the propagated
    implicit registration, never by request_id."""
    n1, n2 = two_peered
    c1 = Client("acnl:lab/node1/worker")
    c2 = Client("acnl:lab/node2/math")
    try:
        c1.connect("127.0.0.1", n1._listener.getsockname()[1])
        c2.connect("127.0.0.1", n2._listener.getsockname()[1])
        c2.set_handler(lambda msg: {"op": "send", "echo": msg.body.get("payload")})
        assert _wait(lambda: c1.agent_urn in n2.remote_agents), (
            "implicit registration must propagate so the peer can route the reply back"
        )
        out = c1.send("acnl:lab/node2/math", {"n": 42})
        assert out["echo"] == {"n": 42}
    finally:
        c1.close()
        c2.close()


def test_peer_death_evicts_federated_entries(two_peered):
    n1, n2 = two_peered
    c2 = Client("acnl:lab/node2/math")
    try:
        c2.connect("127.0.0.1", n2._listener.getsockname()[1])
        c2.advertise("acnl:lab/node2/math", _ref("acnl:lab/node2/math"))
        c1 = Client("acnl:lab/node1/worker")
        try:
            c1.connect("127.0.0.1", n1._listener.getsockname()[1])
            assert _wait(lambda: "acnl:lab/node2/math" in n1.remote_directory)
            # the peer goes down: its table goes with it, locally evicted
            n2.stop()
            assert _wait(lambda: "acnl:lab/node2/math" not in n1.remote_directory)
            with pytest.raises(ClientError) as e:
                c1.resolve("acnl:lab/node2/math")
            assert e.value.body["code"] == "unknown_service"
        finally:
            c1.close()
    finally:
        if not n2._stop:
            n2.stop()


def test_no_echo_clobbering_of_local_agents(two_peered):
    """The federation must not import its own broadcasts back: an event about
    an agent we know locally is a reverb and is ignored."""
    n1, n2 = two_peered
    c1 = Client("acnl:lab/node1/worker")
    try:
        c1.connect("127.0.0.1", n1._listener.getsockname()[1])
        # c1 registered implicitly on n1 (conn_id of c1's own connection)
        own_conn = n1.remote_agents[c1.agent_urn]
        # now link a third broker to n2: n2 rebroadcasts c1's registration
        # hop by hop on its periodic table broadcast; n1 must ignore any
        # reverb and never remap c1 to a peer link
        n3, _cfg3, p3 = _node(name="node3", broadcast_interval=0.5)
        # n2 must rebroadcast promptly too
        n2.cfg.broadcast_interval = 0.5
        try:
            n2.peer("127.0.0.1", p3)
            assert _wait(lambda: c1.agent_urn in n3.remote_directory, timeout=8.0), (
                "learned entries must propagate hop by hop on the periodic table broadcast"
            )
            time.sleep(0.5)  # give any bad reverb time to land
            assert n1.remote_agents[c1.agent_urn] == own_conn
        finally:
            n3.stop()
    finally:
        c1.close()
