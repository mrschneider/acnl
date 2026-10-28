"""Security end-to-end: the policy wired through Node + Client over TCP.

Covers authentication (signed frames, tamper refusal), encryption
(sealed bodies with the routing op visible), authorization (ACL typed
refusal), and forward/backward compatibility with pre-security peers.
"""

import json
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.codec import FrameDecoder, encode_frame
from acnl_core.directory import InMemoryDirectory
from acnl_core.security import SecurityPolicy
from acnl_engine.client import Client, ClientError
from acnl_engine.config import NodeConfig
from acnl_engine.node import Node

PSK = "integration-psk-not-a-real-secret"


def _ref(urn, **kw):
    from acnl_core.addresses import Reference

    return Reference(urn=urn, **kw).to_dict()


def _node(policy, name="node1"):
    cfg = NodeConfig(host="127.0.0.1", port=0, name=name, zone="lab", security=policy)
    node = Node(cfg, InMemoryDirectory())
    node.start()
    return node


def _wait(pred, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


def _raw_frames(sock, decoder, want=1, timeout=3.0, policy=None):
    """Read raw frames off the socket, unprotecting with the policy."""
    out = []
    deadline = time.time() + timeout
    while len(out) < want and time.time() < deadline:
        try:
            data = sock.recv(65536)
        except socket.timeout:
            continue
        if not data:
            break
        for value, _ in decoder.feed(data):
            if policy is not None:
                value = policy.unprotect(value)
            out.append(value)
    return out


# --- authentication ------------------------------------------------------

def test_signed_encrypted_end_to_end():
    pytest.importorskip("cryptography")
    pol = SecurityPolicy.from_secret(PSK, encrypt=True, require_encryption=True)
    node = _node(pol)
    try:
        c = Client("acnl:lab/node1/worker", security=SecurityPolicy.from_secret(
            PSK, encrypt=True, require_encryption=True))
        c.connect("127.0.0.1", node._listener.getsockname()[1])
        try:
            body = c.ping()
            assert body["value"] == "pong"
            c.advertise("acnl:lab/node1/worker", _ref("acnl:lab/node1/worker", capabilities=["math"]))
            got = c.resolve("acnl:lab/node1/worker")
            assert got["reference"]["urn"] == "acnl:lab/node1/worker"
        finally:
            c.close()
    finally:
        node.stop()


def test_unsigned_frame_refused_with_auth_failed_code():
    pytest.importorskip("cryptography")
    pol = SecurityPolicy.from_secret(PSK, encrypt=True)
    node = _node(pol)
    try:
        port = node._listener.getsockname()[1]
        s = socket.create_connection(("127.0.0.1", port), timeout=3)
        s.settimeout(0.5)
        try:
            from acnl_core.messages import WireMessage, make_envelope, new_id

            hello = WireMessage(
                kind="hello",
                envelope=make_envelope(sender="acnl:lab/rogue/agent",
                                       recipient="acnl:lab/node1/broker",
                                       request_id=new_id(), message_id=new_id()),
                body={"capabilities": []},
            ).to_value()
            s.sendall(encode_frame(hello))  # unsigned on purpose
            frames = _raw_frames(s, FrameDecoder(), want=1, policy=pol)
            assert frames, "node must answer a refusal, not silence"
            err = frames[0]
            assert err["kind"] == "error"
            assert err["body"]["code"] == "auth_failed"
            assert err["body"]["kind"] == "user"
            assert err["body"]["retryable"] is False
        finally:
            s.close()
    finally:
        node.stop()


def test_tampered_signed_frame_refused():
    pytest.importorskip("cryptography")
    pol = SecurityPolicy.from_secret(PSK, encrypt=True)
    node = _node(pol)
    try:
        port = node._listener.getsockname()[1]
        s = socket.create_connection(("127.0.0.1", port), timeout=3)
        s.settimeout(0.5)
        try:
            from acnl_core.messages import WireMessage, make_envelope, new_id

            hello = WireMessage(
                kind="hello",
                envelope=make_envelope(sender="acnl:lab/rogue/agent",
                                       recipient="acnl:lab/node1/broker",
                                       request_id=new_id(), message_id=new_id()),
                body={"capabilities": []},
            ).to_value()
            signed = pol.protect(hello)
            signed["envelope"]["ttl"] = 15  # alter after signing
            s.sendall(encode_frame(signed))
            frames = _raw_frames(s, FrameDecoder(), want=1, policy=pol)
            assert frames and frames[0]["kind"] == "error"
            assert frames[0]["body"]["code"] == "auth_failed"
            assert "mac mismatch" in frames[0]["body"]["message"]
        finally:
            s.close()
    finally:
        node.stop()


# --- authorization -------------------------------------------------------

def test_acl_denies_operation_with_typed_refusal():
    acl = [{"sender": "acnl:lab/node1/worker", "ops": ["ping", "resolve"], "recipient": "*"}]
    pol = SecurityPolicy.from_secret(PSK, acl=acl)
    node = _node(pol)
    try:
        c = Client("acnl:lab/node1/worker", security=SecurityPolicy.from_secret(PSK))
        c.connect("127.0.0.1", node._listener.getsockname()[1])
        try:
            assert c.ping()["value"] == "pong"          # allowed by acl
            with pytest.raises(ClientError) as exc:
                c.advertise("acnl:lab/node1/worker", _ref("acnl:lab/node1/worker"))
            assert exc.value.body["code"] == "not_authorized"
            assert exc.value.body["kind"] == "user"
            assert exc.value.body["retryable"] is False
        finally:
            c.close()
    finally:
        node.stop()


def test_signed_but_unencrypted_refused_when_encryption_required():
    pytest.importorskip("cryptography")
    node = _node(SecurityPolicy.from_secret(PSK, encrypt=True, require_encryption=True))
    try:
        # legacy-style client: signs but does not seal
        c = Client("acnl:lab/node1/worker", security=SecurityPolicy.from_secret(PSK))
        c.connect("127.0.0.1", node._listener.getsockname()[1])
        try:
            with pytest.raises(ClientError) as exc:
                c.ping()
            assert exc.value.body["code"] == "auth_failed"
            assert "ciphertext" in exc.value.body["message"]
        finally:
            c.close()
    finally:
        node.stop()


# --- compatibility -------------------------------------------------------

def test_pre_security_node_ignores_auth_block():
    # mixed mesh, both directions: the legacy node drops the auth block
    # (additive wire), and the signed client tolerates the legacy node's
    # unsigned replies via require_auth=False
    node = _node(None)
    try:
        c = Client("acnl:lab/node1/worker",
                   security=SecurityPolicy.from_secret(PSK, require_auth=False))
        c.connect("127.0.0.1", node._listener.getsockname()[1])
        try:
            assert c.ping()["value"] == "pong"
            assert c.rejected_auth == 0  # tolerated, not silently eaten
        finally:
            c.close()
    finally:
        node.stop()


def test_federation_with_policy_end_to_end():
    pytest.importorskip("cryptography")
    pol = SecurityPolicy.from_secret(PSK, encrypt=True)
    n1 = _node(pol, name="node1")
    n2 = _node(SecurityPolicy.from_secret(PSK, encrypt=True), name="node2")
    try:
        n1.peer("127.0.0.1", n2._listener.getsockname()[1])
        ok = _wait(lambda: n2.cfg.self_urn in n1.remote_directory
                   and n1.cfg.self_urn in n2.remote_directory)
        assert ok, "peer links with sealed+signed hello/notify frames must converge"
        c2 = Client("acnl:lab/node2/math", security=SecurityPolicy.from_secret(PSK, encrypt=True))
        c2.connect("127.0.0.1", n2._listener.getsockname()[1])
        try:
            c2.advertise("acnl:lab/node2/math", _ref("acnl:lab/node2/math", capabilities=["math"]))
            c1 = Client("acnl:lab/node1/worker", security=SecurityPolicy.from_secret(PSK, encrypt=True))
            c1.connect("127.0.0.1", n1._listener.getsockname()[1])
            try:
                got = c1.resolve("acnl:lab/node2/math")
                assert got["origin"] == "federated"
            finally:
                c1.close()
        finally:
            c2.close()
    finally:
        n1.stop()
        n2.stop()
