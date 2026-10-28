"""End-to-end tests for the datagram plane over real UDP sockets.

These are the tests the gap called for: the sequenced contract exercised
end-to-end over UDP — ordering, gaps, duplicates, retransmission on loss,
the state machine, refusal caps, and authenticated datagrams.
"""

import json
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.security import SecurityPolicy
from acnl_core.sequencing import ConnectionState, SequenceError
from acnl_engine.datagram import DatagramError, UdpLink

PSK = "udp-psk-not-a-real-secret"


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _addr(link: UdpLink) -> tuple:
    assert link._sock is not None, "link must be open"
    return link._sock.getsockname()


def _pair(**kw):
    """Two links on loopback with fixed cross-pointed ports (the hello in
    open() needs a real peer address)."""
    host = "127.0.0.1"
    port_a, port_b = _free_port(), _free_port()
    a = UdpLink(local=(host, port_a), peer=(host, port_b), **kw)
    b = UdpLink(local=(host, port_b), peer=(host, port_a), **kw)
    a.open()
    b.open()
    # a's first hello raced b's bind (nothing was listening yet): re-send it
    # so BOTH endpoints complete the handshake deterministically
    a._send_raw({"t": "hello"})
    return a, b


def _wait(pred, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


def _close(*links):
    for l in links:
        try:
            l.close()
        except Exception:
            pass


# --- happy path ----------------------------------------------------------

def test_in_order_delivery_and_acks():
    a, b = _pair()
    try:
        # the hello handshake races the assertion on a loaded runner:
        # wait for BOTH endpoints to see their hello before asserting
        assert _wait(lambda: a.state is ConnectionState.ESTABLISHED
                     and b.state is ConnectionState.ESTABLISHED), \
            "hello handshake must establish both sides"
        for i in range(5):
            a.send({"n": i})
        assert _wait(lambda: len(b.delivered) == 5), "all five payloads must arrive"
        assert b.delivered == [{"n": i} for i in range(5)]  # contract order
        assert _wait(lambda: a.unacked == 0), "sender window must drain as acks land"
        assert b.state is ConnectionState.ESTABLISHED
    finally:
        _close(a, b)


def test_on_payload_callback_sees_ordered_run():
    a, b = _pair()
    seen = []
    b.on_payload = seen.append
    try:
        a.send("x")
        a.send("y")
        assert _wait(lambda: seen == ["x", "y"])
    finally:
        _close(a, b)


# --- ordering, gaps, duplicates -----------------------------------------

def test_out_of_order_datagrams_deliver_in_order():
    # exercise the ReceiveWindow directly over the wire: craft seqs 1,2
    # before 0's neighbor fills the gap — here: send 1 and 2 first via raw
    # sockets, then 0, and assert ordered delivery of all three
    a, b = _pair()
    try:
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        raw_sock.bind(("127.0.0.1", 0))
        b_addr = _addr(b)
        try:
            # establish the channel start with seq0 semantics: the contract
            # treats the FIRST received sequence as the start, so 0 goes
            # first; then jump ahead to 2, then fill 1 (gap -> hold -> drain)
            first = {"t": "data", "seq": 0, "p": "zero"}
            raw_sock.sendto(json.dumps(first).encode(), b_addr)
            assert _wait(lambda: b.delivered == ["zero"])
            raw_sock.sendto(json.dumps({"t": "data", "seq": 2, "p": "two"}).encode(), b_addr)
            time.sleep(0.15)
            assert b.delivered == ["zero"]  # gap at 1: delivery held
            raw_sock.sendto(json.dumps({"t": "data", "seq": 1, "p": "one"}).encode(), b_addr)
            assert _wait(lambda: b.delivered == ["zero", "one", "two"])
        finally:
            raw_sock.close()
    finally:
        _close(a, b)


def test_duplicate_datagram_is_refused_not_delivered_twice():
    a, b = _pair()
    try:
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        b_addr = _addr(b)
        try:
            d = json.dumps({"t": "data", "seq": 0, "p": "once"}).encode()
            raw_sock.sendto(d, b_addr)
            assert _wait(lambda: b.delivered == ["once"])
            raw_sock.sendto(d, b_addr)  # the duplicate
            assert _wait(lambda: len(b.refused) >= 1)
            assert "duplicate" in b.refused[0]
            time.sleep(0.2)
            assert b.delivered == ["once"]  # never double-delivered
        finally:
            raw_sock.close()
    finally:
        _close(a, b)


# --- loss and retransmission --------------------------------------------

def test_lost_datagram_is_retransmitted_and_delivered():
    a, b = _pair(retry_after=0.15, max_retries=6)
    try:
        # drop the FIRST transmission of seq1 by racing the real send with a
        # blackout window: stopgap = freeze B's socket reading is hard, so
        # instead send seq0 normally, then inject a foreign seq1 loss at the
        # window level: mark it sent, wait past retry_after, and assert the
        # retry loop re-sent it (unacked try count) and B still converges.
        a.send("zero")
        assert _wait(lambda: b.delivered == ["zero"])
        # simulate a lost first transmission: register the payload in the
        # send window WITHOUT sending it — only the retry timer will put it
        # on the wire, which is exactly what happens to a lost datagram
        with a._win_lock:
            seq = a._window.send("valuable")
            a._payloads[seq] = "valuable"
        assert _wait(lambda: b.delivered[-1] == "valuable", timeout=3.0), (
            "the retry loop must retransmit a payload the direct path lost"
        )
        assert _wait(lambda: a.unacked == 0, timeout=3.0)
        assert b.delivered.count("valuable") == 1  # retransmit dedup'd
    finally:
        _close(a, b)


def test_sequence_past_retry_cap_is_dropped_and_reported():
    drops = []
    a, b = _pair(retry_after=0.05, max_retries=2)
    a.on_drop = lambda seq, payload: drops.append((seq, payload))
    try:
        # a payload nobody will ever ack: send to a blackhole address
        a.peer = ("127.0.0.1", 1)  # nothing listens; datagrams vanish
        with a._win_lock:
            seq = a._window.send("doomed")
            a._payloads[seq] = "doomed"
        assert _wait(lambda: drops and drops[0][0] == seq, timeout=5.0), (
            "past max_retries the window drops the sequence and reports it"
        )
        assert drops[0][1] == "doomed"
        assert seq in a.dropped
    finally:
        _close(a, b)


# --- state machine -------------------------------------------------------

def test_state_machine_end_to_end_and_close():
    a, b = _pair()
    try:
        a.send("hi")
        assert _wait(lambda: b.delivered == ["hi"])
        assert a.state is ConnectionState.ESTABLISHED
        a.close()
        assert a.state is ConnectionState.CLOSED
        with pytest.raises(DatagramError, match="closed"):
            a.send("after close")
        # the peer saw bye: its machine went CLOSING -> CLOSED too
        assert _wait(lambda: b.state is ConnectionState.CLOSED, timeout=3.0)
    finally:
        _close(a, b)


def test_illegal_transition_still_raises():
    a, b = _pair()
    try:
        # the handshake may already have moved the link forward; walk the
        # legal path from wherever it is and prove CLOSED is terminal
        if a.state is ConnectionState.OPEN:
            a.tracker.transition(ConnectionState.ESTABLISHED)
        if a.state is ConnectionState.ESTABLISHED:
            a.tracker.transition(ConnectionState.CLOSING)
        if a.state is ConnectionState.CLOSING:
            a.tracker.transition(ConnectionState.CLOSED)
        assert a.state is ConnectionState.CLOSED
        with pytest.raises(SequenceError, match="illegal"):
            a.tracker.transition(ConnectionState.ESTABLISHED)
    finally:
        _close(a, b)


# --- refusals ------------------------------------------------------------

def test_oversized_payload_refused_before_wire():
    a, b = _pair(max_payload=64)
    try:
        with pytest.raises(DatagramError, match="exceeds max_payload"):
            a.send("x" * 200)
        assert a.unacked == 0  # refused before the window ever saw it
    finally:
        _close(a, b)


def test_garbage_datagram_recorded_not_fatal():
    a, b = _pair()
    try:
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            raw_sock.sendto(b"\xff\xfe not json", b._sock.getsockname())
            assert _wait(lambda: "garbage" in b.refused)
            # the link still works after hostile bytes
            a.send("still-alive")
            assert _wait(lambda: b.delivered == ["still-alive"])
        finally:
            raw_sock.close()
    finally:
        _close(a, b)


# --- authentication ------------------------------------------------------

def test_authenticated_datagrams_deliver_and_tamper_is_refused():
    pol_a = SecurityPolicy.from_secret(PSK)
    pol_b = SecurityPolicy.from_secret(PSK)
    a, b = _pair(security=pol_a)
    b.security = pol_b
    try:
        a.send("sealed-by-mac")
        assert _wait(lambda: b.delivered == ["sealed-by-mac"])
        assert not any("bad-mac" in r for r in b.refused)
        # forge a datagram with the wrong key
        from acnl_core.security import sign_value

        forged = sign_value({"t": "data", "seq": 5, "p": "evil"}, {"psk": "wrong-key"}, "psk")
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            raw_sock.sendto(json.dumps(forged).encode(), b._sock.getsockname())
            assert _wait(lambda: "bad-mac" in b.refused)
            assert "evil" not in b.delivered
        finally:
            raw_sock.close()
    finally:
        _close(a, b)


def test_unsigned_datagram_refused_when_policy_present():
    a, b = _pair(security=SecurityPolicy.from_secret(PSK))
    b.security = SecurityPolicy.from_secret(PSK)
    try:
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            raw_sock.sendto(json.dumps({"t": "data", "seq": 0, "p": "anon"}).encode(),
                            b._sock.getsockname())
            assert _wait(lambda: "bad-mac" in b.refused)
            assert b.delivered == []
        finally:
            raw_sock.close()
    finally:
        _close(a, b)
