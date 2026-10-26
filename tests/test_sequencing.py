import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.sequencing import (
    ConnectionTracker,
    ConnectionState,
    ReceiveWindow,
    SendWindow,
    SequenceError,
)


def test_state_machine_happy_path():
    t = ConnectionTracker()
    t.transition(ConnectionState.ESTABLISHED)
    t.transition(ConnectionState.CLOSING)
    t.transition(ConnectionState.CLOSED)
    assert t.state is ConnectionState.CLOSED


def test_state_machine_refuses_illegal_transition():
    t = ConnectionTracker()
    with pytest.raises(SequenceError):
        t.transition(ConnectionState.CLOSED)  # OPEN -> CLOSED is illegal
    t.transition(ConnectionState.ESTABLISHED)
    with pytest.raises(SequenceError):
        t.transition(ConnectionState.ESTABLISHED)
    # CLOSED is a terminal state
    t.transition(ConnectionState.CLOSED)
    with pytest.raises(SequenceError):
        t.transition(ConnectionState.ESTABLISHED)


def test_send_window_tracks_unacked():
    w = SendWindow(max_seq=4)
    s1 = w.send("one")
    s2 = w.send("two")
    assert (s1, s2) == (0, 1)
    assert w.ack(s1) is True
    assert w.ack(s1) is False  # already acked
    assert set(w.unacked) == {s2}


def test_send_window_retry_marks_and_drops():
    w = SendWindow(retry_after=0.05, max_retries=2)
    seq = w.send("x")
    old = time.time() - 1.0
    w.unacked[seq]["sent_at"] = old
    assert w.retry() == [seq]  # first retry
    w.unacked[seq]["sent_at"] = old
    assert w.retry() == [seq]  # second retry
    w.unacked[seq]["sent_at"] = old
    assert w.retry() == []  # past the cap -> dropped
    assert seq not in w.unacked


def test_receive_window_orders_out_of_order():
    r = ReceiveWindow()
    assert r.receive(1, "a") == ["a"]
    assert r.receive(3, "c") == []  # buffered, gap at 2
    assert r.receive(2, "b") == ["b", "c"]
    assert r.next_expected() == 4


def test_receive_window_refuses_duplicates():
    r = ReceiveWindow()
    r.receive(0, "a")
    with pytest.raises(SequenceError) as e:
        r.receive(0, "a")
    assert "duplicate" in str(e.value)


def test_receive_window_refuses_gap_then_duplicate():
    r = ReceiveWindow()
    r.receive(0, "a")
    r.receive(2, "c")
    with pytest.raises(SequenceError):
        r.receive(2, "c")
