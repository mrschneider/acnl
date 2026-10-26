import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.messages import (
    OPERATIONS,
    WIRE_KINDS,
    Envelope,
    MessageError,
    WireMessage,
    make_envelope,
)


def _env(sender="acnl:lab/node1/worker", recipient="acnl:lab/node1/broker", **kw):
    return make_envelope(sender=sender, recipient=recipient, **kw)


def test_closed_vocabulary_unknown_kind_refused_naming_the_word():
    with pytest.raises(MessageError) as e:
        WireMessage.from_value({"kind": "agent_step", "envelope": _env().to_value(), "body": {}})
    assert "agent_step" in str(e.value)
    assert "hello" in str(e.value)  # the legal set is named


def test_unknown_verb_refused_with_legal_set():
    with pytest.raises(MessageError) as e:
        WireMessage.from_value(
            {"kind": "request", "envelope": _env().to_value(), "body": {"op": "do_magic"}}
        )
    assert "do_magic" in str(e.value)
    for op in OPERATIONS:
        assert op in str(e.value)


def test_wire_kinds_are_exactly_seven():
    assert len(WIRE_KINDS) == 7
    assert set(WIRE_KINDS) == {"hello", "request", "reply", "error", "notify", "fragment", "close"}


def test_operations_are_exactly_seven():
    assert len(OPERATIONS) == 7
    assert set(OPERATIONS) == {"advertise", "withdraw", "resolve", "query", "send", "bind", "ping"}


def test_envelope_roundtrip():
    env = _env(ttl=5, route=["acnl:a/b/c"])
    parsed = Envelope.from_value(env.to_value())
    assert parsed == env
    assert parsed.ttl == 5
    assert parsed.route == ["acnl:a/b/c"]


def test_envelope_missing_field():
    with pytest.raises(MessageError) as e:
        Envelope.from_value({"sender": "acnl:a/b/c"})
    assert "request_id" in str(e.value)


def test_envelope_bad_urn():
    with pytest.raises(MessageError):
        Envelope.from_value(
            {
                "sender": "not-a-urn",
                "recipient": "acnl:a/b/c",
                "request_id": "r",
                "message_id": "m",
            }
        )


def test_version_major_mismatch_refused():
    with pytest.raises(MessageError) as e:
        Envelope.from_value(
            {
                "sender": "acnl:a/b/c",
                "recipient": "acnl:a/b/c",
                "request_id": "r",
                "message_id": "m",
                "version": "2.0",
            }
        )
    assert "version mismatch" in str(e.value)


def test_version_minor_accepted_additive():
    env = Envelope.from_value(
        {
            "sender": "acnl:a/b/c",
            "recipient": "acnl:a/b/c",
            "request_id": "r",
            "message_id": "m",
            "version": "1.7",
        }
    )
    assert env.version == "1.7"


def test_ttl_bounds():
    with pytest.raises(MessageError):
        _env(ttl=0)
    with pytest.raises(MessageError):
        _env(ttl=17)


def test_error_body_validated():
    with pytest.raises(MessageError) as e:
        WireMessage.from_value(
            {
                "kind": "error",
                "envelope": _env().to_value(),
                "body": {"code": "nope"},
            }
        )
    assert "retryable" in str(e.value)


def test_fragment_body_validated():
    with pytest.raises(MessageError):
        WireMessage.from_value(
            {
                "kind": "fragment",
                "envelope": _env().to_value(),
                "body": {"request_id": "r", "index": "not an int", "total": 2, "data": "aa=="},
            }
        )


def test_close_has_no_body():
    env = _env()
    msg = WireMessage(kind="close", envelope=env)
    assert msg.to_value() == {"kind": "close", "envelope": env.to_value()}
    parsed = WireMessage.from_value(msg.to_value())
    assert parsed.kind == "close"
    assert parsed.body is None
