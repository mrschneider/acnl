import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.errors import ErrorBody
from acnl_core.messages import WireMessage, make_envelope, new_id
from acnl_core.security import (
    AuthenticationError,
    AuthorizationError,
    SecurityPolicy,
    canonical,
    open_body,
    seal_body,
    sign_value,
    verify_value,
)

SECRET = "unit-test-psk-please-rotate"


def _frame(op="ping", sender="acnl:lab/node1/worker", recipient="acnl:lab/node1/broker"):
    return WireMessage(
        kind="request",
        envelope=make_envelope(sender=sender, recipient=recipient,
                               request_id=new_id(), message_id=new_id()),
        body={"op": op, "value": "x"},
    ).to_value()


# --- canonical JSON ------------------------------------------------------

def test_canonical_is_key_order_independent():
    a = {"b": 1, "a": {"y": 2, "x": [1, 2]}}
    b = {"a": {"x": [1, 2], "y": 2}, "b": 1}
    assert canonical(a) == canonical(b)


# --- authentication (stdlib HMAC) ---------------------------------------

def test_sign_verify_roundtrip():
    keys = {"psk": SECRET}
    signed = sign_value(_frame(), keys, "psk")
    assert signed["auth"]["kid"] == "psk"
    assert verify_value(signed, keys) is signed


def test_tampered_envelope_fails_mac():
    keys = {"psk": SECRET}
    signed = sign_value(_frame(), keys, "psk")
    signed["envelope"]["ttl"] = 99  # alter after signing
    with pytest.raises(AuthenticationError, match="mac mismatch"):
        verify_value(signed, keys)


def test_tampered_body_fails_mac():
    keys = {"psk": SECRET}
    signed = sign_value(_frame(), keys, "psk")
    signed["body"]["value"] = "forged"
    with pytest.raises(AuthenticationError):
        verify_value(signed, keys)


def test_missing_auth_block_refused():
    with pytest.raises(AuthenticationError, match="missing authentication"):
        verify_value(_frame(), {"psk": SECRET})


def test_wrong_key_refused():
    good = sign_value(_frame(), {"psk": SECRET}, "psk")
    with pytest.raises(AuthenticationError):
        verify_value(good, {"psk": "attacker-secret"})


def test_unknown_kid_refused():
    keys = {"psk": SECRET}
    signed = sign_value(_frame(), keys, "psk")
    signed["auth"]["kid"] = "ghost"
    with pytest.raises(AuthenticationError, match="unknown key id"):
        verify_value(signed, keys)


def test_mac_covers_payload_not_itself():
    # zeroing only the mac breaks verification (the MAC is computed over the
    # value minus the auth block, then compared — never over itself)
    keys = {"psk": SECRET}
    signed = sign_value(_frame(), keys, "psk")
    original = signed["auth"]["mac"]
    assert len(original) == 64
    forged = dict(signed)
    forged["auth"] = dict(signed["auth"], mac="0" * 64)
    with pytest.raises(AuthenticationError):
        verify_value(forged, keys)
    # and the auth block's presence does not change the covered bytes:
    # signing twice over an identical value yields the identical mac
    assert sign_value(_frame(), keys, "psk")["auth"] is not None
    again = sign_value(signed, keys, "psk")  # re-sign a signed value
    stripped = {k: v for k, v in again.items() if k != "auth"}
    ref = {k: v for k, v in signed.items() if k != "auth"}
    assert canonical(stripped) == canonical(ref)


# --- encryption (optional cryptography) ---------------------------------

crypto = pytest.importorskip if False else None


def _require_crypto():
    return pytest.importorskip("cryptography")


def test_seal_open_roundtrip_binds_kind_and_sender():
    _require_crypto()
    body = {"op": "reply", "references": [{"urn": "acnl:lab/node1/worker"}]}
    sealed = seal_body(body, SecurityPolicy.from_secret(SECRET).enc_key,
                       kind="reply", sender="acnl:lab/node1/broker")
    assert "enc" in sealed and "references" not in sealed
    opened = open_body(sealed["enc"], SecurityPolicy.from_secret(SECRET).enc_key,
                       kind="reply", sender="acnl:lab/node1/broker")
    assert opened == body


def test_sealed_body_fails_with_wrong_sender_aad():
    _require_crypto()
    key = SecurityPolicy.from_secret(SECRET).enc_key
    sealed = seal_body({"secret": 1}, key, kind="reply", sender="acnl:lab/node1/broker")
    with pytest.raises(AuthenticationError, match="did not decrypt"):
        open_body(sealed["enc"], key, kind="reply", sender="acnl:lab/evil/broker")


def test_sealed_body_fails_on_tamper():
    _require_crypto()
    key = SecurityPolicy.from_secret(SECRET).enc_key
    sealed = seal_body({"secret": 1}, key, kind="reply", sender="acnl:lab/node1/broker")
    import base64
    ct = bytearray(base64.b64decode(sealed["enc"]["ct"]))
    ct[0] ^= 0xFF
    sealed["enc"]["ct"] = base64.b64encode(bytes(ct)).decode()
    with pytest.raises(AuthenticationError):
        open_body(sealed["enc"], key, kind="reply", sender="acnl:lab/node1/broker")


# --- policy: protect/unprotect ------------------------------------------

def test_policy_protect_signs_and_unprotect_verifies():
    pol = SecurityPolicy.from_secret(SECRET)
    frame = _frame()
    protected = pol.protect(frame)
    assert protected["auth"]["mac"]
    assert pol.unprotect(protected)["body"] == frame["body"]


def test_policy_encrypt_hides_body_but_keeps_op_and_routing_visible():
    _require_crypto()
    pol = SecurityPolicy.from_secret(SECRET, encrypt=True)
    frame = _frame(op="advertise")
    protected = pol.protect(frame)
    body = protected["body"]
    assert body["op"] == "advertise"          # broker routing needs it
    assert "enc" in body                       # the rest is sealed
    assert protected["envelope"]["recipient"]  # envelope stays plaintext
    opened = pol.unprotect(protected)
    assert opened["body"] == frame["body"]


def test_policy_require_encryption_refuses_plaintext_request():
    pol = SecurityPolicy.from_secret(SECRET, require_encryption=True)
    signed_plaintext = SecurityPolicy.from_secret(SECRET).protect(_frame())
    with pytest.raises(AuthenticationError, match="ciphertext required"):
        pol.unprotect(signed_plaintext)


def test_require_auth_refuses_unsigned():
    pol = SecurityPolicy.from_secret(SECRET)
    with pytest.raises(AuthenticationError):
        pol.unprotect(_frame())


def test_require_auth_off_tolerates_legacy_unsigned():
    pol = SecurityPolicy.from_secret(SECRET, require_auth=False)
    out = pol.unprotect(_frame())
    assert out["body"]["op"] == "ping"
    # but a present auth block is still verified
    bad = dict(_frame())
    bad["auth"] = {"v": 1, "kid": "psk", "mac": "00" * 32}
    with pytest.raises(AuthenticationError):
        pol.unprotect(bad)


# --- authorization (ACL) -------------------------------------------------

def test_no_acl_is_allow_all():
    pol = SecurityPolicy.from_secret(SECRET)
    pol.authorize("acnl:lab/any/agent", "send", "acnl:lab/node1/broker")


def test_acl_default_deny_for_unmatched_sender():
    pol = SecurityPolicy.from_secret(SECRET, acl=[
        {"sender": "acnl:lab/node1/*", "ops": ["ping"], "recipient": "*"},
    ])
    with pytest.raises(AuthorizationError, match="acl denied"):
        pol.authorize("acnl:lab/other/agent", "ping", "acnl:lab/node1/broker")


def test_acl_allows_matching_rule_and_refuses_other_op():
    pol = SecurityPolicy.from_secret(SECRET, acl=[
        {"sender": "acnl:lab/*/*", "ops": ["ping", "resolve"], "recipient": "*"},
    ])
    pol.authorize("acnl:lab/node1/worker", "ping", "acnl:lab/node1/broker")
    with pytest.raises(AuthorizationError):
        pol.authorize("acnl:lab/node1/worker", "bind", "acnl:lab/node1/broker")


def test_acl_recipient_scoping():
    pol = SecurityPolicy.from_secret(SECRET, acl=[
        {"sender": "acnl:lab/node1/worker", "ops": ["*"],
         "recipient": "acnl:lab/node1/worker"},
    ])
    pol.authorize("acnl:lab/node1/worker", "send", "acnl:lab/node1/worker")
    with pytest.raises(AuthorizationError):
        pol.authorize("acnl:lab/node1/worker", "send", "acnl:lab/node1/other")


# --- taxonomy: new codes are typed user errors --------------------------

def test_new_error_codes_are_user_errors():
    for code in ("auth_failed", "not_authorized"):
        body = ErrorBody(code=code, message="x")
        assert body.kind == "user"
        assert body.retryable is False
