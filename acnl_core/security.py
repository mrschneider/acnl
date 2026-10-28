"""Authentication, authorization, and encryption for the wire.

Design rules (all IO-free, stdlib-first):

- **Authentication** is a per-hop HMAC-SHA256 over the canonical JSON of the
  frame minus its ``auth`` block. It is computed by the *writer* (so every
  hop re-signs after mutating ``route``/``ttl``) and verified by the
  *reader* before parsing. Canonical JSON is ``sort_keys`` + compact
  separators: the MAC does not depend on dict insertion order.
- **Replay** is covered by the broker's existing ``(sender, message_id)``
  dedupe window for requests; there are no per-sender counters (documented
  gap, not silently implied).
- **Encryption** is AES-256-GCM over the *body* only: the envelope stays
  plaintext because brokers route on ``sender``/``recipient`` (and on the
  visible ``op`` of a request). The AAD binds ``kind`` + origin ``sender``
  (stable across hops — ``route``/``ttl`` mutate, so they cannot be in the
  AAD). Encryption needs the optional ``cryptography`` package
  (``pip install acnl[security]``); authentication and authorization are
  pure stdlib.
- **Authorization** is a default-deny ACL: rules match ``sender`` and
  ``recipient`` by ``fnmatch`` patterns and an explicit op list. ``acl=None``
  means no policy is configured (allow-all, the pre-security behavior);
  any configured ACL defaults to deny.
- **PSK trust model**: one keyring shared by the mesh. Encryption protects
  the wire against observers, not against mesh members — every holder of
  the key can read every body. That is honest for a broker mesh and is
  exactly what a PKI would add later (documented gap).
"""

from __future__ import annotations

import base64
import fnmatch
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

AUTH_VERSION = 1
ENC_ALG = "A256GCM"


class SecurityError(Exception):
    """Base for every security refusal (authentication and decryption)."""


class AuthenticationError(SecurityError):
    """The frame did not authenticate or did not decrypt."""


class AuthorizationError(SecurityError):
    """The authenticated sender is not allowed this operation."""


def canonical(value: Any) -> bytes:
    """Deterministic bytes for MACing/AAD: sorted keys, compact separators."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _secret_bytes(secret: Any) -> bytes:
    if isinstance(secret, bytes):
        return secret
    if isinstance(secret, str):
        return secret.encode("utf-8")
    raise SecurityError(f"key material must be bytes or str, got {type(secret).__name__}")


def _lookup(keys: Dict[str, Any], kid: Optional[str]) -> bytes:
    if not keys:
        raise SecurityError("no keys configured")
    if kid is None:
        if len(keys) == 1:
            kid = next(iter(keys))
        else:
            raise SecurityError("multiple keys configured and no kid given")
    if kid not in keys:
        raise SecurityError(f"unknown key id {kid!r}")
    return _secret_bytes(keys[kid])


def sign_value(value: Dict[str, Any], keys: Dict[str, Any], kid: Optional[str] = None) -> Dict[str, Any]:
    """Return a copy of ``value`` with an ``auth`` block attached."""
    if kid is None:
        kid = next(iter(keys)) if len(keys) == 1 else None
    secret = _lookup(keys, kid)
    unsigned = {k: v for k, v in value.items() if k != "auth"}
    mac = hmac.new(secret, canonical(unsigned), hashlib.sha256).hexdigest()
    out = dict(value)
    out["auth"] = {"v": AUTH_VERSION, "kid": kid, "mac": mac}
    return out


def verify_value(value: Dict[str, Any], keys: Dict[str, Any]) -> Dict[str, Any]:
    """Verify the ``auth`` block. Raises ``AuthenticationError``; never
    returns an unverified value."""
    if not isinstance(value, dict):
        raise AuthenticationError("frame is not an object: nothing to authenticate")
    auth = value.get("auth")
    if not isinstance(auth, dict):
        raise AuthenticationError("missing authentication block: frame was not signed by a mesh key")
    if auth.get("v") != AUTH_VERSION:
        raise AuthenticationError(f"unsupported auth version {auth.get('v')!r}")
    try:
        secret = _lookup(keys, auth.get("kid"))
    except SecurityError as e:
        raise AuthenticationError(str(e)) from e
    unsigned = {k: v for k, v in value.items() if k != "auth"}
    expected = hmac.new(secret, canonical(unsigned), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, str(auth.get("mac", ""))):
        raise AuthenticationError("mac mismatch: frame was altered or forged")
    return value


def seal_body(body: Dict[str, Any], key: Any, *, kind: str, sender: str) -> Dict[str, Any]:
    """AES-256-GCM seal of a body -> ``{"enc": {"alg", "n", "ct"}}`` (b64)."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as e:  # pragma: no cover - depends on environment
        raise SecurityError(
            "encryption requires the optional 'cryptography' package: pip install acnl[security]"
        ) from e
    secret = _secret_bytes(key)
    if len(secret) != 32:
        raise SecurityError(f"AES-256 key must be 32 bytes, got {len(secret)}")
    nonce = secrets.token_bytes(12)
    aad = canonical({"kind": kind, "sender": sender})
    ct = AESGCM(secret).encrypt(nonce, canonical(body), aad)
    return {"enc": {"alg": ENC_ALG, "n": base64.b64encode(nonce).decode(), "ct": base64.b64encode(ct).decode()}}


def open_body(enc_block: Dict[str, Any], key: Any, *, kind: str, sender: str) -> Dict[str, Any]:
    """Open a ``seal_body`` block. Raises ``AuthenticationError`` on any
    failure (tamper, wrong key, wrong AAD) — fail closed, same class as a
    bad MAC, because a GCM tag failure *is* a failed authentication."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as e:  # pragma: no cover
        raise SecurityError(
            "encryption requires the optional 'cryptography' package: pip install acnl[security]"
        ) from e
    if not isinstance(enc_block, dict) or enc_block.get("alg") != ENC_ALG:
        raise AuthenticationError(f"unsupported encryption alg {getattr(enc_block, 'get', lambda k, d=None: d)('alg')!r}")
    try:
        nonce = base64.b64decode(enc_block["n"])
        ct = base64.b64decode(enc_block["ct"])
    except (KeyError, ValueError) as e:
        raise AuthenticationError(f"malformed ciphertext block: {e}") from e
    aad = canonical({"kind": kind, "sender": sender})
    try:
        plain = AESGCM(_secret_bytes(key)).decrypt(nonce, ct, aad)
        return json.loads(plain.decode("utf-8"))
    except Exception as e:  # InvalidTag and friends: all are auth failures
        raise AuthenticationError(f"body did not decrypt (tamper, wrong key, or wrong AAD): {e}") from e


@dataclass
class SecurityPolicy:
    """One mesh-wide policy: keyring + what this participant demands and emits.

    - ``require_auth`` (default True): refuse unsigned or bad-MAC frames.
    - ``encrypt`` (default False): seal bodies on the way out.
    - ``require_encryption`` (default False): refuse plaintext request/reply
      bodies on the way in.
    - ``acl`` (default None): no policy configured -> allow (pre-security
      behavior). Any list -> default deny.
    """

    keys: Dict[str, bytes] = field(default_factory=dict)
    default_kid: Optional[str] = None
    require_auth: bool = True
    encrypt: bool = False
    require_encryption: bool = False
    acl: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def from_secret(
        cls,
        secret: Any,
        kid: str = "psk",
        *,
        encrypt: bool = False,
        require_encryption: bool = False,
        acl: Optional[List[Dict[str, Any]]] = None,
        require_auth: bool = True,
    ) -> "SecurityPolicy":
        return cls(
            keys={kid: _secret_bytes(secret)},
            default_kid=kid,
            require_auth=require_auth,
            encrypt=encrypt,
            require_encryption=require_encryption,
            acl=acl,
        )

    @property
    def enc_key(self) -> bytes:
        """The AES key: derived from the default keyring entry (any single
        32+ byte secret) via SHA-256, so one PSK feeds MAC and cipher."""
        raw = _lookup(self.keys, self.default_kid)
        return hashlib.sha256(raw).digest()

    # -- writer side -----------------------------------------------------
    def protect(self, value: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt (policy on) then sign. Called by the writer for every
        outgoing frame; each hop protects afresh after mutating the
        envelope."""
        kind = str(value.get("kind") or "")
        body = value.get("body")
        out = dict(value)
        if self.encrypt and body is not None:
            if not (isinstance(body, dict) and "enc" in body and "op" not in body):
                # do not double-seal; a request body keeps its op visible
                # for broker routing, everything else is sealed whole
                if isinstance(body, dict) and kind == "request" and "op" in body:
                    sealed = seal_body(
                        {k: v for k, v in body.items() if k != "op"},
                        self.enc_key, kind=kind, sender=value.get("envelope", {}).get("sender", ""),
                    )
                    out["body"] = {"op": body["op"], **sealed}
                else:
                    sealed = seal_body(body, self.enc_key, kind=kind,
                                       sender=value.get("envelope", {}).get("sender", ""))
                    out["body"] = sealed
        if self.require_auth:
            out = sign_value(out, self.keys, self.default_kid)
        else:
            # a policy holder still authenticates what it emits — peers that
            # do not demand auth will simply ignore the block
            out = sign_value(out, self.keys, self.default_kid)
        return out

    # -- reader side -----------------------------------------------------
    def verify(self, value: Dict[str, Any]) -> Dict[str, Any]:
        if not self.require_auth:
            auth = value.get("auth") if isinstance(value, dict) else None
            if auth is None:
                return value  # legacy peer, tolerated by policy
            return verify_value(value, self.keys)
        return verify_value(value, self.keys)

    def unprotect(self, value: Dict[str, Any]) -> Dict[str, Any]:
        """Reader side for clients: verify then open. Raises
        ``AuthenticationError``; never returns an unverified frame."""
        value = self.verify(value)
        return self.open_value(value, require=self.require_encryption)

    def open_value(self, value: Dict[str, Any], require: bool = False) -> Dict[str, Any]:
        """Open the body if sealed (idempotent). ``require=True`` refuses
        plaintext ``request``/``reply`` bodies."""
        body = value.get("body")
        kind = str(value.get("kind") or "")
        sender = value.get("envelope", {}).get("sender", "") or ""
        if isinstance(body, dict) and "enc" in body:
            opened = open_body(body["enc"], self.enc_key, kind=kind, sender=sender)
            if isinstance(body, dict) and "op" in body:
                opened = {"op": body["op"], **opened}
            out = dict(value)
            out["body"] = opened
            return out
        if require and kind in ("request", "reply"):
            raise AuthenticationError(
                "ciphertext required: peer sent a plaintext request/reply body "
                "(require_encryption is on)"
            )
        return value

    # -- authorization ---------------------------------------------------
    def authorize(self, sender: str, op: str, recipient: str) -> None:
        """Default-deny when an ACL is configured. Raises
        ``AuthorizationError`` naming sender, op, and recipient."""
        if self.acl is None:
            return
        for rule in self.acl:
            if not fnmatch.fnmatchcase(sender, rule.get("sender", "*")):
                continue
            if not fnmatch.fnmatchcase(recipient, rule.get("recipient", "*")):
                continue
            ops = rule.get("ops", ["*"])
            if "*" in ops or op in ops:
                return
        raise AuthorizationError(
            f"acl denied: sender {sender!r} may not invoke {op!r} on {recipient!r} "
            f"(default-deny: no rule matched)"
        )
