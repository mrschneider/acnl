"""The datagram plane: the sequenced exchange, end-to-end over UDP.

``acnl_core.sequencing`` defines the contract (state machine, send window,
receive window) with no IO. This module is the transport that exercises it
over real UDP sockets:

- ``{"t": "data", "seq": N, "p": payload}`` / ``{"t": "ack", "seq": N}``
  control datagrams (plus ``hello``/``bye`` for the state machine),
  JSON-encoded, hard-capped at ``MAX_DATAGRAM`` (refused, never truncated).
- The sender keeps payload in the ``SendWindow`` and retransmits after
  ``retry_after`` until acked; sequences past ``max_retries`` are dropped,
  recorded, and reported through ``on_drop``.
- The receiver feeds the ``ReceiveWindow``: out-of-order datagrams buffer,
  gaps hold delivery, duplicates are *refused* (recorded in ``refused``,
  never double-delivered) and contiguous runs are delivered in order.
- The state machine is the contract's: ``OPEN`` on creation,
  ``ESTABLISHED`` on first contact, ``CLOSING`` -> ``CLOSED`` on close;
  an illegal transition raises ``SequenceError``.
- With a ``SecurityPolicy`` each datagram carries the same stdlib HMAC
  ``auth`` block as the TCP plane and is verified on receipt. Honest scope:
  the UDP plane *authenticates*; it does not re-encrypt (payload
  confidentiality is whatever sealing the carried message already has).
- One lock guards the send window: the receiver (acks), the retry timer,
  and the caller all touch it.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from acnl_core.security import SecurityError, SecurityPolicy, sign_value, verify_value
from acnl_core.sequencing import (
    ConnectionTracker,
    ConnectionState,
    ReceiveWindow,
    SendWindow,
    SequenceError,
)

MAX_DATAGRAM = 65_507  # UDP payload ceiling; refuse, never truncate


class DatagramError(ValueError):
    """Refused before hitting the wire: oversized, wrong state, or bad bytes."""


@dataclass
class UdpLink:
    """One sequenced UDP exchange between two endpoints."""

    local: tuple
    peer: tuple
    security: Optional[SecurityPolicy] = None
    max_payload: int = 16_384
    retry_after: float = 0.2
    max_retries: int = 5
    on_payload: Optional[Callable[[Any], None]] = None
    on_drop: Optional[Callable[[int, Any], None]] = None

    tracker: ConnectionTracker = field(default_factory=ConnectionTracker)
    incoming: ReceiveWindow = field(default_factory=ReceiveWindow)
    delivered: List[Any] = field(default_factory=list)
    refused: List[str] = field(default_factory=list)  # dup/bad-mac/garbage
    dropped: List[int] = field(default_factory=list)  # seqs past the retry cap

    _window: SendWindow = field(default=None, repr=False)  # type: ignore[assignment]  (set in __post_init__)
    _sock: Optional[socket.socket] = field(default=None, repr=False)
    _recv_thread: Optional[threading.Thread] = field(default=None, repr=False)
    _retry_thread: Optional[threading.Thread] = field(default=None, repr=False)
    _stop: bool = field(default=False, repr=False)
    _opened: bool = field(default=False, repr=False)
    _win_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _est_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _payloads: Dict[int, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        self._window = SendWindow(retry_after=self.retry_after, max_retries=self.max_retries)

    # -- lifecycle -------------------------------------------------------
    def open(self) -> "UdpLink":
        if self._opened:
            return self
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(self.local)
        self._sock.settimeout(0.1)
        # an explicit hello gives the state machine its OPEN -> ESTABLISHED
        # transition on first contact, not on first payload
        self._send_raw({"t": "hello"})
        self._opened = True
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True, name="acnl-udp-recv")
        self._retry_thread = threading.Thread(target=self._retry_loop, daemon=True, name="acnl-udp-retry")
        self._recv_thread.start()
        self._retry_thread.start()
        return self

    def close(self) -> None:
        if not self._opened:
            return
        state = self.tracker.state
        if state in (ConnectionState.OPEN, ConnectionState.ESTABLISHED):
            self.tracker.transition(ConnectionState.CLOSING)
        try:
            self._send_raw({"t": "bye"})
        except OSError:
            pass
        if self.tracker.state is ConnectionState.CLOSING:
            self.tracker.transition(ConnectionState.CLOSED)
        self._stop = True
        for t in (self._recv_thread, self._retry_thread):
            if t is not None:
                t.join(timeout=1.0)
        if self._sock is not None:
            self._sock.close()
        self._opened = False

    @property
    def state(self) -> ConnectionState:
        return self.tracker.state

    @property
    def unacked(self) -> int:
        with self._win_lock:
            return len(self._window.unacked)

    # -- sending ---------------------------------------------------------
    def send(self, payload: Any) -> int:
        """Queue a payload under a new sequence number. Refuses on a closing
        or closed link: a datagram plane does not resurrect itself."""
        if self.tracker.state in (ConnectionState.CLOSING, ConnectionState.CLOSED):
            raise DatagramError(f"cannot send on a {self.tracker.state.value} link")
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(encoded) > self.max_payload:
            raise DatagramError(
                f"payload of {len(encoded)} bytes exceeds max_payload {self.max_payload}"
            )
        with self._win_lock:
            seq = self._window.send(payload)
            self._payloads[seq] = payload
        self._send_raw({"t": "data", "seq": seq, "p": payload})
        return seq

    def _send_raw(self, value: Dict[str, Any]) -> None:
        if self.security is not None:
            value = sign_value(value, self.security.keys, self.security.default_kid)
        raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(raw) > MAX_DATAGRAM:
            raise DatagramError(f"datagram of {len(raw)} bytes exceeds UDP ceiling {MAX_DATAGRAM}")
        if self._sock is None:
            raise DatagramError("link not open")
        self._sock.sendto(raw, self.peer)

    # -- receiving -------------------------------------------------------
    def _recv_loop(self) -> None:
        assert self._sock is not None
        while not self._stop:
            try:
                raw, _addr = self._sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self.refused.append("garbage")
                continue
            if not isinstance(value, dict):
                self.refused.append("not-an-object")
                continue
            if self.security is not None:
                try:
                    verify_value(value, self.security.keys)
                except SecurityError:
                    self.refused.append("bad-mac")
                    continue
            t = value.get("t")
            if t == "hello":
                self._become_established()
                continue
            if t == "bye":
                state = self.tracker.state
                if state in (ConnectionState.OPEN, ConnectionState.ESTABLISHED):
                    self.tracker.transition(ConnectionState.CLOSING)
                    self.tracker.transition(ConnectionState.CLOSED)
                self._stop = True
                continue
            if t == "ack":
                with self._win_lock:
                    if self._window.ack(int(value.get("seq", -1))):
                        self._payloads.pop(int(value.get("seq", -1)), None)
                continue
            if t != "data":
                self.refused.append(f"unknown-type:{t}")
                continue
            seq = value.get("seq")
            if not isinstance(seq, int):
                self.refused.append("bad-seq")
                continue
            self._become_established()
            # ack every data datagram: an ack may be lost, and a duplicate
            # ack is harmless (the receive window refuses the dup payload)
            try:
                self._send_raw({"t": "ack", "seq": seq})
            except (OSError, DatagramError):
                pass
            try:
                ordered = self.incoming.receive(seq, value.get("p"))
            except SequenceError as e:
                # the contract refuses duplicates; the transport records the
                # refusal and keeps the connection (R7.2: message stays open)
                self.refused.append(str(e))
                ordered = []
            for payload in ordered:
                self.delivered.append(payload)
                if self.on_payload is not None:
                    self.on_payload(payload)

    def _become_established(self) -> None:
        with self._est_lock:
            if self.tracker.state is ConnectionState.OPEN:
                self.tracker.transition(ConnectionState.ESTABLISHED)

    # -- retransmission --------------------------------------------------
    def _retry_loop(self) -> None:
        while not self._stop:
            time.sleep(max(0.02, self.retry_after / 4))
            if self._stop:
                break
            expired: List[int] = []
            over_cap: List[int] = []
            with self._win_lock:
                before = set(self._window.unacked)
                expired = self._window.retry()
                # retry() deletes sequences past max_retries: whatever
                # vanished without being acked is a drop, by definition
                over_cap = sorted(before - set(self._window.unacked))
                for seq in over_cap:
                    self.dropped.append(seq)
            for seq in over_cap:
                if self.on_drop is not None:
                    self.on_drop(seq, self._payloads.pop(seq, None))
            for seq in expired:
                with self._win_lock:
                    item = self._window.unacked.get(seq)
                if item is None:
                    continue
                try:
                    self._send_raw({"t": "data", "seq": seq, "p": item["payload"]})
                except (OSError, DatagramError):
                    break
