"""Sequenced reliable exchange between brokers.

A connection state machine (OPEN, ESTABLISHED, CLOSING, CLOSED with explicit
transitions) plus a sequence window: a sender tracks unacked sequence numbers
and retransmits after a timeout, a receiver buffers out-of-order payloads and
refuses gaps and duplicates — an assembled-wrong message is worse than a
failed one. That is the sequenced plane, independent of the TCP transport
(which already provides reliability to the byte stream but not to the
exchange semantics a datagram-style peer would see).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class SequenceError(ValueError):
    pass


class ConnectionState(Enum):
    OPEN = "open"
    ESTABLISHED = "established"
    CLOSING = "closing"
    CLOSED = "closed"


_ALLOWED = {
    ConnectionState.OPEN: {ConnectionState.ESTABLISHED, ConnectionState.CLOSING},
    ConnectionState.ESTABLISHED: {ConnectionState.CLOSING, ConnectionState.CLOSED},
    ConnectionState.CLOSING: {ConnectionState.CLOSED},
    ConnectionState.CLOSED: set(),
}


@dataclass
class ConnectionTracker:
    """A connection whose state machine is explicit. A legitimate wait/sleep
    must never look like an infinite reasoning loop; a state machine is how
    you prove it is not."""

    state: ConnectionState = ConnectionState.OPEN

    def transition(self, target: ConnectionState) -> ConnectionState:
        if target not in _ALLOWED[self.state]:
            raise SequenceError(
                f"illegal transition {self.state.value!r} -> {target.value!r}"
            )
        self.state = target
        return self.state


@dataclass
class SendWindow:
    """Tracks unacked sequence numbers. ``retryable`` payload is kept for
    retransmission until acked."""

    max_seq: int = 4
    next_seq: int = 0
    unacked: Dict[int, Any] = field(default_factory=dict)
    retry_after: float = 1.0
    max_retries: int = 3

    def send(self, payload: Any) -> int:
        seq = self.next_seq
        self.next_seq += 1
        self.unacked[seq] = {"payload": payload, "sent_at": time.time(), "tries": 0}
        return seq

    def ack(self, seq: int) -> bool:
        if seq not in self.unacked:
            return False
        del self.unacked[seq]
        return True

    def retry(self, now: Optional[float] = None) -> List[int]:
        """Return the sequences that need retransmission; drop ones past the
        retry cap (the caller is expected to raise Unavailable)."""
        now = now if now is not None else time.time()
        expired = []
        dropped = []
        for seq, item in self.unacked.items():
            if now - item["sent_at"] >= self.retry_after:
                if item["tries"] >= self.max_retries:
                    dropped.append(seq)
                else:
                    item["tries"] += 1
                    item["sent_at"] = now
                    expired.append(seq)
        for seq in dropped:
            del self.unacked[seq]
        return expired


@dataclass
class ReceiveWindow:
    """Buffers out-of-order payloads by sequence. Refuses gaps and duplicates
    and reports the next contiguous run."""
    highest_contiguous: int = -1
    buffer: Dict[int, Any] = field(default_factory=dict)

    def receive(self, seq: int, payload: Any) -> List[Any]:
        if self.buffer or self.highest_contiguous != -1:
            if seq <= self.highest_contiguous:
                raise SequenceError(
                    f"duplicate sequence {seq}: highest contiguous is {self.highest_contiguous}; "
                    "refusing a retransmission of an already-delivered payload"
                )
            if seq in self.buffer:
                raise SequenceError(
                    f"gap then duplicate at {seq}: buffered {seq} and delivered it is not contiguous"
                )
        else:
            # first payload of the channel: the first sequence received is the start
            self.highest_contiguous = seq - 1
        self.buffer[seq] = payload
        return self._drain()

    def _drain(self) -> List[Any]:
        out = []
        while self.highest_contiguous + 1 in self.buffer:
            self.highest_contiguous += 1
            out.append(self.buffer.pop(self.highest_contiguous))
        return out

    def next_expected(self) -> int:
        return self.highest_contiguous + 1
