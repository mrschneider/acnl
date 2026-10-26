"""The codec: a length-prefixed frame over a stream.

Frame = ``u32 BE length + JSON``. ``MAX_FRAME`` caps the payload; a zero-length
frame is refused as a bug, a hostile length is refused as a bug instead of
allocating what the peer asks for. ``decode`` distinguishes "need more bytes"
from "corrupt" — a partial buffer is never conflated with a parse failure.
"""

from __future__ import annotations

import json
import struct
from typing import Any, List, Optional, Tuple

MAX_FRAME = 1_048_576  # 1 MiB
HEADER = struct.Struct(">I")


class CodecError(ValueError):
    """The frame could not be decoded. A codec error is a message error:
    answered in-protocol, connection stays open."""

    def __init__(self, message: str, detail: Any = None):
        super().__init__(message)
        self.detail = detail


def encode_frame(value: Any) -> bytes:
    payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(payload) > MAX_FRAME:
        raise CodecError(f"frame too large: {len(payload)} bytes, max {MAX_FRAME}")
    return HEADER.pack(len(payload)) + payload


class FrameDecoder:
    """Incremental decoder: ``feed(data) -> list of decoded values``.

    A partial buffer returns ``[]``; the decoder keeps it. A corrupt frame
    raises ``CodecError``.
    """

    def __init__(self, max_frame: int = MAX_FRAME):
        self.max_frame = max_frame
        self._buffer = bytearray()

    def feed(self, data: bytes) -> List[Tuple[Any, int]]:
        """Append raw bytes and return ``(value, consumed)`` pairs."""
        self._buffer.extend(data)
        out: List[Tuple[Any, int]] = []
        while True:
            if len(self._buffer) < HEADER.size:
                break  # need more bytes
            (length,) = HEADER.unpack_from(self._buffer, 0)
            if length == 0:
                raise CodecError("zero-length frame: a bug, not silence")
            if length > self.max_frame:
                raise CodecError(
                    f"frame too large: {length} bytes, max {self.max_frame}"
                )
            if len(self._buffer) < HEADER.size + length:
                break  # need more bytes
            raw = bytes(self._buffer[HEADER.size : HEADER.size + length])
            del self._buffer[: HEADER.size + length]
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise CodecError(f"bad_json: {e}") from e
            out.append((value, HEADER.size + length))
        return out

    def pending(self) -> int:
        return len(self._buffer)
