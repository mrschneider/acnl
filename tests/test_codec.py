import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from acnl_core.codec import CodecError, FrameDecoder, encode_frame


def test_roundtrip():
    value = {"kind": "hello", "x": 1}
    frame = encode_frame(value)
    dec = FrameDecoder()
    assert dec.feed(frame) == [(value, len(frame))]


def test_partial_buffer():
    dec = FrameDecoder()
    frame = encode_frame({"a": 1})
    assert dec.feed(frame[:5]) == []
    assert dec.pending() == 5
    assert dec.feed(frame[5:]) == [({"a": 1}, len(frame))]


def test_two_frames_in_one_read():
    dec = FrameDecoder()
    f1 = encode_frame({"a": 1})
    f2 = encode_frame({"b": 2})
    assert dec.feed(f1 + f2) == [({"a": 1}, len(f1)), ({"b": 2}, len(f2))]


def test_zero_length_frame_refused():
    dec = FrameDecoder()
    try:
        dec.feed(b"\x00\x00\x00\x00")
        raise AssertionError("zero-length frame must be refused")
    except CodecError as e:
        assert "zero-length" in str(e)


def test_hostile_length_refused():
    dec = FrameDecoder()
    huge = (2**31).to_bytes(4, "big")
    try:
        dec.feed(huge)
        raise AssertionError("hostile length must be refused")
    except CodecError as e:
        assert "too large" in str(e)


def test_bad_json_refused():
    dec = FrameDecoder()
    import struct

    payload = b"not json"
    frame = struct.pack(">I", len(payload)) + payload
    try:
        dec.feed(frame)
        raise AssertionError("bad json must be refused")
    except CodecError as e:
        assert "bad_json" in str(e)


def test_encode_frame_max():
    try:
        encode_frame({"x": "a" * 1_048_577})
        raise AssertionError("oversized frame must be refused")
    except CodecError as e:
        assert "too large" in str(e)
