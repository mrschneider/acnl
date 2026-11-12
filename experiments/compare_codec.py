"""Compare JSON vs msgpack for frame codec, and measure envelope decode cost."""
import sys, os, time, msgpack
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from acnl_core.codec import encode_frame, FrameDecoder
from acnl_core.messages import make_envelope


def sample():
    env = make_envelope(sender="acnl:lab/b/a", recipient="acnl:lab/b/b")
    return {"kind": "request", "envelope": env.to_value(), "body": {"op": "ping"}}


def bench_json(n=30000):
    v = sample()
    data = encode_frame(v)
    t0 = time.perf_counter()
    for _ in range(n):
        encode_frame(v)
    enc = (time.perf_counter() - t0) / n * 1e6
    dec = FrameDecoder()
    t0 = time.perf_counter()
    for _ in range(n):
        dec.feed(data); dec._buffer.clear()
    dec_ = (time.perf_counter() - t0) / n * 1e6
    print(f"JSON  enc={enc:.2f}us dec={dec_:.2f}us size={len(data)}B")


def bench_msgpack(n=30000):
    v = sample()
    data = msgpack.packb(v, use_bin_type=True)
    t0 = time.perf_counter()
    for _ in range(n):
        msgpack.packb(v, use_bin_type=True)
    enc = (time.perf_counter() - t0) / n * 1e6
    t0 = time.perf_counter()
    for _ in range(n):
        msgpack.unpackb(data, raw=False)
    dec_ = (time.perf_counter() - t0) / n * 1e6
    print(f"msgpack enc={enc:.2f}us dec={dec_:.2f}us size={len(data)}B")


if __name__ == "__main__":
    bench_json()
    bench_msgpack()
