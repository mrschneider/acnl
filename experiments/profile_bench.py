"""Profile the hot paths: frame round-trip, envelope build, query eval.
Run: python3 experiments/profile_bench.py
"""
import sys, os, time, cProfile, pstats, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acnl_core.codec import encode_frame, FrameDecoder
from acnl_core.messages import make_envelope, WireMessage, new_id
from acnl_core.addresses import Reference
from acnl_core.attributes import parse_query, _eval
from acnl_core.directory import InMemoryDirectory


def sample_value():
    env = make_envelope(sender="acnl:lab/b/a", recipient="acnl:lab/b/b")
    return {"kind": "request", "envelope": env.to_value(), "body": {"op": "ping"}}


def bench_json_roundtrip(n=20000):
    v = sample_value()
    data = encode_frame(v)
    # measure encode
    t0 = time.perf_counter()
    for _ in range(n):
        encode_frame(v)
    enc = (time.perf_counter() - t0) / n * 1e6
    # measure decode
    dec = FrameDecoder()
    t0 = time.perf_counter()
    for _ in range(n):
        dec.feed(data)
        dec._buffer.clear()
    dec_ = (time.perf_counter() - t0) / n * 1e6
    print(f"JSON encode: {enc:.2f} us/frame")
    print(f"JSON decode: {dec_:.2f} us/frame")


def bench_envelope(n=20000):
    t0 = time.perf_counter()
    for _ in range(n):
        make_envelope(sender="acnl:lab/b/a", recipient="acnl:lab/b/b")
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"make_envelope: {dt:.2f} us (includes 2x parse_urn + 2x uuid4)")


def bench_new_id(n=100000):
    t0 = time.perf_counter()
    for _ in range(n):
        new_id()
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"new_id (uuid4): {dt:.2f} us")


def bench_query_eval(n=50):
    d = InMemoryDirectory()
    for i in range(50000):
        d.bind(f"acnl:lab/x/s{i}", Reference(urn=f"acnl:lab/x/s{i}", capabilities=["svc"], attributes={"shard": i % 32, "load": i % 100}))
    ast = parse_query('load CONTAINS 42 OR shard = 31')
    t0 = time.perf_counter()
    for _ in range(n):
        d.query(ast)
    dt = (time.perf_counter() - t0) / n * 1000
    print(f"query OR+CONTAINS@50k: {dt:.3f} ms")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"
    if cmd in ("all", "json"): bench_json_roundtrip()
    if cmd in ("all", "env"): bench_envelope()
    if cmd in ("all", "id"): bench_new_id()
    if cmd in ("all", "q"): bench_query_eval()
