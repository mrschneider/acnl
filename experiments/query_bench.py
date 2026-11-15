"""Fair before/after comparison of the query optimization.

Usage:
    python3 experiments/query_bench.py          # current code
    git stash && python3 experiments/query_bench.py && git stash pop

Builds a 50k-entry table with an indexed attribute and runs queries that
exercise candidate selection (equality on the indexed attr, OR of equalities,
AND with NOT). Reports best-of-3 latency.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acnl_core.directory import InMemoryDirectory
from acnl_core.addresses import Reference
from acnl_core.attributes import parse_query


def build(n=50_000, load_max=100):
    d = InMemoryDirectory()
    for i in range(n):
        urn = f"acnl:lab/x/s{i}"
        d.bind(urn, Reference(urn=urn, capabilities=["svc"],
                              attributes={"shard": i % 32, "load": i % load_max}))
    return d


def timed(label, fn, reps=5):
    best = float("inf")
    res = None
    for _ in range(reps):
        t0 = time.perf_counter()
        res = fn()
        best = min(best, time.perf_counter() - t0)
    print(f"{label:44s} {best*1e6:10.1f}us  {1/best:11.0f}/s  results={len(res)}")


def run(d):
    timed("load = 42 (indexed equality)", lambda: d.query(parse_query("load = 42")))
    timed("shard = 31 (indexed equality)", lambda: d.query(parse_query("shard = 31")))
    timed("load = 42 OR shard = 31 (OR union)",
          lambda: d.query(parse_query("load = 42 OR shard = 31")))
    timed("load = 42 AND shard = 31 (AND intersect)",
          lambda: d.query(parse_query("load = 42 AND shard = 31")))
    timed("load = 42 AND NOT (shard = 31)",
          lambda: d.query(parse_query("load = 42 AND NOT (shard = 31)")))
    timed("shard = 3 OR shard = 4 OR shard = 5",
          lambda: d.query(parse_query("shard = 3 OR shard = 4 OR shard = 5")))
    timed("full scan (no selective conjunct)",
          lambda: d.query(parse_query("shard = 3 OR shard = 4 OR shard = 5 OR shard = 6 OR shard = 7")))


if __name__ == "__main__":
    run(build())
