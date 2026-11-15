"""Compare directory.query() performance: full-scan vs candidate-selection.

Usage:
    python3 experiments/compare_query.py            # current code
    git stash && python3 experiments/compare_query.py && git stash pop

Builds a realistic 50k-entry table and runs a battery of queries, reporting
per-query latency and throughput. The candidate-selection path only helps
when it actually narrows the scan; these queries are chosen to exercise that.
"""
import sys, os, time, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acnl_core.directory import InMemoryDirectory
from acnl_core.addresses import Reference
from acnl_core.attributes import parse_query

random.seed(7)
N = 50_000
ZONES = ["lab", "home", "office", "travel"]
TYPES = ["request", "response", "ping", "advertise", "close"]
TAGS = [f"t{i}" for i in range(200)]


def build():
    d = InMemoryDirectory()
    for i in range(N):
        urn = f"acnl:{random.choice(ZONES)}/s{i}/a{i}"
        ref = Reference(urn=urn, attributes={"type": random.choice(TYPES),
                                             "tag": random.choice(TAGS),
                                             "seq": i, "zone": urn.split("/")[0]})
        d.bind(urn, ref)
    return d


def timed(label, fn, reps=3):
    best = float("inf")
    total = 0.0
    for _ in range(reps):
        t0 = time.perf_counter()
        res = fn()
        dt = time.perf_counter() - t0
        best = min(best, dt)
        total += dt
    n = len(res)
    print(f"{label:48s} {best*1e6:10.1f}us  {1/best:11.0f}/s  results={n}")


def run(d):
    # selective equality on indexed attr
    timed("tag = t50", lambda: d.query(parse_query("tag = t50")))
    timed("zone = lab", lambda: d.query(parse_query("zone = lab")))
    # selective equality on urn
    timed('urn = acnl:lab/s1/a5', lambda: d.query(parse_query('urn = "acnl:lab/s1/a5"')))
    # AND of two selective conjuncts
    timed("tag = t50 AND zone = lab", lambda: d.query(parse_query("tag = t50 AND zone = lab")))
    # OR of two selective conjuncts (union of buckets)
    timed("tag = t50 OR zone = lab", lambda: d.query(parse_query("tag = t50 OR zone = lab")))
    # OR of many selective conjuncts
    timed("tag = t50 OR tag = t51 OR tag = t52",
          lambda: d.query(parse_query("tag = t50 OR tag = t51 OR tag = t52")))
    # NOT of a selective conjunct (full scan expected)
    timed("NOT (tag = t50)", lambda: d.query(parse_query("NOT (tag = t50)")))
    # broad equality (bucket ~ table size -> full scan)
    timed("zone = lab (broad)", lambda: d.query(parse_query("zone = lab")))
    # ordering predicate
    timed("seq < 1000", lambda: d.query(parse_query("seq < 1000")))
    # AND with a NOT (NOT forces full scan, AND narrows)
    timed("tag = t50 AND NOT (zone = travel)",
          lambda: d.query(parse_query("tag = t50 AND NOT (zone = travel)")))
    # inequality on indexed-ish attr
    timed("seq > 49000", lambda: d.query(parse_query("seq > 49000")))


if __name__ == "__main__":
    d = build()
    run(d)
