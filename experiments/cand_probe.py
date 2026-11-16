"""Probe: index-based candidate generation for OR queries.

The current _cand_node returns None for any Or containing a non-bucket child
(Contains, ordering op), forcing a full 50k scan. But a query like
'load CONTAINS 42 OR shard = 31' has a 'shard = 31' child that IS a bucket.
If we take the union of all bucket children of an Or (ignoring the non-bucket
children), we get a guaranteed SUPERSET of the answer, and evaluate the full
query only over that small set.
"""
import time
from acnl_core.attributes import parse_query, _ed
from acnl_core.addresses import Reference
from acnl_core.directory import InMemoryDirectory

def build(n=50000):
    d = InMemoryDirectory()
    for i in range(n):
        d.bind(f"acnl:lab/x/s{i}",
               Reference(urn=f"acnl:lab/x/s{i}", capabilities=["svc"],
                         attributes={"shard": i % 32, "load": i % 100}))
    return d

def cand_node(d, ast):
    """New candidate logic: Or returns union of bucket children only."""
    from acnl_core.attributes import And, Or, Cmp, Contains, Not
    if isinstance(ast, And):
        acc = None
        for c in ast.parts:
            if isinstance(c, Not):
                continue
            cs = cand_node(d, c)
            if cs is None:
                return None
            acc = cs if acc is None else (acc & cs)
            if acc is not None and not acc:
                return frozenset()
        return acc
    if isinstance(ast, Or):
        acc = None
        for b in ast.parts:
            cs = cand_node(d, b)
            # skip None children (Contains, ordering ops) — they are filters,
            # not narrowing constraints; the union of bucket children is a
            # superset of the Or's answer.
            if cs is None:
                continue
            acc = cs if acc is None else (acc | cs)
        return acc if acc is not None else frozenset()
    if isinstance(ast, Cmp):
        if ast.op != "=":
            return None
        if ast.attr == "urn":
            return frozenset({ast.value}) if isinstance(ast.value, str) else frozenset()
        buckets = d._index.get(ast.attr)
        if buckets is None:
            return None
        got = buckets.get(d._index_key(ast.value))
        return frozenset(got) if got is not None else frozenset()
    return None

if __name__ == "__main__":
    d = build()
    for q in ["load CONTAINS 42 OR shard = 31", "shard = 7",
              "load > 50 AND shard <= 10", "NOT shard = 0",
              "shard = 5 OR shard = 6 OR shard = 7"]:
        ast = parse_query(q)
        cands = cand_node(d, ast)
        # verify correctness: compiled/eval over candidates == eval over all
        n = 20
        t0 = time.perf_counter()
        for _ in range(n):
            old = [e.reference for e in d._entries.values() if _ed(ast, e._doc)]
        t_old = (time.perf_counter() - t0) / n * 1000
        # new: eval full query over candidates only
        t0 = time.perf_counter()
        for _ in range(n):
            new = [e.reference for u in cands
                   if (e := d._entries.get(u)) is not None and _ed(ast, e._doc)]
        t_new = (time.perf_counter() - t0) / n * 1000
        ok = sorted(r.urn for r in old) == sorted(r.urn for r in new)
        print(f"{q!r:40} old={t_old:8.3f}ms new={t_new:8.3f}ms "
              f"cands={len(cands):5d} hits={len(old):5d} correct={ok}")
