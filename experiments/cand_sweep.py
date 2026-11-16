"""Full correctness sweep for the new index-based Or candidate logic."""
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

queries = [
    "load CONTAINS 42 OR shard = 31",
    "shard = 7",
    "load > 50 AND shard <= 10",
    "NOT shard = 0",
    "shard = 5 OR shard = 6 OR shard = 7",
    "shard = 1 AND load = 2",
    "load CONTAINS 1 OR load CONTAINS 2 OR shard = 3",
    "shard = 0 OR shard = 31",
    "load = 100 OR load = 0",
    "shard != 5 OR load CONTAINS 5",
    "name = s100",
    "(shard = 1 OR shard = 2) AND load = 3",
    "shard = 99999",  # empty bucket
    "load CONTAINS 999",  # no bucket -> full scan
]

d = build()
allok = True
for q in queries:
    ast = parse_query(q)
    cands = cand_node(d, ast)
    # ground truth over all rows
    truth = set(r.urn for e in d._entries.values() if _ed(ast, e._doc))
    # candidate-based
    got = set(r.urn for u in cands
              if (e := d._entries.get(u)) is not None and _ed(ast, e._doc))
    ok = truth == got
    allok = allok and ok
    print(f"{q!r:45} cands={len(cands):5d} hits={len(truth):5d} correct={ok}")
print("ALL CORRECT:", allok)
