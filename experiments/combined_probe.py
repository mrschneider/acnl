"""Combined: index candidates + compiled evaluator, over the full sweep."""
import time
from acnl_core.attributes import parse_query, _ed, Cmp, Contains, Or, And, Not
from acnl_core.addresses import Reference
from acnl_core.directory import InMemoryDirectory

# ---- compiled evaluator -------------------------------------------------
def _cgetattr(attr):
    if attr == "urn":
        def get(doc): return doc.get("urn")
    elif attr == "name":
        def get(doc):
            u = doc.get("urn"); return u.split("/")[-1] if u else None
    elif attr == "version":
        def get(doc): return doc.get("protocol_version")
    elif attr == "zone":
        def get(doc):
            u = doc.get("urn")
            return u.split("/")[0].split(":", 1)[-1] if u else None
    else:
        def get(doc):
            a = doc.get("attributes")
            if a is not None and attr in a: return a.get(attr)
            return None
    return get

def _compile_cmp(node):
    get = _cgetattr(node.attr); op, val = node.op, node.value
    if op == "=":
        def f(doc, get=get): return get(doc) == val
    elif op == "!=":
        def f(doc, get=get): return get(doc) != val
    elif op == "<":
        def f(doc, get=get):
            g = get(doc); return g is not None and val is not None and g < val
    elif op == ">":
        def f(doc, get=get):
            g = get(doc); return g is not None and val is not None and g > val
    elif op == "<=":
        def f(doc, get=get):
            g = get(doc); return g is not None and val is not None and g <= val
    else:
        def f(doc, get=get):
            g = get(doc); return g is not None and val is not None and g >= val
    return f

def _compile_contains(node):
    get = _cgetattr(node.attr); val = node.value
    def f(doc, get=get):
        hay = get(doc)
        if isinstance(hay, list): return val in hay
        if isinstance(hay, str): return str(val) in hay
        return False
    return f

def _compile(node):
    if isinstance(node, Cmp): return _compile_cmp(node)
    if isinstance(node, Contains): return _compile_contains(node)
    if isinstance(node, And):
        fs = [_compile(p) for p in node.parts]
        def f(doc, fs=fs):
            for g in fs:
                if not g(doc): return False
            return True
        return f
    if isinstance(node, Or):
        fs = [_compile(p) for p in node.parts]
        def f(doc, fs=fs):
            for g in fs:
                if g(doc): return True
            return False
        return f
    if isinstance(node, Not):
        g = _compile(node.part)
        def f(doc, g=g): return not g(doc)
        return f
    raise ValueError(node)

# ---- index candidates ---------------------------------------------------
def cand_node(d, ast):
    if isinstance(ast, And):
        acc = None
        for c in ast.parts:
            if isinstance(c, Not): continue
            cs = cand_node(d, c)
            if cs is None: return None
            acc = cs if acc is None else (acc & cs)
            if acc is not None and not acc: return frozenset()
        return acc
    if isinstance(ast, Or):
        acc = None
        for b in ast.parts:
            cs = cand_node(d, b)
            if cs is None: continue
            acc = cs if acc is None else (acc | cs)
        return acc if acc is not None else frozenset()
    if isinstance(ast, Cmp):
        if ast.op != "=": return None
        if ast.attr == "urn":
            return frozenset({ast.value}) if isinstance(ast.value, str) else frozenset()
        buckets = d._index.get(ast.attr)
        if buckets is None: return None
        got = buckets.get(d._index_key(ast.value))
        return frozenset(got) if got is not None else frozenset()
    return None

def build(n=50000):
    d = InMemoryDirectory()
    for i in range(n):
        d.bind(f"acnl:lab/x/s{i}",
               Reference(urn=f"acnl:lab/x/s{i}", capabilities=["svc"],
                         attributes={"shard": i % 32, "load": i % 100}))
    return d

queries = [
    "load CONTAINS 42 OR shard = 31", "shard = 7",
    "load > 50 AND shard <= 10", "NOT shard = 0",
    "shard = 5 OR shard = 6 OR shard = 7",
    "shard = 1 AND load = 2",
    "load CONTAINS 1 OR load CONTAINS 2 OR shard = 3",
    "shard = 0 OR shard = 31", "load = 100 OR load = 0",
    "shard != 5 OR load CONTAINS 5", "name = s100",
    "(shard = 1 OR shard = 2) AND load = 3", "shard = 99999",
    "load CONTAINS 999",
]
d = build()
allok = True
for q in queries:
    ast = parse_query(q)
    cf = _compile(ast)
    cands = cand_node(d, ast)
    truth = {e.urn for e in d._entries.values() if _ed(ast, e._doc)}
    got = {e.urn for u in cands
           if (e := d._entries.get(u)) is not None and cf(doc=e._doc)}
    ok = truth == got
    allok = allok and ok
    nn = 20
    t0 = time.perf_counter()
    for _ in range(nn):
        rr = [e.reference for u in cands
              if (e := d._entries.get(u)) is not None and _ed(ast, e._doc)]
    t_old = (time.perf_counter() - t0) / nn * 1000
    t0 = time.perf_counter()
    for _ in range(nn):
        cf = _compile(ast)
        rr = [e.reference for u in cands
              if (e := d._entries.get(u)) is not None and cf(doc=e._doc)]
    t_new = (time.perf_counter() - t0) / nn * 1000
    print(f"{q!r:40} old={t_old:8.3f} new={t_new:8.3f} cands={len(cands):5d} ok={ok}")
print("ALL CORRECT:", allok)
