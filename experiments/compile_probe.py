"""Probe: compiled query evaluator vs current _ed, with a randomized
correctness harness that asserts identical results over many queries/rows."""
import time, random
from acnl_core.attributes import (
    parse_query, _ed, Cmp, Contains, Or, And, Not,
)
from acnl_core.addresses import Reference
from acnl_core.directory import InMemoryDirectory

# ---- compiled evaluator under test -------------------------------------

def _compile_cmp(node):
    attr, op, val = node.attr, node.op, node.value
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
    else:  # >=
        def f(doc, get=get):
            g = get(doc); return g is not None and val is not None and g >= val
    return f

def _compile_contains(node):
    attr = node.attr
    val = node.value
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
    def f(doc, get=get):
        hay = get(doc)
        if isinstance(hay, list): return val in hay
        if isinstance(hay, str): return str(val) in hay
        return False
    return f

def _compile(node):
    if isinstance(node, Cmp):
        return _compile_cmp(node)
    if isinstance(node, Contains):
        return _compile_contains(node)
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


def build(n=50000):
    d = InMemoryDirectory()
    shards = list(range(32))
    loads = list(range(100))
    for i in range(n):
        d.bind(f"acnl:lab/x/s{i}",
               Reference(urn=f"acnl:lab/x/s{i}", capabilities=["svc"],
                         attributes={"shard": shards[i % 32], "load": loads[i % 100]}))
    return d

def correctness(d, ast, n_trials=4000):
    """Compare compiled vs _ed over random rows."""
    docs = [e._doc for e in d._entries.values()]
    cf = _compile(ast)
    mism = 0
    for _ in range(n_trials):
        doc = random.choice(docs)
        a = _ed(ast, doc)
        b = cf(doc)
        if a != b:
            mism += 1
            if mism <= 5:
                print("MISMATCH", _ed(ast, doc), cf(doc), doc)
    return mism

if __name__ == "__main__":
    d = build()
    for q in ["load CONTAINS 42 OR shard = 31", "shard = 7",
              "load > 50 AND shard <= 10", "NOT shard = 0",
              "name = s100", "zone = lab", "shard != 5"]:
        ast = parse_query(q)
        # correctness over ALL rows
        docs = [e._doc for e in d._entries.values()]
        mism = sum(1 for doc in docs if _ed(ast, doc) != _compile(ast)(doc))
        # timing over full table
        n = 20
        t0 = time.perf_counter()
        for _ in range(n):
            for doc in docs: _ed(ast, doc)
        t_old = (time.perf_counter() - t0) / n * 1000
        t0 = time.perf_counter()
        for _ in range(n):
            cf = _compile(ast)
            for doc in docs: cf(doc)
        t_new = (time.perf_counter() - t0) / n * 1000
        print(f"{q!r:40} old={t_old:8.3f}ms new={t_new:8.3f}ms  all-row-mismatch={mism}")
