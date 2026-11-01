"""Performance benchmark: client latency, node concurrency, directory, query."""
import sys, time, asyncio, tempfile, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from acnl_core.directory import InMemoryDirectory, ShelveDirectory
from acnl_core.addresses import Reference
from acnl_core.attributes import parse_query
from acnl_engine.config import NodeConfig
from acnl_engine.client import Client
from acnl_engine.node import Node

def pct(ts, p):
    ts = sorted(ts)
    if not ts:
        return float("nan")
    return ts[min(len(ts) - 1, int(len(ts) * p))]

def bench_client_latency():
    node = Node(NodeConfig(host="127.0.0.1", port=0, name="b", zone="lab"), InMemoryDirectory())
    node.start()
    port = node._listener.getsockname()[1]
    c = Client("acnl:lab/b/a")
    c.connect("127.0.0.1", port)
    ts = []
    for _ in range(200):
        t0 = time.perf_counter()
        c.ping()
        ts.append((time.perf_counter() - t0) * 1000)
    t0 = time.perf_counter()
    n = 300
    for _ in range(n):
        c.ping()
    tput = n / (time.perf_counter() - t0)
    c.close()
    node.stop()
    print(f"client ping: p50={pct(ts,0.5):7.2f}ms p90={pct(ts,0.9):7.2f}ms  seq throughput={tput:7.1f} ops/s")

def bench_concurrent(n, use_async=False):
    if use_async:
        from acnl_engine.async_node import AsyncNode
        from acnl_core.messages import make_envelope, new_id
        from acnl_core.codec import encode_frame, FrameDecoder

        async def one_ping(tag, port):
            sender = f"acnl:lab/bc{tag}/c{tag}"
            r, w = await asyncio.open_connection("127.0.0.1", port)
            env = make_envelope(sender=sender, recipient="acnl:localhost/localhost/broker")
            w.write(encode_frame({"kind": "hello", "envelope": env.to_value(), "body": {"capabilities": [], "host": "127.0.0.1", "port": 0}}))
            await w.drain()
            dec = FrameDecoder()
            broker_urn = None
            while broker_urn is None:
                data = await asyncio.wait_for(r.read(65536), timeout=3)
                if not data:
                    raise RuntimeError("closed")
                for v, _ in dec.feed(data):
                    if v["kind"] == "hello":
                        broker_urn = v["body"].get("urn")
            rid = new_id()
            env = make_envelope(sender=sender, recipient=broker_urn, request_id=rid, message_id=new_id())
            w.write(encode_frame({"kind": "request", "envelope": env.to_value(), "body": {"op": "ping"}}))
            await w.drain()
            t0 = time.perf_counter()
            while True:
                data = await asyncio.wait_for(r.read(65536), timeout=3)
                if not data:
                    raise RuntimeError("closed2")
                for v, _ in dec.feed(data):
                    if v["kind"] in ("reply", "error") and v["envelope"]["request_id"] == rid:
                        w.close()
                        return (time.perf_counter() - t0) * 1000

        async def run():
            node = AsyncNode(NodeConfig(host="127.0.0.1", port=0, name="b", zone="lab", queue_depth=256), InMemoryDirectory())
            await node.start()
            p = node.port
            ts = await asyncio.gather(*[one_ping(i, p) for i in range(n)], return_exceptions=True)
            await node.stop()
            return ts

        ts = asyncio.run(run())
    else:
        node = Node(NodeConfig(host="127.0.0.1", port=0, name="b", zone="lab", queue_depth=256), InMemoryDirectory())
        node.start()
        port = node._listener.getsockname()[1]

        def one(tag):
            c = Client(f"acnl:lab/bc{tag}/c{tag}")
            c.connect("127.0.0.1", port)
            t0 = time.perf_counter()
            c.ping()
            dt = (time.perf_counter() - t0) * 1000
            c.close()
            return dt

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
            ts = list(ex.map(one, range(n)))
        node.stop()
    ok = sorted(t for t in ts if isinstance(t, (int, float)))
    bad = [t for t in ts if not isinstance(t, (int, float))]
    kind = "async" if use_async else "thread"
    print(f"{kind} N={n:4d}: ok={len(ok):4d} bad={len(bad):3d} p50={pct(ok,0.5):7.2f}ms p90={pct(ok,0.9):7.2f}ms max={pct(ok,1.0):7.2f}ms")
    if bad:
        print("   first bad:", repr(bad[0])[:90])

def bench_directory():
    tmp = tempfile.mkdtemp()
    ref = Reference(urn="acnl:lab/x/s", capabilities=["a"], attributes={"k": 1})
    d = ShelveDirectory(os.path.join(tmp, "d.shelf"))
    t0 = time.perf_counter()
    for i in range(2000):
        d.advertise(f"acnl:lab/x/s{i}", Reference(urn=f"acnl:lab/x/s{i}", capabilities=["a"], attributes={"i": i}), ttl=60)
    t_adv = time.perf_counter() - t0
    t0 = time.perf_counter()
    for _ in range(200):
        d.resolve("acnl:lab/x/s10")
    t_res = (time.perf_counter() - t0) / 200 * 1000
    d.close()
    print(f"shelve: 2000 advertises={t_adv*1000:8.1f}ms ({t_adv/2000*1e6:6.1f}us/each)  resolve@2000={t_res:6.2f}ms")

def bench_dedupe():
    node = Node(NodeConfig(host="127.0.0.1", port=0, name="b", zone="lab"), InMemoryDirectory())
    node.start()
    port = node._listener.getsockname()[1]
    c = Client("acnl:lab/b/d")
    c.connect("127.0.0.1", port)
    for _ in range(5000):
        c.ping()
    print(f"dedupe after 5000 requests: deque={len(node._seen)} set={len(node._seen_set)}")
    c.close()
    node.stop()

def bench_query():
    d = InMemoryDirectory()
    for i in range(50000):
        d.bind(f"acnl:lab/x/s{i}", Reference(urn=f"acnl:lab/x/s{i}", capabilities=["svc"], attributes={"shard": i % 32, "load": i % 100}))
    ast = parse_query('shard = 7')
    t0 = time.perf_counter()
    for _ in range(20):
        hits = d.query(ast)
    t_eq = (time.perf_counter() - t0) / 20 * 1000
    ast2 = parse_query('load CONTAINS 42 OR shard = 31')
    t0 = time.perf_counter()
    for _ in range(20):
        hits2 = d.query(ast2)
    t_or = (time.perf_counter() - t0) / 20 * 1000
    print(f"query@50k: 'shard = 7'={t_eq:7.3f}ms ({len(hits)} hits)  OR+CONTAINS={t_or:7.3f}ms ({len(hits2)} hits)")

if __name__ == "__main__":
    bench_client_latency()
    bench_concurrent(32)
    bench_concurrent(128)
    bench_concurrent(32, use_async=True)
    bench_concurrent(128, use_async=True)
    bench_directory()
    bench_dedupe()
    bench_query()
