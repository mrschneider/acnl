"""Parity tests for the asyncio node: same wire, same refusals.

Async tests run under ``asyncio.run`` (no pytest-asyncio dependency — the
repo is stdlib-first). The threaded ``Client`` proves interop from inside
the loop via ``asyncio.to_thread``; the threaded ``Node`` proves
federation across the two implementations.
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.codec import FrameDecoder, encode_frame
from acnl_core.directory import InMemoryDirectory
from acnl_core.messages import WireMessage, make_envelope, new_id
from acnl_core.security import SecurityPolicy
from acnl_engine.async_node import AsyncNode
from acnl_engine.client import Client, ClientError
from acnl_engine.config import NodeConfig
from acnl_engine.node import Node

PSK = "async-psk-not-a-real-secret"


def _ref(urn, **kw):
    from acnl_core.addresses import Reference

    return Reference(urn=urn, **kw).to_dict()


def _async_cfg(name="nodea", **kw):
    return NodeConfig(host="127.0.0.1", port=0, name=name, zone="lab", **kw)


class AClient:
    """A minimal asyncio client for driving the async node in tests."""

    def __init__(self, urn: str, security=None):
        self.urn = urn
        self.sec = security
        self.reader = None
        self.writer = None
        self.decoder = FrameDecoder()
        self.broker = None
        self.replies = {}
        self.notifications = []
        self.requests = []  # inbound requests the node forwards to us
        self.rejected = 0
        self._pump = None

    async def connect(self, host: str, port: int, timeout: float = 5.0) -> None:
        self.reader, self.writer = await asyncio.open_connection(host, port)
        self._pump = asyncio.create_task(self._pump_loop())
        await self._send(
            WireMessage(
                kind="hello",
                envelope=make_envelope(sender=self.urn, recipient="acnl:localhost/localhost/broker"),
                body={"capabilities": [], "host": host, "port": port},
            )
        )
        deadline = time.time() + timeout
        while self.broker is None and time.time() < deadline:
            await asyncio.sleep(0.01)
        if self.broker is None:
            raise RuntimeError("no hello reply from async node")

    async def _send(self, msg: WireMessage) -> None:
        value = msg.to_value()
        if self.sec is not None:
            value = self.sec.protect(value)
        self.writer.write(encode_frame(value))
        await self.writer.drain()

    async def _pump_loop(self) -> None:
        try:
            while True:
                data = await self.reader.read(65536)
                if not data:
                    break
                for value, _ in self.decoder.feed(data):
                    if self.sec is not None:
                        try:
                            value = self.sec.unprotect(value)
                        except Exception:
                            self.rejected += 1
                            continue
                    msg = WireMessage.from_value(value)
                    if msg.kind == "hello":
                        self.broker = (msg.body or {}).get("urn")
                    elif msg.kind in ("reply", "error"):
                        self.replies[msg.envelope.request_id] = msg
                    elif msg.kind == "notify":
                        self.notifications.append(msg)
                    elif msg.kind == "request":
                        self.requests.append(msg)
                        await self._send(
                            WireMessage(
                                kind="reply",
                                envelope=make_envelope(
                                    sender=self.urn,
                                    recipient=msg.envelope.sender,
                                    request_id=msg.envelope.request_id,
                                    message_id=new_id(),
                                ),
                                body={"op": msg.body["op"], "handled": True},
                            )
                        )
        except (ConnectionError, asyncio.CancelledError):
            pass

    async def call(self, op: str, payload: dict, recipient=None, timeout: float = 5.0):
        rid = new_id()
        await self._send(
            WireMessage(
                kind="request",
                envelope=make_envelope(
                    sender=self.urn,
                    recipient=recipient or self.broker,
                    request_id=rid,
                    message_id=new_id(),
                ),
                body={"op": op, **payload},
            )
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            if rid in self.replies:
                msg = self.replies.pop(rid)
                if msg.kind == "error":
                    raise ClientError(msg.body)
                return msg.body
            await asyncio.sleep(0.01)
        raise TimeoutError(f"no reply to {op} within {timeout}s")

    async def close(self) -> None:
        try:
            await self._send(
                WireMessage(
                    kind="close",
                    envelope=make_envelope(sender=self.urn, recipient=self.broker or "acnl:localhost/localhost/broker"),
                )
            )
        except Exception:
            pass
        if self._pump:
            self._pump.cancel()
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


def _run(coro):
    return asyncio.run(coro)


# --- basics ---------------------------------------------------------------

def test_async_hello_ping():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        try:
            c = AClient("acnl:lab/nodea/worker")
            await c.connect("127.0.0.1", node.port)
            body = await c.call("ping", {})
            assert body["value"] == "pong"
            await c.close()
        finally:
            await node.stop()

    _run(scenario())


def test_async_advertise_resolve_query():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        try:
            c = AClient("acnl:lab/nodea/worker")
            await c.connect("127.0.0.1", node.port)
            out = await c.call("advertise", {"urn": "acnl:lab/nodea/worker",
                                             "reference": _ref("acnl:lab/nodea/worker",
                                                               capabilities=["math"]),
                                             "ttl": 60.0})
            assert out["urn"] == "acnl:lab/nodea/worker"
            got = await c.call("resolve", {"urn": "acnl:lab/nodea/worker"})
            assert got["reference"]["capabilities"] == ["math"]
            rows = await c.call("query", {"filter": 'capabilities CONTAINS "math"'})
            assert [r["urn"] for r in rows["references"]] == ["acnl:lab/nodea/worker"]
            await c.close()
        finally:
            await node.stop()

    _run(scenario())


def test_async_duplicate_bind_is_typed_refusal():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        try:
            c = AClient("acnl:lab/nodea/worker")
            await c.connect("127.0.0.1", node.port)
            ref = {"urn": "acnl:lab/nodea/x", "reference": _ref("acnl:lab/nodea/x")}
            await c.call("bind", dict(ref))
            with pytest.raises(ClientError) as exc:
                await c.call("bind", dict(ref))
            assert exc.value.body["code"] == "duplicate_name"
            assert exc.value.body["retryable"] is False
            await c.close()
        finally:
            await node.stop()

    _run(scenario())


def test_async_unknown_verb_names_the_word():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        try:
            c = AClient("acnl:lab/nodea/worker")
            await c.connect("127.0.0.1", node.port)
            # the closed vocabulary refuses at the single entrance
            rid = new_id()
            c.writer.write(encode_frame(
                WireMessage(
                    kind="request",
                    envelope=make_envelope(sender=c.urn, recipient=node.cfg.self_urn,
                                           request_id=rid, message_id=new_id()),
                    body={"op": "explode"},
                ).to_value()))
            await c.writer.drain()
            deadline = time.time() + 5
            while rid not in c.replies and time.time() < deadline:
                await asyncio.sleep(0.01)
            msg = c.replies.get(rid)
            assert msg is not None and msg.kind == "error"
            assert msg.body["code"] in ("unknown_verb", "malformed_frame")
            if msg.body["code"] == "unknown_verb":
                assert msg.body["details"]["sent"] == "explode"
            await c.close()
        finally:
            await node.stop()

    _run(scenario())


def test_async_forward_and_reply_route_by_recipient():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        try:
            a = AClient("acnl:lab/nodea/agent1")
            b = AClient("acnl:lab/nodea/agent2")
            await a.connect("127.0.0.1", node.port)
            await b.connect("127.0.0.1", node.port)
            # wait until the node learned both senders (implicit registration)
            deadline = time.time() + 3
            while time.time() < deadline and len(node.conns_by_sender()) < 2:
                await asyncio.sleep(0.01)
            out = await a.call("send", {"payload": "hi"},
                               recipient="acnl:lab/nodea/agent2")
            # the reply came back through the node: recipient-routed
            assert out.get("op") == "send" or out.get("handled") is True
            await asyncio.sleep(0.05)
            assert len(b.requests) == 1
            assert b.requests[0].body["payload"] == "hi"
            await a.close()
            await b.close()
        finally:
            await node.stop()

    _run(scenario())


# --- interop: threaded client x async node --------------------------------

def test_threaded_client_against_async_node():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        c = Client("acnl:lab/nodea/worker")
        try:
            await asyncio.to_thread(c.connect, "127.0.0.1", node.port)
            body = await asyncio.to_thread(c.ping)
            assert body["value"] == "pong"
            await asyncio.to_thread(c.advertise, "acnl:lab/nodea/worker",
                                    _ref("acnl:lab/nodea/worker", capabilities=["math"]))
            got = await asyncio.to_thread(c.resolve, "acnl:lab/nodea/worker")
            assert got["reference"]["urn"] == "acnl:lab/nodea/worker"
        finally:
            await asyncio.to_thread(c.close)
            await node.stop()

    _run(scenario())


def test_async_node_federates_with_threaded_node():
    async def scenario():
        # the threaded node owns its threads: safe inside the loop
        tnode = Node(NodeConfig(host="127.0.0.1", port=0, name="nodet", zone="lab"),
                     InMemoryDirectory())
        tnode.start()
        anode = AsyncNode(_async_cfg(name="nodea"))
        await anode.start()
        try:
            await anode.peer("127.0.0.1", tnode._listener.getsockname()[1])
            deadline = time.time() + 5
            converged = False
            while time.time() < deadline:
                if (tnode.cfg.self_urn in anode.remote_directory
                        and anode.cfg.self_urn in tnode.remote_directory):
                    converged = True
                    break
                await asyncio.sleep(0.02)
            assert converged, "async<->threaded peer link must converge"

            # a threaded client advertises on the threaded node...
            c = Client("acnl:lab/nodet/math")
            await asyncio.to_thread(c.connect, "127.0.0.1", tnode._listener.getsockname()[1])
            await asyncio.to_thread(c.advertise, "acnl:lab/nodet/math",
                                    _ref("acnl:lab/nodet/math", capabilities=["math"]))
            # ...and the async node answers a federated resolve
            a = AClient("acnl:lab/nodea/worker")
            await a.connect("127.0.0.1", anode.port)
            got = await a.call("resolve", {"urn": "acnl:lab/nodet/math"})
            assert got["origin"] == "federated"
            assert got["reference"]["capabilities"] == ["math"]
            await a.close()
            await asyncio.to_thread(c.close)
        finally:
            await anode.stop()
            tnode.stop()

    _run(scenario())


# --- security on the async node -------------------------------------------

def test_async_node_refuses_unsigned_frames():
    pol = SecurityPolicy.from_secret(PSK)

    async def scenario():
        node = AsyncNode(_async_cfg(security=pol))
        await node.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", node.port)
            writer.write(encode_frame(
                WireMessage(
                    kind="hello",
                    envelope=make_envelope(sender="acnl:lab/rogue/agent",
                                           recipient=node.cfg.self_urn,
                                           request_id=new_id(), message_id=new_id()),
                    body={"capabilities": []},
                ).to_value()))
            await writer.drain()
            decoder = FrameDecoder()
            err = None
            deadline = time.time() + 5
            while time.time() < deadline and err is None:
                data = await reader.read(65536)
                if not data:
                    break
                for value, _ in decoder.feed(data):
                    value = pol.unprotect(value)
                    if value.get("kind") == "error":
                        err = value
                        break
            assert err is not None, "signed node must refuse, not stay silent"
            assert err["body"]["code"] == "auth_failed"
            writer.close()
        finally:
            await node.stop()

    _run(scenario())


def test_async_node_signed_encrypted_end_to_end():
    pytest.importorskip("cryptography")
    pol_tx = SecurityPolicy.from_secret(PSK, encrypt=True, require_encryption=True)

    async def scenario():
        node = AsyncNode(_async_cfg(security=SecurityPolicy.from_secret(
            PSK, encrypt=True, require_encryption=True)))
        await node.start()
        c = Client("acnl:lab/nodea/worker", security=pol_tx)
        try:
            await asyncio.to_thread(c.connect, "127.0.0.1", node.port)
            body = await asyncio.to_thread(c.ping)
            assert body["value"] == "pong"
        finally:
            await asyncio.to_thread(c.close)
            await node.stop()

    _run(scenario())


# --- lifecycle -------------------------------------------------------------

def test_async_stop_releases_the_listener():
    async def scenario():
        node = AsyncNode(_async_cfg())
        await node.start()
        port = node.port
        c = AClient("acnl:lab/nodea/worker")
        await c.connect("127.0.0.1", port)
        await c.call("ping", {})
        await c.close()
        await node.stop()
        with pytest.raises((ConnectionRefusedError, OSError)):
            await asyncio.open_connection("127.0.0.1", port)

    _run(scenario())
