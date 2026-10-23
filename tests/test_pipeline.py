"""Outbox -> SQS -> worker, dead-lettering, and the HTTP/WebSocket API."""
import asyncio
import json

import httpx
import websockets

from app import dispatch, outbox, worker
from app.config import get_settings

from .helpers import body, db_order, event_types, mk_courier, mk_order


# ---------------------------------------------------------------- outbox + SQS + worker
async def test_order_flows_through_outbox_sqs_and_worker(queue):
    c = await mk_courier()
    o = await mk_order()
    assert await outbox.relay_once(queue) >= 1
    assert await outbox.relay_once(queue) == 0  # nothing left to publish
    msgs = await queue.receive(10, wait=1)
    assert any(m.body.get("order_id") == o["id"] for m in msgs)
    for m in msgs:
        await worker.process(queue, m)
    assert (await db_order(o["id"])).courier_id == c["id"]
    assert sum((await queue.depth()).values()) == 0  # processed messages were deleted


async def test_unassignable_message_is_retried_with_backoff_not_deleted(queue):
    o = await mk_order()  # no couriers
    await outbox.relay_once(queue)
    (m,) = [m for m in await queue.receive(10, wait=1) if m.body.get("order_id") == o["id"]]
    await worker.process(queue, m)
    depth = await queue.depth()
    assert depth["in_flight"] + depth["delayed"] + depth["visible"] == 1  # still there, invisible until the backoff ends
    assert "order.assignment_retry" in await event_types(o["id"])


async def test_poison_message_goes_straight_to_the_dlq(queue):
    await queue.client.send_message(QueueUrl=queue.url, MessageBody=json.dumps({"type": "order.assign", "order_id": {"x": 1}}))
    (m,) = await queue.receive(1, wait=1)
    await worker.process(queue, m)
    assert sum((await queue.depth()).values()) == 0
    (dead,) = await queue.receive(1, wait=1, dlq=True)
    assert dead.body["order_id"] == {"x": 1}


async def test_dead_lettered_order_is_failed_not_left_hanging():
    o = await mk_order()
    await dispatch.reap_dead_letter({"type": "order.assign", "order_id": o["id"]})
    order = await db_order(o["id"])
    assert order.status == "FAILED" and order.failure_reason == "dead_letter"
    await dispatch.reap_dead_letter({"type": "order.assign", "order_id": o["id"]})  # idempotent
    assert (await event_types(o["id"])).count("order.failed") == 1


async def test_sqs_redelivers_when_a_handler_crashes(queue, monkeypatch):
    """The transaction rolls back, nothing is half-applied, and the message is retried."""
    await mk_courier()
    o = await mk_order()
    await outbox.relay_once(queue)
    monkeypatch.setattr(get_settings(), "chaos_worker_fail_rate", 1.0)
    (m,) = [m for m in await queue.receive(10, wait=1) if m.body.get("order_id") == o["id"]]
    await worker.process(queue, m)  # fails inside the handler
    order = await db_order(o["id"])
    assert order.status == "CREATED" and order.assignment_attempts == 0  # rolled back cleanly
    monkeypatch.setattr(get_settings(), "chaos_worker_fail_rate", 0.0)
    await asyncio.sleep(2.2)  # backoff (>=1s) then the message reappears
    again = [m for m in await queue.receive(10, wait=2) if m.body.get("order_id") == o["id"]]
    assert again and again[0].receive_count >= 2
    await worker.process(queue, again[0])
    assert (await db_order(o["id"])).status == "ASSIGNED"


# ---------------------------------------------------------------- HTTP API
async def test_http_idempotency_headers_and_validation(server):
    async with httpx.AsyncClient(base_url=server) as http:
        h = {"Idempotency-Key": "http-1"}
        r1 = await http.post("/api/orders", json=body(), headers=h)
        r2 = await http.post("/api/orders", json=body(), headers=h)
        assert (r1.status_code, r2.status_code) == (201, 200)
        assert r2.headers["idempotent-replay"] == "true" and r1.json()["id"] == r2.json()["id"]
        r3 = await http.post("/api/orders", json=body(consumer="other"), headers=h)
        assert r3.status_code == 422 and r3.json()["error"] == "idempotency_key_reuse"
        bad = body(); bad["pickup"]["lat"] = 123
        assert (await http.post("/api/orders", json=bad)).status_code == 422
        assert (await http.get("/api/orders/ord_doesnotexist")).status_code == 404


async def test_http_full_flow_and_stats(server):
    async with httpx.AsyncClient(base_url=server) as http:
        c = (await http.post("/api/couriers", json={"name": "Zed", "lat": 37.775, "lng": -122.42, "online": True})).json()
        o = (await http.post("/api/orders", json=body())).json()
        await dispatch.assign_order(o["id"])
        j = {"courier_id": c["id"]}
        assert (await http.post(f"/api/orders/{o['id']}/pickup", json=j)).json()["outcome"] == "applied"
        assert (await http.post(f"/api/orders/{o['id']}/pickup", json=j)).json()["outcome"] == "duplicate"
        assert (await http.post(f"/api/orders/{o['id']}/deliver", json=j)).json()["order"]["status"] == "DELIVERED"
        assert (await http.post(f"/api/orders/{o['id']}/cancel", json={})).status_code == 409
        s = (await http.get("/api/stats/summary")).json()
        assert s["orders"]["by_status"]["DELIVERED"] == 1 and s["redis_healthy"] is True
        assert [e["type"] for e in (await http.get(f"/api/events?order_id={o['id']}")).json()][-1] == "order.delivered"
        m = (await http.get("/metrics")).text
        assert "dispatchflow_http_requests_total" in m and "dispatchflow_duplicates_absorbed_total" in m
        assert (await http.get("/ready")).json()["status"] == "ok"
        assert "DispatchFlow" in (await http.get("/")).text


async def _next(ws, want, timeout=4):
    async with asyncio.timeout(timeout):
        while True:
            m = json.loads(await ws.recv())
            if m.get("type") == want:
                return m


async def test_websocket_streams_events_live_and_filters_by_merchant(server):
    ws_url = server.replace("http", "ws") + "/ws"
    async with httpx.AsyncClient(base_url=server) as http:
        async with websockets.connect(ws_url + "?role=ops") as ops, websockets.connect(ws_url + "?role=merchant&id=m-A") as merch:
            await ops.recv(); await merch.recv()  # hello
            await http.post("/api/orders", json=body(merchant="m-B"))
            a = (await http.post("/api/orders", json=body(merchant="m-A"))).json()
            seen_ops = [(await _next(ops, "order.created"))["merchant_id"], (await _next(ops, "order.created"))["merchant_id"]]
            assert sorted(seen_ops) == ["m-A", "m-B"]
            first = await _next(merch, "order.created")
            assert first["order_id"] == a["id"]  # merchant A never sees merchant B's order


async def test_websocket_reconnect_replays_what_was_missed(server):
    ws_url = server.replace("http", "ws") + "/ws?role=ops"
    async with httpx.AsyncClient(base_url=server) as http:
        async with websockets.connect(ws_url) as ws:
            await ws.recv()
            await http.post("/api/orders", json=body())
            last = (await _next(ws, "order.created"))["id"]
        missed = [(await http.post("/api/orders", json=body())).json()["id"] for _ in range(3)]  # while "offline"
        async with websockets.connect(f"{ws_url}&last_event_id={last}") as ws:
            got = set()
            async with asyncio.timeout(4):
                while not set(missed) <= got:
                    m = json.loads(await ws.recv())
                    if m.get("type") == "order.created":
                        got.add(m["order_id"])
            assert set(missed) <= got


async def test_consumer_stream_only_sees_its_own_orders_and_gets_full_history(server):
    """What the customer screen relies on: role=consumer + last_event_id=0 replays that customer's history."""
    ws_url = server.replace("http", "ws") + "/ws"
    async with httpx.AsyncClient(base_url=server) as http:
        mine = (await http.post("/api/orders", json=body(consumer="cust-1"))).json()
        await http.post("/api/orders", json=body(consumer="cust-2"))
        async with websockets.connect(ws_url + "?role=consumer&id=cust-1&last_event_id=0") as ws:
            await ws.recv()  # hello
            first = await _next(ws, "order.created")
            assert first["order_id"] == mine["id"] and first.get("replay") is True
            other = (await http.post("/api/orders", json=body(consumer="cust-2"))).json()
            again = (await http.post("/api/orders", json=body(consumer="cust-1"))).json()
            live = await _next(ws, "order.created")
            assert live["order_id"] == again["id"] != other["id"]


async def test_dashboard_endpoints(server):
    async with httpx.AsyncClient(base_url=server) as http:
        a = (await http.post("/api/orders", json=body(merchant="m-X", consumer="cust-9"))).json()
        await http.post("/api/orders", json=body(merchant="m-Y"))
        rows = {r["merchant_id"]: r for r in (await http.get("/api/merchants")).json()}
        assert rows["m-X"]["total"] == 1 and rows["m-X"]["waiting"] + rows["m-X"]["courier_coming"] == 1
        by_consumer = (await http.get("/api/orders?consumer_id=cust-9")).json()
        assert [o["id"] for o in by_consumer] == [a["id"]]
        many = (await http.get("/api/orders?status=CREATED,ASSIGNING,FAILED")).json()
        assert {o["status"] for o in many} <= {"CREATED", "ASSIGNING", "FAILED"} and len(many) == 2
        await http.get(f"/api/orders/{a['id']}")  # a cached read shows up in the metrics overview
        m = (await http.get("/api/metrics/overview")).json()
        names = {s["name"] for s in m["api"]}
        assert "dispatchflow_http_requests_total" in names and "dispatchflow_cache_requests_total" in names
        s = (await http.get("/api/stats/summary")).json()
        assert "geo_couriers" in s
        assert (await http.get("/api/loadtest/latest?kind=bogus")).status_code == 422
        assert (await http.get("/static/fonts/archivo-wdth.woff2")).status_code == 200
