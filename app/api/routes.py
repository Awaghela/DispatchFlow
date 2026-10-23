import asyncio
import json
import logging
from pathlib import Path

from fastapi import APIRouter, Header, Query, Response, WebSocket, WebSocketDisconnect
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import select, text
from starlette.responses import FileResponse

from .. import couriers, orders, stats
from ..config import get_settings
from ..db import SessionLocal
from ..hub import ROLES, hub
from ..models import OrderEvent
from ..redis_store import store
from ..schemas import (
    CancelIn,
    CourierAction,
    CourierCreate,
    CourierOut,
    CourierStatusIn,
    LocationIn,
    OrderCreate,
    OrderOut,
    TransitionOut,
)
from ..serializers import event_to_dict

log = logging.getLogger("dispatchflow.api")
router = APIRouter()
cfg = get_settings()
STATIC = Path(__file__).resolve().parent.parent / "static"


# ---------------------------------------------------------------- orders
@router.post("/api/orders", response_model=OrderOut, status_code=201, tags=["orders"])
async def create_order(
    body: OrderCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=128),
):
    """Create an order. Send an `Idempotency-Key` header and retry freely: the same key always
    yields the same order (replays return 200 with `Idempotent-Replay: true`)."""
    order, created = await orders.create_order(body.model_dump(), idempotency_key)
    if not created:
        response.status_code = 200
        response.headers["Idempotent-Replay"] = "true"
    return order


@router.get("/api/orders", response_model=list[OrderOut], tags=["orders"])
async def list_orders(
    status: str | None = None,
    merchant_id: str | None = None,
    courier_id: str | None = None,
    consumer_id: str | None = None,
    limit: int = Query(50, ge=1, le=500),
):
    return await orders.list_orders(status, merchant_id, courier_id, limit, consumer_id)


@router.get("/api/orders/{order_id}", response_model=OrderOut, tags=["orders"])
async def get_order(order_id: str, fresh: bool = False):
    """Cached read. Pass `fresh=true` to bypass the cache (e.g. right after receiving a push event)."""
    return await orders.get_order(order_id, fresh)


async def _do(order_id: str, target: str, **kw) -> TransitionOut:
    order, applied, outcome = await orders.transition(order_id, target, **kw)
    return TransitionOut(applied=applied, outcome=outcome, order=order)


@router.post("/api/orders/{order_id}/pickup", response_model=TransitionOut, tags=["courier actions"])
async def pickup(order_id: str, body: CourierAction):
    return await _do(order_id, "PICKED_UP", courier_id=body.courier_id, expected_version=body.expected_version)


@router.post("/api/orders/{order_id}/deliver", response_model=TransitionOut, tags=["courier actions"])
async def deliver(order_id: str, body: CourierAction):
    return await _do(order_id, "DELIVERED", courier_id=body.courier_id, expected_version=body.expected_version)


@router.post("/api/orders/{order_id}/reject", response_model=TransitionOut, tags=["courier actions"])
async def reject(order_id: str, body: CourierAction):
    """Courier gives the job back; it is re-queued for matching and this courier is excluded."""
    return await _do(order_id, "ASSIGNING", courier_id=body.courier_id, reason=body.reason, expected_version=body.expected_version)


@router.post("/api/orders/{order_id}/cancel", response_model=TransitionOut, tags=["orders"])
async def cancel(order_id: str, body: CancelIn | None = None):
    body = body or CancelIn()
    return await _do(order_id, "CANCELLED", reason=body.reason, expected_version=body.expected_version)


# ---------------------------------------------------------------- couriers
@router.post("/api/couriers", response_model=CourierOut, status_code=201, tags=["couriers"])
async def create_courier(body: CourierCreate):
    return await couriers.create_courier(body.name, body.lat, body.lng, body.online)


@router.get("/api/couriers", response_model=list[CourierOut], tags=["couriers"])
async def list_couriers(status: str | None = None, limit: int = Query(500, ge=1, le=5000)):
    return await couriers.list_couriers(status, limit)


@router.get("/api/couriers/{courier_id}", response_model=CourierOut, tags=["couriers"])
async def get_courier(courier_id: str):
    return await couriers.get_courier(courier_id)


@router.post("/api/couriers/{courier_id}/status", response_model=CourierOut, tags=["couriers"])
async def set_courier_status(courier_id: str, body: CourierStatusIn):
    doc, _ = await couriers.set_status(courier_id, body.status)
    return doc


@router.put("/api/couriers/{courier_id}/location", tags=["couriers"])
async def update_location(courier_id: str, body: LocationIn):
    applied = await couriers.update_location(courier_id, body.lat, body.lng, body.ts)
    return {"applied": applied}


# ---------------------------------------------------------------- events / stats
@router.get("/api/events", tags=["events"])
async def list_events(
    after_id: int = Query(0, ge=0),
    order_id: str | None = None,
    merchant_id: str | None = None,
    limit: int = Query(200, ge=1, le=2000),
):
    stmt = select(OrderEvent).where(OrderEvent.id > after_id).order_by(OrderEvent.id).limit(limit)
    if order_id:
        stmt = stmt.where(OrderEvent.order_id == order_id)
    if merchant_id:
        stmt = stmt.where(OrderEvent.merchant_id == merchant_id)
    async with SessionLocal() as s:
        return [event_to_dict(e) for e in (await s.execute(stmt)).scalars()]


@router.get("/api/stats/summary", tags=["stats"])
async def stats_summary(window_minutes: int = Query(15, ge=1, le=1440)):
    doc = await stats.summary(window_minutes)
    return {**doc, "ws_connections": hub.connections}


@router.get("/api/stats/throughput", tags=["stats"])
async def stats_throughput(minutes: int = Query(5, ge=1, le=60), bucket_s: int = Query(5, ge=1, le=60)):
    return await stats.throughput(minutes, bucket_s)


@router.get("/api/merchants", tags=["stats"])
async def merchants(window_minutes: int = Query(60, ge=1, le=1440)):
    """Per-merchant view of the last N minutes: what is in the kitchen, on the road, and done."""
    return await stats.merchants(window_minutes)


@router.get("/api/metrics/overview", tags=["stats"])
async def metrics_overview():
    """The dashboard's Metrics view: this API instance's Prometheus registry plus the worker's
    (scraped over HTTP when WORKER_METRICS_URL is reachable), flattened to JSON samples."""
    return await stats.metrics_overview(generate_latest().decode())


@router.get("/api/loadtest/latest", tags=["stats"])
async def latest_loadtest(kind: str = Query("chaos", pattern="^(chaos|steady)$")):
    """Serves the most recent load-test report of that kind (written by `python -m loadtest.run`)."""
    base = Path(__file__).resolve().parents[2] / "loadtest" / "results"
    path = base / f"latest-{kind}.json"
    if not path.exists():
        return Response(status_code=204)
    return json.loads(path.read_text())


# ---------------------------------------------------------------- ops
@router.get("/health", tags=["ops"])
async def health():
    return {"status": "ok"}


@router.get("/ready", tags=["ops"])
async def ready(response: Response):
    try:
        async with SessionLocal() as s:
            await s.execute(text("SELECT 1"))
    except Exception as exc:
        response.status_code = 503
        return {"status": "unavailable", "database": str(exc)[:120]}
    # Redis is an accelerator: the service is ready (degraded) without it.
    return {"status": "ok", "redis": "up" if store.healthy else "degraded"}


@router.get("/metrics", include_in_schema=False)
async def metrics_endpoint():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------- websocket
@router.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    role: str = "ops",
    id: str | None = None,
    last_event_id: int | None = None,
):
    """Live event stream.

    role=ops                      everything (dashboard)
    role=merchant&id=<merchant>   that merchant's orders
    role=consumer&id=<consumer>   that consumer's orders
    role=order&id=<order>         one order
    role=courier&id=<courier>     one courier's jobs

    Reconnect with `last_event_id=<n>` to receive everything you missed. Events carry a numeric `id`;
    de-duplicate on it (replays deliberately overlap a little to never leave gaps).
    """
    if role not in ROLES or (role != "ops" and not id):
        await ws.close(code=4400)
        return
    await ws.accept()
    sub = hub.subscribe(role, id)  # subscribe BEFORE replaying so nothing falls in the gap
    try:
        await ws.send_text(json.dumps({"type": "hello", "role": role, "redis": store.healthy}))
        if last_event_id is not None:
            for e in await hub.replay(sub, last_event_id):
                await ws.send_text(json.dumps({**e, "replay": True}, separators=(",", ":")))
        pump = asyncio.create_task(_pump(ws, sub))
        recv = asyncio.create_task(_drain(ws))
        done, pending = await asyncio.wait({pump, recv}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            t.result()  # surface unexpected errors
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("ws closed: %r", exc)
    finally:
        hub.unsubscribe(sub)


async def _pump(ws: WebSocket, sub) -> None:
    from .. import metrics

    while True:
        try:
            text_msg = await asyncio.wait_for(sub.queue.get(), timeout=20)
        except asyncio.TimeoutError:
            await ws.send_text('{"type":"ping"}')
            continue
        await ws.send_text(text_msg)
        metrics.WS_MESSAGES.labels("sent").inc()
        if sub.dropped:
            await ws.send_text(json.dumps({"type": "lagged", "dropped": sub.dropped}))
            sub.dropped = 0


async def _drain(ws: WebSocket) -> None:
    while True:
        await ws.receive_text()  # we ignore client text (pings); raises on disconnect


# ---------------------------------------------------------------- dashboard
@router.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC / "index.html")
