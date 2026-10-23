"""Unit of work: one DB transaction + the side effects that must happen only *after* it commits
(event fan-out, cache refresh, GEO index update, shared counters).

    async with uow() as s:
        order = ...lock & mutate...
        await emit(s, "order.assigned", order, payload={...})
        track_order(s, order)
    # <- commit happened; Redis/WebSocket side effects were applied (best effort, never raises)
"""
import json
import logging
from collections import defaultdict
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession

from . import metrics
from .db import SessionLocal
from .hub import hub
from .models import Order, OrderEvent, utcnow
from .redis_store import store
from .serializers import event_to_dict, order_to_dict

log = logging.getLogger("dispatchflow.uow")


@asynccontextmanager
async def uow():
    async with SessionLocal() as s:
        s.info.update(
            events=[],
            ephemeral=[],
            orders={},
            geo_add={},
            geo_rm=set(),
            counters=defaultdict(int),
        )
        async with s.begin():
            yield s
        await _post_commit(s)


async def emit(
    s: AsyncSession, type_: str, order: Order, *, courier_id: str | None = None, payload: dict | None = None
) -> dict:
    """Persist a business event (audit log + replay source) and queue it for fan-out."""
    ev = OrderEvent(
        order_id=order.id,
        type=type_,
        merchant_id=order.merchant_id,
        consumer_id=order.consumer_id,
        courier_id=courier_id if courier_id is not None else order.courier_id,
        payload=payload or {},
        dedupe_key=f"{order.id}:{type_}:{order.version}",
        created_at=utcnow(),
    )
    s.add(ev)
    await s.flush()  # assigns the monotonically increasing id
    d = event_to_dict(ev)
    s.info["events"].append(d)
    return d


def emit_ephemeral(s: AsyncSession, type_: str, *, courier_id: str | None, payload: dict, order: Order | None = None) -> None:
    """High-frequency telemetry (courier GPS pings, status flips): fanned out live, not stored."""
    s.info["ephemeral"].append(
        {
            "id": None,
            "type": type_,
            "ts": utcnow().isoformat(),
            "order_id": order.id if order else None,
            "merchant_id": order.merchant_id if order else None,
            "consumer_id": order.consumer_id if order else None,
            "courier_id": courier_id,
            "payload": payload,
        }
    )


def track_order(s: AsyncSession, order: Order) -> None:
    s.info["orders"][order.id] = (order.version, json.dumps(order_to_dict(order), separators=(",", ":")))


def geo_add(s: AsyncSession, courier_id: str, lat: float, lng: float) -> None:
    s.info["geo_rm"].discard(courier_id)
    s.info["geo_add"][courier_id] = (lat, lng)


def geo_remove(s: AsyncSession, courier_id: str) -> None:
    s.info["geo_add"].pop(courier_id, None)
    s.info["geo_rm"].add(courier_id)


def bump(s: AsyncSession, name: str, n: int = 1) -> None:
    """Shared (cross-process) reliability counter shown on the dashboard."""
    s.info["counters"][name] += n


async def _post_commit(s: AsyncSession) -> None:
    try:
        info = s.info
        out = info["events"] + info["ephemeral"]
        msgs = [json.dumps(e, separators=(",", ":")) for e in out]
        ok = await store.apply_post_commit(
            publish=msgs,
            orders=info["orders"],
            geo_add=info["geo_add"],
            geo_rm=info["geo_rm"],
            counters=dict(info["counters"]),
        )
        if not ok and out:
            for e, text in zip(out, msgs):  # Redis down: serve sockets connected to this process
                hub.dispatch(e, text=text)
    except Exception:  # side effects are best-effort by design; the DB commit already happened
        log.exception("post-commit side effects failed")
