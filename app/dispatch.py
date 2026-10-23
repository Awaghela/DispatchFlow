"""Courier matching and the SQS message handlers (run inside the worker process).

Both entry points are safe to run concurrently and repeatedly:

  order.assign       "find a courier for this order"   (sent on creation and on courier rejection)
  courier.available  "this courier is free, is anyone waiting?"  (sent when a courier frees up / goes online)

Concurrency safety comes from row locks: the order row is locked first, then candidate courier rows are
locked with SKIP LOCKED so two workers never fight over (or double-book) the same courier.
"""
import logging
import math
import random
import re
from dataclasses import dataclass

from sqlalchemy import select

from . import metrics
from .config import get_settings
from .domain import CourierStatus, OrderStatus, backoff_delay
from .models import Courier, Order, utcnow
from .redis_store import store
from .uow import bump, emit, emit_ephemeral, geo_remove, track_order, uow

log = logging.getLogger("dispatchflow.dispatch")
S = OrderStatus


@dataclass
class Outcome:
    ack: bool = True  # delete the message
    delay: int = 0  # if not acked: retry after this many seconds
    note: str = ""


class PermanentError(Exception):
    """The message can never succeed (malformed / invalid ids): send it straight to the DLQ
    instead of burning minutes of retries."""


_ORDER_ID = re.compile(r"^ord_[0-9a-f]{16}$")
_COURIER_ID = re.compile(r"^crr_[0-9a-f]{16}$")


class ChaosError(RuntimeError):
    """Injected failure used by the load tests (CHAOS_WORKER_FAIL_RATE)."""


def _maybe_chaos() -> None:
    rate = get_settings().chaos_worker_fail_rate
    if rate and random.random() < rate:
        raise ChaosError("injected worker failure")


# ---------------------------------------------------------------------------------------------
async def find_courier(s, order: Order) -> Courier | None:
    """Nearest AVAILABLE courier that hasn't rejected this order, locked FOR UPDATE.

    1. Redis GEO gives nearest-first candidate ids quickly (approximate: may be stale).
    2. PostgreSQL is the authority: we re-check status under a row lock.
    3. If Redis is down/empty/stale we fall back to a plain SQL nearest-neighbour query.

    Both queries use the "queue pop" pattern  ORDER BY distance LIMIT 1 FOR UPDATE SKIP LOCKED  so a
    transaction locks only the courier it will actually take. (Locking every candidate would make
    concurrent assigners starve each other.)
    """
    excluded = list(order.excluded_courier_ids or [])
    plat, plng = order.pickup_lat, order.pickup_lng
    lng_scale = math.cos(math.radians(plat))
    dist = (Courier.lat - plat) * (Courier.lat - plat) + ((Courier.lng - plng) * lng_scale) * ((Courier.lng - plng) * lng_scale)
    pop = (
        select(Courier)
        .where(Courier.status == CourierStatus.AVAILABLE.value)
        .order_by(dist)
        .limit(1)
        .with_for_update(skip_locked=True)
    )

    candidates = [c for c in await store.geo_candidates(plat, plng) if c not in excluded]
    if candidates:
        courier = (await s.execute(pop.where(Courier.id.in_(candidates)))).scalar_one_or_none()
        if courier is not None:
            return courier
    if excluded:
        pop = pop.where(Courier.id.not_in(excluded))
    return (await s.execute(pop)).scalar_one_or_none()


async def assign(s, order: Order, courier: Courier) -> None:
    now = utcnow()
    order.status, order.courier_id, order.assigned_at = S.ASSIGNED.value, courier.id, now
    order.version += 1
    courier.status, courier.active_order_id = CourierStatus.BUSY.value, order.id
    geo_remove(s, courier.id)
    latency = (now - order.created_at).total_seconds()
    await emit(
        s,
        "order.assigned",
        order,
        payload={
            "courier_id": courier.id,
            "courier_name": courier.name,
            "attempts": order.assignment_attempts,
            "assignment_latency_ms": round(latency * 1000, 1),
            # everything the courier app needs to start the job: no extra round trip
            "pickup": {"lat": order.pickup_lat, "lng": order.pickup_lng},
            "dropoff": {"lat": order.dropoff_lat, "lng": order.dropoff_lng},
        },
    )
    emit_ephemeral(
        s,
        "courier.status",
        courier_id=courier.id,
        order=order,
        payload={"status": courier.status, "lat": courier.lat, "lng": courier.lng, "name": courier.name},
    )
    track_order(s, order)
    metrics.ASSIGN_LATENCY.observe(latency)
    metrics.ASSIGN_ATTEMPTS.labels("assigned").inc()
    metrics.ORDER_TRANSITIONS.labels(S.ASSIGNED.value).inc()


async def fail_order(s, order: Order, reason: str) -> None:
    order.status, order.failure_reason = S.FAILED.value, reason
    order.version += 1
    await emit(s, "order.failed", order, payload={"reason": reason, "attempts": order.assignment_attempts})
    track_order(s, order)
    metrics.ASSIGN_ATTEMPTS.labels("dead_letter" if reason == "dead_letter" else "exhausted").inc()
    metrics.ORDER_TRANSITIONS.labels(S.FAILED.value).inc()
    bump(s, "orders_failed")


# ---------------------------------------------------------------------------------------------
async def assign_order(order_id: str, receive_count: int = 1) -> Outcome:
    cfg = get_settings()
    async with uow() as s:
        order = (await s.execute(select(Order).where(Order.id == order_id).with_for_update())).scalar_one_or_none()
        if order is None or order.status not in (S.CREATED.value, S.ASSIGNING.value):
            # duplicate delivery, or the order was already assigned / cancelled / failed
            metrics.DUPLICATES_ABSORBED.labels("message").inc()
            bump(s, "duplicates_absorbed")
            return Outcome(note="stale")

        _maybe_chaos()  # raises -> transaction rolls back -> SQS redelivers

        if order.status == S.CREATED.value:
            order.status = S.ASSIGNING.value
            order.version += 1
            await emit(s, "order.assigning", order)
            metrics.ORDER_TRANSITIONS.labels(S.ASSIGNING.value).inc()
        order.assignment_attempts += 1

        courier = await find_courier(s, order)
        if courier is not None:
            await assign(s, order, courier)
            return Outcome(note="assigned")

        order.version += 1
        if order.assignment_attempts >= cfg.max_assign_attempts:
            await fail_order(s, order, "no_courier_available")
            return Outcome(note="exhausted")

        await emit(s, "order.assignment_retry", order, payload={"attempt": order.assignment_attempts})
        track_order(s, order)
        bump(s, "assignment_retries")
        metrics.ASSIGN_ATTEMPTS.labels("no_courier").inc()
        return Outcome(
            ack=False,
            delay=backoff_delay(order.assignment_attempts, cfg.retry_base_delay, cfg.retry_max_delay),
            note="no_courier",
        )


async def match_courier(courier_id: str) -> Outcome:
    """A courier became free: give them the longest-waiting order (if any)."""
    async with uow() as s:
        order = (
            await s.execute(
                select(Order)
                .where(
                    Order.status == S.ASSIGNING.value,
                    ~Order.excluded_courier_ids.contains([courier_id]),
                )
                .order_by(Order.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)  # order first, then courier: global lock order
            )
        ).scalar_one_or_none()
        if order is None:
            return Outcome(note="nobody_waiting")
        courier = (
            await s.execute(
                select(Courier)
                .where(Courier.id == courier_id, Courier.status == CourierStatus.AVAILABLE.value)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()
        if courier is None:
            return Outcome(note="courier_unavailable")
        order.assignment_attempts += 1
        await assign(s, order, courier)
        return Outcome(note="matched_waiting_order")


async def handle(body: dict, receive_count: int) -> Outcome:
    kind = body.get("type") if isinstance(body, dict) else None
    if kind == "order.assign":
        oid = body.get("order_id")
        if not isinstance(oid, str) or not _ORDER_ID.match(oid):
            raise PermanentError(f"invalid order_id in {kind}: {oid!r}")
        return await assign_order(oid, receive_count)
    if kind == "courier.available":
        cid = body.get("courier_id")
        if not isinstance(cid, str) or not _COURIER_ID.match(cid):
            raise PermanentError(f"invalid courier_id in {kind}: {cid!r}")
        return await match_courier(cid)
    raise PermanentError(f"unknown message type {kind!r}")


async def reap_dead_letter(body: dict) -> None:
    """Messages that exhausted SQS redelivery: make sure the order doesn't hang forever."""
    oid = body.get("order_id") if isinstance(body, dict) else None
    if not isinstance(oid, str) or not _ORDER_ID.match(oid):
        log.warning("dead-letter message cannot be mapped to an order: %.200r", body)
        return
    async with uow() as s:
        order = (
            await s.execute(select(Order).where(Order.id == oid).with_for_update())
        ).scalar_one_or_none()
        if order is not None and order.status in (S.CREATED.value, S.ASSIGNING.value):
            await fail_order(s, order, "dead_letter")
