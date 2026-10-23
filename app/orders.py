"""Order service: idempotent creation and race-safe state transitions.

Correctness model
-----------------
* Creation idempotency  - ``INSERT ... ON CONFLICT (idempotency_key) DO NOTHING``. PostgreSQL makes
  concurrent duplicates wait for the first insert to commit, so N parallel retries create exactly one order.
* Transition idempotency - every transition runs under ``SELECT ... FOR UPDATE`` on the order row and is
  classified by the state machine: replay -> no-op, delayed/out-of-order -> ignored, conflict -> 409.
* Lock order is always  order -> courier  (never the reverse), so concurrent flows can't deadlock.
"""
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import metrics
from .db import SessionLocal
from .domain import CourierStatus, OrderStatus, Verdict, classify, new_id
from .errors import Conflict, DomainError, IdempotencyMismatch, NotFound
from .models import Courier, Order, Outbox, utcnow
from .redis_store import store
from .serializers import order_to_dict
from .uow import bump, emit, emit_ephemeral, geo_add, track_order, uow

S = OrderStatus
COURIER_ACTIONS = {S.PICKED_UP, S.DELIVERED, S.ASSIGNING}  # ASSIGNING == "reject"


def _same_payload(o: Order, data: dict) -> bool:
    return (
        o.merchant_id == data["merchant_id"]
        and o.consumer_id == data["consumer_id"]
        and abs(o.pickup_lat - data["pickup"]["lat"]) < 1e-9
        and abs(o.pickup_lng - data["pickup"]["lng"]) < 1e-9
        and abs(o.dropoff_lat - data["dropoff"]["lat"]) < 1e-9
        and abs(o.dropoff_lng - data["dropoff"]["lng"]) < 1e-9
    )


async def create_order(data: dict, idempotency_key: str | None) -> tuple[dict, bool]:
    """Returns (order, created). ``created`` is False when an identical request was already processed."""
    oid, now = new_id("ord"), utcnow()
    values = dict(
        id=oid,
        idempotency_key=idempotency_key or None,
        merchant_id=data["merchant_id"],
        consumer_id=data["consumer_id"],
        pickup_lat=data["pickup"]["lat"],
        pickup_lng=data["pickup"]["lng"],
        dropoff_lat=data["dropoff"]["lat"],
        dropoff_lng=data["dropoff"]["lng"],
        status=S.CREATED.value,
        excluded_courier_ids=[],
        assignment_attempts=0,
        reassignments=0,
        version=1,
        created_at=now,
        updated_at=now,
    )
    async with uow() as s:
        if idempotency_key:
            stmt = (
                pg_insert(Order)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[Order.idempotency_key])
                .returning(Order.id)
            )
            inserted = (await s.execute(stmt)).scalar_one_or_none()
        else:
            s.add(Order(**values))
            inserted = oid

        if inserted is None:  # a request with this key already committed
            existing = (
                await s.execute(select(Order).where(Order.idempotency_key == idempotency_key))
            ).scalar_one()
            if not _same_payload(existing, data):
                raise IdempotencyMismatch("Idempotency-Key was already used with a different request body")
            metrics.DUPLICATES_ABSORBED.labels("create").inc()
            bump(s, "duplicates_absorbed")
            return order_to_dict(existing), False

        order = await s.get(Order, oid)
        await emit(s, "order.created", order, payload=order_to_dict(order))
        # same transaction as the order: the assignment request cannot be lost
        s.add(Outbox(topic="order.assign", payload={"type": "order.assign", "order_id": oid}))
        track_order(s, order)
        metrics.ORDERS_CREATED.inc()
        metrics.ORDER_TRANSITIONS.labels(S.CREATED.value).inc()
        return order_to_dict(order), True


async def get_order(order_id: str, fresh: bool = False) -> dict:
    """Read-through cache. ``fresh=True`` bypasses it (use after a push notification that says the data changed)."""
    doc = None if fresh else await store.get_order(order_id)
    if doc is not None:
        return doc
    async with SessionLocal() as s:
        order = await s.get(Order, order_id)
    if order is None:
        raise NotFound(f"order {order_id} not found")
    doc = order_to_dict(order)
    await store.set_order(order.id, order.version, doc)  # version CAS: can't overwrite newer data
    return doc


async def list_orders(
    status: str | None = None,
    merchant_id: str | None = None,
    courier_id: str | None = None,
    limit: int = 50,
    consumer_id: str | None = None,
) -> list[dict]:
    stmt = select(Order).order_by(Order.created_at.desc()).limit(min(max(limit, 1), 500))
    if status:  # one status or a comma-separated list
        stmt = stmt.where(Order.status.in_([x.strip().upper() for x in status.split(",") if x.strip()]))
    if merchant_id:
        stmt = stmt.where(Order.merchant_id == merchant_id)
    if courier_id:
        stmt = stmt.where(Order.courier_id == courier_id)
    if consumer_id:
        stmt = stmt.where(Order.consumer_id == consumer_id)
    async with SessionLocal() as s:
        return [order_to_dict(o) for o in (await s.execute(stmt)).scalars()]


async def release_courier(s, order: Order, courier_id: str | None, *, at_dropoff: bool) -> None:
    """Free a courier after delivery / cancellation / rejection and hint the matcher."""
    if not courier_id:
        return
    c = (await s.execute(select(Courier).where(Courier.id == courier_id).with_for_update())).scalar_one_or_none()
    if c is None or c.active_order_id != order.id:
        return  # courier already moved on: nothing to release
    c.active_order_id = None
    if c.status == CourierStatus.BUSY:
        c.status = CourierStatus.AVAILABLE.value
    if at_dropoff:
        c.lat, c.lng = order.dropoff_lat, order.dropoff_lng
    if c.status == CourierStatus.AVAILABLE:
        geo_add(s, c.id, c.lat, c.lng)
        s.add(Outbox(topic="courier.available", payload={"type": "courier.available", "courier_id": c.id}))
    emit_ephemeral(s, "courier.status", courier_id=c.id, payload={"status": c.status, "lat": c.lat, "lng": c.lng})


async def _reassign(s, order: Order, rejected_by: str | None, reason: str | None) -> None:
    """Take the job away from a courier (rejected it / never picked it up) and re-queue it for matching."""
    order.excluded_courier_ids = [*(order.excluded_courier_ids or []), *([rejected_by] if rejected_by else [])]
    order.status, order.courier_id, order.assigned_at = S.ASSIGNING.value, None, None
    order.reassignments += 1
    order.assignment_attempts = 0
    order.version += 1
    await emit(s, "order.reassigning", order, courier_id=rejected_by, payload={"rejected_by": rejected_by, "reason": reason})
    await release_courier(s, order, rejected_by, at_dropoff=False)
    s.add(Outbox(topic="order.assign", payload={"type": "order.assign", "order_id": order.id}))
    bump(s, "reassignments")
    track_order(s, order)


async def expire_stale_assignments(timeout_s: int, limit: int = 50) -> int:
    """Reclaim jobs whose courier went silent (app crash, dead battery...). Safe to run from many workers."""
    async with uow() as s:
        rows = (
            await s.execute(
                select(Order)
                .where(Order.status == S.ASSIGNED.value, Order.assigned_at < utcnow() - timedelta(seconds=timeout_s))
                .order_by(Order.assigned_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).scalars().all()
        for order in rows:
            await _reassign(s, order, order.courier_id, "pickup_timeout")
            metrics.ASSIGNMENT_TIMEOUTS.inc()
            bump(s, "assignment_timeouts")
        return len(rows)


async def redrive_stuck_orders(after_s: int, limit: int = 100) -> int:
    """Reconciler: an order that is still waiting for a courier but has shown no activity for ``after_s``
    gets its assignment message re-enqueued. Handlers are idempotent, so a spurious re-drive is harmless,
    and it guarantees no order hangs forever if a queue message is ever lost (purge, expiry, operator error)."""
    async with uow() as s:
        rows = (
            await s.execute(
                select(Order)
                .where(Order.status.in_([S.CREATED.value, S.ASSIGNING.value]), Order.updated_at < utcnow() - timedelta(seconds=after_s))
                .order_by(Order.updated_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).scalars().all()
        for order in rows:
            s.add(Outbox(topic="order.assign", payload={"type": "order.assign", "order_id": order.id}))
            order.version += 1  # also refreshes updated_at so we don't re-drive it every sweep
            track_order(s, order)
            metrics.ORDERS_REDRIVEN.inc()
            bump(s, "orders_redriven")
        return len(rows)


async def transition(
    order_id: str,
    target: str,
    *,
    courier_id: str | None = None,
    reason: str | None = None,
    expected_version: int | None = None,
) -> tuple[dict, bool, str]:
    """Apply a client-driven transition. Returns (order, applied, outcome) where outcome is one of
    applied | duplicate | stale | stale_courier."""
    target = S(target)
    async with uow() as s:
        order = (await s.execute(select(Order).where(Order.id == order_id).with_for_update())).scalar_one_or_none()
        if order is None:
            raise NotFound(f"order {order_id} not found")
        if expected_version is not None and order.version != expected_version:
            raise Conflict(f"version mismatch: order is at v{order.version}", code="version_mismatch")

        if target in COURIER_ACTIONS:
            if not courier_id:
                raise DomainError("courier_id is required for this action", code="courier_required")
            if order.courier_id != courier_id:
                if courier_id in (order.excluded_courier_ids or []):
                    # a retry/late message from a courier that already gave the order up
                    metrics.STALE_IGNORED.labels("transition").inc()
                    bump(s, "stale_ignored")
                    return order_to_dict(order), False, "stale_courier"
                raise Conflict("order is not assigned to this courier", code="not_assigned_courier")

        verdict = classify(order.status, target)
        if verdict is Verdict.NOOP:
            metrics.DUPLICATES_ABSORBED.labels("transition").inc()
            bump(s, "duplicates_absorbed")
            return order_to_dict(order), False, "duplicate"
        if verdict is Verdict.STALE:
            metrics.STALE_IGNORED.labels("transition").inc()
            bump(s, "stale_ignored")
            return order_to_dict(order), False, "stale"
        if verdict is Verdict.ILLEGAL:
            raise Conflict(f"cannot move order from {order.status} to {target}", code="illegal_transition")

        now = utcnow()
        if target is S.PICKED_UP:
            order.status, order.picked_up_at = S.PICKED_UP.value, now
            order.version += 1
            await emit(s, "order.picked_up", order)
            metrics.ORDER_TRANSITIONS.labels(S.PICKED_UP.value).inc()

        elif target is S.DELIVERED:
            if order.status == S.ASSIGNED:  # the pickup update was delayed/lost: infer it
                order.status, order.picked_up_at = S.PICKED_UP.value, now
                order.version += 1
                await emit(s, "order.picked_up", order, payload={"inferred": True})
                metrics.ORDER_TRANSITIONS.labels(S.PICKED_UP.value).inc()
            order.status, order.delivered_at = S.DELIVERED.value, now
            order.version += 1
            await emit(
                s,
                "order.delivered",
                order,
                payload={"total_seconds": round((now - order.created_at).total_seconds(), 2)},
            )
            await release_courier(s, order, order.courier_id, at_dropoff=True)
            metrics.ORDER_TRANSITIONS.labels(S.DELIVERED.value).inc()

        elif target is S.CANCELLED:
            was_assigned = order.status == S.ASSIGNED
            order.status = S.CANCELLED.value
            order.version += 1
            await emit(s, "order.cancelled", order, payload={"reason": reason or "cancelled"})
            if was_assigned:
                await release_courier(s, order, order.courier_id, at_dropoff=False)
            metrics.ORDER_TRANSITIONS.labels(S.CANCELLED.value).inc()

        elif target is S.ASSIGNING:  # courier rejected the job -> back into the matching pool
            await _reassign(s, order, order.courier_id, reason)
            metrics.ORDER_TRANSITIONS.labels("REASSIGNING").inc()

        track_order(s, order)
        return order_to_dict(order), True, "applied"
