from datetime import datetime

from sqlalchemy import or_, select, update

from . import metrics
from .db import SessionLocal
from .domain import CourierStatus, new_id
from .errors import Conflict, NotFound
from .models import Courier, Order, Outbox, utcnow
from .serializers import courier_to_dict
from .uow import bump, emit_ephemeral, geo_add, geo_remove, uow

C = CourierStatus


async def create_courier(name: str, lat: float, lng: float, online: bool = False) -> dict:
    now = utcnow()
    c = Courier(
        id=new_id("crr"),
        name=name,
        status=(C.AVAILABLE if online else C.OFFLINE).value,
        lat=lat,
        lng=lng,
        location_ts=now,
        created_at=now,
    )
    async with uow() as s:
        s.add(c)
        await s.flush()
        if online:
            geo_add(s, c.id, lat, lng)
            s.add(Outbox(topic="courier.available", payload={"type": "courier.available", "courier_id": c.id}))
        emit_ephemeral(s, "courier.status", courier_id=c.id, payload={"status": c.status, "lat": lat, "lng": lng, "name": name})
        return courier_to_dict(c)


async def get_courier(courier_id: str) -> dict:
    async with SessionLocal() as s:
        c = await s.get(Courier, courier_id)
    if c is None:
        raise NotFound(f"courier {courier_id} not found")
    return courier_to_dict(c)


async def list_couriers(status: str | None = None, limit: int = 500) -> list[dict]:
    stmt = select(Courier).order_by(Courier.created_at).limit(min(max(limit, 1), 5000))
    if status:
        stmt = stmt.where(Courier.status == status)
    async with SessionLocal() as s:
        return [courier_to_dict(c) for c in (await s.execute(stmt)).scalars()]


async def set_status(courier_id: str, target: str) -> tuple[dict, bool]:
    target = C(target)
    if target is C.BUSY:
        raise Conflict("BUSY is assigned by the dispatcher, not set by clients", code="invalid_status")
    async with uow() as s:
        c = (await s.execute(select(Courier).where(Courier.id == courier_id).with_for_update())).scalar_one_or_none()
        if c is None:
            raise NotFound(f"courier {courier_id} not found")
        if c.status == target:
            metrics.DUPLICATES_ABSORBED.labels("transition").inc()
            return courier_to_dict(c), False
        if c.status == C.BUSY:
            raise Conflict("courier is on a delivery", code="courier_busy")
        c.status = target.value
        if target is C.AVAILABLE:
            geo_add(s, c.id, c.lat, c.lng)
            s.add(Outbox(topic="courier.available", payload={"type": "courier.available", "courier_id": c.id}))
        else:
            geo_remove(s, c.id)
        emit_ephemeral(s, "courier.status", courier_id=c.id, payload={"status": c.status, "lat": c.lat, "lng": c.lng, "name": c.name})
        return courier_to_dict(c), True


async def update_location(courier_id: str, lat: float, lng: float, ts: datetime | None) -> bool:
    """Compare-and-set on the location timestamp: a delayed ping can never move a courier backwards."""
    now = utcnow()
    ts = min(ts, now) if ts else now  # a bad client clock must not freeze the courier in the "future"
    async with uow() as s:
        row = (
            await s.execute(
                update(Courier)
                .where(Courier.id == courier_id, or_(Courier.location_ts.is_(None), Courier.location_ts < ts))
                .values(lat=lat, lng=lng, location_ts=ts)
                .returning(Courier.status, Courier.active_order_id)
            )
        ).first()
        if row is None:
            if (await s.scalar(select(Courier.id).where(Courier.id == courier_id))) is None:
                raise NotFound(f"courier {courier_id} not found")
            metrics.STALE_IGNORED.labels("location").inc()
            bump(s, "stale_ignored")
            return False
        status, active_order_id = row
        order = await s.get(Order, active_order_id) if active_order_id else None
        if status == C.AVAILABLE:
            geo_add(s, courier_id, lat, lng)
        emit_ephemeral(
            s,
            "courier.location",
            courier_id=courier_id,
            order=order,
            payload={"lat": lat, "lng": lng, "status": status},
        )
        return True
