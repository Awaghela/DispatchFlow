from sqlalchemy import select

from app import couriers, orders
from app.db import SessionLocal
from app.models import Courier, Order, OrderEvent


def body(merchant="m1", consumer="c1", plat=37.776, plng=-122.419, dlat=37.79, dlng=-122.41):
    return {"merchant_id": merchant, "consumer_id": consumer,
            "pickup": {"lat": plat, "lng": plng}, "dropoff": {"lat": dlat, "lng": dlng}}


async def mk_courier(name="c", lat=37.775, lng=-122.42, online=True):
    return await couriers.create_courier(name, lat, lng, online)


async def mk_order(key=None, **kw):
    doc, _ = await orders.create_order(body(**kw), key)
    return doc


async def db_order(oid) -> Order:
    async with SessionLocal() as s:
        return await s.get(Order, oid)


async def db_courier(cid) -> Courier:
    async with SessionLocal() as s:
        return await s.get(Courier, cid)


async def event_types(oid) -> list[str]:
    async with SessionLocal() as s:
        rows = (await s.execute(select(OrderEvent.type).where(OrderEvent.order_id == oid).order_by(OrderEvent.id))).all()
    return [r[0] for r in rows]
