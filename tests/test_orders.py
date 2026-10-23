"""Order lifecycle, idempotency, matching and concurrency (service layer + worker handlers)."""
import asyncio
import time

import pytest
from sqlalchemy import func, select

from app import couriers, dispatch, orders
from app.config import get_settings
from app.db import SessionLocal
from app.errors import Conflict, IdempotencyMismatch
from app.models import Courier, Order, Outbox
from app.redis_store import store

from .helpers import body, db_courier, db_order, event_types, mk_courier, mk_order


# ---------------------------------------------------------------- idempotent creation
async def test_concurrent_duplicate_creates_yield_one_order():
    results = await asyncio.gather(*[orders.create_order(body(), "same-key") for _ in range(25)])
    assert sum(1 for _, created in results if created) == 1
    assert len({doc["id"] for doc, _ in results}) == 1
    async with SessionLocal() as s:
        assert (await s.execute(select(func.count()).select_from(Order))).scalar_one() == 1
        assert (await s.execute(select(func.count()).select_from(Outbox))).scalar_one() == 1  # one assignment message


async def test_same_key_different_body_is_rejected():
    await orders.create_order(body(consumer="a"), "k")
    with pytest.raises(IdempotencyMismatch):
        await orders.create_order(body(consumer="b"), "k")


async def test_no_key_means_every_request_is_a_new_order():
    a, _ = await orders.create_order(body(), None)
    b, _ = await orders.create_order(body(), None)
    assert a["id"] != b["id"]


# ---------------------------------------------------------------- matching
async def test_nearest_courier_is_chosen():
    near = await mk_courier("near", 37.775, -122.42)
    await mk_courier("far", 37.80, -122.38)
    o = await mk_order(plat=37.776, plng=-122.419)
    out = await dispatch.assign_order(o["id"])
    assert out.note == "assigned"
    order = await db_order(o["id"])
    assert order.status == "ASSIGNED" and order.courier_id == near["id"]
    c = await db_courier(near["id"])
    assert c.status == "BUSY" and c.active_order_id == o["id"]
    assert await event_types(o["id"]) == ["order.created", "order.assigning", "order.assigned"]


async def test_matching_still_works_when_redis_is_down():
    near = await mk_courier("near", 37.775, -122.42)
    await mk_courier("far", 37.80, -122.38)
    store._open_until = time.monotonic() + 3600  # circuit breaker open: Redis is skipped entirely
    o = await mk_order()
    out = await dispatch.assign_order(o["id"])
    assert out.note == "assigned" and (await db_order(o["id"])).courier_id == near["id"]


async def test_no_courier_backs_off_then_fails_after_max_attempts(monkeypatch):
    monkeypatch.setattr(get_settings(), "max_assign_attempts", 3)
    o = await mk_order()
    first = await dispatch.assign_order(o["id"])
    second = await dispatch.assign_order(o["id"])
    assert (first.ack, second.ack) == (False, False) and first.delay >= 1
    assert (await db_order(o["id"])).status == "ASSIGNING"
    third = await dispatch.assign_order(o["id"])
    assert third.ack and third.note == "exhausted"
    order = await db_order(o["id"])
    assert order.status == "FAILED" and order.failure_reason == "no_courier_available"
    assert (await event_types(o["id"]))[-1] == "order.failed"


async def test_duplicate_assignment_message_is_a_noop():
    await mk_courier()
    o = await mk_order()
    assert (await dispatch.assign_order(o["id"])).note == "assigned"
    again = await dispatch.assign_order(o["id"])  # SQS delivered it twice
    assert again.ack and again.note == "stale"
    assert (await event_types(o["id"])).count("order.assigned") == 1


async def test_one_courier_is_never_double_booked_under_concurrency():
    c = await mk_courier()
    ids = [(await mk_order())["id"] for _ in range(12)]
    outs = await asyncio.gather(*[dispatch.assign_order(i) for i in ids])
    assert sum(1 for o in outs if o.note == "assigned") == 1
    async with SessionLocal() as s:
        assigned = (await s.execute(select(func.count()).select_from(Order).where(Order.status == "ASSIGNED"))).scalar_one()
    assert assigned == 1 and (await db_courier(c["id"])).status == "BUSY"


async def test_many_orders_many_couriers_each_courier_gets_at_most_one():
    cs = [await mk_courier(f"c{i}", 37.77 + i * 0.001, -122.42) for i in range(8)]
    ids = [(await mk_order())["id"] for _ in range(20)]
    outs = await asyncio.gather(*[dispatch.assign_order(i) for i in ids])
    assert sum(1 for o in outs if o.note == "assigned") == 8
    async with SessionLocal() as s:
        rows = (await s.execute(select(Order.courier_id, func.count()).where(Order.status == "ASSIGNED").group_by(Order.courier_id))).all()
    assert len(rows) == 8 and all(n == 1 for _, n in rows)
    assert {c["id"] for c in cs} == {r[0] for r in rows}


async def test_freed_courier_is_matched_to_the_longest_waiting_order():
    o1 = await mk_order()
    o2 = await mk_order()
    await dispatch.assign_order(o1["id"])
    await dispatch.assign_order(o2["id"])  # both waiting (ASSIGNING)
    c = await mk_courier()
    out = await dispatch.match_courier(c["id"])
    assert out.note == "matched_waiting_order"
    assert (await db_order(o1["id"])).status == "ASSIGNED" and (await db_order(o2["id"])).status == "ASSIGNING"


async def test_malformed_messages_are_permanent_errors():
    for bad in ({"type": "order.assign"}, {"type": "order.assign", "order_id": 5}, {"type": "courier.available", "courier_id": "nope"}, {"type": "wat"}, "str"):
        with pytest.raises(dispatch.PermanentError):
            await dispatch.handle(bad, 1)


# ---------------------------------------------------------------- courier actions
async def _assigned():
    c = await mk_courier()
    o = await mk_order()
    await dispatch.assign_order(o["id"])
    return o["id"], c["id"]


async def test_happy_path_and_courier_release():
    oid, cid = await _assigned()
    _, applied, why = await orders.transition(oid, "PICKED_UP", courier_id=cid)
    assert (applied, why) == (True, "applied")
    _, applied, _ = await orders.transition(oid, "DELIVERED", courier_id=cid)
    assert applied
    assert await event_types(oid) == ["order.created", "order.assigning", "order.assigned", "order.picked_up", "order.delivered"]
    c = await db_courier(cid)
    assert c.status == "AVAILABLE" and c.active_order_id is None
    o = await db_order(oid)
    assert o.created_at <= o.assigned_at <= o.picked_up_at <= o.delivered_at


async def test_duplicate_transitions_are_noops():
    oid, cid = await _assigned()
    await orders.transition(oid, "PICKED_UP", courier_id=cid)
    outs = await asyncio.gather(*[orders.transition(oid, "PICKED_UP", courier_id=cid) for _ in range(10)])
    assert all(not applied and why == "duplicate" for _, applied, why in outs)
    outs = await asyncio.gather(*[orders.transition(oid, "DELIVERED", courier_id=cid) for _ in range(10)])
    assert sum(1 for _, applied, _ in outs if applied) == 1
    types = await event_types(oid)
    assert types.count("order.picked_up") == 1 and types.count("order.delivered") == 1


async def test_delayed_pickup_is_inferred_and_the_late_update_is_ignored():
    oid, cid = await _assigned()
    _, applied, _ = await orders.transition(oid, "DELIVERED", courier_id=cid)  # pickup update was delayed
    assert applied
    types = await event_types(oid)
    assert types[-2:] == ["order.picked_up", "order.delivered"]
    _, applied, why = await orders.transition(oid, "PICKED_UP", courier_id=cid)  # ...and now it arrives
    assert (applied, why) == (False, "stale")
    assert (await event_types(oid)).count("order.picked_up") == 1


async def test_cancel_frees_the_courier_and_blocks_later_pickup():
    oid, cid = await _assigned()
    _, applied, _ = await orders.transition(oid, "CANCELLED", reason="changed mind")
    assert applied
    assert (await db_courier(cid)).status == "AVAILABLE"
    with pytest.raises(Conflict):
        await orders.transition(oid, "PICKED_UP", courier_id=cid)


async def test_cannot_cancel_after_pickup():
    oid, cid = await _assigned()
    await orders.transition(oid, "PICKED_UP", courier_id=cid)
    with pytest.raises(Conflict):
        await orders.transition(oid, "CANCELLED")


async def test_wrong_courier_is_rejected():
    oid, _ = await _assigned()
    other = await mk_courier("other", online=False)
    with pytest.raises(Conflict) as e:
        await orders.transition(oid, "PICKED_UP", courier_id=other["id"])
    assert e.value.code == "not_assigned_courier"


async def test_reject_reassigns_to_someone_else_and_excludes_the_rejecter():
    a = await mk_courier("a", 37.775, -122.42)
    o = await mk_order()
    await dispatch.assign_order(o["id"])
    assert (await db_order(o["id"])).courier_id == a["id"]
    b = await mk_courier("b", 37.80, -122.38)
    _, applied, _ = await orders.transition(o["id"], "ASSIGNING", courier_id=a["id"], reason="too far")
    assert applied
    order = await db_order(o["id"])
    assert order.status == "ASSIGNING" and order.courier_id is None and order.excluded_courier_ids == [a["id"]]
    # `a` is free again and nearer, but must not get the order back
    out = await dispatch.assign_order(o["id"])
    assert out.note == "assigned" and (await db_order(o["id"])).courier_id == b["id"]
    # a duplicate / late reject from `a` is recognised and ignored
    _, applied, why = await orders.transition(o["id"], "ASSIGNING", courier_id=a["id"])
    assert (applied, why) == (False, "stale_courier")


async def test_silent_courier_assignment_is_reclaimed():
    oid, cid = await _assigned()
    await asyncio.sleep(0.05)
    assert await orders.expire_stale_assignments(0) == 1
    order = await db_order(oid)
    assert order.status == "ASSIGNING" and cid in order.excluded_courier_ids
    assert (await db_courier(cid)).status == "AVAILABLE"
    assert await orders.expire_stale_assignments(60) == 0


async def test_reconciler_requeues_orders_that_lost_their_message():
    o = await mk_order()
    async with SessionLocal() as s, s.begin():
        await s.execute(Outbox.__table__.delete())  # simulate a lost assignment message
    await asyncio.sleep(0.05)
    assert await orders.redrive_stuck_orders(0) == 1
    async with SessionLocal() as s:
        assert (await s.execute(select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None)))).scalar_one() == 1
    assert (await db_order(o["id"])).version == 2


# ---------------------------------------------------------------- couriers
async def test_stale_location_ping_cannot_move_a_courier_backwards():
    from datetime import datetime, timedelta, timezone

    c = await mk_courier()
    now = datetime.now(timezone.utc)
    assert await couriers.update_location(c["id"], 37.70, -122.40, now)
    assert not await couriers.update_location(c["id"], 37.50, -122.00, now - timedelta(seconds=30))  # delayed ping
    row = await db_courier(c["id"])
    assert (row.lat, row.lng) == (37.70, -122.40)


async def test_busy_courier_cannot_go_offline():
    oid, cid = await _assigned()
    with pytest.raises(Conflict):
        await couriers.set_status(cid, "OFFLINE")


# ---------------------------------------------------------------- cache
async def test_cache_never_goes_backwards_in_version():
    o = await mk_order()
    oid = o["id"]
    doc = await orders.get_order(oid)  # populates the cache at v1
    assert doc["version"] == 1
    await store.set_order(oid, 5, {**doc, "version": 5, "status": "ASSIGNED"})
    await store.set_order(oid, 3, {**doc, "version": 3, "status": "ASSIGNING"})  # a slow reader loses
    cached = await store.get_order(oid)
    assert cached["version"] == 5 and cached["status"] == "ASSIGNED"
    fresh = await orders.get_order(oid, fresh=True)  # bypass reads the database
    assert fresh["version"] == 1
