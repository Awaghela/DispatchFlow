"""Post-run invariant checks. These are the actual proof of correctness: after duplicates, crashes,
retries and restarts, the database must still tell a consistent story.

Each check returns (name, passed, detail)."""
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DB_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://dispatch:dispatch@localhost:5432/dispatchflow")
TERMINAL = "('DELIVERED','CANCELLED','FAILED')"


async def run_invariants(*, run_prefix: str, expected_keys: int, baseline_event_id: int, ws_seen_ids: set[int],
                         queue_depths: dict, extra: dict | None = None) -> tuple[list[dict], dict]:
    eng = create_async_engine(DB_URL)
    checks: list[dict] = []
    facts: dict = {}

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    like = f"{run_prefix}%"
    async with eng.connect() as c:
        q = lambda sql, **p: c.execute(text(sql), p)  # noqa: E731

        by_status = dict((await q("SELECT status, count(*) FROM orders WHERE idempotency_key LIKE :l GROUP BY 1", l=like)).all())
        n_orders = sum(by_status.values())
        facts["orders_by_status"] = by_status
        facts["orders_total"] = n_orders
        fail_reasons = dict((await q("SELECT coalesce(failure_reason,'-'), count(*) FROM orders WHERE idempotency_key LIKE :l AND status='FAILED' GROUP BY 1", l=like)).all())
        facts["failed_by_reason"] = fail_reasons

        # 1 -- exactly one order per idempotency key, despite duplicate / retried POSTs
        add("one order per idempotency key", n_orders == expected_keys,
            f"{expected_keys} unique keys sent -> {n_orders} orders stored")

        # 2 -- nothing is stuck mid-flight after the system settles
        stuck = n_orders - sum(v for k, v in by_status.items() if k in ("DELIVERED", "CANCELLED", "FAILED"))
        add("every order reached a terminal state", stuck == 0, f"{stuck} stuck; by status: {by_status}")

        # 3 -- no state transition was recorded twice
        dup = (await q(
            "SELECT count(*) FROM (SELECT order_id, type FROM order_events WHERE order_id IN "
            "(SELECT id FROM orders WHERE idempotency_key LIKE :l) AND type IN "
            "('order.created','order.picked_up','order.delivered','order.cancelled','order.failed') "
            "GROUP BY 1,2 HAVING count(*) > 1) x", l=like)).scalar_one()
        add("no transition recorded twice (created/picked_up/delivered/cancelled/failed)", dup == 0, f"{dup} duplicated")

        # 4 -- assigned-minus-reassigned is 0 or 1; delivered orders must have exactly one live assignment
        bad = (await q(
            """SELECT count(*) FROM orders o LEFT JOIN (
                 SELECT order_id, sum(CASE WHEN type='order.assigned' THEN 1 WHEN type='order.reassigning' THEN -1 ELSE 0 END) AS net
                 FROM order_events GROUP BY 1) e ON e.order_id = o.id
               WHERE o.idempotency_key LIKE :l
                 AND ((o.status IN ('DELIVERED','PICKED_UP') AND coalesce(e.net,0) <> 1)
                   OR (o.status NOT IN ('DELIVERED','PICKED_UP') AND coalesce(e.net,0) NOT IN (0,1)))""", l=like)).scalar_one()
        add("assignment history is consistent (assigned - reassigned in {0,1})", bad == 0, f"{bad} orders inconsistent")

        # 5 -- delivered orders are complete and time moves forward
        bad = (await q(
            """SELECT count(*) FROM orders WHERE idempotency_key LIKE :l AND status='DELIVERED' AND NOT (
                 courier_id IS NOT NULL AND assigned_at IS NOT NULL AND picked_up_at IS NOT NULL AND delivered_at IS NOT NULL
                 AND assigned_at >= created_at AND picked_up_at >= assigned_at AND delivered_at >= picked_up_at)""", l=like)).scalar_one()
        add("delivered orders have a courier and monotonic timestamps", bad == 0, f"{bad} malformed")

        # 6 -- the event log never shows a courier holding two orders at once (replays the whole history)
        overlaps = (await q(
            """SELECT count(*) FROM (
                 SELECT courier_id, max(running) mx, min(running) mn FROM (
                   SELECT courier_id, sum(delta) OVER (PARTITION BY courier_id ORDER BY id) AS running FROM (
                     SELECT id, courier_id, CASE WHEN type='order.assigned' THEN 1 ELSE -1 END AS delta
                     FROM order_events
                     WHERE courier_id IS NOT NULL AND type IN ('order.assigned','order.delivered','order.cancelled','order.reassigning')
                   ) x) y GROUP BY courier_id HAVING max(running) > 1 OR min(running) < 0) z""")).scalar_one()
        add("no courier was ever double-booked (event-log replay)", overlaps == 0, f"{overlaps} couriers with overlapping jobs")

        # 7 -- courier rows agree with order rows
        busy_bad = (await q(
            """SELECT count(*) FROM couriers c WHERE
                 (c.status='BUSY' AND (c.active_order_id IS NULL OR NOT EXISTS
                    (SELECT 1 FROM orders o WHERE o.id=c.active_order_id AND o.courier_id=c.id AND o.status IN ('ASSIGNED','PICKED_UP'))))
                 OR (c.status<>'BUSY' AND c.active_order_id IS NOT NULL)""")).scalar_one()
        orphan = (await q(
            """SELECT count(*) FROM orders o WHERE o.status IN ('ASSIGNED','PICKED_UP')
               AND NOT EXISTS (SELECT 1 FROM couriers c WHERE c.id=o.courier_id AND c.active_order_id=o.id)""")).scalar_one()
        add("courier and order rows agree (no leaked or orphaned assignments)", busy_bad == 0 and orphan == 0,
            f"{busy_bad} bad courier rows, {orphan} orphaned orders")

        # 8 -- ordering sanity: the first event for every order is order.created
        bad = (await q(
            """SELECT count(*) FROM (SELECT order_id FROM order_events WHERE order_id IN
                 (SELECT id FROM orders WHERE idempotency_key LIKE :l)
               GROUP BY 1 HAVING min(id) <> min(id) FILTER (WHERE type='order.created')) x""", l=like)).scalar_one()
        add("event log starts with order.created for every order", bad == 0, f"{bad} violations")

        # 9 -- delivery reliability: transactional outbox fully relayed
        ob = (await q("SELECT count(*) FROM outbox WHERE published_at IS NULL")).scalar_one()
        add("outbox fully relayed to SQS", ob == 0, f"{ob} unpublished rows")

        # 10 -- WebSocket stream completeness: every persisted event reached the ops subscriber
        ids = {r[0] for r in (await q("SELECT id FROM order_events WHERE id > :b", b=baseline_event_id)).all()}
        missing = ids - ws_seen_ids
        facts["events_persisted"] = len(ids)
        facts["events_ws_received"] = len(ids & ws_seen_ids)
        add("every persisted event was delivered over WebSocket (incl. replay after reconnect)", not missing,
            f"{len(ids & ws_seen_ids)}/{len(ids)} delivered, {len(missing)} missing")

        # evidence of the failure-handling paths actually being exercised
        ev = dict((await q(
            """SELECT type, count(*) FROM order_events WHERE order_id IN (SELECT id FROM orders WHERE idempotency_key LIKE :l)
               GROUP BY 1""", l=like)).all())
        facts["events_by_type"] = ev
        facts["assignment_latency_s"] = dict(zip(
            ("p50", "p95", "p99", "max", "n"),
            (await q(
                """SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY x),0), coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY x),0),
                          coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY x),0), coalesce(max(x),0), count(*)
                   FROM (SELECT extract(epoch FROM assigned_at - created_at) x FROM orders
                         WHERE idempotency_key LIKE :l AND assigned_at IS NOT NULL) t""", l=like)).one()))
        facts["order_total_time_s"] = dict(zip(
            ("p50", "p95", "p99", "max", "n"),
            (await q(
                """SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY x),0), coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY x),0),
                          coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY x),0), coalesce(max(x),0), count(*)
                   FROM (SELECT extract(epoch FROM delivered_at - created_at) x FROM orders
                         WHERE idempotency_key LIKE :l AND delivered_at IS NOT NULL) t""", l=like)).one()))
        facts["max_assignment_attempts"] = (await q("SELECT coalesce(max(assignment_attempts),0) FROM orders WHERE idempotency_key LIKE :l", l=like)).scalar_one()
        facts["orders_reassigned"] = (await q("SELECT count(*) FROM orders WHERE idempotency_key LIKE :l AND reassignments > 0", l=like)).scalar_one()
        facts["orders_with_retries"] = (await q("SELECT count(DISTINCT order_id) FROM order_events WHERE type='order.assignment_retry' AND order_id IN (SELECT id FROM orders WHERE idempotency_key LIKE :l)", l=like)).scalar_one()
        facts["couriers_by_status"] = dict((await q("SELECT status, count(*) FROM couriers GROUP BY 1")).all())
    await eng.dispose()

    # queues
    main, dlq = queue_depths.get("main", {}), queue_depths.get("dlq", {})
    main_total, dlq_total = sum(main.values()), sum(dlq.values())
    add("SQS main queue fully drained", main_total == 0, f"depth={main}")
    add("dead-letter queue empty after reaping", dlq_total == 0, f"depth={dlq}")
    return checks, facts
