"""Dashboard / KPI queries. Everything is derived from PostgreSQL (the source of truth) so numbers are
correct regardless of which process handled the work; Redis only adds queue depth and shared counters."""
import time

from sqlalchemy import text

from .db import SessionLocal
from .redis_store import store

_cache: dict[str, tuple[float, dict]] = {}


async def summary(window_minutes: int = 15) -> dict:
    hit = _cache.get("summary")
    if hit and time.monotonic() - hit[0] < 1.0:  # 1s micro-cache: many dashboards, one query
        return hit[1]
    w = max(1, min(int(window_minutes), 1440))
    async with SessionLocal() as s:
        orders = {r[0]: r[1] for r in (await s.execute(text("SELECT status, count(*) FROM orders GROUP BY status"))).all()}
        couriers = {r[0]: r[1] for r in (await s.execute(text("SELECT status, count(*) FROM couriers GROUP BY status"))).all()}
        lat = (
            await s.execute(
                text(
                    f"""
                    SELECT count(*),
                           coalesce(avg(x), 0),
                           coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY x), 0),
                           coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY x), 0),
                           coalesce(percentile_cont(0.99) WITHIN GROUP (ORDER BY x), 0)
                    FROM (SELECT extract(epoch FROM assigned_at - created_at) AS x
                          FROM orders WHERE assigned_at > now() - interval '{w} minutes') t
                    """
                )
            )
        ).one()
        dur = (
            await s.execute(
                text(
                    f"""SELECT count(*), coalesce(avg(extract(epoch FROM delivered_at - created_at)), 0)
                        FROM orders WHERE delivered_at > now() - interval '{w} minutes'"""
                )
            )
        ).one()
        rate = (
            await s.execute(
                text(
                    """SELECT
                         (SELECT count(*) FROM orders WHERE created_at > now() - interval '60 seconds'),
                         (SELECT count(*) FROM orders WHERE delivered_at > now() - interval '60 seconds'),
                         (SELECT count(*) FROM outbox WHERE published_at IS NULL),
                         (SELECT count(*) FROM order_events)"""
                )
            )
        ).one()

    depth = await store.get_json("df:queue_depth") or {}
    counters = await store.get_counters()
    geo = await store.geo_size()
    total = sum(orders.values())
    active = sum(orders.get(k, 0) for k in ("CREATED", "ASSIGNING", "ASSIGNED", "PICKED_UP"))
    doc = {
        "orders": {"total": total, "active": active, "by_status": orders},
        "couriers": {"total": sum(couriers.values()), "by_status": couriers},
        "assignment_latency_s": {
            "window_minutes": w,
            "count": lat[0],
            "avg": round(float(lat[1]), 4),
            "p50": round(float(lat[2]), 4),
            "p95": round(float(lat[3]), 4),
            "p99": round(float(lat[4]), 4),
        },
        "delivery_s": {"count": dur[0], "avg": round(float(dur[1]), 2)},
        "throughput_per_min": {"created": rate[0], "delivered": rate[1]},
        "outbox_backlog": rate[2],
        "events_total": rate[3],
        "queue": depth,
        "redis_healthy": store.healthy,
        "geo_couriers": geo,
        "reliability": {
            "duplicates_absorbed": counters.get("duplicates_absorbed", 0),
            "stale_ignored": counters.get("stale_ignored", 0),
            "assignment_retries": counters.get("assignment_retries", 0),
            "reassignments": counters.get("reassignments", 0),
            "assignment_timeouts": counters.get("assignment_timeouts", 0),
            "orders_redriven": counters.get("orders_redriven", 0),
            "orders_failed": counters.get("orders_failed", 0),
            "dead_lettered": counters.get("dead_lettered", 0),
        },
        "ts": time.time(),
    }
    _cache["summary"] = (time.monotonic(), doc)
    return doc


async def throughput(minutes: int = 5, bucket_s: int = 5) -> list[dict]:
    minutes = max(1, min(int(minutes), 60))
    bucket_s = max(1, min(int(bucket_s), 60))
    sql = f"""
    WITH grid AS (
      SELECT generate_series(
               date_bin('{bucket_s} seconds', now(), TIMESTAMPTZ '2000-01-01') - interval '{minutes} minutes',
               date_bin('{bucket_s} seconds', now(), TIMESTAMPTZ '2000-01-01'),
               interval '{bucket_s} seconds') AS t),
    c AS (SELECT date_bin('{bucket_s} seconds', created_at, TIMESTAMPTZ '2000-01-01') AS t, count(*) AS n
          FROM orders WHERE created_at > now() - interval '{minutes + 1} minutes' GROUP BY 1),
    d AS (SELECT date_bin('{bucket_s} seconds', delivered_at, TIMESTAMPTZ '2000-01-01') AS t, count(*) AS n
          FROM orders WHERE delivered_at > now() - interval '{minutes + 1} minutes' GROUP BY 1)
    SELECT grid.t, coalesce(c.n, 0), coalesce(d.n, 0)
    FROM grid LEFT JOIN c USING (t) LEFT JOIN d USING (t) ORDER BY grid.t
    """
    async with SessionLocal() as s:
        rows = (await s.execute(text(sql))).all()
    return [{"t": r[0].isoformat(), "created": r[1], "delivered": r[2]} for r in rows]


async def merchants(window_minutes: int = 60) -> list[dict]:
    w = max(1, min(int(window_minutes), 1440))
    sql = f"""
    SELECT merchant_id,
           count(*) FILTER (WHERE status IN ('CREATED','ASSIGNING'))           AS waiting,
           count(*) FILTER (WHERE status = 'ASSIGNED')                         AS courier_coming,
           count(*) FILTER (WHERE status = 'PICKED_UP')                        AS on_the_way,
           count(*) FILTER (WHERE status = 'DELIVERED')                        AS delivered,
           count(*) FILTER (WHERE status IN ('FAILED','CANCELLED'))            AS problems,
           count(*)                                                            AS total,
           coalesce(avg(extract(epoch FROM assigned_at - created_at)), 0)      AS avg_match_s,
           coalesce(avg(extract(epoch FROM delivered_at - created_at))
                    FILTER (WHERE delivered_at IS NOT NULL), 0)                AS avg_delivery_s
    FROM orders WHERE created_at > now() - interval '{w} minutes'
    GROUP BY merchant_id ORDER BY merchant_id
    """
    async with SessionLocal() as s:
        rows = (await s.execute(text(sql))).all()
    keys = ("merchant_id", "waiting", "courier_coming", "on_the_way", "delivered", "problems", "total")
    out = []
    for r in rows:
        d = dict(zip(keys, r[:7]))
        d["avg_match_s"] = round(float(r[7]), 3)
        d["avg_delivery_s"] = round(float(r[8]), 1)
        out.append(d)
    return out


def _parse_prom(body: str) -> list[dict]:
    from prometheus_client.parser import text_string_to_metric_families

    out = []
    for fam in text_string_to_metric_families(body):
        if not (fam.name.startswith("dispatchflow") or fam.name.startswith("process")):
            continue
        for smp in fam.samples:
            if smp.name.endswith("_created"):
                continue
            out.append({"name": smp.name, "labels": dict(smp.labels), "value": smp.value})
    return out


async def metrics_overview(api_body: str) -> dict:
    import httpx

    from .config import get_settings

    worker, err = None, None
    url = get_settings().worker_metrics_url
    if url:
        try:
            async with httpx.AsyncClient(timeout=1.5) as c:
                r = await c.get(url)
                r.raise_for_status()
                worker = _parse_prom(r.text)
        except Exception as exc:  # the worker may run elsewhere (e.g. its own ECS task)
            err = f"{type(exc).__name__}"
    return {"ts": time.time(), "api": _parse_prom(api_body), "worker": worker, "worker_error": err}
