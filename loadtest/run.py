"""DispatchFlow load / chaos / demo harness.

  # live demo traffic for the dashboard (couriers drive around, orders flow, forever)
  python -m loadtest.run --mode demo

  # the reliability test: thousands of orders with duplicates, rejections, delayed updates,
  # retries and real service interruptions (needs the local stack: scripts/stackctl.py)
  python -m loadtest.run --mode load --orders 4000 --rate 50 --couriers 250 --chaos

It writes loadtest/results/latest.json (+ .md), which the dashboard's "Reliability lab" tab displays.
"""
import argparse
import asyncio
import json
import os
import platform
import random
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import stackctl  # noqa: E402

for _k, _v in stackctl.APP_ENV.items():  # same env as the local stack: lets us talk to SQS/DB directly
    os.environ.setdefault(_k, _v)

from loadtest.sim import (  # noqa: E402
    Api, Behavior, CourierAgent, EventTap, pct, rand_point,
)
from loadtest.verify import DB_URL, run_invariants  # noqa: E402

RESULTS = ROOT / "loadtest" / "results"
MERCHANTS = [f"merchant-{i:02d}" for i in range(1, 13)]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


async def sh(*args: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(ROOT / "scripts" / "stackctl.py"), *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    await proc.communicate()


class Harness:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        self.rng = random.Random(a.seed)
        self.apis = [u.strip() for u in a.apis.split(",")]
        self.api = Api(self.apis, concurrency=a.concurrency)
        self.tap = EventTap(self.apis)
        self.agents: dict[str, CourierAgent] = {}
        self.prefix = f"run-{int(time.time())}-"
        self.counts: Counter = Counter()
        self.chaos_log: list[dict] = []
        self.ev_counts: Counter = Counter()  # live events seen on the WebSocket, by type
        self.timeline: list[dict] = []  # one sample every 2s: the report's run-over-time chart
        self.t0 = 0.0
        self.engine = create_async_engine(DB_URL)

    # ---------------------------------------------------------------------- setup
    async def scalar(self, sql: str, **p):
        async with self.engine.connect() as c:
            return (await c.execute(text(sql), p)).scalar_one()

    async def wait_ready(self) -> None:
        for _ in range(60):
            r = await self.api.call("ready", "GET", "/ready", )
            if r is not None and r.status_code == 200:
                return
            await asyncio.sleep(1)
        sys.exit("API not ready")

    async def setup_couriers(self, n: int, behavior: Behavior) -> None:
        async def one(i: int):
            p = rand_point(self.rng)
            r = await self.api.call("POST /couriers", "POST", "/api/couriers",
                                    json_body={"name": f"Courier-{i:03d}", "lat": p["lat"], "lng": p["lng"], "online": True})
            c = r.json()
            ag = CourierAgent(self.api, c["id"], c["name"], c["lat"], c["lng"], behavior, rng=random.Random(self.rng.random()))
            self.agents[c["id"]] = ag

        await asyncio.gather(*[one(i + 1) for i in range(n)])
        for ag in self.agents.values():
            ag.start()

    def on_event(self, e: dict) -> None:
        self.ev_counts[e.get("type", "?")] += 1
        if e.get("type") == "order.assigned":
            ag = self.agents.get(e["payload"].get("courier_id"))
            if ag:
                ag.offer(e["order_id"], e["payload"])

    # ---------------------------------------------------------------------- orders
    def new_order_body(self) -> dict:
        return {"merchant_id": self.rng.choice(MERCHANTS), "consumer_id": f"consumer-{self.rng.randint(1, 5000):04d}",
                "pickup": rand_point(self.rng), "dropoff": rand_point(self.rng)}

    async def post_order(self, key: str, body: dict, kind: str):
        r = await self.api.call("POST /orders", "POST", "/api/orders", json_body=body, headers={"Idempotency-Key": key})
        if r is not None:
            self.counts[f"create_{kind}_{r.status_code}"] += 1
            if r.headers.get("idempotent-replay") == "true":
                self.counts["server_idempotent_replays"] += 1
        return r

    async def place_order(self, i: int) -> None:
        key, body = f"{self.prefix}{i}", self.new_order_body()
        self.counts["unique_orders_sent"] += 1
        dups = []
        if self.rng.random() < self.a.p_dup_create:
            for _ in range(self.rng.randint(1, 3)):  # clients that time out and retry, double-submits...
                delay = 0.0 if self.rng.random() < 0.5 else self.rng.uniform(0.2, 1.5)
                dups.append(self._dup_later(key, body, delay))
                self.counts["duplicate_creates_sent"] += 1
        results = await asyncio.gather(self.post_order(key, body, "first"), *dups)
        r = results[0]
        if r is not None and r.status_code in (200, 201) and self.rng.random() < self.a.p_cancel:
            await asyncio.sleep(self.rng.uniform(0.05, 1.0))
            oid = r.json()["id"]
            c = await self.api.call("POST /orders/:id/cancel", "POST", f"/api/orders/{oid}/cancel", json_body={"reason": "customer_changed_mind"})
            self.api.note_outcome(c)
            self.counts[f"cancel_{c.status_code if c is not None else 'none'}"] += 1

    async def _dup_later(self, key, body, delay):
        if delay:
            await asyncio.sleep(delay)
        return await self.post_order(key, body, "dup")

    # ---------------------------------------------------------------------- chaos
    async def chaos_step(self, name: str, fn, *args) -> None:
        t = time.time() - self.t0
        log(f"CHAOS ▶ {name}")
        await fn(*args)
        self.chaos_log.append({"name": name, "at_s": round(t, 1), "duration_s": round(time.time() - self.t0 - t, 1)})
        log(f"CHAOS ■ {name} restored")

    async def worker_crash(self, down: float):
        await sh("stop", "worker", "--kill"); await asyncio.sleep(down); await sh("start", "worker")

    async def redis_outage(self, down: float):
        await sh("stop", "redis", "--kill"); await asyncio.sleep(down); await sh("start", "redis")

    async def api_outage(self, down: float):
        await sh("stop", "api", "api2", "--kill"); await asyncio.sleep(down)
        await asyncio.gather(sh("start", "api"), sh("start", "api2"))

    async def postgres_crash(self, down: float):
        await sh("stop", "postgres", "--kill"); await asyncio.sleep(down); await sh("start", "postgres")

    async def shift_change(self, frac: float, dur: float):
        pick = self.rng.sample(list(self.agents.values()), int(len(self.agents) * frac))
        for ag in pick:
            ag.online = False
        await asyncio.sleep(dur)
        for ag in pick:
            ag.online = True

    async def chaos_timeline(self, total_s: float) -> None:
        plan = [
            (0.12, "worker killed (SIGKILL) mid-flight", self.worker_crash, 4.0),
            (0.28, "Redis down (cache, GEO index, pub/sub)", self.redis_outage, 8.0),
            (0.42, "shift change: 90% of couriers go offline", self.shift_change, 0.9, 14.0),
            (0.62, "ALL API instances killed", self.api_outage, 6.0),
            (0.78, "PostgreSQL crash (immediate shutdown + WAL recovery)", self.postgres_crash, 5.0),
        ]
        start = time.time()
        for frac, name, fn, *args in plan:
            await asyncio.sleep(max(0.0, start + frac * total_s - time.time()))
            await self.chaos_step(name, fn, *args)

    # ---------------------------------------------------------------------- poison + drain
    async def inject_poison(self, n: int) -> None:
        from app.queue import SQSQueue

        q = SQSQueue()
        await q.start()
        bad = [{"type": "order.assign", "order_id": {"not": "a string"}}, {"type": "order.assign"},
               {"type": "courier.available", "courier_id": 42}, {"type": "mystery.event"}, "not-even-a-dict"]
        for i in range(n):
            await q.client.send_message(QueueUrl=q.url, MessageBody=json.dumps(bad[i % len(bad)]))
        await q.client.send_message(QueueUrl=q.url, MessageBody="{{{ not json")
        await q.stop()
        self.counts["poison_injected"] = n + 1

    async def drain(self, timeout: float) -> bool:
        like = f"{self.prefix}%"
        end, quiet = time.time() + timeout, 0
        while time.time() < end:
            try:
                active = await self.scalar(
                    "SELECT count(*) FROM orders WHERE idempotency_key LIKE :l AND status NOT IN ('DELIVERED','CANCELLED','FAILED')", l=like)
            except Exception:
                active = -1
            quiet = quiet + 1 if active == 0 else 0
            if quiet >= 3:
                return True
            await asyncio.sleep(1.0)
        return False

    async def queue_depths(self, timeout: float = 45.0) -> dict:
        from app.queue import SQSQueue

        q = SQSQueue()
        await q.start()
        end, d = time.time() + timeout, {}
        while time.time() < end:
            d = {"main": await q.depth(), "dlq": await q.depth(dlq=True)}
            if sum(d["main"].values()) == 0 and sum(d["dlq"].values()) == 0:
                break
            await asyncio.sleep(1.5)
        await q.stop()
        return d

    # ---------------------------------------------------------------------- run
    async def run_load(self) -> dict:
        a = self.a
        await self.wait_ready()
        baseline = await self.scalar("SELECT coalesce(max(id),0) FROM order_events")
        self.tap.on_event = self.on_event
        self.tap.start()
        await asyncio.wait_for(self.tap.connected.wait(), 15)
        beh = Behavior(p_reject=a.p_reject, p_dup_action=a.p_dup_action, p_late_pickup=a.p_late_pickup,
                       travel_s=(0.05, 0.35), pings_per_leg=1)
        log(f"registering {a.couriers} couriers")
        await self.setup_couriers(a.couriers, beh)
        await asyncio.sleep(1.5)

        total_s = a.orders / a.rate
        log(f"load: {a.orders} orders @ {a.rate}/s (~{total_s:.0f}s), chaos={'on' if a.chaos else 'off'}")
        self.t0 = time.time()
        chaos_task = asyncio.create_task(self.chaos_timeline(total_s)) if a.chaos else None
        sampler = asyncio.create_task(self.sample_timeline())
        tasks = []
        for i in range(a.orders):
            await asyncio.sleep(max(0.0, self.t0 + i / a.rate - time.time()))
            tasks.append(asyncio.create_task(self.place_order(i)))
            if i and i % 500 == 0:
                log(f"  submitted {i}/{a.orders}  reqs={self.api.counts['sent']} retries={self.api.counts['retries']}")
        await asyncio.gather(*tasks)
        if chaos_task:
            await chaos_task
        submit_s = time.time() - self.t0
        log(f"all orders submitted in {submit_s:.1f}s; injecting poison messages and draining")
        await self.inject_poison(a.poison)
        drained = await self.drain(a.drain_timeout)
        total_run_s = time.time() - self.t0
        log(f"drain {'complete' if drained else 'TIMED OUT'} after {total_run_s:.1f}s total")
        await asyncio.sleep(3)  # let the last events cross the WebSocket
        sampler.cancel()
        depths = await self.queue_depths()
        summary = (await self.api.call("stats", "GET", "/api/stats/summary"))
        server_stats = summary.json() if summary is not None and summary.status_code == 200 else {}

        checks, facts = await run_invariants(
            run_prefix=self.prefix, expected_keys=self.counts["unique_orders_sent"], baseline_event_id=baseline,
            ws_seen_ids=self.tap.seen_ids, queue_depths=depths)
        if self.counts["poison_injected"]:
            dl = server_stats.get("reliability", {}).get("dead_lettered", 0)
            checks.append({"name": "poison messages were parked in the DLQ and reaped", "passed": dl >= self.counts["poison_injected"],
                           "detail": f"{self.counts['poison_injected']} injected, dead_lettered counter={dl}"})
        return self.report(checks, facts, server_stats, submit_s, total_run_s, drained)

    async def sample_timeline(self) -> None:
        prev = None
        while True:
            c, e = self.api.counts, self.ev_counts
            cur = {"sent": c["sent"], "retries": c["retries"], "errors": c["conn_errors"] + c["http_5xx"],
                   "created": e["order.created"], "assigned": e["order.assigned"], "delivered": e["order.delivered"]}
            if prev is not None:
                self.timeline.append({"t": round(time.time() - self.t0, 1), **{k: cur[k] - prev[k] for k in cur}})
            prev = cur
            await asyncio.sleep(2)

    def report(self, checks, facts, server_stats, submit_s, total_s, drained) -> dict:
        a, api = self.a, self.api
        agent_stats: Counter = Counter()
        for ag in self.agents.values():
            agent_stats.update(ag.stats)
        lat = {}
        allv = []
        for name, vals in sorted(api.latency.items()):
            if name not in ("POST /couriers", "ready", "stats"):  # setup / probes are not workload
                allv += vals
            lat[name] = {"count": len(vals), "p50": round(pct(vals, 50), 1), "p95": round(pct(vals, 95), 1),
                         "p99": round(pct(vals, 99), 1), "max": round(max(vals), 1)}
        lat["ALL"] = {"count": len(allv), "p50": round(pct(allv, 50), 1), "p95": round(pct(allv, 95), 1),
                      "p99": round(pct(allv, 99), 1), "max": round(max(allv or [0]), 1)}
        logical = api.counts["sent"] - api.counts["retries"]
        orders_total = facts.get("orders_total", 0)
        events_persisted = facts.get("events_persisted", 0)
        by_status = facts.get("orders_by_status", {})
        return {
            "meta": {
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "mode": "load+chaos" if a.chaos else "load",
                "params": {"orders": a.orders, "rate_per_s": a.rate, "couriers": a.couriers, "api_instances": len(self.apis),
                          "p_duplicate_create": a.p_dup_create, "p_reject": a.p_reject, "p_duplicate_action": a.p_dup_action,
                          "p_late_pickup": a.p_late_pickup, "p_cancel": a.p_cancel, "seed": a.seed},
                "environment": {"python": platform.python_version(), "cpus": os.cpu_count(), "platform": platform.platform(),
                                "note": "single machine: PostgreSQL, Redis, SQS emulator (moto), 2 API processes, 1 worker and the load generator all share it"},
                "submit_seconds": round(submit_s, 1), "total_seconds": round(total_s, 1), "drained": drained,
            },
            "volume": {
                "orders_unique": self.counts["unique_orders_sent"],
                "duplicate_create_requests": self.counts["duplicate_creates_sent"],
                "client_requests_logical": logical,
                "client_requests_incl_retries": api.counts["sent"],
                "gps_pings": agent_stats["pings"],
                "courier_dup_actions": agent_stats["dup_actions"],
                "late_pickups_after_delivery": agent_stats["late_pickups"],
                "rejections": agent_stats["rejected"],
                "poison_messages": self.counts["poison_injected"],
                "order_events_persisted": events_persisted,
                "live_events_over_websocket": sum(self.tap.by_type.values()),
                "total_simulated_events": logical + events_persisted,
            },
            "throughput": {
                "orders_per_s_submitted": round(a.orders / max(submit_s, 1e-9), 1),
                "requests_per_s": round(api.counts["sent"] / max(total_s, 1e-9), 1),
                "delivered_per_s": round(by_status.get("DELIVERED", 0) / max(total_s, 1e-9), 1),
            },
            "latency_ms": lat,
            "outcomes": {"orders_total": orders_total, "by_status": by_status, "failed_by_reason": facts.get("failed_by_reason", {})},
            "assignment_latency_s": facts.get("assignment_latency_s"),
            "order_total_time_s": facts.get("order_total_time_s"),
            "reliability": {
                "idempotent_replays_returned": self.counts["server_idempotent_replays"],
                "duplicate_transitions_absorbed": api.outcomes["duplicate"],
                "stale_updates_ignored": api.outcomes["stale"] + api.outcomes["stale_courier"] + agent_stats["pings_ignored"],
                "transitions_applied": api.outcomes["applied"],
                "orders_with_assignment_retries": facts.get("orders_with_retries", 0),
                "orders_reassigned_after_rejection": facts.get("orders_reassigned", 0),
                "client_retries": api.counts["retries"], "client_connection_errors": api.counts["conn_errors"],
                "client_5xx_seen": api.counts["http_5xx"], "client_gave_up": api.counts["gave_up"],
                "websocket_reconnects": self.tap.reconnects, "websocket_lagged": self.tap.lagged,
                "courier_pull_recoveries": agent_stats["poll_recoveries"],
                "orders_redriven_by_reconciler": server_stats.get("reliability", {}).get("orders_redriven", 0),
                "assignments_reclaimed_after_courier_timeout": server_stats.get("reliability", {}).get("assignment_timeouts", 0),
                "agent_errors": agent_stats["agent_errors"],
                "events_by_type": facts.get("events_by_type", {}),
            },
            "chaos": self.chaos_log,
            "timeline": {"bucket_s": 2, "series": self.timeline},
            "invariants": checks,
            "invariants_passed": all(c["passed"] for c in checks),
            "server_reliability_counters": server_stats.get("reliability", {}),
        }

    # ---------------------------------------------------------------------- demo
    async def demo_customer(self, key: str, body: dict) -> None:
        """A customer places an order, then their app refreshes the order screen a few times
        (cached reads: these are what the Redis order cache serves)."""
        r = await self.post_order(key, body, "first")
        if r is None or r.status_code not in (200, 201):
            return
        oid = r.json()["id"]
        for wait in (1.5, 4.0, 8.0, 12.0):
            await asyncio.sleep(wait)
            await self.api.call("GET /orders/:id", "GET", f"/api/orders/{oid}")

    async def run_demo(self) -> None:
        a = self.a
        await self.wait_ready()
        self.tap.on_event = self.on_event
        self.tap.start()
        await asyncio.wait_for(self.tap.connected.wait(), 15)
        beh = Behavior(p_reject=0.04, p_dup_action=0.05, p_late_pickup=0.04, travel_s=(5.0, 12.0), ping_interval=0.8)
        existing = await self.api.call("GET /couriers", "GET", "/api/couriers?limit=500")
        rows = existing.json() if existing is not None and existing.status_code == 200 else []
        # Couriers left over from an earlier run have no agent behind them: clock them out so the
        # matcher doesn't hand jobs to people who will never pick up (the timeout would reclaim them).
        cutoff = datetime.now(timezone.utc).timestamp() - 15
        idle = [c for c in rows if c["status"] == "AVAILABLE" and (not c.get("location_ts") or
                datetime.fromisoformat(c["location_ts"]).timestamp() < cutoff)]
        for c in idle:
            await self.api.call("POST /couriers/status", "POST", f"/api/couriers/{c['id']}/status", json_body={"status": "OFFLINE"})
        log(f"demo: spawning {a.couriers} couriers ({len(rows)} existing, {len(idle)} idle ones clocked out)")
        await self.setup_couriers(a.couriers, beh)
        i, end = 0, time.time() + a.duration if a.duration else float("inf")
        log(f"demo traffic running ({1 / a.demo_interval:.1f} orders/s). Open the dashboard; Ctrl-C to stop.")
        try:
            while time.time() < end:
                body = self.new_order_body()
                asyncio.create_task(self.demo_customer(f"{self.prefix}{i}", body))
                i += 1
                await asyncio.sleep(self.rng.expovariate(1 / a.demo_interval))
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass


def write_report(rep: dict) -> None:
    """latest-chaos.json / latest-steady.json are what the dashboard shows; latest.json is the last run."""
    RESULTS.mkdir(exist_ok=True)
    kind = "chaos" if "chaos" in rep["meta"]["mode"] else "steady"
    body = json.dumps(rep, indent=2, default=float)
    for name in (f"latest-{kind}.json", "latest.json"):
        (RESULTS / name).write_text(body)
    (RESULTS / f"latest-{kind}.md").write_text(to_markdown(rep))
    (RESULTS / "latest.md").write_text(to_markdown(rep))


def to_markdown(r: dict) -> str:
    m, v, t, rel = r["meta"], r["volume"], r["throughput"], r["reliability"]
    L = [f"# DispatchFlow load test — {m['finished_at'][:19]}Z", "",
         f"**Mode:** {m['mode']} · **orders:** {v['orders_unique']} · **couriers:** {m['params']['couriers']} · "
         f"**API instances:** {m['params']['api_instances']} · **duration:** {m['total_seconds']}s", "",
         f"> {m['environment']['note']} ({m['environment']['cpus']} CPUs, Python {m['environment']['python']}).", "",
         "## Volume", "", "| metric | value |", "|---|---|"]
    L += [f"| {k.replace('_', ' ')} | {val:,} |" for k, val in v.items()]
    L += ["", "## Throughput", "", "| metric | value |", "|---|---|"] + [f"| {k.replace('_', ' ')} | {val} |" for k, val in t.items()]
    L += ["", "## Outcomes", "", f"`{json.dumps(r['outcomes']['by_status'])}` · failures: `{json.dumps(r['outcomes']['failed_by_reason'])}`", ""]
    a = r.get("assignment_latency_s") or {}
    L += [f"Assignment latency (create → courier assigned): p50 {a.get('p50', 0):.2f}s · p95 {a.get('p95', 0):.2f}s · p99 {a.get('p99', 0):.2f}s", ""]
    L += ["## API latency (client-side, ms)", "", "| endpoint | n | p50 | p95 | p99 | max |", "|---|---|---|---|---|---|"]
    for k, d in r["latency_ms"].items():
        L.append(f"| {k} | {d['count']:,} | {d['p50']} | {d['p95']} | {d['p99']} | {d['max']} |")
    L += ["", "## Reliability signals", "", "| signal | count |", "|---|---|"]
    L += [f"| {k.replace('_', ' ')} | {val} |" for k, val in rel.items() if not isinstance(val, dict)]
    L += ["", "## Injected service interruptions", ""]
    L += [f"- t+{c['at_s']}s — {c['name']} (down ~{c['duration_s']}s)" for c in r["chaos"]] or ["- none"]
    L += ["", "## Invariants", "", "| | check | detail |", "|---|---|---|"]
    L += [f"| {'✅' if c['passed'] else '❌'} | {c['name']} | {c['detail']} |" for c in r["invariants"]]
    return "\n".join(L) + "\n"


def print_summary(rep: dict) -> None:
    v, rel = rep["volume"], rep["reliability"]
    print("\n" + "=" * 78)
    print(f"orders {v['orders_unique']:,} | duplicate creates {v['duplicate_create_requests']:,} | requests {v['client_requests_logical']:,} (+{rel['client_retries']:,} retries)")
    print(f"order events persisted {v['order_events_persisted']:,} | total simulated events {v['total_simulated_events']:,} | duration {rep['meta']['total_seconds']}s")
    print(f"outcomes {rep['outcomes']['by_status']} failed_by_reason={rep['outcomes']['failed_by_reason']}")
    a = rep["assignment_latency_s"] or {}
    print(f"assignment latency p50/p95/p99: {a.get('p50', 0):.2f}/{a.get('p95', 0):.2f}/{a.get('p99', 0):.2f}s")
    for c in rep["chaos"]:
        print(f"chaos: t+{c['at_s']}s {c['name']} ({c['duration_s']}s)")
    print("-" * 78)
    for c in rep["invariants"]:
        print(f"{'PASS' if c['passed'] else 'FAIL'}  {c['name']}  [{c['detail']}]")
    print("=" * 78)
    print("ALL INVARIANTS HELD" if rep["invariants_passed"] else "INVARIANT FAILURES - see above")


async def amain(a: argparse.Namespace) -> int:
    h = Harness(a)
    try:
        if a.mode == "demo":
            await h.run_demo()
            return 0
        rep = await h.run_load()
        write_report(rep)
        print_summary(rep)
        return 0 if rep["invariants_passed"] else 1
    finally:
        for ag in h.agents.values():
            await ag.stop()
        await h.tap.stop()
        await h.api.close()
        await h.engine.dispose()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["load", "demo"], default="load")
    p.add_argument("--apis", default="http://127.0.0.1:8000,http://127.0.0.1:8001", help="comma-separated API base URLs")
    p.add_argument("--orders", type=int, default=4000)
    p.add_argument("--rate", type=float, default=50.0, help="orders per second")
    p.add_argument("--couriers", type=int, default=250)
    p.add_argument("--concurrency", type=int, default=256)
    p.add_argument("--chaos", action="store_true", help="inject worker/Redis/API/PostgreSQL failures and a courier shift change")
    p.add_argument("--p-dup-create", type=float, default=0.15, dest="p_dup_create")
    p.add_argument("--p-cancel", type=float, default=0.03, dest="p_cancel")
    p.add_argument("--p-reject", type=float, default=0.05, dest="p_reject")
    p.add_argument("--p-dup-action", type=float, default=0.12, dest="p_dup_action")
    p.add_argument("--p-late-pickup", type=float, default=0.08, dest="p_late_pickup")
    p.add_argument("--poison", type=int, default=5, help="malformed SQS messages to inject")
    p.add_argument("--drain-timeout", type=float, default=240.0, dest="drain_timeout")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--duration", type=float, default=0, help="demo: stop after N seconds (0 = forever)")
    p.add_argument("--demo-interval", type=float, default=1.5, dest="demo_interval", help="demo: mean seconds between orders")
    a = p.parse_args()
    if a.mode == "demo" and a.couriers == 250:
        a.couriers = 24
    if a.mode == "demo":
        a.apis = a.apis.split(",")[0]
    try:
        sys.exit(asyncio.run(amain(a)))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
