"""Simulation building blocks shared by the live demo and the load test:

  Api          HTTP client with retries/fail-over, latency + outcome accounting
  EventTap     WebSocket subscriber that reconnects with ?last_event_id= and de-duplicates by id
  CourierAgent a courier app: accepts / rejects jobs, sends GPS pings, picks up and delivers,
               and (optionally) misbehaves: duplicate taps, delayed pickups, retries after timeouts
"""
import asyncio
import itertools
import json
import math
import random
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
import websockets

# A ~10 km x 8 km patch of San Francisco
LAT_MIN, LAT_MAX = 37.72, 37.80
LNG_MIN, LNG_MAX = -122.48, -122.38


# Approximate eastern shoreline (lat, lng), north to south: points east of it are in the bay.
SHORE = [(37.806, -122.405), (37.799, -122.398), (37.792, -122.392), (37.785, -122.388), (37.776, -122.387),
         (37.766, -122.386), (37.757, -122.384), (37.748, -122.384), (37.739, -122.379), (37.730, -122.372), (37.718, -122.367)]


def on_land(lat: float, lng: float) -> bool:
    for (la0, ln0), (la1, ln1) in zip(SHORE, SHORE[1:]):
        if la1 <= lat <= la0:
            shore = ln0 + (ln1 - ln0) * (la0 - lat) / (la0 - la1)
            return lng < shore - 0.002
    return True


def rand_point(rng: random.Random) -> dict:
    while True:
        lat, lng = rng.uniform(LAT_MIN, LAT_MAX), rng.uniform(LNG_MIN, LNG_MAX)
        if on_land(lat, lng):
            return {"lat": round(lat, 6), "lng": round(lng, 6)}


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(math.ceil(q / 100 * len(s))) - 1)]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -------------------------------------------------------------------------------------------------
class Api:
    def __init__(self, bases: list[str], concurrency: int = 256, retries: int = 8):
        self.bases = [b.rstrip("/") for b in bases]
        self.retries = retries
        self._rr = itertools.count()
        self.sem = asyncio.Semaphore(concurrency)
        self.http = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency),
            timeout=httpx.Timeout(10.0, connect=3.0),
        )
        self.latency: dict[str, list[float]] = defaultdict(list)
        self.counts: Counter = Counter()  # sent, retries, conn_errors, http_5xx, gave_up, ...
        self.outcomes: Counter = Counter()  # transition outcomes reported by the server

    async def close(self) -> None:
        await self.http.aclose()

    async def call(self, name: str, method: str, path: str, *, json_body=None, headers=None) -> httpx.Response | None:
        attempt = 0
        resp = None
        async with self.sem:
            while True:
                base = self.bases[(next(self._rr) + attempt) % len(self.bases)]  # fail over to the next instance
                t0 = time.perf_counter()
                self.counts["sent"] += 1
                try:
                    resp = await self.http.request(method, base + path, json=json_body, headers=headers)
                except httpx.TransportError:
                    self.counts["conn_errors"] += 1
                    resp = None
                else:
                    self.latency[name].append((time.perf_counter() - t0) * 1000)
                    if resp.status_code < 500:
                        return resp
                    self.counts["http_5xx"] += 1
                attempt += 1
                if attempt > self.retries:
                    self.counts["gave_up"] += 1
                    return resp
                self.counts["retries"] += 1
                await asyncio.sleep(min(4.0, 0.2 * 2**attempt) * (0.5 + random.random()))

    def note_outcome(self, resp: httpx.Response | None) -> dict | None:
        if resp is None or resp.status_code >= 300:
            return None
        try:
            doc = resp.json()
        except ValueError:
            return None
        if "outcome" in doc:
            self.outcomes[doc["outcome"]] += 1
        return doc


# -------------------------------------------------------------------------------------------------
class EventTap:
    """Subscribes to the ops stream, survives disconnects (replay from last id) and fans assignments out."""

    def __init__(self, bases: list[str]):
        self.bases = [b.rstrip("/").replace("http://", "ws://").replace("https://", "wss://") for b in bases]
        self.last_id = 0
        self.seen_ids: set[int] = set()
        self.by_type: Counter = Counter()
        self.reconnects = 0
        self.lagged = 0
        self.on_event = None  # callback(dict)
        self._stop = False
        self._task: asyncio.Task | None = None
        self.connected = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop = True
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        n = 0
        while not self._stop:
            base = self.bases[n % len(self.bases)]
            n += 1
            url = f"{base}/ws?role=ops" + (f"&last_event_id={self.last_id}" if self.last_id else "")
            try:
                async with websockets.connect(url, max_size=None, ping_interval=20, open_timeout=3) as ws:
                    self.connected.set()
                    if self.last_id:
                        self.reconnects += 1
                    async for raw in ws:
                        self._handle(json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            self.connected.clear()
            await asyncio.sleep(0.3)

    def _handle(self, e: dict) -> None:
        t = e.get("type")
        if t == "lagged":
            self.lagged += 1
            return
        if t in ("hello", "ping"):
            return
        eid = e.get("id")
        if eid is not None:
            if eid in self.seen_ids:  # replays overlap on purpose; ids make them harmless
                return
            self.seen_ids.add(eid)
            self.last_id = max(self.last_id, eid)
        self.by_type[t] += 1
        if self.on_event:
            self.on_event(e)


# -------------------------------------------------------------------------------------------------
@dataclass
class Behavior:
    p_reject: float = 0.05  # courier gives the job back
    p_dup_action: float = 0.12  # taps "pickup"/"deliver" more than once (double tap / retry after timeout)
    p_late_pickup: float = 0.08  # the pickup update is delayed: arrives AFTER the delivery update
    travel_s: tuple = (0.05, 0.3)  # per leg
    pings_per_leg: int = 1
    ping_interval: float = 0.0  # >0 => stream pings along the path (demo)
    idle_poll_s: float = 10.0  # pull fallback if a push is ever missed


@dataclass
class CourierAgent:
    api: Api
    courier_id: str
    name: str
    lat: float
    lng: float
    behavior: Behavior
    rng: random.Random = field(default_factory=random.Random)
    inbox: asyncio.Queue = field(default_factory=asyncio.Queue)
    online: bool = True  # what the courier *wants* (shift change toggles this)
    is_online: bool = True  # what the server believes
    handled: set = field(default_factory=set)
    stats: Counter = field(default_factory=Counter)
    _task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def offer(self, order_id: str, job: dict | None = None) -> None:
        if order_id not in self.handled:
            self.inbox.put_nowait((order_id, job))

    # ---- helpers ------------------------------------------------------------------------------
    async def ping(self, lat: float, lng: float) -> None:
        self.lat, self.lng = lat, lng
        r = await self.api.call(
            "PUT /couriers/:id/location", "PUT", f"/api/couriers/{self.courier_id}/location",
            json_body={"lat": lat, "lng": lng, "ts": now_iso()},
        )
        self.stats["pings"] += 1
        if r is not None and r.status_code == 200 and not r.json().get("applied", True):
            self.stats["pings_ignored"] += 1

    async def travel(self, to_lat: float, to_lng: float) -> None:
        b = self.behavior
        total = self.rng.uniform(*b.travel_s)
        if b.ping_interval > 0:
            steps = max(1, int(total / b.ping_interval))
            lat0, lng0 = self.lat, self.lng
            for i in range(1, steps + 1):
                await asyncio.sleep(total / steps)
                f = i / steps
                await self.ping(lat0 + (to_lat - lat0) * f, lng0 + (to_lng - lng0) * f)
        else:
            await asyncio.sleep(total)
            for _ in range(b.pings_per_leg):
                await self.ping(to_lat, to_lng)

    async def act(self, name: str, order_id: str, action: str, reason: str | None = None):
        body = {"courier_id": self.courier_id}
        if reason:
            body["reason"] = reason
        r = await self.api.call(f"POST /orders/:id/{action}", "POST", f"/api/orders/{order_id}/{action}", json_body=body)
        self.api.note_outcome(r)
        return r

    async def act_maybe_duplicated(self, order_id: str, action: str) -> int | None:
        """Send once, sometimes several times at once (double tap / client retry after a timeout)."""
        if self.rng.random() < self.behavior.p_dup_action:
            n = self.rng.randint(2, 4)
            self.stats["dup_actions"] += n - 1
            rs = await asyncio.gather(*[self.act("", order_id, action) for _ in range(n)])
            codes = [r.status_code for r in rs if r is not None]
            return codes[0] if codes else None
        r = await self.act("", order_id, action)
        return r.status_code if r is not None else None

    # ---- a job ----------------------------------------------------------------------------------
    async def do_job(self, order_id: str, job: dict | None) -> None:
        b = self.behavior
        self.handled.add(order_id)
        if job and "pickup" in job:  # the push carries the job details
            order = {"pickup": job["pickup"], "dropoff": job["dropoff"]}
        else:  # fall back to reading the order (bypass the cache: the push told us it changed)
            r = await self.api.call("GET /orders/:id?fresh", "GET", f"/api/orders/{order_id}?fresh=true")
            if r is None or r.status_code != 200:
                return
            order = r.json()
            if order["courier_id"] != self.courier_id or order["status"] != "ASSIGNED":
                return  # stale offer (already cancelled / reassigned)

        if self.rng.random() < b.p_reject:
            self.stats["rejected"] += 1
            await self.act("", order_id, "reject", reason="too_far")
            return

        await self.travel(order["pickup"]["lat"], order["pickup"]["lng"])
        late_pickup = self.rng.random() < b.p_late_pickup
        if not late_pickup:
            code = await self.act_maybe_duplicated(order_id, "pickup")
            if code is not None and code >= 400:  # e.g. order cancelled while we were driving
                self.stats["job_aborted"] += 1
                return
        await self.travel(order["dropoff"]["lat"], order["dropoff"]["lng"])
        code = await self.act_maybe_duplicated(order_id, "deliver")
        if code is not None and code >= 400:
            self.stats["job_aborted"] += 1
            return
        self.stats["delivered"] += 1
        if late_pickup:  # the delayed "pickup" finally shows up after the delivery: must be ignored
            self.stats["late_pickups"] += 1
            await asyncio.sleep(self.rng.uniform(0.05, 0.4))
            await self.act("", order_id, "pickup")

    # ---- main loop ------------------------------------------------------------------------------
    async def set_presence(self, online: bool) -> bool:
        r = await self.api.call(
            "POST /couriers/:id/status", "POST", f"/api/couriers/{self.courier_id}/status",
            json_body={"status": "AVAILABLE" if online else "OFFLINE"},
        )
        if r is not None and r.status_code == 200:
            self.is_online = online
            return True
        return False  # 409 while on a delivery: try again after the job

    async def _run(self) -> None:
        idle_since = time.monotonic()
        while True:
            try:
                order_id, job = await asyncio.wait_for(self.inbox.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if self.online != self.is_online:
                    await self.set_presence(self.online)
                elif self.is_online and time.monotonic() - idle_since > self.behavior.idle_poll_s:
                    idle_since = time.monotonic()  # pull fallback: did we miss a push?
                    r = await self.api.call("GET /couriers/:id", "GET", f"/api/couriers/{self.courier_id}")
                    if r is not None and r.status_code == 200 and r.json().get("active_order_id"):
                        oid = r.json()["active_order_id"]
                        if oid not in self.handled:
                            self.stats["poll_recoveries"] += 1
                            self.inbox.put_nowait((oid, None))
                continue
            try:
                await self.do_job(order_id, job)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.stats["agent_errors"] += 1
            idle_since = time.monotonic()
