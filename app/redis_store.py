"""Redis adapter.

Redis is an *accelerator*, never the source of truth: every method degrades to a harmless
default when Redis is down, and a small circuit breaker stops us paying a connect timeout on
every request during an outage. PostgreSQL always has the authoritative data.

Used for:
  * order read-through cache with a version compare-and-set (a slow reader can't resurrect stale data)
  * GEO index of AVAILABLE couriers for fast nearest-courier candidate search
  * pub/sub fan-out of events to every API instance (-> WebSockets)
  * shared reliability counters shown on the dashboard
"""
import json
import logging
import time

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from . import metrics
from .config import get_settings

log = logging.getLogger("dispatchflow.redis")

EVENTS_CHANNEL = "df:events"
GEO_KEY = "df:couriers:geo"
COUNTERS_KEY = "df:counters"

# Only overwrite when we are not older than what is cached.
_CAS_LUA = """
local cur = redis.call('HGET', KEYS[1], 'v')
if cur and tonumber(cur) > tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'v', ARGV[1], 'j', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
"""

_ERRORS = (RedisError, OSError, TimeoutError)


class RedisStore:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client: aioredis.Redis | None = None
        self._cas = None
        self._open_until = 0.0

    # ---- lifecycle -------------------------------------------------------------------------
    async def start(self) -> None:
        if not self.settings.redis_enabled:
            return
        self.client = aioredis.from_url(
            self.settings.redis_url,
            decode_responses=True,
            socket_timeout=0.5,
            socket_connect_timeout=0.5,
            health_check_interval=15,
            max_connections=100,
        )
        self._cas = self.client.register_script(_CAS_LUA)
        try:
            await self.client.ping()
        except _ERRORS as exc:
            self._trip("start", exc)

    async def stop(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    # ---- circuit breaker -------------------------------------------------------------------
    def available(self) -> bool:
        return self.client is not None and time.monotonic() >= self._open_until

    @property
    def healthy(self) -> bool:
        return self.available()

    def _trip(self, op: str, exc: Exception) -> None:
        was_open = time.monotonic() < self._open_until
        self._open_until = time.monotonic() + self.settings.redis_breaker_seconds
        metrics.REDIS_ERRORS.labels(op).inc()
        if not was_open:
            log.warning("redis unavailable (%s): %s -- falling back to PostgreSQL", op, exc)

    # ---- order cache -----------------------------------------------------------------------
    async def get_order(self, order_id: str) -> dict | None:
        if not self.available():
            return None
        try:
            raw = await self.client.hget(f"order:{order_id}", "j")
        except _ERRORS as exc:
            self._trip("get_order", exc)
            return None
        metrics.CACHE_REQUESTS.labels("hit" if raw else "miss").inc()
        return json.loads(raw) if raw else None

    async def set_order(self, order_id: str, version: int, doc: dict) -> None:
        if not self.available():
            return
        try:
            await self._cas(
                keys=[f"order:{order_id}"],
                args=[version, json.dumps(doc), self.settings.cache_ttl_seconds],
            )
        except _ERRORS as exc:
            self._trip("set_order", exc)

    # ---- courier GEO index -----------------------------------------------------------------
    async def geo_candidates(self, lat: float, lng: float, radii_km=(3, 10, 40), count=15) -> list[str]:
        """Nearest-first ids of couriers believed AVAILABLE. Approximate: callers must verify in DB."""
        if not self.available():
            return []
        try:
            for r in radii_km:
                found = await self.client.geosearch(
                    GEO_KEY, longitude=lng, latitude=lat, radius=r, unit="km", sort="ASC", count=count
                )
                if found:
                    return list(found)
        except _ERRORS as exc:
            self._trip("geo_search", exc)
        return []

    async def rebuild_geo(self, couriers: list[tuple[str, float, float]]) -> None:
        """Atomically replace the GEO index (self-heals after a Redis restart / missed update)."""
        if not self.available():
            return
        tmp = f"{GEO_KEY}:tmp"
        try:
            pipe = self.client.pipeline(transaction=True)
            pipe.delete(tmp)
            if couriers:
                for i in range(0, len(couriers), 500):
                    chunk = couriers[i : i + 500]
                    pipe.geoadd(tmp, [v for cid, lat, lng in chunk for v in (lng, lat, cid)])
                pipe.rename(tmp, GEO_KEY)
            else:
                pipe.delete(GEO_KEY)
            await pipe.execute()
        except _ERRORS as exc:
            self._trip("rebuild_geo", exc)

    # ---- one round trip after every commit -------------------------------------------------
    async def apply_post_commit(
        self,
        *,
        publish: list[str],
        orders: dict[str, tuple[int, str]],
        geo_add: dict[str, tuple[float, float]],
        geo_rm: set[str],
        counters: dict[str, int],
    ) -> bool:
        """Publish events, refresh the order cache, update the GEO index and bump shared counters
        in a single pipelined round trip. Returns False if Redis could not be reached."""
        if not (publish or orders or geo_add or geo_rm or counters):
            return True
        if not self.available():
            return False
        try:
            pipe = self.client.pipeline(transaction=False)
            for msg in publish:
                pipe.publish(EVENTS_CHANNEL, msg)
            for oid, (version, doc) in orders.items():
                await self._cas(  # with client=pipe this only queues the script call
                    keys=[f"order:{oid}"],
                    args=[version, doc, self.settings.cache_ttl_seconds],
                    client=pipe,
                )
            if geo_add:
                pipe.geoadd(GEO_KEY, [v for cid, (lat, lng) in geo_add.items() for v in (lng, lat, cid)])
            if geo_rm:
                pipe.zrem(GEO_KEY, *geo_rm)
            for name, n in counters.items():
                pipe.hincrby(COUNTERS_KEY, name, n)
            await pipe.execute()
            return True
        except _ERRORS as exc:
            self._trip("post_commit", exc)
            return False

    # ---- small shared state ----------------------------------------------------------------
    async def incr_counter(self, name: str, n: int = 1) -> None:
        if not self.available():
            return
        try:
            await self.client.hincrby(COUNTERS_KEY, name, n)
        except _ERRORS as exc:
            self._trip("incr", exc)

    async def get_counters(self) -> dict[str, int]:
        if not self.available():
            return {}
        try:
            raw = await self.client.hgetall(COUNTERS_KEY)
            return {k: int(v) for k, v in raw.items()}
        except _ERRORS as exc:
            self._trip("counters", exc)
            return {}

    async def geo_size(self) -> int | None:
        """How many AVAILABLE couriers the matcher can see in the GEO index (None when Redis is down)."""
        if not self.available():
            return None
        try:
            return int(await self.client.zcard(GEO_KEY))
        except _ERRORS as exc:
            self._trip("geo_size", exc)
            return None

    async def set_json(self, key: str, value: dict, ttl: int = 15) -> None:
        if not self.available():
            return
        try:
            await self.client.set(key, json.dumps(value), ex=ttl)
        except _ERRORS as exc:
            self._trip("set_json", exc)

    async def get_json(self, key: str) -> dict | None:
        if not self.available():
            return None
        try:
            raw = await self.client.get(key)
            return json.loads(raw) if raw else None
        except _ERRORS as exc:
            self._trip("get_json", exc)
            return None


store = RedisStore()
