"""WebSocket fan-out.

Event path:    service commits -> Redis PUBLISH -> every API instance's listener -> matching sockets.

Two safety nets make the stream robust:
  * DB tailer  - if Redis pub/sub is down, each API instance tails the order_events table every
                 ~1s, so clients keep receiving (delayed) updates. De-duplicated by event id.
  * Replay     - a reconnecting client passes ?last_event_id=N and gets everything it missed.
Clients are expected to de-duplicate by event id (the dashboard does).
"""
import asyncio
import json
import logging
from collections import OrderedDict

import redis.asyncio as aioredis
from sqlalchemy import select

from . import metrics
from .config import get_settings
from .db import SessionLocal
from .models import OrderEvent
from .redis_store import EVENTS_CHANNEL
from .serializers import event_to_dict

log = logging.getLogger("dispatchflow.hub")

ROLES = ("ops", "merchant", "consumer", "order", "courier")


class Subscription:
    __slots__ = ("role", "key", "queue", "dropped")

    def __init__(self, role: str, key: str | None, size: int) -> None:
        self.role, self.key = role, key
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=size)
        self.dropped = 0

    def matches(self, e: dict) -> bool:
        r = self.role
        if r == "ops":
            return True
        if r == "merchant":
            return e.get("merchant_id") == self.key
        if r == "consumer":
            return e.get("consumer_id") == self.key
        if r == "order":
            return e.get("order_id") == self.key
        if r == "courier":
            return e.get("courier_id") == self.key
        return False


class Hub:
    def __init__(self) -> None:
        self.s = get_settings()
        self._subs: set[Subscription] = set()
        self._seen: OrderedDict[int, None] = OrderedDict()
        self._tasks: list[asyncio.Task] = []
        self._cursor = 0

    # ---- subscriptions ---------------------------------------------------------------------
    def subscribe(self, role: str, key: str | None) -> Subscription:
        sub = Subscription(role, key, self.s.ws_queue_size)
        self._subs.add(sub)
        metrics.WS_CONNECTIONS.labels(role).inc()
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subs:
            self._subs.discard(sub)
            metrics.WS_CONNECTIONS.labels(sub.role).dec()

    @property
    def connections(self) -> int:
        return len(self._subs)

    # ---- delivery --------------------------------------------------------------------------
    def dispatch(self, e: dict, text: str | None = None) -> None:
        eid = e.get("id")
        if eid is not None:  # persisted event: drop duplicates (pub/sub + tailer + replay overlap)
            if eid in self._seen:
                return
            self._seen[eid] = None
            if len(self._seen) > 50_000:
                self._seen.popitem(last=False)
        if not self._subs:
            return
        text = text or json.dumps(e, separators=(",", ":"))
        for sub in tuple(self._subs):
            if sub.matches(e):
                try:
                    sub.queue.put_nowait(text)
                except asyncio.QueueFull:
                    sub.dropped += 1  # slow consumer: tell it to resync instead of blocking everyone
                    metrics.WS_MESSAGES.labels("dropped").inc()

    # ---- replay ----------------------------------------------------------------------------
    async def replay(self, sub: Subscription, last_event_id: int) -> list[dict]:
        stmt = select(OrderEvent).where(OrderEvent.id > max(0, last_event_id - self.s.replay_overlap))
        if sub.role == "merchant":
            stmt = stmt.where(OrderEvent.merchant_id == sub.key)
        elif sub.role == "consumer":
            stmt = stmt.where(OrderEvent.consumer_id == sub.key)
        elif sub.role == "order":
            stmt = stmt.where(OrderEvent.order_id == sub.key)
        elif sub.role == "courier":
            stmt = stmt.where(OrderEvent.courier_id == sub.key)
        stmt = stmt.order_by(OrderEvent.id).limit(self.s.replay_limit)
        async with SessionLocal() as s:
            return [event_to_dict(e) for e in (await s.execute(stmt)).scalars()]

    # ---- background tasks ------------------------------------------------------------------
    async def start(self) -> None:
        if self.s.redis_enabled:
            self._tasks.append(asyncio.create_task(self._listen(), name="hub-listen"))
        self._tasks.append(asyncio.create_task(self._tail(), name="hub-tail"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _listen(self) -> None:
        while True:
            client = None
            try:
                client = aioredis.from_url(
                    self.s.redis_url, decode_responses=True, socket_connect_timeout=1, health_check_interval=10
                )
                pubsub = client.pubsub()
                await pubsub.subscribe(EVENTS_CHANNEL)
                log.info("subscribed to %s", EVENTS_CHANNEL)
                while True:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg and msg.get("type") == "message":
                        self.dispatch(json.loads(msg["data"]), text=msg["data"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # reconnect forever; the tailer covers the gap
                log.warning("pubsub listener lost (%s); reconnecting", exc)
                await asyncio.sleep(1.0)
            finally:
                if client is not None:
                    try:
                        await client.aclose()
                    except Exception:
                        pass

    async def _tail(self) -> None:
        """Polls the event log. Normally redundant with pub/sub (everything is deduped by id);
        it is what keeps WebSockets live while Redis is down."""
        while True:
            try:
                async with SessionLocal() as s:
                    if self._cursor == 0:
                        from sqlalchemy import func

                        self._cursor = (await s.execute(select(func.coalesce(func.max(OrderEvent.id), 0)))).scalar_one()
                    rows = (
                        await s.execute(
                            select(OrderEvent)
                            .where(OrderEvent.id > self._cursor - self.s.replay_overlap)
                            .order_by(OrderEvent.id)
                            .limit(1000)
                        )
                    ).scalars().all()
                for row in rows:
                    self._cursor = max(self._cursor, row.id)
                    if self._subs:
                        self.dispatch(event_to_dict(row))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("event tailer error: %s", exc)
            await asyncio.sleep(self.s.tailer_interval)


hub = Hub()
