"""Background worker: SQS consumer + outbox relay + housekeeping loops.

    python -m app.worker

Run as many copies as you like: SKIP LOCKED row locks and idempotent handlers make them safe in parallel.
"""
import asyncio
import logging
import signal
import time

from prometheus_client import start_http_server
from sqlalchemy import select

from . import dispatch, metrics
from .config import get_settings
from .db import SessionLocal, init_db
from .domain import backoff_delay
from .models import Courier
from .orders import expire_stale_assignments, redrive_stuck_orders
from .outbox import backlog, relay_once
from .queue import TRANSIENT, Message, SQSQueue
from .redis_store import store

log = logging.getLogger("dispatchflow.worker")
cfg = get_settings()


async def supervise(name: str, fn, *args) -> None:
    """Restart a loop with backoff if it ever dies; one bad iteration must not take the worker down."""
    delay = 0.5
    while True:
        try:
            await fn(*args)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("loop %s crashed; restarting in %.1fs", name, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)


async def process(queue: SQSQueue, msg: Message) -> None:
    kind = str(msg.body.get("type", "unknown")) if isinstance(msg.body, dict) else "invalid"
    started = time.perf_counter()
    log.debug("recv %s type=%s body=%.120s", msg.message_id[:8], kind, msg.body)
    try:
        outcome = await dispatch.handle(msg.body, msg.receive_count)
    except dispatch.PermanentError as exc:
        metrics.SQS_MESSAGES.labels(kind, "poison").inc()
        log.warning("poison message -> DLQ: %s", exc)
        try:
            await queue.send_dlq(msg.body)
            await queue.delete(msg)
        except TRANSIENT:
            pass  # stays in flight, redelivered, and parked next time
        return
    except Exception as exc:
        # Leave the message in flight: retry sooner than the full visibility timeout, with backoff.
        # After sqs_max_receive_count receives SQS moves it to the DLQ for us.
        metrics.SQS_MESSAGES.labels(kind, "error").inc()
        log.warning("handler failed type=%s attempt=%s: %r", kind, msg.receive_count, exc)
        try:
            await queue.change_visibility(msg, backoff_delay(msg.receive_count, cfg.retry_base_delay, cfg.retry_max_delay))
        except TRANSIENT:
            pass
        return
    finally:
        metrics.SQS_PROCESS_SECONDS.labels(kind).observe(time.perf_counter() - started)

    log.debug("done %s type=%s ack=%s note=%s", msg.message_id[:8], kind, outcome.ack, outcome.note)
    try:
        if outcome.ack:
            await queue.delete(msg)
            metrics.SQS_MESSAGES.labels(kind, "ack").inc()
        else:
            await queue.change_visibility(msg, outcome.delay)
            metrics.SQS_MESSAGES.labels(kind, "retry").inc()
    except TRANSIENT as exc:  # message will simply reappear; handlers are idempotent
        log.warning("could not settle message: %r", exc)


async def consume_loop(queue: SQSQueue, stop: asyncio.Event) -> None:
    tasks: set[asyncio.Task] = set()
    while not stop.is_set():
        free = cfg.worker_concurrency - len(tasks)
        if free <= 0:
            await asyncio.wait(set(tasks), return_when=asyncio.FIRST_COMPLETED)
            continue
        try:
            messages = await queue.receive(free)
        except TRANSIENT as exc:
            log.warning("receive failed (%r); retrying", exc)
            await asyncio.sleep(1.0)
            continue
        for m in messages:
            t = asyncio.create_task(process(queue, m))
            tasks.add(t)
            t.add_done_callback(tasks.discard)
    if tasks:  # graceful shutdown: finish in-flight work
        await asyncio.wait(tasks, timeout=20)


async def outbox_loop(queue: SQSQueue, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            sent = await relay_once(queue)
        except Exception as exc:
            log.warning("outbox relay failed (%r); will retry", exc)
            await asyncio.sleep(1.0)
            continue
        if sent < cfg.outbox_batch_size:  # drained: idle briefly; otherwise go again immediately
            await asyncio.sleep(cfg.outbox_poll_interval)


async def dlq_loop(queue: SQSQueue, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            msgs = await queue.receive(10, wait=2, dlq=True)
            for m in msgs:
                # Fail the affected order (if any) and log the payload, then clear the parking lot.
                # Production tip: alarm on DLQ depth and keep messages for manual redrive instead.
                await dispatch.reap_dead_letter(m.body)
                await queue.delete(m, dlq=True)
                metrics.SQS_MESSAGES.labels(str(m.body.get("type", "unknown")) if isinstance(m.body, dict) else "invalid", "dead_letter").inc()
                await store.incr_counter("dead_lettered")
        except Exception as exc:
            log.warning("dlq reaper error (%r)", exc)
            await asyncio.sleep(2.0)


async def sweeper_loop(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            n = await expire_stale_assignments(cfg.assignment_timeout_seconds)
            if n:
                log.warning("reclaimed %d assignments that were never picked up", n)
            n = await redrive_stuck_orders(cfg.redrive_after_seconds)
            if n:
                log.warning("re-drove %d orders that were waiting with no activity", n)
        except Exception as exc:
            log.warning("assignment sweeper error (%r)", exc)
        await asyncio.sleep(cfg.sweeper_interval)


async def geo_loop(stop: asyncio.Event) -> None:
    """Rebuild the Redis GEO index from PostgreSQL so it self-heals after a Redis restart."""
    while not stop.is_set():
        try:
            async with SessionLocal() as s:
                rows = (await s.execute(select(Courier.id, Courier.lat, Courier.lng).where(Courier.status == "AVAILABLE"))).all()
            await store.rebuild_geo([(r[0], r[1], r[2]) for r in rows])
        except Exception as exc:
            log.warning("geo reconcile failed (%r)", exc)
        await asyncio.sleep(cfg.geo_reconcile_interval)


async def depth_loop(queue: SQSQueue, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            main, dlq = await queue.depth(), await queue.depth(dlq=True)
            for state, v in main.items():
                metrics.QUEUE_DEPTH.labels("main", state).set(v)
            for state, v in dlq.items():
                metrics.QUEUE_DEPTH.labels("dlq", state).set(v)
            ob = await backlog()
            metrics.OUTBOX_BACKLOG.set(ob)
            await store.set_json("df:queue_depth", {"main": main, "dlq": dlq, "outbox": ob, "ts": time.time()}, ttl=15)
        except Exception as exc:
            log.warning("depth poll failed (%r)", exc)
        await asyncio.sleep(cfg.depth_poll_interval)


async def main() -> None:
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    await init_db()
    await store.start()
    queue = SQSQueue()
    await queue.start()
    start_http_server(cfg.worker_metrics_port)
    log.info("worker up: concurrency=%s metrics=:%s", cfg.worker_concurrency, cfg.worker_metrics_port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    runners = [
        asyncio.create_task(supervise("consume", consume_loop, queue, stop)),
        asyncio.create_task(supervise("outbox", outbox_loop, queue, stop)),
        asyncio.create_task(supervise("dlq", dlq_loop, queue, stop)),
        asyncio.create_task(supervise("sweeper", sweeper_loop, stop)),
        asyncio.create_task(supervise("geo", geo_loop, stop)),
        asyncio.create_task(supervise("depth", depth_loop, queue, stop)),
    ]
    await stop.wait()
    log.info("shutting down: draining in-flight messages")
    await asyncio.wait(runners[:1], timeout=25)  # consumer finishes in-flight work
    for t in runners:
        t.cancel()
    await asyncio.gather(*runners, return_exceptions=True)
    await queue.stop()
    await store.stop()


if __name__ == "__main__":
    asyncio.run(main())
