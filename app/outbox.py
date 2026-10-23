"""Transactional-outbox relay: moves rows written alongside state changes onto SQS.

At-least-once by construction (a crash after SQS accepts but before we mark the row published
re-sends it), which is fine because every consumer is idempotent.
"""
import logging

from sqlalchemy import func, select

from . import metrics
from .config import get_settings
from .db import SessionLocal
from .models import Outbox, utcnow
from .queue import SQSQueue

log = logging.getLogger("dispatchflow.outbox")


async def relay_once(queue: SQSQueue) -> int:
    cfg = get_settings()
    async with SessionLocal() as s, s.begin():
        rows = (
            await s.execute(
                select(Outbox)
                .where(Outbox.published_at.is_(None))
                .order_by(Outbox.id)
                .limit(cfg.outbox_batch_size)
                .with_for_update(skip_locked=True)  # several relays can run without double-sending
            )
        ).scalars().all()
        if not rows:
            return 0
        await queue.send_batch([(r.id, r.payload) for r in rows])  # raises -> rollback -> retried
        now = utcnow()
        for r in rows:
            r.published_at = now
    metrics.OUTBOX_PUBLISHED.inc(len(rows))
    return len(rows)


async def backlog() -> int:
    async with SessionLocal() as s:
        return (await s.execute(select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None)))).scalar_one()
