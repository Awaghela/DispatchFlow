from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


TS = DateTime(timezone=True)


class Courier(Base):
    __tablename__ = "couriers"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(16), default="OFFLINE")
    lat: Mapped[float] = mapped_column(Float)
    lng: Mapped[float] = mapped_column(Float)
    location_ts: Mapped[datetime | None] = mapped_column(TS, nullable=True)
    active_order_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TS, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(TS, default=utcnow, onupdate=utcnow)

    __table_args__ = (Index("ix_couriers_status", "status"),)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    # client-supplied Idempotency-Key; the UNIQUE index is what makes POST /orders safe to retry
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True)
    merchant_id: Mapped[str] = mapped_column(String(64))
    consumer_id: Mapped[str] = mapped_column(String(64))
    pickup_lat: Mapped[float] = mapped_column(Float)
    pickup_lng: Mapped[float] = mapped_column(Float)
    dropoff_lat: Mapped[float] = mapped_column(Float)
    dropoff_lng: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16), default="CREATED")
    courier_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    excluded_courier_ids: Mapped[list] = mapped_column(JSONB, default=list)  # couriers that rejected it
    assignment_attempts: Mapped[int] = mapped_column(Integer, default=0)
    reassignments: Mapped[int] = mapped_column(Integer, default=0)
    failure_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)  # bumped on every change
    created_at: Mapped[datetime] = mapped_column(TS, default=utcnow)
    assigned_at: Mapped[datetime | None] = mapped_column(TS, nullable=True)
    picked_up_at: Mapped[datetime | None] = mapped_column(TS, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(TS, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TS, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index("ix_orders_status_created", "status", "created_at"),
        Index("ix_orders_merchant_created", "merchant_id", "created_at"),
        Index("ix_orders_consumer", "consumer_id"),
        Index("ix_orders_courier", "courier_id"),
        Index("ix_orders_created", "created_at"),
        Index("ix_orders_delivered", "delivered_at"),
    )


class OrderEvent(Base):
    """Append-only log of business events. Serves as audit trail, WebSocket replay source
    and the fallback feed when Redis pub/sub is unavailable."""

    __tablename__ = "order_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(40))
    type: Mapped[str] = mapped_column(String(40))
    merchant_id: Mapped[str] = mapped_column(String(64))
    consumer_id: Mapped[str] = mapped_column(String(64))
    courier_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    # "<order>:<type>:<version>" -- a database-level backstop against double-recording a transition
    dedupe_key: Mapped[str | None] = mapped_column(String(160), nullable=True, unique=True)
    created_at: Mapped[datetime] = mapped_column(TS, default=utcnow)

    __table_args__ = (
        Index("ix_events_order", "order_id", "id"),
        Index("ix_events_merchant", "merchant_id", "id"),
        Index("ix_events_type", "type"),
    )


class Outbox(Base):
    """Transactional outbox: a message is written in the same transaction as the state change,
    then relayed to SQS. A crash between "commit" and "send" can no longer lose work."""

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(40))
    payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(TS, default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(TS, nullable=True)

    __table_args__ = (
        Index("ix_outbox_unpublished", "id", postgresql_where=text("published_at IS NULL")),
    )
