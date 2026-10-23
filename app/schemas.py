from datetime import datetime

from pydantic import BaseModel, Field


class Point(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)


class OrderCreate(BaseModel):
    merchant_id: str = Field(min_length=1, max_length=64, examples=["merchant-07"])
    consumer_id: str = Field(min_length=1, max_length=64, examples=["consumer-1042"])
    pickup: Point
    dropoff: Point


class OrderOut(BaseModel):
    id: str
    merchant_id: str
    consumer_id: str
    status: str
    courier_id: str | None
    version: int
    pickup: Point
    dropoff: Point
    assignment_attempts: int
    reassignments: int
    failure_reason: str | None
    assignment_latency_ms: float | None
    created_at: str | None
    assigned_at: str | None
    picked_up_at: str | None
    delivered_at: str | None
    updated_at: str | None


class CourierAction(BaseModel):
    courier_id: str
    reason: str | None = None
    expected_version: int | None = None


class CancelIn(BaseModel):
    reason: str | None = None
    expected_version: int | None = None


class TransitionOut(BaseModel):
    applied: bool
    outcome: str = Field(description="applied | duplicate | stale | stale_courier")
    order: OrderOut


class CourierCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    online: bool = False


class CourierOut(BaseModel):
    id: str
    name: str
    status: str
    lat: float
    lng: float
    active_order_id: str | None
    location_ts: str | None


class LocationIn(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    ts: datetime | None = Field(default=None, description="client timestamp; older-than-stored pings are ignored")


class CourierStatusIn(BaseModel):
    status: str = Field(pattern="^(AVAILABLE|OFFLINE)$")
