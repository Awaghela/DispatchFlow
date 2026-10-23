from .models import Courier, Order, OrderEvent


def _iso(dt):
    return dt.isoformat() if dt else None


def order_to_dict(o: Order) -> dict:
    latency = None
    if o.assigned_at and o.created_at:
        latency = round((o.assigned_at - o.created_at).total_seconds() * 1000, 1)
    return {
        "id": o.id,
        "merchant_id": o.merchant_id,
        "consumer_id": o.consumer_id,
        "status": o.status,
        "courier_id": o.courier_id,
        "version": o.version,
        "pickup": {"lat": o.pickup_lat, "lng": o.pickup_lng},
        "dropoff": {"lat": o.dropoff_lat, "lng": o.dropoff_lng},
        "assignment_attempts": o.assignment_attempts,
        "reassignments": o.reassignments,
        "failure_reason": o.failure_reason,
        "assignment_latency_ms": latency,
        "created_at": _iso(o.created_at),
        "assigned_at": _iso(o.assigned_at),
        "picked_up_at": _iso(o.picked_up_at),
        "delivered_at": _iso(o.delivered_at),
        "updated_at": _iso(o.updated_at),
    }


def courier_to_dict(c: Courier) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "status": c.status,
        "lat": c.lat,
        "lng": c.lng,
        "active_order_id": c.active_order_id,
        "location_ts": _iso(c.location_ts),
    }


def event_to_dict(e: OrderEvent) -> dict:
    return {
        "id": e.id,
        "type": e.type,
        "ts": _iso(e.created_at),
        "order_id": e.order_id,
        "merchant_id": e.merchant_id,
        "consumer_id": e.consumer_id,
        "courier_id": e.courier_id,
        "payload": e.payload or {},
    }
