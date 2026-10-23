"""Pure domain logic: statuses, the order state machine and small helpers.
No I/O here so it is trivially unit-testable."""
import math
import random
import uuid
from enum import StrEnum


class OrderStatus(StrEnum):
    CREATED = "CREATED"
    ASSIGNING = "ASSIGNING"
    ASSIGNED = "ASSIGNED"
    PICKED_UP = "PICKED_UP"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class CourierStatus(StrEnum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    BUSY = "BUSY"


S = OrderStatus

TERMINAL = frozenset({S.DELIVERED, S.CANCELLED, S.FAILED})
ACTIVE = frozenset({S.CREATED, S.ASSIGNING, S.ASSIGNED, S.PICKED_UP})

# current -> targets that may legally be applied
_ALLOWED: dict[S, frozenset[S]] = {
    S.CREATED: frozenset({S.ASSIGNING, S.CANCELLED}),
    S.ASSIGNING: frozenset({S.ASSIGNED, S.FAILED, S.CANCELLED}),
    # ASSIGNED -> DELIVERED is the "late pickup" case: the pickup update was delayed
    # or lost, so we infer it instead of rejecting a legitimate delivery.
    S.ASSIGNED: frozenset({S.PICKED_UP, S.DELIVERED, S.ASSIGNING, S.CANCELLED}),
    S.PICKED_UP: frozenset({S.DELIVERED}),
}

# progress along the happy path, used to tell "late/duplicate" from "illegal"
_RANK = {S.CREATED: 0, S.ASSIGNING: 1, S.ASSIGNED: 2, S.PICKED_UP: 3, S.DELIVERED: 4}


class Verdict(StrEnum):
    APPLY = "apply"  # perform the transition
    NOOP = "noop"  # already there: an idempotent replay
    STALE = "stale"  # behind the order's progress: a delayed / out-of-order update
    ILLEGAL = "illegal"  # conflicts with the current state


def classify(current: str, target: str) -> Verdict:
    cur, tgt = S(current), S(target)
    if cur == tgt:
        return Verdict.NOOP
    if tgt in _ALLOWED.get(cur, frozenset()):
        return Verdict.APPLY
    if cur in _RANK and tgt in _RANK and _RANK[tgt] < _RANK[cur]:
        return Verdict.STALE
    return Verdict.ILLEGAL


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dl = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def backoff_delay(attempt: int, base: int, cap: int, jitter: bool = True) -> int:
    """Whole seconds (SQS visibility is integer seconds): exponential with equal jitter."""
    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    if not jitter or raw <= 1:
        return max(1, int(raw))
    return max(1, int(raw / 2 + random.random() * raw / 2))


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"
