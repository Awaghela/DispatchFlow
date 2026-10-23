import pytest

from app.domain import Verdict, backoff_delay, classify, haversine_km


@pytest.mark.parametrize(
    "current,target,expected",
    [
        ("ASSIGNED", "PICKED_UP", Verdict.APPLY),
        ("PICKED_UP", "DELIVERED", Verdict.APPLY),
        ("ASSIGNED", "DELIVERED", Verdict.APPLY),  # delayed pickup update: inferred
        ("ASSIGNED", "ASSIGNING", Verdict.APPLY),  # courier rejects
        ("DELIVERED", "DELIVERED", Verdict.NOOP),  # duplicate request
        ("PICKED_UP", "PICKED_UP", Verdict.NOOP),
        ("DELIVERED", "PICKED_UP", Verdict.STALE),  # late pickup after delivery
        ("DELIVERED", "ASSIGNED", Verdict.STALE),
        ("PICKED_UP", "ASSIGNING", Verdict.STALE),  # late rejection after pickup
        ("PICKED_UP", "CANCELLED", Verdict.ILLEGAL),  # too late to cancel
        ("DELIVERED", "CANCELLED", Verdict.ILLEGAL),
        ("CANCELLED", "PICKED_UP", Verdict.ILLEGAL),
        ("CREATED", "DELIVERED", Verdict.ILLEGAL),
        ("CREATED", "CANCELLED", Verdict.APPLY),
        ("ASSIGNING", "FAILED", Verdict.APPLY),
    ],
)
def test_state_machine(current, target, expected):
    assert classify(current, target) is expected


def test_backoff_grows_and_is_capped():
    assert [backoff_delay(a, 1, 30, jitter=False) for a in (1, 2, 3, 4, 5, 6, 7)] == [1, 2, 4, 8, 16, 30, 30]
    for _ in range(200):
        assert 1 <= backoff_delay(4, 1, 30) <= 8  # jitter stays inside [raw/2, raw]


def test_haversine():
    assert haversine_km(37.7749, -122.4194, 37.7749, -122.4194) == 0
    assert 8.5 < haversine_km(37.7749, -122.4194, 37.8044, -122.2712) < 14.5  # SF -> Oakland
