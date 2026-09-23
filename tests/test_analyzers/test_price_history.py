"""Proof-of-equivalence tests for the bisect-based ``_PriceHistory``.

The optimisation must give the EXACT same answer as the previous linear walk.
We prove this by re-implementing the old algorithm here and asserting equality
across random data, edge cases, and the awkward non-monotonic load order that
``_load_price_history`` produces.
"""

import random
from collections import deque

import pytest

from src.analyzers.online.historical_pattern_tracker import _PriceHistory


# --- reference: the original linear scan, kept verbatim ---------------------

def _old_find(entries: list[dict], target_time: float) -> float | None:
    """Pre-optimisation algorithm copied verbatim from _check_crypto_move."""
    old_price = None
    for entry in entries:
        if entry["timestamp"] <= target_time:
            old_price = entry["price"]
        elif old_price is not None:
            break
    return old_price


def _build_both(entries: list[tuple[float, float]]) -> tuple[list[dict], _PriceHistory]:
    """Build the legacy dict-list and a _PriceHistory from the same sequence."""
    legacy = [{"timestamp": ts, "price": p} for ts, p in entries]
    ph = _PriceHistory()
    for ts, p in entries:
        ph.append(ts, p)
    return legacy, ph


# ---------------------------------------------------------------------------


def test_empty_history():
    ph = _PriceHistory()
    assert ph.find_price_at_or_before(100.0) is None
    assert len(ph) == 0


def test_single_entry_target_before():
    legacy, ph = _build_both([(100.0, 60_000.0)])
    target = 50.0
    # Target predates the only entry -> both must return None.
    assert ph.find_price_at_or_before(target) is None
    assert _old_find(legacy, target) is None


def test_single_entry_target_equal_and_after():
    legacy, ph = _build_both([(100.0, 60_000.0)])
    for target in (100.0, 150.0):
        assert ph.find_price_at_or_before(target) == _old_find(legacy, target)


def test_monotonic_series_matches_linear_scan_at_every_target():
    rng = random.Random(7)
    ts = 0.0
    entries: list[tuple[float, float]] = []
    price = 60_000.0
    for _ in range(500):
        ts += rng.uniform(1, 30)  # strictly increasing
        price += rng.uniform(-50, 50)
        entries.append((ts, price))

    legacy, ph = _build_both(entries)
    last_ts = entries[-1][0]
    first_ts = entries[0][0]

    # Sample targets across the range AND just outside it
    targets = (
        [first_ts - 1, first_ts, last_ts, last_ts + 1]
        + [rng.uniform(first_ts, last_ts) for _ in range(200)]
        + [entries[i][0] for i in range(0, len(entries), 17)]   # exact-match edges
    )
    for t in targets:
        assert ph.find_price_at_or_before(t) == _old_find(legacy, t), f"mismatch at target={t}"


def test_non_monotonic_load_then_sort_gives_chronologically_correct_answer():
    """``_load_price_history`` walks daily files newest-first, so the legacy
    deque is non-monotonic until trimmed by live appends. The old linear scan
    actually returned the WRONG answer in that window: it would walk past
    yesterday's correct entries into day-before-yesterday's data and overwrite
    ``old_price`` with the older (and chronologically *later in iteration*) value.

    After ``sort()`` the bisect implementation returns the chronologically correct
    answer — the latest entry with ts <= target_time — which we verify against
    a SORTED reference (the right answer, not the buggy unsorted scan).
    """
    rng = random.Random(13)
    def make_day(t0: float, n: int) -> list[tuple[float, float]]:
        out = []
        ts = t0
        price = 60_000.0
        for _ in range(n):
            ts += rng.uniform(1, 30)
            price += rng.uniform(-20, 20)
            out.append((ts, price))
        return out

    day_today = make_day(2_000_000.0, 100)
    day_yesterday = make_day(1_900_000.0, 100)
    day_before = make_day(1_800_000.0, 100)
    out_of_order = day_today + day_yesterday + day_before  # mirrors _load_price_history

    ph = _PriceHistory()
    for ts, p in out_of_order:
        ph.append(ts, p)
    ph.sort()

    sorted_entries = [{"timestamp": ts, "price": p} for ts, p in sorted(out_of_order)]
    for target in [
        1_750_000.0,   # before everything
        1_850_000.0,   # in day_before
        1_950_000.0,   # in day_yesterday
        2_050_000.0,   # in day_today
        2_500_000.0,   # after everything
    ]:
        expected = _old_find(sorted_entries, target)
        assert ph.find_price_at_or_before(target) == expected, \
            f"mismatch at target={target}: bisect={ph.find_price_at_or_before(target)} expected={expected}"


def test_duplicate_timestamps_pick_latest_appended():
    """When several entries share the same timestamp, both implementations must
    return the one whose price was *appended last* (the legacy walk overwrites
    ``old_price`` as it goes; bisect_right hits the rightmost of equal keys)."""
    entries = [(100.0, 1.0), (100.0, 2.0), (100.0, 3.0), (200.0, 4.0)]
    legacy, ph = _build_both(entries)
    assert ph.find_price_at_or_before(100.0) == _old_find(legacy, 100.0) == 3.0
    assert ph.find_price_at_or_before(150.0) == _old_find(legacy, 150.0) == 3.0
    assert ph.find_price_at_or_before(200.0) == _old_find(legacy, 200.0) == 4.0


def test_maxlen_keeps_recent_window_intact():
    ph = _PriceHistory(maxlen=100)
    for i in range(500):
        ph.append(float(i), float(i * 10))
    # Length cap is amortised (only trims past 2*maxlen) but must never exceed it.
    assert len(ph) <= 200
    # The newest entry must still be the answer for any target >= last ts
    assert ph.find_price_at_or_before(499.0) == 4990.0
    # Anything older than the oldest retained entry returns None
    assert ph.find_price_at_or_before(-1.0) is None


def test_clear_resets_history():
    ph = _PriceHistory()
    ph.append(1.0, 1.0); ph.append(2.0, 2.0)
    ph.clear()
    assert len(ph) == 0
    assert ph.find_price_at_or_before(5.0) is None
