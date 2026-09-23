"""Tests for the shuffle test — it must distinguish real alignment from chance."""

import random

from src.analyzers.offline.shuffle_test import (
    extract_anomalies,
    count_l3plus,
    circular_shift,
    run_shuffle_test,
)


def test_extract_skips_cluster_records():
    records = [
        {"timestamp": 1.0, "sensor_source": "crypto"},
        {"timestamp": 2.0, "cluster": {"level": 3}},  # summary record -> skip
        {"timestamp": 3.0, "sensor_source": "news"},
        {"foo": "bar"},  # malformed -> skip
    ]
    events = extract_anomalies(records)
    assert events == [(1.0, "crypto"), (3.0, "news")]


def test_count_l3plus_basic():
    # three distinct sources within a 30s window -> L3 moment on the 3rd event
    events = [(0.0, "a"), (5.0, "b"), (10.0, "c"), (100.0, "a")]
    l3, by_level = count_l3plus(events, window=30.0)
    assert l3 == 1                 # only the moment at t=10 had 3 sources
    assert by_level[1] >= 2        # the lone events count as level 1


def test_count_l3plus_window_expiry():
    # spread beyond the window -> never 3 simultaneous
    events = [(0.0, "a"), (40.0, "b"), (80.0, "c")]
    l3, _ = count_l3plus(events, window=30.0)
    assert l3 == 0


def test_circular_shift_preserves_counts():
    events = [(t, "a") for t in range(0, 100, 10)] + [(t + 3, "b") for t in range(0, 100, 10)]
    events.sort()
    shifted = circular_shift(events, random.Random(1))
    assert len(shifted) == len(events)
    # per-source counts preserved
    from collections import Counter
    assert Counter(s for _, s in shifted) == Counter(s for _, s in events)
    # all timestamps stay within the original span
    lo, hi = events[0][0], events[-1][0]
    assert all(lo <= t <= hi for t, _ in shifted)


def test_detects_real_alignment_as_signal():
    # Three sources firing at the SAME instants = perfect synchronization.
    instants = [i * 600.0 for i in range(60)]  # every 10 min over 10h
    events = sorted([(t, s) for t in instants for s in ("a", "b", "c")])
    report = run_shuffle_test(events, window=30.0, n_iter=200, seed=7)
    assert report["observed_l3plus"] > report["null_mean"]
    assert report["p_value"] < 0.05
    assert "exceeds chance" in report["verdict"]


def test_independent_streams_are_consistent_with_chance():
    # Three independent random streams over the same span -> no real sync.
    rng = random.Random(42)
    span = 10 * 3600.0
    events = []
    for s in ("a", "b", "c"):
        for _ in range(200):
            events.append((rng.uniform(0, span), s))
    events.sort()
    report = run_shuffle_test(events, window=30.0, n_iter=200, seed=7)
    # observed should sit inside the null distribution, not far above it
    assert report["p_value"] > 0.05
    assert "exceeds chance" not in report["verdict"]


# -------- Schedule-aware null tests ----------------------------------------


def test_schedule_aware_shift_keeps_each_source_on_its_polling_grid():
    """For a 300s-period source, every shifted event must still land at a
    300s offset from t_min — proving the shift stays on the sampling grid."""
    from src.analyzers.offline.shuffle_test import schedule_aware_shift

    # source 'A' polls every 300s, originally at 0, 300, 600, ..., 30*300
    times_a = [float(i * 300) for i in range(31)]
    events = [(t, "A") for t in times_a]

    shifted = schedule_aware_shift(events, random.Random(11), periods={"A": 300.0})
    # Each (new_ts - t_min) must be a multiple of 300 (within float tolerance)
    t_min = events[0][0]
    for new_ts, _ in shifted:
        offset = (new_ts - t_min) % 300.0
        assert offset < 1e-6 or 300.0 - offset < 1e-6, f"off-grid: {new_ts}"


def test_schedule_aware_preserves_two_locked_sources():
    """Two sources sharing a 300s period and starting in lockstep should
    remain in lockstep under schedule-aware shift, because both can only
    be shifted by integer multiples of the shared period."""
    from src.analyzers.offline.shuffle_test import schedule_aware_shift, count_l3plus

    # A and B each fire 30 times at the same 300s instants — they "coincide"
    # within any sane cluster window in every cycle. (Add a 3rd lone source
    # at offset 5s so each instant becomes L3.)
    base = [float(i * 300) for i in range(30)]
    events = sorted(
        [(t, "A") for t in base] +
        [(t, "B") for t in base] +
        [(t + 5.0, "C") for t in base]
    )
    rng = random.Random(99)
    observed, _ = count_l3plus(events, window=30.0)

    # circular shift breaks lockstep -> null L3+ should drop sharply
    from src.analyzers.offline.shuffle_test import circular_shift
    cir_l3 = [
        count_l3plus(circular_shift(events, rng), window=30.0)[0]
        for _ in range(50)
    ]
    # schedule-aware shift preserves lockstep -> null L3+ should stay near observed
    sa_l3 = [
        count_l3plus(schedule_aware_shift(events, rng, periods={"A": 300.0, "B": 300.0, "C": 300.0}), window=30.0)[0]
        for _ in range(50)
    ]
    cir_mean = sum(cir_l3) / len(cir_l3)
    sa_mean = sum(sa_l3) / len(sa_l3)
    assert sa_mean > cir_mean * 1.5, (
        "schedule-aware null should preserve locked-source coincidences "
        f"(observed={observed}, schedule_aware_mean={sa_mean}, circular_mean={cir_mean})"
    )


def test_schedule_aware_null_strips_scheduling_artefact():
    """If the apparent excess L3+ is *purely* scheduling, the schedule-aware
    null should reproduce it and the test should NOT flag a signal."""
    from src.analyzers.offline.shuffle_test import run_shuffle_test, SENSOR_PERIODS

    # Two sources both polled every 300s, perfectly synchronised.
    # circular-shift null treats them as independent -> looks like signal;
    # schedule-aware null preserves the lockstep -> no signal flagged.
    base = [float(i * 300) for i in range(40)]
    events = sorted([(t, "quantum_rng") for t in base] + [(t, "space_weather") for t in base])

    cir_report = run_shuffle_test(events, window=30.0, n_iter=200, seed=7, null_mode="circular")
    sa_report = run_shuffle_test(events, window=30.0, n_iter=200, seed=7, null_mode="schedule_aware")

    assert cir_report["observed_l3plus"] == sa_report["observed_l3plus"]
    # schedule-aware null must mostly match the observed count
    # (the lockstep is preserved -> null = observed almost always)
    assert sa_report["null_mean"] >= cir_report["null_mean"]
    assert "exceeds chance" not in sa_report["verdict"]
