"""Tests for ClusterDetector level logic (Level 1-5 by distinct sources)."""

import time

from src.core.types import AnomalyEvent
from src.analyzers.online.cluster_detector import ClusterDetector


def _anomaly(source: str, ts: float | None = None) -> AnomalyEvent:
    return AnomalyEvent(
        timestamp=ts if ts is not None else time.time(),
        parameter=f"{source}.value",
        value=1.0,
        mean=0.0,
        std=1.0,
        z_score=10.0,
        sensor_source=source,
        metadata={"severity": "high"},
    )


def _feed(detector: ClusterDetector, sources):
    """Feed one anomaly per source (all 'now') and return the last cluster."""
    cluster = None
    now = time.time()
    for s in sources:
        cluster = detector.add_anomaly(_anomaly(s, now))
    return cluster


def test_level_1_single_source():
    d = ClusterDetector(cluster_window_seconds=30.0)
    cluster = _feed(d, ["crypto"])
    assert cluster.level == 1
    assert cluster.probability == 1.0


def test_level_2_two_sources():
    d = ClusterDetector(cluster_window_seconds=30.0)
    cluster = _feed(d, ["crypto", "earthquake"])
    assert cluster.level == 2


def test_level_3_three_sources():
    d = ClusterDetector(cluster_window_seconds=30.0)
    cluster = _feed(d, ["crypto", "earthquake", "space_weather"])
    assert cluster.level == 3


def test_level_4_four_sources():
    d = ClusterDetector(cluster_window_seconds=30.0)
    cluster = _feed(d, ["crypto", "earthquake", "space_weather", "news"])
    assert cluster.level == 4


def test_level_5_five_plus_sources_is_reachable():
    d = ClusterDetector(cluster_window_seconds=30.0)
    cluster = _feed(d, ["crypto", "earthquake", "space_weather", "news", "quantum_rng"])
    assert cluster.level == 5
    assert cluster.description == "Critical synchronicity"
    # six distinct sources still labels as 5 (cap), never 6
    cluster = d.add_anomaly(_anomaly("blockchain"))
    assert cluster.level == 5


def test_duplicate_source_does_not_raise_level():
    d = ClusterDetector(cluster_window_seconds=30.0)
    # Same source three times = still a single distinct source = level 1
    cluster = _feed(d, ["crypto", "crypto", "crypto"])
    assert cluster.level == 1


def test_anomaly_outside_window_is_ignored():
    d = ClusterDetector(cluster_window_seconds=30.0)
    now = time.time()
    # An old crypto anomaly (200s ago) should be cleaned and not counted
    d.add_anomaly(_anomaly("crypto", now - 200))
    cluster = d.add_anomaly(_anomaly("earthquake", now))
    assert cluster.level == 1  # only earthquake is within the window


def test_rarity_indicator_shrinks_with_size():
    d = ClusterDetector(cluster_window_seconds=30.0)
    l2 = _feed(ClusterDetector(30.0), ["a", "b"]).probability
    l3 = _feed(ClusterDetector(30.0), ["a", "b", "c"]).probability
    l4 = _feed(ClusterDetector(30.0), ["a", "b", "c", "d"]).probability
    l5 = _feed(ClusterDetector(30.0), ["a", "b", "c", "d", "e"]).probability
    assert l2 > l3 > l4 > l5
