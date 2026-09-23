"""Separate USGS events must survive a persistent last-hour feed and restarts."""

import json

from src.analyzers.online.hybrid_detector import HybridDetector
from src.analyzers.online.cluster_detector import ClusterDetector
from src.core.types import Event, EventType


def reading(ts, quakes):
    return Event(
        timestamp=ts, source="earthquake", event_type=EventType.DATA,
        payload={"max_magnitude": max((q["magnitude"] for q in quakes), default=0),
                 "count": len(quakes), "significant_events": quakes},
    )


def quake(quake_id, magnitude, place):
    return {"id": quake_id, "magnitude": magnitude, "time": 90.0,
            "place": place, "latitude": 1.0, "longitude": 2.0}


def quake_anomalies(detector, event):
    return [a for a in detector.process(event) if (a.metadata or {}).get("usgs_id")]


def test_second_quake_detected_while_first_is_still_in_feed():
    detector = HybridDetector()
    first = quake("usgs-a", 5.2, "A")
    second = quake("usgs-b", 4.9, "B")
    found = quake_anomalies(detector, reading(100, [first]))
    assert [a.metadata["usgs_id"] for a in found] == ["usgs-a"]
    assert found[0].timestamp == 90.0
    detector.mark_persisted(found[0])
    found = quake_anomalies(detector, reading(160, [first, second]))
    assert [a.metadata["usgs_id"] for a in found] == ["usgs-b"]
    assert found[0].metadata["place"] == "B"
    detector.mark_persisted(found[0])
    assert quake_anomalies(detector, reading(220, [first, second])) == []


def test_failed_write_retries_and_restart_restores_identity(tmp_path):
    detector = HybridDetector()
    event = reading(100, [quake("usgs-c", 5.1, "C")])
    assert len(quake_anomalies(detector, event)) == 1
    # No acknowledgement: the persistence step could have failed.
    found = quake_anomalies(detector, event)
    assert len(found) == 1
    log_dir = tmp_path / "anomalies"
    log_dir.mkdir()
    (log_dir / "2026-09-23.jsonl").write_text(json.dumps(found[0].to_dict()) + "\n")
    restarted = HybridDetector(anomaly_log_dir=log_dir)
    assert quake_anomalies(restarted, event) == []


def test_legacy_aggregate_record_still_supported():
    detector = HybridDetector()
    event = Event(timestamp=100, source="earthquake", event_type=EventType.DATA,
                  payload={"max_magnitude": 5.4, "count": 1})
    assert any(a.parameter == "earthquake.max_magnitude" for a in detector.process(event))


def test_multiple_quakes_in_one_poll_are_all_emitted():
    detector = HybridDetector()
    found = quake_anomalies(detector, reading(100, [
        quake("usgs-d", 5.7, "D"), quake("usgs-e", 5.0, "E"),
    ]))
    assert {a.metadata["usgs_id"] for a in found} == {"usgs-d", "usgs-e"}


def test_backfilled_quake_cannot_create_current_cluster():
    detector = HybridDetector()
    cluster = ClusterDetector(cluster_window_seconds=30)
    current = detector.process(Event(timestamp=1_000, source="space_weather",
        event_type=EventType.DATA, payload={"kp_index": 6}))
    assert current
    cluster.add_anomaly(current[0], now=1_000)
    old_quake = quake_anomalies(detector, reading(1_005, [quake("usgs-old", 5.1, "Old")]))[0]
    assert old_quake.timestamp == 90
    assert cluster.add_anomaly(old_quake, now=1_005) is None
