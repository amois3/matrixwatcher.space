"""Current collector failures must not be masked by a fresh previous poll."""

from src.monitoring.coverage import merge_collector_health


def test_failed_current_poll_downgrades_fresh_source():
    raw = {
        "sensors": {"quantum_rng": {"status": "ok", "issues": [], "age_seconds": 120}},
        "summary": {"ok": 1},
        "pipeline": {"status": "ok"},
    }
    live = {"status": "degraded", "sensors": {"quantum_rng": {"status": "error"}}}
    merged = merge_collector_health(raw, live)

    assert raw["sensors"]["quantum_rng"]["status"] == "ok"
    assert merged["sensors"]["quantum_rng"]["status"] == "partial"
    assert merged["sensors"]["quantum_rng"]["age_seconds"] == 120
    assert "error" in merged["sensors"]["quantum_rng"]["issues"][0]
    assert merged["summary"]["ok"] == 0
    assert merged["summary"]["partial"] == 1


def test_missing_health_is_explicitly_unknown():
    raw = {"sensors": {"earthquake": {"status": "ok", "issues": []}}, "summary": {"ok": 1}}
    merged = merge_collector_health(raw, None)

    assert merged["collector"]["status"] == "unavailable"
    assert merged["sensors"]["earthquake"]["status"] == "ok"
