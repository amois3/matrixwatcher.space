"""Tests for the daily digest generator (honest, data-driven narrative)."""

from src.analyzers.online.digest_generator import build_digest, date_str_utc


def _cluster(level, sources, index=20.0, status="elevated", ts=1779900000.0):
    return {
        "timestamp": ts,
        "cluster": {
            "level": level,
            "anomalies": [{"sensor_source": s} for s in sources],
        },
        "index": {"value": index, "status": status},
    }


def test_quiet_day_when_no_clusters():
    d = build_digest("2026-05-27", clusters=[], predictions=[], generated_at=1.0)
    assert d["clusters_total"] == 0
    assert d["highest_level"] == 0
    assert "slow day" in d["narrative"].lower()


def test_level_2_clusters_are_not_counted():
    # Only L3+ count as multi-domain clusters
    clusters = [_cluster(2, ["crypto", "news"])]
    d = build_digest("2026-05-27", clusters, predictions=[], generated_at=1.0)
    assert d["clusters_total"] == 0


def test_counts_and_peak_and_sources():
    clusters = [
        _cluster(3, ["crypto", "news", "earthquake"], index=40.0, status="high"),
        _cluster(3, ["crypto", "quantum_rng", "space_weather"], index=30.0),
        _cluster(5, ["crypto", "news", "earthquake", "quantum_rng", "space_weather"], index=85.0, status="critical"),
    ]
    d = build_digest("2026-05-27", clusters, predictions=[], generated_at=1.0)

    assert d["clusters_total"] == 3
    assert d["by_level"] == {"3": 2, "4": 0, "5": 1}
    assert d["highest_level"] == 5
    assert set(d["highest_level_sources"]) == {"crypto", "news", "earthquake", "quantum_rng", "space_weather"}
    assert d["peak_index"] == 85.0
    assert d["peak_status"] == "critical"
    # crypto appears in all three clusters -> most active
    assert d["most_active_source"] == "crypto"
    # narrative names the sources of the strongest moment in plain words
    assert "strongest" in d["narrative"].lower()


def test_top_prediction_selected_by_probability():
    preds = [
        {"event": "btc_pump_1h", "description": "BTC surge", "probability": 40, "observations": 12, "avg_time_hours": 3.0},
        {"event": "btc_dump_4h", "description": "BTC drop", "probability": 70, "observations": 8, "avg_time_hours": 5.0},
    ]
    d = build_digest("2026-05-27", clusters=[], predictions=preds, generated_at=1.0)
    assert d["predictions_count"] == 2
    assert d["top_prediction"]["event"] == "btc_dump_4h"
    assert d["top_prediction"]["probability"] == 70
    # The digest narrative no longer makes a prediction itself (that lives in
    # the Predictions panel); it just describes the day's observations.


def test_date_str_utc_format():
    assert date_str_utc(0) == "1970-01-01"
