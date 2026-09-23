import json
from datetime import datetime, timezone

import pytest

from src.analyzers.offline.lag_lab import (
    PROSPECTIVE_END, PROSPECTIVE_START, analyze, match_count,
    prospective_status, source_episodes,
)
from src.analyzers.offline.quality_atlas import build_atlas, scan_day
from src.sensors.fireball_sensor import parse_fireballs


def test_atlas_marks_partial_readings_as_unobserved(tmp_path):
    start = datetime(2026, 9, 22, tzinfo=timezone.utc).timestamp()
    path = tmp_path / "crypto" / "2026-09-22.jsonl"
    path.parent.mkdir()
    records = [
        {"timestamp": start + 60, "pairs": [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]},
        {"timestamp": start + 120, "pairs": [{"symbol": "BTCUSDT"}]},
    ]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\nINVALID\n")
    row = scan_day(path, "crypto", 60, start)
    assert (row["count"], row["partial"], row["invalid"]) == (2, 1, 1)
    assert row["poll_coverage"] == round(1 / 1440, 4)
    assert row["sampling_opportunity"]["30"] == round(30 / 86400, 4)
    report = build_atlas(tmp_path, {"sensors": {"crypto": {"interval_seconds": 60}}},
                         days=1, now=start + 86400)
    assert report["sensors"]["crypto"]["partial_records"] == 1


def test_fireball_schema_units_and_missing_location():
    payload = {
        "signature": {"version": "1.2"}, "count": 2,
        "fields": ["date", "lat", "lat-dir", "lon", "lon-dir", "energy", "impact-e"],
        "data": [
            ["2026-09-15 01:02:03", "8", "S", "52", "W", "2.3", "0.082"],
            ["2026-09-14 01:02:03", None, None, None, None, "1", "0.04"],
        ],
    }
    events = parse_fireballs(payload)
    assert events[0]["latitude"] == -8 and events[0]["longitude"] == -52
    assert events[0]["estimated_impact_kt"] == 0.082
    assert events[1]["latitude"] is None
    with pytest.raises(ValueError):
        parse_fireballs({**payload, "signature": {"version": "2.0"}})


def test_lag_episode_collapse_direction_and_multiple_testing():
    base = datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp()
    records = [
        {"sensor_source": "earthquake", "timestamp": base + 10},
        {"sensor_source": "earthquake", "timestamp": base + 100},
        {"sensor_source": "earthquake", "timestamp": base + 1900},
        {"sensor_source": "news", "timestamp": base + 3500},
        {"sensor_source": "quantum_rng", "timestamp": base + 4000},
    ]
    episodes = source_episodes(records, after=base, verified_quantum=set())
    assert len(episodes["earthquake"]) == 1
    assert "quantum_rng" not in episodes
    assert match_count(episodes["earthquake"], episodes["news"], 300, 3600) == 1
    assert match_count(episodes["news"], episodes["earthquake"], 300, 3600) == 0
    report = analyze(episodes, iterations=10)
    assert len(report["tests"]) == 12
    assert all(item["p_holm"] >= item["p_value"] for item in report["tests"])


def test_prospective_study_has_no_interim_p_and_freezes_final(tmp_path):
    final = tmp_path / "prospective-final.json"
    records = [
        {"sensor_source": "earthquake", "timestamp": PROSPECTIVE_START + 100},
        {"sensor_source": "crypto", "timestamp": PROSPECTIVE_START + 800},
    ]
    interim = prospective_status(records, set(), PROSPECTIVE_START + 30 * 86400, final, 10)
    assert interim["status"] == "collecting_no_interim_test"
    assert "analysis" not in interim and not final.exists()
    completed = prospective_status(records, set(), PROSPECTIVE_END, final, 10)
    assert completed["status"] == "prospective_screen_completed_not_replication"
    assert final.exists()
    assert prospective_status([], set(), PROSPECTIVE_END + 86400, final, 10) == completed
