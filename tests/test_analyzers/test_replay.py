"""Tests for the offline replay tool.

The pipeline must:
  - read raw sensor JSONL chronologically across sensors;
  - apply the same hybrid detector as live and produce anomaly + cluster records;
  - use *event* timestamps as the clock, so historical data is windowed correctly;
  - write daily anomaly files and a fresh patterns.json.
"""

import json
from datetime import date
from pathlib import Path

import pytest

from src.analyzers.offline.replay import (
    ENABLED_SENSORS,
    Pipeline,
    discover_date_range,
    iter_records_for_date,
    replay,
)
from src.core.types import Event, EventType


def _write_jsonl(path: Path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


# ---- iter_records_for_date / discover_date_range -------------------------


def test_iter_records_skips_disabled_sensors_and_sorts(tmp_path):
    logs = tmp_path
    _write_jsonl(logs / "crypto" / "2026-02-15.jsonl", [
        {"timestamp": 100.0, "source": "crypto", "btcusdt.price": 60000.0},
        {"timestamp": 50.0, "source": "crypto", "btcusdt.price": 59900.0},
    ])
    _write_jsonl(logs / "earthquake" / "2026-02-15.jsonl", [
        {"timestamp": 80.0, "source": "earthquake", "max_magnitude": 5.5},
    ])
    # noise sensor — must be IGNORED by default
    _write_jsonl(logs / "system" / "2026-02-15.jsonl", [
        {"timestamp": 60.0, "source": "system", "cpu_usage_percent": 90.0},
    ])

    out = list(iter_records_for_date(logs, ENABLED_SENSORS, date(2026, 2, 15)))
    ts_order = [ts for ts, _, _ in out]
    sources = [src for _, src, _ in out]
    assert ts_order == sorted(ts_order), "records must be chronological"
    assert "system" not in sources, "disabled sensors must be skipped"
    assert {"crypto", "earthquake"}.issubset(set(sources))


def test_discover_date_range(tmp_path):
    logs = tmp_path
    _write_jsonl(logs / "crypto" / "2025-12-12.jsonl", [{"timestamp": 1.0, "source": "crypto"}])
    _write_jsonl(logs / "earthquake" / "2026-02-17.jsonl", [{"timestamp": 2.0, "source": "earthquake"}])
    lo, hi = discover_date_range(logs, ENABLED_SENSORS)
    assert lo == date(2025, 12, 12)
    assert hi == date(2026, 2, 17)


def test_discover_date_range_empty(tmp_path):
    assert discover_date_range(tmp_path, ENABLED_SENSORS) == (None, None)


# ---- Pipeline: event-time clock + threshold/cluster behavior --------------


def test_pipeline_detects_sharp_crypto_move_with_event_time(tmp_path):
    pipe = Pipeline(patterns_path=tmp_path / "patterns")
    # Hybrid detection needs a distribution of one-hour returns before a
    # volatility-relative price move can be called unusual.
    base = 1_000_000.0
    for minute in range(105):
        price = 60000 + (minute % 7) * 2 + minute * .03
        pipe.process_event(Event(base + minute * 60, "crypto", EventType.DATA,
                                 {"btcusdt.price": price}))
    anomalies, _ = pipe.process_event(Event(base + 105 * 60, "crypto", EventType.DATA,
                                            {"btcusdt.price": 57000.0}))
    assert any(rec["sensor_source"] == "crypto" for rec in anomalies)


def test_pipeline_clusters_two_sources_within_window(tmp_path):
    """Anomalies from two distinct sources within 30s must form an L2 cluster
    using event time — proving the now-override is wired through."""
    pipe = Pipeline(patterns_path=tmp_path / "patterns")
    base = 2_000_000_000.0  # far in the past for wall clock

    # Named blockchain event is immediately observable without a warm-up.
    _, cl_blockchain = pipe.process_event(Event(timestamp=base + 60, source="blockchain", event_type=EventType.DATA,
                                                payload={"any_anomalous": 1}))

    # earthquake within the 30s window from the crypto anomaly -> M6 triggers
    _, cl_quake = pipe.process_event(Event(timestamp=base + 70, source="earthquake", event_type=EventType.DATA,
                                           payload={"max_magnitude": 6.2}))

    last_cluster = (cl_quake or cl_blockchain)[-1] if (cl_quake or cl_blockchain) else None
    assert last_cluster is not None
    # The quake's cluster should see two distinct sources within 30s
    assert cl_quake and cl_quake[-1]["cluster"]["level"] == 2
    srcs = {a["sensor_source"] for a in cl_quake[-1]["cluster"]["anomalies"]}
    assert srcs == {"blockchain", "earthquake"}


# ---- End-to-end replay over synthetic raw data ---------------------------


def test_replay_writes_anomaly_file_and_patterns(tmp_path):
    logs = tmp_path / "raw"
    out = tmp_path / "out"
    day = date(2026, 2, 15)
    ts0 = 1_771_113_600.0  # 2026-02-15 00:00:00 UTC

    _write_jsonl(logs / "blockchain" / "2026-02-15.jsonl", [
        {"timestamp": ts0 + 60, "source": "blockchain", "any_anomalous": 1},
    ])
    # Earthquake within the 30s cluster window -> L2 cluster
    _write_jsonl(logs / "earthquake" / "2026-02-15.jsonl", [
        {"timestamp": ts0 + 70, "source": "earthquake", "max_magnitude": 6.2},
    ])

    stats = replay(
        logs_root=logs,
        out_anomalies_dir=out / "anomalies",
        out_patterns_dir=out / "patterns",
        start=day, end=day,
    )
    assert stats.events_processed == 2
    assert stats.anomalies_total >= 2

    out_file = out / "anomalies" / "2026-02-15.jsonl"
    assert out_file.exists()
    records = [json.loads(line) for line in out_file.read_text().splitlines() if line.strip()]
    cluster_records = [r for r in records if "cluster" in r]
    assert cluster_records, "expected at least one cluster record"
    assert (out / "patterns" / "patterns.json").exists()


def test_replay_dry_run_writes_nothing(tmp_path):
    logs = tmp_path / "raw"
    out = tmp_path / "out"
    _write_jsonl(logs / "crypto" / "2026-02-15.jsonl",
                 [{"timestamp": 1.0, "source": "crypto", "btcusdt.price": 60000.0}])
    stats = replay(
        logs_root=logs,
        out_anomalies_dir=out / "anomalies",
        out_patterns_dir=out / "patterns",
        dry_run=True,
    )
    assert stats.events_processed == 1
    assert not (out / "anomalies").exists()
    assert not (out / "patterns").exists()


def test_replay_skips_disabled_noise_sensors(tmp_path):
    logs = tmp_path / "raw"
    out = tmp_path / "out"
    # network is disabled; the rule "network.avg_latency_ms" used to trigger on
    # huge spikes — must NOT contribute to anomalies in replay.
    _write_jsonl(logs / "network" / "2026-02-15.jsonl", [
        {"timestamp": 1.0,  "source": "network", "avg_latency_ms": 50.0},
        {"timestamp": 30.0, "source": "network", "avg_latency_ms": 5000.0},
    ])
    stats = replay(
        logs_root=logs,
        out_anomalies_dir=out / "anomalies",
        out_patterns_dir=out / "patterns",
        start=date(2026, 2, 15), end=date(2026, 2, 15),
    )
    assert stats.events_processed == 0
    assert stats.anomalies_total == 0
