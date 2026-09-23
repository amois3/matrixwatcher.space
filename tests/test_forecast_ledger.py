"""The immutable forecast diary must settle only future, observable outcomes."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from src.analyzers.online.forecast_ledger import (
    END_AT, START_AT, ForecastLedger, target_coverage_complete,
)


def anomaly(source, ts, metadata=None):
    return SimpleNamespace(sensor_source=source, timestamp=ts, metadata=metadata)


def test_ledger_issues_once_per_episode_and_scores_future_target(tmp_path):
    ledger = ForecastLedger(tmp_path / "diary", tmp_path / "raw")
    issued_at = START_AT + 100
    assert ledger.record_anomaly(anomaly("solar_wind", issued_at), START_AT - 1) == []
    first = ledger.record_anomaly(anomaly("solar_wind", issued_at), issued_at)
    assert len(first) == 3
    assert ledger.record_anomaly(anomaly("solar_wind", issued_at + 5), issued_at + 5) == []
    assert all(row["issued_at"] == issued_at for row in first)
    assert all(row["source_anomaly_at"] == issued_at for row in first)
    target_at = issued_at + 4200
    ledger.record_anomaly(anomaly("space_weather", target_at), target_at)
    report = ledger.report(target_at)
    solar_kp = next(row for row in report["rules"] if row["rule"] == "solar-kp-1h6h")
    assert solar_kp["scored"] == 1 and solar_kp["outcomes_positive"] == 1
    assert solar_kp["model_brier"] is not None

    # Reconstruct the diary after a restart without reissuing the same episode.
    restored = ForecastLedger(tmp_path / "diary", tmp_path / "raw")
    assert restored.record_anomaly(anomaly("solar_wind", issued_at + 10), issued_at + 10) == []
    assert restored.report(target_at)["rules"][0]["scored"] == 1


def test_unobserved_negative_is_not_scored(tmp_path):
    ledger = ForecastLedger(tmp_path / "diary", tmp_path / "raw")
    issued_at = START_AT + 100
    ledger.record_anomaly(anomaly("solar_wind", issued_at), issued_at)
    due = issued_at + 21600 + 601
    ledger.settle_due(due, coverage_check=lambda target, start, end: False)
    assert all(row["status"] == "unscorable" for row in ledger.outcomes.values())
    assert all(row["outcome"] is None for row in ledger.outcomes.values())
    assert ledger.settle_due(due + 100, coverage_check=lambda target, start, end: True) == 0


def test_complete_negative_is_scored_once_and_quantum_provenance_required(tmp_path):
    ledger = ForecastLedger(tmp_path / "diary", tmp_path / "raw")
    issued_at = START_AT + 100
    assert ledger.record_anomaly(anomaly("quantum_rng", issued_at), issued_at) == []
    issued = ledger.record_anomaly(anomaly("quantum_rng", issued_at, {"measurement_source": "anu_quantum"}), issued_at)
    assert len(issued) == 2
    assert ledger.settle_due(issued_at + 21600 + 601,
                           coverage_check=lambda target, start, end: True) == 2
    assert all(row["outcome"] == 0 for row in ledger.outcomes.values())
    assert ledger.record_anomaly(anomaly("solar_wind", END_AT), END_AT) == []


def test_raw_coverage_gate_rejects_gap_and_partial_records(tmp_path):
    start = START_AT + 1000
    end = start + 300
    day = datetime.fromtimestamp(start, timezone.utc).date().isoformat()
    path = tmp_path / "crypto" / f"{day}.jsonl"
    path.parent.mkdir(parents=True)
    rows = [{"timestamp": start + step,
             "pairs": [{"symbol": "BTCUSDT"}, {"symbol": "ETHUSDT"}]}
            for step in range(0, 301, 60)]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    assert target_coverage_complete(tmp_path, "crypto", start, end)
    rows[2]["pairs"] = [{"symbol": "ETHUSDT"}]
    rows[3]["pairs"] = [{"symbol": "ETHUSDT"}]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    assert not target_coverage_complete(tmp_path, "crypto", start, end)


def test_persisted_target_is_recovered_before_negative_settlement(tmp_path):
    ledger = ForecastLedger(tmp_path / "diary", tmp_path / "raw")
    issued_at = START_AT + 100
    ledger.record_anomaly(anomaly("solar_wind", issued_at), issued_at)
    detected_at = issued_at + 4200
    day = datetime.fromtimestamp(detected_at, timezone.utc).date().isoformat()
    path = tmp_path / "raw" / "anomalies" / f"{day}.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"sensor_source": "space_weather",
                                "timestamp": detected_at,
                                "detected_at": detected_at}) + "\n")
    ledger.settle_due(issued_at + 21600 + 601,
                      coverage_check=lambda target, start, end: True)
    kp = next(row for row in ledger.report()["rules"] if row["rule"] == "solar-kp-1h6h")
    assert kp["outcomes_positive"] == 1
