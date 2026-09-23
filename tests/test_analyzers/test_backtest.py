"""Tests for the out-of-sample backtester — must reward skill and expose its absence."""

import json
import tempfile
from pathlib import Path

from src.analyzers.offline.backtest import (
    make_btc_move_outcome,
    walk_forward,
    block_bootstrap_skill,
    load_conditions,
)


def _write_clusters(dirpath, rows):
    """rows: list of (ts, level, [sources]) -> one daily jsonl file."""
    p = Path(dirpath) / "2026-05-27.jsonl"
    with open(p, "w") as f:
        for ts, level, sources in rows:
            rec = {"timestamp": ts, "cluster": {"level": level,
                   "anomalies": [{"sensor_source": s} for s in sources]}}
            f.write(json.dumps(rec) + "\n")


def test_exclude_sources_filters_conditions():
    with tempfile.TemporaryDirectory() as d:
        _write_clusters(d, [
            (1.0, 2, ["crypto", "news"]),
            (2.0, 2, ["earthquake", "quantum_rng"]),
            (3.0, 3, ["crypto", "earthquake", "quantum_rng"]),
        ])
        # without exclusion: 3 conditions
        assert len(load_conditions(d, days=1, min_level=2)) == 3
        # excluding crypto: only the earthquake+quantum one remains
        kept = load_conditions(d, days=1, min_level=2, exclude_sources={"crypto"})
        assert len(kept) == 1
        assert "earthquake_quantum_rng" in kept[0][1]


def test_outcome_detects_move_within_horizon():
    # price jumps 3% 30 min after t=0
    series = [(0.0, 100.0), (1800.0, 103.0), (3600.0, 101.0)]
    outcome = make_btc_move_outcome(series, horizon_hours=1.0, threshold_pct=2.0)
    assert outcome(0.0) == 1


def test_outcome_zero_when_flat():
    series = [(0.0, 100.0), (1800.0, 100.5), (3600.0, 100.2), (7200.0, 100.0)]
    outcome = make_btc_move_outcome(series, horizon_hours=1.0, threshold_pct=2.0)
    assert outcome(0.0) == 0


def test_outcome_none_without_future_coverage():
    series = [(0.0, 100.0), (60.0, 100.1)]  # no data out to 1h
    outcome = make_btc_move_outcome(series, horizon_hours=1.0, threshold_pct=2.0)
    assert outcome(0.0) is None


def test_no_skill_when_outcome_independent_of_key():
    # Outcome alternates regardless of condition key -> system can't beat base rate
    conditions = [(float(i), f"L2_k{i % 3}") for i in range(200)]
    outcome_fn = lambda t: int(t) % 2  # 50/50, unrelated to key
    r = walk_forward(conditions, outcome_fn)
    assert r["conditions_scored"] == 200
    # System must NOT beat the base rate on noise (skill not positive)
    assert r["skill_score"] <= 0.02
    assert "positive skill" not in r["verdict"]


def test_positive_skill_when_key_determines_outcome():
    # One key always -> event, another key never. System should learn this.
    conditions = []
    for i in range(200):
        key = "L3_always" if i % 2 == 0 else "L3_never"
        conditions.append((float(i), key))
    outcome_fn = lambda t: 1 if int(t) % 2 == 0 else 0
    r = walk_forward(conditions, outcome_fn)
    assert r["skill_score"] > 0.5  # strong, learnable signal
    assert "positive skill" in r["verdict"]


def test_insufficient_data_is_honest():
    r = walk_forward([], lambda t: 1)
    assert r["conditions_scored"] == 0
    assert r["skill_score"] is None
    assert "insufficient" in r["verdict"]


def test_bootstrap_flags_real_skill_significant():
    conditions = [(float(i), "L3_always" if i % 2 == 0 else "L3_never") for i in range(400)]
    outcome_fn = lambda t: 1 if int(t) % 2 == 0 else 0
    r = walk_forward(conditions, outcome_fn, bootstrap=300)
    assert r["bootstrap"]["significant"] is True
    assert r["bootstrap"]["skill_ci_low"] > 0
    assert "SIGNIFICANT" in r["verdict"]


def test_bootstrap_marks_noise_not_significant():
    # outcome independent of key -> skill CI should include 0
    conditions = [(float(i), f"L2_k{i % 5}") for i in range(400)]
    rng_outcomes = [(i * 2654435761) % 2 for i in range(400)]  # deterministic pseudo-noise
    outcome_fn = lambda t: rng_outcomes[int(t)]
    r = walk_forward(conditions, outcome_fn, bootstrap=300)
    # The key guarantee: noise must NOT be flagged as significant positive skill.
    assert r["bootstrap"]["significant"] is False
    assert r["bootstrap"]["skill_ci_low"] <= 0


def test_block_bootstrap_direct_no_skill():
    # identical sys/base errors -> skill exactly 0 -> CI around 0, not significant
    sys_sq = [0.25] * 200
    base_sq = [0.25] * 200
    out = block_bootstrap_skill(sys_sq, base_sq, block_size=14, n_boot=200)
    assert out["significant"] is False


# ---- Symmetric (any source → any target) tests ---------------------------


def test_make_anomaly_outcome_detects_target_event_in_horizon():
    from src.analyzers.offline.backtest import make_anomaly_outcome
    # Extra late events extend the coverage so we can score t=8500 honestly.
    events_by_src = {
        "earthquake": [1000.0, 5000.0, 9000.0, 50000.0],
        "quantum_rng": [500.0, 3000.0, 60000.0],
    }
    outcome = make_anomaly_outcome(events_by_src, target_source="earthquake", horizon_hours=1.0)
    # at t=900, an earthquake at 1000 lies within the 3600s horizon -> 1
    assert outcome(900.0) == 1
    # at t=2000, next earthquake at 5000 is 3000s away — within 1h
    assert outcome(2000.0) == 1
    # at t=1500, next earthquake at 5000 is 3500s away — within 1h
    assert outcome(1500.0) == 1
    # at t=8500, next earthquake at 9000 is 500s away — within 1h
    assert outcome(8500.0) == 1


def test_make_anomaly_outcome_returns_zero_when_no_event_in_horizon():
    from src.analyzers.offline.backtest import make_anomaly_outcome
    events_by_src = {
        "earthquake": [1000.0, 50000.0],  # 13.6h apart
    }
    outcome = make_anomaly_outcome(events_by_src, target_source="earthquake", horizon_hours=1.0)
    # at t=2000 the next earthquake is at 50000 — way past 1h horizon
    # but coverage check requires last_ts >= t+horizon; here last_ts = 50000 > 2000+3600 -> OK to score
    assert outcome(2000.0) == 0


def test_make_anomaly_outcome_returns_none_when_horizon_past_coverage():
    from src.analyzers.offline.backtest import make_anomaly_outcome
    events_by_src = {"earthquake": [1000.0, 2000.0]}
    outcome = make_anomaly_outcome(events_by_src, target_source="earthquake", horizon_hours=1.0)
    # t=1500 + horizon=3600 = 5100 > last_ts (2000) -> None
    assert outcome(1500.0) is None


def test_make_anomaly_outcome_unknown_target_returns_zero_within_coverage():
    from src.analyzers.offline.backtest import make_anomaly_outcome
    events_by_src = {"crypto": [1000.0, 50000.0]}
    outcome = make_anomaly_outcome(events_by_src, target_source="unknown_sensor", horizon_hours=1.0)
    # within coverage but no events of that source
    assert outcome(2000.0) == 0


def test_load_anomaly_events_by_source_skips_cluster_records(tmp_path):
    from src.analyzers.offline.backtest import load_anomaly_events_by_source
    f = tmp_path / "2026-05-27.jsonl"
    with f.open("w") as fh:
        fh.write(json.dumps({"timestamp": 1.0, "sensor_source": "earthquake"}) + "\n")
        fh.write(json.dumps({"timestamp": 2.0, "cluster": {"level": 3}}) + "\n")  # cluster — skip
        fh.write(json.dumps({"timestamp": 3.0, "sensor_source": "quantum_rng"}) + "\n")
        fh.write(json.dumps({"timestamp": 4.0, "sensor_source": "earthquake"}) + "\n")
    by_src = load_anomaly_events_by_source(str(tmp_path), days=1)
    assert by_src == {"earthquake": [1.0, 4.0], "quantum_rng": [3.0]}


def test_symmetric_walk_forward_finds_learnable_signal():
    """With a deterministic dependence (every condition with key X is
    followed by an earthquake event within an hour, while key Y is not), the
    symmetric backtester must learn it. Same correctness check we did for the
    BTC-move outcome, but in the cross-domain direction."""
    from src.analyzers.offline.backtest import make_anomaly_outcome
    # Conditions spaced 2h apart so that a single earthquake after an X-condition
    # is still well clear of the NEXT condition (Y) -> outcome cleanly separates.
    spacing = 7200.0
    conditions = [(float(i * spacing), "L2_quantum_space" if i % 2 == 0 else "L2_news_weather")
                  for i in range(200)]
    # earthquake 30s after every even (X) condition only
    earthquake_ts = sorted([c[0] + 30 for c in conditions if c[1] == "L2_quantum_space"])
    # extend coverage past the last condition's horizon
    earthquake_ts.append(conditions[-1][0] + 7200.0)
    events_by_src = {"earthquake": earthquake_ts}
    outcome = make_anomaly_outcome(events_by_src, target_source="earthquake", horizon_hours=1.0)
    r = walk_forward(conditions, outcome, bootstrap=200)
    assert r["skill_score"] > 0.4
    assert r["bootstrap"]["significant"] is True
