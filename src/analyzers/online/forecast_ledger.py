"""Append-only future-data audit of five frozen, exploratory forecast rules.

Probabilities below were frozen on 2026-09-23 from the historical Lag Lab:
observed source-episode match rates shrink toward the month-shifted timing
control with 20 pseudo-episodes. They are *candidates*, not calibrated claims.
The future score compares every issued probability with its binary outcome and
with the frozen matched-control probability. Repeated source anomalies within
30 minutes form one episode. Event times here are collector detection times,
so a delayed upstream report cannot manufacture a forecast issued in the past.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ...monitoring.coverage import _issues

START_AT = datetime(2026, 9, 24, tzinfo=timezone.utc).timestamp()
END_AT = START_AT + 120 * 86400
VERSION = "forecast-v1-20260924"
EPISODE_GAP_SECONDS = 1800
SETTLEMENT_GRACE_SECONDS = 600


@dataclass(frozen=True)
class Rule:
    id: str
    source: str
    target: str
    lag_min: int
    lag_max: int
    training_matches: int
    training_sources: int
    shifted_mean_matches: float

    @property
    def baseline_probability(self) -> float:
        return self.shifted_mean_matches / self.training_sources

    @property
    def probability(self) -> float:
        return (self.training_matches + 20 * self.baseline_probability) / (self.training_sources + 20)


# Frozen before the first eligible forecast. A known-physics control stays
# labelled as such; none of these rules enters the validated-signals panel.
RULES = (
    Rule("solar-kp-1h6h", "solar_wind", "space_weather", 3600, 21600, 51, 397, 18.11),
    Rule("solar-crypto-5m1h", "solar_wind", "crypto", 300, 3600, 50, 397, 45.16),
    Rule("solar-crypto-1h6h", "solar_wind", "crypto", 3600, 21600, 147, 397, 170.31),
    Rule("quantum-crypto-5m1h", "quantum_rng", "crypto", 300, 3600, 52, 383, 39.44),
    Rule("quantum-crypto-1h6h", "quantum_rng", "crypto", 3600, 21600, 163, 383, 155.58),
)
TARGET_INTERVALS = {"space_weather": 300, "crypto": 2}


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _read_jsonl(path: Path):
    try:
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    yield value
    except OSError:
        return


def target_coverage_complete(logs: Path, target: str, start: float, end: float) -> bool:
    """Require usable raw polls through the outcome window before scoring 0.

    This is a collection audit, not a guarantee that the detector could have
    recognized every physical event or that the upstream published on time.
    """
    interval = TARGET_INTERVALS[target]
    tolerance = max(120, 3 * interval)
    day = datetime.fromtimestamp(start - tolerance, timezone.utc).date()
    final = datetime.fromtimestamp(end + tolerance, timezone.utc).date()
    times = []
    while day <= final:
        path = logs / target / f"{day.isoformat()}.jsonl"
        for record in _read_jsonl(path):
            ts = record.get("timestamp")
            if (isinstance(ts, (int, float)) and start - tolerance <= ts <= end + tolerance
                    and not _issues(target, record)):
                times.append(float(ts))
        day += timedelta(days=1)
    times.sort()
    if not times or times[0] > start + tolerance or times[-1] < end - tolerance:
        return False
    return all(right - left <= tolerance for left, right in zip(times, times[1:]))


def persisted_target_episode(logs: Path, target: str, start: float, end: float) -> float | None:
    """Recover a target episode persisted just before a collector crash."""
    day = datetime.fromtimestamp(start - EPISODE_GAP_SECONDS, timezone.utc).date()
    final = datetime.fromtimestamp(end, timezone.utc).date()
    times = []
    while day <= final:
        for record in _read_jsonl(logs / "anomalies" / f"{day.isoformat()}.jsonl"):
            ts = record.get("detected_at")
            if (record.get("sensor_source") == target and isinstance(ts, (int, float))
                    and start - EPISODE_GAP_SECONDS <= ts < end):
                times.append(float(ts))
        day += timedelta(days=1)
    last = None
    for ts in sorted(times):
        if last is None or ts - last > EPISODE_GAP_SECONDS:
            if start <= ts < end:
                return ts
        last = ts
    return None


class ForecastLedger:
    def __init__(self, root: Path = Path("logs/forecast_ledger"),
                 raw_logs: Path = Path("logs")):
        self.root = root
        self.raw_logs = raw_logs
        self.issued = {row["id"]: row for row in _read_jsonl(root / "issued.jsonl") if "id" in row}
        self.outcomes = {row["id"]: row for row in _read_jsonl(root / "outcomes.jsonl") if "id" in row}
        self.last_source = {}
        for row in self.issued.values():
            source = row.get("source")
            if source:
                self.last_source[source] = max(self.last_source.get(source, 0), row["issued_at"])
        self.last_target = {}
        for row in _read_jsonl(root / "target_episodes.jsonl"):
            target = row.get("target")
            if target:
                self.last_target[target] = max(self.last_target.get(target, 0), row["detected_at"])

    def record_anomaly(self, anomaly, detected_at: float | None = None) -> list[dict]:
        now = time.time() if detected_at is None else detected_at
        if not START_AT <= now < END_AT:
            return []
        source = anomaly.sensor_source
        if source == "quantum_rng" and (anomaly.metadata or {}).get("measurement_source") != "anu_quantum":
            return []
        if source in TARGET_INTERVALS and now - self.last_target.get(source, 0) > EPISODE_GAP_SECONDS:
            target_event = {"target": source, "detected_at": now, "anomaly_at": anomaly.timestamp}
            _append_jsonl(self.root / "target_episodes.jsonl", target_event)
            self.last_target[source] = now
            for issued in self.issued.values():
                if issued["id"] in self.outcomes or issued["target"] != source:
                    continue
                if issued["issued_at"] + issued["lag_min_seconds"] <= now < issued["issued_at"] + issued["lag_max_seconds"]:
                    self._resolve(issued, 1, now, "target_episode_detected")
        if now - self.last_source.get(source, 0) <= EPISODE_GAP_SECONDS:
            return []
        matching = [rule for rule in RULES if rule.source == source]
        if not matching:
            return []
        results = []
        for rule in matching:
            issue_id = f"{VERSION}:{rule.id}:{int(now * 1000)}"
            issued = {"id": issue_id, "version": VERSION, "rule": rule.id,
                      "source": source, "target": rule.target,
                      "issued_at": now, "source_anomaly_at": anomaly.timestamp,
                      "lag_min_seconds": rule.lag_min, "lag_max_seconds": rule.lag_max,
                      "probability": round(rule.probability, 6),
                      "baseline_probability": round(rule.baseline_probability, 6),
                      "role": "known_physics_calibration" if rule.id == "solar-kp-1h6h" else "exploratory"}
            _append_jsonl(self.root / "issued.jsonl", issued)
            self.issued[issue_id] = issued
            results.append(issued)
        self.last_source[source] = now
        return results

    def _resolve(self, issued: dict, outcome: int | None, now: float, reason: str) -> None:
        row = {"id": issued["id"], "resolved_at": now, "outcome": outcome,
               "status": "scored" if outcome is not None else "unscorable",
               "reason": reason}
        _append_jsonl(self.root / "outcomes.jsonl", row)
        self.outcomes[issued["id"]] = row

    def settle_due(self, now: float | None = None, coverage_check=None) -> int:
        now = time.time() if now is None else now
        check = coverage_check or (lambda target, start, end: target_coverage_complete(self.raw_logs, target, start, end))
        resolved = 0
        for issued in self.issued.values():
            if issued["id"] in self.outcomes or now < issued["issued_at"] + issued["lag_max_seconds"] + SETTLEMENT_GRACE_SECONDS:
                continue
            start = issued["issued_at"] + issued["lag_min_seconds"]
            end = issued["issued_at"] + issued["lag_max_seconds"]
            recovered = persisted_target_episode(self.raw_logs, issued["target"], start, end)
            if recovered is not None:
                self._resolve(issued, 1, now, "persisted_target_episode_recovered")
                resolved += 1
                continue
            complete = check(issued["target"], start, end)
            self._resolve(issued, 0 if complete else None, now,
                          "observed_no_target_episode" if complete else "incomplete_target_coverage")
            resolved += 1
        return resolved

    def report(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        rows = []
        for rule in RULES:
            forecasts = [row for row in self.issued.values() if row["rule"] == rule.id]
            scored = [(item, self.outcomes[item["id"]]["outcome"]) for item in forecasts
                      if item["id"] in self.outcomes and self.outcomes[item["id"]]["status"] == "scored"]
            n = len(scored)
            model_brier = sum((item["probability"] - y) ** 2 for item, y in scored) / n if n else None
            base_brier = sum((item["baseline_probability"] - y) ** 2 for item, y in scored) / n if n else None
            rows.append({"rule": rule.id, "source": rule.source, "target": rule.target,
                         "role": "known_physics_calibration" if rule.id == "solar-kp-1h6h" else "exploratory",
                         "lag_min_seconds": rule.lag_min, "lag_max_seconds": rule.lag_max,
                         "frozen_probability": round(rule.probability, 6),
                         "frozen_baseline_probability": round(rule.baseline_probability, 6),
                         "issued": len(forecasts), "scored": n,
                         "unscorable": sum(self.outcomes.get(item["id"], {}).get("status") == "unscorable" for item in forecasts),
                         "outcomes_positive": sum(y == 1 for _, y in scored),
                         "model_brier": round(model_brier, 5) if model_brier is not None else None,
                         "baseline_brier": round(base_brier, 5) if base_brier is not None else None,
                         "descriptive_brier_skill": round(1 - model_brier / base_brier, 4)
                         if n and base_brier and n >= 30 else None})
        return {"status": "collecting_no_validated_forecast" if now < END_AT else "endpoint_review_required",
                "version": VERSION, "start_at": START_AT, "end_at": END_AT,
                "time_basis": "collector detection times; late source reports never backdate issuance",
                "method": "fixed historical probabilities, 30-minute source episode collapse, observed positive outcomes, negative outcomes gated by raw-target coverage, Brier versus matched timing-control baseline",
                "caveat": "Descriptive scores across related rules are not significance tests or validated signals; review data coverage and the frozen 120-day study at the endpoint.",
                "rules": rows}
