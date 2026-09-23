"""Predeclared, exploratory lag search with timing controls.

Stored anomaly timestamps are used throughout. Earthquakes carry USGS origin
time, while most other feeds carry observation time. Upstream reporting delays
can create an apparent direction. This report never
promotes historical associations to discoveries or forecasts.
"""

from __future__ import annotations

import argparse
import bisect
import json
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .evidence import DETECTOR_EPOCH, _holm_adjust, verified_quantum_times
from .shuffle_test import load_anomalies_from_logs

# Freeze the candidate family before running the scheduled historical search.
# The solar pair is a known physical chain: it calibrates whether the method can
# see a plausible lag, and is explicitly excluded from "unexplained" candidates.
PAIRS = (
    ("solar_wind", "space_weather", "known_physics"),
    ("earthquake", "wikipedia_edits", "human_response"),
    ("earthquake", "news", "human_response"),
    ("quantum_rng", "crypto", "exploratory"),
    ("earthquake", "crypto", "exploratory"),
    ("solar_wind", "crypto", "exploratory"),
)
WINDOWS = ((300, 3600), (3600, 21600))
EPISODE_GAP = 1800


def source_episodes(records: list[dict], after: float, verified_quantum: set[float]) -> dict[str, list[float]]:
    raw: dict[str, list[float]] = defaultdict(list)
    for record in records:
        if "cluster" in record:
            continue
        source, ts = record.get("sensor_source"), record.get("timestamp")
        if not source or not isinstance(ts, (int, float)) or ts < after:
            continue
        if source == "quantum_rng" and (
            (record.get("metadata") or {}).get("measurement_source") != "anu_quantum"
            and float(ts) not in verified_quantum
        ):
            continue
        raw[source].append(float(ts))
    episodes = {}
    for source, times in raw.items():
        collapsed = []
        last_raw = None
        for ts in sorted(times):
            if last_raw is None or ts - last_raw > EPISODE_GAP:
                collapsed.append(ts)
            last_raw = ts
        episodes[source] = collapsed
    return episodes


def match_count(source: list[float], target: list[float], lag_min: int, lag_max: int) -> int:
    """Each source episode scores at most once within a directed lag band."""
    target = sorted(target)
    count = 0
    for ts in source:
        i = bisect.bisect_left(target, ts + lag_min)
        count += i < len(target) and target[i] < ts + lag_max
    return count


def shifted_days(times: list[float], rng: random.Random) -> list[float]:
    """Shift target days per month while preserving within-day timing and bursts."""
    groups: dict[tuple[int, int], list[float]] = defaultdict(list)
    for ts in times:
        dt = datetime.fromtimestamp(ts, timezone.utc)
        groups[(dt.year, dt.month)].append(ts)
    result = []
    for (year, month), group in groups.items():
        start = datetime(year, month, 1, tzinfo=timezone.utc).timestamp()
        next_month = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc).timestamp()
        days = round((next_month - start) / 86400)
        offset = rng.randrange(days) * 86400
        result.extend(start + (ts - start + offset) % (days * 86400) for ts in group)
    return sorted(result)


def analyze(episodes: dict[str, list[float]], iterations: int = 500, seed: int = 20260923) -> dict:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    rng = random.Random(seed)
    rows = []
    for source, target, role in PAIRS:
        a, b = episodes.get(source, []), episodes.get(target, [])
        null_targets = [shifted_days(b, rng) for _ in range(iterations)] if a and b else []
        for low, high in WINDOWS:
            observed = match_count(a, b, low, high)
            null = [match_count(a, shifted, low, high) for shifted in null_targets]
            p = (1 + sum(value >= observed for value in null)) / (iterations + 1) if null else 1.0
            rows.append({
                "source": source, "target": target, "role": role,
                "power_warning": "too_few_episodes" if min(len(a), len(b)) < 20 else None,
                "lag_min_seconds": low, "lag_max_seconds": high,
                "source_episodes": len(a), "target_episodes": len(b),
                "observed_matches": observed,
                "null_mean_matches": round(sum(null) / len(null), 2) if null else None,
                "p_value": round(p, 4),
            })
    adjusted = _holm_adjust([row["p_value"] for row in rows])
    for row, p in zip(rows, adjusted):
        row["p_holm"] = round(p, 4)
    return {
        "generated_at": time.time(), "status": "exploratory_not_validated",
        "method": "30-minute episode collapse; directed lag bands; target whole-day shifts within UTC months; Holm over all 12 tests",
        "time_basis": "stored anomaly timestamps: USGS origin time for earthquakes, usually observation time for other feeds; reporting delays vary",
        "seed": seed, "iterations": iterations, "episodes": {name: len(times) for name, times in episodes.items()},
        "tests": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", type=Path, default=Path("logs/anomalies"))
    parser.add_argument("--out", type=Path, default=Path("logs/research/lags.json"))
    parser.add_argument("--days", type=int, default=120)
    parser.add_argument("--iterations", type=int, default=500)
    args = parser.parse_args()
    records = load_anomalies_from_logs(args.logs, args.days)
    episodes = source_episodes(records, max(DETECTOR_EPOCH, time.time() - args.days * 86400),
                               verified_quantum_times(args.logs.parent, args.days))
    report = analyze(episodes, args.iterations)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.out.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, separators=(",", ":")))
    temporary.replace(args.out)
    print(json.dumps({"episodes": report["episodes"], "tests": len(report["tests"])}))


if __name__ == "__main__":
    main()
