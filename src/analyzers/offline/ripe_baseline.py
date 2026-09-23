"""Freeze a predeclared 14-day reference for the paired RIPE root panel.

The reference is descriptive context. Probe routes, DNS anycast placement and
RIPE maintenance can change; deviations are never anomaly votes by themselves.
The training window cannot be extended after looking at the results.
"""

from __future__ import annotations

import json
import statistics
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ...sensors.ripe_atlas_sensor import PROBES, TARGETS

START_AT = datetime(2026, 9, 24, tzinfo=timezone.utc).timestamp()
END_AT = START_AT + 14 * 86400
VERSION = "ripe-reference-v1-20260924"
MIN_UNIQUE_MEASUREMENTS = 100
MIN_OBSERVED_DAYS = 10


def build_baseline(logs: Path, now: float | None = None) -> dict:
    now = time.time() if now is None else now
    samples = defaultdict(dict)
    day = datetime.fromtimestamp(START_AT, timezone.utc).date()
    final = datetime.fromtimestamp(min(now, END_AT - 1), timezone.utc).date()
    while day <= final:
        path = logs / "ripe_atlas" / f"{day.isoformat()}.jsonl"
        try:
            stream = path.open(encoding="utf-8")
        except OSError:
            day += timedelta(days=1)
            continue
        with stream:
            for line in stream:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                retrieved_at = record.get("timestamp")
                if not isinstance(retrieved_at, (int, float)) or not START_AT <= retrieved_at < END_AT:
                    continue
                for target_key, target in (record.get("targets") or {}).items():
                    if target_key not in TARGETS or target.get("measurement_id") != TARGETS[target_key][0]:
                        continue
                    for region, body in (target.get("regions") or {}).items():
                        if region not in PROBES:
                            continue
                        for probe in body.get("probes") or []:
                            probe_id = probe.get("id")
                            age = probe.get("age_seconds")
                            if (probe_id not in PROBES[region] or probe.get("status") != "ok"
                                    or not isinstance(age, (int, float)) or age < 0):
                                continue
                            measurement_at = retrieved_at - age
                            if not START_AT <= measurement_at < END_AT:
                                continue
                            latency = probe.get("latency_ms")
                            loss = probe.get("packet_loss")
                            if not isinstance(loss, (int, float)) or not 0 <= loss <= 1:
                                continue
                            # RIPE's latest endpoint may repeat the same 240 s
                            # measurement across several 300 s fetches.
                            slot = round(measurement_at / 30)
                            samples[(target_key, region, probe_id)][slot] = (
                                latency if isinstance(latency, (int, float)) and latency >= 0 else None,
                                float(loss), datetime.fromtimestamp(measurement_at, timezone.utc).date().isoformat())
        day += timedelta(days=1)

    targets = {}
    problems = []
    for target_key, (measurement_id, hostname) in TARGETS.items():
        regions = {}
        for region, probe_ids in PROBES.items():
            probes = []
            stable_medians = []
            for probe_id in probe_ids:
                values = list(samples[(target_key, region, probe_id)].values())
                latencies = [row[0] for row in values if row[0] is not None]
                days = len({row[2] for row in values})
                eligible = len(values) >= MIN_UNIQUE_MEASUREMENTS and days >= MIN_OBSERVED_DAYS and len(latencies) >= 50
                median = round(statistics.median(latencies), 2) if eligible else None
                if median is not None:
                    stable_medians.append(median)
                probes.append({"id": probe_id, "unique_measurements": len(values),
                               "observed_days": days, "eligible": eligible,
                               "median_latency_ms": median,
                               "loss_frequency": round(sum(row[1] > 0 for row in values) / len(values), 4) if eligible else None})
            if len(stable_medians) < 2:
                problems.append(f"{target_key}/{region}: fewer than two stable probes")
            regions[region] = {"eligible_probes": len(stable_medians),
                               "reference_median_latency_ms": round(statistics.median(stable_medians), 2) if len(stable_medians) >= 2 else None,
                               "probes": probes}
        targets[target_key] = {"measurement_id": measurement_id, "hostname": hostname,
                               "regions": regions}
    return {"version": VERSION, "start_at": START_AT, "end_at": END_AT,
            "generated_at": now,
            "status": "collecting_reference" if now < END_AT else
                      "frozen_reference" if not problems else "insufficient_frozen_reference",
            "complete_days": max(0, min(14, int((now - START_AT) // 86400))),
            "minimum_unique_measurements_per_probe": MIN_UNIQUE_MEASUREMENTS,
            "minimum_observed_days_per_probe": MIN_OBSERVED_DAYS,
            "problems": problems if now >= END_AT else [], "targets": targets,
            "caveat": "Fixed-probe medians are network context, not independent evidence of a global anomaly."}


def write_report(logs: Path = Path("logs"), out: Path = Path("logs/research/ripe-baseline.json"),
                 now: float | None = None) -> dict:
    now = time.time() if now is None else now
    if now >= END_AT:
        try:
            return json.loads(out.read_text())
        except (OSError, json.JSONDecodeError):
            pass
    report = build_baseline(logs, now)
    path = out if now >= END_AT else out.with_name("ripe-baseline-progress.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(report, separators=(",", ":")))
    tmp.replace(path)
    return report


def main() -> None:
    result = write_report()
    print(json.dumps({"status": result["status"], "complete_days": result["complete_days"],
                      "problems": len(result["problems"])}))


if __name__ == "__main__":
    main()
