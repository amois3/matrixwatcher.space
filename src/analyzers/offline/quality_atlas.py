"""Historical observation quality and sampling opportunity.

This is a measurement audit, not an anomaly detector. A poll that happened is
not proof that an event was detectable: thresholds, source publication delays
and upstream outages remain separate limitations.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ...monitoring.coverage import SENSOR_MODES, _issues

WINDOWS = (30, 300, 3600)
DAY_SECONDS = 86400


def _union_seconds(intervals: list[tuple[float, float]]) -> float:
    total = 0.0
    end = float("-inf")
    for start, stop in sorted(intervals):
        if stop <= end:
            continue
        total += stop - max(start, end)
        end = stop
    return total


def scan_day(path: Path, sensor: str, interval: float, day_start: float) -> dict:
    """Summarize one UTC day without retaining high-frequency raw records."""
    bins: set[int] = set()
    opportunities: dict[int, list[tuple[float, float]]] = {w: [] for w in WINDOWS}
    count = partial = invalid = 0
    sample_seconds = 0.0
    first = last = None
    first_usable = last_usable = None
    max_gap = 0.0
    try:
        stream = path.open(encoding="utf-8")
    except OSError:
        stream = None
    if stream is not None:
        with stream:
            for line in stream:
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    invalid += 1
                    continue
                ts = record.get("timestamp") if isinstance(record, dict) else None
                if not isinstance(ts, (int, float)) or not day_start <= ts < day_start + DAY_SECONDS:
                    invalid += 1
                    continue
                count += 1
                degraded = bool(_issues(sensor, record))
                if degraded:
                    partial += 1
                if first is None:
                    first = float(ts)
                last = float(ts)
                if degraded:
                    continue
                if first_usable is None:
                    first_usable = float(ts)
                if last_usable is not None:
                    max_gap = max(max_gap, float(ts) - last_usable)
                last_usable = float(ts)
                bins.add(int((ts - day_start) / interval))
                sample_end = float(ts)
                observed_span = 0.0
                if sensor == "wikipedia_edits":
                    covered_start = record.get("covered_start_at")
                    covered_end = record.get("covered_until_at")
                    if isinstance(covered_start, (int, float)) and isinstance(covered_end, (int, float)):
                        sample_end = min(float(covered_end), day_start + DAY_SECONDS)
                        observed_span = max(0.0, sample_end - max(float(covered_start), day_start))
                    else:
                        observed_span = max(0.0, min(float(record.get("sample_seconds") or 0), interval))
                    sample_seconds += observed_span
                # An event starting up to `window` seconds before an observed
                # span could overlap it. This is sampling opportunity only.
                for window in WINDOWS:
                    start = max(day_start, sample_end - observed_span - window)
                    stop = min(day_start + DAY_SECONDS, sample_end)
                    if stop > start:
                        opportunities[window].append((start, stop))
    expected = max(1, round(DAY_SECONDS / interval))
    if first_usable is not None:
        max_gap = max(max_gap, first_usable - day_start, day_start + DAY_SECONDS - last_usable)
    else:
        max_gap = DAY_SECONDS
    return {
        "day": datetime.fromtimestamp(day_start, timezone.utc).date().isoformat(),
        "count": count, "partial": partial, "invalid": invalid,
        "first_at": first, "last_at": last,
        "expected_polls": expected,
        "poll_coverage": round(min(1.0, len(bins) / expected), 4),
        "max_gap_seconds": round(max_gap),
        "sampled_seconds": round(sample_seconds, 1) if sensor == "wikipedia_edits" else None,
        "sampling_opportunity": {
            str(window): round(_union_seconds(spans) / DAY_SECONDS, 4)
            for window, spans in opportunities.items()
        },
    }


def build_atlas(logs: Path, config: dict, days: int = 30, now: float | None = None,
                cache: dict | None = None) -> dict:
    now = time.time() if now is None else now
    today = datetime.fromtimestamp(now, timezone.utc).date()
    previous = (cache or {}).get("files", {})
    files = {}
    sensors = {}
    for sensor, mode in SENSOR_MODES.items():
        settings = config.get("sensors", {}).get(sensor, {})
        if not settings.get("enabled", True):
            continue
        interval = max(1.0, float(settings.get("interval_seconds", 300)))
        rows = []
        for offset in range(days, 0, -1):
            day = today - timedelta(days=offset)
            day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
            path = logs / sensor / f"{day.isoformat()}.jsonl"
            try:
                stat = path.stat()
                signature = [stat.st_size, stat.st_mtime_ns, interval]
            except OSError:
                signature = [0, 0, interval]
            key = f"{sensor}/{day.isoformat()}"
            old = previous.get(key)
            row = old["row"] if old and old.get("signature") == signature else scan_day(
                path, sensor, interval, day_start)
            files[key] = {"signature": signature, "row": row}
            rows.append(row)
        sensors[sensor] = {
            "mode": mode, "interval_seconds": interval,
            "observed_days": sum(row["count"] > 0 for row in rows),
            "partial_records": sum(row["partial"] for row in rows),
            "poll_coverage_mean": round(sum(row["poll_coverage"] for row in rows) / days, 4),
            "sampling_opportunity": {
                str(window): round(sum(row["sampling_opportunity"][str(window)] for row in rows) / days, 4)
                for window in WINDOWS
            },
            "days": rows,
        }
    return {
        "generated_at": now, "period_start": (today - timedelta(days=days)).isoformat(),
        "period_end": (today - timedelta(days=1)).isoformat(),
        "days": days, "status": "sampling_audit_not_detection_probability",
        "method": "UTC poll bins and union of observed sampling windows; excludes detector sensitivity and source publication lag",
        "windows_seconds": list(WINDOWS), "sensors": sensors, "files": files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", type=Path, default=Path("logs"))
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    parser.add_argument("--out", type=Path, default=Path("logs/research/quality.json"))
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.days <= 366:
        parser.error("days must be 1..366")
    try:
        config = json.loads(args.config.read_text())
    except (OSError, json.JSONDecodeError):
        config = {}
    try:
        cache = json.loads(args.out.read_text())
    except (OSError, json.JSONDecodeError):
        cache = {}
    report = build_atlas(args.logs, config, days=args.days, cache=cache)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.out.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, separators=(",", ":")))
    temporary.replace(args.out)
    print(json.dumps({"days": args.days, "sensors": len(report["sensors"])}))


if __name__ == "__main__":
    main()
