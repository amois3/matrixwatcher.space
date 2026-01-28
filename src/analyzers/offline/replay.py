"""Offline replay of raw sensor logs through the live detection pipeline.

Replays recorded raw observations (prices, earthquakes, space weather, quantum
randomness, etc.) chronologically through the current ``ThresholdDetector`` +
``ClusterDetector`` + ``HistoricalPatternTracker``, rebuilding the derived
anomaly and pattern labels (``logs/anomalies/<date>.jsonl`` + ``logs/patterns/``).

This lets the full history be re-analysed whenever the detection rules change,
using each event's own timestamp as the clock so time windows apply correctly
to historical data.

Usage
-----
Stop ``main.py`` first (so no concurrent writes), then::

    python -m src.analyzers.offline.replay
        --logs logs                           # raw sensor dirs live under here
        --out-dir logs                        # rewrites logs/anomalies + logs/patterns
        --start 2025-12-12 --end 2026-05-28   # inclusive UTC range; default: full history

Pass ``--dry-run`` to compute without writing output files.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date as date_cls
from pathlib import Path
from typing import Any, Iterable, Iterator

from ...core.types import Event, EventType
from ..online.anomaly_index import AnomalyIndexCalculator
from ..online.cluster_detector import ClusterDetector
from ..online.historical_pattern_tracker import (
    Condition,
    HistoricalPatternTracker,
)
from ..online.threshold_detector import ThresholdDetector


logger = logging.getLogger(__name__)


# Sensors the LIVE system processes today (system/time_drift/network/random are
# disabled — their raw files are ignored to match production behaviour).
ENABLED_SENSORS: tuple[str, ...] = (
    "crypto",
    "blockchain",
    "weather",
    "news",
    "earthquake",
    "space_weather",
    "quantum_rng",
)


# --------------------------------------------------------------------------
# Reading raw observations
# --------------------------------------------------------------------------

DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:\.\d+)?$")  # accepts "2026-05-27" and "2026-05-27.1"


def _file_date(path: Path) -> date_cls | None:
    m = DATE_RE.match(path.stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def discover_date_range(logs_root: Path, sensors: Iterable[str] = ENABLED_SENSORS) -> tuple[date_cls | None, date_cls | None]:
    """Return (min, max) UTC date present in the raw sensor logs."""
    dates: list[date_cls] = []
    for s in sensors:
        d = logs_root / s
        if not d.is_dir():
            continue
        for f in d.glob("*.jsonl"):
            dt = _file_date(f)
            if dt is not None:
                dates.append(dt)
    if not dates:
        return None, None
    return min(dates), max(dates)


def iter_records_for_date(logs_root: Path, sensors: Iterable[str], day: date_cls) -> Iterator[tuple[float, str, dict]]:
    """Yield (timestamp, source, record) for one UTC date, sorted by timestamp.

    ``source`` is the sensor directory name (the value live ``main.py`` puts on
    the EventBus), NOT the inner ``source`` field of the record — those can
    differ (e.g. quantum_rng records store the real entropy source).
    """
    records: list[tuple[float, str, dict]] = []
    date_str = day.strftime("%Y-%m-%d")
    for sensor in sensors:
        sensor_dir = logs_root / sensor
        if not sensor_dir.is_dir():
            continue
        for f in sorted(sensor_dir.glob(f"{date_str}*.jsonl")):
            try:
                with f.open() as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        ts = rec.get("timestamp")
                        if isinstance(ts, (int, float)):
                            records.append((float(ts), sensor, rec))
            except OSError as e:
                logger.warning("could not read %s: %s", f, e)
    records.sort(key=lambda x: x[0])
    return iter(records)


# --------------------------------------------------------------------------
# Driving the pipeline
# --------------------------------------------------------------------------


class ReplayStats:
    def __init__(self) -> None:
        self.events_processed = 0
        self.anomalies_total = 0
        self.cluster_counts: dict[int, int] = defaultdict(int)
        self.start_ts: float | None = None
        self.end_ts: float | None = None

    def note_event(self, ts: float) -> None:
        self.events_processed += 1
        if self.start_ts is None or ts < self.start_ts:
            self.start_ts = ts
        if self.end_ts is None or ts > self.end_ts:
            self.end_ts = ts

    def to_dict(self) -> dict:
        return {
            "events_processed": self.events_processed,
            "anomalies_total": self.anomalies_total,
            "cluster_counts_by_level": dict(self.cluster_counts),
            "period_start": (
                datetime.fromtimestamp(self.start_ts, tz=timezone.utc).isoformat()
                if self.start_ts else None
            ),
            "period_end": (
                datetime.fromtimestamp(self.end_ts, tz=timezone.utc).isoformat()
                if self.end_ts else None
            ),
        }


class Pipeline:
    """The fixed pipeline as a single replay-friendly unit.

    No event_bus is wired — the detector publishes nowhere; we capture its
    return values directly. ``time.time()`` is overridden everywhere it
    matters so the cluster window is applied to historical timestamps.
    """

    def __init__(self, patterns_path: Path):
        self.detector = ThresholdDetector(event_bus=None, enable_calibration_tracking=False)
        # 30s window matches the live config and the README's documented method
        self.cluster_detector = ClusterDetector(cluster_window_seconds=30.0)
        self.anomaly_index = AnomalyIndexCalculator(baseline_window_hours=24)
        # patterns_path is where the rebuilt patterns.json lands
        self.pattern_tracker = HistoricalPatternTracker(storage_path=str(patterns_path))
        # The tracker eagerly preloads recent prices on construction for live use.
        # In replay we feed prices in historical order ourselves, so wipe the
        # preload to avoid mixing recent prices with the historical timeline.
        for series in self.pattern_tracker._price_history.values():
            series.clear()

    def process_event(self, event: Event) -> tuple[list[dict], list[dict]]:
        """Process one historical event, returning (anomaly_records, cluster_records)."""
        # Pattern-tracker event detection (uses event timestamp as the clock)
        self.pattern_tracker.check_events(event.payload, current_time=event.timestamp)

        # Threshold anomalies
        anomalies = self.detector.process(event)
        anomaly_records: list[dict] = []
        cluster_records: list[dict] = []

        for anomaly in anomalies:
            anomaly_records.append(anomaly.to_dict())
            cluster = self.cluster_detector.add_anomaly(anomaly, now=event.timestamp)
            if not cluster:
                continue
            snapshot = self.anomaly_index.calculate(cluster.anomalies)
            unique_sources = sorted(set(a.sensor_source for a in cluster.anomalies))
            condition = Condition(
                timestamp=cluster.timestamp,
                level=cluster.level,
                sources=unique_sources,
                anomaly_index=snapshot.index,
                baseline_ratio=snapshot.baseline_ratio,
            )
            self.pattern_tracker.record_condition(condition)
            probabilities = self.pattern_tracker.get_probabilities(condition, category_filter=None)

            cluster_records.append({
                "source": "anomalies",
                "cluster": {
                    "level": cluster.level,
                    "timestamp": cluster.timestamp,
                    "probability": cluster.probability,
                    "anomalies": [a.to_dict() for a in cluster.anomalies],
                },
                "index": {
                    "value": snapshot.index,
                    "status": snapshot.status,
                    "baseline_ratio": snapshot.baseline_ratio,
                    "breakdown": snapshot.breakdown,
                },
                "probabilities": probabilities,
                "timestamp": event.timestamp,
            })
        return anomaly_records, cluster_records


def _utc_date_str(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def replay(
    logs_root: Path,
    out_anomalies_dir: Path,
    out_patterns_dir: Path,
    *,
    start: date_cls | None = None,
    end: date_cls | None = None,
    sensors: Iterable[str] = ENABLED_SENSORS,
    progress_every_days: int = 1,
    dry_run: bool = False,
) -> ReplayStats:
    """Replay every raw observation in [start, end] through the fixed pipeline."""
    sensors = tuple(sensors)
    auto_start, auto_end = discover_date_range(logs_root, sensors)
    if start is None:
        start = auto_start
    if end is None:
        end = auto_end
    if start is None or end is None:
        logger.warning("no raw data found in %s for sensors=%s", logs_root, sensors)
        return ReplayStats()

    import tempfile

    tmp_for_dry_run: tempfile.TemporaryDirectory | None = None
    if dry_run:
        # Run the pipeline against a scratch directory so the real output is untouched.
        tmp_for_dry_run = tempfile.TemporaryDirectory(prefix="replay_dryrun_")
        patterns_path_for_pipeline = Path(tmp_for_dry_run.name)
    else:
        out_anomalies_dir.mkdir(parents=True, exist_ok=True)
        out_patterns_dir.mkdir(parents=True, exist_ok=True)
        patterns_path_for_pipeline = out_patterns_dir

    pipe = Pipeline(patterns_path_for_pipeline)
    stats = ReplayStats()

    one_day = timedelta(days=1)
    day = start
    while day <= end:
        day_records_iter = iter_records_for_date(logs_root, sensors, day)
        # Buffer one day of output, then write at day boundary.
        day_buffers: dict[str, list[dict]] = defaultdict(list)
        events_this_day = 0
        for ts, source, raw in day_records_iter:
            event = Event(timestamp=ts, source=source, event_type=EventType.DATA, payload=raw)
            stats.note_event(ts)
            events_this_day += 1

            anomaly_records, cluster_records = pipe.process_event(event)

            for rec in anomaly_records:
                stats.anomalies_total += 1
                day_buffers[_utc_date_str(rec.get("timestamp", ts))].append(rec)
            for rec in cluster_records:
                stats.cluster_counts[rec["cluster"]["level"]] += 1
                day_buffers[_utc_date_str(rec.get("timestamp", ts))].append(rec)

        if not dry_run:
            for date_str, records in day_buffers.items():
                out_file = out_anomalies_dir / f"{date_str}.jsonl"
                with out_file.open("a") as fh:
                    for rec in records:
                        fh.write(json.dumps(rec) + "\n")

        if progress_every_days and ((day - start).days % progress_every_days == 0):
            logger.info(
                "replayed %s: %d events, totals: %d anomalies, clusters %s",
                day, events_this_day, stats.anomalies_total, dict(stats.cluster_counts),
            )
        day += one_day

    if not dry_run:
        pipe.pattern_tracker.save()
    if tmp_for_dry_run is not None:
        tmp_for_dry_run.cleanup()
    return stats


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _parse_date(s: str) -> date_cls:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--logs", type=Path, default=Path("logs"), help="root containing per-sensor subdirs")
    p.add_argument("--out-dir", type=Path, default=None,
                   help="destination root (defaults to --logs). anomalies -> <out>/anomalies/, patterns -> <out>/patterns/")
    p.add_argument("--start", type=_parse_date, default=None, help="UTC start date (YYYY-MM-DD), default: earliest found")
    p.add_argument("--end", type=_parse_date, default=None, help="UTC end date (YYYY-MM-DD), default: latest found")
    p.add_argument("--dry-run", action="store_true", help="run without writing outputs")
    p.add_argument("--wipe", action="store_true",
                   help="before replay, archive existing labels (anomalies/, patterns/) to <out>/_pre_replay_backup/")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    # Replay processes millions of events; per-event INFO chatter from the
    # detector and pattern tracker is informational only (not data) and
    # dominates wall-clock time at this scale. Silence it unless --verbose.
    if not args.verbose:
        for noisy in (
            "src.analyzers.online.threshold_detector",
            "src.analyzers.online.historical_pattern_tracker",
            "__main__",  # legacy module logger in main.py
        ):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    out_root = args.out_dir or args.logs
    out_anomalies = out_root / "anomalies"
    out_patterns = out_root / "patterns"

    if args.wipe and not args.dry_run:
        backup = out_root / "_pre_replay_backup"
        backup.mkdir(parents=True, exist_ok=True)
        for sub in ("anomalies", "patterns", "anomaly_index"):
            src = out_root / sub
            if src.exists():
                dst = backup / sub
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.move(str(src), str(dst))
                logger.info("archived %s -> %s", src, dst)

    started = time.time()
    stats = replay(
        logs_root=args.logs,
        out_anomalies_dir=out_anomalies,
        out_patterns_dir=out_patterns,
        start=args.start,
        end=args.end,
        dry_run=args.dry_run,
    )
    elapsed = time.time() - started

    summary = stats.to_dict()
    summary["elapsed_seconds"] = round(elapsed, 1)
    summary["dry_run"] = args.dry_run
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
