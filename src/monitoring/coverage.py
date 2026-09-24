"""Read-only coverage audit for the collector and public dashboard."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..analyzers.online.cluster_detector import source_domain


SENSOR_MODES = {
    "crypto": "anomaly", "blockchain": "anomaly", "weather": "anomaly",
    "news": "anomaly", "earthquake": "anomaly", "space_weather": "anomaly",
    "solar_activity": "anomaly", "solar_wind": "anomaly",
    "quantum_rng": "anomaly", "wikipedia_edits": "anomaly",
    "volcanic_activity": "anomaly", "earth_tides": "covariate",
    "fireball": "context", "ripe_atlas": "context", "weather_grid": "context",
    "remote_stations": "context",
}


def _last_json(path: Path) -> dict | None:
    """Read a JSONL tail without loading an entire high-frequency day."""
    try:
        with path.open("rb") as stream:
            stream.seek(0, 2)
            size = stream.tell()
            if not size:
                return None
            stream.seek(max(0, size - 65536))
            lines = stream.read().splitlines()
        for line in reversed(lines):
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    return value
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
    except OSError:
        pass
    return None


def _latest_reading(logs: Path, sensor: str, now: float) -> dict | None:
    directory = logs / sensor
    if not directory.is_dir():
        return None
    today = datetime.fromtimestamp(now, tz=timezone.utc).date()
    candidates = []
    for day in (today, today - timedelta(days=1)):
        candidates.extend(directory.glob(f"{day.isoformat()}*.jsonl"))
    for path in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
        record = _last_json(path)
        if record:
            return record
    return None


def _issues(name: str, reading: dict) -> list[str]:
    issues = []
    quality = reading.get("quality") or {}
    if quality.get("complete") is False:
        missing = quality.get("missing_pairs") or quality.get("failed_feeds") or quality.get("missing_fields") or []
        issues.append("Missing: " + ", ".join(map(str, missing)))
    if name == "crypto":
        symbols = {pair.get("symbol") for pair in reading.get("pairs", []) if isinstance(pair, dict)}
        for symbol in ("BTCUSDT", "ETHUSDT"):
            if symbol not in symbols:
                issue = f"Missing {symbol} price"
                if issue not in issues:
                    issues.append(issue)
    elif name == "news" and reading.get("feeds_successful", 0) < 3:
        issues.append("Fewer than three news feeds available")
    elif name == "space_weather" and reading.get("kp_index") is None:
        issues.append("Kp measurement unavailable")
    elif name == "quantum_rng" and reading.get("source") != "anu_quantum":
        issues.append("ANU quantum sample unavailable; prior fallback is not quantum")
    elif name == "weather" and (reading.get("from_cache") or reading.get("error")):
        issues.append("Weather model update unavailable; cached values excluded from anomaly detection")
    return issues


def get_coverage(logs: Path = Path("logs"), config_path: Path = Path("config.json"), now: float | None = None) -> dict:
    now = time.time() if now is None else now
    try:
        config = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError):
        config = {}
    configured = config.get("sensors", {})
    sensors = {}
    for name, mode in SENSOR_MODES.items():
        settings = configured.get(name, {})
        enabled = settings.get("enabled", True)
        interval = max(1.0, float(settings.get("interval_seconds", 300)))
        record = _latest_reading(logs, name, now) if enabled else None
        ts = record.get("timestamp") if record else None
        age = max(0, round(now - ts)) if isinstance(ts, (int, float)) else None
        issues = _issues(name, record) if record else []
        if not enabled:
            status = "disabled"
        elif age is None:
            status = "missing"
        elif age > max(3 * interval, 120):
            status = "stale"
        elif issues:
            status = "partial"
        else:
            status = "ok"
        sensors[name] = {
            "status": status, "mode": mode, "domain": source_domain(name),
            "age_seconds": age, "interval_seconds": interval, "issues": issues,
            "last_reading_at": ts,
        }
    try:
        pipeline = json.loads((logs / "pipeline_status.json").read_text())
    except (OSError, json.JSONDecodeError):
        pipeline = None
    if pipeline:
        recent_error = pipeline.get("last_error_at") and now - pipeline["last_error_at"] < 900
        pipeline = {**pipeline, "status": "ok" if
                    now - pipeline.get("updated_at", 0) < 180 and not recent_error
                    else "degraded"}
    else:
        pipeline = {"status": "missing", "errors": None}
    return {
        "timestamp": now,
        "summary": {status: sum(s["status"] == status for s in sensors.values())
                    for status in ("ok", "partial", "stale", "missing", "disabled")},
        "sensors": sensors, "pipeline": pipeline,
    }


def merge_collector_health(coverage: dict, health: dict | None) -> dict:
    """Show a failed *current poll* even while its previous reading is fresh.

    The raw-reading audit remains authoritative for observation age and quality.
    A live collector error adds a warning, never fabricates a missing reading.
    """
    result = {**coverage, "sensors": {name: {**item, "issues": list(item.get("issues", []))}
                                       for name, item in coverage["sensors"].items()}}
    if not isinstance(health, dict) or not isinstance(health.get("sensors"), dict):
        result["collector"] = {"status": "unavailable"}
        return result
    result["collector"] = {"status": health.get("status", "unknown")}
    for name, sensor in result["sensors"].items():
        live = health["sensors"].get(name)
        if not isinstance(live, dict):
            continue
        state = live.get("status")
        if state in ("error", "degraded", "rate_limited", "stopped"):
            if sensor["status"] == "ok":
                sensor["status"] = "partial"
            sensor["issues"].append(f"Current collector status: {state}; latest saved reading may be older")
    result["summary"] = {status: sum(s["status"] == status for s in result["sensors"].values())
                         for status in ("ok", "partial", "stale", "missing", "disabled")}
    return result
