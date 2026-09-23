"""Validate and durably store timestamped readings from two real stations.

This is a local CLI boundary, intended to be called through a restricted SSH
account after physical commissioning. No HTTP endpoint, anonymous upload or
synthetic station data is enabled by installing this module.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROTOCOL_VERSION = 1
MAX_DELAY_SECONDS = 600
MAX_CLOCK_ERROR_MS = 100


def _number(value, low, high):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and low <= value <= high


def validate_reading(payload: dict, registry: dict, received_at: float) -> dict:
    """Reject unknown devices, stale clocks, expired calibration and bad units."""
    if payload.get("protocol_version") != PROTOCOL_VERSION:
        raise ValueError("unsupported protocol version")
    station_id = payload.get("station_id")
    stations = registry.get("stations") or {}
    if not isinstance(station_id, str) or not re.fullmatch(r"[a-z0-9_]{1,32}", station_id):
        raise ValueError("station is not commissioned")
    device = stations.get(station_id) if isinstance(stations, dict) else None
    if not isinstance(device, dict) or not device.get("enabled"):
        raise ValueError("station is not commissioned")
    if payload.get("calibration_id") != device.get("calibration_id"):
        raise ValueError("calibration ID mismatch")
    valid_until = device.get("calibration_valid_until")
    if not _number(valid_until, 0, float("inf")) or received_at >= valid_until:
        raise ValueError("calibration expired")
    sequence = payload.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
        raise ValueError("invalid sequence")
    observed_at = payload.get("observed_at")
    if not _number(observed_at, 0, received_at + 2):
        raise ValueError("invalid UTC observation time")
    delay = received_at - observed_at
    if delay < -2 or delay > MAX_DELAY_SECONDS:
        raise ValueError("station reading delayed beyond live observation limit")
    if not _number(payload.get("ntp_offset_ms"), -MAX_CLOCK_ERROR_MS, MAX_CLOCK_ERROR_MS):
        raise ValueError("clock offset exceeds protocol limit")
    if not _number(payload.get("clock_uncertainty_ms"), 0, MAX_CLOCK_ERROR_MS):
        raise ValueError("clock uncertainty exceeds protocol limit")
    for key, low, high in (("temperature_celsius", -60, 65),
                           ("humidity_percent", 0, 100),
                           ("pressure_hpa", 800, 1100)):
        if not _number(payload.get(key), low, high):
            raise ValueError(f"invalid {key}")
    return {"protocol_version": PROTOCOL_VERSION, "station_id": station_id,
            "site_code": device.get("site_code"), "sequence": sequence,
            "observed_at": observed_at, "received_at": received_at,
            "reporting_delay_seconds": round(delay, 3),
            "ntp_offset_ms": payload["ntp_offset_ms"],
            "clock_uncertainty_ms": payload["clock_uncertainty_ms"],
            "calibration_id": payload["calibration_id"],
            "temperature_celsius": payload["temperature_celsius"],
            "humidity_percent": payload["humidity_percent"],
            "pressure_hpa": payload["pressure_hpa"],
            "data_kind": "physical_station_observation", "context_only": True}


def ingest(payload: dict, registry: dict, logs: Path, received_at: float | None = None) -> dict:
    received_at = time.time() if received_at is None else received_at
    row = validate_reading(payload, registry, received_at)
    station_id = row["station_id"]
    directory = logs / "stations" / station_id
    directory.mkdir(parents=True, exist_ok=True)
    state_path = directory / "sequence.json"
    lock_path = directory / ".ingest.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            state = json.loads(state_path.read_text())
        except (OSError, json.JSONDecodeError):
            state = {"last_sequence": -1}
        if row["sequence"] <= state.get("last_sequence", -1):
            raise ValueError("duplicate or out-of-order station sequence")
        if row["observed_at"] < state.get("last_observed_at", 0):
            raise ValueError("station observation time moved backwards")
        day = datetime.fromtimestamp(row["observed_at"], timezone.utc).date().isoformat()
        path = directory / f"{day}.jsonl"
        # If the prior process crashed after fsync of the JSONL row but before
        # the small sequence index was replaced, a retry is still rejected.
        try:
            with path.open("rb") as existing:
                existing.seek(max(0, existing.seek(0, 2) - 4096))
                last_line = existing.read().splitlines()[-1]
            if row["sequence"] <= json.loads(last_line).get("sequence", -1):
                raise ValueError("duplicate or out-of-order station sequence")
        except (FileNotFoundError, IndexError, json.JSONDecodeError):
            pass
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        tmp = state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last_sequence": row["sequence"],
                                   "last_observed_at": row["observed_at"]}))
        tmp.replace(state_path)
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--logs", type=Path, default=Path("logs"))
    args = parser.parse_args()
    registry = json.loads(args.registry.read_text())
    payload = json.load(sys.stdin)
    row = ingest(payload, registry, args.logs)
    print(json.dumps({"stored": True, "station_id": row["station_id"],
                      "sequence": row["sequence"]}))


if __name__ == "__main__":
    main()
