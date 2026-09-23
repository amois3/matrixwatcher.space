"""Fixed global airport METAR panel, used as atmospheric context only.

The source is one Aviation Weather Center API. Each METAR is a physical
observation, but repeated polls and shared distribution are not independent
anomaly evidence. The observation timestamp comes from ``obsTime``.
"""

from __future__ import annotations

import math
import time

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading

STATIONS = (
    ("KJFK", "New York · JFK", "North America", 40.6392, -73.7639),
    ("EVRA", "Riga · RIX", "Europe", 56.9240, 23.9680),
    ("SBGR", "São Paulo · GRU", "South America", -23.4320, -46.4690),
    ("FACT", "Cape Town · CPT", "Africa", -33.9650, 18.6020),
    ("RJTT", "Tokyo · HND", "Asia", 35.5530, 139.7810),
    ("YSSY", "Sydney · SYD", "Oceania", -33.9460, 151.1730),
)
MAX_OBSERVATION_AGE_SECONDS = 5400
API_URL = "https://aviationweather.gov/api/data/metar"


def _number(value: object, low: float, high: float) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or not low <= value <= high:
        return None
    return float(value)


def summarize_station_observations(payload: object, now: float) -> dict:
    """Validate a fixed panel regardless of API row order or missing rows."""
    expected = {station_id for station_id, *_ in STATIONS}
    by_id = {}
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict) or item.get("icaoId") not in expected:
            continue
        station_id = item["icaoId"]
        previous = by_id.get(station_id)
        item_time = _number(item.get("obsTime"), 0, now + 120) or -1
        previous_time = _number(previous.get("obsTime"), 0, now + 120) if previous else None
        if previous is None or item_time > (previous_time if previous_time is not None else -1):
            by_id[station_id] = item

    stations = []
    missing = []
    for station_id, label, region, latitude, longitude in STATIONS:
        report = by_id.get(station_id) or {}
        observed_at = _number(report.get("obsTime"), 0, now + 120)
        age = now - observed_at if observed_at is not None else None
        temp = _number(report.get("temp"), -100, 65)
        dewpoint = _number(report.get("dewp"), -110, 65)
        # The AWC OpenAPI schema defines altim as altimeter setting in hPa.
        altimeter = _number(report.get("altim"), 800, 1100)
        source_lat = _number(report.get("lat"), -90, 90)
        source_lon = _number(report.get("lon"), -180, 180)
        issues = []
        if not report:
            issues.append("station report missing")
        if age is None or age < -120 or age > MAX_OBSERVATION_AGE_SECONDS:
            issues.append("observation time")
        if temp is None:
            issues.append("temperature")
        if altimeter is None:
            issues.append("altimeter setting")
        if (source_lat is None or source_lon is None
                or abs(source_lat - latitude) > 0.3 or abs(source_lon - longitude) > 0.3):
            issues.append("station coordinates")
        good = not issues
        row = {
            "id": station_id, "label": label, "region": region,
            "latitude": latitude, "longitude": longitude,
            "status": "ok" if good else "missing",
            "observed_at": observed_at,
            "reporting_lag_seconds": round(age) if age is not None else None,
            "temperature_celsius": round(temp, 1) if good else None,
            "dewpoint_celsius": round(dewpoint, 1) if good and dewpoint is not None else None,
            "altimeter_hpa": round(altimeter, 1) if good else None,
            "raw_metar": report["rawOb"][:512] if good and isinstance(report.get("rawOb"), str) else None,
        }
        if issues:
            row["issues"] = issues
            missing.append(station_id)
        stations.append(row)
    return {
        "stations": stations, "fresh_stations": len(stations) - len(missing),
        "expected_stations": len(STATIONS), "data_kind": "global_metar_observation",
        "context_only": True,
        "quality": {"complete": not missing,
                    "missing_fields": [f"{station}: missing or stale METAR" for station in missing]},
    }


class RemoteStationsSensor(BaseSensor):
    def __init__(self, config: SensorConfig | None = None):
        super().__init__("remote_stations", config, event_bus=None)

    async def collect(self) -> SensorReading:
        async with aiohttp.ClientSession(headers={
            "User-Agent": "MatrixWatcher/1.0 (https://matrixwatcher.space)",
            "Accept": "application/json",
        }) as session:
            async with session.get(
                API_URL,
                params={"ids": ",".join(row[0] for row in STATIONS), "format": "json"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"AWC METAR API HTTP {response.status}")
                payload = await response.json()
        result = summarize_station_observations(payload, time.time())
        if not result["fresh_stations"]:
            raise RuntimeError("AWC returned no fresh METAR observations")
        return SensorReading.create(self.name, result)

    def get_schema(self) -> dict[str, type]:
        return {"stations": list, "fresh_stations": int, "context_only": bool}
