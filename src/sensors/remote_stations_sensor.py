"""Public NWS station observations, shown as delayed atmospheric context.

These are physical observations distributed through one NWS API. They are not
independent anomaly votes, and polling the latest report does not create a
new observation when the station has not reported again.
"""

from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading

STATIONS = (
    ("KJFK", "New York · JFK"),
    ("KLAX", "Los Angeles · LAX"),
    ("KSEA", "Seattle · SEA"),
)
MAX_OBSERVATION_AGE_SECONDS = 5400
API_URL = "https://api.weather.gov/stations/{station}/observations/latest"


def _number(properties: dict, name: str, unit: str, low: float, high: float) -> float | None:
    item = properties.get(name)
    if not isinstance(item, dict) or item.get("unitCode") != unit:
        return None
    value = item.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value) or not low <= value <= high:
        return None
    return float(value)


def summarize_station_observations(payloads: dict, now: float) -> dict:
    """Accept only identified, fresh reports with declared units and valid values."""
    stations = []
    missing = []
    for station_id, label in STATIONS:
        payload = payloads.get(station_id)
        properties = payload.get("properties") if isinstance(payload, dict) else None
        properties = properties if isinstance(properties, dict) else {}
        observation_id = payload.get("id") if isinstance(payload, dict) else None
        stamp = properties.get("timestamp")
        observed_at = None
        try:
            if isinstance(stamp, str):
                observed_at = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
        age = now - observed_at if observed_at is not None else None
        temp = _number(properties, "temperature", "wmoUnit:degC", -100, 65)
        pressure = _number(properties, "barometricPressure", "wmoUnit:Pa", 80000, 110000)
        humidity = _number(properties, "relativeHumidity", "wmoUnit:percent", 0, 100)
        issues = []
        if not isinstance(observation_id, str) or not observation_id.startswith(
            f"https://api.weather.gov/stations/{station_id}/observations/"
        ):
            issues.append("station identity")
        if age is None or age < -120 or age > MAX_OBSERVATION_AGE_SECONDS:
            issues.append("observation time")
        if temp is None:
            issues.append("temperature or unit")
        if pressure is None:
            issues.append("barometric pressure or unit")
        if humidity is None:
            issues.append("humidity or unit")
        good = not issues
        row = {
            "id": station_id, "label": label, "status": "ok" if good else "missing",
            "observed_at": observed_at, "reporting_lag_seconds": round(age) if age is not None else None,
            "observation_id": observation_id if good else None,
            "temperature_celsius": round(temp, 1) if good else None,
            "barometric_pressure_hpa": round(pressure / 100, 1) if good else None,
            "humidity_percent": round(humidity) if good else None,
        }
        if issues:
            row["issues"] = issues
            missing.append(station_id)
        stations.append(row)
    return {
        "stations": stations, "fresh_stations": len(stations) - len(missing),
        "expected_stations": len(STATIONS), "data_kind": "physical_station_observation",
        "context_only": True,
        "quality": {"complete": not missing,
                    "missing_fields": [f"{station}: missing or stale observation" for station in missing]},
    }


class RemoteStationsSensor(BaseSensor):
    def __init__(self, config: SensorConfig | None = None):
        super().__init__("remote_stations", config, event_bus=None)

    async def collect(self) -> SensorReading:
        async with aiohttp.ClientSession(headers={
            "User-Agent": "MatrixWatcher/1.0 (https://matrixwatcher.space)",
            "Accept": "application/geo+json",
        }) as session:
            async def get_one(station_id: str) -> dict:
                async with session.get(API_URL.format(station=station_id),
                                       timeout=aiohttp.ClientTimeout(total=18)) as response:
                    if response.status != 200:
                        raise RuntimeError(f"NWS {station_id} HTTP {response.status}")
                    return await response.json()

            responses = await asyncio.gather(
                *(get_one(station_id) for station_id, _ in STATIONS),
                return_exceptions=True,
            )
        payloads = {station_id: result for (station_id, _), result in zip(STATIONS, responses)
                    if isinstance(result, dict)}
        result = summarize_station_observations(payloads, time.time())
        if not result["fresh_stations"]:
            raise RuntimeError("NWS returned no fresh station observations")
        return SensorReading.create(self.name, result)

    def get_schema(self) -> dict[str, type]:
        return {"stations": list, "fresh_stations": int, "context_only": bool}
