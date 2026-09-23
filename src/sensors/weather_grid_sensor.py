"""Frozen six-city Open-Meteo model panel, used only as geographic context.

Open-Meteo's ``current`` values are model estimates, not independent station
observations. A single batched request keeps coordinates, units and the model
time basis consistent. This feed never publishes anomaly events.
"""

from __future__ import annotations

import math
import time

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading

API_URL = "https://api.open-meteo.com/v1/forecast"
LOCATIONS = (
    ("new_york", "New York", 40.7128, -74.0060),
    ("riga", "Riga", 56.9496, 24.1052),
    ("sao_paulo", "São Paulo", -23.5505, -46.6333),
    ("cape_town", "Cape Town", -33.9249, 18.4241),
    ("tokyo", "Tokyo", 35.6762, 139.6503),
    ("sydney", "Sydney", -33.8688, 151.2093),
)
VARIABLES = "temperature_2m,surface_pressure,relative_humidity_2m,weather_code"
MAX_MODEL_LAG_SECONDS = 3600


def _finite_number(value: object, low: float, high: float) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and low <= value <= high


def summarize_weather_grid(payload: object, now: float) -> dict:
    """Validate a batch without treating absent or stale model cells as data."""
    rows = payload if isinstance(payload, list) else [payload]
    if not isinstance(rows, list):
        rows = []
    cells = []
    missing = []
    for index, (key, label, latitude, longitude) in enumerate(LOCATIONS):
        row = rows[index] if index < len(rows) and isinstance(rows[index], dict) else {}
        current = row.get("current") if isinstance(row.get("current"), dict) else {}
        units = row.get("current_units") if isinstance(row.get("current_units"), dict) else {}
        valid_at = current.get("time")
        lag = now - valid_at if _finite_number(valid_at, 0, now + 120) else None
        temp = current.get("temperature_2m")
        pressure = current.get("surface_pressure")
        humidity = current.get("relative_humidity_2m")
        reasons = []
        if row.get("location_id", index) != index:
            reasons.append("location order")
        if units.get("temperature_2m") != "°C" or units.get("surface_pressure") != "hPa" or units.get("time") != "unixtime":
            reasons.append("units")
        if lag is None or lag < -120 or lag > MAX_MODEL_LAG_SECONDS:
            reasons.append("model time")
        if not _finite_number(temp, -100, 65) or not _finite_number(pressure, 800, 1100):
            reasons.append("temperature or pressure")
        if not _finite_number(humidity, 0, 100):
            reasons.append("humidity")
        cell = {"id": key, "label": label, "latitude": latitude, "longitude": longitude,
                "status": "ok" if not reasons else "missing",
                "model_valid_at": valid_at if isinstance(valid_at, (int, float)) else None,
                "retrieval_lag_seconds": round(lag) if lag is not None else None,
                "temperature_celsius": round(temp, 1) if not reasons else None,
                "pressure_hpa": round(pressure, 1) if not reasons else None,
                "humidity_percent": round(humidity) if not reasons else None,
                "weather_code": current.get("weather_code") if not reasons else None}
        if reasons:
            cell["issues"] = reasons
            missing.append(label)
        cells.append(cell)
    return {"cells": cells, "fresh_locations": len(cells) - len(missing),
            "expected_locations": len(LOCATIONS), "data_kind": "weather_model_current",
            "context_only": True,
            "quality": {"complete": not missing,
                        "missing_fields": [f"{label}: missing or stale model data" for label in missing]}}


class WeatherGridSensor(BaseSensor):
    def __init__(self, config: SensorConfig | None = None):
        super().__init__("weather_grid", config, event_bus=None)

    async def collect(self) -> SensorReading:
        params = {
            "latitude": ",".join(str(row[2]) for row in LOCATIONS),
            "longitude": ",".join(str(row[3]) for row in LOCATIONS),
            "current": VARIABLES,
            "temperature_unit": "celsius", "wind_speed_unit": "ms",
            "timeformat": "unixtime", "timezone": "GMT", "forecast_days": 1,
        }
        async with aiohttp.ClientSession(headers={"User-Agent": "MatrixWatcher/1.0 (matrixwatcher.space)"}) as session:
            async with session.get(API_URL, params=params, timeout=aiohttp.ClientTimeout(total=20)) as response:
                if response.status != 200:
                    raise RuntimeError(f"Open-Meteo API HTTP {response.status}")
                payload = await response.json()
        result = summarize_weather_grid(payload, time.time())
        if not result["fresh_locations"]:
            raise RuntimeError("Open-Meteo returned no fresh city model values")
        return SensorReading.create(self.name, result)

    def get_schema(self) -> dict[str, type]:
        return {"cells": list, "fresh_locations": int, "context_only": bool}
