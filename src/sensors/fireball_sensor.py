"""NASA/JPL CNEOS fireballs: delayed astronomical context, never a live trigger.

https://ssd-api.jpl.nasa.gov/doc/fireball.html documents the 1.2 schema.
Records may arrive well after the event. Polling them must not create an
apparently synchronous anomaly with unrelated live feeds.
"""

from __future__ import annotations

from datetime import datetime, timezone

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading


API_URL = "https://ssd-api.jpl.nasa.gov/fireball.api?limit=20"


def _number(value):
    return float(value) if value is not None else None


def parse_fireballs(payload: dict) -> list[dict]:
    signature = payload.get("signature") or {}
    if signature.get("version") != "1.2":
        raise ValueError("Unexpected NASA fireball API version")
    fields = payload.get("fields") or []
    required = {"date", "energy", "impact-e"}
    if not required.issubset(fields):
        if int(payload.get("count", 0)) == 0:
            return []
        raise ValueError("NASA fireball API fields missing")
    events = []
    for row in payload.get("data") or []:
        if len(row) != len(fields):
            raise ValueError("NASA fireball API field count changed")
        item = dict(zip(fields, row))
        occurred = datetime.strptime(item["date"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        latitude = _number(item.get("lat"))
        longitude = _number(item.get("lon"))
        if latitude is not None and item.get("lat-dir") == "S":
            latitude = -latitude
        if longitude is not None and item.get("lon-dir") == "W":
            longitude = -longitude
        events.append({
            "id": item["date"], "occurred_at": occurred,
            "latitude": latitude, "longitude": longitude,
            "altitude_km": _number(item.get("alt")),
            "radiated_energy_1e10_j": float(item["energy"]),
            "estimated_impact_kt": float(item["impact-e"]),
        })
    return sorted(events, key=lambda event: event["occurred_at"], reverse=True)


class FireballSensor(BaseSensor):
    def __init__(self, config: SensorConfig | None = None):
        # No event bus: context records cannot enter the anomaly detector.
        super().__init__("fireball", config, event_bus=None)

    async def collect(self) -> SensorReading:
        async with aiohttp.ClientSession(headers={"User-Agent": "MatrixWatcher/1.0 (matrixwatcher.space)"}) as session:
            async with session.get(API_URL, timeout=aiohttp.ClientTimeout(total=20)) as response:
                if response.status != 200:
                    raise RuntimeError(f"NASA fireball API HTTP {response.status}")
                payload = await response.json()
        events = parse_fireballs(payload)
        return SensorReading.create("fireball", {
            "catalogue": "NASA/JPL CNEOS fireballs", "events": events,
            "latest_event_at": events[0]["occurred_at"] if events else None,
            "event_count": len(events), "context_only": True,
        })

    def get_schema(self) -> dict[str, type]:
        return {"events": list, "event_count": int, "context_only": bool}
