"""Solar Wind Sensor — real-time DSCOVR/ACE plasma + IMF from NOAA SWPC.

This is the physical *driver* upstream of our existing space_weather (Kp) and
solar_activity sensors: a southward interplanetary magnetic field (negative
Bz_GSM) combined with elevated solar-wind speed reconnects with Earths
magnetosphere and produces a geomagnetic storm 1-6 hours later. Unlike the
3-hourly Kp index, this is the continuous precursor — so it is the one stream
where our forward predict-verify loop can plausibly earn *real* skill on an
intra-domain target (solar_wind -> geomagnetic_storm), rather than a coincidence.

Endpoints (real-time, no API key, plain HTTPS — verified reachable from the box):
  - https://services.swpc.noaa.gov/products/solar-wind/plasma-5-minute.json
  - https://services.swpc.noaa.gov/products/solar-wind/mag-5-minute.json
Both are arrays-of-arrays with a header row first and chronological rows after
(last row = most recent).

NB: cosmic-ray neutron-monitor data (NMDB / Oulu / IZMIRAN) was surveyed again
and remains unreachable from this host (TLS to those EU servers does not connect),
so the Forbush/cosmic-ray physics stays covered via this sensor + proton flux.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


def _parse_product(rows: list[list], field: str) -> float | None:
    """Latest numeric value of `field` from a SWPC product array (header row first)."""
    if not rows or len(rows) < 2:
        return None
    header = rows[0]
    if field not in header:
        return None
    idx = header.index(field)
    # rows[1:] are chronological; walk from the end for the newest non-null value
    for row in reversed(rows[1:]):
        try:
            val = row[idx]
        except (IndexError, TypeError):
            continue
        if val is None or val == "":
            continue
        try:
            return float(val)
        except (TypeError, ValueError):
            continue
    return None


class SolarWindSensor(BaseSensor):
    """Real-time solar-wind plasma (speed/density) and IMF (Bz/Bt) from NOAA SWPC."""

    PLASMA_URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-5-minute.json"
    MAG_URL = "https://services.swpc.noaa.gov/products/solar-wind/mag-5-minute.json"

    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None):
        super().__init__("solar_wind", config, event_bus)

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> list | None:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning("Solar wind: %s returned HTTP %s", url, resp.status)
                    return None
                return await resp.json()
        except Exception as e:
            logger.warning("Solar wind: failed to fetch %s: %s", url, e)
            return None

    async def collect(self) -> SensorReading:
        async with aiohttp.ClientSession() as session:
            plasma = await self._fetch(session, self.PLASMA_URL) or []
            mag = await self._fetch(session, self.MAG_URL) or []

        speed = _parse_product(plasma, "speed")          # km/s
        density = _parse_product(plasma, "density")      # protons/cm^3
        bz = _parse_product(mag, "bz_gsm")               # nT (negative = southward = geoeffective)
        bt = _parse_product(mag, "bt")                   # nT (total field magnitude)

        if speed is None and bz is None and bt is None:
            raise RuntimeError("solar_wind: both NOAA SWPC endpoints failed")

        # Derived non-negative "southward component": bz_south = max(0, -bz).
        # The threshold detector only triggers on values ABOVE a level, so we
        # expose the geoeffective magnitude directly (bz_south >= 10 nT == strong
        # southward IMF, the classic storm driver).
        bz_south = max(0.0, -bz) if bz is not None else 0.0

        return SensorReading.create(
            source="solar_wind",
            data={
                "speed": float(speed) if speed is not None else 0.0,
                "density": float(density) if density is not None else 0.0,
                "bz_gsm": float(bz) if bz is not None else 0.0,
                "bt": float(bt) if bt is not None else 0.0,
                "bz_south": float(bz_south),
                "fetched_at_utc": datetime.now(tz=timezone.utc).isoformat(),
            },
        )

    def get_schema(self) -> dict[str, type]:
        return {
            "speed": float,
            "density": float,
            "bz_gsm": float,
            "bt": float,
            "bz_south": float,
        }
