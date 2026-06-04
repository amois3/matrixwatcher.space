"""Solar Activity Sensor — F10.7 cm radio flux, GOES X-ray flux, proton flux.

These three NOAA SWPC feeds capture the physical drivers we hoped Schumann
resonance and cosmic-ray flux would surface (Schumann's amplitude is modulated
by ionospheric conductivity, which solar X-rays drive; cosmic-ray ground
intensity is anti-correlated with solar activity via the Forbush effect). The
public Schumann and NMDB cosmic-ray endpoints we surveyed required JS sessions
or had no clean API, so we cover the same physics through endpoints we can
reliably fetch.

Endpoints (real-time, no API key):
  - https://services.swpc.noaa.gov/json/f107_cm_flux.json
  - https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json
  - https://services.swpc.noaa.gov/json/goes/primary/integral-protons-1-day.json
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


def _xray_flare_class(flux: float | None) -> str:
    """Map peak 0.1–0.8 nm flux (W/m²) to NOAA flare class letter."""
    if flux is None or flux <= 0:
        return "none"
    if flux >= 1e-4:
        return "X"
    if flux >= 1e-5:
        return "M"
    if flux >= 1e-6:
        return "C"
    if flux >= 1e-7:
        return "B"
    return "A"


def parse_f107(records: list[dict]) -> tuple[float | None, float | None]:
    """Return (latest_flux, pct_change_vs_previous)."""
    # Records are NOT sorted chronologically (`first` is most recent).
    sorted_recs = sorted(
        (r for r in records if r.get("flux") is not None and r.get("time_tag")),
        key=lambda r: r["time_tag"],
    )
    if not sorted_recs:
        return None, None
    latest = float(sorted_recs[-1]["flux"])
    if len(sorted_recs) < 2:
        return latest, None
    previous = float(sorted_recs[-2]["flux"])
    pct = ((latest - previous) / previous) * 100.0 if previous else None
    return latest, pct


def parse_xray_peak(records: list[dict], band: str = "0.1-0.8nm") -> tuple[float | None, str]:
    """Return (peak_flux_W_per_m2, NOAA flare class letter) over the recorded window."""
    fluxes = [
        float(r["flux"])
        for r in records
        if r.get("energy") == band and r.get("flux") is not None and not r.get("electron_contaminaton", False)
    ]
    if not fluxes:
        return None, "none"
    peak = max(fluxes)
    return peak, _xray_flare_class(peak)


def parse_proton(records: list[dict], energy_band: str = ">=10 MeV") -> float | None:
    """Latest integral proton flux at the given energy threshold (pfu)."""
    candidates = [
        r for r in records
        if r.get("energy") == energy_band and r.get("flux") is not None and r.get("time_tag")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda r: r["time_tag"])
    return float(candidates[-1]["flux"])


class SolarActivitySensor(BaseSensor):
    """Solar X-ray, F10.7 radio flux, and proton flux from NOAA SWPC."""

    F107_URL = "https://services.swpc.noaa.gov/json/f107_cm_flux.json"
    XRAY_URL = "https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json"
    PROTON_URL = "https://services.swpc.noaa.gov/json/goes/primary/integral-protons-1-day.json"

    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None):
        super().__init__("solar_activity", config, event_bus)

    async def _fetch_json(self, session: aiohttp.ClientSession, url: str) -> list[dict] | None:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning("Solar activity: %s returned HTTP %s", url, resp.status)
                    return None
                return await resp.json()
        except Exception as e:
            logger.warning("Solar activity: failed to fetch %s: %s", url, e)
            return None

    async def collect(self) -> SensorReading:
        async with aiohttp.ClientSession() as session:
            f107_data = await self._fetch_json(session, self.F107_URL) or []
            xray_data = await self._fetch_json(session, self.XRAY_URL) or []
            proton_data = await self._fetch_json(session, self.PROTON_URL) or []

        f107_flux, f107_change_pct = parse_f107(f107_data)
        xray_peak, flare_class = parse_xray_peak(xray_data)
        proton_10mev = parse_proton(proton_data, ">=10 MeV")
        proton_100mev = parse_proton(proton_data, ">=100 MeV")

        # log-scale convenience: A1.0 = 1e-7 -> -7.0
        import math
        xray_log = math.log10(xray_peak) if (xray_peak and xray_peak > 0) else None

        # Coarse "any data?" check
        if f107_flux is None and xray_peak is None and proton_10mev is None:
            raise RuntimeError("solar_activity: all three NOAA endpoints failed")

        # solar event flag (NOAA's own threshold: >= 10 pfu at >=10 MeV is a "proton event")
        is_proton_event = bool(proton_10mev is not None and proton_10mev >= 10.0)

        # A missing feed value is OMITTED, never written as 0.0: f107_flux feeds
        # the adaptive detector, so a fabricated 0.0 (vs the real ~150) would fire
        # a false "crash to zero" anomaly. When a feed is present its parser always
        # returns a real positive number; None means the feed was missing/empty, so
        # omitting the field is the honest representation (the rule just skips that
        # cycle). String tags stay (flare_class is "none" when quiet).
        data: dict = {
            "flare_class": flare_class,
            "proton_event_in_progress": is_proton_event,
            "fetched_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        }
        if f107_flux is not None:
            data["f107_flux"] = float(f107_flux)
        if f107_change_pct is not None:
            data["f107_change_pct"] = float(f107_change_pct)
        if xray_peak is not None:
            data["xray_peak_long_flux"] = float(xray_peak)
            if xray_log is not None:
                data["xray_peak_long_log"] = float(xray_log)
        if proton_10mev is not None:
            data["proton_flux_10mev"] = float(proton_10mev)
        if proton_100mev is not None:
            data["proton_flux_100mev"] = float(proton_100mev)

        return SensorReading.create(source="solar_activity", data=data)

    def get_schema(self) -> dict[str, type]:
        return {
            "f107_flux": float,
            "f107_change_pct": float,
            "xray_peak_long_flux": float,
            "xray_peak_long_log": float,
            "proton_flux_10mev": float,
            "proton_flux_100mev": float,
        }
