"""Earth Tides Sensor — solid-earth tidal gravity, computed LOCALLY (no network).

This is the one stream that is physically impossible to correlate through any
shared infrastructure failure: it is pure deterministic astronomy. From the
geocentric positions of the Moon and Sun (via the offline `ephem` ephemeris) we
compute the degree-2 equilibrium tidal acceleration — the vertical "gravity
tide" — at a fixed ground station. If the internet, the ISP, Cloudflare, and
every API on Earth went dark, this sensor would keep emitting exact values.

DESIGN — covariate, NOT an anomaly source. The tidal signal is a smooth
deterministic sinusoid (diurnal + semidiurnal + the spring/neap envelope); it
has no "anomalies" in the robust-z sense, and forcing it into the detector would
only spam predictable peaks and inflate the look-elsewhere problem we removed
with the time-of-day keys. So earth_tides is deliberately kept OUT of the
HybridDetector FEATURE_SPEC and out of NAMED_EVENTS: it is logged as a phase
covariate. The real scientific question — do OTHER domains' anomalies cluster by
tidal phase (e.g. the known weak tidal triggering of shallow earthquakes)? — is
answered OFFLINE by replaying logs/anomalies against this signal with proper
multiple-comparison correction, not by a live blanket split.

Vertical (radial) tidal acceleration of one body, at the surface:
    a_r = (G * M * R / d^3) * (3 * cos^2(psi) - 1)
where psi is the geocentric zenith angle of the body at the station,
    cos(psi) = sin(lat) sin(dec) + cos(lat) cos(dec) cos(H),  H = LST - RA.
Total tide = Moon + Sun. Output in nm/s^2 (1 nm/s^2 = 0.1 microGal); the
peak-to-peak solid-earth tide is ~+-1100 nm/s^2.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone

import ephem

from .base import BaseSensor, SensorConfig
from ..core.event_bus import EventBus
from ..core.types import SensorReading

logger = logging.getLogger(__name__)

_G = 6.67430e-11          # gravitational constant, m^3 kg^-1 s^-2
_R_EARTH = 6.371e6        # mean Earth radius, m
_M_MOON = 7.342e22        # kg
_M_SUN = 1.98892e30       # kg
_AU = 1.495978707e11      # m


def _body_vertical_accel(body, lat_rad: float, lst_rad: float, mass: float) -> tuple[float, float]:
    """Return (vertical tidal accel in m/s^2, altitude_deg) for a geocentric body."""
    ra = float(body.a_ra)        # apparent geocentric RA (rad)
    dec = float(body.a_dec)      # apparent geocentric Dec (rad)
    dist = float(body.earth_distance) * _AU  # m
    hour_angle = lst_rad - ra
    cos_psi = (math.sin(lat_rad) * math.sin(dec)
               + math.cos(lat_rad) * math.cos(dec) * math.cos(hour_angle))
    a_r = (_G * mass * _R_EARTH / dist ** 3) * (3.0 * cos_psi * cos_psi - 1.0)
    alt_deg = math.degrees(math.asin(max(-1.0, min(1.0, cos_psi))))
    return a_r, alt_deg


class EarthTidesSensor(BaseSensor):
    """Solid-earth gravity tide at a fixed station, computed offline from ephem."""

    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None):
        super().__init__("earth_tides", config, event_bus)
        params = (config.custom_params if config and config.custom_params else {}) or {}
        # Default station: New York (matches the weather sensor's locale).
        self.lat = float(params.get("latitude", 40.7128))
        self.lon = float(params.get("longitude", -74.0060))

    async def collect(self) -> SensorReading:
        now = datetime.now(tz=timezone.utc)
        obs = ephem.Observer()
        obs.lat = math.radians(self.lat)
        obs.lon = math.radians(self.lon)
        obs.elevation = 0
        obs.date = ephem.Date(now)
        lst_rad = float(obs.sidereal_time())  # local apparent sidereal time (rad)

        moon = ephem.Moon(obs)
        sun = ephem.Sun(obs)
        lat_rad = math.radians(self.lat)

        a_moon, moon_alt = _body_vertical_accel(moon, lat_rad, lst_rad, _M_MOON)
        a_sun, sun_alt = _body_vertical_accel(sun, lat_rad, lst_rad, _M_SUN)
        total = (a_moon + a_sun) * 1e9  # -> nm/s^2

        return SensorReading.create(
            source="earth_tides",
            data={
                "vertical_nm_s2": float(total),
                "moon_nm_s2": float(a_moon * 1e9),
                "sun_nm_s2": float(a_sun * 1e9),
                "moon_alt_deg": float(moon_alt),
                "sun_alt_deg": float(sun_alt),
                "lunar_illumination_pct": float(moon.phase),  # 0..100
                "station_lat": self.lat,
                "station_lon": self.lon,
                "computed_at_utc": now.isoformat(),
            },
        )

    def get_schema(self) -> dict[str, type]:
        return {
            "vertical_nm_s2": float,
            "moon_nm_s2": float,
            "sun_nm_s2": float,
            "moon_alt_deg": float,
            "sun_alt_deg": float,
            "lunar_illumination_pct": float,
        }
