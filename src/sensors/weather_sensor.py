"""Weather Sensor for Matrix Watcher.

Collects real atmospheric data from the Open-Meteo API (https://open-meteo.com).

Open-Meteo is a free, key-less weather API based on national weather services
(no API key, no registration). City names are resolved to coordinates via the
Open-Meteo geocoding API; coordinates are cached after the first lookup.

No fake data: on failure the sensor returns the last real cached reading
(flagged as such) or an explicit empty reading with the error — never invented
values.
"""

import logging
import time
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)

OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_GEOCODING_API = "https://geocoding-api.open-meteo.com/v1/search"

# WMO weather interpretation codes -> (main, description)
# https://open-meteo.com/en/docs (WMO Weather interpretation codes WW)
WMO_CODES: dict[int, tuple[str, str]] = {
    0: ("Clear", "clear sky"),
    1: ("Clouds", "mainly clear"),
    2: ("Clouds", "partly cloudy"),
    3: ("Clouds", "overcast"),
    45: ("Fog", "fog"),
    48: ("Fog", "depositing rime fog"),
    51: ("Drizzle", "light drizzle"),
    53: ("Drizzle", "moderate drizzle"),
    55: ("Drizzle", "dense drizzle"),
    56: ("Drizzle", "light freezing drizzle"),
    57: ("Drizzle", "dense freezing drizzle"),
    61: ("Rain", "slight rain"),
    63: ("Rain", "moderate rain"),
    65: ("Rain", "heavy rain"),
    66: ("Rain", "light freezing rain"),
    67: ("Rain", "heavy freezing rain"),
    71: ("Snow", "slight snow fall"),
    73: ("Snow", "moderate snow fall"),
    75: ("Snow", "heavy snow fall"),
    77: ("Snow", "snow grains"),
    80: ("Rain", "slight rain showers"),
    81: ("Rain", "moderate rain showers"),
    82: ("Rain", "violent rain showers"),
    85: ("Snow", "slight snow showers"),
    86: ("Snow", "heavy snow showers"),
    95: ("Thunderstorm", "thunderstorm"),
    96: ("Thunderstorm", "thunderstorm with slight hail"),
    99: ("Thunderstorm", "thunderstorm with heavy hail"),
}


def wmo_to_text(code: int | None) -> tuple[str, str]:
    """Map a WMO weather code to (weather_main, weather_description)."""
    if code is None:
        return ("", "")
    return WMO_CODES.get(int(code), ("Unknown", f"wmo code {int(code)}"))


def parse_open_meteo(payload: dict[str, Any], location: str, country: str) -> dict[str, Any]:
    """Parse an Open-Meteo /forecast response into our flat schema.

    Pure function (no I/O) so it can be unit-tested without network access.
    """
    current = payload.get("current", {}) or {}
    code = current.get("weather_code")
    weather_main, weather_description = wmo_to_text(code)

    def _round(value, ndigits=1):
        return round(value, ndigits) if isinstance(value, (int, float)) else value

    return {
        "location": location,
        "country": country,
        "temperature_celsius": _round(current.get("temperature_2m")),
        "feels_like_celsius": _round(current.get("apparent_temperature")),
        "humidity_percent": current.get("relative_humidity_2m"),
        "pressure_hpa": _round(current.get("surface_pressure")),
        "clouds_percent": current.get("cloud_cover"),
        "wind_speed_ms": _round(current.get("wind_speed_10m")),
        "wind_direction_deg": current.get("wind_direction_10m"),
        "weather_code": int(code) if isinstance(code, (int, float)) else None,
        "weather_main": weather_main,
        "weather_description": weather_description,
    }


class WeatherSensor(BaseSensor):
    """Sensor for collecting real weather data via Open-Meteo (no API key).

    Collects:
    - temperature_celsius, feels_like_celsius
    - humidity_percent, pressure_hpa, clouds_percent
    - wind_speed_ms, wind_direction_deg
    - weather_code / weather_main / weather_description (WMO codes)

    Caches the last real reading and reuses it (flagged from_cache) on failure.

    Example:
        sensor = WeatherSensor(location="London")
        reading = await sensor.collect()
        print(f"Temperature: {reading.data['temperature_celsius']}°C")
    """

    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        api_key: str | None = None,  # kept for backward-compat; unused (Open-Meteo needs no key)
        location: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ):
        """Initialize Weather Sensor.

        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            api_key: Deprecated/unused (Open-Meteo requires no key)
            location: City name (e.g., "London" or "New York,US")
            lat: Latitude (alternative to location)
            lon: Longitude (alternative to location)
        """
        super().__init__("weather", config, event_bus)
        self.location = location
        self.lat = lat
        self.lon = lon

        # Resolved coordinates / place label (cached after first geocode)
        self._resolved_lat: float | None = lat
        self._resolved_lon: float | None = lon
        self._place_name: str = (location.split(",")[0].strip() if location else "")
        self._country: str = ""

        # Cache for last known real values
        self._cached_data: dict[str, Any] | None = None
        self._cache_time: float = 0

    async def collect(self) -> SensorReading:
        """Collect real weather data from Open-Meteo."""
        logger.info("Weather: Starting data collection")
        timestamp = time.time()

        try:
            data = await self._fetch_weather()
            if data:
                logger.info(
                    f"Weather: Collected data for {data.get('location', 'Unknown')}, "
                    f"temp={data.get('temperature_celsius')}°C"
                )
                self._cached_data = data
                self._cache_time = timestamp
                return SensorReading.create(self.name, {
                    "timestamp": timestamp,
                    **data,
                    "from_cache": False,
                    "error": None,
                })
            logger.warning("Weather: API returned no data")
            return self._create_cached_or_empty_reading(timestamp, "API returned no data")
        except Exception as e:
            logger.warning(f"Weather: Failed to fetch weather: {e}")
            return self._create_cached_or_empty_reading(timestamp, str(e))

    async def _ensure_coordinates(self, session: aiohttp.ClientSession) -> bool:
        """Resolve lat/lon if not already known. Returns True on success."""
        if self._resolved_lat is not None and self._resolved_lon is not None:
            return True

        # Resolve a configured city name via Open-Meteo geocoding
        if self._place_name:
            try:
                async with session.get(
                    OPEN_METEO_GEOCODING_API,
                    params={"name": self._place_name, "count": 1, "format": "json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        results = (await response.json()).get("results") or []
                        if results:
                            r = results[0]
                            self._resolved_lat = r.get("latitude")
                            self._resolved_lon = r.get("longitude")
                            self._place_name = r.get("name", self._place_name)
                            self._country = r.get("country_code", "") or r.get("country", "")
                            return self._resolved_lat is not None
            except Exception as e:
                logger.debug(f"Geocoding failed for '{self._place_name}': {e}")

        # Fallback: IP-based geolocation
        geo = await self._get_ip_location(session)
        if geo:
            self._resolved_lat = geo["lat"]
            self._resolved_lon = geo["lon"]
            if not self._place_name:
                self._place_name = geo.get("city", "") or "Unknown"
            self._country = self._country or geo.get("country", "")
            return True

        logger.warning("Weather: no coordinates and geolocation failed")
        return False

    async def _fetch_weather(self) -> dict[str, Any] | None:
        """Fetch current weather from Open-Meteo (no key required)."""
        async with aiohttp.ClientSession() as session:
            if not await self._ensure_coordinates(session):
                return None

            params = {
                "latitude": self._resolved_lat,
                "longitude": self._resolved_lon,
                "current": ",".join([
                    "temperature_2m",
                    "relative_humidity_2m",
                    "apparent_temperature",
                    "surface_pressure",
                    "cloud_cover",
                    "wind_speed_10m",
                    "wind_direction_10m",
                    "weather_code",
                ]),
                "wind_speed_unit": "ms",
            }

            async with session.get(
                OPEN_METEO_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status != 200:
                    logger.warning(f"Open-Meteo returned status {response.status}")
                    return None
                payload = await response.json()

        return parse_open_meteo(payload, self._place_name or "Unknown", self._country)

    async def _get_ip_location(self, session: aiohttp.ClientSession) -> dict[str, Any] | None:
        """Get approximate location from IP address (real lookup, no fake fallback)."""
        try:
            async with session.get(
                "http://ip-api.com/json/",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                if data.get("status") == "success":
                    return {
                        "lat": data["lat"],
                        "lon": data["lon"],
                        "city": data.get("city", ""),
                        "country": data.get("countryCode", ""),
                    }
        except Exception as e:
            logger.debug(f"IP geolocation failed: {e}")
        return None

    def _create_cached_or_empty_reading(self, timestamp: float, error: str) -> SensorReading:
        """Return last real cached reading, or an explicit empty reading. Never fabricate."""
        if self._cached_data:
            return SensorReading.create(self.name, {
                "timestamp": timestamp,
                **self._cached_data,
                "from_cache": True,
                "cache_age_seconds": round(timestamp - self._cache_time, 1),
                "error": error,
            })

        return SensorReading.create(self.name, {
            "timestamp": timestamp,
            "location": None,
            "temperature_celsius": None,
            "humidity_percent": None,
            "pressure_hpa": None,
            "clouds_percent": None,
            "wind_speed_ms": None,
            "from_cache": False,
            "error": error,
        })

    def get_schema(self) -> dict[str, type]:
        """Get schema for weather sensor data."""
        return {
            "timestamp": float,
            "from_cache": bool,
        }
