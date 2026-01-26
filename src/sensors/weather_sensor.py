"""Weather Sensor for Matrix Watcher.

Collects weather data from OpenWeatherMap API.
"""

import logging
import time
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)

OPENWEATHERMAP_API = "https://api.openweathermap.org/data/2.5/weather"


class WeatherSensor(BaseSensor):
    """Sensor for collecting weather data.
    
    Uses OpenWeatherMap API to collect:
    - temperature_celsius: Current temperature
    - humidity_percent: Relative humidity
    - pressure_hpa: Atmospheric pressure
    - clouds_percent: Cloud coverage
    - wind_speed_ms: Wind speed in m/s
    
    Caches last known values on API failure.
    
    Example:
        sensor = WeatherSensor(api_key="your_key", location="London")
        reading = await sensor.collect()
        print(f"Temperature: {reading.data['temperature_celsius']}°C")
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        api_key: str | None = None,
        location: str | None = None,
        lat: float | None = None,
        lon: float | None = None
    ):
        """Initialize Weather Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            api_key: OpenWeatherMap API key
            location: City name (e.g., "London" or "London,UK")
            lat: Latitude (alternative to location)
            lon: Longitude (alternative to location)
        """
        super().__init__("weather", config, event_bus)
        self.api_key = api_key
        self.location = location
        self.lat = lat
        self.lon = lon
        
        # Cache for last known values
        self._cached_data: dict[str, Any] | None = None
        self._cache_time: float = 0
    
    async def collect(self) -> SensorReading:
        """Collect weather data."""
        logger.info("Weather: Starting data collection")
        timestamp = time.time()
        
        if not self.api_key:
            logger.warning("Weather: No OpenWeatherMap API key configured")
            return self._create_cached_or_empty_reading(timestamp, "No API key")
        
        try:
            data = await self._fetch_weather()
            if data:
                logger.info(f"Weather: Collected data for {data.get('location', 'Unknown')}, temp={data.get('temperature_celsius')}°C")
                self._cached_data = data
                self._cache_time = timestamp
                return SensorReading.create(self.name, {
                    "timestamp": timestamp,
                    **data,
                    "from_cache": False,
                    "error": None
                })
            else:
                logger.warning("Weather: API returned no data")
                return self._create_cached_or_empty_reading(timestamp, "API returned no data")
                
        except Exception as e:
            logger.warning(f"Weather: Failed to fetch weather: {e}")
            return self._create_cached_or_empty_reading(timestamp, str(e))
    
    async def _fetch_weather(self) -> dict[str, Any] | None:
        """Fetch weather data from API."""
        params = {"appid": self.api_key, "units": "metric"}
        
        if self.lat is not None and self.lon is not None:
            params["lat"] = self.lat
            params["lon"] = self.lon
        elif self.location:
            params["q"] = self.location
        else:
            # Try IP-based geolocation
            geo = await self._get_ip_location()
            if geo:
                params["lat"] = geo["lat"]
                params["lon"] = geo["lon"]
            else:
                logger.warning("No location configured and IP geolocation failed")
                return None
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                OPENWEATHERMAP_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status != 200:
                    logger.warning(f"OpenWeatherMap returned status {response.status}")
                    return None
                
                data = await response.json()
        
        main = data.get("main", {})
        wind = data.get("wind", {})
        clouds = data.get("clouds", {})
        
        return {
            "location": data.get("name", "Unknown"),
            "country": data.get("sys", {}).get("country", ""),
            "temperature_celsius": round(main.get("temp", 0), 1),
            "feels_like_celsius": round(main.get("feels_like", 0), 1),
            "humidity_percent": main.get("humidity", 0),
            "pressure_hpa": main.get("pressure", 0),
            "clouds_percent": clouds.get("all", 0),
            "wind_speed_ms": round(wind.get("speed", 0), 1),
            "wind_direction_deg": wind.get("deg", 0),
            "visibility_m": data.get("visibility", 0),
            "weather_main": data.get("weather", [{}])[0].get("main", ""),
            "weather_description": data.get("weather", [{}])[0].get("description", "")
        }
    
    async def _get_ip_location(self) -> dict[str, float] | None:
        """Get location from IP address."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "http://ip-api.com/json/",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status != 200:
                        return None
                    data = await response.json()
                    if data.get("status") == "success":
                        return {"lat": data["lat"], "lon": data["lon"]}
        except Exception as e:
            logger.debug(f"IP geolocation failed: {e}")
        return None
    
    def _create_cached_or_empty_reading(
        self, 
        timestamp: float, 
        error: str
    ) -> SensorReading:
        """Create reading from cache or empty data."""
        if self._cached_data:
            return SensorReading.create(self.name, {
                "timestamp": timestamp,
                **self._cached_data,
                "from_cache": True,
                "cache_age_seconds": round(timestamp - self._cache_time, 1),
                "error": error
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
            "error": error
        })
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for weather sensor data."""
        return {
            "timestamp": float,
            "from_cache": bool
        }
