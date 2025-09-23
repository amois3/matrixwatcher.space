"""Space Weather Sensor - Solar Activity and Geomagnetic Storms.

Monitors solar flares, CMEs, and geomagnetic activity.
"""

import logging
from typing import Any
import aiohttp
from datetime import datetime, timedelta

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


class SpaceWeatherSensor(BaseSensor):
    """Sensor for monitoring space weather and solar activity.
    
    Uses NOAA Space Weather Prediction Center API.
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None
    ):
        """Initialize Space Weather Sensor."""
        super().__init__("space_weather", config, event_bus)
        self.base_url = "https://services.swpc.noaa.gov/json"
    
    async def collect(self) -> SensorReading:
        """Collect space weather data."""
        logger.info("Space Weather: Starting data collection")
        async with aiohttp.ClientSession() as session:
            # Get solar flares
            flares = await self._get_solar_flares(session)
            logger.info(f"Space Weather: Found {len(flares)} flares")
            
            # Get geomagnetic K-index
            kp_index = await self._get_kp_index(session)
            logger.info(f"Space Weather: Kp index = {kp_index}")
            
            # Get solar wind
            solar_wind = await self._get_solar_wind(session)
            logger.info(f"Space Weather: Solar wind = {solar_wind}")
            
            # Determine alert level
            alert_level = self._calculate_alert_level(flares, kp_index, solar_wind)
            
            reading = SensorReading.create(
                source="space_weather",
                data={
                    "solar_flares_24h": len(flares),
                    "flare_count": len(flares),  # For detector compatibility
                    "max_flare_class": flares[0]["class_type"] if flares else "A",
                    "kp_index": kp_index,
                    "geomagnetic_storm": kp_index >= 5,
                    "solar_wind_speed_kms": solar_wind.get("speed", 0),
                    "solar_wind_density": solar_wind.get("density", 0),
                    "alert_level": alert_level,
                    "recent_flares": flares[:3]
                }
            )
            logger.info(f"Space Weather: Collection complete, alert_level={alert_level}")
            return reading
    
    async def _get_solar_flares(self, session: aiohttp.ClientSession) -> list[dict]:
        """Get recent solar flares."""
        try:
            url = f"{self.base_url}/goes/xrs-2-day.json"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return []
                
                data = await response.json()
                
                # Parse flares (simplified - look for X-ray flux spikes)
                flares = []
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                
                for entry in data[-100:]:  # Last 100 entries
                    try:
                        time_str = entry.get("time_tag", "")
                        flux = entry.get("flux", 0)
                        
                        if flux > 1e-6:  # M-class or higher
                            flare_class = self._classify_flare(flux)
                            flares.append({
                                "time": time_str,
                                "flux": flux,
                                "class_type": flare_class
                            })
                    except:
                        continue
                
                return flares
        except Exception as e:
            logger.warning(f"Failed to get solar flares: {e}")
            return []
    
    def _classify_flare(self, flux: float) -> str:
        """Classify solar flare by X-ray flux."""
        if flux >= 1e-4:
            return "X"  # X-class (major)
        elif flux >= 1e-5:
            return "M"  # M-class (medium)
        elif flux >= 1e-6:
            return "C"  # C-class (minor)
        elif flux >= 1e-7:
            return "B"  # B-class (small)
        else:
            return "A"  # A-class (minimal)
    
    async def _get_kp_index(self, session: aiohttp.ClientSession) -> float:
        """Get current Kp index (geomagnetic activity)."""
        try:
            url = f"{self.base_url}/planetary_k_index_1m.json"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return 0.0
                
                data = await response.json()
                if data:
                    latest = data[-1]
                    return float(latest.get("kp_index", 0))
                return 0.0
        except Exception as e:
            logger.warning(f"Failed to get Kp index: {e}")
            return 0.0
    
    async def _get_solar_wind(self, session: aiohttp.ClientSession) -> dict:
        """Get solar wind data."""
        try:
            url = f"{self.base_url}/rtsw/rtsw_wind_1m.json"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return {}
                
                data = await response.json()
                if data:
                    latest = data[-1]
                    return {
                        "speed": float(latest.get("wind_speed", 0)),
                        "density": float(latest.get("density", 0))
                    }
                return {}
        except Exception as e:
            logger.warning(f"Failed to get solar wind: {e}")
            return {}
    
    def _calculate_alert_level(self, flares: list, kp: float, solar_wind: dict) -> str:
        """Calculate overall alert level."""
        score = 0
        
        # Check for X-class flares
        if any(f["class_type"] == "X" for f in flares):
            score += 3
        elif any(f["class_type"] == "M" for f in flares):
            score += 2
        
        # Check Kp index
        if kp >= 7:
            score += 3
        elif kp >= 5:
            score += 2
        elif kp >= 4:
            score += 1
        
        # Check solar wind speed
        speed = solar_wind.get("speed", 0)
        if speed > 700:
            score += 2
        elif speed > 500:
            score += 1
        
        # Determine level
        if score >= 5:
            return "extreme"
        elif score >= 3:
            return "high"
        elif score >= 1:
            return "moderate"
        else:
            return "low"
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for space weather data."""
        return {
            "solar_flares_24h": int,
            "max_flare_class": str,
            "kp_index": float,
            "geomagnetic_storm": bool,
            "solar_wind_speed_kms": float,
            "alert_level": str
        }
