"""Space Weather Sensor - Solar Activity and Geomagnetic Storms.

Monitors solar flares, CMEs, and geomagnetic activity.
"""

import logging
from typing import Any
import aiohttp
from datetime import datetime, timedelta, timezone

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
            flares_result = await self._get_solar_flares(session)
            flares = flares_result or []
            logger.info(f"Space Weather: Found {len(flares)} M/X flares")
            
            # Get geomagnetic K-index
            kp_index = await self._get_kp_index(session)
            if kp_index is None:
                raise RuntimeError("NOAA Kp feed returned no usable measurement")
            logger.info(f"Space Weather: Kp index = {kp_index}")
            
            # Get solar wind
            solar_wind_result = await self._get_solar_wind(session)
            solar_wind = solar_wind_result or {}
            logger.info(f"Space Weather: Solar wind = {solar_wind}")
            
            # Determine alert level
            alert_level = self._calculate_alert_level(flares, kp_index, solar_wind)
            
            reading = SensorReading.create(
                source="space_weather",
                data={
                    "solar_flares_24h": len(flares) if flares_result is not None else None,
                    "flare_count": len(flares) if flares_result is not None else None,
                    "max_flare_class": max((f["class_type"] for f in flares), default="A") if flares_result is not None else None,
                    "kp_index": kp_index,
                    "geomagnetic_storm": kp_index >= 5,
                    "solar_wind_speed_kms": solar_wind.get("speed"),
                    "solar_wind_density": solar_wind.get("density"),
                    "alert_level": alert_level,
                    "recent_flares": flares[-3:],
                    "solar_wind_source": solar_wind.get("source"),
                    "solar_wind_observed_at": solar_wind.get("observed_at"),
                    "quality": {
                        "complete": flares_result is not None and solar_wind_result is not None,
                        "missing_fields": (["xray_flares"] if flares_result is None else [])
                                          + (["solar_wind"] if solar_wind_result is None else []),
                    },
                }
            )
            logger.info(f"Space Weather: Collection complete, alert_level={alert_level}")
            return reading
    
    async def _get_solar_flares(self, session: aiohttp.ClientSession) -> list[dict] | None:
        """Get distinct NOAA flare events with M/X peak in the last 24 hours."""
        try:
            url = f"{self.base_url}/goes/primary/xray-flares-7-day.json"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                flares = []
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
                for entry in data:
                    try:
                        time_str = entry.get("max_time") or entry.get("time_tag")
                        peak = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                        flare_class = (entry.get("max_class") or "")[:1]
                        if peak >= cutoff_time and flare_class in ("M", "X"):
                            flares.append({
                                "time": time_str,
                                "flux": entry.get("max_xrlong"),
                                "class_type": flare_class
                            })
                    except (ValueError, TypeError, AttributeError):
                        continue
                
                return flares
        except Exception as e:
            logger.warning(f"Failed to get solar flares: {e}")
            return None
    
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
    
    async def _get_kp_index(self, session: aiohttp.ClientSession) -> float | None:
        """Get current Kp index (geomagnetic activity)."""
        try:
            url = f"{self.base_url}/planetary_k_index_1m.json"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                if data:
                    latest = data[-1]
                    measured_at = datetime.fromisoformat(latest["time_tag"].replace("Z", "+00:00"))
                    if measured_at.tzinfo is None:
                        measured_at = measured_at.replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - measured_at).total_seconds() > 1800:
                        return None
                    value = latest.get("kp_index")
                    return float(value) if value is not None else None
                return None
        except Exception as e:
            logger.warning(f"Failed to get Kp index: {e}")
            return None
    
    async def _get_solar_wind(self, session: aiohttp.ClientSession) -> dict | None:
        """Use the freshest active spacecraft row, with source provenance."""
        try:
            url = f"{self.base_url}/rtsw/rtsw_wind_1m.json"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                latest = next((row for row in data if row.get("active") is True
                               and row.get("proton_speed") is not None
                               and row.get("proton_density") is not None), None)
                if latest is None:
                    return None
                measured_at = datetime.fromisoformat(latest["time_tag"].replace("Z", "+00:00"))
                if measured_at.tzinfo is None:
                    measured_at = measured_at.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - measured_at).total_seconds() > 1800:
                    return None
                return {
                    "speed": float(latest["proton_speed"]),
                    "density": float(latest["proton_density"]),
                    "source": latest.get("source"),
                    "observed_at": measured_at.isoformat(),
                }
        except Exception as e:
            logger.warning(f"Failed to get solar wind: {e}")
            return None
    
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
