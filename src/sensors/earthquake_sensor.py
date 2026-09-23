"""Earthquake Sensor - USGS Real-time Earthquake Data.

Monitors global seismic activity for anomalies and correlations.
"""

import logging
import time
from typing import Any
import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


class EarthquakeSensor(BaseSensor):
    """Sensor for monitoring earthquakes worldwide.
    
    Uses USGS Earthquake API to get real-time seismic data.
    Focuses on significant earthquakes (M4.5+) in the last hour.
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        min_magnitude: float = 4.5
    ):
        """Initialize Earthquake Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing readings
            min_magnitude: Minimum magnitude to report
        """
        super().__init__("earthquake", config, event_bus)
        self.min_magnitude = min_magnitude
        # A day-long M4.5+ feed catches events after collector outages. The
        # aggregate fields below still describe only the most recent hour.
        self.api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"
    
    async def collect(self) -> SensorReading:
        """Collect earthquake data from USGS."""
        async with aiohttp.ClientSession() as session:
            async with session.get(self.api_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    raise Exception(f"USGS API returned {response.status}")
                
                data = await response.json()
                
                # Parse earthquakes
                now = time.time()
                significant_events = []
                for feature in data.get("features", []):
                    props = feature.get("properties", {})
                    coords = feature.get("geometry", {}).get("coordinates", [])
                    
                    mag = props.get("mag")
                    if mag is None or mag < self.min_magnitude:
                        continue
                    occurred_ms = props.get("time")
                    if not isinstance(occurred_ms, (int, float)) or occurred_ms <= 0:
                        continue
                    occurred = occurred_ms / 1000
                    if occurred > now + 120:
                        continue
                    
                    significant_events.append({
                        "id": str(feature.get("id") or f"{props.get('time')}:{coords}:{mag}"),
                        "magnitude": mag,
                        "place": props.get("place", "Unknown"),
                        "time": occurred,
                        "depth_km": coords[2] if len(coords) > 2 else None,
                        "latitude": coords[1] if len(coords) > 1 else None,
                        "longitude": coords[0] if len(coords) > 0 else None,
                        "type": props.get("type", "earthquake"),
                        "tsunami": props.get("tsunami", 0) == 1
                    })
                
                # The one-hour aggregate retains its original meaning, while
                # the full day of IDs supports catch-up after a short outage.
                earthquakes = [eq for eq in significant_events if eq["time"] >= now - 3600]
                # Sort by magnitude (strongest first)
                earthquakes.sort(key=lambda x: x["magnitude"], reverse=True)
                
                # Calculate statistics
                count = len(earthquakes)
                max_mag = earthquakes[0]["magnitude"] if earthquakes else 0.0
                avg_mag = sum(eq["magnitude"] for eq in earthquakes) / count if count > 0 else 0.0
                
                # Get coordinates of strongest earthquake
                strongest_lat = earthquakes[0]["latitude"] if earthquakes else None
                strongest_lon = earthquakes[0]["longitude"] if earthquakes else None
                
                # Check for shallow earthquakes (< 70km = more dangerous)
                shallow_count = sum(1 for eq in earthquakes if eq["depth_km"] and eq["depth_km"] < 70)
                
                return SensorReading.create(
                    source="earthquake",
                    data={
                        "count": count,
                        "max_magnitude": max_mag,
                        "avg_magnitude": avg_mag,
                        "latitude": strongest_lat,  # Coordinates of strongest
                        "longitude": strongest_lon,
                        "shallow_count": shallow_count,
                        "has_tsunami_risk": any(eq["tsunami"] for eq in earthquakes),
                        "earthquakes": earthquakes[:5],  # Top 5 strongest
                        "significant_events": significant_events,
                        "total_energy_released": sum(10 ** (1.5 * eq["magnitude"]) for eq in earthquakes)  # Richter energy
                    }
                )
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for earthquake data."""
        return {
            "count": int,
            "max_magnitude": float,
            "avg_magnitude": float,
            "shallow_count": int,
            "has_tsunami_risk": bool,
            "total_energy_released": float
        }
