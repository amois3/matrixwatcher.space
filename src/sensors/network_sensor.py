"""Network Sensor for Matrix Watcher.

Probes network targets and measures latency, status codes, and response sizes.
"""

import logging
import time
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)

DEFAULT_TARGETS = [
    {"url": "https://www.google.com", "name": "google"},
    {"url": "https://api.binance.com/api/v3/ping", "name": "binance"},
    {"url": "https://cloudflare.com", "name": "cloudflare"},
    {"url": "https://www.random.org", "name": "random_org"},
]


class NetworkSensor(BaseSensor):
    """Sensor for measuring network latency and connectivity.
    
    Probes configurable targets and records:
    - target: Target name
    - url: Target URL
    - latency_ms: Round-trip time in milliseconds (-1 if unreachable)
    - status_code: HTTP status code (0 if unreachable)
    - response_size_bytes: Response body size
    - reachable: Boolean indicating if target was reached
    
    Example:
        sensor = NetworkSensor()
        reading = await sensor.collect()
        for probe in reading.data["probes"]:
            print(f"{probe['target']}: {probe['latency_ms']}ms")
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        targets: list[dict[str, str]] | None = None,
        timeout: float = 10.0
    ):
        """Initialize Network Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            targets: List of targets with 'url' and 'name' keys
            timeout: Request timeout in seconds
        """
        super().__init__("network", config, event_bus)
        self.targets = targets or DEFAULT_TARGETS
        self.timeout = timeout
    
    async def collect(self) -> SensorReading:
        """Probe all targets and collect latency data."""
        probes = []
        
        async with aiohttp.ClientSession() as session:
            for target in self.targets:
                probe_result = await self._probe_target(session, target)
                probes.append(probe_result)
        
        # Calculate aggregate stats
        reachable_probes = [p for p in probes if p["reachable"]]
        avg_latency = (
            sum(p["latency_ms"] for p in reachable_probes) / len(reachable_probes)
            if reachable_probes else -1
        )
        
        data = {
            "timestamp": time.time(),
            "probes": probes,
            "targets_total": len(self.targets),
            "targets_reachable": len(reachable_probes),
            "avg_latency_ms": round(avg_latency, 2) if avg_latency >= 0 else -1
        }
        
        return SensorReading.create(self.name, data)
    
    async def _probe_target(
        self, 
        session: aiohttp.ClientSession, 
        target: dict[str, str]
    ) -> dict[str, Any]:
        """Probe a single target.
        
        Args:
            session: aiohttp session
            target: Target dict with 'url' and 'name'
            
        Returns:
            Probe result dictionary
        """
        url = target["url"]
        name = target["name"]
        
        try:
            start_time = time.perf_counter()
            
            async with session.get(
                url, 
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                ssl=False  # Skip SSL verification for speed
            ) as response:
                content = await response.read()
                end_time = time.perf_counter()
                
                latency_ms = (end_time - start_time) * 1000
                
                return {
                    "target": name,
                    "url": url,
                    "latency_ms": round(latency_ms, 2),
                    "status_code": response.status,
                    "response_size_bytes": len(content),
                    "reachable": True
                }
                
        except aiohttp.ClientError as e:
            logger.debug(f"Network probe failed for {name}: {e}")
            return {
                "target": name,
                "url": url,
                "latency_ms": -1,
                "status_code": 0,
                "response_size_bytes": 0,
                "reachable": False,
                "error": str(e)
            }
        except Exception as e:
            logger.warning(f"Unexpected error probing {name}: {e}")
            return {
                "target": name,
                "url": url,
                "latency_ms": -1,
                "status_code": 0,
                "response_size_bytes": 0,
                "reachable": False,
                "error": str(e)
            }
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for network sensor data."""
        return {
            "timestamp": float,
            "probes": list,
            "targets_total": int,
            "targets_reachable": int,
            "avg_latency_ms": float
        }
