"""Time Drift Sensor for Matrix Watcher.

Monitors time synchronization across different sources:
- Local OS time
- NTP server time
- External API time (Binance)
"""

import logging
import time
from typing import Any

import ntplib
import requests

from .base import SyncSensor, SensorConfig
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


class TimeDriftSensor(SyncSensor):
    """Sensor for monitoring time drift between sources."""
    
    NTP_SERVERS = ["pool.ntp.org", "time.google.com", "time.cloudflare.com"]
    BINANCE_TIME_URL = "https://api.binance.com/api/v3/time"
    
    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None):
        super().__init__("time_drift", config, event_bus)
        self._ntp_client = ntplib.NTPClient()
        self._current_ntp_index = 0
    
    def collect_data(self) -> dict[str, Any]:
        local_time = time.time()
        ntp_time = self._get_ntp_time()
        api_time = self._get_api_time()
        
        diff_local_ntp = (local_time - ntp_time) * 1000 if ntp_time else None
        diff_local_api = (local_time - api_time) * 1000 if api_time else None
        
        return {
            "local_time": local_time,
            "ntp_time": ntp_time,
            "api_time": api_time,
            "diff_local_ntp_ms": round(diff_local_ntp, 2) if diff_local_ntp else None,
            "diff_local_api_ms": round(diff_local_api, 2) if diff_local_api else None
        }
    
    def _get_ntp_time(self) -> float | None:
        for i in range(len(self.NTP_SERVERS)):
            server = self.NTP_SERVERS[(self._current_ntp_index + i) % len(self.NTP_SERVERS)]
            try:
                response = self._ntp_client.request(server, version=3, timeout=5)
                self._current_ntp_index = (self._current_ntp_index + i) % len(self.NTP_SERVERS)
                return response.tx_time
            except Exception as e:
                logger.debug(f"NTP server {server} failed: {e}")
        return None
    
    def _get_api_time(self) -> float | None:
        try:
            response = requests.get(self.BINANCE_TIME_URL, timeout=5)
            if response.status_code == 200:
                return response.json()["serverTime"] / 1000.0
        except Exception as e:
            logger.debug(f"Binance API time failed: {e}")
        return None
    
    def get_schema(self) -> dict[str, type]:
        return {"local_time": float, "ntp_time": float, "api_time": float}
    
    @staticmethod
    def calculate_drift(local: float, ntp: float | None, api: float | None) -> dict[str, float | None]:
        """Calculate time drifts from known values."""
        return {
            "diff_local_ntp_ms": (local - ntp) * 1000 if ntp else None,
            "diff_local_api_ms": (local - api) * 1000 if api else None
        }
