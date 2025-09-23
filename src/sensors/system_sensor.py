"""System Sensor for Matrix Watcher.

Collects internal system metrics:
- CPU usage
- RAM usage
- CPU temperature (if available)
- Process info (PID, uptime)
- Loop timing and drift
"""

import logging
import os
import time
from typing import Any

import psutil

from .base import SyncSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


class SystemSensor(SyncSensor):
    """Sensor for collecting system metrics.
    
    Collects:
    - local_time_unix: Current Unix timestamp
    - loop_interval_ms: Actual time since last collection
    - loop_drift_ms: Drift from expected interval
    - cpu_usage_percent: CPU utilization
    - ram_usage_percent: RAM utilization
    - cpu_temperature: CPU temperature (None if unavailable)
    - process_pid: Current process ID
    - process_uptime_seconds: Process uptime
    
    Example:
        sensor = SystemSensor()
        reading = sensor.collect_data()
        print(reading["cpu_usage_percent"])
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None
    ):
        """Initialize System Sensor."""
        super().__init__("system", config, event_bus)
        
        self._last_collect_time: float | None = None
        self._process_start_time = time.time()
        self._process = psutil.Process(os.getpid())
        
        # Get expected interval from config
        self._expected_interval = self.config.interval_seconds if self.config else 1.0
    
    def collect_data(self) -> dict[str, Any]:
        """Collect system metrics."""
        current_time = time.time()
        
        # Calculate loop timing
        if self._last_collect_time is not None:
            loop_interval_ms = (current_time - self._last_collect_time) * 1000
            loop_drift_ms = loop_interval_ms - (self._expected_interval * 1000)
        else:
            loop_interval_ms = self._expected_interval * 1000
            loop_drift_ms = 0.0
        
        self._last_collect_time = current_time
        
        # Collect CPU and memory
        cpu_percent = psutil.cpu_percent(interval=None)
        memory = psutil.virtual_memory()
        
        # Try to get CPU temperature
        cpu_temperature = self._get_cpu_temperature()
        
        # Process info
        process_uptime = current_time - self._process_start_time
        
        return {
            "local_time_unix": current_time,
            "loop_interval_ms": round(loop_interval_ms, 2),
            "loop_drift_ms": round(loop_drift_ms, 2),
            "cpu_usage_percent": round(cpu_percent, 1),
            "ram_usage_percent": round(memory.percent, 1),
            "cpu_temperature": cpu_temperature,
            "process_pid": os.getpid(),
            "process_uptime_seconds": round(process_uptime, 1)
        }
    
    def _get_cpu_temperature(self) -> float | None:
        """Get CPU temperature if available.
        
        Returns:
            Temperature in Celsius, or None if unavailable
        """
        try:
            # Try psutil sensors_temperatures (Linux)
            if hasattr(psutil, "sensors_temperatures"):
                temps = psutil.sensors_temperatures()
                if temps:
                    # Try common sensor names
                    for name in ["coretemp", "cpu_thermal", "cpu-thermal", "k10temp"]:
                        if name in temps and temps[name]:
                            return round(temps[name][0].current, 1)
                    # Try first available
                    for sensors in temps.values():
                        if sensors:
                            return round(sensors[0].current, 1)
        except Exception as e:
            logger.debug(f"Could not get CPU temperature: {e}")
        
        return None
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for system sensor data."""
        return {
            "local_time_unix": float,
            "loop_interval_ms": float,
            "loop_drift_ms": float,
            "cpu_usage_percent": float,
            "ram_usage_percent": float,
            # cpu_temperature can be None, so not in strict schema
            "process_pid": int,
            "process_uptime_seconds": float
        }
    
    def set_expected_interval(self, interval: float) -> None:
        """Set expected collection interval for drift calculation.
        
        Args:
            interval: Expected interval in seconds
        """
        self._expected_interval = interval
