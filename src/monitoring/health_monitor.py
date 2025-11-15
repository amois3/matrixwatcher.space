"""Health Monitor for Matrix Watcher.

Tracks sensor status, API quotas, calibration status, and exposes health endpoint.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from aiohttp import web

from ..core.types import SensorStatus
from .calibration_tracker import get_tracker

logger = logging.getLogger(__name__)


@dataclass
class SensorHealth:
    """Health status for a single sensor."""
    name: str
    status: SensorStatus = SensorStatus.STOPPED
    last_success: float | None = None
    last_error: float | None = None
    error_message: str | None = None
    consecutive_failures: int = 0
    total_successes: int = 0
    total_failures: int = 0
    disabled: bool = False
    disabled_reason: str | None = None


@dataclass
class APIQuota:
    """API quota tracking."""
    name: str
    limit: int
    used: int = 0
    reset_time: float = 0
    
    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)
    
    @property
    def usage_percent(self) -> float:
        return (self.used / self.limit * 100) if self.limit > 0 else 0


class HealthMonitor:
    """Monitors health of all sensors and system components.
    
    Features:
    - Track sensor status (running, stopped, error, rate-limited)
    - Track API quota usage
    - Detect sensor failures within 30 seconds
    - Auto-disable sensors after 3 consecutive failures
    - HTTP health endpoint on configurable port
    - Log health metrics
    
    Example:
        monitor = HealthMonitor(port=8080)
        monitor.register_sensor("system")
        monitor.record_success("system")
        await monitor.start()
    """
    
    def __init__(
        self,
        port: int = 8080,
        failure_threshold: int = 3,
        failure_window: float = 30.0,
        on_sensor_disabled: Callable[[str], None] | None = None
    ):
        """Initialize health monitor.
        
        Args:
            port: HTTP port for health endpoint
            failure_threshold: Consecutive failures before disabling
            failure_window: Time window for failure detection (seconds)
            on_sensor_disabled: Callback when sensor is disabled
        """
        self.port = port
        self.failure_threshold = failure_threshold
        self.failure_window = failure_window
        self.on_sensor_disabled = on_sensor_disabled
        
        self._sensors: dict[str, SensorHealth] = {}
        self._quotas: dict[str, APIQuota] = {}
        self._start_time = time.time()
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
    
    def register_sensor(self, name: str) -> None:
        """Register a sensor for monitoring.
        
        Args:
            name: Sensor name
        """
        if name not in self._sensors:
            self._sensors[name] = SensorHealth(name=name)
            logger.debug(f"Registered sensor: {name}")
    
    def register_api_quota(self, name: str, limit: int, reset_interval: float = 3600) -> None:
        """Register an API quota for tracking.
        
        Args:
            name: API name
            limit: Request limit
            reset_interval: Reset interval in seconds
        """
        self._quotas[name] = APIQuota(
            name=name,
            limit=limit,
            reset_time=time.time() + reset_interval
        )
    
    def record_success(self, sensor_name: str) -> None:
        """Record successful sensor execution.
        
        Args:
            sensor_name: Sensor name
        """
        if sensor_name not in self._sensors:
            self.register_sensor(sensor_name)
        
        health = self._sensors[sensor_name]
        health.status = SensorStatus.RUNNING
        health.last_success = time.time()
        health.consecutive_failures = 0
        health.total_successes += 1
    
    def record_failure(self, sensor_name: str, error: str | None = None) -> None:
        """Record sensor failure.
        
        Args:
            sensor_name: Sensor name
            error: Error message
        """
        if sensor_name not in self._sensors:
            self.register_sensor(sensor_name)
        
        health = self._sensors[sensor_name]
        health.status = SensorStatus.ERROR
        health.last_error = time.time()
        health.error_message = error
        health.consecutive_failures += 1
        health.total_failures += 1
        
        # Check for auto-disable (only if not already disabled)
        if not health.disabled and health.consecutive_failures >= self.failure_threshold:
            self._disable_sensor(sensor_name, f"Exceeded {self.failure_threshold} consecutive failures")
    
    def record_rate_limit(self, sensor_name: str) -> None:
        """Record rate limiting for a sensor.
        
        Args:
            sensor_name: Sensor name
        """
        if sensor_name not in self._sensors:
            self.register_sensor(sensor_name)
        
        health = self._sensors[sensor_name]
        health.status = SensorStatus.RATE_LIMITED
    
    def record_api_usage(self, api_name: str, count: int = 1) -> None:
        """Record API usage.
        
        Args:
            api_name: API name
            count: Number of requests
        """
        if api_name in self._quotas:
            quota = self._quotas[api_name]
            
            # Check for reset
            if time.time() >= quota.reset_time:
                quota.used = 0
                quota.reset_time = time.time() + 3600  # Reset hourly
            
            quota.used += count
    
    def _disable_sensor(self, sensor_name: str, reason: str) -> None:
        """Disable a sensor.
        
        Args:
            sensor_name: Sensor name
            reason: Reason for disabling
        """
        health = self._sensors[sensor_name]
        health.disabled = True
        health.disabled_reason = reason
        health.status = SensorStatus.STOPPED
        
        logger.warning(f"Sensor {sensor_name} disabled: {reason}")
        
        if self.on_sensor_disabled:
            self.on_sensor_disabled(sensor_name)
    
    def enable_sensor(self, sensor_name: str) -> None:
        """Re-enable a disabled sensor.
        
        Args:
            sensor_name: Sensor name
        """
        if sensor_name in self._sensors:
            health = self._sensors[sensor_name]
            health.disabled = False
            health.disabled_reason = None
            health.consecutive_failures = 0
            health.status = SensorStatus.STOPPED
            logger.info(f"Sensor {sensor_name} re-enabled")
    
    def get_sensor_status(self, sensor_name: str) -> SensorHealth | None:
        """Get status for a sensor.
        
        Args:
            sensor_name: Sensor name
            
        Returns:
            SensorHealth or None if not found
        """
        return self._sensors.get(sensor_name)
    
    def get_all_status(self) -> dict[str, Any]:
        """Get full health status.
        
        Returns:
            Health status dictionary
        """
        now = time.time()
        
        sensors_status = {}
        for name, health in self._sensors.items():
            sensors_status[name] = {
                "status": health.status.value,
                "disabled": health.disabled,
                "disabled_reason": health.disabled_reason,
                "consecutive_failures": health.consecutive_failures,
                "total_successes": health.total_successes,
                "total_failures": health.total_failures,
                "last_success_ago": round(now - health.last_success, 1) if health.last_success else None,
                "last_error": health.error_message
            }
        
        quotas_status = {}
        for name, quota in self._quotas.items():
            quotas_status[name] = {
                "limit": quota.limit,
                "used": quota.used,
                "remaining": quota.remaining,
                "usage_percent": round(quota.usage_percent, 1),
                "resets_in": round(quota.reset_time - now, 0)
            }
        
        # Overall health
        healthy_sensors = sum(1 for h in self._sensors.values() if h.status == SensorStatus.RUNNING)
        total_sensors = len(self._sensors)
        
        # Calibration status
        try:
            from .auto_calibrator import get_auto_calibrator
            calibration_stats = get_auto_calibrator().get_calibration_status()
        except Exception as e:
            logger.error(f"Failed to get calibration stats: {e}")
            calibration_stats = {"error": str(e)}
        
        return {
            "status": "healthy" if healthy_sensors == total_sensors else "degraded",
            "uptime_seconds": round(now - self._start_time, 1),
            "sensors": sensors_status,
            "sensors_healthy": healthy_sensors,
            "sensors_total": total_sensors,
            "api_quotas": quotas_status,
            "calibration": calibration_stats,
            "timestamp": now
        }
    
    async def _handle_health(self, request: web.Request) -> web.Response:
        """Handle health endpoint request."""
        status = self.get_all_status()
        return web.json_response(status)
    
    async def _handle_sensor(self, request: web.Request) -> web.Response:
        """Handle sensor status request."""
        sensor_name = request.match_info.get("name")
        health = self.get_sensor_status(sensor_name)
        
        if health is None:
            return web.json_response({"error": "Sensor not found"}, status=404)
        
        return web.json_response({
            "name": health.name,
            "status": health.status.value,
            "disabled": health.disabled,
            "consecutive_failures": health.consecutive_failures
        })
    
    async def start(self) -> None:
        """Start the health monitor HTTP server."""
        self._app = web.Application()
        self._app.router.add_get("/health", self._handle_health)
        self._app.router.add_get("/sensor/{name}", self._handle_sensor)
        
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        
        logger.info(f"Health monitor started on port {self.port}")
    
    async def stop(self) -> None:
        """Stop the health monitor."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Health monitor stopped")
    
    def log_health(self) -> None:
        """Log current health status."""
        status = self.get_all_status()
        logger.info(f"Health: {status['sensors_healthy']}/{status['sensors_total']} sensors healthy")
