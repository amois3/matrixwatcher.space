"""Base sensor class for Matrix Watcher.

Defines the abstract interface for all sensors with common functionality:
- Error handling and retry logic
- Status tracking
- Schema definition
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from ..core.types import SensorReading, SensorStatus, Event, EventType
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


@dataclass
class SensorConfig:
    """Configuration for a sensor."""
    enabled: bool = True
    interval_seconds: float = 5.0
    priority: str = "medium"
    custom_params: dict[str, Any] | None = None
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0


class BaseSensor(ABC):
    """Abstract base class for all sensors.
    
    Provides common functionality:
    - Status tracking (running, stopped, error, rate_limited)
    - Error handling with retry logic
    - Event bus integration
    - Schema definition for data validation
    
    Subclasses must implement:
    - collect(): Gather data from the source
    - get_schema(): Define expected data fields
    
    Example:
        class MySensor(BaseSensor):
            def collect(self) -> SensorReading:
                data = {"value": 42}
                return SensorReading.create(self.name, data)
            
            def get_schema(self) -> dict[str, type]:
                return {"value": float}
    """
    
    def __init__(
        self,
        name: str,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None
    ):
        """Initialize sensor.
        
        Args:
            name: Unique sensor name
            config: Sensor configuration
            event_bus: Event bus for publishing data
        """
        self.name = name
        self.config = config or SensorConfig()
        self.event_bus = event_bus
        
        self._status = SensorStatus.STOPPED
        self._last_reading: SensorReading | None = None
        self._last_error: Exception | None = None
        self._error_count = 0
        self._consecutive_errors = 0
        self._success_count = 0
        self._start_time: float | None = None
    
    @abstractmethod
    async def collect(self) -> SensorReading:
        """Collect data from the sensor source.
        
        Returns:
            SensorReading with collected data
            
        Raises:
            Exception: If data collection fails
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> dict[str, type]:
        """Get the schema for this sensor's data.
        
        Returns:
            Dictionary mapping field names to expected types
        """
        pass
    
    def collect_sync(self) -> SensorReading:
        """Synchronous wrapper for collect().
        
        For sensors that don't need async, override this method
        and have collect() call it.
        
        Returns:
            SensorReading with collected data
        """
        import asyncio
        return asyncio.get_event_loop().run_until_complete(self.collect())
    
    async def safe_collect(self) -> SensorReading | None:
        """Collect data with error handling and retry logic.
        
        Returns:
            SensorReading on success, None on failure
        """
        for attempt in range(self.config.max_retries):
            try:
                reading = await self.collect()
                
                self._last_reading = reading
                self._success_count += 1
                self._consecutive_errors = 0
                self._status = SensorStatus.RUNNING
                
                # Publish to event bus
                if self.event_bus:
                    event = reading.to_event()
                    self.event_bus.publish(event)
                
                return reading
                
            except Exception as e:
                self._last_error = e
                self._error_count += 1
                self._consecutive_errors += 1
                
                logger.warning(
                    f"Sensor {self.name} collection failed (attempt {attempt + 1}): {e}"
                )
                
                if attempt < self.config.max_retries - 1:
                    await self._async_sleep(self.config.retry_delay * (attempt + 1))
        
        # All retries failed
        self._status = SensorStatus.ERROR
        
        if self.event_bus:
            error_event = Event.create(
                source=self.name,
                event_type=EventType.ERROR,
                payload={
                    "error": str(self._last_error),
                    "consecutive_errors": self._consecutive_errors
                }
            )
            self.event_bus.publish(error_event)
        
        return None
    
    async def _async_sleep(self, seconds: float) -> None:
        """Async sleep helper."""
        import asyncio
        await asyncio.sleep(seconds)
    
    def start(self) -> None:
        """Start the sensor."""
        self._status = SensorStatus.RUNNING
        self._start_time = time.time()
        logger.info(f"Sensor {self.name} started")
    
    def stop(self) -> None:
        """Stop the sensor."""
        self._status = SensorStatus.STOPPED
        logger.info(f"Sensor {self.name} stopped")
    
    def get_status(self) -> SensorStatus:
        """Get current sensor status."""
        return self._status
    
    def set_status(self, status: SensorStatus) -> None:
        """Set sensor status."""
        self._status = status
    
    def is_enabled(self) -> bool:
        """Check if sensor is enabled."""
        return self.config.enabled
    
    def get_stats(self) -> dict[str, Any]:
        """Get sensor statistics.
        
        Returns:
            Dictionary with stats
        """
        return {
            "name": self.name,
            "status": self._status.value,
            "enabled": self.config.enabled,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "consecutive_errors": self._consecutive_errors,
            "last_error": str(self._last_error) if self._last_error else None,
            "uptime_seconds": time.time() - self._start_time if self._start_time else 0
        }
    
    def get_last_reading(self) -> SensorReading | None:
        """Get the last successful reading."""
        return self._last_reading
    
    def reset_errors(self) -> None:
        """Reset error counters."""
        self._error_count = 0
        self._consecutive_errors = 0
        self._last_error = None
        if self._status == SensorStatus.ERROR:
            self._status = SensorStatus.RUNNING
    
    def validate_reading(self, reading: SensorReading) -> list[str]:
        """Validate a reading against the schema.
        
        Args:
            reading: Reading to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        schema = self.get_schema()
        
        for field, expected_type in schema.items():
            if field not in reading.data:
                errors.append(f"Missing field: {field}")
            elif not isinstance(reading.data[field], expected_type):
                actual_type = type(reading.data[field]).__name__
                errors.append(f"Field {field}: expected {expected_type.__name__}, got {actual_type}")
        
        return errors


class SyncSensor(BaseSensor):
    """Base class for synchronous sensors.
    
    For sensors that don't need async operations, inherit from this
    class and implement collect_data() instead of collect().
    """
    
    @abstractmethod
    def collect_data(self) -> dict[str, Any]:
        """Collect data synchronously.
        
        Returns:
            Dictionary with sensor data
        """
        pass
    
    async def collect(self) -> SensorReading:
        """Async wrapper that calls collect_data()."""
        data = self.collect_data()
        return SensorReading.create(self.name, data)
