"""Core type definitions for Matrix Watcher.

This module defines the fundamental data structures used throughout the system:
- Event: Messages passed through the Event Bus
- SensorReading: Data collected by sensors
- Enums for status, priority, and event types
"""

from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any
import time


class EventType(str, Enum):
    """Types of events in the system."""
    DATA = "data"           # Regular sensor data
    ANOMALY = "anomaly"     # Detected anomaly
    ERROR = "error"         # Error event
    HEALTH = "health"       # Health status update
    ALERT = "alert"         # Alert notification


class SensorStatus(str, Enum):
    """Status of a sensor."""
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    RATE_LIMITED = "rate_limited"
    DISABLED = "disabled"


class Priority(str, Enum):
    """Task/sensor priority levels."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Severity(str, Enum):
    """Severity levels for events and alerts."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Event:
    """Event passed through the Event Bus.
    
    Attributes:
        timestamp: Unix timestamp (float) when event was created
        source: Name of the sensor or component that created the event
        event_type: Type of event (data, anomaly, error, health)
        payload: Event-specific data
        severity: Severity level of the event
        metadata: Optional additional metadata
    """
    timestamp: float
    source: str
    event_type: EventType
    payload: dict[str, Any]
    severity: Severity = Severity.INFO
    metadata: dict[str, Any] | None = None
    
    def __post_init__(self):
        """Validate and convert event_type if needed."""
        if isinstance(self.event_type, str):
            self.event_type = EventType(self.event_type)
        if isinstance(self.severity, str):
            self.severity = Severity(self.severity)
    
    @classmethod
    def create(
        cls,
        source: str,
        event_type: EventType | str,
        payload: dict[str, Any],
        severity: Severity | str = Severity.INFO,
        metadata: dict[str, Any] | None = None
    ) -> "Event":
        """Factory method to create an event with current timestamp."""
        return cls(
            timestamp=time.time(),
            source=source,
            event_type=event_type if isinstance(event_type, EventType) else EventType(event_type),
            payload=payload,
            severity=severity if isinstance(severity, Severity) else Severity(severity),
            metadata=metadata
        )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary for serialization."""
        result = {
            "timestamp": self.timestamp,
            "source": self.source,
            "event_type": self.event_type.value,
            "payload": self.payload,
            "severity": self.severity.value,
        }
        if self.metadata:
            result["metadata"] = self.metadata
        return result
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        """Create event from dictionary."""
        return cls(
            timestamp=data["timestamp"],
            source=data["source"],
            event_type=EventType(data["event_type"]),
            payload=data["payload"],
            severity=Severity(data.get("severity", "info")),
            metadata=data.get("metadata")
        )


@dataclass
class SensorReading:
    """Data collected by a sensor.
    
    Attributes:
        timestamp: Unix timestamp (float) when reading was taken
        source: Name of the sensor
        data: Sensor-specific data fields
        metadata: Optional additional metadata (e.g., collection duration)
    """
    timestamp: float
    source: str
    data: dict[str, Any]
    metadata: dict[str, Any] | None = None
    
    @classmethod
    def create(
        cls,
        source: str,
        data: dict[str, Any],
        metadata: dict[str, Any] | None = None
    ) -> "SensorReading":
        """Factory method to create a reading with current timestamp."""
        return cls(
            timestamp=time.time(),
            source=source,
            data=data,
            metadata=metadata
        )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert reading to dictionary for serialization.
        
        Note: Reserved keys (timestamp, source, _metadata) in data are prefixed
        with 'data_' to avoid conflicts.
        """
        result = {
            "timestamp": self.timestamp,
            "source": self.source,
        }
        # Add data fields, prefixing reserved keys to avoid conflicts
        reserved_keys = {"timestamp", "source", "_metadata"}
        for key, value in self.data.items():
            if key in reserved_keys:
                result[f"data_{key}"] = value
            else:
                result[key] = value
        if self.metadata:
            result["_metadata"] = self.metadata
        return result
    
    @classmethod
    def from_dict(cls, data_dict: dict[str, Any]) -> "SensorReading":
        """Create reading from dictionary."""
        # Make a copy to avoid modifying the original
        data_dict = data_dict.copy()
        timestamp = data_dict.pop("timestamp")
        source = data_dict.pop("source")
        metadata = data_dict.pop("_metadata", None)
        
        # Restore prefixed reserved keys
        sensor_data = {}
        for key, value in data_dict.items():
            if key.startswith("data_") and key[5:] in {"timestamp", "source", "_metadata"}:
                sensor_data[key[5:]] = value
            else:
                sensor_data[key] = value
        
        return cls(
            timestamp=timestamp,
            source=source,
            data=sensor_data,
            metadata=metadata
        )
    
    def to_event(self, event_type: EventType = EventType.DATA) -> Event:
        """Convert sensor reading to an Event for the Event Bus."""
        # Use to_dict() to include source in payload
        payload_dict = self.to_dict()
        # Remove timestamp as it's already in Event
        payload_dict.pop('timestamp', None)
        
        return Event(
            timestamp=self.timestamp,
            source=self.source,
            event_type=event_type,
            payload=payload_dict,
            metadata=self.metadata
        )


@dataclass
class AnomalyEvent:
    """Detected anomaly event.
    
    Attributes:
        timestamp: When the anomaly was detected
        parameter: Name of the parameter that triggered the anomaly
        value: The anomalous value
        mean: Mean of the sliding window
        std: Standard deviation of the sliding window
        z_score: Z-score of the value
        sensor_source: Which sensor produced the anomalous reading
        metadata: Optional additional context
    """
    timestamp: float
    parameter: str
    value: float
    mean: float
    std: float
    z_score: float
    sensor_source: str
    metadata: dict[str, Any] | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "timestamp": self.timestamp,
            "source": "anomaly_detector",
            "parameter": self.parameter,
            "value": self.value,
            "mean": self.mean,
            "std": self.std,
            "z_score": self.z_score,
            "sensor_source": self.sensor_source,
        }
        if self.metadata:
            result["metadata"] = self.metadata
        return result
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AnomalyEvent":
        """Create from dictionary."""
        return cls(
            timestamp=data["timestamp"],
            parameter=data["parameter"],
            value=data["value"],
            mean=data["mean"],
            std=data["std"],
            z_score=data["z_score"],
            sensor_source=data["sensor_source"],
            metadata=data.get("metadata")
        )
    
    def to_event(self) -> Event:
        """Convert to Event for the Event Bus."""
        return Event(
            timestamp=self.timestamp,
            source="anomaly_detector",
            event_type=EventType.ANOMALY,
            payload=self.to_dict(),
            severity=Severity.WARNING if abs(self.z_score) < 5 else Severity.CRITICAL
        )


@dataclass
class TaskStats:
    """Statistics for a scheduled task.
    
    Attributes:
        name: Task name
        last_run: Timestamp of last execution
        next_run: Timestamp of next scheduled execution
        run_count: Total number of executions
        error_count: Number of failed executions
        avg_duration_ms: Average execution duration in milliseconds
        last_drift_ms: Drift from scheduled time in last execution
    """
    name: str
    last_run: float | None = None
    next_run: float | None = None
    run_count: int = 0
    error_count: int = 0
    avg_duration_ms: float = 0.0
    last_drift_ms: float = 0.0
    consecutive_failures: int = 0


# Type aliases for clarity
Timestamp = float
SensorName = str
ParameterName = str
