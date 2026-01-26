"""Online Anomaly Detector for Matrix Watcher.

Real-time anomaly detection using z-score on sliding windows.
"""

import logging
import math
import statistics
from collections import deque
from dataclasses import dataclass
from typing import Any

from ...core.types import Event, EventType, AnomalyEvent
from ...core.event_bus import EventBus
from ...utils.statistics import z_score, sliding_window_stats

logger = logging.getLogger(__name__)


class SlidingWindow:
    """Sliding window for tracking values and computing statistics.
    
    Maintains a fixed-size window of recent values and provides
    efficient computation of mean, std, and z-score.
    """
    
    def __init__(self, max_size: int = 100):
        """Initialize sliding window.
        
        Args:
            max_size: Maximum number of values to keep
        """
        self.max_size = max_size
        self._values: deque = deque(maxlen=max_size)
    
    def add(self, value: float) -> None:
        """Add a value to the window."""
        self._values.append(value)
    
    def __len__(self) -> int:
        """Return number of values in window."""
        return len(self._values)
    
    def mean(self) -> float:
        """Calculate mean of values in window."""
        if not self._values:
            return 0.0
        return statistics.mean(self._values)
    
    def std(self) -> float:
        """Calculate standard deviation of values in window."""
        if len(self._values) < 2:
            return 0.0
        return statistics.stdev(self._values)
    
    def z_score(self, value: float) -> float:
        """Calculate z-score for a value.
        
        Args:
            value: Value to calculate z-score for
            
        Returns:
            Z-score (0 if std is 0)
        """
        std = self.std()
        if std == 0:
            return 0.0
        return (value - self.mean()) / std
    
    def values(self) -> list[float]:
        """Return list of values in window."""
        return list(self._values)
    
    def clear(self) -> None:
        """Clear all values from window."""
        self._values.clear()


class AnomalyDetector:
    """Simple anomaly detector using z-score threshold.
    
    Alternative interface for anomaly detection that tracks
    parameters by source:parameter key.
    """
    
    def __init__(self, window_size: int = 100, threshold: float = 4.0):
        """Initialize detector.
        
        Args:
            window_size: Size of sliding windows
            threshold: Z-score threshold for anomaly detection
        """
        self.window_size = window_size
        self.threshold = threshold
        self._windows: dict[str, SlidingWindow] = {}
    
    def process(self, source: str, parameter: str, value: float) -> dict[str, Any] | None:
        """Process a value and check for anomaly.
        
        Args:
            source: Data source name
            parameter: Parameter name
            value: Value to check
            
        Returns:
            Anomaly record if detected, None otherwise
        """
        key = f"{source}:{parameter}"
        
        if key not in self._windows:
            self._windows[key] = SlidingWindow(max_size=self.window_size)
        
        window = self._windows[key]
        
        result = None
        if len(window) >= 10:  # Need minimum data
            z = window.z_score(value)
            is_anomaly = abs(z) > self.threshold
            
            result = {
                "source": source,
                "parameter": parameter,
                "value": value,
                "z_score": z,
                "mean": window.mean(),
                "std": window.std(),
                "threshold": self.threshold,
                "is_anomaly": is_anomaly
            }
            
            if not is_anomaly:
                result = None
        
        window.add(value)
        return result
    
    def get_stats(self, source: str, parameter: str) -> dict[str, Any]:
        """Get statistics for a parameter.
        
        Args:
            source: Data source name
            parameter: Parameter name
            
        Returns:
            Statistics dictionary
        """
        key = f"{source}:{parameter}"
        
        if key not in self._windows:
            return {"count": 0, "mean": 0.0, "std": 0.0}
        
        window = self._windows[key]
        return {
            "count": len(window),
            "mean": window.mean(),
            "std": window.std()
        }
    
    def clear(self) -> None:
        """Clear all windows."""
        self._windows.clear()


@dataclass
class WindowConfig:
    """Configuration for a parameter's sliding window."""
    size: int = 100
    z_threshold: float = 4.0


class OnlineAnomalyDetector:
    """Real-time anomaly detector using z-score.
    
    Maintains sliding windows for each parameter and detects
    anomalies when values exceed z-score threshold.
    """
    
    def __init__(
        self,
        default_window_size: int = 100,
        default_z_threshold: float = 4.0,
        event_bus: EventBus | None = None
    ):
        self.default_window_size = default_window_size
        self.default_z_threshold = default_z_threshold
        self.event_bus = event_bus
        
        self._windows: dict[str, deque] = {}
        self._configs: dict[str, WindowConfig] = {}
        self._anomaly_count = 0
    
    def configure_parameter(self, param: str, window_size: int, z_threshold: float) -> None:
        """Configure window for a specific parameter."""
        self._configs[param] = WindowConfig(size=window_size, z_threshold=z_threshold)
        if param in self._windows:
            # Resize window
            old = list(self._windows[param])
            self._windows[param] = deque(old[-window_size:], maxlen=window_size)
    
    def process(self, event: Event) -> list[AnomalyEvent]:
        """Process an event and detect anomalies."""
        anomalies = []
        
        for key, value in event.payload.items():
            if not isinstance(value, (int, float)) or value is None:
                continue
            
            param_key = f"{event.source}.{key}"
            anomaly = self._check_value(param_key, float(value), event.source, event.timestamp)
            if anomaly:
                anomalies.append(anomaly)
        
        return anomalies
    
    def _check_value(self, param: str, value: float, source: str, timestamp: float) -> AnomalyEvent | None:
        """Check if a value is anomalous."""
        config = self._configs.get(param, WindowConfig(self.default_window_size, self.default_z_threshold))
        
        if param not in self._windows:
            self._windows[param] = deque(maxlen=config.size)
        
        window = self._windows[param]
        
        if len(window) >= 10:  # Need minimum data
            stats = sliding_window_stats(list(window))
            z = z_score(value, stats["mean"], stats["std"])
            
            if abs(z) > config.z_threshold:
                self._anomaly_count += 1
                anomaly = AnomalyEvent(
                    timestamp=timestamp,
                    parameter=param,
                    value=value,
                    mean=stats["mean"],
                    std=stats["std"],
                    z_score=z,
                    sensor_source=source
                )
                
                if self.event_bus:
                    self.event_bus.publish(anomaly.to_event())
                
                logger.info(f"Anomaly detected: {param}={value}, z={z:.2f}")
                return anomaly
        
        window.append(value)
        return None
    
    def get_window_stats(self, param: str) -> dict[str, Any] | None:
        """Get statistics for a parameter's window."""
        if param not in self._windows:
            return None
        return sliding_window_stats(list(self._windows[param]))
    
    def reset_window(self, param: str) -> None:
        """Reset a parameter's window."""
        if param in self._windows:
            self._windows[param].clear()
    
    def get_anomaly_count(self) -> int:
        """Get total anomaly count."""
        return self._anomaly_count
