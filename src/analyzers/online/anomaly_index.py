"""Anomaly Index Calculator.

Calculates overall anomaly score (0-100) across all sensors.
Provides baseline comparison and breakdown by sensor.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class AnomalyIndexSnapshot:
    """Snapshot of anomaly index at a point in time."""
    timestamp: float
    index: float  # 0-100
    breakdown: dict[str, float]  # sensor -> score
    baseline_ratio: float  # Current / baseline
    status: str  # normal, elevated, high, critical
    active_anomalies: list[Any]


class AnomalyIndexCalculator:
    """Calculates overall anomaly index across all sensors."""
    
    # Weights for different sensors (how much they contribute)
    # NOTE: Currently EQUAL weights (1.0) - not yet calibrated!
    # To calibrate properly, we need:
    # 1. Historical data (1-2 months)
    # 2. Analysis of which sensors predict significant events
    # 3. Validation against real outcomes
    # 
    # Until then, we treat all sensors equally to avoid bias.
    SENSOR_WEIGHTS = {
        "quantum_rng": 1.0,      # Equal weight (not yet calibrated)
        "earthquake": 1.0,       # Equal weight (not yet calibrated)
        "crypto": 1.0,           # Equal weight (not yet calibrated)
        "space_weather": 1.0,    # Equal weight (not yet calibrated)
        "blockchain": 1.0,       # Equal weight (not yet calibrated)
        "weather": 1.0,          # Equal weight (not yet calibrated)
        "news": 1.0              # Equal weight (not yet calibrated)
    }
    
    def __init__(self, baseline_window_hours: int = 24):
        """Initialize anomaly index calculator.
        
        Args:
            baseline_window_hours: Hours to use for baseline calculation
        """
        self.baseline_window = baseline_window_hours * 3600  # Convert to seconds
        self._history: deque = deque(maxlen=10000)
        self._baseline_cache: dict[str, float] = {}
        self._last_baseline_update = 0
    
    def calculate(self, recent_anomalies: list[Any]) -> AnomalyIndexSnapshot:
        """Calculate current anomaly index.
        
        Args:
            recent_anomalies: List of recent anomaly events
            
        Returns:
            AnomalyIndexSnapshot with current state
        """
        current_time = time.time()
        
        # Update baseline if needed (every hour)
        if current_time - self._last_baseline_update > 3600:
            self._update_baseline()
        
        # Calculate scores by sensor
        breakdown = self._calculate_breakdown(recent_anomalies)
        
        # Calculate weighted total
        total_score = 0
        for sensor, score in breakdown.items():
            weight = self.SENSOR_WEIGHTS.get(sensor, 1.0)
            total_score += score * weight
        
        # Normalize to 0-100
        max_possible = sum(self.SENSOR_WEIGHTS.values()) * 100
        index = min(100, (total_score / max_possible) * 100)
        
        # Compare to baseline
        baseline = self._get_baseline_index()
        baseline_ratio = index / baseline if baseline > 0 else 1.0
        
        # Determine status
        status = self._determine_status(index, baseline_ratio)
        
        # Create snapshot
        snapshot = AnomalyIndexSnapshot(
            timestamp=current_time,
            index=round(index, 1),
            breakdown={k: round(v, 1) for k, v in breakdown.items()},
            baseline_ratio=round(baseline_ratio, 2),
            status=status,
            active_anomalies=recent_anomalies
        )
        
        # Store in history
        self._history.append(snapshot)
        
        return snapshot

    
    def _calculate_breakdown(self, anomalies: list[Any]) -> dict[str, float]:
        """Calculate score breakdown by sensor."""
        breakdown = {}
        
        # Group anomalies by sensor
        by_sensor = {}
        for anomaly in anomalies:
            sensor = anomaly.sensor_source
            if sensor not in by_sensor:
                by_sensor[sensor] = []
            by_sensor[sensor].append(anomaly)
        
        # Calculate score for each sensor
        for sensor, sensor_anomalies in by_sensor.items():
            # Base score: number of anomalies * severity
            score = 0
            for anomaly in sensor_anomalies:
                # Get severity from metadata or use z-score
                severity = "medium"
                if anomaly.metadata and "severity" in anomaly.metadata:
                    severity = anomaly.metadata["severity"]
                elif abs(anomaly.z_score) > 5:
                    severity = "high"
                elif abs(anomaly.z_score) > 3:
                    severity = "medium"
                else:
                    severity = "low"
                
                severity_map = {"low": 10, "medium": 30, "high": 50, "critical": 100}
                severity_score = severity_map.get(severity, 30)
                score += severity_score
            
            # Cap at 100 per sensor
            breakdown[sensor] = min(100, score)
        
        return breakdown
    
    def _update_baseline(self):
        """Update baseline from historical data."""
        current_time = time.time()
        cutoff = current_time - self.baseline_window
        
        # Get recent history
        recent = [s for s in self._history if s.timestamp > cutoff]
        
        if len(recent) < 10:
            # Not enough data, use default
            self._baseline_cache = {"index": 15.0}
        else:
            # Calculate average index
            avg_index = sum(s.index for s in recent) / len(recent)
            self._baseline_cache = {"index": avg_index}
        
        self._last_baseline_update = current_time
        logger.info(f"Baseline updated: {self._baseline_cache['index']:.1f}")
    
    def _get_baseline_index(self) -> float:
        """Get baseline anomaly index."""
        return self._baseline_cache.get("index", 15.0)
    
    def _determine_status(self, index: float, baseline_ratio: float) -> str:
        """Determine status based on index and baseline ratio."""
        if index >= 80 or baseline_ratio >= 3.0:
            return "critical"
        elif index >= 60 or baseline_ratio >= 2.0:
            return "high"
        elif index >= 40 or baseline_ratio >= 1.5:
            return "elevated"
        else:
            return "normal"
    
    def get_stats(self) -> dict[str, Any]:
        """Get calculator statistics."""
        return {
            "history_size": len(self._history),
            "baseline": self._baseline_cache.get("index", 0),
            "last_baseline_update": self._last_baseline_update
        }
