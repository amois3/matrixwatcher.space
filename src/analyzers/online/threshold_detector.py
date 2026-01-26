"""Threshold-based Anomaly Detector.

Detects real anomalies using percentage changes and absolute thresholds
instead of statistical z-scores. More suitable for real-world data.

All thresholds are logged for future calibration.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from ...core.types import Event, EventType, AnomalyEvent
from ...core.event_bus import EventBus
from ...monitoring.calibration_tracker import get_tracker

logger = logging.getLogger(__name__)


@dataclass
class ThresholdRule:
    """Rule for detecting anomalies in a parameter."""
    parameter_pattern: str  # e.g., "crypto.*price"
    min_change_percent: float | None = None  # Minimum % change to trigger
    max_absolute_value: float | None = None  # Maximum absolute value
    min_absolute_value: float | None = None  # Minimum absolute value
    trigger_when_above: float | None = None  # Trigger when value >= this (opposite of min_absolute_value)
    lookback_seconds: float = 60.0  # How far back to compare
    description: str = ""


class ThresholdDetector:
    """Detects anomalies using configurable thresholds.
    
    More accurate than z-score for real-world data where we know
    what constitutes a real anomaly.
    """
    
    def __init__(self, event_bus: EventBus | None = None, enable_calibration_tracking: bool = True):
        """Initialize threshold detector.
        
        Args:
            event_bus: Event bus for publishing anomalies
            enable_calibration_tracking: Whether to log threshold checks for calibration
        """
        self.event_bus = event_bus
        self._history: dict[str, deque] = {}
        self._anomaly_count = 0
        self._enable_calibration_tracking = enable_calibration_tracking
        
        # Load calibrated thresholds first
        self._calibrated_thresholds = self._load_calibrated_thresholds()
        
        # Define rules for different sensors (with calibrated values applied)
        self._rules = self._create_default_rules()
        
        # Get calibration tracker
        if self._enable_calibration_tracking:
            self._tracker = get_tracker()
        else:
            self._tracker = None
    
    def _load_calibrated_thresholds(self) -> dict:
        """Load calibrated thresholds from file."""
        import json
        from pathlib import Path
        
        calibrated_file = Path("logs/calibration/calibrated_thresholds.json")
        if calibrated_file.exists():
            try:
                with open(calibrated_file, "r") as f:
                    data = json.load(f)
                logger.info(f"Loaded {len(data)} calibrated thresholds")
                return data
            except Exception as e:
                logger.error(f"Failed to load calibrated thresholds: {e}")
        return {}
    
    def _get_calibrated_value(self, threshold_name: str, default: float) -> float:
        """Get calibrated value for threshold or return default."""
        if threshold_name in self._calibrated_thresholds:
            value = self._calibrated_thresholds[threshold_name]["value"]
            logger.debug(f"Using calibrated {threshold_name}: {value} (was {default})")
            return value
        return default
    
    def _create_default_rules(self) -> list[ThresholdRule]:
        """Create default detection rules."""
        return [
            # Crypto: Detect price changes > 1% in 60 seconds
            ThresholdRule(
                parameter_pattern="crypto.*.price",
                min_change_percent=self._get_calibrated_value("crypto.btcusdt.price.change_pct", 1.0),
                lookback_seconds=60.0,
                description="Резкое изменение цены криптовалюты"
            ),
            
            # Crypto: Detect volume spikes > 50%
            ThresholdRule(
                parameter_pattern="crypto.*.volume_24h",
                min_change_percent=self._get_calibrated_value("crypto.btcusdt.volume_24h.change_pct", 50.0),
                lookback_seconds=300.0,
                description="Всплеск объёма торгов"
            ),
            
            # Network: Detect high latency > 1000ms
            ThresholdRule(
                parameter_pattern="network.*.latency_ms",
                max_absolute_value=1000.0,
                description="Высокая задержка сети"
            ),
            
            # Network: Detect latency spikes > 100% increase
            ThresholdRule(
                parameter_pattern="network.avg_latency_ms",
                min_change_percent=100.0,
                lookback_seconds=30.0,
                description="Резкий рост задержки сети"
            ),
            
            # Time drift: Detect drift change > 100ms
            ThresholdRule(
                parameter_pattern="time_drift.diff_local_ntp_ms",
                min_change_percent=150.0,  # Change in drift magnitude
                lookback_seconds=60.0,
                description="Резкое изменение синхронизации времени"
            ),
            
            # Time drift: Detect extreme drift > 500ms
            ThresholdRule(
                parameter_pattern="time_drift.diff_local_ntp_ms",
                max_absolute_value=500.0,
                min_absolute_value=-500.0,
                description="Экстремальная рассинхронизация времени"
            ),
            
            # News: Detect headline spikes > 2x average
            ThresholdRule(
                parameter_pattern="news.headline_count",
                min_change_percent=100.0,
                lookback_seconds=300.0,
                description="Всплеск новостей"
            ),
            
            # Blockchain: Detect unusual block times
            ThresholdRule(
                parameter_pattern="blockchain.*.block_time_seconds",
                min_change_percent=50.0,
                lookback_seconds=600.0,
                description="Необычное время блока"
            ),
            
            # Weather: Detect rapid temperature changes > 5°C
            ThresholdRule(
                parameter_pattern="weather.temperature",
                min_change_percent=10.0,
                lookback_seconds=300.0,
                description="Резкое изменение температуры"
            ),
            
            # Weather: Detect rapid pressure changes
            ThresholdRule(
                parameter_pattern="weather.pressure",
                min_change_percent=2.0,
                lookback_seconds=300.0,
                description="Резкое изменение давления"
            ),
            
            # Random: Detect bias in random numbers
            ThresholdRule(
                parameter_pattern="random.mean",
                min_absolute_value=0.45,
                max_absolute_value=0.55,
                description="Смещение в генераторе случайных чисел"
            ),
            
            # Quantum RNG: Detect low randomness (calibrated from data)
            ThresholdRule(
                parameter_pattern="quantum_rng.randomness_score",
                min_absolute_value=self._get_calibrated_value("quantum_rng.randomness_score.min", 0.85),
                description="Квантовая случайность ниже нормы"
            ),
            
            # Earthquake: Detect significant earthquakes (calibrated)
            ThresholdRule(
                parameter_pattern="earthquake.max_magnitude",
                trigger_when_above=self._get_calibrated_value("earthquake.max_magnitude.trigger_above", 4.5),
                description="Значительное землетрясение"
            ),
            
            # Earthquake: Detect multiple earthquakes
            ThresholdRule(
                parameter_pattern="earthquake.count",
                trigger_when_above=self._get_calibrated_value("earthquake.count.trigger_above", 2),
                description="Множественные землетрясения"
            ),
            
            # Space Weather: Detect high Kp index >= 5 (geomagnetic storm)
            ThresholdRule(
                parameter_pattern="space_weather.kp_index",
                trigger_when_above=5.0,
                description="Геомагнитная буря"
            ),
            
            # Space Weather: Detect rapid Kp increase
            ThresholdRule(
                parameter_pattern="space_weather.kp_index",
                min_change_percent=50.0,
                lookback_seconds=3600.0,  # 1 hour
                description="Резкий рост геомагнитной активности"
            ),
            
            # Space Weather: Detect solar flares
            ThresholdRule(
                parameter_pattern="space_weather.flare_count",
                trigger_when_above=0.5,  # > 0 (at least 1 flare)
                description="Солнечная вспышка"
            ),
        ]
    
    def add_rule(self, rule: ThresholdRule) -> None:
        """Add a custom detection rule."""
        self._rules.append(rule)
    
    def process(self, event: Event) -> list[AnomalyEvent]:
        """Process an event and detect anomalies.
        
        Args:
            event: Event to process
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        for key, value in event.payload.items():
            if not isinstance(value, (int, float)) or value is None:
                continue
            
            param_key = f"{event.source}.{key}"
            
            # Log value for distribution analysis
            if self._tracker:
                self._tracker.log_value_distribution(
                    parameter_name=param_key,
                    value=float(value),
                    metadata={"source": event.source, "timestamp": event.timestamp}
                )
            
            # Check all matching rules
            for rule in self._rules:
                if self._matches_pattern(param_key, rule.parameter_pattern):
                    anomaly = self._check_rule(
                        param_key=param_key,
                        value=float(value),
                        rule=rule,
                        timestamp=event.timestamp,
                        source=event.source
                    )
                    if anomaly:
                        anomalies.append(anomaly)
                        break  # Only one anomaly per parameter
        
        return anomalies
    
    def _matches_pattern(self, param_key: str, pattern: str) -> bool:
        """Check if parameter matches pattern (supports wildcards)."""
        import re
        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*")
        return bool(re.match(f"^{regex_pattern}$", param_key))
    
    def _check_rule(
        self,
        param_key: str,
        value: float,
        rule: ThresholdRule,
        timestamp: float,
        source: str
    ) -> AnomalyEvent | None:
        """Check if value violates a rule."""
        # Store value in history
        if param_key not in self._history:
            self._history[param_key] = deque(maxlen=1000)
        
        self._history[param_key].append({
            "timestamp": timestamp,
            "value": value
        })
        
        history = self._history[param_key]
        
        # Check absolute thresholds (work on first value)
        if rule.max_absolute_value is not None:
            triggered = value > rule.max_absolute_value
            
            # Log for calibration
            if self._tracker:
                self._tracker.log_threshold_check(
                    threshold_name=f"{param_key}.max",
                    value=value,
                    threshold_value=rule.max_absolute_value,
                    triggered=triggered,
                    metadata={"rule": rule.description, "source": source}
                )
            
            if triggered:
                return self._create_anomaly(
                    param_key=param_key,
                    value=value,
                    timestamp=timestamp,
                    source=source,
                    reason=f"Превышен порог: {value:.2f} > {rule.max_absolute_value:.2f}",
                    rule_description=rule.description,
                    severity="high"
                )
        
        if rule.min_absolute_value is not None:
            triggered = value < rule.min_absolute_value
            
            # Log for calibration
            if self._tracker:
                self._tracker.log_threshold_check(
                    threshold_name=f"{param_key}.min",
                    value=value,
                    threshold_value=rule.min_absolute_value,
                    triggered=triggered,
                    metadata={"rule": rule.description, "source": source}
                )
            
            if triggered:
                return self._create_anomaly(
                    param_key=param_key,
                    value=value,
                    timestamp=timestamp,
                    source=source,
                    reason=f"Ниже порога: {value:.2f} < {rule.min_absolute_value:.2f}",
                    rule_description=rule.description,
                    severity="high"
                )
        
        if rule.trigger_when_above is not None:
            triggered = value >= rule.trigger_when_above
            
            # Log for calibration
            if self._tracker:
                self._tracker.log_threshold_check(
                    threshold_name=f"{param_key}.trigger_above",
                    value=value,
                    threshold_value=rule.trigger_when_above,
                    triggered=triggered,
                    metadata={"rule": rule.description, "source": source}
                )
            
            if triggered:
                return self._create_anomaly(
                    param_key=param_key,
                    value=value,
                    timestamp=timestamp,
                    source=source,
                    reason=f"Обнаружено: {value:.2f} >= {rule.trigger_when_above:.2f}",
                    rule_description=rule.description,
                    severity="high"
                )
        
        # Check percentage change (needs at least 2 values)
        if rule.min_change_percent is not None and len(history) >= 2:
            # Find value from lookback period
            lookback_time = timestamp - rule.lookback_seconds
            old_values = [h for h in history if h["timestamp"] >= lookback_time]
            
            if len(old_values) >= 2:
                old_value = old_values[0]["value"]
                
                # Calculate percentage change
                if old_value != 0:
                    change_pct = abs((value - old_value) / old_value * 100)
                    triggered = change_pct >= rule.min_change_percent
                    
                    # Log for calibration
                    if self._tracker:
                        self._tracker.log_threshold_check(
                            threshold_name=f"{param_key}.change_pct",
                            value=change_pct,
                            threshold_value=rule.min_change_percent,
                            triggered=triggered,
                            metadata={
                                "rule": rule.description,
                                "source": source,
                                "lookback_seconds": rule.lookback_seconds,
                                "old_value": old_value,
                                "new_value": value
                            }
                        )
                    
                    if triggered:
                        direction = "выросло" if value > old_value else "упало"
                        return self._create_anomaly(
                            param_key=param_key,
                            value=value,
                            timestamp=timestamp,
                            source=source,
                            reason=f"Значение {direction} на {change_pct:.1f}% за {rule.lookback_seconds:.0f}с (было {old_value:.2f})",
                            rule_description=rule.description,
                            severity=self._calculate_severity(change_pct, rule.min_change_percent)
                        )
        
        return None
    
    def _create_anomaly(
        self,
        param_key: str,
        value: float,
        timestamp: float,
        source: str,
        reason: str,
        rule_description: str,
        severity: str
    ) -> AnomalyEvent:
        """Create an anomaly event."""
        self._anomaly_count += 1
        
        # Calculate z_score equivalent for compatibility (just use severity)
        z_score_map = {"low": 5.0, "medium": 7.0, "high": 10.0, "critical": 15.0}
        z_score = z_score_map.get(severity, 10.0)
        
        anomaly = AnomalyEvent(
            timestamp=timestamp,
            parameter=param_key,
            value=value,
            mean=0.0,  # Not used in threshold detection
            std=1.0,   # Not used in threshold detection
            z_score=z_score,
            sensor_source=source,
            metadata={
                "reason": reason,
                "rule_description": rule_description,
                "severity": severity,
                "detection_method": "threshold"
            }
        )
        
        if self.event_bus:
            self.event_bus.publish(anomaly.to_event())
        
        logger.info(f"Anomaly detected: {rule_description} - {reason}")
        
        return anomaly
    
    def _calculate_severity(self, change_pct: float, threshold: float) -> str:
        """Calculate severity based on how much threshold was exceeded."""
        ratio = change_pct / threshold
        
        if ratio >= 3.0:
            return "critical"
        elif ratio >= 2.0:
            return "high"
        elif ratio >= 1.5:
            return "medium"
        else:
            return "low"
    
    def get_anomaly_count(self) -> int:
        """Get total anomaly count."""
        return self._anomaly_count
    
    def get_history(self, param_key: str) -> list[dict]:
        """Get history for a parameter."""
        if param_key not in self._history:
            return []
        return list(self._history[param_key])
