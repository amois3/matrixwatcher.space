"""Threshold-based Anomaly Detector.

Detects real anomalies using percentage changes and absolute thresholds
instead of statistical z-scores. More suitable for real-world data.

All thresholds are logged for future calibration.
"""

import logging
import re
import time
from collections import defaultdict, deque
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
    min_change_absolute: float | None = None  # Minimum absolute change to trigger (e.g. °C, hPa)
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
        # Edge-trigger latches: a rule fires once when its condition becomes true
        # and stays suppressed while the condition persists, so a single ongoing
        # event (e.g. one earthquake that USGS keeps in its "last hour" feed for
        # ~60 polls, or a multi-hour geomagnetic storm) is not re-emitted on every
        # poll. Keyed by f"{param_key}::{rule.description}".
        self._latched: dict[str, bool] = {}
        self._anomaly_count = 0
        self._enable_calibration_tracking = enable_calibration_tracking
        
        # Load calibrated thresholds first
        self._calibrated_thresholds = self._load_calibrated_thresholds()

        # Define rules for different sensors (with calibrated values applied)
        self._rules = self._create_default_rules()
        self._index_rules()

        # Get calibration tracker
        if self._enable_calibration_tracking:
            self._tracker = get_tracker()
        else:
            self._tracker = None

    @staticmethod
    def _compile_pattern(pattern: str) -> re.Pattern:
        """Compile a parameter_pattern (with dots literal and ``*`` as wildcard) to regex."""
        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*")
        return re.compile(f"^{regex_pattern}$")

    def _index_rules(self) -> None:
        """Pre-compile each rule's regex and group rules by source prefix.

        This is a pure performance optimisation: ``process()`` no longer
        re-compiles the regex on every event and only iterates rules whose
        source token matches the event's source (the regex still validates the
        full ``param_key`` so semantics are identical to the previous
        per-rule loop).
        """
        self._rules_by_source: dict[str, list[ThresholdRule]] = defaultdict(list)
        self._wildcard_rules: list[ThresholdRule] = []
        for rule in self._rules:
            # attach the compiled regex to the rule for fast matching
            object.__setattr__(rule, "_compiled", self._compile_pattern(rule.parameter_pattern))
            first = rule.parameter_pattern.split(".", 1)[0]
            if first == "" or "*" in first:
                # pattern with a wildcard *source* — must be checked against every event
                self._wildcard_rules.append(rule)
            else:
                self._rules_by_source[first].append(rule)
    
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
                description="Sharp cryptocurrency price change"
            ),
            
            # Crypto: Detect volume spikes > 50%
            ThresholdRule(
                parameter_pattern="crypto.*.volume_24h",
                min_change_percent=self._get_calibrated_value("crypto.btcusdt.volume_24h.change_pct", 50.0),
                lookback_seconds=300.0,
                description="Trading volume spike"
            ),
            
            # Network: Detect high latency > 1000ms
            ThresholdRule(
                parameter_pattern="network.*.latency_ms",
                max_absolute_value=1000.0,
                description="High network latency"
            ),
            
            # Network: Detect latency spikes > 100% increase
            ThresholdRule(
                parameter_pattern="network.avg_latency_ms",
                min_change_percent=100.0,
                lookback_seconds=30.0,
                description="Sharp network latency increase"
            ),
            
            # Time drift: Detect drift change > 100ms
            ThresholdRule(
                parameter_pattern="time_drift.diff_local_ntp_ms",
                min_change_percent=150.0,  # Change in drift magnitude
                lookback_seconds=60.0,
                description="Sharp time sync change"
            ),
            
            # Time drift: Detect extreme drift > 500ms
            ThresholdRule(
                parameter_pattern="time_drift.diff_local_ntp_ms",
                max_absolute_value=500.0,
                min_absolute_value=-500.0,
                description="Extreme time desynchronization"
            ),
            
            # News: Detect headline volume spikes > 2x. Lookback must exceed the
            # ~900s poll interval, otherwise there is never a prior reading in
            # the window to compare against (the old 300s lookback never fired).
            ThresholdRule(
                parameter_pattern="news.headline_count",
                min_change_percent=100.0,
                lookback_seconds=2000.0,
                description="News spike"
            ),

            # Blockchain: the sensor sets `any_anomalous` (a top-level 0/1 flag)
            # when block times deviate. The previous rule targeted a NESTED field
            # (blockchain.*.block_time_seconds) which is never a top-level numeric
            # key, so it could never fire. Use the flag the sensor actually emits.
            ThresholdRule(
                parameter_pattern="blockchain.any_anomalous",
                trigger_when_above=0.5,
                description="Blockchain block-time anomaly"
            ),

            # Weather: rapid temperature swing (≥5 °C). Field is `temperature_celsius`,
            # not `temperature` (the old rule never matched). Uses absolute change
            # because a percentage is unstable near 0 °C.
            ThresholdRule(
                parameter_pattern="weather.temperature_celsius",
                min_change_absolute=5.0,
                lookback_seconds=1800.0,
                description="Sharp temperature change"
            ),

            # Weather: rapid pressure swing (≥8 hPa over 3h = a real front).
            # Field is `pressure_hpa`; absolute change (pressure barely moves in %).
            ThresholdRule(
                parameter_pattern="weather.pressure_hpa",
                min_change_absolute=8.0,
                lookback_seconds=10800.0,
                description="Sharp pressure change"
            ),
            
            # Random: Detect bias in random numbers
            ThresholdRule(
                parameter_pattern="random.mean",
                min_absolute_value=0.45,
                max_absolute_value=0.55,
                description="Random number generator bias"
            ),
            
            # Quantum RNG: Detect low randomness (calibrated from data)
            ThresholdRule(
                parameter_pattern="quantum_rng.randomness_score",
                min_absolute_value=self._get_calibrated_value("quantum_rng.randomness_score.min", 0.85),
                description="Quantum randomness below normal"
            ),
            
            # Earthquake: Detect significant earthquakes (calibrated)
            ThresholdRule(
                parameter_pattern="earthquake.max_magnitude",
                trigger_when_above=self._get_calibrated_value("earthquake.max_magnitude.trigger_above", 4.5),
                description="Significant earthquake"
            ),
            
            # Earthquake: Detect multiple earthquakes
            ThresholdRule(
                parameter_pattern="earthquake.count",
                trigger_when_above=self._get_calibrated_value("earthquake.count.trigger_above", 2),
                description="Multiple earthquakes"
            ),
            
            # Space Weather: Detect high Kp index >= 5 (geomagnetic storm)
            ThresholdRule(
                parameter_pattern="space_weather.kp_index",
                trigger_when_above=5.0,
                description="Geomagnetic storm"
            ),
            
            # Space Weather: Detect rapid Kp increase
            ThresholdRule(
                parameter_pattern="space_weather.kp_index",
                min_change_percent=50.0,
                lookback_seconds=3600.0,  # 1 hour
                description="Sharp geomagnetic activity increase"
            ),
            
            # Space Weather: Detect solar flares
            ThresholdRule(
                parameter_pattern="space_weather.flare_count",
                trigger_when_above=0.5,  # > 0 (at least 1 flare)
                description="Solar flare"
            ),

            # Solar Activity: M-class or stronger flare (peak >= 1e-5 W/m^2)
            ThresholdRule(
                parameter_pattern="solar_activity.xray_peak_long_flux",
                trigger_when_above=1.0e-5,
                description="X-ray flare M-class or stronger (peak 24h)",
            ),
            # Solar Activity: NOAA proton-event threshold (>=10 pfu at >=10 MeV)
            ThresholdRule(
                parameter_pattern="solar_activity.proton_flux_10mev",
                trigger_when_above=10.0,
                description="Solar proton event in progress",
            ),
            # Solar Activity: high overall radio flux (above 200 sfu is "high")
            ThresholdRule(
                parameter_pattern="solar_activity.f107_flux",
                trigger_when_above=200.0,
                description="High F10.7 solar radio flux",
            ),
            # Solar Activity: rapid F10.7 swing — same shape as the existing Kp rule
            ThresholdRule(
                parameter_pattern="solar_activity.f107_flux",
                min_change_percent=15.0,
                lookback_seconds=86400.0,  # 24h (F10.7 updates ~hourly)
                description="Sharp F10.7 flux change",
            ),

            # Volcanic Activity: fire ONLY when a NEW weekly report brings new
            # eruptions/unrest. The *_fresh fields are zero on re-reads of the same
            # report, so we no longer emit 3 duplicate anomalies on every hourly
            # poll (the old rules on raw new_eruptions/new_unrest/active_count were
            # ~always true for the weekly digest → an always-on noise source).
            ThresholdRule(
                parameter_pattern="volcanic_activity.new_eruptions_fresh",
                trigger_when_above=0.5,  # >=1 new eruption in a fresh weekly report
                description="New volcanic eruption reported",
            ),
            ThresholdRule(
                parameter_pattern="volcanic_activity.new_unrest_fresh",
                trigger_when_above=0.5,
                description="New volcanic unrest reported",
            ),
        ]
    
    def add_rule(self, rule: ThresholdRule) -> None:
        """Add a custom detection rule."""
        self._rules.append(rule)
        # Keep the source index consistent with the rule list
        object.__setattr__(rule, "_compiled", self._compile_pattern(rule.parameter_pattern))
        first = rule.parameter_pattern.split(".", 1)[0]
        if first == "" or "*" in first:
            self._wildcard_rules.append(rule)
        else:
            self._rules_by_source[first].append(rule)
    
    def process(self, event: Event) -> list[AnomalyEvent]:
        """Process an event and detect anomalies.

        Args:
            event: Event to process

        Returns:
            List of detected anomalies
        """
        anomalies = []
        # Only rules that could possibly match this event's source. The regex
        # still validates the full param_key, so behaviour matches the previous
        # "iterate every rule" implementation exactly.
        candidate_rules = self._rules_by_source.get(event.source, [])
        if self._wildcard_rules:
            candidate_rules = candidate_rules + self._wildcard_rules

        if not candidate_rules and not self._tracker:
            return anomalies  # no rules can match and we are not training calibration -> skip

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

            # Check matching rules against this parameter
            for rule in candidate_rules:
                if rule._compiled.match(param_key):
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
        """Public-API check (kept for backwards compatibility with any caller)."""
        return bool(self._compile_pattern(pattern).match(param_key))
    
    def _check_rule(
        self,
        param_key: str,
        value: float,
        rule: ThresholdRule,
        timestamp: float,
        source: str
    ) -> AnomalyEvent | None:
        """Check if value violates a rule.

        Evaluates the rule's single active check into (triggered, reason, severity),
        then applies EDGE-TRIGGERING: an anomaly is emitted only on the rising edge
        (normal → anomalous). While the condition persists, it stays latched and is
        suppressed — so one ongoing event is not re-emitted on every poll. The latch
        resets when the condition clears, so the next genuine occurrence fires again.
        """
        # Store value in history
        if param_key not in self._history:
            self._history[param_key] = deque(maxlen=1000)
        self._history[param_key].append({"timestamp": timestamp, "value": value})
        history = self._history[param_key]

        triggered = False
        reason: str | None = None
        severity = "medium"

        if rule.max_absolute_value is not None:
            triggered = value > rule.max_absolute_value
            if self._tracker:
                self._tracker.log_threshold_check(
                    threshold_name=f"{param_key}.max", value=value,
                    threshold_value=rule.max_absolute_value, triggered=triggered,
                    metadata={"rule": rule.description, "source": source})
            if triggered:
                reason = f"Threshold exceeded: {value:.4g} > {rule.max_absolute_value:.4g}"
                severity = "high"

        elif rule.min_absolute_value is not None:
            triggered = value < rule.min_absolute_value
            if self._tracker:
                self._tracker.log_threshold_check(
                    threshold_name=f"{param_key}.min", value=value,
                    threshold_value=rule.min_absolute_value, triggered=triggered,
                    metadata={"rule": rule.description, "source": source})
            if triggered:
                reason = f"Below threshold: {value:.4g} < {rule.min_absolute_value:.4g}"
                severity = "high"

        elif rule.trigger_when_above is not None:
            triggered = value >= rule.trigger_when_above
            if self._tracker:
                self._tracker.log_threshold_check(
                    threshold_name=f"{param_key}.trigger_above", value=value,
                    threshold_value=rule.trigger_when_above, triggered=triggered,
                    metadata={"rule": rule.description, "source": source})
            if triggered:
                reason = f"Detected: {value:.4g} >= {rule.trigger_when_above:.4g}"
                severity = "high"

        elif rule.min_change_percent is not None and len(history) >= 2:
            lookback_time = timestamp - rule.lookback_seconds
            old_values = [h for h in history if h["timestamp"] >= lookback_time]
            if len(old_values) >= 2:
                old_value = old_values[0]["value"]
                if old_value != 0:
                    change_pct = abs((value - old_value) / old_value * 100)
                    triggered = change_pct >= rule.min_change_percent
                    if self._tracker:
                        self._tracker.log_threshold_check(
                            threshold_name=f"{param_key}.change_pct", value=change_pct,
                            threshold_value=rule.min_change_percent, triggered=triggered,
                            metadata={"rule": rule.description, "source": source,
                                      "lookback_seconds": rule.lookback_seconds,
                                      "old_value": old_value, "new_value": value})
                    if triggered:
                        direction = "increased" if value > old_value else "decreased"
                        reason = (f"Value {direction} by {change_pct:.1f}% in "
                                  f"{rule.lookback_seconds:.0f}s (was {old_value:.2f})")
                        severity = self._calculate_severity(change_pct, rule.min_change_percent)

        # Absolute change (°C, hPa) — where a percentage is unstable/meaningless.
        elif rule.min_change_absolute is not None and len(history) >= 2:
            lookback_time = timestamp - rule.lookback_seconds
            old_values = [h for h in history if h["timestamp"] >= lookback_time]
            if len(old_values) >= 2:
                old_value = old_values[0]["value"]
                change_abs = abs(value - old_value)
                triggered = change_abs >= rule.min_change_absolute
                if self._tracker:
                    self._tracker.log_threshold_check(
                        threshold_name=f"{param_key}.change_abs", value=change_abs,
                        threshold_value=rule.min_change_absolute, triggered=triggered,
                        metadata={"rule": rule.description, "source": source,
                                  "lookback_seconds": rule.lookback_seconds,
                                  "old_value": old_value, "new_value": value})
                if triggered:
                    direction = "increased" if value > old_value else "decreased"
                    reason = (f"Value {direction} by {change_abs:.2f} in "
                              f"{rule.lookback_seconds:.0f}s (was {old_value:.2f})")
                    severity = "medium"

        # --- edge-triggering ---
        latch_key = f"{param_key}::{rule.description}"
        if triggered:
            already_firing = self._latched.get(latch_key, False)
            self._latched[latch_key] = True
            if already_firing:
                return None  # condition still ongoing — don't re-emit
            return self._create_anomaly(
                param_key=param_key, value=value, timestamp=timestamp, source=source,
                reason=reason or "Anomaly", rule_description=rule.description, severity=severity,
            )
        else:
            self._latched[latch_key] = False
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
