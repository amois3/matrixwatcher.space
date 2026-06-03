"""Hybrid anomaly detector for Matrix Watcher.

Combines two honest detection strategies, each applied where it is correct:

  * ADAPTIVE (robust, floating) — for stationary continuous streams, an
    anomaly is a value far in the tail of that stream's OWN recent
    distribution (median +/- MAD). The threshold floats with the regime; no
    hardcoded per-sensor numbers. Crypto uses a volatility-relative 1h return.

  * NAMED PHYSICAL EVENTS (fixed thresholds) — for things defined by physics,
    not by a stream's self-distribution: a real earthquake (M5/M6), a
    geomagnetic storm (Kp>=5), an M-class solar flare, a proton event, a new
    volcanic eruption, an abnormal block interval. These are also the
    prediction targets, so their thresholds are physically meaningful.

``process(event)`` returns the union, so this is a drop-in for ThresholdDetector.
"""

from __future__ import annotations

from ...core.types import Event, AnomalyEvent
from ...core.event_bus import EventBus
from .anomaly_detector import OnlineAnomalyDetector
from .threshold_detector import ThresholdDetector


# Stationary streams watched adaptively, with the right per-stream feature.
FEATURE_SPEC: dict[str, dict[str, str]] = {
    "crypto":         {"btcusdt.price": "ret:3600", "ethusdt.price": "ret:3600"},
    "weather":        {"temperature_celsius": "level", "pressure_hpa": "level"},
    "quantum_rng":    {"randomness_score": "level"},
    "solar_activity": {"f107_flux": "level"},
    "solar_wind":     {"speed": "level", "density": "level", "bt": "level", "bz_gsm": "level"},
    "wikipedia_edits":{"edits_per_sec": "level"},
}

# Threshold rules to KEEP (named physical events). Everything else in the
# ThresholdDetector (crypto %, quantum, weather change, the Kp %-change, dead
# fields) is dropped — those streams are handled adaptively or are noise.
NAMED_EVENTS: set[str] = {
    "Significant earthquake",
    "Multiple earthquakes",
    "Geomagnetic storm",
    "X-ray flare M-class or stronger (peak 24h)",
    "Solar proton event in progress",
    "New volcanic eruption reported",
    "New volcanic unrest reported",
    "Blockchain block-time anomaly",
    "Geoeffective solar wind (southward IMF)",
}


class HybridDetector:
    """Adaptive robust detection + named physical-event thresholds."""

    def __init__(self, event_bus: EventBus | None = None, robust_z_threshold: float = 3.0):
        self.adaptive = OnlineAnomalyDetector(
            event_bus=event_bus,
            feature_spec=FEATURE_SPEC,
            robust_z_threshold=robust_z_threshold,
        )
        self.threshold = ThresholdDetector(event_bus=event_bus, enable_calibration_tracking=False)
        # keep only the named physical-event rules
        self.threshold._rules = [r for r in self.threshold._rules if r.description in NAMED_EVENTS]
        self.threshold._index_rules()

    def process(self, event: Event) -> list[AnomalyEvent]:
        return self.adaptive.process(event) + self.threshold.process(event)

    def get_anomaly_count(self) -> int:
        return self.adaptive.get_anomaly_count() + getattr(self.threshold, "_anomaly_count", 0)
