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

import json
import logging
from pathlib import Path

from ...core.types import Event, AnomalyEvent
from ...core.event_bus import EventBus
from .anomaly_detector import OnlineAnomalyDetector
from .threshold_detector import ThresholdDetector

logger = logging.getLogger(__name__)


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
    "Geomagnetic storm",
    "X-ray flare M-class or stronger (peak 24h)",
    "Solar proton event in progress",
    "New volcanic eruption reported",
    "New volcanic unrest reported",
    "Blockchain block-time anomaly",
    "Geoeffective solar wind (southward IMF)",
    "News publication burst",
}


class HybridDetector:
    """Adaptive robust detection + named physical-event thresholds."""

    def __init__(self, event_bus: EventBus | None = None, robust_z_threshold: float = 3.0,
                 anomaly_log_dir: str | Path | None = None):
        self.adaptive = OnlineAnomalyDetector(
            event_bus=event_bus,
            feature_spec=FEATURE_SPEC,
            robust_z_threshold=robust_z_threshold,
        )
        self.threshold = ThresholdDetector(event_bus=event_bus, enable_calibration_tracking=False)
        # keep only the named physical-event rules
        self.threshold._rules = [r for r in self.threshold._rules if r.description in NAMED_EVENTS]
        self.threshold._index_rules()
        self._seen_quake_ids: set[str] = set()
        if anomaly_log_dir is not None:
            self._load_seen_quakes(Path(anomaly_log_dir))

    def _load_seen_quakes(self, log_dir: Path) -> None:
        """Restore IDs from durable anomalies before the first live poll."""
        if not log_dir.exists():
            return
        # The USGS feed covers one hour; two recent daily files span restarts
        # around midnight without scanning the full historical archive.
        for path in sorted(log_dir.glob("*.jsonl"), reverse=True)[:2]:
            try:
                with path.open(encoding="utf-8") as stream:
                    for line in stream:
                        try:
                            record = json.loads(line)
                            quake_id = (record.get("metadata") or {}).get("usgs_id")
                            if record.get("sensor_source") == "earthquake" and quake_id:
                                self._seen_quake_ids.add(str(quake_id))
                        except (ValueError, TypeError, AttributeError):
                            continue
            except OSError:
                logger.exception("Could not restore earthquake IDs from %s", path)

    def mark_persisted(self, anomaly: AnomalyEvent) -> None:
        """Acknowledge an earthquake only after its anomaly is durable."""
        quake_id = (anomaly.metadata or {}).get("usgs_id")
        if quake_id:
            self._seen_quake_ids.add(str(quake_id))

    def _quake_events(self, event: Event) -> list[AnomalyEvent]:
        emitted: set[str] = set()
        out: list[AnomalyEvent] = []
        for quake in event.payload.get("significant_events", []):
            if not isinstance(quake, dict):
                continue
            quake_id = str(quake.get("id") or "")
            if not quake_id or quake_id in self._seen_quake_ids or quake_id in emitted:
                continue
            try:
                magnitude = float(quake["magnitude"])
            except (KeyError, TypeError, ValueError):
                continue
            if magnitude < 4.5:
                continue
            try:
                origin_time = float(quake["time"])
            except (KeyError, TypeError, ValueError):
                origin_time = event.timestamp
            if not 0 < origin_time <= event.timestamp + 120:
                continue
            emitted.add(quake_id)
            out.append(AnomalyEvent(
                timestamp=origin_time, parameter="earthquake.max_magnitude",
                value=magnitude, mean=0.0, std=1.0, z_score=10.0,
                sensor_source="earthquake", metadata={
                    "reason": f"USGS earthquake M{magnitude:.1f}",
                    "rule_description": "Significant earthquake",
                    "severity": "high", "detection_method": "usgs_event",
                    "usgs_id": quake_id, "event_time": origin_time,
                    "observed_at": event.timestamp,
                    "place": quake.get("place"), "latitude": quake.get("latitude"),
                    "longitude": quake.get("longitude"),
                },
            ))
        return out

    def process(self, event: Event) -> list[AnomalyEvent]:
        if event.source == "quantum_rng" and event.payload.get("source") != "anu_quantum":
            return []
        if event.source == "news" and event.payload.get("feeds_successful", 0) < 3:
            return []
        adaptive = self.adaptive.process(event)
        if event.source == "quantum_rng":
            for anomaly in adaptive:
                anomaly.metadata = {**(anomaly.metadata or {}), "measurement_source": "anu_quantum"}
        if event.source == "earthquake" and isinstance(event.payload.get("significant_events"), list):
            # The old max-magnitude edge latch conflates two distinct USGS IDs.
            # Keep it for legacy replay records that lack per-event IDs.
            threshold_event = Event(
                timestamp=event.timestamp, source=event.source, event_type=event.event_type,
                payload={k: v for k, v in event.payload.items() if k != "max_magnitude"},
                severity=event.severity, metadata=event.metadata,
            )
            return adaptive + self.threshold.process(threshold_event) + self._quake_events(event)
        return adaptive + self.threshold.process(event)

    def get_anomaly_count(self) -> int:
        return self.adaptive.get_anomaly_count() + getattr(self.threshold, "_anomaly_count", 0)
