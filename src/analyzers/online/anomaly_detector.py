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
    """Real-time ROBUST ADAPTIVE anomaly detector (debugged from the original
    z-score design).

    For each parameter it keeps a TIME-BASED rolling window of recent values and
    flags a value as anomalous when it sits far in the tail of that stream's OWN
    recent distribution. It uses robust statistics (median + MAD) instead of
    mean/std, so:
      - a single glitch does not poison detection for many subsequent samples;
      - a constant stream (MAD == 0) never fires (no fake anomalies);
      - the threshold FLOATS with each stream's regime (quiet sun vs storm,
        calm vs volatile market) — no hardcoded per-sensor numbers.
    Edge-triggered: one ongoing excursion fires once (rising edge), with
    hysteresis so it does not flap.
    """

    def __init__(
        self,
        window_seconds: float = 7 * 86400,
        robust_z_threshold: float = 3.5,
        ret_floor: float = 1.0,
        min_obs: int = 40,
        max_points: int = 5000,
        recompute_interval: float = 300.0,
        event_bus: "EventBus | None" = None,
        # kept for backwards compatibility with old callers/tests:
        default_window_size: int = 100,
        default_z_threshold: float = 4.0,
        feature_spec: "dict | None" = None,
    ):
        self.window_seconds = float(window_seconds)
        self.k = float(robust_z_threshold)
        self._ret_floor = float(ret_floor)  # min |move| for return features (no micro-noise in calm)
        self.min_obs = int(min_obs)
        self.max_points = int(max_points)
        self.recompute_interval = float(recompute_interval)
        self.event_bus = event_bus
        self.default_window_size = default_window_size
        self.default_z_threshold = default_z_threshold
        # feature_spec: {sensor_source: set(field_names)} — only these fields are
        # watched adaptively. None = watch every numeric field (legacy).
        if feature_spec:
            norm = {}
            for sensor, fields in feature_spec.items():
                norm[sensor] = dict(fields) if isinstance(fields, dict) else {f: "level" for f in fields}
            self.feature_spec = norm
        else:
            self.feature_spec = None
        self._price_hist: dict[str, deque] = {}

        self._win: dict[str, deque] = {}        # param -> deque[(ts, value)]
        self._cache: dict[str, tuple] = {}       # param -> (median, scale, computed_ts)
        self._latched: dict[str, bool] = {}
        self._cfg: dict[str, tuple] = {}         # param -> (window_seconds, k)
        self._anomaly_count = 0

    def configure_parameter(self, param: str, window_seconds: float | None = None,
                            robust_z_threshold: float | None = None) -> None:
        self._cfg[param] = (
            float(window_seconds) if window_seconds else self.window_seconds,
            float(robust_z_threshold) if robust_z_threshold else self.k,
        )

    @staticmethod
    def _median(xs: list[float]) -> float:
        n = len(xs)
        s = sorted(xs)
        m = n // 2
        return s[m] if n % 2 else 0.5 * (s[m - 1] + s[m])

    def process(self, event: Event) -> list[AnomalyEvent]:
        out = []
        ts = event.timestamp
        for key, value in event.payload.items():
            if not isinstance(value, (int, float)) or value is None:
                continue
            transform = "level"
            if self.feature_spec is not None:
                fields = self.feature_spec.get(event.source)
                if not fields or key not in fields:
                    continue
                transform = fields[key]
            if transform == "level":
                a = self._check(f"{event.source}.{key}", float(value), event.source, ts)
                if a:
                    out.append(a)
            elif transform.startswith("ret:"):
                # adaptive, volatility-relative: robust-z of the H-second return,
                # whose own dispersion IS the recent realised volatility.
                horizon = float(transform.split(":", 1)[1])
                hk = f"{event.source}.{key}"
                ph = self._price_hist.get(hk)
                if ph is None:
                    ph = self._price_hist[hk] = deque()
                val = float(value)
                while len(ph) >= 2 and ph[1][0] <= ts - horizon:
                    ph.popleft()
                old = ph[0][1] if (ph and ph[0][0] <= ts - horizon) else None
                ph.append((ts, val))
                if old is not None and old != 0:
                    ret = (val / old - 1.0) * 100.0
                    a = self._check(f"{event.source}.{key}.ret{int(horizon)}", ret, event.source, ts)
                    if a:
                        out.append(a)
        return out

    def _check(self, param: str, value: float, source: str, ts: float) -> "AnomalyEvent | None":
        win_s, k = self._cfg.get(param, (self.window_seconds, self.k))
        w = self._win.get(param)
        if w is None:
            w = self._win[param] = deque(maxlen=self.max_points)
        # time-based eviction (maxlen also caps the count)
        cutoff = ts - win_s
        while w and w[0][0] < cutoff:
            w.popleft()

        fired = None
        if len(w) >= self.min_obs:
            cached = self._cache.get(param)
            if cached is None or (ts - cached[2]) >= self.recompute_interval:
                vals = [v for _, v in w]
                med = self._median(vals)
                mad = self._median([abs(v - med) for v in vals])
                scale = 1.4826 * mad
                self._cache[param] = (med, scale, ts)
            else:
                med, scale, _ = cached

            rz = (value - med) / scale if scale > 0 else 0.0
            if abs(rz) > k and ((".ret" not in param) or abs(value) >= self._ret_floor):
                if not self._latched.get(param, False):
                    self._latched[param] = True
                    self._anomaly_count += 1
                    direction = "above" if value > med else "below"
                    reason = (f"{value:.4g} is {abs(rz):.1f} robust-σ {direction} its "
                              f"{int(win_s / 86400)}d median {med:.4g}")
                    sev = ("critical" if abs(rz) >= 2 * k else
                           "high" if abs(rz) >= 1.4 * k else "medium")
                    fired = AnomalyEvent(
                        timestamp=ts, parameter=param, value=value,
                        mean=med, std=scale, z_score=round(rz, 2),
                        sensor_source=source,
                        metadata={"reason": reason, "rule_description": "adaptive deviation",
                                  "severity": sev, "detection_method": "robust_rolling"},
                    )
                    if self.event_bus:
                        self.event_bus.publish(fired.to_event())
            elif abs(rz) < 0.7 * k:
                self._latched[param] = False   # hysteresis reset

        w.append((ts, value))
        return fired

    def get_window_stats(self, param: str) -> dict | None:
        w = self._win.get(param)
        if not w:
            return None
        vals = [v for _, v in w]
        med = self._median(vals)
        mad = self._median([abs(v - med) for v in vals])
        return {"count": len(vals), "median": med, "mad": mad, "scale": 1.4826 * mad}

    def reset_window(self, param: str) -> None:
        self._win.pop(param, None)
        self._cache.pop(param, None)
        self._latched.pop(param, None)

    def get_anomaly_count(self) -> int:
        return self._anomaly_count

    def clear(self) -> None:
        self._win.clear()
        self._cache.clear()
        self._latched.clear()
