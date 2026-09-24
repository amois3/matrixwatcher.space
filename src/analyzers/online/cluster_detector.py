"""Cluster Detector - Detects synchronization of unrelated systems.

A cluster is formed when anomalies from several *independent* sensor sources
occur within the same short time window. The level reflects how many distinct
sources participated:

Level 1: Single source (background fluctuation, not a cluster)
Level 2: 2 independent domains (temporal coincidence)
Level 3: 3 independent domains (multi-domain cluster)
Level 4: 4 independent domains (rare, significant)
Level 5: 5+ independent domains (extreme synchronicity — stands out against
         the whole observation history)

The level is determined by the count of distinct physical/social domains in the
window. The accompanying ``probability`` field is a qualitative rarity tier,
NOT a calibrated statistical p-value (see ``_rarity_indicator``).
"""

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from ...core.types import AnomalyEvent

logger = logging.getLogger(__name__)


# Human-readable label per level (level -> description)
LEVEL_DESCRIPTIONS = {
    1: "Single anomaly",
    2: "Double correlation",
    3: "Triple cluster",
    4: "System disturbance",
    5: "Critical synchronicity",
}

# Related feeds must not be counted as independent evidence. In particular,
# solar activity, solar wind and Kp measure one physical chain.
SOURCE_DOMAINS = {
    "solar_activity": "heliophysics", "solar_wind": "heliophysics",
    "space_weather": "heliophysics", "earthquake": "geophysics",
    "volcanic_activity": "geophysics", "earth_tides": "geophysics",
    "news": "human_activity", "wikipedia_edits": "human_activity",
    "wikimedia_edits": "human_activity",  # Historical source name in anomaly logs.
    "crypto": "markets", "blockchain": "blockchain",
    "weather": "atmosphere", "weather_grid": "atmosphere",
    "remote_stations": "atmosphere", "quantum_rng": "quantum",
    "fireball": "astronomy", "ripe_atlas": "network",
    "system": "local_system", "network": "local_system",
    "random": "local_system", "time_drift": "local_system",
}


def source_domain(source: str) -> str:
    return SOURCE_DOMAINS.get(source, source)


@dataclass
class AnomalyCluster:
    """Represents a cluster of anomalies."""
    level: int  # 1-5
    anomalies: list[AnomalyEvent]
    timestamp: float
    probability: float  # Qualitative rarity tier, NOT a calibrated p-value
    description: str
    is_precursor: bool = False
    precursor_event: Any = None
    domains: tuple[str, ...] = ()
    source_count: int = 0


class ClusterDetector:
    """Detects clusters and correlations between anomalies.

    A cluster's level equals the number of distinct domains that produced
    an anomaly within ``cluster_window_seconds``. Level 5 (5+ sources) is a
    genuinely extreme event and is intentionally reachable so rare synchronicities
    are never silently dropped.
    """

    def __init__(
        self,
        cluster_window_seconds: float = 30.0,
        precursor_window_seconds: float = 3600.0,  # reserved (precursor analysis is a separate future module)
    ):
        """Initialize cluster detector.

        Args:
            cluster_window_seconds: Time window within which anomalies from
                different sources are considered synchronous.
            precursor_window_seconds: Reserved for a future precursor analyzer;
                accepted for backward compatibility, currently unused here.
        """
        self.cluster_window = cluster_window_seconds
        self.precursor_window = precursor_window_seconds

        self._recent_anomalies: deque = deque(maxlen=1000)

    def add_anomaly(self, anomaly: AnomalyEvent, now: float | None = None) -> AnomalyCluster | None:
        """Add an anomaly and check whether it forms a cluster.

        Args:
            anomaly: the anomaly event to ingest.
            now: optional override for the current clock (defaults to time.time()).
                Pass the event time for offline replay so the cluster window is
                applied against historical timestamps, not wall-clock now.

        Returns:
            The detected AnomalyCluster (level 1-5), or None if no anomaly is
            in the current window.
        """
        current_time = time.time() if now is None else now

        # A delayed or backfilled observation is still a valid individual
        # anomaly, but must never create a live coincidence with current data.
        if not 0 <= current_time - anomaly.timestamp < self.cluster_window:
            return None

        # Store anomaly
        self._recent_anomalies.append({
            "anomaly": anomaly,
            "timestamp": anomaly.timestamp,
        })

        # Drop anomalies older than twice the window (keep a small margin)
        cutoff = current_time - self.cluster_window * 2
        self._recent_anomalies = deque(
            [a for a in self._recent_anomalies if a["timestamp"] > cutoff],
            maxlen=1000,
        )

        return self._detect_cluster(anomaly, now=current_time)

    def _detect_cluster(self, new_anomaly: AnomalyEvent, now: float | None = None) -> AnomalyCluster | None:
        """Detect the cluster level from distinct sources within the window.

        Level == number of distinct sensor sources with an anomaly in the
        window (capped at 5 for labelling). Level 1 means a lone anomaly.

        ``now`` lets offline replay use event time as the clock; live calls
        pass nothing and fall back to ``time.time()``.
        """
        current_time = time.time() if now is None else now

        recent = [
            a for a in self._recent_anomalies
            if current_time - a["timestamp"] < self.cluster_window
        ]

        if not recent:
            return None

        sources = {a["anomaly"].sensor_source for a in recent}
        domains = tuple(sorted({source_domain(source) for source in sources}))
        n_sources = len(domains)

        if n_sources == 1:
            return AnomalyCluster(
                level=1,
                anomalies=[new_anomaly],
                timestamp=new_anomaly.timestamp,
                probability=1.0,  # lone anomaly, not a cluster
                description=LEVEL_DESCRIPTIONS[1],
                domains=domains, source_count=len(sources),
            )

        # Multiple distinct sources -> cluster. Level capped at 5 for labelling.
        level = min(n_sources, 5)
        anomaly_list = [a["anomaly"] for a in recent]

        return AnomalyCluster(
            level=level,
            anomalies=anomaly_list,
            timestamp=new_anomaly.timestamp,
            probability=self._rarity_indicator(n_sources),
            description=LEVEL_DESCRIPTIONS[level],
            domains=domains, source_count=len(sources),
        )

    @staticmethod
    def _rarity_indicator(n_sources: int) -> float:
        """Qualitative rarity tier based on cluster size.

        IMPORTANT: these are NOT calibrated statistical p-values. A real
        probability would require long-term calibration of per-sensor anomaly
        rates, multiple-testing correction, autocorrelation and cross-sensor
        dependence. Until that calibration exists (see backtesting/shuffle work),
        we expose only a coarse rarity indicator that shrinks as more independent
        sources participate.
        """
        if n_sources <= 2:
            return 0.10   # common
        elif n_sources == 3:
            return 0.05   # uncommon
        elif n_sources == 4:
            return 0.01   # rare
        return 0.001      # 5+ : very rare

    def get_stats(self) -> dict[str, Any]:
        """Get detector statistics."""
        return {
            "recent_anomalies": len(self._recent_anomalies),
            "cluster_window": self.cluster_window,
        }
