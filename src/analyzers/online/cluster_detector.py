"""Cluster Detector - Detects synchronization of unrelated systems.

Implements the 5-level anomaly system:
Level 1: Single anomaly
Level 2: Two systems correlated
Level 3: Three systems (cluster)
Level 4: Four+ systems (mega-cluster)
Level 5: Precursor detected (anomaly before event)
"""

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from ...core.types import AnomalyEvent

logger = logging.getLogger(__name__)


@dataclass
class AnomalyCluster:
    """Represents a cluster of anomalies."""
    level: int  # 1-5
    anomalies: list[AnomalyEvent]
    timestamp: float
    probability: float  # Probability of being random
    description: str
    is_precursor: bool = False
    precursor_event: Any = None


class ClusterDetector:
    """Detects clusters and correlations between anomalies."""
    
    def __init__(
        self,
        cluster_window_seconds: float = 30.0,
        precursor_window_seconds: float = 3600.0
    ):
        """Initialize cluster detector.
        
        Args:
            cluster_window_seconds: Time window for clustering
            precursor_window_seconds: Time window for precursor detection
        """
        self.cluster_window = cluster_window_seconds
        self.precursor_window = precursor_window_seconds
        
        self._recent_anomalies: deque = deque(maxlen=1000)
        self._precursor_candidates: deque = deque(maxlen=100)
    
    def add_anomaly(self, anomaly: AnomalyEvent) -> AnomalyCluster | None:
        """Add anomaly and check for clusters.
        
        Returns:
            AnomalyCluster if cluster detected, None otherwise
        """
        current_time = time.time()
        
        # Store anomaly
        self._recent_anomalies.append({
            "anomaly": anomaly,
            "timestamp": anomaly.timestamp
        })
        
        # Clean old anomalies
        cutoff = current_time - self.cluster_window * 2
        self._recent_anomalies = deque(
            [a for a in self._recent_anomalies if a["timestamp"] > cutoff],
            maxlen=1000
        )
        
        # Check for cluster
        cluster = self._detect_cluster(anomaly)
        
        # Check for precursor
        if not cluster or cluster.level < 3:
            precursor = self._check_precursor(anomaly)
            if precursor:
                return precursor
        
        return cluster
    
    def _detect_cluster(self, new_anomaly: AnomalyEvent) -> AnomalyCluster | None:
        """Detect if new anomaly forms a cluster.
        
        STRICT RULES FOR LEVELS:
        - Level 1: Single anomaly (always)
        - Level 2: 2 sources within window (temporal correlation)
        - Level 3: 3 sources within window (multi-system event)
        - Level 4: 4+ sources within window (rare, significant)
        - Level 5: RESERVED (disabled, requires extreme conditions)
        """
        current_time = time.time()
        
        # Find recent anomalies within window
        recent = [
            a for a in self._recent_anomalies
            if current_time - a["timestamp"] < self.cluster_window
        ]
        
        if len(recent) < 1:
            return None
        
        # Get unique sources
        sources = set(a["anomaly"].sensor_source for a in recent)
        
        # Determine level STRICTLY by source count
        level = len(sources)
        
        if level == 1:
            # Single anomaly
            return AnomalyCluster(
                level=1,
                anomalies=[new_anomaly],
                timestamp=new_anomaly.timestamp,
                probability=1.0,  # Not a cluster
                description="Одиночная аномалия"
            )
        
        # Multiple sources - calculate probability
        anomaly_list = [a["anomaly"] for a in recent]
        probability = self._calculate_cluster_probability(anomaly_list)
        
        # STRICT: Level must match source count exactly
        if level == 2:
            return AnomalyCluster(
                level=2,
                anomalies=anomaly_list,
                timestamp=new_anomaly.timestamp,
                probability=probability,
                description="Двойная корреляция"
            )
        elif level == 3:
            return AnomalyCluster(
                level=3,
                anomalies=anomaly_list,
                timestamp=new_anomaly.timestamp,
                probability=probability,
                description="Тройной кластер"
            )
        elif level == 4:
            return AnomalyCluster(
                level=4,
                anomalies=anomaly_list,
                timestamp=new_anomaly.timestamp,
                probability=probability,
                description="Системное возмущение"
            )
        elif level >= 5:
            # Level 5: EXTREME - 5+ independent sources in 30s window
            # This is statistically very rare (< 0.1% probability)
            return AnomalyCluster(
                level=5,
                anomalies=anomaly_list,
                timestamp=new_anomaly.timestamp,
                probability=probability,
                description="Критическая синхронность"
            )
        
        return None
    
    def _calculate_cluster_probability(self, anomalies: list[AnomalyEvent]) -> float:
        """Calculate probability that cluster is random.
        
        HONEST APPROACH: We don't have enough data to calculate accurate probabilities.
        This would require:
        - Long-term calibration of anomaly rates per sensor
        - Accounting for multiple testing (continuous monitoring)
        - Accounting for autocorrelation
        - Accounting for sensor dependencies
        
        Instead, return a simple indicator based on cluster size:
        - 2 sources: common (happens regularly)
        - 3 sources: uncommon (happens occasionally)
        - 4+ sources: rare (happens infrequently)
        - 5+ sources: very rare (almost never)
        
        These are qualitative, not quantitative probabilities.
        """
        n = len(set(a.sensor_source for a in anomalies))
        
        # Return qualitative probability indicators (NOT statistical p-values!)
        if n == 2:
            return 0.10  # 10% - common, happens regularly
        elif n == 3:
            return 0.05  # 5% - uncommon
        elif n == 4:
            return 0.01  # 1% - rare
        else:  # 5+
            return 0.001  # 0.1% - very rare
        
        # NOTE: These are NOT calibrated statistical probabilities!
        # They are rough indicators of rarity based on cluster size.
    
    def _check_precursor(self, anomaly: AnomalyEvent) -> AnomalyCluster | None:
        """Check if this anomaly is a precursor to a later event.
        
        DISABLED: Precursor detection requires much more data and validation.
        Level 5 should be reserved for truly exceptional events.
        """
        # Store as potential precursor for future analysis
        self._precursor_candidates.append({
            "anomaly": anomaly,
            "timestamp": anomaly.timestamp
        })
        
        # Clean old candidates
        cutoff = time.time() - self.precursor_window
        self._precursor_candidates = deque(
            [p for p in self._precursor_candidates if p["timestamp"] > cutoff],
            maxlen=100
        )
        
        # DISABLED: Do not auto-assign Level 5
        # Precursor detection needs:
        # 1. Statistical validation (not just time correlation)
        # 2. Historical pattern confirmation
        # 3. Minimum anomaly strength threshold
        # 4. Multiple occurrences of the same pattern
        
        return None
    
    def _calculate_precursor_probability(self, time_diff_seconds: float) -> float:
        """Calculate probability that precursor is random."""
        # Longer time difference = more likely to be random
        # Use exponential decay
        import math
        p = math.exp(-time_diff_seconds / 1800.0)  # Half-life of 30 minutes
        return min(1.0, p * 10)  # Scale up for readability
    
    def get_stats(self) -> dict[str, Any]:
        """Get detector statistics."""
        return {
            "recent_anomalies": len(self._recent_anomalies),
            "precursor_candidates": len(self._precursor_candidates),
            "cluster_window": self.cluster_window,
            "precursor_window": self.precursor_window
        }
