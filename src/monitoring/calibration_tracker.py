"""Calibration Tracker - Tracks threshold hits for future calibration.

Logs all threshold checks to enable data-driven calibration.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class CalibrationTracker:
    """Tracks threshold hits for calibration purposes."""
    
    def __init__(self, log_dir: str = "logs/calibration"):
        """Initialize calibration tracker.
        
        Args:
            log_dir: Directory for calibration logs
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.threshold_hits_file = self.log_dir / "threshold_hits.jsonl"
        self.value_distributions_file = self.log_dir / "value_distributions.jsonl"
        self.metadata_file = self.log_dir / "tracker_metadata.json"
        
        # Load or initialize start time
        self._start_time = self._load_start_time()
        self._hit_count = 0
        self._value_count = 0
    
    def _load_start_time(self) -> float:
        """Load start time from disk or initialize new one."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r") as f:
                    metadata = json.load(f)
                    start_time = metadata.get("start_time")
                    if start_time:
                        logger.info(f"Loaded calibration start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
                        return start_time
            except Exception as e:
                logger.warning(f"Failed to load start time: {e}")
        
        # Initialize new start time
        start_time = time.time()
        self._save_start_time(start_time)
        logger.info(f"Initialized new calibration start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        return start_time
    
    def _save_start_time(self, start_time: float) -> None:
        """Save start time to disk."""
        try:
            metadata = {"start_time": start_time}
            with open(self.metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save start time: {e}")
    
    def log_threshold_check(
        self,
        threshold_name: str,
        value: float,
        threshold_value: float,
        triggered: bool,
        metadata: dict[str, Any] | None = None
    ) -> None:
        """Log a threshold check.
        
        Args:
            threshold_name: Name of threshold (e.g., "quantum_rng.randomness_score")
            value: Actual value
            threshold_value: Threshold value
            triggered: Whether threshold was triggered
            metadata: Additional context
        """
        record = {
            "timestamp": time.time(),
            "threshold_name": threshold_name,
            "value": value,
            "threshold_value": threshold_value,
            "triggered": triggered,
            "metadata": metadata or {}
        }
        
        try:
            with open(self.threshold_hits_file, "a") as f:
                f.write(json.dumps(record) + "\n")
            
            if triggered:
                self._hit_count += 1
        except Exception as e:
            logger.error(f"Failed to log threshold check: {e}")
    
    def log_value_distribution(
        self,
        parameter_name: str,
        value: float,
        metadata: dict[str, Any] | None = None
    ) -> None:
        """Log a parameter value for distribution analysis.
        
        Args:
            parameter_name: Name of parameter (e.g., "quantum_rng.randomness_score")
            value: Parameter value
            metadata: Additional context
        """
        record = {
            "timestamp": time.time(),
            "parameter_name": parameter_name,
            "value": value,
            "metadata": metadata or {}
        }
        
        try:
            with open(self.value_distributions_file, "a") as f:
                f.write(json.dumps(record) + "\n")
            
            self._value_count += 1
        except Exception as e:
            logger.error(f"Failed to log value distribution: {e}")
    
    def get_stats(self) -> dict[str, Any]:
        """Get calibration tracking statistics.
        
        Returns:
            Statistics about data collection
        """
        days_running = (time.time() - self._start_time) / 86400
        
        return {
            "days_collecting": days_running,
            "threshold_hits_logged": self._hit_count,
            "values_logged": self._value_count,
            "ready_for_calibration": days_running >= 30,  # 30 days minimum
            "log_files": {
                "threshold_hits": str(self.threshold_hits_file),
                "value_distributions": str(self.value_distributions_file)
            }
        }
    
    def analyze_threshold(self, threshold_name: str) -> dict[str, Any]:
        """Analyze a specific threshold from logged data.
        
        Args:
            threshold_name: Name of threshold to analyze
            
        Returns:
            Analysis results with recommendations
        """
        hits = []
        
        try:
            with open(self.threshold_hits_file, "r") as f:
                for line in f:
                    record = json.loads(line)
                    if record["threshold_name"] == threshold_name:
                        hits.append(record)
        except FileNotFoundError:
            return {"error": "No data collected yet"}
        
        if not hits:
            return {"error": f"No data for threshold {threshold_name}"}
        
        # Calculate statistics
        total_checks = len(hits)
        triggered_count = sum(1 for h in hits if h["triggered"])
        trigger_rate = triggered_count / total_checks if total_checks > 0 else 0
        
        values = [h["value"] for h in hits]
        values.sort()
        
        # Percentiles
        def percentile(data, p):
            k = (len(data) - 1) * p
            f = int(k)
            c = k - f
            if f + 1 < len(data):
                return data[f] + c * (data[f + 1] - data[f])
            return data[f]
        
        analysis = {
            "threshold_name": threshold_name,
            "total_checks": total_checks,
            "triggered_count": triggered_count,
            "trigger_rate": trigger_rate,
            "value_stats": {
                "min": min(values),
                "max": max(values),
                "p50": percentile(values, 0.50),
                "p90": percentile(values, 0.90),
                "p95": percentile(values, 0.95),
                "p99": percentile(values, 0.99)
            },
            "current_threshold": hits[0]["threshold_value"],
            "recommendation": self._generate_recommendation(trigger_rate, values, hits[0]["threshold_value"])
        }
        
        return analysis
    
    def _generate_recommendation(
        self,
        trigger_rate: float,
        values: list[float],
        current_threshold: float
    ) -> str:
        """Generate calibration recommendation.
        
        Args:
            trigger_rate: Current trigger rate
            values: All observed values
            current_threshold: Current threshold value
            
        Returns:
            Recommendation text
        """
        if trigger_rate > 0.1:  # > 10% trigger rate
            return f"⚠️ High trigger rate ({trigger_rate:.1%}). Consider raising threshold."
        elif trigger_rate < 0.01:  # < 1% trigger rate
            return f"⚠️ Low trigger rate ({trigger_rate:.1%}). Consider lowering threshold or it's working well."
        else:
            return f"✅ Trigger rate ({trigger_rate:.1%}) seems reasonable."


# Global instance
_tracker: CalibrationTracker | None = None


def get_tracker() -> CalibrationTracker:
    """Get global calibration tracker instance."""
    global _tracker
    if _tracker is None:
        _tracker = CalibrationTracker()
    return _tracker
