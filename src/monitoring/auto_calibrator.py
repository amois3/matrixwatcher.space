"""Auto-Calibrator - Automatic threshold calibration system.

Automatically calibrates thresholds based on collected data when ready.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

import numpy as np

from .calibration_tracker import get_tracker

logger = logging.getLogger(__name__)


class AutoCalibrator:
    """Automatically calibrates thresholds when enough data is collected.
    
    Features:
    - Monitors data collection progress
    - Automatically triggers calibration when ready
    - Calculates optimal thresholds from data
    - Validates new thresholds
    - Generates calibration reports
    - Can auto-apply or suggest changes
    """
    
    def __init__(
        self,
        min_days: int = 30,
        min_observations: int = 1000,
        target_trigger_rate: float = 0.02,  # 2% target
        auto_apply: bool = False,
        calibration_dir: str = "logs/calibration"
    ):
        """Initialize auto-calibrator.
        
        Args:
            min_days: Minimum days of data before calibration
            min_observations: Minimum observations per threshold
            target_trigger_rate: Target trigger rate (2% = good balance)
            auto_apply: Whether to auto-apply calibrations (False = suggest only)
            calibration_dir: Directory for calibration data
        """
        self.min_days = min_days
        self.min_observations = min_observations
        self.target_trigger_rate = target_trigger_rate
        self.auto_apply = auto_apply
        self.calibration_dir = Path(calibration_dir)
        
        self._tracker = get_tracker()
        self._last_calibration_check = 0
        self._calibration_history: list[dict] = []
        
        # Load calibration history
        self._load_history()
    
    def _load_history(self) -> None:
        """Load calibration history from disk."""
        history_file = self.calibration_dir / "calibration_history.json"
        if history_file.exists():
            try:
                with open(history_file, "r") as f:
                    self._calibration_history = json.load(f)
                logger.info(f"Loaded {len(self._calibration_history)} calibration records")
            except Exception as e:
                logger.error(f"Failed to load calibration history: {e}")
    
    def _save_history(self) -> None:
        """Save calibration history to disk."""
        history_file = self.calibration_dir / "calibration_history.json"
        try:
            with open(history_file, "w") as f:
                json.dump(self._calibration_history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save calibration history: {e}")
    
    def check_and_calibrate(self) -> dict[str, Any]:
        """Check if calibration is needed and perform it.
        
        Returns:
            Calibration results with recommendations
        """
        # Don't check too often (once per day)
        now = time.time()
        if now - self._last_calibration_check < 86400:
            return {"status": "skipped", "reason": "checked recently"}
        
        self._last_calibration_check = now
        
        # Check if we calibrated recently (wait 30 days between calibrations)
        if self._calibration_history:
            last_calibration_time = self._calibration_history[-1]["timestamp"]
            days_since_last = (now - last_calibration_time) / 86400
            
            if days_since_last < self.min_days:
                days_left = self.min_days - days_since_last
                return {
                    "status": "waiting",
                    "days_since_last_calibration": days_since_last,
                    "days_until_next": days_left,
                    "message": f"Next calibration in {days_left:.1f} days"
                }
        
        # Check if ready (first calibration)
        stats = self._tracker.get_stats()
        
        if not stats["ready_for_calibration"]:
            days_left = self.min_days - stats["days_collecting"]
            return {
                "status": "not_ready",
                "days_collecting": stats["days_collecting"],
                "days_needed": self.min_days,
                "days_left": days_left,
                "message": f"Need {days_left:.1f} more days of data"
            }
        
        # Ready! Perform calibration
        logger.info("🔬 Starting automatic calibration...")
        
        results = self._perform_calibration()
        
        # Save to history
        self._calibration_history.append({
            "timestamp": now,
            "days_of_data": stats["days_collecting"],
            "results": results
        })
        self._save_history()
        
        return results
    
    def _perform_calibration(self) -> dict[str, Any]:
        """Perform actual calibration on all thresholds.
        
        Returns:
            Calibration results
        """
        # Get list of thresholds to calibrate
        thresholds_to_calibrate = self._get_thresholds_to_calibrate()
        
        results = {
            "status": "completed",
            "timestamp": time.time(),
            "thresholds_analyzed": len(thresholds_to_calibrate),
            "recommendations": [],
            "auto_applied": []
        }
        
        for threshold_name in thresholds_to_calibrate:
            try:
                recommendation = self._calibrate_threshold(threshold_name)
                
                if recommendation:
                    results["recommendations"].append(recommendation)
                    
                    # Auto-apply if enabled and confidence is high
                    if self.auto_apply and recommendation.get("confidence") == "high":
                        self._apply_calibration(recommendation)
                        results["auto_applied"].append(threshold_name)
                        logger.info(f"✅ Auto-applied calibration for {threshold_name}")
            
            except Exception as e:
                logger.error(f"Failed to calibrate {threshold_name}: {e}")
        
        # Generate summary
        results["summary"] = self._generate_summary(results)
        
        # Save detailed report
        self._save_calibration_report(results)
        
        return results
    
    def _get_thresholds_to_calibrate(self) -> list[str]:
        """Get list of thresholds that need calibration.
        
        Returns:
            List of threshold names
        """
        # Priority list (critical thresholds first)
        priority_thresholds = [
            "quantum_rng.randomness_score.min",
            "earthquake.max_magnitude.trigger_above",
            "crypto.BTCUSDT.price.change_pct",
            "crypto.ETHUSDT.price.change_pct",
        ]
        
        # Get all thresholds from logs
        all_thresholds = set()
        hits_file = self.calibration_dir / "threshold_hits.jsonl"
        
        if hits_file.exists():
            try:
                with open(hits_file, "r") as f:
                    for line in f:
                        record = json.loads(line)
                        all_thresholds.add(record["threshold_name"])
            except Exception as e:
                logger.error(f"Failed to read threshold hits: {e}")
        
        # Priority first, then others
        result = []
        for t in priority_thresholds:
            if t in all_thresholds:
                result.append(t)
        
        for t in sorted(all_thresholds):
            if t not in result:
                result.append(t)
        
        return result
    
    def _calibrate_threshold(self, threshold_name: str) -> dict[str, Any] | None:
        """Calibrate a specific threshold.
        
        Args:
            threshold_name: Name of threshold to calibrate
            
        Returns:
            Calibration recommendation or None
        """
        # Get analysis from tracker
        analysis = self._tracker.analyze_threshold(threshold_name)
        
        if "error" in analysis:
            return None
        
        # Check if enough observations
        if analysis["total_checks"] < self.min_observations:
            return None
        
        # Calculate optimal threshold
        optimal = self._calculate_optimal_threshold(analysis)
        
        if optimal is None:
            return None
        
        # Determine confidence
        confidence = self._determine_confidence(analysis)
        
        # Generate recommendation
        recommendation = {
            "threshold_name": threshold_name,
            "current_value": analysis["current_threshold"],
            "recommended_value": optimal["value"],
            "change_percent": optimal["change_percent"],
            "confidence": confidence,
            "reason": optimal["reason"],
            "stats": {
                "total_checks": analysis["total_checks"],
                "current_trigger_rate": analysis["trigger_rate"],
                "expected_trigger_rate": optimal["expected_trigger_rate"],
                "percentiles": analysis["value_stats"]
            }
        }
        
        return recommendation
    
    def _calculate_optimal_threshold(self, analysis: dict) -> dict[str, Any] | None:
        """Calculate optimal threshold value.
        
        Args:
            analysis: Threshold analysis from tracker
            
        Returns:
            Optimal threshold info or None
        """
        current_threshold = analysis["current_threshold"]
        trigger_rate = analysis["trigger_rate"]
        stats = analysis["value_stats"]
        
        # Strategy depends on threshold type
        threshold_name = analysis["threshold_name"]
        
        # For "min" thresholds (trigger when value < threshold)
        if ".min" in threshold_name:
            # Want trigger_rate close to target (e.g., 2%)
            if trigger_rate > self.target_trigger_rate * 2:
                # Too many triggers, lower threshold
                # Use P95 or P90 depending on how far off we are
                if trigger_rate > self.target_trigger_rate * 5:
                    new_value = stats["p90"]
                    reason = f"Trigger rate too high ({trigger_rate:.1%}), using P90"
                else:
                    new_value = stats["p95"]
                    reason = f"Trigger rate high ({trigger_rate:.1%}), using P95"
            
            elif trigger_rate < self.target_trigger_rate * 0.5:
                # Too few triggers, raise threshold
                new_value = stats["p99"]
                reason = f"Trigger rate too low ({trigger_rate:.1%}), using P99"
            
            else:
                # Good as is
                return None
        
        # For "max" thresholds (trigger when value > threshold)
        elif ".max" in threshold_name:
            if trigger_rate > self.target_trigger_rate * 2:
                # Too many triggers, raise threshold
                if trigger_rate > self.target_trigger_rate * 5:
                    new_value = stats["p90"]
                    reason = f"Trigger rate too high ({trigger_rate:.1%}), using P90"
                else:
                    new_value = stats["p95"]
                    reason = f"Trigger rate high ({trigger_rate:.1%}), using P95"
            
            elif trigger_rate < self.target_trigger_rate * 0.5:
                # Too few triggers, lower threshold
                new_value = stats["p99"]
                reason = f"Trigger rate too low ({trigger_rate:.1%}), using P99"
            
            else:
                return None
        
        # For "trigger_above" thresholds
        elif ".trigger_above" in threshold_name:
            # Similar to max
            if trigger_rate > self.target_trigger_rate * 2:
                new_value = stats["p95"]
                reason = f"Trigger rate too high ({trigger_rate:.1%}), using P95"
            elif trigger_rate < self.target_trigger_rate * 0.5:
                new_value = stats["p90"]
                reason = f"Trigger rate too low ({trigger_rate:.1%}), using P90"
            else:
                return None
        
        # For "change_pct" thresholds
        elif ".change_pct" in threshold_name:
            # Use P95 for good balance
            if trigger_rate > self.target_trigger_rate * 2:
                new_value = stats["p95"]
                reason = f"Trigger rate too high ({trigger_rate:.1%}), using P95"
            elif trigger_rate < self.target_trigger_rate * 0.5:
                new_value = stats["p90"]
                reason = f"Trigger rate too low ({trigger_rate:.1%}), using P90"
            else:
                return None
        
        else:
            # Unknown type, use P95 as safe default
            new_value = stats["p95"]
            reason = "Using P95 as safe default"
        
        # Calculate change
        change_percent = ((new_value - current_threshold) / current_threshold * 100) if current_threshold != 0 else 0
        
        # Estimate new trigger rate
        # Simple approximation: if we move from current to P95, trigger rate becomes ~5%
        if ".min" in threshold_name or ".trigger_above" in threshold_name:
            if new_value == stats["p95"]:
                expected_trigger_rate = 0.05
            elif new_value == stats["p90"]:
                expected_trigger_rate = 0.10
            elif new_value == stats["p99"]:
                expected_trigger_rate = 0.01
            else:
                expected_trigger_rate = self.target_trigger_rate
        else:
            expected_trigger_rate = self.target_trigger_rate
        
        return {
            "value": new_value,
            "change_percent": change_percent,
            "reason": reason,
            "expected_trigger_rate": expected_trigger_rate
        }
    
    def _determine_confidence(self, analysis: dict) -> str:
        """Determine confidence level for calibration.
        
        Args:
            analysis: Threshold analysis
            
        Returns:
            Confidence level: "high", "medium", "low"
        """
        total_checks = analysis["total_checks"]
        trigger_rate = analysis["trigger_rate"]
        
        # High confidence: lots of data, clear signal
        if total_checks >= 5000 and (trigger_rate > 0.05 or trigger_rate < 0.005):
            return "high"
        
        # Medium confidence: decent data
        if total_checks >= 2000:
            return "medium"
        
        # Low confidence: not enough data
        return "low"
    
    def _apply_calibration(self, recommendation: dict) -> None:
        """Apply calibration recommendation.
        
        Args:
            recommendation: Calibration recommendation
        """
        threshold_name = recommendation['threshold_name']
        new_value = recommendation['recommended_value']
        
        logger.info(f"Applying calibration: {threshold_name} = {new_value}")
        
        # Save to calibrated_thresholds.json for persistence
        calibrated_file = self.calibration_dir / "calibrated_thresholds.json"
        
        try:
            # Load existing calibrated thresholds
            if calibrated_file.exists():
                with open(calibrated_file, "r") as f:
                    calibrated = json.load(f)
            else:
                calibrated = {}
            
            # Update threshold
            calibrated[threshold_name] = {
                "value": new_value,
                "applied_at": time.time(),
                "previous_value": recommendation.get("current_value"),
                "confidence": recommendation.get("confidence")
            }
            
            # Save
            with open(calibrated_file, "w") as f:
                json.dump(calibrated, f, indent=2)
            
            logger.info(f"✅ Saved calibrated threshold: {threshold_name} = {new_value}")
            
        except Exception as e:
            logger.error(f"Failed to save calibrated threshold: {e}")
    
    def _generate_summary(self, results: dict) -> str:
        """Generate human-readable summary.
        
        Args:
            results: Calibration results
            
        Returns:
            Summary text
        """
        total = results["thresholds_analyzed"]
        recommendations = len(results["recommendations"])
        auto_applied = len(results["auto_applied"])
        
        summary = f"Проанализировано {total} порогов. "
        
        if recommendations == 0:
            summary += "Все пороги оптимальны, изменений не требуется."
        else:
            summary += f"Рекомендовано изменить {recommendations} порогов. "
            
            if auto_applied > 0:
                summary += f"Автоматически применено {auto_applied} изменений."
            else:
                summary += "Изменения требуют ручного подтверждения."
        
        return summary
    
    def _save_calibration_report(self, results: dict) -> None:
        """Save detailed calibration report.
        
        Args:
            results: Calibration results
        """
        report_file = self.calibration_dir / f"calibration_report_{int(time.time())}.json"
        
        try:
            with open(report_file, "w") as f:
                json.dump(results, f, indent=2)
            logger.info(f"Calibration report saved to {report_file}")
        except Exception as e:
            logger.error(f"Failed to save calibration report: {e}")
    
    def get_calibration_status(self) -> dict[str, Any]:
        """Get current calibration status.
        
        Returns:
            Status information
        """
        stats = self._tracker.get_stats()
        
        return {
            "ready_for_calibration": stats["ready_for_calibration"],
            "days_collecting": stats["days_collecting"],
            "days_needed": self.min_days,
            "auto_apply_enabled": self.auto_apply,
            "last_calibration": self._calibration_history[-1] if self._calibration_history else None,
            "total_calibrations": len(self._calibration_history)
        }


# Global instance
_auto_calibrator: AutoCalibrator | None = None


def get_auto_calibrator(auto_apply: bool = False) -> AutoCalibrator:
    """Get global auto-calibrator instance.
    
    Args:
        auto_apply: Whether to auto-apply calibrations
        
    Returns:
        AutoCalibrator instance
    """
    global _auto_calibrator
    if _auto_calibrator is None:
        _auto_calibrator = AutoCalibrator(auto_apply=auto_apply)
    return _auto_calibrator
