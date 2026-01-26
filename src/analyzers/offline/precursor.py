"""Precursor Analyzer for Matrix Watcher.

Identifies patterns that precede anomalies.
"""

import logging
from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PrecursorAnalyzer:
    """Analyzer for finding precursor patterns before anomalies.
    
    Extracts data windows at specified intervals before anomalies
    and identifies recurring patterns that may predict anomalies.
    
    Example:
        analyzer = PrecursorAnalyzer(windows=[5, 10, 30])
        patterns = analyzer.find_precursors(data_df, anomalies_df)
    """
    
    def __init__(
        self,
        windows: list[int] | None = None,
        min_frequency: float = 0.3,
        z_threshold: float = 2.0
    ):
        """Initialize analyzer.
        
        Args:
            windows: Time windows before anomaly in seconds (default: [5, 10, 30])
            min_frequency: Minimum frequency to flag as precursor (0-1)
            z_threshold: Z-score threshold for pattern detection
        """
        self.windows = windows or [5, 10, 30]
        self.min_frequency = min_frequency
        self.z_threshold = z_threshold
    
    def extract_pre_anomaly_windows(
        self,
        data: pd.DataFrame,
        anomalies: pd.DataFrame,
        timestamp_col: str = "timestamp"
    ) -> dict[int, list[pd.DataFrame]]:
        """Extract data windows before each anomaly.
        
        Args:
            data: Full data DataFrame
            anomalies: Anomaly records DataFrame
            timestamp_col: Timestamp column name
            
        Returns:
            Dictionary mapping window size to list of window DataFrames
        """
        windows_data = {w: [] for w in self.windows}
        
        data_sorted = data.sort_values(timestamp_col)
        
        for _, anomaly in anomalies.iterrows():
            anomaly_time = anomaly[timestamp_col]
            
            for window_size in self.windows:
                window_start = anomaly_time - window_size
                window_end = anomaly_time
                
                # Extract window
                mask = (
                    (data_sorted[timestamp_col] >= window_start) &
                    (data_sorted[timestamp_col] < window_end)
                )
                window_df = data_sorted[mask].copy()
                
                if not window_df.empty:
                    window_df["anomaly_time"] = anomaly_time
                    window_df["window_size"] = window_size
                    windows_data[window_size].append(window_df)
        
        return windows_data
    
    def analyze_window_patterns(
        self,
        windows: list[pd.DataFrame],
        parameters: list[str] | None = None
    ) -> dict[str, Any]:
        """Analyze patterns in pre-anomaly windows.
        
        Args:
            windows: List of window DataFrames
            parameters: Parameters to analyze (None = all numeric)
            
        Returns:
            Pattern analysis results
        """
        if not windows:
            return {"patterns": [], "total_windows": 0}
        
        # Combine all windows
        combined = pd.concat(windows, ignore_index=True)
        
        if parameters is None:
            parameters = combined.select_dtypes(include=[np.number]).columns.tolist()
            # Remove metadata columns
            for col in ["timestamp", "anomaly_time", "window_size"]:
                if col in parameters:
                    parameters.remove(col)
        
        patterns = []
        
        for param in parameters:
            if param not in combined.columns:
                continue
            
            values = combined[param].dropna()
            if len(values) < 10:
                continue
            
            mean = values.mean()
            std = values.std()
            
            # Check for elevated/depressed values
            if std > 0:
                # Calculate what fraction of windows show elevated values
                elevated_count = 0
                depressed_count = 0
                
                for window_df in windows:
                    if param in window_df.columns:
                        window_mean = window_df[param].mean()
                        z = (window_mean - mean) / std if std > 0 else 0
                        
                        if z > self.z_threshold:
                            elevated_count += 1
                        elif z < -self.z_threshold:
                            depressed_count += 1
                
                total = len(windows)
                elevated_freq = elevated_count / total if total > 0 else 0
                depressed_freq = depressed_count / total if total > 0 else 0
                
                if elevated_freq >= self.min_frequency:
                    patterns.append({
                        "parameter": param,
                        "pattern_type": "elevated",
                        "frequency": round(elevated_freq, 3),
                        "count": elevated_count,
                        "total_windows": total,
                        "mean": round(mean, 4),
                        "std": round(std, 4),
                        "is_precursor": True
                    })
                
                if depressed_freq >= self.min_frequency:
                    patterns.append({
                        "parameter": param,
                        "pattern_type": "depressed",
                        "frequency": round(depressed_freq, 3),
                        "count": depressed_count,
                        "total_windows": total,
                        "mean": round(mean, 4),
                        "std": round(std, 4),
                        "is_precursor": True
                    })
        
        # Sort by frequency
        patterns.sort(key=lambda p: p["frequency"], reverse=True)
        
        return {
            "patterns": patterns,
            "total_windows": len(windows),
            "parameters_analyzed": len(parameters)
        }
    
    def find_precursors(
        self,
        data: pd.DataFrame,
        anomalies: pd.DataFrame,
        timestamp_col: str = "timestamp"
    ) -> dict[str, Any]:
        """Find precursor patterns for all window sizes.
        
        Args:
            data: Full data DataFrame
            anomalies: Anomaly records DataFrame
            timestamp_col: Timestamp column name
            
        Returns:
            Precursor analysis results
        """
        windows_data = self.extract_pre_anomaly_windows(
            data, anomalies, timestamp_col
        )
        
        results_by_window = {}
        all_precursors = []
        
        for window_size, windows in windows_data.items():
            analysis = self.analyze_window_patterns(windows)
            results_by_window[window_size] = analysis
            
            # Add window size to precursors
            for pattern in analysis["patterns"]:
                pattern["window_seconds"] = window_size
                all_precursors.append(pattern)
        
        # Sort all precursors by frequency
        all_precursors.sort(key=lambda p: p["frequency"], reverse=True)
        
        return {
            "by_window": results_by_window,
            "all_precursors": all_precursors,
            "precursor_count": len(all_precursors),
            "windows_analyzed": self.windows,
            "min_frequency_threshold": self.min_frequency
        }
    
    def generate_report(
        self, 
        results: dict[str, Any]
    ) -> str:
        """Generate text report of precursor analysis.
        
        Args:
            results: Analysis results from find_precursors
            
        Returns:
            Formatted report string
        """
        lines = ["=" * 60]
        lines.append("PRECURSOR ANALYSIS REPORT")
        lines.append("=" * 60)
        lines.append("")
        
        precursors = results.get("all_precursors", [])
        
        if not precursors:
            lines.append("No significant precursor patterns found.")
        else:
            lines.append(f"Found {len(precursors)} precursor patterns:")
            lines.append("")
            
            for i, p in enumerate(precursors[:20], 1):  # Top 20
                lines.append(f"{i}. {p['parameter']} ({p['pattern_type']})")
                lines.append(f"   Window: {p['window_seconds']}s before anomaly")
                lines.append(f"   Frequency: {p['frequency']*100:.1f}% of anomalies")
                lines.append(f"   Count: {p['count']}/{p['total_windows']} windows")
                lines.append("")
        
        lines.append("=" * 60)
        return "\n".join(lines)
    
    def analyze(
        self,
        data: pd.DataFrame,
        anomalies: pd.DataFrame,
        timestamp_col: str = "timestamp"
    ) -> dict[str, Any]:
        """Run full precursor analysis.
        
        Args:
            data: Full data DataFrame
            anomalies: Anomaly records DataFrame
            timestamp_col: Timestamp column name
            
        Returns:
            Complete analysis results
        """
        results = self.find_precursors(data, anomalies, timestamp_col)
        results["report"] = self.generate_report(results)
        return results
