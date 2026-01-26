"""Advanced Statistical Analyzer for Matrix Watcher.

Implements mutual information, FFT periodicity detection, and entropy-based metrics.
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats, fft

logger = logging.getLogger(__name__)


class AdvancedAnalyzer:
    """Advanced statistical analyzer for non-linear relationships.
    
    Provides:
    - Mutual information between parameter pairs
    - FFT analysis for periodicity detection
    - Entropy-based correlation metrics
    
    Example:
        analyzer = AdvancedAnalyzer()
        mi = analyzer.mutual_information(df, "param1", "param2")
        periods = analyzer.detect_periodicity(df, "param1")
    """
    
    def __init__(
        self,
        n_bins: int = 20,
        min_period: float = 60.0,
        max_period: float = 86400.0,
        significance_threshold: float = 0.1
    ):
        """Initialize analyzer.
        
        Args:
            n_bins: Number of bins for discretization
            min_period: Minimum period to detect (seconds)
            max_period: Maximum period to detect (seconds)
            significance_threshold: Threshold for significant MI
        """
        self.n_bins = n_bins
        self.min_period = min_period
        self.max_period = max_period
        self.significance_threshold = significance_threshold
    
    def mutual_information(
        self, 
        df: pd.DataFrame, 
        param1: str, 
        param2: str
    ) -> float:
        """Calculate mutual information between two parameters.
        
        MI measures how much knowing one variable reduces uncertainty
        about the other. MI >= 0, with 0 meaning independence.
        
        Args:
            df: DataFrame with data
            param1: First parameter name
            param2: Second parameter name
            
        Returns:
            Mutual information value (>= 0)
        """
        x = df[param1].dropna().values
        y = df[param2].dropna().values
        
        # Align lengths
        min_len = min(len(x), len(y))
        x = x[:min_len]
        y = y[:min_len]
        
        if len(x) < 10:
            return 0.0
        
        # Discretize into bins
        x_bins = np.histogram_bin_edges(x, bins=self.n_bins)
        y_bins = np.histogram_bin_edges(y, bins=self.n_bins)
        
        x_discrete = np.digitize(x, x_bins[:-1])
        y_discrete = np.digitize(y, y_bins[:-1])
        
        # Calculate joint and marginal probabilities
        joint_hist, _, _ = np.histogram2d(x_discrete, y_discrete, bins=self.n_bins)
        joint_prob = joint_hist / joint_hist.sum()
        
        x_prob = joint_prob.sum(axis=1)
        y_prob = joint_prob.sum(axis=0)
        
        # Calculate MI
        mi = 0.0
        for i in range(len(x_prob)):
            for j in range(len(y_prob)):
                if joint_prob[i, j] > 0 and x_prob[i] > 0 and y_prob[j] > 0:
                    mi += joint_prob[i, j] * np.log2(
                        joint_prob[i, j] / (x_prob[i] * y_prob[j])
                    )
        
        return max(0.0, mi)  # Ensure non-negative
    
    def mutual_information_matrix(
        self, 
        df: pd.DataFrame,
        parameters: list[str] | None = None
    ) -> pd.DataFrame:
        """Calculate mutual information matrix for all pairs.
        
        Args:
            df: DataFrame with data
            parameters: Parameters to analyze (None = all numeric)
            
        Returns:
            MI matrix as DataFrame
        """
        if parameters is None:
            parameters = df.select_dtypes(include=[np.number]).columns.tolist()
        
        n = len(parameters)
        mi_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(n):
                if i == j:
                    # Self-MI is entropy
                    mi_matrix[i, j] = self._entropy(df[parameters[i]].dropna().values)
                else:
                    mi_matrix[i, j] = self.mutual_information(
                        df, parameters[i], parameters[j]
                    )
        
        return pd.DataFrame(mi_matrix, index=parameters, columns=parameters)
    
    def _entropy(self, x: np.ndarray) -> float:
        """Calculate Shannon entropy of a variable."""
        if len(x) < 2:
            return 0.0
        
        bins = np.histogram_bin_edges(x, bins=self.n_bins)
        hist, _ = np.histogram(x, bins=bins)
        prob = hist / hist.sum()
        
        entropy = 0.0
        for p in prob:
            if p > 0:
                entropy -= p * np.log2(p)
        
        return entropy
    
    def detect_periodicity(
        self, 
        df: pd.DataFrame, 
        parameter: str,
        timestamp_col: str = "timestamp"
    ) -> dict[str, Any]:
        """Detect periodic patterns using FFT.
        
        Args:
            df: DataFrame with data
            parameter: Parameter to analyze
            timestamp_col: Timestamp column name
            
        Returns:
            Periodicity analysis results
        """
        if parameter not in df.columns:
            return {"error": f"Parameter not found: {parameter}"}
        
        # Sort by timestamp and get values
        sorted_df = df.sort_values(timestamp_col)
        values = sorted_df[parameter].dropna().values
        timestamps = sorted_df[timestamp_col].dropna().values
        
        if len(values) < 64:
            return {"error": "Need at least 64 samples for FFT"}
        
        # Estimate sampling rate
        dt = np.median(np.diff(timestamps))
        if dt <= 0:
            return {"error": "Invalid timestamps"}
        
        sampling_rate = 1.0 / dt
        
        # Remove mean and apply FFT
        values_centered = values - np.mean(values)
        n = len(values_centered)
        
        fft_result = fft.fft(values_centered)
        frequencies = fft.fftfreq(n, dt)
        
        # Get power spectrum (positive frequencies only)
        positive_mask = frequencies > 0
        freqs = frequencies[positive_mask]
        power = np.abs(fft_result[positive_mask]) ** 2
        
        # Convert to periods
        periods = 1.0 / freqs
        
        # Filter to valid period range
        valid_mask = (periods >= self.min_period) & (periods <= self.max_period)
        valid_periods = periods[valid_mask]
        valid_power = power[valid_mask]
        
        if len(valid_power) == 0:
            return {
                "parameter": parameter,
                "dominant_periods": [],
                "has_periodicity": False
            }
        
        # Find peaks
        mean_power = np.mean(valid_power)
        std_power = np.std(valid_power)
        threshold = mean_power + 3 * std_power
        
        peak_mask = valid_power > threshold
        peak_periods = valid_periods[peak_mask]
        peak_powers = valid_power[peak_mask]
        
        # Sort by power
        sorted_indices = np.argsort(peak_powers)[::-1]
        
        dominant_periods = []
        for idx in sorted_indices[:5]:  # Top 5 periods
            period = peak_periods[idx]
            dominant_periods.append({
                "period_seconds": round(float(period), 2),
                "period_hours": round(float(period) / 3600, 2),
                "power": round(float(peak_powers[idx]), 2),
                "is_suspicious": period < 86400  # Less than 24 hours
            })
        
        return {
            "parameter": parameter,
            "dominant_periods": dominant_periods,
            "has_periodicity": len(dominant_periods) > 0,
            "sampling_rate_hz": round(sampling_rate, 4),
            "samples_analyzed": n
        }
    
    def detect_all_periodicities(
        self, 
        df: pd.DataFrame,
        parameters: list[str] | None = None,
        timestamp_col: str = "timestamp"
    ) -> list[dict[str, Any]]:
        """Detect periodicity in all parameters.
        
        Args:
            df: DataFrame with data
            parameters: Parameters to analyze (None = all numeric)
            timestamp_col: Timestamp column name
            
        Returns:
            List of periodicity results
        """
        if parameters is None:
            parameters = df.select_dtypes(include=[np.number]).columns.tolist()
            if timestamp_col in parameters:
                parameters.remove(timestamp_col)
        
        results = []
        for param in parameters:
            result = self.detect_periodicity(df, param, timestamp_col)
            if "error" not in result:
                results.append(result)
        
        # Sort by whether periodicity was found
        results.sort(key=lambda r: len(r.get("dominant_periods", [])), reverse=True)
        return results
    
    def generate_spectrum_plot(
        self, 
        df: pd.DataFrame, 
        parameter: str,
        output_path: str | Path,
        timestamp_col: str = "timestamp"
    ) -> None:
        """Generate frequency spectrum plot.
        
        Args:
            df: DataFrame with data
            parameter: Parameter to plot
            output_path: Path to save PNG file
            timestamp_col: Timestamp column name
        """
        try:
            import matplotlib.pyplot as plt
            
            sorted_df = df.sort_values(timestamp_col)
            values = sorted_df[parameter].dropna().values
            timestamps = sorted_df[timestamp_col].dropna().values
            
            dt = np.median(np.diff(timestamps))
            values_centered = values - np.mean(values)
            n = len(values_centered)
            
            fft_result = fft.fft(values_centered)
            frequencies = fft.fftfreq(n, dt)
            
            positive_mask = frequencies > 0
            freqs = frequencies[positive_mask]
            power = np.abs(fft_result[positive_mask]) ** 2
            periods = 1.0 / freqs
            
            # Filter to valid range
            valid_mask = (periods >= self.min_period) & (periods <= self.max_period)
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            # Time series
            ax1.plot(timestamps - timestamps[0], values, 'b-', alpha=0.7)
            ax1.set_xlabel("Time (seconds)")
            ax1.set_ylabel(parameter)
            ax1.set_title(f"Time Series: {parameter}")
            ax1.grid(True, alpha=0.3)
            
            # Power spectrum (in hours)
            periods_hours = periods[valid_mask] / 3600
            power_valid = power[valid_mask]
            
            ax2.semilogy(periods_hours, power_valid, 'r-', alpha=0.7)
            ax2.set_xlabel("Period (hours)")
            ax2.set_ylabel("Power")
            ax2.set_title(f"Power Spectrum: {parameter}")
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig(output_path, dpi=150)
            plt.close()
            
            logger.info(f"Spectrum plot saved to {output_path}")
            
        except ImportError:
            logger.warning("matplotlib not available for plotting")
    
    def analyze(
        self, 
        df: pd.DataFrame,
        timestamp_col: str = "timestamp"
    ) -> dict[str, Any]:
        """Run full advanced analysis.
        
        Args:
            df: DataFrame with data
            timestamp_col: Timestamp column name
            
        Returns:
            Analysis results dictionary
        """
        mi_matrix = self.mutual_information_matrix(df)
        periodicities = self.detect_all_periodicities(df, timestamp_col=timestamp_col)
        
        # Find significant MI pairs
        significant_mi = []
        cols = mi_matrix.columns.tolist()
        for i, col1 in enumerate(cols):
            for j, col2 in enumerate(cols):
                if i < j:
                    mi = mi_matrix.loc[col1, col2]
                    if mi >= self.significance_threshold:
                        significant_mi.append({
                            "param1": col1,
                            "param2": col2,
                            "mutual_information": round(mi, 4)
                        })
        
        significant_mi.sort(key=lambda x: x["mutual_information"], reverse=True)
        
        # Find suspicious periodicities
        suspicious = [
            p for p in periodicities 
            if any(d.get("is_suspicious") for d in p.get("dominant_periods", []))
        ]
        
        return {
            "mutual_information_matrix": mi_matrix,
            "significant_mi_pairs": significant_mi,
            "periodicities": periodicities,
            "suspicious_periodicities": suspicious,
            "parameters_analyzed": len(mi_matrix.columns),
            "significant_mi_count": len(significant_mi),
            "periodic_parameters": len([p for p in periodicities if p["has_periodicity"]])
        }
