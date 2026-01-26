"""Lag-Correlation Analyzer for Matrix Watcher.

Analyzes time-shifted correlations to find causal relationships.
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class LagCorrelationAnalyzer:
    """Analyzer for computing lag-correlations between parameters.
    
    Tests time shifts from -max_lag to +max_lag seconds to find
    optimal lag for maximum correlation between parameter pairs.
    
    Example:
        analyzer = LagCorrelationAnalyzer(max_lag=60)
        results = analyzer.analyze_pair(df, "param1", "param2")
        print(f"Optimal lag: {results['optimal_lag']}s")
    """
    
    def __init__(
        self,
        max_lag: int = 60,
        lag_step: int = 1,
        min_correlation: float = 0.3,
        causal_threshold: int = 5
    ):
        """Initialize analyzer.
        
        Args:
            max_lag: Maximum lag in seconds (tests -max_lag to +max_lag)
            lag_step: Step size for lag testing
            min_correlation: Minimum correlation to consider significant
            causal_threshold: Minimum lag to flag as potential causal
        """
        self.max_lag = max_lag
        self.lag_step = lag_step
        self.min_correlation = min_correlation
        self.causal_threshold = causal_threshold
    
    def analyze_pair(
        self, 
        df: pd.DataFrame, 
        param1: str, 
        param2: str,
        timestamp_col: str = "timestamp"
    ) -> dict[str, Any]:
        """Analyze lag-correlation between two parameters.
        
        Args:
            df: DataFrame with data
            param1: First parameter name
            param2: Second parameter name
            timestamp_col: Name of timestamp column
            
        Returns:
            Analysis results for this pair
        """
        if param1 not in df.columns or param2 not in df.columns:
            return {"error": f"Column not found: {param1} or {param2}"}
        
        # Ensure sorted by timestamp
        df_sorted = df.sort_values(timestamp_col).copy()
        
        lags = range(-self.max_lag, self.max_lag + 1, self.lag_step)
        correlations = []
        
        for lag in lags:
            if lag == 0:
                corr = df_sorted[param1].corr(df_sorted[param2])
            elif lag > 0:
                # param2 shifted forward (param1 leads)
                shifted = df_sorted[param2].shift(-lag)
                corr = df_sorted[param1].corr(shifted)
            else:
                # param2 shifted backward (param2 leads)
                shifted = df_sorted[param2].shift(-lag)
                corr = df_sorted[param1].corr(shifted)
            
            correlations.append({
                "lag": lag,
                "correlation": corr if not np.isnan(corr) else 0.0
            })
        
        # Find optimal lag
        best = max(correlations, key=lambda x: abs(x["correlation"]))
        optimal_lag = best["lag"]
        max_correlation = best["correlation"]
        
        # Determine relationship
        is_significant = abs(max_correlation) >= self.min_correlation
        is_causal = abs(optimal_lag) >= self.causal_threshold
        
        if optimal_lag > 0:
            relationship = f"{param1} leads {param2} by {optimal_lag}s"
        elif optimal_lag < 0:
            relationship = f"{param2} leads {param1} by {-optimal_lag}s"
        else:
            relationship = "Simultaneous"
        
        return {
            "param1": param1,
            "param2": param2,
            "optimal_lag": optimal_lag,
            "max_correlation": round(max_correlation, 4),
            "is_significant": is_significant,
            "is_causal": is_causal and is_significant,
            "relationship": relationship,
            "all_correlations": correlations
        }
    
    def analyze_all_pairs(
        self, 
        df: pd.DataFrame,
        parameters: list[str] | None = None,
        timestamp_col: str = "timestamp"
    ) -> list[dict[str, Any]]:
        """Analyze lag-correlations for all parameter pairs.
        
        Args:
            df: DataFrame with data
            parameters: List of parameters to analyze (None = all numeric)
            timestamp_col: Name of timestamp column
            
        Returns:
            List of analysis results for all pairs
        """
        if parameters is None:
            parameters = df.select_dtypes(include=[np.number]).columns.tolist()
            if timestamp_col in parameters:
                parameters.remove(timestamp_col)
        
        results = []
        n = len(parameters)
        
        for i in range(n):
            for j in range(i + 1, n):
                result = self.analyze_pair(
                    df, parameters[i], parameters[j], timestamp_col
                )
                results.append(result)
        
        # Sort by absolute correlation
        results.sort(key=lambda x: abs(x.get("max_correlation", 0)), reverse=True)
        return results
    
    def get_causal_relationships(
        self, 
        results: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Filter results to only causal relationships.
        
        Args:
            results: List of analysis results
            
        Returns:
            Filtered list with only causal relationships
        """
        return [r for r in results if r.get("is_causal", False)]
    
    def generate_lag_plot(
        self, 
        result: dict[str, Any], 
        output_path: str | Path
    ) -> None:
        """Generate lag-correlation plot for a pair.
        
        Args:
            result: Analysis result for a pair
            output_path: Path to save PNG file
        """
        try:
            import matplotlib.pyplot as plt
            
            correlations = result.get("all_correlations", [])
            if not correlations:
                return
            
            lags = [c["lag"] for c in correlations]
            corrs = [c["correlation"] for c in correlations]
            
            plt.figure(figsize=(10, 6))
            plt.plot(lags, corrs, 'b-', linewidth=1.5)
            plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
            plt.axvline(x=0, color='gray', linestyle='--', alpha=0.5)
            
            # Mark optimal lag
            optimal_lag = result["optimal_lag"]
            max_corr = result["max_correlation"]
            plt.scatter([optimal_lag], [max_corr], color='red', s=100, zorder=5)
            plt.annotate(
                f"Optimal: {optimal_lag}s\nr={max_corr:.3f}",
                (optimal_lag, max_corr),
                xytext=(10, 10),
                textcoords='offset points'
            )
            
            plt.xlabel("Lag (seconds)")
            plt.ylabel("Correlation")
            plt.title(f"Lag-Correlation: {result['param1']} vs {result['param2']}")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(output_path, dpi=150)
            plt.close()
            
            logger.info(f"Lag plot saved to {output_path}")
            
        except ImportError:
            logger.warning("matplotlib not available for plotting")
    
    def analyze(self, df: pd.DataFrame) -> dict[str, Any]:
        """Run full lag-correlation analysis.
        
        Args:
            df: DataFrame with data
            
        Returns:
            Analysis results dictionary
        """
        all_results = self.analyze_all_pairs(df)
        causal = self.get_causal_relationships(all_results)
        
        return {
            "all_pairs": all_results,
            "causal_relationships": causal,
            "total_pairs": len(all_results),
            "causal_count": len(causal),
            "max_lag_tested": self.max_lag,
            "causal_threshold": self.causal_threshold
        }
