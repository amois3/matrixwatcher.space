"""Correlation Analyzer for Matrix Watcher.

Builds correlation matrices between all numeric parameters.
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)


class CorrelationAnalyzer:
    """Analyzer for computing correlations between parameters.
    
    Builds full correlation matrix using Pearson correlation,
    identifies significant pairs, and generates visualizations.
    
    Example:
        analyzer = CorrelationAnalyzer(significance_threshold=0.7)
        matrix = analyzer.compute_matrix(df)
        significant = analyzer.get_significant_pairs(matrix)
    """
    
    def __init__(
        self,
        significance_threshold: float = 0.7,
        min_samples: int = 30
    ):
        """Initialize analyzer.
        
        Args:
            significance_threshold: Minimum abs correlation for significance
            min_samples: Minimum samples required for correlation
        """
        self.significance_threshold = significance_threshold
        self.min_samples = min_samples
    
    def compute_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute correlation matrix for all numeric columns.
        
        Args:
            df: DataFrame with numeric columns
            
        Returns:
            Correlation matrix as DataFrame
        """
        # Select only numeric columns
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            logger.warning("Need at least 2 numeric columns for correlation")
            return pd.DataFrame()
        
        if len(numeric_df) < self.min_samples:
            logger.warning(f"Need at least {self.min_samples} samples")
            return pd.DataFrame()
        
        # Compute Pearson correlation matrix
        corr_matrix = numeric_df.corr(method='pearson')
        
        return corr_matrix
    
    def compute_with_pvalues(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Compute correlation matrix with p-values.
        
        Args:
            df: DataFrame with numeric columns
            
        Returns:
            Tuple of (correlation matrix, p-value matrix)
        """
        numeric_df = df.select_dtypes(include=[np.number])
        cols = numeric_df.columns
        n = len(cols)
        
        corr_matrix = np.zeros((n, n))
        pval_matrix = np.zeros((n, n))
        
        for i in range(n):
            for j in range(n):
                if i == j:
                    corr_matrix[i, j] = 1.0
                    pval_matrix[i, j] = 0.0
                else:
                    # Drop NaN values for this pair
                    valid = numeric_df[[cols[i], cols[j]]].dropna()
                    if len(valid) >= self.min_samples:
                        r, p = stats.pearsonr(valid[cols[i]], valid[cols[j]])
                        corr_matrix[i, j] = r
                        pval_matrix[i, j] = p
                    else:
                        corr_matrix[i, j] = np.nan
                        pval_matrix[i, j] = np.nan
        
        return (
            pd.DataFrame(corr_matrix, index=cols, columns=cols),
            pd.DataFrame(pval_matrix, index=cols, columns=cols)
        )
    
    def get_significant_pairs(
        self, 
        corr_matrix: pd.DataFrame
    ) -> list[dict[str, Any]]:
        """Get pairs with significant correlation.
        
        Args:
            corr_matrix: Correlation matrix
            
        Returns:
            List of significant pairs with correlation values
        """
        pairs = []
        cols = corr_matrix.columns
        
        for i, col1 in enumerate(cols):
            for j, col2 in enumerate(cols):
                if i < j:  # Upper triangle only
                    corr = corr_matrix.loc[col1, col2]
                    if not np.isnan(corr) and abs(corr) >= self.significance_threshold:
                        pairs.append({
                            "param1": col1,
                            "param2": col2,
                            "correlation": round(corr, 4),
                            "abs_correlation": round(abs(corr), 4),
                            "direction": "positive" if corr > 0 else "negative"
                        })
        
        # Sort by absolute correlation
        pairs.sort(key=lambda x: x["abs_correlation"], reverse=True)
        return pairs
    
    def generate_heatmap(
        self, 
        corr_matrix: pd.DataFrame, 
        output_path: str | Path
    ) -> None:
        """Generate heatmap visualization.
        
        Args:
            corr_matrix: Correlation matrix
            output_path: Path to save PNG file
        """
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            plt.figure(figsize=(12, 10))
            sns.heatmap(
                corr_matrix,
                annot=True,
                fmt=".2f",
                cmap="RdBu_r",
                center=0,
                vmin=-1,
                vmax=1,
                square=True
            )
            plt.title("Correlation Matrix")
            plt.tight_layout()
            plt.savefig(output_path, dpi=150)
            plt.close()
            
            logger.info(f"Heatmap saved to {output_path}")
            
        except ImportError:
            logger.warning("matplotlib/seaborn not available for heatmap")
    
    def export_csv(
        self, 
        corr_matrix: pd.DataFrame, 
        output_path: str | Path
    ) -> None:
        """Export correlation matrix to CSV.
        
        Args:
            corr_matrix: Correlation matrix
            output_path: Path to save CSV file
        """
        corr_matrix.to_csv(output_path)
        logger.info(f"Correlation matrix exported to {output_path}")
    
    def analyze(self, df: pd.DataFrame) -> dict[str, Any]:
        """Run full correlation analysis.
        
        Args:
            df: DataFrame with data
            
        Returns:
            Analysis results dictionary
        """
        corr_matrix, pval_matrix = self.compute_with_pvalues(df)
        significant_pairs = self.get_significant_pairs(corr_matrix)
        
        return {
            "correlation_matrix": corr_matrix,
            "pvalue_matrix": pval_matrix,
            "significant_pairs": significant_pairs,
            "total_parameters": len(corr_matrix.columns),
            "significant_count": len(significant_pairs),
            "threshold": self.significance_threshold
        }
