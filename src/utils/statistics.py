"""Statistical utility functions for Matrix Watcher."""

import math
from typing import Sequence
from scipy import stats
import numpy as np


def chi_square_test(observed_zeros: int, observed_ones: int) -> tuple[float, float]:
    """Perform chi-square test for randomness.
    
    Returns:
        Tuple of (chi_square_statistic, p_value)
    """
    total = observed_zeros + observed_ones
    expected = total / 2
    chi_sq = ((observed_zeros - expected) ** 2 + (observed_ones - expected) ** 2) / expected
    p_value = 1 - stats.chi2.cdf(chi_sq, df=1)
    return chi_sq, p_value


def shannon_entropy(text: str) -> float:
    """Calculate Shannon entropy of text.
    
    Returns:
        Entropy value (bits per character)
    """
    if not text:
        return 0.0
    
    freq = {}
    for char in text:
        freq[char] = freq.get(char, 0) + 1
    
    length = len(text)
    entropy = 0.0
    for count in freq.values():
        p = count / length
        if p > 0:
            entropy -= p * math.log2(p)
    
    return entropy


def pearson_correlation(x: Sequence[float], y: Sequence[float]) -> float:
    """Calculate Pearson correlation coefficient."""
    if len(x) != len(y) or len(x) < 2:
        return 0.0
    return float(np.corrcoef(x, y)[0, 1])


def z_score(value: float, mean: float, std: float) -> float:
    """Calculate z-score."""
    if std == 0:
        return 0.0
    return (value - mean) / std


def sliding_window_stats(values: Sequence[float]) -> dict[str, float]:
    """Calculate statistics for a sliding window."""
    if not values:
        return {"mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}
    
    arr = np.array(values)
    return {
        "mean": float(np.mean(arr)),
        "std": float(np.std(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr))
    }
