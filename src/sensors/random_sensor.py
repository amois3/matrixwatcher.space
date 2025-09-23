"""Random Sensor for Matrix Watcher.

Collects random data from multiple sources and calculates statistical properties.
"""

import logging
import os
import random
import time
from typing import Any

import aiohttp
from scipy import stats

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus
from ..utils.statistics import chi_square_test

logger = logging.getLogger(__name__)


class RandomSensor(BaseSensor):
    """Sensor for collecting and analyzing random number generation.
    
    Sources:
    - Python random.random() (Mersenne Twister)
    - os.urandom() (OS entropy)
    - random.org API (optional, true random)
    
    For each batch, calculates:
    - zeros_count, ones_count: Bit distribution
    - chi_square: Chi-square statistic
    - p_value: Statistical significance
    - is_anomalous: True if p_value < 0.01
    
    Example:
        sensor = RandomSensor(batch_size=1024)
        reading = await sensor.collect()
        print(f"Chi-square: {reading.data['python_random']['chi_square']}")
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        batch_size: int = 1024,
        use_random_org: bool = False,
        random_org_api_key: str | None = None
    ):
        """Initialize Random Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            batch_size: Number of values per batch (default 1024)
            use_random_org: Whether to query random.org API
            random_org_api_key: API key for random.org
        """
        super().__init__("random", config, event_bus)
        self.batch_size = batch_size
        self.use_random_org = use_random_org
        self.random_org_api_key = random_org_api_key
    
    async def collect(self) -> SensorReading:
        """Collect random data from all sources."""
        timestamp = time.time()
        
        # Python random (Mersenne Twister)
        python_random_data = self._collect_python_random()
        
        # OS urandom
        urandom_data = self._collect_urandom()
        
        # Random.org (optional)
        random_org_data = None
        if self.use_random_org:
            random_org_data = await self._collect_random_org()
        
        data = {
            "timestamp": timestamp,
            "batch_size": self.batch_size,
            "python_random": python_random_data,
            "urandom": urandom_data,
            "random_org": random_org_data,
            "any_anomalous": (
                python_random_data["is_anomalous"] or 
                urandom_data["is_anomalous"] or
                (random_org_data["is_anomalous"] if random_org_data else False)
            )
        }
        
        return SensorReading.create(self.name, data)
    
    def _collect_python_random(self) -> dict[str, Any]:
        """Collect from Python's random module."""
        values = [random.random() for _ in range(self.batch_size)]
        return self._analyze_random_values(values, "python_random")
    
    def _collect_urandom(self) -> dict[str, Any]:
        """Collect from os.urandom."""
        # Get random bytes and convert to floats [0, 1)
        raw_bytes = os.urandom(self.batch_size * 8)
        values = []
        for i in range(self.batch_size):
            # Convert 8 bytes to uint64, then normalize to [0, 1)
            chunk = raw_bytes[i*8:(i+1)*8]
            uint_val = int.from_bytes(chunk, byteorder='little')
            values.append(uint_val / (2**64))
        
        return self._analyze_random_values(values, "urandom")
    
    async def _collect_random_org(self) -> dict[str, Any] | None:
        """Collect from random.org API."""
        try:
            # Use the simple API (limited but no key required for small requests)
            url = f"https://www.random.org/integers/?num={min(self.batch_size, 100)}&min=0&max=1000000&col=1&base=10&format=plain&rnd=new"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        logger.warning(f"random.org returned status {response.status}")
                        return None
                    
                    text = await response.text()
                    values = [int(x) / 1000000 for x in text.strip().split('\n') if x]
                    
                    # Pad to batch_size if needed
                    while len(values) < self.batch_size:
                        values.extend(values[:self.batch_size - len(values)])
                    values = values[:self.batch_size]
                    
                    return self._analyze_random_values(values, "random_org")
                    
        except Exception as e:
            logger.warning(f"Failed to collect from random.org: {e}")
            return None
    
    def _analyze_random_values(self, values: list[float], source: str) -> dict[str, Any]:
        """Analyze a batch of random values.
        
        Args:
            values: List of float values in [0, 1)
            source: Source name
            
        Returns:
            Analysis results
        """
        # Convert to bits (threshold at 0.5)
        bits = [1 if v >= 0.5 else 0 for v in values]
        ones_count = sum(bits)
        zeros_count = len(bits) - ones_count
        
        # Chi-square test for uniform distribution
        # Expected: 50% zeros, 50% ones
        expected = len(bits) / 2
        chi_sq = ((zeros_count - expected) ** 2 / expected + 
                  (ones_count - expected) ** 2 / expected)
        
        # P-value from chi-square distribution with 1 degree of freedom
        p_value = 1 - stats.chi2.cdf(chi_sq, df=1)
        
        # Also test the actual values for uniformity
        # Divide into 10 bins and check distribution
        bins = [0] * 10
        for v in values:
            bin_idx = min(int(v * 10), 9)
            bins[bin_idx] += 1
        
        expected_per_bin = len(values) / 10
        chi_sq_uniform = sum((b - expected_per_bin) ** 2 / expected_per_bin for b in bins)
        p_value_uniform = 1 - stats.chi2.cdf(chi_sq_uniform, df=9)
        
        return {
            "source": source,
            "sample_size": len(values),
            "zeros_count": zeros_count,
            "ones_count": ones_count,
            "zeros_ratio": round(zeros_count / len(bits), 4),
            "chi_square": round(float(chi_sq), 4),
            "p_value": round(float(p_value), 6),
            "chi_square_uniform": round(float(chi_sq_uniform), 4),
            "p_value_uniform": round(float(p_value_uniform), 6),
            "is_anomalous": bool(p_value < 0.01 or p_value_uniform < 0.01),
            "bin_distribution": bins
        }
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for random sensor data."""
        return {
            "timestamp": float,
            "batch_size": int,
            "python_random": dict,
            "urandom": dict,
            "any_anomalous": bool
        }
