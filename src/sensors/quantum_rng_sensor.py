"""Quantum RNG Sensor - True Random Number Generation.

Monitors quantum random number generators for non-random patterns.
Uses multiple sources with intelligent fallback:
1. ANU QRNG (quantum vacuum fluctuations) - PRIMARY
2. Random.org (atmospheric noise) - FALLBACK 1  
3. Local entropy (hardware RNG) - FALLBACK 2
"""

import logging
from typing import Any
import aiohttp
import statistics
import secrets

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)


class QuantumRNGSensor(BaseSensor):
    """Sensor for monitoring quantum random number generators.
    
    Uses multiple sources with fallback:
    - ANU QRNG: True quantum randomness from vacuum fluctuations
    - Random.org: Atmospheric noise (radio static)
    - Local entropy: Hardware RNG (/dev/urandom + secrets)
    
    Analyzes for non-random patterns (potential "glitches").
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        sample_size: int = 1024,
        anu_api_key: str | None = None
    ):
        """Initialize Quantum RNG Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing readings
            sample_size: Number of random numbers to fetch
            anu_api_key: ANU QRNG API key for higher rate limits
        """
        super().__init__("quantum_rng", config, event_bus)
        self.sample_size = sample_size
        self.anu_api_key = anu_api_key
        
        # Use authenticated endpoint if API key provided
        if anu_api_key:
            self.anu_url = "https://api.quantumnumbers.anu.edu.au"
        else:
            self.anu_url = "https://qrng.anu.edu.au/API/jsonI.php"
        
        self.random_org_url = "https://www.random.org/integers/"
        
        # Track failures for intelligent fallback
        self.anu_failures = 0
        self.random_org_failures = 0
        self.current_source = "unknown"
    
    async def collect(self) -> SensorReading:
        """Collect quantum random numbers and analyze."""
        numbers = None
        source = "unknown"
        
        # Try ANU QRNG first (quantum vacuum)
        try:
            numbers = await self._fetch_anu_quantum()
            source = "anu_quantum"
            self.anu_failures = 0
            logger.info("QRNG: ✓ Using ANU Quantum (vacuum fluctuations)")
        except Exception as e:
            self.anu_failures += 1
            logger.warning(f"QRNG: ✗ ANU failed (attempt {self.anu_failures}): {e}")
            
            # Try Random.org (atmospheric noise)
            try:
                numbers = await self._fetch_random_org()
                source = "random_org_atmospheric"
                self.random_org_failures = 0
                logger.info("QRNG: ✓ Using Random.org (atmospheric noise)")
            except Exception as e2:
                self.random_org_failures += 1
                logger.warning(f"QRNG: ✗ Random.org failed (attempt {self.random_org_failures}): {e2}")
                
                # Fallback to local entropy
                numbers = self._generate_local_entropy()
                source = "local_entropy"
                logger.info("QRNG: ✓ Using local entropy (hardware RNG)")
        
        self.current_source = source
        
        # Analyze randomness
        analysis = self._analyze_randomness(numbers)
        analysis["source"] = source
        analysis["anu_failures"] = self.anu_failures
        analysis["random_org_failures"] = self.random_org_failures
        
        return SensorReading.create(
            source="quantum_rng",
            data=analysis
        )
    
    def _analyze_randomness(self, numbers: list[int]) -> dict[str, Any]:
        """Analyze random numbers for patterns.
        
        True randomness should have:
        - Mean close to 127.5 (for 0-255 range)
        - Uniform distribution
        - No autocorrelation
        - Chi-square test passes
        """
        # Basic statistics
        mean = statistics.mean(numbers)
        std = statistics.stdev(numbers)
        
        # Expected values for uniform distribution (0-255)
        expected_mean = 127.5
        expected_std = 73.9  # sqrt((256^2 - 1) / 12)
        
        # Deviation from expected
        mean_deviation = abs(mean - expected_mean) / expected_std
        std_deviation = abs(std - expected_std) / expected_std
        
        # Count runs (sequences of increasing/decreasing)
        runs = self._count_runs(numbers)
        expected_runs = (2 * len(numbers) - 1) / 3
        runs_deviation = abs(runs - expected_runs) / (expected_runs ** 0.5)
        
        # Bit balance (for binary representation)
        bit_balance = self._check_bit_balance(numbers)
        
        # Autocorrelation (should be near 0)
        autocorr = self._autocorrelation(numbers, lag=1)
        
        # Overall randomness score (0-1, where 1 is perfectly random)
        randomness_score = self._calculate_randomness_score(
            mean_deviation, std_deviation, runs_deviation, bit_balance, autocorr
        )
        
        # Detect anomaly
        # Calibrated threshold: 0.85 (P10 percentile) gives ~10% trigger rate
        # Old threshold 0.95 gave 75% trigger rate - too high!
        is_anomalous = randomness_score < 0.85  # Less than 85% random
        
        return {
            "sample_size": len(numbers),
            "mean": mean,
            "std": std,
            "mean_deviation": mean_deviation,
            "std_deviation": std_deviation,
            "runs": runs,
            "runs_deviation": runs_deviation,
            "bit_balance": bit_balance,
            "autocorrelation": autocorr,
            "randomness_score": randomness_score,
            "is_anomalous": is_anomalous,
            "anomaly_type": self._classify_anomaly(mean_deviation, bit_balance, autocorr) if is_anomalous else None
        }
    
    def _count_runs(self, numbers: list[int]) -> int:
        """Count number of runs (monotonic sequences)."""
        if len(numbers) < 2:
            return 0
        
        runs = 1
        for i in range(1, len(numbers)):
            if (numbers[i] > numbers[i-1]) != (numbers[i-1] > numbers[i-2] if i > 1 else True):
                runs += 1
        
        return runs
    
    def _check_bit_balance(self, numbers: list[int]) -> float:
        """Check balance of 0s and 1s in binary representation.
        
        Returns ratio of 1s (should be close to 0.5).
        """
        total_bits = 0
        one_bits = 0
        
        for num in numbers:
            for i in range(8):  # 8 bits per byte
                total_bits += 1
                if num & (1 << i):
                    one_bits += 1
        
        return one_bits / total_bits if total_bits > 0 else 0.5
    
    def _autocorrelation(self, numbers: list[int], lag: int = 1) -> float:
        """Calculate autocorrelation at given lag."""
        if len(numbers) <= lag:
            return 0.0
        
        mean = statistics.mean(numbers)
        
        numerator = sum(
            (numbers[i] - mean) * (numbers[i + lag] - mean)
            for i in range(len(numbers) - lag)
        )
        
        denominator = sum((x - mean) ** 2 for x in numbers)
        
        return numerator / denominator if denominator != 0 else 0.0
    
    def _calculate_randomness_score(
        self,
        mean_dev: float,
        std_dev: float,
        runs_dev: float,
        bit_balance: float,
        autocorr: float
    ) -> float:
        """Calculate overall randomness score (0-1)."""
        # Each component contributes to score
        mean_score = max(0, 1 - mean_dev / 3)  # Penalize if > 3 std devs
        std_score = max(0, 1 - std_dev / 3)
        runs_score = max(0, 1 - runs_dev / 3)
        bit_score = 1 - abs(bit_balance - 0.5) * 2  # Should be 0.5
        autocorr_score = max(0, 1 - abs(autocorr) * 10)  # Should be near 0
        
        # Weighted average
        score = (
            mean_score * 0.25 +
            std_score * 0.20 +
            runs_score * 0.20 +
            bit_score * 0.20 +
            autocorr_score * 0.15
        )
        
        return max(0.0, min(1.0, score))
    
    def _classify_anomaly(self, mean_dev: float, bit_balance: float, autocorr: float) -> str:
        """Classify type of anomaly detected."""
        if abs(autocorr) > 0.1:
            return "correlation"  # Numbers are correlated (not independent)
        elif abs(bit_balance - 0.5) > 0.05:
            return "bias"  # Biased towards 0s or 1s
        elif mean_dev > 2:
            return "mean_shift"  # Mean shifted from expected
        else:
            return "unknown"
    
    async def _fetch_anu_quantum(self) -> list[int]:
        """Fetch quantum random numbers from ANU QRNG (quantum vacuum)."""
        # Create SSL context that doesn't verify certificates (ANU cert expired)
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            if self.anu_api_key:
                # Use authenticated API with key (correct header format)
                headers = {"X-Api-Key": self.anu_api_key}
                params = {
                    "length": self.sample_size,
                    "type": "uint8"
                }
                
                async with session.get(
                    self.anu_url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status != 200:
                        raise Exception(f"ANU API returned {response.status}")
                    
                    data = await response.json()
                    
                    if not data.get("success"):
                        raise Exception("ANU API request failed")
                    
                    numbers = data.get("data", [])
                    
                    if len(numbers) < 100:
                        raise Exception("Insufficient numbers received")
                    
                    return numbers
            else:
                # Use public API (rate limited)
                params = {
                    "length": self.sample_size,
                    "type": "uint8"
                }
                
                async with session.get(
                    self.anu_url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        raise Exception(f"ANU API returned {response.status}")
                    
                    data = await response.json()
                    
                    if not data.get("success"):
                        raise Exception("ANU API request failed")
                    
                    numbers = data.get("data", [])
                    
                    if len(numbers) < 100:
                        raise Exception("Insufficient numbers received")
                    
                    return numbers
    
    async def _fetch_random_org(self) -> list[int]:
        """Fetch random numbers from Random.org (atmospheric noise)."""
        params = {
            "num": self.sample_size,
            "min": 0,
            "max": 255,
            "col": 1,
            "base": 10,
            "format": "plain",
            "rnd": "new"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                self.random_org_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status != 200:
                    raise Exception(f"Random.org returned {response.status}")
                
                text = await response.text()
                numbers = [int(x.strip()) for x in text.strip().split('\n') if x.strip()]
                
                if len(numbers) < 100:
                    raise Exception(f"Random.org returned too few numbers: {len(numbers)}")
                
                return numbers[:self.sample_size]
    
    def _generate_local_entropy(self) -> list[int]:
        """Generate random numbers from local entropy sources.
        
        Uses Python's secrets module which uses:
        - /dev/urandom on Unix (hardware RNG)
        - CryptGenRandom on Windows
        """
        numbers = [secrets.randbelow(256) for _ in range(self.sample_size)]
        return numbers
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for quantum RNG data."""
        return {
            "sample_size": int,
            "source": str,
            "mean": float,
            "randomness_score": float,
            "is_anomalous": bool,
            "bit_balance": float,
            "autocorrelation": float
        }
