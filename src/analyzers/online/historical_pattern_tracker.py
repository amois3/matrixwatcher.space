"""Historical Pattern Tracker - Tracks "condition → event" patterns.

Follows Matrix Watcher philosophy:
- Observes, doesn't predict
- Tracks statistical patterns
- Provides probabilistic estimates (not predictions)
- Self-calibrates and validates
"""

import logging
import time
import json
from collections import deque, defaultdict
from dataclasses import dataclass, asdict, field
from typing import Any
from pathlib import Path

logger = logging.getLogger(__name__)


# Categories for filtering notifications
CRYPTO_EVENTS = {
    "btc_pump_1h", "btc_dump_1h",
    "btc_pump_4h", "btc_dump_4h",
    "btc_pump_24h", "btc_dump_24h",
    "eth_pump_1h", "eth_dump_1h",
    "eth_pump_4h", "eth_dump_4h",
    "eth_pump_24h", "eth_dump_24h",
    "btc_volatility_high", "btc_volatility_medium",
    "blockchain_anomaly"
}


@dataclass
class Condition:
    """Represents a system condition (state)."""
    timestamp: float
    level: int  # Cluster level 1-5
    sources: list[str]  # Which sensors involved
    anomaly_index: float  # 0-100
    baseline_ratio: float  # How much above baseline
    
    def to_key(self) -> str:
        """Generate unique key for this condition type."""
        sources_key = "_".join(sorted(self.sources))
        return f"L{self.level}_{sources_key}"


@dataclass
class Event:
    """Represents an external event that happened."""
    timestamp: float
    event_type: str  # e.g., "btc_pump_1h", "btc_dump_24h", etc.
    severity: str  # "low", "medium", "high"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Pattern:
    """Represents a "condition → event" pattern."""
    condition_key: str
    event_type: str
    
    # Statistics
    condition_count: int = 0  # How many times condition occurred
    event_after_count: int = 0  # How many times event followed
    
    # Timing
    avg_time_to_event: float = 0.0  # Average seconds between condition and event
    min_time_to_event: float = float('inf')
    max_time_to_event: float = 0.0
    
    # Calibration
    predicted_probability: float = 0.0  # What we said
    actual_probability: float = 0.0  # What actually happened
    brier_score: float = 0.0  # Calibration metric (lower is better)
    
    def update_probability(self):
        """Update actual probability based on observations."""
        if self.condition_count > 0:
            # Cap at 1.0 (100%) since one event can match multiple conditions
            self.actual_probability = min(1.0, self.event_after_count / self.condition_count)
        else:
            self.actual_probability = 0.0
    
    def update_brier_score(self):
        """Update Brier score for calibration."""
        if self.condition_count > 0:
            # Brier score = mean squared error of probability predictions
            self.brier_score = (self.predicted_probability - self.actual_probability) ** 2


class HistoricalPatternTracker:
    """Tracks historical patterns between conditions and events."""
    
    # Extended lookback window: 72 hours (3 days)
    LOOKBACK_WINDOW_HOURS = 72
    
    def __init__(self, storage_path: str = "logs/patterns"):
        """Initialize pattern tracker.
        
        Args:
            storage_path: Where to store pattern data
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Recent conditions (for matching with future events)
        self._recent_conditions: deque = deque(maxlen=5000)  # More capacity for 72h window
        
        # Pattern statistics
        self._patterns: dict[str, dict[str, Pattern]] = defaultdict(dict)
        
        # Price history for detecting pumps/dumps
        self._price_history: dict[str, deque] = {
            "btc": deque(maxlen=10000),  # ~3 days at 30s intervals
            "eth": deque(maxlen=10000),
        }
        
        # Event definitions (what we track)
        self._event_definitions = self._create_event_definitions()
        
        # Load existing patterns
        self._load_patterns()
        
        logger.info(f"Historical Pattern Tracker initialized (lookback: {self.LOOKBACK_WINDOW_HOURS}h)")
    
    def _create_event_definitions(self) -> dict[str, dict]:
        """Define what events we track.
        
        CRYPTO EVENTS (main focus):
        - Pumps/Dumps on different timeframes (1h, 4h, 24h)
        - Thresholds based on typical crypto volatility
        
        OTHER EVENTS (for correlation analysis):
        - Earthquakes, space weather, quantum anomalies
        - Recorded for statistics but notifications filtered
        """
        return {
            # ============ CRYPTO: BTC ============
            # 1-hour movements
            "btc_pump_1h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "pump", hours=1, threshold=1.5),
                "severity": "medium",
                "description": "BTC рост > 1.5% за 1ч",
                "category": "crypto"
            },
            "btc_dump_1h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "dump", hours=1, threshold=1.5),
                "severity": "medium",
                "description": "BTC падение > 1.5% за 1ч",
                "category": "crypto"
            },
            # 4-hour movements
            "btc_pump_4h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "pump", hours=4, threshold=3.0),
                "severity": "high",
                "description": "BTC рост > 3% за 4ч",
                "category": "crypto"
            },
            "btc_dump_4h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "dump", hours=4, threshold=3.0),
                "severity": "high",
                "description": "BTC падение > 3% за 4ч",
                "category": "crypto"
            },
            # 24-hour movements
            "btc_pump_24h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "pump", hours=24, threshold=5.0),
                "severity": "high",
                "description": "BTC рост > 5% за 24ч",
                "category": "crypto"
            },
            "btc_dump_24h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "dump", hours=24, threshold=5.0),
                "severity": "high",
                "description": "BTC падение > 5% за 24ч",
                "category": "crypto"
            },
            
            # ============ CRYPTO: ETH ============
            # 1-hour movements
            "eth_pump_1h": {
                "check": lambda data: self._check_crypto_move(data, "ETH", "pump", hours=1, threshold=2.0),
                "severity": "medium",
                "description": "ETH рост > 2% за 1ч",
                "category": "crypto"
            },
            "eth_dump_1h": {
                "check": lambda data: self._check_crypto_move(data, "ETH", "dump", hours=1, threshold=2.0),
                "severity": "medium",
                "description": "ETH падение > 2% за 1ч",
                "category": "crypto"
            },
            # 4-hour movements
            "eth_pump_4h": {
                "check": lambda data: self._check_crypto_move(data, "ETH", "pump", hours=4, threshold=4.0),
                "severity": "high",
                "description": "ETH рост > 4% за 4ч",
                "category": "crypto"
            },
            "eth_dump_4h": {
                "check": lambda data: self._check_crypto_move(data, "ETH", "dump", hours=4, threshold=4.0),
                "severity": "high",
                "description": "ETH падение > 4% за 4ч",
                "category": "crypto"
            },
            # 24-hour movements
            "eth_pump_24h": {
                "check": lambda data: self._check_crypto_move(data, "ETH", "pump", hours=24, threshold=7.0),
                "severity": "high",
                "description": "ETH рост > 7% за 24ч",
                "category": "crypto"
            },
            "eth_dump_24h": {
                "check": lambda data: self._check_crypto_move(data, "ETH", "dump", hours=24, threshold=7.0),
                "severity": "high",
                "description": "ETH падение > 7% за 24ч",
                "category": "crypto"
            },
            
            # ============ CRYPTO: Volatility ============
            "btc_volatility_high": {
                "check": lambda data: self._check_btc_volatility(data, threshold=3.0),
                "severity": "high",
                "description": "Высокая волатильность BTC > 3%",
                "category": "crypto"
            },
            "btc_volatility_medium": {
                "check": lambda data: self._check_btc_volatility(data, threshold=2.0),
                "severity": "medium",
                "description": "Средняя волатильность BTC > 2%",
                "category": "crypto"
            },
            
            # ============ BLOCKCHAIN ============
            "blockchain_anomaly": {
                "check": lambda data: self._check_blockchain_anomaly(data),
                "severity": "medium",
                "description": "Blockchain anomaly (block time)",
                "category": "blockchain"
            },
            
            # ============ EARTHQUAKES ============
            "earthquake_moderate": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=4.5),
                "severity": "medium",
                "description": "Earthquake M4.5+",
                "category": "earthquake"
            },
            "earthquake_strong": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=5.5),
                "severity": "high",
                "description": "Earthquake M5.5+",
                "category": "earthquake"
            },
            "earthquake_major": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=6.5),
                "severity": "critical",
                "description": "Earthquake M6.5+",
                "category": "earthquake"
            },
            
            # ============ SPACE WEATHER ============
            "solar_storm": {
                "check": lambda data: self._check_solar_storm(data),
                "severity": "high",
                "description": "Solar storm detected",
                "category": "space_weather"
            },
            
            # ============ OTHER (recorded, not displayed) ============
            "earthquake_significant": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=5.5),
                "severity": "high",
                "description": "Землетрясение > 5.5",
                "category": "other"
            },
            "earthquake_moderate": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=5.0),
                "severity": "medium",
                "description": "Землетрясение > 5.0",
                "category": "other"
            },
            "news_spike": {
                "check": lambda data: self._check_news_spike(data, multiplier=2.0),
                "severity": "medium",
                "description": "Всплеск новостей > 2x",
                "category": "other"
            },
            "space_weather_storm": {
                "check": lambda data: self._check_space_weather(data, min_kp=5),
                "severity": "high",
                "description": "Геомагнитная буря Kp > 5",
                "category": "other"
            },
            "quantum_anomaly": {
                "check": lambda data: self._check_quantum_anomaly(data, threshold=0.90),
                "severity": "medium",
                "description": "Квантовая аномалия",
                "category": "other"
            }
        }
    
    def record_condition(self, condition: Condition):
        """Record a new condition (cluster detected).
        
        Args:
            condition: The condition to record
        """
        # Store condition for future matching
        self._recent_conditions.append({
            "condition": condition,
            "timestamp": condition.timestamp,
            "matched_events": []  # Will be filled when events occur
        })
        
        # Update condition count for this pattern type
        condition_key = condition.to_key()
        for event_type in self._event_definitions.keys():
            # Check if pattern exists for this condition+event combination
            if event_type not in self._patterns[condition_key]:
                self._patterns[condition_key][event_type] = Pattern(
                    condition_key=condition_key,
                    event_type=event_type
                )
            
            # Increment condition count
            self._patterns[condition_key][event_type].condition_count += 1
        
        logger.debug(f"Recorded condition: {condition_key}, total patterns: {len(self._patterns)}")
    
    def check_events(self, sensor_data: dict[str, Any]) -> list[Event]:
        """Check if any tracked events occurred.
        
        Args:
            sensor_data: Latest sensor readings
            
        Returns:
            List of events that occurred
        """
        events = []
        current_time = time.time()
        source = sensor_data.get('source', 'unknown')
        
        for event_type, definition in self._event_definitions.items():
            if definition["check"](sensor_data):
                event = Event(
                    timestamp=current_time,
                    event_type=event_type,
                    severity=definition["severity"],
                    metadata={"description": definition["description"]}
                )
                events.append(event)
                logger.info(f"🎯 Event detected: {event_type} from {source}")
                
                # Match with recent conditions
                self._match_event_with_conditions(event)
        
        return events
    
    def _match_event_with_conditions(self, event: Event):
        """Match an event with recent conditions."""
        current_time = event.timestamp
        lookback_window = self.LOOKBACK_WINDOW_HOURS * 3600  # 72 hours in seconds
        
        for item in self._recent_conditions:
            condition = item["condition"]
            time_diff = current_time - condition.timestamp
            
            # Only match if event happened after condition (within window)
            if 0 < time_diff < lookback_window:
                condition_key = condition.to_key()
                
                if event.event_type in self._patterns[condition_key]:
                    pattern = self._patterns[condition_key][event.event_type]
                    
                    # Update statistics
                    pattern.event_after_count += 1
                    
                    # Update timing
                    if time_diff < pattern.min_time_to_event:
                        pattern.min_time_to_event = time_diff
                    if time_diff > pattern.max_time_to_event:
                        pattern.max_time_to_event = time_diff
                    
                    # Update average time
                    n = pattern.event_after_count
                    pattern.avg_time_to_event = (
                        (pattern.avg_time_to_event * (n - 1) + time_diff) / n
                    )
                    
                    # Update probabilities
                    pattern.update_probability()
                    
                    # Mark as matched
                    item["matched_events"].append(event.event_type)
                    
                    logger.debug(
                        f"Pattern matched: {condition_key} → {event.event_type} "
                        f"(probability: {pattern.actual_probability:.1%}, "
                        f"avg time: {pattern.avg_time_to_event/3600:.1f}h)"
                    )
    
    def get_probabilities(self, condition: Condition, min_observations: int = 5, 
                           category_filter: str | None = None) -> dict[str, dict]:
        """Get probabilistic estimates for a condition.
        
        Args:
            condition: The current condition
            min_observations: Minimum observations needed for reliable estimate
            category_filter: Filter by category ("crypto", "earthquake", "space_weather", "blockchain", or None for all except "other")
            
        Returns:
            Dictionary of event_type → probability info
        """
        condition_key = condition.to_key()
        results = {}
        
        if condition_key not in self._patterns:
            return results
        
        for event_type, pattern in self._patterns[condition_key].items():
            # Filter by category if specified
            event_def = self._event_definitions.get(event_type, {})
            event_category = event_def.get("category", "other")
            
            # Skip "other" category events (internal use only)
            if event_category == "other":
                continue
            
            # Apply category filter if specified
            if category_filter and event_category != category_filter:
                continue
            
            # Only return if we have enough observations
            if pattern.condition_count >= min_observations:
                # Only include if there's actual probability > 0
                if pattern.actual_probability > 0:
                    results[event_type] = {
                        "probability": pattern.actual_probability,
                        "avg_time_hours": pattern.avg_time_to_event / 3600,
                        "min_time_hours": pattern.min_time_to_event / 3600 if pattern.min_time_to_event != float('inf') else None,
                        "max_time_hours": pattern.max_time_to_event / 3600 if pattern.max_time_to_event > 0 else None,
                        "observations": pattern.condition_count,
                        "occurrences": pattern.event_after_count,
                        "description": self._event_definitions.get(event_type, {}).get("description", event_type),
                        "severity": self._event_definitions.get(event_type, {}).get("severity", "medium"),
                        "category": self._event_definitions.get(event_type, {}).get("category", "other")
                    }
        
        return results
    
    def get_calibration_stats(self) -> dict[str, Any]:
        """Get calibration statistics for all patterns."""
        total_patterns = 0
        total_brier = 0.0
        well_calibrated = 0
        
        for condition_patterns in self._patterns.values():
            for pattern in condition_patterns.values():
                if pattern.condition_count >= 5:
                    total_patterns += 1
                    pattern.update_brier_score()
                    total_brier += pattern.brier_score
                    
                    # Well calibrated if Brier score < 0.1
                    if pattern.brier_score < 0.1:
                        well_calibrated += 1
        
        return {
            "total_patterns": total_patterns,
            "avg_brier_score": total_brier / total_patterns if total_patterns > 0 else 0.0,
            "well_calibrated_percent": (well_calibrated / total_patterns * 100) if total_patterns > 0 else 0.0
        }
    
    def _save_patterns(self):
        """Save patterns to disk."""
        # Save patterns
        data = {}
        for condition_key, patterns in self._patterns.items():
            data[condition_key] = {}
            for event_type, pattern in patterns.items():
                pattern_dict = asdict(pattern)
                # Replace inf with None for valid JSON
                if pattern_dict['min_time_to_event'] == float('inf'):
                    pattern_dict['min_time_to_event'] = None
                data[condition_key][event_type] = pattern_dict
        
        file_path = self.storage_path / "patterns.json"
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Save recent conditions for persistence across restarts
        conditions_data = []
        for item in self._recent_conditions:
            cond = item["condition"]
            conditions_data.append({
                "timestamp": cond.timestamp,
                "level": cond.level,
                "sources": cond.sources,
                "anomaly_index": cond.anomaly_index,
                "baseline_ratio": cond.baseline_ratio,
                "matched_events": item["matched_events"]
            })
        
        conditions_file = self.storage_path / "recent_conditions.json"
        with open(conditions_file, 'w') as f:
            json.dump(conditions_data, f, indent=2)
    
    def _load_patterns(self):
        """Load patterns from disk."""
        # Load patterns
        file_path = self.storage_path / "patterns.json"
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                for condition_key, patterns in data.items():
                    for event_type, pattern_dict in patterns.items():
                        # Replace None with inf when loading
                        if pattern_dict.get('min_time_to_event') is None:
                            pattern_dict['min_time_to_event'] = float('inf')
                        self._patterns[condition_key][event_type] = Pattern(**pattern_dict)
                
                logger.info(f"Loaded {len(self._patterns)} pattern groups from disk")
            except Exception as e:
                logger.error(f"Failed to load patterns: {e}")
        
        # Load recent conditions
        conditions_file = self.storage_path / "recent_conditions.json"
        if conditions_file.exists():
            try:
                with open(conditions_file, 'r') as f:
                    conditions_data = json.load(f)
                
                # Only load conditions from lookback window (72 hours)
                current_time = time.time()
                lookback = self.LOOKBACK_WINDOW_HOURS * 3600
                
                for item in conditions_data:
                    if current_time - item["timestamp"] < lookback:
                        condition = Condition(
                            timestamp=item["timestamp"],
                            level=item["level"],
                            sources=item["sources"],
                            anomaly_index=item["anomaly_index"],
                            baseline_ratio=item["baseline_ratio"]
                        )
                        self._recent_conditions.append({
                            "condition": condition,
                            "timestamp": item["timestamp"],
                            "matched_events": item.get("matched_events", [])
                        })
                
                logger.info(f"Loaded {len(self._recent_conditions)} recent conditions from disk")
            except Exception as e:
                logger.error(f"Failed to load recent conditions: {e}")
    
    # Event check methods - REAL IMPLEMENTATIONS
    # Data comes directly from sensors, with 'source' field indicating sensor type
    
    def _check_btc_volatility(self, data: dict, threshold: float) -> bool:
        """Check if BTC volatility exceeded threshold."""
        try:
            source = data.get('source', '')
            if source != 'crypto':
                return False
            
            # Check pairs for BTCUSDT
            pairs = data.get('pairs', {})
            btc = pairs.get('BTCUSDT', {})
            price_change = abs(btc.get('price_change_pct', 0))
            
            if price_change >= threshold:
                logger.debug(f"BTC volatility detected: {price_change:.2f}% >= {threshold}%")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking BTC volatility: {e}")
            return False
    
    def _check_earthquake(self, data: dict, min_magnitude: float) -> bool:
        """Check if significant earthquake occurred."""
        try:
            # Check if this is earthquake data
            if 'max_magnitude' not in data:
                return False
            
            max_mag = data.get('max_magnitude', 0)
            
            if max_mag >= min_magnitude:
                logger.info(f"🌍 Earthquake detected: {max_mag} >= {min_magnitude}")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking earthquake: {e}")
            return False
    
    def _check_news_spike(self, data: dict, multiplier: float) -> bool:
        """Check if news spike occurred."""
        try:
            source = data.get('source', '')
            if source != 'news':
                return False
            
            new_items = data.get('new_items_count', 0)
            # Spike if more than 50 new items (2x of typical 25)
            baseline = 25
            
            if new_items >= baseline * multiplier:
                logger.debug(f"News spike detected: {new_items} >= {baseline * multiplier}")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking news spike: {e}")
            return False
    
    def _check_space_weather(self, data: dict, min_kp: int) -> bool:
        """Check if space weather storm occurred."""
        try:
            source = data.get('source', '')
            if source != 'space_weather':
                return False
            
            kp_index = data.get('kp_index', 0)
            
            if kp_index >= min_kp:
                logger.debug(f"Space weather storm detected: Kp={kp_index} >= {min_kp}")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking space weather: {e}")
            return False
    
    def _check_quantum_anomaly(self, data: dict, threshold: float) -> bool:
        """Check if quantum RNG anomaly occurred."""
        try:
            # Check if this is quantum_rng data (source can be 'random_org_atmospheric' or similar)
            if 'randomness_score' not in data:
                return False
            
            randomness = data.get('randomness_score', 1.0)
            
            if randomness < threshold:
                logger.info(f"🎲 Quantum anomaly detected: {randomness:.3f} < {threshold}")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking quantum anomaly: {e}")
            return False
    
    def _check_crypto_move(self, data: dict, coin: str, direction: str, hours: int, threshold: float) -> bool:
        """Check if crypto moved significantly over time period.
        
        Args:
            data: Sensor data
            coin: "BTC" or "ETH"
            direction: "pump" (up) or "dump" (down)
            hours: Time period in hours (1, 4, 24)
            threshold: Minimum % change to trigger
        """
        try:
            source = data.get('source', '')
            if source != 'crypto':
                return False
            
            # Get current price - pairs is a list, not dict
            pairs = data.get('pairs', [])
            pair_name = f"{coin}USDT"
            
            # Find the pair in the list
            pair_data = None
            for p in pairs:
                if p.get('symbol') == pair_name:
                    pair_data = p
                    break
            
            if not pair_data:
                return False
                
            current_price = pair_data.get('price', 0)
            
            if current_price <= 0:
                return False
            
            # Record price in history
            coin_key = coin.lower()
            current_time = time.time()
            self._price_history[coin_key].append({
                "timestamp": current_time,
                "price": current_price
            })
            
            # Find price from N hours ago
            lookback_seconds = hours * 3600
            target_time = current_time - lookback_seconds
            
            old_price = None
            for entry in self._price_history[coin_key]:
                if entry["timestamp"] <= target_time:
                    old_price = entry["price"]
                elif old_price is not None:
                    break
            
            if old_price is None or old_price <= 0:
                return False
            
            # Calculate change
            change_pct = ((current_price - old_price) / old_price) * 100
            
            # Check direction and threshold
            if direction == "pump" and change_pct >= threshold:
                logger.info(f"📈 {coin} PUMP detected: +{change_pct:.2f}% over {hours}h (threshold: {threshold}%)")
                return True
            elif direction == "dump" and change_pct <= -threshold:
                logger.info(f"📉 {coin} DUMP detected: {change_pct:.2f}% over {hours}h (threshold: -{threshold}%)")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking crypto move: {e}")
            return False
    
    def _check_blockchain_anomaly(self, data: dict) -> bool:
        """Check if blockchain has anomalous block times."""
        try:
            source = data.get('source', '')
            if source != 'blockchain':
                return False
            
            # Check for unusual block times
            networks = data.get('networks', {})
            
            for network, net_data in networks.items():
                block_time = net_data.get('block_time_seconds', 0)
                expected = net_data.get('expected_block_time', 0)
                
                if expected > 0 and block_time > 0:
                    # Anomaly if block time is 2x expected or more
                    if block_time >= expected * 2:
                        logger.info(f"⛓️ Blockchain anomaly: {network} block time {block_time}s (expected {expected}s)")
                        return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking blockchain anomaly: {e}")
            return False
    
    def _check_solar_storm(self, data: dict) -> bool:
        """Check for solar storm activity."""
        try:
            source = data.get('source', '')
            if source != 'space_weather':
                return False
            
            # Check for high solar activity
            kp_index = data.get('kp_index', 0)
            solar_wind_speed = data.get('solar_wind_speed', 0)
            
            # Kp index > 5 = geomagnetic storm
            # Solar wind > 600 km/s = high speed stream
            if kp_index >= 5 or solar_wind_speed >= 600:
                logger.info(f"☀️ Solar storm detected: Kp={kp_index}, wind={solar_wind_speed}km/s")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking solar storm: {e}")
            return False
    
    def save(self):
        """Save all patterns to disk."""
        self._save_patterns()
        logger.info("Patterns saved to disk")
