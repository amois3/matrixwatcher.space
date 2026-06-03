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
import bisect
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

    # Temporal features (auto-calculated from timestamp)
    hour_of_day: int = -1  # 0-23 UTC
    day_of_week: int = -1  # 0=Monday, 6=Sunday
    is_weekend: bool = False
    month: int = -1  # 1-12

    def __post_init__(self):
        """Auto-calculate temporal features from timestamp."""
        if self.hour_of_day == -1:  # Not manually set
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
            self.hour_of_day = dt.hour
            self.day_of_week = dt.weekday()  # 0=Monday, 6=Sunday
            self.is_weekend = self.day_of_week >= 5
            self.month = dt.month

    def to_key(self) -> str:
        """Generate unique key for this condition type (base pattern)."""
        sources_key = "_".join(sorted(self.sources))
        return f"L{self.level}_{sources_key}"

    def to_temporal_key(self) -> str:
        """Generate key including temporal features for more specific patterns."""
        base = self.to_key()
        # Time of day buckets: night(0-6), morning(6-12), afternoon(12-18), evening(18-24)
        if self.hour_of_day < 6:
            time_bucket = "night"
        elif self.hour_of_day < 12:
            time_bucket = "morning"
        elif self.hour_of_day < 18:
            time_bucket = "afternoon"
        else:
            time_bucket = "evening"

        weekend_str = "weekend" if self.is_weekend else "weekday"
        return f"{base}_{time_bucket}_{weekend_str}"

    def get_time_bucket(self) -> str:
        """Get human-readable time bucket."""
        if self.hour_of_day < 6:
            return "night (00-06 UTC)"
        elif self.hour_of_day < 12:
            return "morning (06-12 UTC)"
        elif self.hour_of_day < 18:
            return "afternoon (12-18 UTC)"
        else:
            return "evening (18-24 UTC)"


@dataclass
class Event:
    """Represents an external event that happened."""
    timestamp: float
    event_type: str  # e.g., "btc_pump_1h", "btc_dump_24h", etc.
    severity: str  # "low", "medium", "high"
    metadata: dict[str, Any] = field(default_factory=dict)
    location: tuple[float, float] | None = None  # (lat, lon) for geographic events


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
    
    # Geographic data (for future clustering)
    event_locations: list[tuple[float, float]] = field(default_factory=list)  # [(lat, lon), ...]
    
    def update_probability(self):
        """Update actual probability based on observations.

        HONEST CALCULATION:
        probability = unique_conditions_where_event_occurred / total_conditions

        Example: If 100 L3_crypto conditions occurred and 30 of them were
        followed by btc_pump_1h, probability = 30/100 = 30%

        Capped at 1.0 for backwards compatibility with old data.
        """
        if self.condition_count > 0:
            self.actual_probability = min(1.0, self.event_after_count / self.condition_count)
        else:
            self.actual_probability = 0.0
    
    def update_brier_score(self):
        """Update Brier score for calibration."""
        if self.condition_count > 0:
            # Brier score = mean squared error of probability predictions
            self.brier_score = (self.predicted_probability - self.actual_probability) ** 2


# --- Geographic region resolution (offline reverse-geocoding) ---
# Uses the `reverse_geocoder` package (offline k-d tree over GeoNames populated
# places) plus `pycountry` for ISO-3166 country names. No network, no API key,
# fully deterministic. Replaces the previous hand-drawn lat/lon rectangles.

import functools

_RG = None  # lazy singleton; loading ~150k places is deferred to first use


def _get_rg():
    """Lazily build the offline reverse geocoder (single-threaded mode)."""
    global _RG
    if _RG is None:
        import reverse_geocoder
        _RG = reverse_geocoder.RGeocoder(mode=1, verbose=False)
    return _RG


@functools.lru_cache(maxsize=512)
def _country_name(cc: str) -> str:
    """ISO 3166 alpha-2 -> country name (e.g. 'JP' -> 'Japan')."""
    if not cc:
        return ""
    try:
        import pycountry
        country = pycountry.countries.get(alpha_2=cc.upper())
        return country.name if country else cc
    except Exception:
        return cc


@functools.lru_cache(maxsize=8192)
def get_region_from_coords(lat: float, lon: float) -> str:
    """Resolve a human-readable region (country) for coordinates, offline.

    Returns the country of the nearest populated place (e.g. "Japan",
    "Chile"). For remote ocean events this is the nearest land region, which
    matches how seismic catalogues label such quakes. Returns "Unknown" if
    resolution fails — never a fabricated location.
    """
    try:
        result = _get_rg().query([(float(lat), float(lon))])[0]
        country = _country_name(result.get("cc", ""))
        admin1 = (result.get("admin1") or "").strip()
        if admin1 and country and admin1 != country:
            return f"{admin1}, {country}"
        return country or admin1 or "Unknown"
    except Exception as e:
        logger.debug(f"Reverse geocoding failed for ({lat},{lon}): {e}")
        return "Unknown"


def get_most_frequent_region(locations: list[tuple[float, float]]) -> str | None:
    """Most frequent region across recent event coordinates (offline geocoding).

    Returns a region only if it accounts for >=30% of the last 100 events and
    resolves to a real region (not "Unknown").
    """
    if not locations or len(locations) < 3:
        return None

    from collections import Counter
    regions = [get_region_from_coords(lat, lon) for lat, lon in locations[-100:]]
    region_counts = Counter(regions)

    most_common = region_counts.most_common(1)
    if most_common:
        region, count = most_common[0]
        if count / len(regions) >= 0.3 and region != "Unknown":
            return region
    return None


class _PriceHistory:
    """Sorted (by timestamp) price series with O(log n) lookback.

    Replaces the original ``deque[dict]`` with parallel timestamp/price lists so
    ``find_price_at_or_before`` can use binary search instead of an O(n) walk.
    The answer is mathematically identical to the previous linear scan
    (``last entry with ts <= target_time``) but the cost drops from O(n) to
    O(log n), which dominates the pattern tracker once n grows toward 10k.

    Invariant: timestamps must be non-decreasing. Live ``append`` calls preserve
    this because ``current_time = time.time()`` is monotonic on a single thread.
    Bulk loaders that mix days out of order (see ``_load_price_history``, which
    walks daily files newest-first) must call ``sort()`` once when done.
    """
    __slots__ = ("maxlen", "timestamps", "prices")

    def __init__(self, maxlen: int = 10000):
        self.maxlen = maxlen
        self.timestamps: list[float] = []
        self.prices: list[float] = []

    def append(self, timestamp: float, price: float) -> None:
        self.timestamps.append(timestamp)
        self.prices.append(price)
        # Trim only when comfortably oversized -> amortized O(1) per append.
        if len(self.timestamps) > self.maxlen * 2:
            drop = len(self.timestamps) - self.maxlen
            del self.timestamps[:drop]
            del self.prices[:drop]

    def sort(self) -> None:
        """Sort both arrays by timestamp; idempotent on already-sorted data."""
        if len(self.timestamps) < 2:
            return
        order = sorted(range(len(self.timestamps)), key=self.timestamps.__getitem__)
        self.timestamps = [self.timestamps[i] for i in order]
        self.prices = [self.prices[i] for i in order]

    def find_price_at_or_before(self, target_time: float) -> float | None:
        """Latest price recorded at a time ``<= target_time``, or None."""
        idx = bisect.bisect_right(self.timestamps, target_time)
        if idx == 0:
            return None
        return self.prices[idx - 1]

    def clear(self) -> None:
        self.timestamps.clear()
        self.prices.clear()

    def __len__(self) -> int:
        return len(self.timestamps)


class HistoricalPatternTracker:
    """Tracks historical patterns between conditions and events."""
    
    # Extended lookback window: 72 hours (3 days)
    LOOKBACK_WINDOW_HOURS = 6  # 6h: rare targets become predictable (headroom)
    
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
        
        # Price history for detecting pumps/dumps (sorted, O(log n) lookback)
        self._price_history: dict[str, _PriceHistory] = {
            "btc": _PriceHistory(maxlen=10000),  # ~3 days at 30s intervals
            "eth": _PriceHistory(maxlen=10000),
        }
        
        # Event definitions (what we track)
        self._event_definitions = self._create_event_definitions()
        
        # Load existing patterns
        self._load_patterns()
        
        # Load price history from logs (for crypto pump/dump detection)
        self._load_price_history()
        
        logger.info(f"Historical Pattern Tracker initialized (lookback: {self.LOOKBACK_WINDOW_HOURS}h)")
    
    def _create_event_definitions(self) -> dict[str, dict]:
        """Define what events we track.

        BALANCED across domains — every domain has 1-2 events of comparable rarity.
        Crypto is intentionally NOT over-represented (used to be 14/25 events
        which biased every backtest toward "crypto predicts crypto"). The set
        is now ~1 event per domain so cross-domain correlations are tested
        symmetrically, not crypto-out as the only target.
        """
        return {
            # ============ CRYPTO (3 events — 1 representative coin, 1 horizon) ============
            "btc_pump_1h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "pump", hours=1, threshold=3.0),
                "severity": "medium",
                "description": "BTC surge > 3% in 1h",
                "category": "crypto",
            },
            "btc_dump_1h": {
                "check": lambda data: self._check_crypto_move(data, "BTC", "dump", hours=1, threshold=3.0),
                "severity": "medium",
                "description": "BTC drop > 3% in 1h",
                "category": "crypto",
            },
            "btc_volatility_high": {
                "check": lambda data: self._check_btc_volatility(data, threshold=3.0),
                "severity": "high",
                "description": "BTC high volatility > 3%",
                "category": "crypto",
            },

            # ============ BLOCKCHAIN ============
            "blockchain_anomaly": {
                "check": lambda data: self._check_blockchain_anomaly(data),
                "severity": "medium",
                "description": "Blockchain anomaly (block time)",
                "category": "blockchain",
            },

            # ============ EARTHQUAKE (2 events) ============
            "earthquake_moderate": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=5.5),
                "severity": "medium",
                "description": "Earthquake M5.5+",
                "category": "earthquake",
            },
            "earthquake_strong": {
                "check": lambda data: self._check_earthquake(data, min_magnitude=6.0),
                "severity": "high",
                "description": "Earthquake M6.0+",
                "category": "earthquake",
            },

            # ============ SPACE WEATHER (2 events) ============
            "solar_storm_moderate": {
                "check": lambda data: self._check_solar_storm(data, min_kp=5),
                "severity": "medium",
                "description": "Solar storm Kp5+",
                "category": "space_weather",
            },
            "solar_storm_strong": {
                "check": lambda data: self._check_solar_storm(data, min_kp=7),
                "severity": "high",
                "description": "Solar storm Kp7+",
                "category": "space_weather",
            },

            # ============ QUANTUM ============
            "quantum_low_randomness": {
                "check": lambda data: self._check_quantum_anomaly(data, threshold=0.76),
                "severity": "medium",
                "description": "Quantum randomness drop (score < 0.76)",
                "category": "quantum",
            },

            # ============ NEWS ============
            "news_spike": {
                "check": lambda data: self._check_news_spike(data, multiplier=2.0),
                "severity": "medium",
                "description": "News volume spike > 2x",
                "category": "news",
            },
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

        # Update condition count for the base pattern only.
        # (Time-of-day/weekend split removed: it multiplied hypotheses for no
        #  current benefit; time-of-day can be tested later via offline replay.)
        condition_key = condition.to_key()

        for event_type in self._event_definitions.keys():
            # Base pattern (always tracked)
            if event_type not in self._patterns[condition_key]:
                self._patterns[condition_key][event_type] = Pattern(
                    condition_key=condition_key,
                    event_type=event_type
                )
            self._patterns[condition_key][event_type].condition_count += 1
            self._patterns[condition_key][event_type].update_probability()

        logger.debug(f"Recorded condition: {condition_key}")
    
    def check_events(self, sensor_data: dict[str, Any], current_time: float | None = None) -> list[Event]:
        """Check if any tracked events occurred.

        Args:
            sensor_data: Latest sensor readings.
            current_time: Optional clock override (defaults to time.time()).
                Pass the event's own timestamp for offline replay so detected
                events carry the historical time rather than "now".

        Returns:
            List of events that occurred.
        """
        events = []
        if current_time is None:
            current_time = time.time()
        source = sensor_data.get('source', 'unknown')
        
        for event_type, definition in self._event_definitions.items():
            if definition["check"](sensor_data):
                # Extract location for geographic events
                location = None
                if 'earthquake' in event_type and 'latitude' in sensor_data and 'longitude' in sensor_data:
                    location = (sensor_data['latitude'], sensor_data['longitude'])
                
                event = Event(
                    timestamp=current_time,
                    event_type=event_type,
                    severity=definition["severity"],
                    metadata={"description": definition["description"]},
                    location=location
                )
                events.append(event)
                logger.info(f"🎯 Event detected: {event_type} from {source}")
                
                # Match with recent conditions
                self._match_event_with_conditions(event)
        
        return events
    
    def _match_event_with_conditions(self, event: Event):
        """Match an event with recent conditions.

        HONEST MATCHING RULES:
        - Each condition can only be matched ONCE per event type
        - This prevents inflated probabilities (e.g., 10000 matches for 1 event)
        - probability = unique_conditions_with_event / total_conditions
        """
        current_time = event.timestamp
        lookback_window = self.LOOKBACK_WINDOW_HOURS * 3600  # 72 hours in seconds

        for item in self._recent_conditions:
            condition = item["condition"]
            time_diff = current_time - condition.timestamp

            # Only match if event happened after condition (within window)
            if 0 < time_diff < lookback_window:
                condition_key = condition.to_key()

                # HONEST: Skip if this condition was already matched with this event type
                # Each condition counts only ONCE per event type
                if event.event_type in item.get("matched_events", []):
                    continue

                # Update BOTH base and temporal patterns
                for pattern_key in [condition_key]:
                    if event.event_type in self._patterns[pattern_key]:
                        pattern = self._patterns[pattern_key][event.event_type]

                        # Update statistics (only counts unique condition matches)
                        pattern.event_after_count += 1

                        # Save location for geographic events (limit to 1000 most recent)
                        if event.location:
                            pattern.event_locations.append(event.location)
                            if len(pattern.event_locations) > 1000:
                                pattern.event_locations = pattern.event_locations[-1000:]

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

                # Mark as matched (once for both patterns)
                item["matched_events"].append(event.event_type)

                logger.debug(
                    f"Pattern matched: {condition_key} → {event.event_type}"
                )
    
    def get_probabilities(self, condition: Condition, min_observations: int = 5,
                           category_filter: str | None = None) -> dict[str, dict]:
        """Get probabilistic estimates for a condition.

        Uses the base pattern per condition. The time-of-day/weekend split
        was removed to avoid multiple-comparison inflation.

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

        for event_type, base_pattern in self._patterns[condition_key].items():
            # Filter by category if specified
            event_def = self._event_definitions.get(event_type, {})
            event_category = event_def.get("category", "other")

            # Skip "other" category events (internal use only)
            if event_category == "other":
                continue

            # Skip earthquake_moderate (M5.0+) - too frequent, not meaningful
            if event_type == "earthquake_moderate":
                continue

            # Apply category filter if specified
            if category_filter and event_category != category_filter:
                continue

            # Base pattern only (time-of-day split removed)
            pattern = base_pattern

            # Only return if we have enough observations
            if pattern.condition_count >= min_observations:
                # Only include if there's actual probability > 0
                if pattern.actual_probability > 0:
                    # Calculate time window width
                    min_time_h = pattern.min_time_to_event / 3600 if pattern.min_time_to_event != float('inf') else None
                    max_time_h = pattern.max_time_to_event / 3600 if pattern.max_time_to_event > 0 else None
                    avg_time_h = pattern.avg_time_to_event / 3600

                    # HONEST SYSTEM: Skip predictions with no meaningful lead time
                    # If avg_time < 0.5 hours (30 min), it's not a prediction -
                    # it's describing what's happening NOW. That's embarrassing.
                    MIN_PREDICTION_LEAD_TIME = 0.5  # hours (30 minutes minimum)
                    if avg_time_h < MIN_PREDICTION_LEAD_TIME:
                        continue

                    # Filter by time window width for earthquakes
                    # Only show if window < 12 hours (precise prediction)
                    if 'earthquake' in event_type:
                        if min_time_h is not None and max_time_h is not None:
                            window_width = max_time_h - min_time_h
                            if window_width >= 12.0:
                                # Window too wide - not precise enough
                                continue

                    result = {
                        "probability": pattern.actual_probability,
                        "avg_time_hours": avg_time_h,
                        "min_time_hours": min_time_h,
                        "max_time_hours": max_time_h,
                        "observations": pattern.condition_count,
                        "occurrences": pattern.event_after_count,
                        "description": self._event_definitions.get(event_type, {}).get("description", event_type),
                        "severity": self._event_definitions.get(event_type, {}).get("severity", "medium"),
                        "category": self._event_definitions.get(event_type, {}).get("category", "other")
                    }
                    
                    # SELF-LEARNING: Save what we predicted for future calibration
                    # This allows the system to compare "what we said" vs "what happened"
                    pattern.predicted_probability = pattern.actual_probability

                    # Add temporal info if using temporal pattern
                    if use_temporal:
                        result["temporal_pattern"] = True
                        result["time_bucket"] = condition.get_time_bucket()
                        result["is_weekend"] = condition.is_weekend

                    # Add region info for earthquake events
                    if 'earthquake' in event_type and pattern.event_locations:
                        region = get_most_frequent_region(pattern.event_locations)
                        if region:
                            result["region"] = region

                    results[event_type] = result

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
    
    def _load_price_history(self):
        """Load recent price history from crypto logs for pump/dump detection."""
        try:
            import json
            from pathlib import Path
            
            crypto_dir = Path("logs/crypto")
            if not crypto_dir.exists():
                return
            
            # Load last 3 days of data
            current_time = time.time()
            lookback = 72 * 3600  # 72 hours
            cutoff = current_time - lookback
            
            # Read last 3 log files
            for log_file in sorted(crypto_dir.glob("*.jsonl"), reverse=True)[:3]:
                try:
                    with open(log_file, 'r') as f:
                        for line in f:
                            try:
                                data = json.loads(line.strip())
                                timestamp = data.get('timestamp', 0)
                                
                                if timestamp < cutoff:
                                    continue
                                
                                pairs = data.get('pairs', [])
                                
                                # Extract BTC price
                                for p in pairs:
                                    if p.get('symbol') == 'BTCUSDT':
                                        price = p.get('price', 0)
                                        if price > 0:
                                            self._price_history['btc'].append(timestamp, price)
                                    elif p.get('symbol') == 'ETHUSDT':
                                        price = p.get('price', 0)
                                        if price > 0:
                                            self._price_history['eth'].append(timestamp, price)
                            except json.JSONDecodeError:
                                continue
                except Exception as e:
                    logger.debug(f"Error reading {log_file}: {e}")

            # We walked daily files newest-first so segments arrive out of order.
            # bisect requires non-decreasing timestamps, so sort once at the end.
            self._price_history['btc'].sort()
            self._price_history['eth'].sort()

            btc_count = len(self._price_history['btc'])
            eth_count = len(self._price_history['eth'])
            logger.info(f"Loaded price history: BTC={btc_count}, ETH={eth_count} data points")
            
        except Exception as e:
            logger.error(f"Failed to load price history: {e}")
    
    # Event check methods - REAL IMPLEMENTATIONS
    # Data comes directly from sensors, with 'source' field indicating sensor type
    
    def _check_btc_volatility(self, data: dict, threshold: float) -> bool:
        """Check if BTC volatility exceeded threshold."""
        try:
            source = data.get('source', '')
            if source != 'crypto':
                return False
            
            # Check direct field (btcusdt.price_change_24h_percent)
            price_change = abs(data.get('btcusdt.price_change_24h_percent', 0))
            
            if price_change >= threshold:
                logger.info(f"📊 BTC volatility detected: {price_change:.2f}% >= {threshold}%")
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
            self._price_history[coin_key].append(current_time, current_price)

            # Find price from N hours ago (O(log n) bisect on sorted timestamps;
            # exact same answer as the previous linear walk)
            lookback_seconds = hours * 3600
            target_time = current_time - lookback_seconds

            old_price = self._price_history[coin_key].find_price_at_or_before(target_time)

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
    
    def _check_solar_storm(self, data: dict, min_kp: int = 5) -> bool:
        """Check for solar storm activity.
        
        Kp index scale:
        - Kp 5-6: Minor storm (moderate)
        - Kp 7-8: Strong storm
        - Kp 9: Extreme storm (rare!)
        """
        try:
            source = data.get('source', '')
            if source != 'space_weather':
                return False
            
            # Check for high solar activity
            kp_index = data.get('kp_index', 0)
            solar_wind_speed = data.get('solar_wind_speed', 0)
            
            # Kp index threshold
            if kp_index >= min_kp:
                logger.info(f"☀️ Solar storm detected: Kp={kp_index} (threshold: {min_kp})")
                return True
            
            # Alternative: very high solar wind speed
            if min_kp <= 5 and solar_wind_speed >= 700:
                logger.info(f"☀️ Solar storm detected: wind={solar_wind_speed}km/s")
                return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking solar storm: {e}")
            return False
    
    def save(self):
        """Save all patterns to disk."""
        self._save_patterns()
        logger.info("Patterns saved to disk")
