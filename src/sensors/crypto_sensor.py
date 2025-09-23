"""Crypto Market Sensor for Matrix Watcher.

Collects cryptocurrency market data from Binance API.
"""

import logging
import time
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)

DEFAULT_PAIRS = ["BTCUSDT", "ETHUSDT"]
BINANCE_API_BASE = "https://api.binance.com/api/v3"


class CryptoSensor(BaseSensor):
    """Sensor for collecting cryptocurrency market data.
    
    Uses Binance API to collect:
    - price: Current price
    - best_bid, best_ask: Order book top
    - spread: Bid-ask spread
    - volume_24h: 24-hour trading volume
    - price_delta_percent: Price change from last reading
    
    Implements exponential backoff on rate limiting.
    
    Example:
        sensor = CryptoSensor(pairs=["BTCUSDT", "ETHUSDT"])
        reading = await sensor.collect()
        for pair_data in reading.data["pairs"]:
            print(f"{pair_data['symbol']}: ${pair_data['price']}")
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        pairs: list[str] | None = None,
        price_change_threshold: float = 1.0
    ):
        """Initialize Crypto Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            pairs: Trading pairs to monitor (default: BTCUSDT, ETHUSDT)
            price_change_threshold: Threshold for flagging significant changes (%)
        """
        super().__init__("crypto", config, event_bus)
        self.pairs = pairs or DEFAULT_PAIRS
        self.price_change_threshold = price_change_threshold
        
        # Track last prices for delta calculation
        self._last_prices: dict[str, float] = {}
        
        # Rate limiting
        self._backoff_until: float = 0
        self._backoff_multiplier: float = 1.0
    
    async def collect(self) -> SensorReading:
        """Collect market data for all pairs."""
        timestamp = time.time()
        
        # Check rate limit backoff
        if timestamp < self._backoff_until:
            logger.warning(f"Rate limited, waiting until {self._backoff_until}")
            return SensorReading.create(self.name, {
                "timestamp": timestamp,
                "pairs": [],
                "rate_limited": True,
                "error": "Rate limited"
            })
        
        pairs_data = []
        any_significant_change = False
        
        async with aiohttp.ClientSession() as session:
            for symbol in self.pairs:
                pair_data = await self._collect_pair(session, symbol)
                if pair_data:
                    pairs_data.append(pair_data)
                    if pair_data.get("significant_change"):
                        any_significant_change = True
        
        # Reset backoff on success
        if pairs_data:
            self._backoff_multiplier = 1.0
        
        # Build data with both array format (for storage) and flat format (for detection)
        data = {
            "timestamp": timestamp,
            "pairs": pairs_data,
            "pairs_count": len(pairs_data),
            "any_significant_change": any_significant_change,
            "rate_limited": False
        }
        
        # Add flat fields for each pair (for anomaly detection)
        for pair_data in pairs_data:
            symbol = pair_data["symbol"].lower()
            data[f"{symbol}.price"] = pair_data["price"]
            data[f"{symbol}.volume_24h"] = pair_data["volume_24h"]
            data[f"{symbol}.price_change_24h_percent"] = pair_data["price_change_24h_percent"]
            data[f"{symbol}.price_delta_percent"] = pair_data["price_delta_percent"]
            data[f"{symbol}.significant_change"] = pair_data["significant_change"]
        
        return SensorReading.create(self.name, data)
    
    async def _collect_pair(
        self, 
        session: aiohttp.ClientSession, 
        symbol: str
    ) -> dict[str, Any] | None:
        """Collect data for a single trading pair.
        
        Args:
            session: aiohttp session
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            
        Returns:
            Pair data dictionary or None on error
        """
        try:
            # Get ticker data
            ticker_url = f"{BINANCE_API_BASE}/ticker/24hr?symbol={symbol}"
            book_url = f"{BINANCE_API_BASE}/ticker/bookTicker?symbol={symbol}"
            
            async with session.get(ticker_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    self._handle_rate_limit()
                    return None
                if resp.status != 200:
                    logger.warning(f"Binance ticker API returned {resp.status}")
                    return None
                ticker = await resp.json()
            
            async with session.get(book_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    self._handle_rate_limit()
                    return None
                if resp.status != 200:
                    logger.warning(f"Binance book API returned {resp.status}")
                    return None
                book = await resp.json()
            
            price = float(ticker["lastPrice"])
            best_bid = float(book["bidPrice"])
            best_ask = float(book["askPrice"])
            spread = best_ask - best_bid
            volume_24h = float(ticker["volume"])
            
            # Calculate price delta
            last_price = self._last_prices.get(symbol)
            if last_price and last_price > 0:
                price_delta_percent = ((price - last_price) / last_price) * 100
            else:
                price_delta_percent = 0.0
            
            self._last_prices[symbol] = price
            
            significant_change = abs(price_delta_percent) > self.price_change_threshold
            
            return {
                "symbol": symbol,
                "price": price,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": round(spread, 8),
                "spread_percent": round((spread / price) * 100, 6) if price > 0 else 0,
                "volume_24h": volume_24h,
                "price_change_24h_percent": float(ticker["priceChangePercent"]),
                "price_delta_percent": round(price_delta_percent, 4),
                "significant_change": significant_change,
                "trade_count_24h": int(ticker["count"])
            }
            
        except aiohttp.ClientError as e:
            logger.warning(f"Failed to collect {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error collecting {symbol}: {e}")
            return None
    
    def _handle_rate_limit(self) -> None:
        """Handle rate limiting with exponential backoff."""
        self._backoff_multiplier = min(self._backoff_multiplier * 2, 60)
        self._backoff_until = time.time() + self._backoff_multiplier
        logger.warning(f"Rate limited, backing off for {self._backoff_multiplier}s")
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for crypto sensor data."""
        return {
            "timestamp": float,
            "pairs": list,
            "pairs_count": int,
            "any_significant_change": bool,
            "rate_limited": bool
        }
