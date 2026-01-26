"""News Sensor for Matrix Watcher.

Collects news headlines from RSS feeds and calculates text entropy.
"""

import hashlib
import logging
import math
import re
import time
from collections import Counter
from typing import Any
from xml.etree import ElementTree

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus
from ..utils.statistics import shannon_entropy

logger = logging.getLogger(__name__)

DEFAULT_RSS_FEEDS = [
    {"url": "https://feeds.bbci.co.uk/news/rss.xml", "name": "bbc"},
    {"url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "name": "nytimes"},
    {"url": "https://www.theguardian.com/world/rss", "name": "guardian"},
    {"url": "https://feeds.skynews.com/feeds/rss/world.xml", "name": "skynews"},
]


class NewsSensor(BaseSensor):
    """Sensor for collecting news headlines and calculating text entropy.
    
    Collects from RSS feeds:
    - source: Feed name
    - headline: Article title
    - headline_hash: SHA256 hash of headline
    - text_length: Character count
    - word_count: Word count
    - text_entropy: Shannon entropy of text
    
    Example:
        sensor = NewsSensor()
        reading = await sensor.collect()
        for item in reading.data["items"]:
            print(f"{item['source']}: {item['headline'][:50]}...")
    """
    
    def __init__(
        self,
        config: SensorConfig | None = None,
        event_bus: EventBus | None = None,
        feeds: list[dict[str, str]] | None = None,
        max_items_per_feed: int = 10
    ):
        """Initialize News Sensor.
        
        Args:
            config: Sensor configuration
            event_bus: Event bus for publishing
            feeds: List of RSS feeds with 'url' and 'name' keys
            max_items_per_feed: Maximum items to collect per feed
        """
        super().__init__("news", config, event_bus)
        self.feeds = feeds or DEFAULT_RSS_FEEDS
        self.max_items_per_feed = max_items_per_feed
        
        # Track seen headlines to detect new ones
        self._seen_hashes: set[str] = set()
    
    async def collect(self) -> SensorReading:
        """Collect news from all feeds."""
        logger.info("News: Starting data collection")
        timestamp = time.time()
        all_items = []
        feed_stats = []
        new_items_count = 0
        
        async with aiohttp.ClientSession() as session:
            for feed in self.feeds:
                items, stats = await self._collect_feed(session, feed)
                all_items.extend(items)
                feed_stats.append(stats)
                new_items_count += stats.get("new_items", 0)
        
        logger.info(f"News: Collected {len(all_items)} items from {len(self.feeds)} feeds")
        
        # Calculate aggregate entropy
        all_text = " ".join(item["headline"] for item in all_items)
        aggregate_entropy = self._calculate_entropy(all_text) if all_text else 0
        
        data = {
            "timestamp": timestamp,
            "items": all_items,
            "items_count": len(all_items),
            "new_items_count": new_items_count,
            "feeds_stats": feed_stats,
            "feeds_successful": sum(1 for s in feed_stats if s["success"]),
            "feeds_total": len(self.feeds),
            "aggregate_entropy": round(aggregate_entropy, 4),
            # Add headline_count for anomaly detection (same as items_count)
            "headline_count": len(all_items)
        }
        
        logger.info(f"News: Collection complete, {new_items_count} new items")
        return SensorReading.create(self.name, data)
    
    async def _collect_feed(
        self, 
        session: aiohttp.ClientSession, 
        feed: dict[str, str]
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Collect items from a single RSS feed.
        
        Args:
            session: aiohttp session
            feed: Feed dict with 'url' and 'name'
            
        Returns:
            Tuple of (items list, feed stats)
        """
        url = feed["url"]
        name = feed["name"]
        items = []
        
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "MatrixWatcher/1.0"}
            ) as response:
                if response.status != 200:
                    return [], {
                        "feed": name,
                        "success": False,
                        "error": f"HTTP {response.status}",
                        "items_count": 0,
                        "new_items": 0
                    }
                
                content = await response.text()
            
            # Parse RSS
            root = ElementTree.fromstring(content)
            
            # Handle different RSS formats
            channel = root.find("channel")
            if channel is not None:
                item_elements = channel.findall("item")
            else:
                # Atom format
                item_elements = root.findall(".//{http://www.w3.org/2005/Atom}entry")
            
            new_items = 0
            for item_elem in item_elements[:self.max_items_per_feed]:
                item_data = self._parse_item(item_elem, name)
                if item_data:
                    items.append(item_data)
                    if item_data["is_new"]:
                        new_items += 1
            
            return items, {
                "feed": name,
                "success": True,
                "error": None,
                "items_count": len(items),
                "new_items": new_items
            }
            
        except ElementTree.ParseError as e:
            logger.warning(f"Failed to parse RSS from {name}: {e}")
            return [], {
                "feed": name,
                "success": False,
                "error": f"Parse error: {e}",
                "items_count": 0,
                "new_items": 0
            }
        except Exception as e:
            logger.warning(f"Failed to collect from {name}: {e}")
            return [], {
                "feed": name,
                "success": False,
                "error": str(e),
                "items_count": 0,
                "new_items": 0
            }
    
    def _parse_item(
        self, 
        item_elem: ElementTree.Element, 
        source: str
    ) -> dict[str, Any] | None:
        """Parse a single RSS item.
        
        Args:
            item_elem: XML element for the item
            source: Feed source name
            
        Returns:
            Parsed item data or None
        """
        # Try different tag names for title
        title_elem = item_elem.find("title")
        if title_elem is None:
            title_elem = item_elem.find("{http://www.w3.org/2005/Atom}title")
        
        if title_elem is None or not title_elem.text:
            return None
        
        headline = title_elem.text.strip()
        if not headline:
            return None
        
        # Calculate hash
        headline_hash = hashlib.sha256(headline.encode()).hexdigest()[:16]
        
        # Check if new
        is_new = headline_hash not in self._seen_hashes
        self._seen_hashes.add(headline_hash)
        
        # Limit seen hashes to prevent memory growth
        if len(self._seen_hashes) > 10000:
            # Remove oldest half
            self._seen_hashes = set(list(self._seen_hashes)[5000:])
        
        # Calculate text metrics
        text_length = len(headline)
        words = re.findall(r'\w+', headline.lower())
        word_count = len(words)
        entropy = self._calculate_entropy(headline)
        
        return {
            "source": source,
            "headline": headline,
            "headline_hash": headline_hash,
            "text_length": text_length,
            "word_count": word_count,
            "text_entropy": round(entropy, 4),
            "is_new": is_new
        }
    
    def _calculate_entropy(self, text: str) -> float:
        """Calculate Shannon entropy of text.
        
        Args:
            text: Input text
            
        Returns:
            Shannon entropy in bits
        """
        if not text:
            return 0.0
        
        # Character-level entropy
        freq = Counter(text.lower())
        total = len(text)
        
        entropy = 0.0
        for count in freq.values():
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        
        return entropy
    
    def get_schema(self) -> dict[str, type]:
        """Get schema for news sensor data."""
        return {
            "timestamp": float,
            "items": list,
            "items_count": int,
            "new_items_count": int,
            "feeds_successful": int,
            "feeds_total": int,
            "aggregate_entropy": float
        }
