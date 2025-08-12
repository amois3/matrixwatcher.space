"""Event Bus for Matrix Watcher.

Implements publish-subscribe pattern for event distribution between components.
Supports event filtering, buffering, and multiple subscribers.
"""

import logging
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Any

from .types import Event, EventType, Severity

logger = logging.getLogger(__name__)


@dataclass
class EventFilter:
    """Filter criteria for event subscription.
    
    Attributes:
        event_types: List of event types to receive (None = all)
        sources: List of sources to receive (None = all)
        min_severity: Minimum severity level to receive
    """
    event_types: list[EventType] | None = None
    sources: list[str] | None = None
    min_severity: Severity | None = None
    
    def matches(self, event: Event) -> bool:
        """Check if event matches filter criteria."""
        if self.event_types is not None and event.event_type not in self.event_types:
            return False
        if self.sources is not None and event.source not in self.sources:
            return False
        if self.min_severity is not None:
            severity_order = {Severity.INFO: 0, Severity.WARNING: 1, Severity.CRITICAL: 2}
            if severity_order.get(event.severity, 0) < severity_order.get(self.min_severity, 0):
                return False
        return True


@dataclass
class Subscription:
    """Represents a subscription to the event bus.
    
    Attributes:
        id: Unique subscription identifier
        callback: Function to call with events
        filter: Optional filter criteria
        buffer: Event buffer for slow subscribers
        max_buffer_size: Maximum buffer size before dropping
        dropped_count: Number of events dropped due to buffer overflow
    """
    id: str
    callback: Callable[[Event], None]
    filter: EventFilter | None = None
    buffer: deque = field(default_factory=lambda: deque(maxlen=1000))
    max_buffer_size: int = 1000
    dropped_count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    
    def __post_init__(self):
        # Recreate buffer with correct maxlen
        self.buffer = deque(maxlen=self.max_buffer_size)


class EventBus:
    """Central event distribution mechanism.
    
    Supports:
    - Multiple subscribers per event type
    - Event filtering by type, source, and severity
    - Buffering for slow subscribers (max 1000 events)
    - Thread-safe operations
    
    Example:
        bus = EventBus()
        
        def handler(event):
            print(f"Received: {event}")
        
        sub_id = bus.subscribe(handler)
        bus.publish(Event.create("sensor", EventType.DATA, {"value": 42}))
        bus.unsubscribe(sub_id)
    """
    
    def __init__(self, max_buffer_size: int = 1000):
        """Initialize EventBus.
        
        Args:
            max_buffer_size: Maximum events to buffer per subscriber
        """
        self._subscriptions: dict[str, Subscription] = {}
        self._lock = threading.RLock()
        self._max_buffer_size = max_buffer_size
        self._total_published = 0
        self._total_delivered = 0
        self._total_dropped = 0
    
    def subscribe(
        self,
        callback: Callable[[Event], None],
        event_types: list[EventType] | None = None,
        sources: list[str] | None = None,
        min_severity: Severity | None = None
    ) -> str:
        """Subscribe to events.
        
        Args:
            callback: Function to call with matching events
            event_types: Filter by event types (None = all)
            sources: Filter by sources (None = all)
            min_severity: Minimum severity level
            
        Returns:
            Subscription ID for unsubscribing
        """
        sub_id = str(uuid.uuid4())
        
        event_filter = None
        if event_types or sources or min_severity:
            event_filter = EventFilter(
                event_types=event_types,
                sources=sources,
                min_severity=min_severity
            )
        
        subscription = Subscription(
            id=sub_id,
            callback=callback,
            filter=event_filter,
            max_buffer_size=self._max_buffer_size
        )
        
        with self._lock:
            self._subscriptions[sub_id] = subscription
        
        logger.debug(f"New subscription: {sub_id}")
        return sub_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events.
        
        Args:
            subscription_id: ID returned from subscribe()
            
        Returns:
            True if subscription was found and removed
        """
        with self._lock:
            if subscription_id in self._subscriptions:
                del self._subscriptions[subscription_id]
                logger.debug(f"Unsubscribed: {subscription_id}")
                return True
        return False
    
    def publish(self, event: Event) -> int:
        """Publish an event to all matching subscribers.
        
        Args:
            event: Event to publish
            
        Returns:
            Number of subscribers that received the event
        """
        delivered = 0
        
        with self._lock:
            self._total_published += 1
            subscriptions = list(self._subscriptions.values())
        
        for sub in subscriptions:
            # Check filter
            if sub.filter and not sub.filter.matches(event):
                continue
            
            # Try to deliver
            try:
                sub.callback(event)
                delivered += 1
                self._total_delivered += 1
            except Exception as e:
                logger.error(f"Error in subscriber {sub.id}: {e}")
                # Buffer the event for retry
                with sub._lock:
                    if len(sub.buffer) >= sub.max_buffer_size:
                        # Drop oldest event
                        sub.buffer.popleft()
                        sub.dropped_count += 1
                        self._total_dropped += 1
                    sub.buffer.append(event)
        
        return delivered
    
    def publish_async(self, event: Event) -> None:
        """Publish event asynchronously (non-blocking).
        
        Args:
            event: Event to publish
        """
        thread = threading.Thread(target=self.publish, args=(event,), daemon=True)
        thread.start()
    
    def get_buffer_size(self, subscription_id: str) -> int:
        """Get current buffer size for a subscriber.
        
        Args:
            subscription_id: Subscription ID
            
        Returns:
            Number of buffered events, or -1 if not found
        """
        with self._lock:
            if subscription_id in self._subscriptions:
                return len(self._subscriptions[subscription_id].buffer)
        return -1
    
    def get_dropped_count(self, subscription_id: str) -> int:
        """Get number of dropped events for a subscriber.
        
        Args:
            subscription_id: Subscription ID
            
        Returns:
            Number of dropped events, or -1 if not found
        """
        with self._lock:
            if subscription_id in self._subscriptions:
                return self._subscriptions[subscription_id].dropped_count
        return -1
    
    def flush_buffer(self, subscription_id: str) -> int:
        """Attempt to deliver buffered events.
        
        Args:
            subscription_id: Subscription ID
            
        Returns:
            Number of events successfully delivered
        """
        with self._lock:
            if subscription_id not in self._subscriptions:
                return 0
            sub = self._subscriptions[subscription_id]
        
        delivered = 0
        while True:
            with sub._lock:
                if not sub.buffer:
                    break
                event = sub.buffer[0]
            
            try:
                sub.callback(event)
                with sub._lock:
                    sub.buffer.popleft()
                delivered += 1
            except Exception:
                break
        
        return delivered
    
    def get_stats(self) -> dict[str, Any]:
        """Get event bus statistics.
        
        Returns:
            Dictionary with stats
        """
        with self._lock:
            return {
                "subscriber_count": len(self._subscriptions),
                "total_published": self._total_published,
                "total_delivered": self._total_delivered,
                "total_dropped": self._total_dropped,
            }
    
    def clear(self) -> None:
        """Remove all subscriptions."""
        with self._lock:
            self._subscriptions.clear()
            logger.info("Event bus cleared")
