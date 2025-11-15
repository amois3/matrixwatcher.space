"""Alerting System for Matrix Watcher.

Sends notifications via webhooks (Discord, Telegram, Slack).
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class AlertType(Enum):
    """Types of alerts."""
    MULTI_SOURCE_CLUSTER = "multi_source_cluster"
    SIGNIFICANT_CORRELATION = "significant_correlation"
    SENSOR_FAILURE = "sensor_failure"
    ANOMALY_DETECTED = "anomaly_detected"
    RATE_LIMIT = "rate_limit"
    SYSTEM_ERROR = "system_error"


class AlertPriority(Enum):
    """Alert priority levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Alert data structure."""
    alert_type: AlertType
    priority: AlertPriority
    title: str
    message: str
    data: dict[str, Any] | None = None
    timestamp: float | None = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()
    
    @property
    def alert_id(self) -> str:
        """Generate unique ID for deduplication."""
        content = f"{self.alert_type.value}:{self.title}"
        return hashlib.md5(content.encode()).hexdigest()[:12]


@dataclass
class WebhookConfig:
    """Webhook configuration."""
    url: str
    platform: str  # discord, telegram, slack
    enabled: bool = True


class AlertingSystem:
    """System for sending alerts via webhooks.
    
    Features:
    - Support for Discord, Telegram, Slack webhooks
    - Configurable cooldown periods
    - Duplicate alert suppression
    - Priority-based filtering
    
    Example:
        alerting = AlertingSystem()
        alerting.add_webhook("https://discord.com/api/webhooks/...", "discord")
        await alerting.send_alert(Alert(
            alert_type=AlertType.SENSOR_FAILURE,
            priority=AlertPriority.HIGH,
            title="System Sensor Failed",
            message="3 consecutive failures detected"
        ))
    """
    
    def __init__(
        self,
        cooldown_seconds: float = 300.0,
        min_priority: AlertPriority = AlertPriority.LOW
    ):
        """Initialize alerting system.
        
        Args:
            cooldown_seconds: Cooldown between duplicate alerts
            min_priority: Minimum priority to send alerts
        """
        self.cooldown_seconds = cooldown_seconds
        self.min_priority = min_priority
        
        self._webhooks: list[WebhookConfig] = []
        self._last_alerts: dict[str, float] = {}  # alert_id -> timestamp
        self._alert_count = 0
        self._suppressed_count = 0
    
    def add_webhook(self, url: str, platform: str, enabled: bool = True) -> None:
        """Add a webhook endpoint.
        
        Args:
            url: Webhook URL
            platform: Platform type (discord, telegram, slack)
            enabled: Whether webhook is enabled
        """
        self._webhooks.append(WebhookConfig(url=url, platform=platform, enabled=enabled))
        logger.info(f"Added {platform} webhook")
    
    def remove_webhook(self, url: str) -> None:
        """Remove a webhook endpoint.
        
        Args:
            url: Webhook URL to remove
        """
        self._webhooks = [w for w in self._webhooks if w.url != url]
    
    def _should_send(self, alert: Alert) -> bool:
        """Check if alert should be sent.
        
        Args:
            alert: Alert to check
            
        Returns:
            True if alert should be sent
        """
        # Check priority
        priority_order = [AlertPriority.LOW, AlertPriority.MEDIUM, AlertPriority.HIGH, AlertPriority.CRITICAL]
        if priority_order.index(alert.priority) < priority_order.index(self.min_priority):
            return False
        
        # Check cooldown
        alert_id = alert.alert_id
        last_time = self._last_alerts.get(alert_id)
        
        if last_time and (time.time() - last_time) < self.cooldown_seconds:
            self._suppressed_count += 1
            logger.debug(f"Alert suppressed (cooldown): {alert.title}")
            return False
        
        return True
    
    async def send_alert(self, alert: Alert) -> bool:
        """Send an alert to all configured webhooks.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully to at least one webhook
        """
        if not self._should_send(alert):
            return False
        
        if not self._webhooks:
            logger.warning("No webhooks configured")
            return False
        
        # Update last alert time
        self._last_alerts[alert.alert_id] = time.time()
        self._alert_count += 1
        
        # Clean old entries
        self._cleanup_old_alerts()
        
        success = False
        async with aiohttp.ClientSession() as session:
            for webhook in self._webhooks:
                if not webhook.enabled:
                    continue
                
                try:
                    payload = self._format_payload(alert, webhook.platform)
                    async with session.post(
                        webhook.url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        if response.status in (200, 204):
                            success = True
                            logger.info(f"Alert sent to {webhook.platform}: {alert.title}")
                        else:
                            logger.warning(f"Webhook returned {response.status}")
                except Exception as e:
                    logger.error(f"Failed to send to {webhook.platform}: {e}")
        
        return success
    
    def _format_payload(self, alert: Alert, platform: str) -> dict[str, Any]:
        """Format alert payload for specific platform.
        
        Args:
            alert: Alert to format
            platform: Target platform
            
        Returns:
            Formatted payload dictionary
        """
        # Priority emoji
        priority_emoji = {
            AlertPriority.LOW: "ℹ️",
            AlertPriority.MEDIUM: "⚠️",
            AlertPriority.HIGH: "🔴",
            AlertPriority.CRITICAL: "🚨"
        }
        emoji = priority_emoji.get(alert.priority, "📢")
        
        if platform == "discord":
            return {
                "embeds": [{
                    "title": f"{emoji} {alert.title}",
                    "description": alert.message,
                    "color": self._get_color(alert.priority),
                    "fields": [
                        {"name": "Type", "value": alert.alert_type.value, "inline": True},
                        {"name": "Priority", "value": alert.priority.value, "inline": True}
                    ],
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(alert.timestamp))
                }]
            }
        
        elif platform == "slack":
            return {
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": f"{emoji} {alert.title}"}
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": alert.message}
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"*Type:* {alert.alert_type.value}"},
                            {"type": "mrkdwn", "text": f"*Priority:* {alert.priority.value}"}
                        ]
                    }
                ]
            }
        
        elif platform == "telegram":
            # Telegram uses chat_id in URL, just send text
            text = f"{emoji} *{alert.title}*\n\n{alert.message}\n\n_Type: {alert.alert_type.value} | Priority: {alert.priority.value}_"
            return {"text": text, "parse_mode": "Markdown"}
        
        else:
            # Generic JSON
            return {
                "title": alert.title,
                "message": alert.message,
                "type": alert.alert_type.value,
                "priority": alert.priority.value,
                "timestamp": alert.timestamp
            }
    
    def _get_color(self, priority: AlertPriority) -> int:
        """Get Discord embed color for priority."""
        colors = {
            AlertPriority.LOW: 0x3498db,      # Blue
            AlertPriority.MEDIUM: 0xf39c12,   # Orange
            AlertPriority.HIGH: 0xe74c3c,     # Red
            AlertPriority.CRITICAL: 0x9b59b6  # Purple
        }
        return colors.get(priority, 0x95a5a6)
    
    def _cleanup_old_alerts(self) -> None:
        """Remove old alert entries to prevent memory growth."""
        cutoff = time.time() - self.cooldown_seconds * 2
        self._last_alerts = {
            k: v for k, v in self._last_alerts.items()
            if v > cutoff
        }
    
    def get_stats(self) -> dict[str, Any]:
        """Get alerting statistics.
        
        Returns:
            Statistics dictionary
        """
        return {
            "webhooks_configured": len(self._webhooks),
            "webhooks_enabled": sum(1 for w in self._webhooks if w.enabled),
            "alerts_sent": self._alert_count,
            "alerts_suppressed": self._suppressed_count,
            "cooldown_seconds": self.cooldown_seconds,
            "min_priority": self.min_priority.value
        }
    
    # Convenience methods for common alerts
    
    async def alert_sensor_failure(self, sensor_name: str, error: str) -> bool:
        """Send sensor failure alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.SENSOR_FAILURE,
            priority=AlertPriority.HIGH,
            title=f"Sensor Failure: {sensor_name}",
            message=f"Sensor '{sensor_name}' has failed.\n\nError: {error}"
        ))
    
    async def alert_multi_source_cluster(
        self, 
        sources: list[str], 
        anomaly_count: int,
        time_span: float
    ) -> bool:
        """Send multi-source cluster alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.MULTI_SOURCE_CLUSTER,
            priority=AlertPriority.CRITICAL,
            title="Multi-Source Anomaly Cluster Detected",
            message=(
                f"Detected cluster of {anomaly_count} anomalies across {len(sources)} sources "
                f"within {time_span:.1f} seconds.\n\n"
                f"Sources: {', '.join(sources)}"
            ),
            data={"sources": sources, "count": anomaly_count, "span": time_span}
        ))
    
    async def alert_significant_correlation(
        self,
        param1: str,
        param2: str,
        correlation: float
    ) -> bool:
        """Send significant correlation alert."""
        return await self.send_alert(Alert(
            alert_type=AlertType.SIGNIFICANT_CORRELATION,
            priority=AlertPriority.MEDIUM,
            title="Significant Correlation Discovered",
            message=(
                f"Found significant correlation between parameters:\n\n"
                f"• {param1}\n• {param2}\n\n"
                f"Correlation coefficient: {correlation:.3f}"
            ),
            data={"param1": param1, "param2": param2, "correlation": correlation}
        ))
