"""Telegram Bot for Matrix Watcher.

Sends human-readable notifications about anomalies, correlations,
clusters, precursors, and other discoveries.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of messages."""
    ANOMALY = "anomaly"
    CORRELATION = "correlation"
    LAG_CORRELATION = "lag_correlation"
    CLUSTER = "cluster"
    PRECURSOR = "precursor"
    PERIODICITY = "periodicity"
    SENSOR_STATUS = "sensor_status"
    SYSTEM_STATUS = "system_status"


@dataclass
class TelegramConfig:
    """Telegram bot configuration."""
    token: str
    chat_id: str
    enabled: bool = True
    cooldown_seconds: float = 60.0  # Cooldown between similar messages
    

class TelegramBot:
    """Telegram bot for Matrix Watcher notifications.
    
    Sends human-readable messages about:
    - Anomalies detected in real-time
    - Significant correlations between parameters
    - Lag-correlations (cause-effect relationships)
    - Multi-source anomaly clusters
    - Precursor patterns
    - Periodic patterns (FFT analysis)
    """
    
    BASE_URL = "https://api.telegram.org/bot{token}"
    
    def __init__(
        self,
        token: str,
        chat_id: str | None = None,
        cooldown_seconds: float = 60.0
    ):
        """Initialize Telegram bot.
        
        Args:
            token: Bot API token from @BotFather
            chat_id: Chat ID to send messages to (optional, can be set later)
            cooldown_seconds: Minimum time between similar messages
        """
        self.token = token
        self.chat_id = chat_id
        self.cooldown_seconds = cooldown_seconds
        
        self._base_url = self.BASE_URL.format(token=token)
        self._last_messages: dict[str, float] = {}  # message_key -> timestamp
        self._message_count = 0
        self._session: aiohttp.ClientSession | None = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        """Close the session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _should_send(self, message_key: str) -> bool:
        """Check if message should be sent (cooldown check)."""
        last_time = self._last_messages.get(message_key)
        if last_time and (time.time() - last_time) < self.cooldown_seconds:
            return False
        return True
    
    def _record_sent(self, message_key: str):
        """Record that a message was sent."""
        self._last_messages[message_key] = time.time()
        self._message_count += 1
        
        # Cleanup old entries
        cutoff = time.time() - self.cooldown_seconds * 2
        self._last_messages = {k: v for k, v in self._last_messages.items() if v > cutoff}
    
    async def send_message(
        self,
        text: str,
        parse_mode: str | None = "HTML",
        disable_notification: bool = False,
        message_key: str | None = None
    ) -> bool:
        """Send a message to Telegram.
        
        Args:
            text: Message text (HTML or Markdown)
            parse_mode: Parse mode (HTML or Markdown), None for plain text
            disable_notification: Send silently
            message_key: Key for cooldown deduplication
            
        Returns:
            True if sent successfully
        """
        if not self.chat_id:
            logger.warning("No chat_id configured for Telegram bot")
            return False
        
        if message_key and not self._should_send(message_key):
            logger.debug(f"Message suppressed (cooldown): {message_key}")
            return False
        
        try:
            session = await self._get_session()
            url = f"{self._base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "disable_notification": disable_notification
            }
            
            # Only add parse_mode if specified
            if parse_mode:
                payload["parse_mode"] = parse_mode
            
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    if message_key:
                        self._record_sent(message_key)
                    logger.info("Telegram message sent successfully")
                    return True
                else:
                    error = await response.text()
                    logger.error(f"Telegram API error {response.status}: {error}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    async def get_updates(self, offset: int | None = None) -> list[dict]:
        """Get updates (messages) from Telegram.
        
        Used to get chat_id from first message.
        """
        try:
            session = await self._get_session()
            url = f"{self._base_url}/getUpdates"
            params = {}
            if offset:
                params["offset"] = offset
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("result", [])
                return []
        except Exception as e:
            logger.error(f"Failed to get updates: {e}")
            return []
    
    # ==================== Human-Readable Message Formatters ====================
    
    def _format_timestamp(self, ts: float | None = None) -> str:
        """Format timestamp for display."""
        if ts is None:
            ts = time.time()
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M:%S")
    
    def _severity_emoji(self, z_score: float) -> str:
        """Get emoji based on z-score severity."""
        if abs(z_score) >= 6:
            return "🚨"  # Critical
        elif abs(z_score) >= 5:
            return "🔴"  # High
        elif abs(z_score) >= 4:
            return "🟠"  # Medium
        else:
            return "🟡"  # Low
    
    def _source_emoji(self, source: str) -> str:
        """Get emoji for data source."""
        emojis = {
            "system": "💻",
            "time_drift": "⏰",
            "network": "🌐",
            "random": "🎲",
            "crypto": "₿",
            "blockchain": "⛓️",
            "weather": "🌤️",
            "news": "📰"
        }
        return emojis.get(source, "📊")

    async def notify_anomaly(
        self,
        source: str,
        parameter: str,
        value: float,
        z_score: float,
        mean: float,
        std: float,
        timestamp: float | None = None
    ) -> bool:
        """Send anomaly notification.
        
        Args:
            source: Data source (system, crypto, etc.)
            parameter: Parameter name
            value: Anomalous value
            z_score: Z-score of the anomaly
            mean: Historical mean
            std: Historical standard deviation
            timestamp: Event timestamp
        """
        emoji = self._severity_emoji(z_score)
        source_emoji = self._source_emoji(source)
        direction = "выше" if z_score > 0 else "ниже"
        
        text = f"""{emoji} <b>АНОМАЛИЯ ОБНАРУЖЕНА</b>

{source_emoji} <b>Источник:</b> {source}
📊 <b>Параметр:</b> {parameter}

📈 <b>Значение:</b> {value:.4f}
📉 <b>Норма:</b> {mean:.4f} ± {std:.4f}
⚡ <b>Отклонение:</b> {abs(z_score):.2f}σ {direction} нормы

🕐 {self._format_timestamp(timestamp)}"""

        return await self.send_message(
            text,
            message_key=f"anomaly:{source}:{parameter}"
        )
    
    async def notify_correlation(
        self,
        param1: str,
        param2: str,
        correlation: float,
        p_value: float | None = None
    ) -> bool:
        """Send correlation discovery notification.
        
        Args:
            param1: First parameter
            param2: Second parameter
            correlation: Correlation coefficient (-1 to 1)
            p_value: Statistical significance
        """
        if correlation > 0:
            emoji = "📈"
            direction = "положительная"
            meaning = "растут/падают вместе"
        else:
            emoji = "📉"
            direction = "отрицательная"
            meaning = "движутся в противофазе"
        
        strength = abs(correlation)
        if strength >= 0.9:
            strength_text = "Очень сильная"
            strength_emoji = "🔥"
        elif strength >= 0.7:
            strength_text = "Сильная"
            strength_emoji = "💪"
        elif strength >= 0.5:
            strength_text = "Умеренная"
            strength_emoji = "👍"
        else:
            strength_text = "Слабая"
            strength_emoji = "🤔"
        
        text = f"""{emoji} <b>КОРРЕЛЯЦИЯ ОБНАРУЖЕНА</b>

{strength_emoji} <b>Сила связи:</b> {strength_text} ({direction})

🔗 <b>Параметры:</b>
  • {param1}
  • {param2}

📊 <b>Коэффициент:</b> {correlation:.3f}
💡 <b>Интерпретация:</b> Параметры {meaning}"""

        if p_value is not None:
            significance = "статистически значима" if p_value < 0.05 else "может быть случайной"
            text += f"\n📐 <b>p-value:</b> {p_value:.4f} (связь {significance})"
        
        text += f"\n\n🕐 {self._format_timestamp()}"
        
        return await self.send_message(
            text,
            message_key=f"correlation:{param1}:{param2}"
        )
    
    async def notify_lag_correlation(
        self,
        param1: str,
        param2: str,
        lag_seconds: int,
        correlation: float,
        is_causal: bool = False
    ) -> bool:
        """Send lag-correlation (potential causality) notification.
        
        Args:
            param1: First parameter (potential cause)
            param2: Second parameter (potential effect)
            lag_seconds: Time lag in seconds
            correlation: Correlation at this lag
            is_causal: Whether this suggests causality
        """
        if is_causal:
            emoji = "⚡"
            title = "ПРИЧИННО-СЛЕДСТВЕННАЯ СВЯЗЬ"
        else:
            emoji = "🔄"
            title = "LAG-КОРРЕЛЯЦИЯ"
        
        if lag_seconds > 0:
            direction = f"{param1} → {param2}"
            timing = f"{param1} предшествует {param2} на {abs(lag_seconds)} сек"
        else:
            direction = f"{param2} → {param1}"
            timing = f"{param2} предшествует {param1} на {abs(lag_seconds)} сек"
        
        text = f"""{emoji} <b>{title}</b>

🔗 <b>Направление:</b> {direction}
⏱️ <b>Задержка:</b> {abs(lag_seconds)} секунд
📊 <b>Корреляция:</b> {correlation:.3f}

💡 <b>Интерпретация:</b>
{timing}

🎯 Это может указывать на причинно-следственную связь!

🕐 {self._format_timestamp()}"""

        return await self.send_message(
            text,
            message_key=f"lag:{param1}:{param2}:{lag_seconds}"
        )
    
    async def notify_cluster(
        self,
        sources: list[str],
        anomaly_count: int,
        time_span_seconds: float,
        timestamp: float | None = None
    ) -> bool:
        """Send multi-source anomaly cluster notification.
        
        Args:
            sources: List of affected sources
            anomaly_count: Number of anomalies in cluster
            time_span_seconds: Time span of cluster
            timestamp: Cluster timestamp
        """
        source_list = "\n".join(f"  • {self._source_emoji(s)} {s}" for s in sources)
        
        text = f"""🚨 <b>КЛАСТЕР АНОМАЛИЙ</b>

⚠️ <b>Множественные аномалии одновременно!</b>

📊 <b>Количество:</b> {anomaly_count} аномалий
⏱️ <b>Временное окно:</b> {time_span_seconds:.1f} сек
🎯 <b>Затронуто источников:</b> {len(sources)}

📡 <b>Источники:</b>
{source_list}

💡 <b>Интерпретация:</b>
Одновременные аномалии в разных системах могут указывать на:
• Глобальное событие
• Системный сбой
• Скрытую взаимосвязь

🕐 {self._format_timestamp(timestamp)}"""

        return await self.send_message(
            text,
            message_key=f"cluster:{len(sources)}:{anomaly_count}"
        )
    
    async def notify_precursor(
        self,
        precursor_param: str,
        target_param: str,
        lead_time_seconds: int,
        frequency: float,
        confidence: float
    ) -> bool:
        """Send precursor pattern notification.
        
        Args:
            precursor_param: Parameter that precedes
            target_param: Parameter that follows
            lead_time_seconds: How many seconds before
            frequency: How often this pattern occurs (0-1)
            confidence: Confidence level (0-1)
        """
        confidence_emoji = "🎯" if confidence >= 0.7 else "🔮" if confidence >= 0.5 else "❓"
        
        text = f"""{confidence_emoji} <b>ПРЕДВЕСТНИК ОБНАРУЖЕН</b>

🔮 <b>Паттерн:</b>
Изменения в <b>{precursor_param}</b> предшествуют
изменениям в <b>{target_param}</b>

⏱️ <b>Опережение:</b> {lead_time_seconds} секунд
📊 <b>Частота:</b> {frequency*100:.1f}% случаев
🎯 <b>Уверенность:</b> {confidence*100:.1f}%

💡 <b>Применение:</b>
Отслеживайте {precursor_param} для раннего
предупреждения об изменениях в {target_param}

🕐 {self._format_timestamp()}"""

        return await self.send_message(
            text,
            message_key=f"precursor:{precursor_param}:{target_param}"
        )
    
    async def notify_periodicity(
        self,
        parameter: str,
        period_seconds: float,
        strength: float
    ) -> bool:
        """Send periodicity detection notification.
        
        Args:
            parameter: Parameter with periodic behavior
            period_seconds: Period in seconds
            strength: Strength of periodicity (0-1)
        """
        # Convert to human-readable period
        if period_seconds < 60:
            period_text = f"{period_seconds:.1f} сек"
        elif period_seconds < 3600:
            period_text = f"{period_seconds/60:.1f} мин"
        elif period_seconds < 86400:
            period_text = f"{period_seconds/3600:.1f} час"
        else:
            period_text = f"{period_seconds/86400:.1f} дней"
        
        strength_emoji = "🔥" if strength >= 0.8 else "💪" if strength >= 0.6 else "👍"
        
        text = f"""🔄 <b>ПЕРИОДИЧНОСТЬ ОБНАРУЖЕНА</b>

📊 <b>Параметр:</b> {parameter}
⏱️ <b>Период:</b> {period_text}
{strength_emoji} <b>Сила:</b> {strength*100:.1f}%

💡 <b>Интерпретация:</b>
Параметр демонстрирует циклическое поведение
с периодом ~{period_text}

🕐 {self._format_timestamp()}"""

        return await self.send_message(
            text,
            message_key=f"periodicity:{parameter}:{int(period_seconds)}"
        )
    
    async def notify_startup(self, sensors: list[str]) -> bool:
        """Send startup notification."""
        sensor_list = "\n".join(f"  • {self._source_emoji(s)} {s}" for s in sensors)
        
        text = f"""🚀 <b>MATRIX WATCHER ЗАПУЩЕН</b>

✅ <b>Активные сенсоры:</b>
{sensor_list}

🔍 Система начала мониторинг цифровой реальности.
Вы будете получать уведомления о:
• 🔴 Аномалиях
• 🔗 Корреляциях
• ⚡ Причинно-следственных связях
• 🚨 Кластерах аномалий
• 🔮 Предвестниках
• 🔄 Периодических паттернах

🕐 {self._format_timestamp()}"""

        return await self.send_message(text, disable_notification=False)
    
    async def notify_shutdown(self, reason: str = "Штатное завершение") -> bool:
        """Send shutdown notification."""
        text = f"""⏹️ <b>MATRIX WATCHER ОСТАНОВЛЕН</b>

📝 <b>Причина:</b> {reason}
📊 <b>Отправлено сообщений:</b> {self._message_count}

🕐 {self._format_timestamp()}"""

        return await self.send_message(text, disable_notification=True)
    
    async def send_daily_summary(
        self,
        anomaly_count: int,
        correlation_count: int,
        cluster_count: int,
        top_anomalies: list[dict] | None = None
    ) -> bool:
        """Send daily summary."""
        text = f"""📋 <b>ЕЖЕДНЕВНЫЙ ОТЧЁТ</b>

📊 <b>Статистика за 24 часа:</b>
  • 🔴 Аномалий: {anomaly_count}
  • 🔗 Корреляций: {correlation_count}
  • 🚨 Кластеров: {cluster_count}
"""
        
        if top_anomalies:
            text += "\n🔝 <b>Топ аномалий:</b>\n"
            for i, a in enumerate(top_anomalies[:5], 1):
                source = a.get("source", "unknown")
                param = a.get("parameter", "unknown")
                z = a.get("z_score", 0)
                text += f"  {i}. {self._source_emoji(source)} {param}: {abs(z):.1f}σ\n"
        
        text += f"\n🕐 {self._format_timestamp()}"
        
        return await self.send_message(text)
    
    def get_stats(self) -> dict[str, Any]:
        """Get bot statistics."""
        return {
            "messages_sent": self._message_count,
            "cooldown_seconds": self.cooldown_seconds,
            "chat_id_configured": self.chat_id is not None
        }
