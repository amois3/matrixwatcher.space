"""Smart Analyzer - Intelligent anomaly analysis with explanations.

Provides:
- Real-time correlation detection
- Precursor analysis (what happened before anomaly)
- Human-readable explanations
- Context-aware insights
"""

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from ...core.types import AnomalyEvent, Event

logger = logging.getLogger(__name__)


@dataclass
class SmartInsight:
    """Intelligent insight about an anomaly."""
    anomaly: AnomalyEvent
    explanation: str
    possible_causes: list[str]
    correlations: list[dict[str, Any]]
    precursors: list[dict[str, Any]]
    severity_level: str  # "low", "medium", "high", "critical"
    
    def to_telegram_message(self) -> str:
        """Generate human-readable Telegram message."""
        # Emoji based on severity
        emoji_map = {
            "low": "🟡",
            "medium": "🟠", 
            "high": "🔴",
            "critical": "🚨"
        }
        emoji = emoji_map.get(self.severity_level, "🔴")
        
        # Sensor name mapping
        sensor_names = {
            "crypto": "₿ Криптовалюты",
            "network": "🌐 Сеть",
            "time_drift": "⏰ Время",
            "news": "📰 Новости",
            "blockchain": "⛓️ Блокчейн",
            "weather": "🌤️ Погода",
            "random": "🎲 Случайность"
        }
        
        sensor_name = sensor_names.get(self.anomaly.sensor_source, self.anomaly.sensor_source)
        
        msg = f"{emoji} <b>АНОМАЛИЯ: {sensor_name}</b>\n\n"
        msg += f"📊 <b>Что произошло:</b>\n{self.explanation}\n\n"
        
        if self.possible_causes:
            msg += f"🤔 <b>Возможные причины:</b>\n"
            for cause in self.possible_causes[:3]:
                msg += f"• {cause}\n"
            msg += "\n"
        
        if self.correlations:
            msg += f"🔗 <b>Связанные события:</b>\n"
            for corr in self.correlations[:3]:
                msg += f"• {corr['description']}\n"
            msg += "\n"
        
        if self.precursors:
            msg += f"⏱️ <b>Предвестники (за 60 сек):</b>\n"
            for prec in self.precursors[:3]:
                msg += f"• {prec['description']}\n"
            msg += "\n"
        elif not self.correlations:
            msg += f"⏱️ <b>Предвестники:</b> Не обнаружено\n\n"
        
        # Technical details
        msg += f"📈 <b>Детали:</b>\n"
        msg += f"• Параметр: <code>{self.anomaly.parameter}</code>\n"
        msg += f"• Значение: <code>{self.anomaly.value:.2f}</code>\n"
        msg += f"• Z-score: <code>{self.anomaly.z_score:.2f}</code>\n"
        
        return msg


class SmartAnalyzer:
    """Intelligent analyzer for anomalies with explanations."""
    
    def __init__(
        self,
        lookback_seconds: int = 60,
        correlation_threshold: float = 0.7,
        precursor_threshold: float = 0.3
    ):
        """Initialize smart analyzer.
        
        Args:
            lookback_seconds: How far back to look for precursors
            correlation_threshold: Minimum correlation to report
            precursor_threshold: Minimum correlation for precursor
        """
        self.lookback_seconds = lookback_seconds
        self.correlation_threshold = correlation_threshold
        self.precursor_threshold = precursor_threshold
        
        # Store recent events for analysis
        self._recent_events: deque = deque(maxlen=1000)
        self._recent_anomalies: deque = deque(maxlen=100)
        
        # Parameter history for correlation
        self._parameter_history: dict[str, deque] = {}
    
    def record_event(self, event: Event) -> None:
        """Record an event for future analysis."""
        self._recent_events.append({
            "timestamp": event.timestamp,
            "source": event.source,
            "payload": event.payload
        })
        
        # Track numeric parameters
        for key, value in event.payload.items():
            if isinstance(value, (int, float)):
                param_key = f"{event.source}.{key}"
                if param_key not in self._parameter_history:
                    self._parameter_history[param_key] = deque(maxlen=200)
                self._parameter_history[param_key].append({
                    "timestamp": event.timestamp,
                    "value": float(value)
                })
    
    def record_anomaly(self, anomaly: AnomalyEvent) -> None:
        """Record an anomaly."""
        self._recent_anomalies.append({
            "timestamp": anomaly.timestamp,
            "source": anomaly.sensor_source,
            "parameter": anomaly.parameter,
            "value": anomaly.value,
            "z_score": anomaly.z_score
        })
    
    def analyze(self, anomaly: AnomalyEvent) -> SmartInsight:
        """Analyze anomaly and generate insights."""
        # Generate explanation
        explanation = self._generate_explanation(anomaly)
        
        # Find possible causes
        causes = self._find_causes(anomaly)
        
        # Detect correlations
        correlations = self._detect_correlations(anomaly)
        
        # Find precursors
        precursors = self._find_precursors(anomaly)
        
        # Determine severity
        severity = self._determine_severity(anomaly, correlations, precursors)
        
        return SmartInsight(
            anomaly=anomaly,
            explanation=explanation,
            possible_causes=causes,
            correlations=correlations,
            precursors=precursors,
            severity_level=severity
        )
    
    def _generate_explanation(self, anomaly: AnomalyEvent) -> str:
        """Generate human-readable explanation."""
        # Check if this is from threshold detector
        if anomaly.metadata and "reason" in anomaly.metadata:
            return anomaly.metadata["reason"]
        
        source = anomaly.sensor_source
        param = anomaly.parameter.split(".")[-1]  # Get last part
        value = anomaly.value
        z = anomaly.z_score
        mean = anomaly.mean
        
        direction = "выше" if z > 0 else "ниже"
        change_pct = abs((value - mean) / mean * 100) if mean != 0 else 0
        
        # Source-specific explanations
        if source == "crypto":
            if "price" in param.lower():
                return f"Цена криптовалюты резко изменилась: {value:.2f} ({direction} среднего на {change_pct:.1f}%)"
            elif "volume" in param.lower():
                return f"Объём торгов аномален: {value:.0f} ({direction} среднего на {change_pct:.1f}%)"
        
        elif source == "network":
            if "latency" in param.lower() or "ping" in param.lower():
                return f"Задержка сети: {value:.1f}ms ({direction} среднего на {change_pct:.1f}%)"
            elif "packet_loss" in param.lower():
                return f"Потеря пакетов: {value:.1f}% (аномально высокая)"
        
        elif source == "time_drift":
            return f"Рассинхронизация времени: {value:.1f}ms ({direction} нормы)"
        
        elif source == "news":
            return f"Всплеск новостей: {int(value)} заголовков (обычно {int(mean)})"
        
        elif source == "blockchain":
            if "block_time" in param.lower():
                return f"Время блока: {value:.1f}s (ожидалось {mean:.1f}s)"
        
        elif source == "weather":
            if "temperature" in param.lower():
                return f"Температура: {value:.1f}°C ({direction} среднего на {change_pct:.1f}%)"
            elif "pressure" in param.lower():
                return f"Давление: {value:.1f} hPa (резкое изменение)"
        
        elif source == "random":
            return f"Генератор случайных чисел ведёт себя неслучайно (отклонение {abs(z):.1f}σ)"
        
        # Generic explanation
        return f"{param} = {value:.2f} ({direction} среднего {mean:.2f} на {abs(z):.1f} стандартных отклонений)"
    
    def _find_causes(self, anomaly: AnomalyEvent) -> list[str]:
        """Find possible causes for anomaly."""
        source = anomaly.sensor_source
        param = anomaly.parameter
        z = anomaly.z_score
        
        causes = []
        
        if source == "crypto":
            causes = [
                "Крупная сделка на бирже",
                "Новости о регулировании",
                "Манипуляция рынком",
                "Технический сбой биржи"
            ]
        
        elif source == "network":
            causes = [
                "Проблемы у интернет-провайдера",
                "DDoS атака на целевой сервер",
                "Перегрузка сети",
                "Проблемы маршрутизации"
            ]
        
        elif source == "time_drift":
            causes = [
                "Проблемы с NTP сервером",
                "Системные часы отстают/спешат",
                "Сетевая задержка до NTP",
                "Изменение часового пояса"
            ]
        
        elif source == "news":
            causes = [
                "Крупное событие в мире",
                "Политический кризис",
                "Природная катастрофа",
                "Технологический прорыв"
            ]
        
        elif source == "blockchain":
            causes = [
                "Майнер нашёл блок быстрее обычного",
                "Изменение сложности сети",
                "Атака 51%",
                "Форк блокчейна"
            ]
        
        elif source == "weather":
            causes = [
                "Приближение циклона/антициклона",
                "Резкое изменение температуры",
                "Атмосферный фронт",
                "Ошибка метеостанции"
            ]
        
        elif source == "random":
            causes = [
                "Проблема с генератором случайных чисел",
                "Детерминированный паттерн",
                "Квантовая аномалия (маловероятно)",
                "Ошибка в алгоритме"
            ]
        
        return causes
    
    def _detect_correlations(self, anomaly: AnomalyEvent) -> list[dict[str, Any]]:
        """Detect correlations with other parameters at same time."""
        correlations = []
        anomaly_time = anomaly.timestamp
        time_window = 10.0  # seconds
        
        # Find events near anomaly time
        for param_key, history in self._parameter_history.items():
            if param_key == anomaly.parameter:
                continue
            
            # Get values near anomaly time
            nearby_values = [
                h["value"] for h in history
                if abs(h["timestamp"] - anomaly_time) < time_window
            ]
            
            if len(nearby_values) < 3:
                continue
            
            # Check if this parameter also changed significantly
            recent_mean = sum(nearby_values) / len(nearby_values)
            all_values = [h["value"] for h in history]
            
            if len(all_values) < 10:
                continue
            
            overall_mean = sum(all_values) / len(all_values)
            
            # Calculate change
            change_pct = abs((recent_mean - overall_mean) / overall_mean * 100) if overall_mean != 0 else 0
            
            if change_pct > 5:  # 5% change
                source = param_key.split(".")[0]
                param_name = param_key.split(".")[-1]
                
                correlations.append({
                    "parameter": param_key,
                    "change_percent": change_pct,
                    "description": f"{source}: {param_name} изменился на {change_pct:.1f}%"
                })
        
        # Sort by change magnitude
        correlations.sort(key=lambda x: x["change_percent"], reverse=True)
        
        return correlations
    
    def _find_precursors(self, anomaly: AnomalyEvent) -> list[dict[str, Any]]:
        """Find events that preceded the anomaly."""
        precursors = []
        anomaly_time = anomaly.timestamp
        lookback_start = anomaly_time - self.lookback_seconds
        
        # Look for anomalies in the lookback window
        for past_anomaly in self._recent_anomalies:
            if past_anomaly["timestamp"] < lookback_start:
                continue
            if past_anomaly["timestamp"] >= anomaly_time:
                continue
            if past_anomaly["parameter"] == anomaly.parameter:
                continue
            
            time_before = anomaly_time - past_anomaly["timestamp"]
            
            precursors.append({
                "parameter": past_anomaly["parameter"],
                "time_before_seconds": time_before,
                "z_score": past_anomaly["z_score"],
                "description": f"{past_anomaly['source']}: аномалия за {int(time_before)}с до этого"
            })
        
        # Sort by time (most recent first)
        precursors.sort(key=lambda x: x["time_before_seconds"])
        
        return precursors
    
    def _determine_severity(
        self,
        anomaly: AnomalyEvent,
        correlations: list[dict],
        precursors: list[dict]
    ) -> str:
        """Determine severity level."""
        z = abs(anomaly.z_score)
        
        # Base severity on z-score
        if z > 10:
            base_severity = "critical"
        elif z > 7:
            base_severity = "high"
        elif z > 5:
            base_severity = "medium"
        else:
            base_severity = "low"
        
        # Upgrade if there are correlations or precursors
        if len(correlations) >= 2 or len(precursors) >= 1:
            if base_severity == "medium":
                base_severity = "high"
            elif base_severity == "low":
                base_severity = "medium"
        
        return base_severity
