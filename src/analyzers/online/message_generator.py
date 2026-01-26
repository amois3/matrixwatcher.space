"""Message Generator - Creates beautiful Telegram messages.

Generates human-readable messages for different anomaly levels.
"""

import logging
from datetime import datetime
from typing import Any

from ...core.types import AnomalyEvent
from .cluster_detector import AnomalyCluster

logger = logging.getLogger(__name__)


class MessageGenerator:
    """Generates formatted messages for Telegram."""
    
    # Emoji mapping
    SENSOR_EMOJI = {
        "crypto": "₿",
        "earthquake": "🌍",
        "space_weather": "☀️",
        "quantum_rng": "🎲",
        "weather": "🌤️",
        "news": "📰",
        "blockchain": "⛓️"
    }
    
    LEVEL_EMOJI = {
        1: "🟡",
        2: "🟠",
        3: "🔴",
        4: "🚨",
        5: "⚡"
    }
    
    def generate_message(self, cluster: AnomalyCluster) -> str:
        """Generate message based on cluster level."""
        if cluster.level == 1:
            return self._generate_level1(cluster)
        elif cluster.level == 2:
            return self._generate_level2(cluster)
        elif cluster.level == 3:
            return self._generate_level3(cluster)
        elif cluster.level == 4:
            return self._generate_level4(cluster)
        elif cluster.level == 5:
            return self._generate_level5(cluster)
        else:
            return "Unknown anomaly level"
    
    def _generate_level1(self, cluster: AnomalyCluster) -> str:
        """Level 1: Single anomaly - short message."""
        anomaly = cluster.anomalies[0]
        emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
        
        # Get human-readable description
        desc = self._get_anomaly_description(anomaly)
        
        msg = f"{self.LEVEL_EMOJI[1]} <b>АНОМАЛИЯ</b>\n\n"
        msg += f"{emoji} <b>{self._get_sensor_name(anomaly.sensor_source)}</b>\n"
        msg += f"{desc}\n\n"
        msg += f"🕐 {self._format_time(anomaly.timestamp)}\n"
        msg += f"📊 Мониторим другие системы..."
        
        return msg
    
    def _generate_level2(self, cluster: AnomalyCluster) -> str:
        """Level 2: Two systems - medium message."""
        msg = f"{self.LEVEL_EMOJI[2]} <b>КОРРЕЛЯЦИЯ (2 системы)</b>\n\n"
        
        for i, anomaly in enumerate(cluster.anomalies, 1):
            emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
            desc = self._get_anomaly_description(anomaly)
            msg += f"{i}️⃣ {emoji} {desc}\n"
        
        msg += f"\n⏱️ Разница: {self._get_time_diff(cluster.anomalies)}\n"
        msg += f"🤔 Вероятность случайности: {cluster.probability:.2f}%\n\n"
        msg += "Возможно совпадение, но интересно.\n"
        msg += "Продолжаем наблюдение."
        
        return msg
    
    def _generate_level3(self, cluster: AnomalyCluster) -> str:
        """Level 3: Three systems - detailed message."""
        msg = f"{self.LEVEL_EMOJI[3]} <b>КЛАСТЕР (3 системы)</b>\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for i, anomaly in enumerate(cluster.anomalies, 1):
            emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
            desc = self._get_anomaly_description(anomaly)
            msg += f"{i}️⃣ {emoji} {desc}\n"
        
        msg += f"\n⏱️ Все в окне {self._get_time_diff(cluster.anomalies)}\n"
        msg += f"🤔 Вероятность случайности: {cluster.probability:.3f}%\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "💭 <b>Возможные объяснения:</b>\n"
        msg += self._get_explanations(cluster.anomalies)
        msg += "\n\nЭто интересно! 🧐"
        
        return msg
    
    def _generate_level4(self, cluster: AnomalyCluster) -> str:
        """Level 4: Four+ systems - full analysis."""
        msg = f"{self.LEVEL_EMOJI[4]} <b>КРИТИЧЕСКАЯ АНОМАЛИЯ УРОВНЯ 4</b>\n\n"
        msg += "⚡ <b>СИНХРОНИЗАЦИЯ НЕСВЯЗАННЫХ СИСТЕМ</b>\n\n"
        msg += f"🕐 {self._format_time(cluster.timestamp)}\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "📊 <b>ЧТО ПРОИЗОШЛО:</b>\n\n"
        msg += f"В течение {self._get_time_diff(cluster.anomalies)} одновременно\n"
        msg += f"сработали {len(cluster.anomalies)} независимых систем:\n\n"
        
        for i, anomaly in enumerate(cluster.anomalies, 1):
            emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
            desc = self._get_anomaly_description(anomaly)
            msg += f"{i}️⃣ {emoji} {desc}\n"
            msg += f"   └─ {self._get_anomaly_details(anomaly)}\n\n"
        
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "🤔 <b>АНАЛИЗ:</b>\n\n"
        msg += "Эти события НЕ должны быть связаны:\n"
        msg += self._get_independence_explanation(cluster.anomalies)
        msg += "\n\nНо они произошли ОДНОВРЕМЕННО.\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "🔬 <b>ВОЗМОЖНЫЕ ОБЪЯСНЕНИЯ:</b>\n\n"
        msg += self._get_detailed_explanations(cluster)
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += f"🎯 <b>СТАТИСТИКА:</b>\n\n"
        msg += f"Вероятность такого кластера: 1 к {int(1/max(cluster.probability/100, 0.0001)):,}\n"
        msg += f"Уровень критичности: ВЫСОКИЙ\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "💭 <b>ВЫВОД:</b>\n\n"
        msg += "Либо мы наблюдаем редкое совпадение,\n"
        msg += "либо существует неизвестная науке связь\n"
        msg += "между этими системами.\n\n"
        msg += "Продолжаем наблюдение. 👁️"
        
        return msg
    
    def _generate_level5(self, cluster: AnomalyCluster) -> str:
        """Level 5: Precursor - special message."""
        precursor = cluster.precursor_event
        event = cluster.anomalies[-1]
        
        time_diff = event.timestamp - precursor.timestamp
        
        msg = f"{self.LEVEL_EMOJI[5]} <b>ПРЕДВЕСТНИК ОБНАРУЖЕН</b>\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        emoji1 = self.SENSOR_EMOJI.get(precursor.sensor_source, "🔍")
        emoji2 = self.SENSOR_EMOJI.get(event.sensor_source, "🔍")
        
        msg += f"{emoji1} {self._get_anomaly_description(precursor)}\n"
        msg += f"   🕐 {self._format_time(precursor.timestamp)}\n\n"
        msg += "        ⬇️\n"
        msg += f"   ⏱️ {int(time_diff/60)} минут спустя\n"
        msg += "        ⬇️\n\n"
        msg += f"{emoji2} {self._get_anomaly_description(event)}\n"
        msg += f"   🕐 {self._format_time(event.timestamp)}\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "🤔 <b>ЧТО ЭТО ЗНАЧИТ?</b>\n\n"
        msg += "Система в <b>несвязанной</b> области показала\n"
        msg += "аномалию ДО основного события.\n\n"
        msg += "Возможные объяснения:\n"
        msg += "1️⃣ Случайное совпадение\n"
        msg += "2️⃣ Общая скрытая причина\n"
        msg += "3️⃣ Ретрокаузальность (будущее влияет на прошлое)\n"
        msg += "4️⃣ Квантовая запутанность систем\n\n"
        msg += f"🎯 Вероятность случайности: {cluster.probability:.2f}%\n\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += "🔮 Это может быть признаком того, что\n"
        msg += "системы связаны на более глубоком уровне,\n"
        msg += "чем мы понимаем.\n\n"
        msg += "Продолжаем мониторинг. 👁️"
        
        return msg
    
    def _get_sensor_name(self, source: str) -> str:
        """Get human-readable sensor name."""
        names = {
            "crypto": "Криптовалюты",
            "earthquake": "Землетрясения",
            "space_weather": "Космическая погода",
            "quantum_rng": "Квантовая случайность",
            "weather": "Погода",
            "news": "Новости",
            "blockchain": "Блокчейн"
        }
        return names.get(source, source)
    
    def _get_anomaly_description(self, anomaly: AnomalyEvent) -> str:
        """Get short description of anomaly."""
        source = anomaly.sensor_source
        
        if source == "crypto":
            return f"BTC изменился на {abs(anomaly.value):.1f}%"
        elif source == "earthquake":
            return f"Землетрясение M{anomaly.value:.1f}"
        elif source == "space_weather":
            return "Солнечная вспышка"
        elif source == "quantum_rng":
            return "Квантовый RNG показал паттерн"
        elif source == "weather":
            return "Резкое изменение погоды"
        elif source == "news":
            return "Всплеск новостей"
        else:
            return f"{source}: аномалия"
    
    def _get_anomaly_details(self, anomaly: AnomalyEvent) -> str:
        """Get detailed info about anomaly."""
        if anomaly.metadata and "reason" in anomaly.metadata:
            return anomaly.metadata["reason"]
        return f"Значение: {anomaly.value:.2f}"
    
    def _format_time(self, timestamp: float) -> str:
        """Format timestamp."""
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%d.%m.%Y, %H:%M:%S")
    
    def _get_time_diff(self, anomalies: list[AnomalyEvent]) -> str:
        """Get time difference between first and last anomaly."""
        if len(anomalies) < 2:
            return "мгновенно"
        
        times = [a.timestamp for a in anomalies]
        diff = max(times) - min(times)
        
        if diff < 1:
            return f"{int(diff*1000)}мс"
        elif diff < 60:
            return f"{int(diff)}с"
        else:
            return f"{int(diff/60)}мин {int(diff%60)}с"
    
    def _get_explanations(self, anomalies: list[AnomalyEvent]) -> str:
        """Get possible explanations for cluster."""
        msg = "1️⃣ Случайное совпадение\n"
        msg += "2️⃣ Общая скрытая причина\n"
        msg += "3️⃣ Синхронизация систем"
        return msg
    
    def _get_detailed_explanations(self, cluster: AnomalyCluster) -> str:
        """Get detailed explanations."""
        prob = cluster.probability
        
        msg = f"1. <b>СЛУЧАЙНОСТЬ</b> ({prob:.3f}% вероятность)\n"
        msg += "   Просто невероятное совпадение\n\n"
        msg += "2. <b>ОБЩАЯ ПРИЧИНА</b>\n"
        msg += "   Солнечная активность → магнитное поле →\n"
        msg += "   влияет на тектонику + электронику +\n"
        msg += "   квантовые процессы\n"
        msg += "   Проблема: механизм неизвестен науке\n\n"
        msg += "3. <b>РЕТРОКАУЗАЛЬНОСТЬ</b>\n"
        msg += "   Событие \"отправило сигнал в прошлое\"\n"
        msg += "   через квантовую запутанность\n"
        msg += "   Проблема: нарушает причинность\n\n"
        msg += "4. <b>СИНХРОНИЗАЦИЯ СИСТЕМ</b>\n"
        msg += "   Вселенная - единая система\n"
        msg += "   Все процессы связаны на глубинном уровне\n"
        msg += "   Проблема: противоречит локальности\n\n"
        msg += "5. <b>ГЛИТЧ В СИМУЛЯЦИИ</b> 👁️\n"
        msg += "   Если мы в симуляции, такие синхронизации\n"
        msg += "   могут быть \"багами\" в коде реальности"
        
        return msg
    
    def _get_independence_explanation(self, anomalies: list[AnomalyEvent]) -> str:
        """Explain why systems should be independent."""
        sources = [a.sensor_source for a in anomalies]
        
        explanations = {
            "crypto": "• Крипта = человеческая экономика",
            "earthquake": "• Землетрясение = тектоника плит",
            "space_weather": "• Солнце = космос",
            "quantum_rng": "• Квантовый RNG = фундамент реальности",
            "weather": "• Погода = атмосфера",
            "news": "• Новости = человеческие события"
        }
        
        return "\n".join(explanations.get(s, f"• {s}") for s in set(sources))
