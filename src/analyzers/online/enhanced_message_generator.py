"""Enhanced Message Generator - Observer-style messages.

Creates calm, factual Telegram messages following Matrix Watcher philosophy:
- Observes, doesn't interpret
- States what is seen, not what it "means"
- Calm, neutral, confident tone
- Statistical facts, not predictions
"""

import logging
from datetime import datetime
from typing import Any

from ...core.types import AnomalyEvent
from .cluster_detector import AnomalyCluster
from .anomaly_index import AnomalyIndexSnapshot

logger = logging.getLogger(__name__)


class EnhancedMessageGenerator:
    """Generates observer-style messages following Matrix Watcher philosophy."""
    
    # Level emoji (updated to match new philosophy)
    LEVEL_EMOJI = {
        1: "🟡",  # Локальное отклонение
        2: "🟠",  # Синхронизация
        3: "🔴",  # Аномальный кластер
        4: "🟣",  # Глобальное возмущение
        5: "⚫"   # Критическая синхронность
    }
    
    # Level names (Russian, honest and calibrated)
    LEVEL_NAMES = {
        1: "Локальное отклонение",
        2: "Временная синхронизация",  # 2 sources in 30s
        3: "Множественная корреляция",  # 3 sources in 30s
        4: "Системное возмущение",  # 4 sources in 30s
        5: "Критическая синхронность"  # 5+ sources in 30s (extremely rare)
    }
    
    def generate_with_index(
        self,
        cluster: AnomalyCluster,
        index_snapshot: AnomalyIndexSnapshot,
        probabilities: dict[str, dict] | None = None
    ) -> str:
        """Generate observer-style message following Matrix Watcher philosophy."""
        # Header with level and timestamp
        msg = self._generate_header(cluster, index_snapshot)
        msg += "\n"
        
        # Observed sources (factual)
        msg += self._generate_sources_list(cluster.anomalies)
        msg += "\n"
        
        # System comment (calm, factual)
        msg += self._generate_system_comment(cluster, index_snapshot)
        msg += "\n"
        
        # Statistical context (if available)
        msg += self._generate_statistical_context(cluster, index_snapshot)
        
        # Probabilistic estimates (if available and level >= 2)
        if probabilities and cluster.level >= 2:
            msg += "\n"
            msg += self._generate_probabilistic_estimates(probabilities)
        
        # Footer with metadata
        msg += self._generate_footer(cluster)
        
        return msg
    
    def _generate_header(self, cluster: AnomalyCluster, snapshot: AnomalyIndexSnapshot) -> str:
        """Generate calm, factual header."""
        timestamp = datetime.fromtimestamp(cluster.timestamp)
        emoji = self.LEVEL_EMOJI.get(cluster.level, "🔍")
        level_name = self.LEVEL_NAMES.get(cluster.level, "Наблюдение")
        
        msg = f"🕒 <b>{timestamp.strftime('%d %b · %H:%M')}</b>\n"
        msg += f"Уровень: {emoji} <b>{level_name}</b>"
        
        return msg

    def _generate_sources_list(self, anomalies: list[AnomalyEvent]) -> str:
        """Generate factual list of observed sources."""
        sources = set(a.sensor_source for a in anomalies)
        
        msg = "\n<b>Источники:</b>\n"
        
        source_names = {
            "quantum_rng": "🎲 Quantum RNG",
            "crypto": "💰 Crypto",
            "earthquake": "🌍 Earthquake",
            "space_weather": "☀️ Space Weather",
            "weather": "🌤️ Weather",
            "news": "📰 News",
            "blockchain": "⛓️ Blockchain"
        }
        
        for source in sorted(sources):
            name = source_names.get(source, source)
            msg += f"• {name}\n"
        
        return msg
    
    def _generate_system_comment(self, cluster: AnomalyCluster, snapshot: AnomalyIndexSnapshot) -> str:
        """Generate calm, factual system comment based on level."""
        msg = "\n<b>Комментарий системы:</b>\n"
        
        if cluster.level == 1:
            msg += "Зафиксировано кратковременное отклонение в одном из источников. "
            msg += "Подобные флуктуации регулярно возникают и не выходят за рамки фонового шума."
        
        elif cluster.level == 2:
            msg += "Несколько независимых источников показали отклонения в близком временном окне. "
            msg += "Зафиксирована кратковременная синхронизация процессов."
        
        elif cluster.level == 3:
            msg += "Зафиксирован устойчивый кластер отклонений в нескольких независимых доменах. "
            msg += "Наблюдаемое поведение выходит за рамки обычного фона."
        
        elif cluster.level == 4:
            msg += "Обнаружены синхронные аномалии в физических, цифровых и вероятностных источниках. "
            msg += "Состояние выходит за пределы стандартных режимов."
        
        elif cluster.level == 5:
            msg += "Зафиксирована редкая конфигурация синхронных аномалий across multiple domains. "
            msg += "Подобные события выделяются на фоне всей истории наблюдений."
        
        return msg
    
    def _generate_statistical_context(self, cluster: AnomalyCluster, snapshot: AnomalyIndexSnapshot) -> str:
        """Generate statistical context (baseline comparison)."""
        msg = "\n<b>Статистический контекст:</b>\n"
        
        # Anomaly Index
        msg += f"• Индекс аномальности: {snapshot.index:.0f}/100\n"
        
        # Baseline comparison
        if snapshot.baseline_ratio > 1.2:
            msg += f"• Отклонение от фона: {snapshot.baseline_ratio:.1f}x\n"
        else:
            msg += f"• В пределах нормального фона\n"
        
        # Rarity indicator (honest, qualitative)
        if cluster.level == 2:
            msg += f"• Частота: регулярно (2 источника)\n"
        elif cluster.level == 3:
            msg += f"• Частота: периодически (3 источника)\n"
        elif cluster.level == 4:
            msg += f"• Частота: редко (4 источника)\n"
        elif cluster.level >= 5:
            msg += f"• Частота: очень редко (5+ источников)\n"
        
        return msg
    
    # Old detailed formatting methods removed - new philosophy is "observe, don't interpret"
    
    def _format_anomaly_details_DEPRECATED(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format detailed explanation for one anomaly."""
        sensor = anomaly.sensor_source
        
        if sensor == "quantum_rng":
            return self._format_quantum_rng(anomaly, score)
        elif sensor == "crypto":
            return self._format_crypto(anomaly, score)
        elif sensor == "earthquake":
            return self._format_earthquake(anomaly, score)
        elif sensor == "space_weather":
            return self._format_space_weather(anomaly, score)
        elif sensor == "weather":
            return self._format_weather(anomaly, score)
        elif sensor == "news":
            return self._format_news(anomaly, score)
        elif sensor == "blockchain":
            return self._format_blockchain(anomaly, score)
        else:
            return f"<b>{sensor}</b>: {anomaly.description}"
    
    def _format_quantum_rng(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Quantum RNG anomaly with explanation."""
        meta = anomaly.metadata or {}
        
        msg = f"🎲 <b>Quantum RNG: {score:.0f} баллов</b>\n"
        
        # Get values
        randomness = meta.get("randomness_score", anomaly.value)
        expected = meta.get("expected", 0.95)
        source = meta.get("source", "unknown")
        
        # Source explanation
        source_text = {
            "anu_quantum": "квантовый вакуум (Австралия)",
            "random_org_atmospheric": "атмосферный шум",
            "local_entropy": "локальная энтропия"
        }.get(source, source)
        
        msg += f"Случайность: {randomness:.1%} (норма {expected:.1%})\n"
        msg += f"Источник: {source_text}\n"
        
        # Explanation
        if randomness < 0.90:
            msg += "→ <i>Квантовые числа показывают паттерны</i>\n"
            msg += "→ <i>Возможный \"глитч\" в случайности</i>"
        elif randomness < 0.93:
            msg += "→ <i>Небольшое отклонение от идеальной случайности</i>"
        
        # Additional details
        if "autocorrelation" in meta:
            autocorr = meta["autocorrelation"]
            if abs(autocorr) > 0.1:
                msg += f"\n→ <i>Числа коррелируют (r={autocorr:.2f})</i>"
        
        if "bit_balance" in meta:
            balance = meta["bit_balance"]
            if abs(balance - 0.5) > 0.05:
                msg += f"\n→ <i>Дисбаланс битов ({balance:.1%} единиц)</i>"
        
        return msg
    
    def _format_crypto(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Crypto anomaly with explanation."""
        meta = anomaly.metadata or {}
        
        msg = f"💰 <b>Crypto: {score:.0f} баллов</b>\n"
        
        # Get values
        symbol = meta.get("symbol", "BTC")
        prev_price = meta.get("previous_price", 0)
        new_price = meta.get("new_price", anomaly.value)
        change_pct = meta.get("change_percent", 0)
        
        msg += f"{symbol}: ${prev_price:,.0f} → ${new_price:,.0f} "
        
        if change_pct > 0:
            msg += f"(+{change_pct:.2f}%)\n"
        else:
            msg += f"({change_pct:.2f}%)\n"
        
        # Explanation
        if abs(change_pct) > 2:
            msg += f"→ <i>Резкий {'рост' if change_pct > 0 else 'падение'} за короткое время</i>\n"
        else:
            msg += f"→ <i>Значительное изменение цены</i>\n"
        
        # Volume
        if "volume_spike" in meta and meta["volume_spike"]:
            msg += "→ <i>Объем торгов резко вырос</i>"
        
        return msg
    
    def _format_earthquake(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Earthquake anomaly with explanation."""
        meta = anomaly.metadata or {}
        
        msg = f"🌍 <b>Earthquake: {score:.0f} баллов</b>\n"
        
        magnitude = anomaly.value
        location = meta.get("location", "Unknown")
        depth = meta.get("depth_km", 0)
        
        msg += f"Магнитуда {magnitude} в {location}\n"
        msg += f"Глубина: {depth} км "
        
        # Depth explanation
        if depth < 70:
            msg += "(мелкое, более опасное)\n"
        else:
            msg += "(глубокое)\n"
        
        # Magnitude explanation
        if magnitude >= 7.0:
            msg += "→ <i>Очень сильное землетрясение</i>\n"
            msg += "→ <i>Может вызвать цунами</i>"
        elif magnitude >= 6.0:
            msg += "→ <i>Сильное землетрясение</i>\n"
            msg += "→ <i>Возможны разрушения</i>"
        elif magnitude >= 5.0:
            msg += "→ <i>Умеренное землетрясение</i>"
        
        return msg

    
    def _format_space_weather(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Space Weather anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"☀️ <b>Space Weather: {score:.0f} баллов</b>\n"
        
        kp_index = meta.get("kp_index", 0)
        flare_class = meta.get("max_flare_class", "A")
        
        msg += f"Kp индекс: {kp_index} "
        
        if kp_index >= 7:
            msg += "(сильная геомагнитная буря)\n"
        elif kp_index >= 5:
            msg += "(геомагнитная буря)\n"
        else:
            msg += "\n"
        
        if flare_class in ["X", "M"]:
            msg += f"Солнечная вспышка класса {flare_class}\n"
            msg += "→ <i>Может влиять на электронику</i>"
        
        return msg
    
    def _format_weather(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Weather anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"🌤️ <b>Weather: {score:.0f} баллов</b>\n"
        
        temp = meta.get("temperature", 0)
        change = meta.get("change_percent", 0)
        
        msg += f"Температура: {temp}°C "
        
        if abs(change) > 10:
            msg += f"({'рост' if change > 0 else 'падение'} на {abs(change):.1f}%)\n"
            msg += "→ <i>Резкое изменение погоды</i>"
        else:
            msg += "\n"
        
        return msg
    
    def _format_news(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format News anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"📰 <b>News: {score:.0f} баллов</b>\n"
        
        count = meta.get("headline_count", 0)
        normal = meta.get("normal_count", 0)
        
        msg += f"Новостей: {count} (обычно {normal})\n"
        
        if count > normal * 2:
            msg += "→ <i>Резкий всплеск новостей</i>\n"
            msg += "→ <i>Возможно важное событие</i>"
        
        return msg
    
    def _format_blockchain(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Blockchain anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"⛓️ <b>Blockchain: {score:.0f} баллов</b>\n"
        
        network = meta.get("network", "Unknown")
        block_time = meta.get("block_time", 0)
        
        msg += f"{network}: время блока {block_time}с\n"
        msg += "→ <i>Аномалия в скорости блоков</i>"
        
        return msg
    
    def _generate_correlation_explanation(self, cluster: AnomalyCluster) -> str:
        """Generate explanation of correlation."""
        msg = "🔗 <b>Возможная связь:</b>\n"
        
        sources = [a.sensor_source for a in cluster.anomalies]
        explanations = []
        
        # Generate smart explanation based on combination
        if "quantum_rng" in sources and "earthquake" in sources:
            explanations.append("Квантовые флуктуации перед геофизическим событием")
            explanations.append("→ Возможное влияние на квантовый уровень")
        
        if "crypto" in sources and "earthquake" in sources:
            if explanations:
                explanations.append("")  # Empty line
            explanations.append("Рыночная реакция на природное событие")
            explanations.append("→ Инвесторы реагируют на новости")
        
        if "quantum_rng" in sources and "crypto" in sources:
            if explanations:
                explanations.append("")  # Empty line
            explanations.append("Квантовая аномалия + рыночная волатильность")
            explanations.append("→ Необъяснимая корреляция")
        
        if "space_weather" in sources:
            if explanations:
                explanations.append("")  # Empty line
            explanations.append("Солнечная активность может влиять на другие системы")
            explanations.append("→ Геомагнитные эффекты")
        
        if len(sources) >= 3 and not explanations:
            explanations.append("Множественные системы показывают аномалии")
            explanations.append("→ Возможно глобальное событие")
        
        msg += "\n".join(explanations)
        return msg
    
    def _generate_probabilistic_estimates(self, probabilities: dict[str, dict]) -> str:
        """Generate probabilistic estimates section (calm, factual)."""
        if not probabilities:
            return ""
        
        msg = "<b>Исторически после подобных условий:</b>\n"
        
        # Sort by probability (highest first)
        sorted_probs = sorted(
            probabilities.items(),
            key=lambda x: x[1]["probability"],
            reverse=True
        )
        
        for event_type, info in sorted_probs:
            prob = info["probability"]
            avg_time = info["avg_time_hours"]
            observations = info["observations"]
            description = info["description"]
            
            # Only show if probability > 5% and enough observations
            if prob > 0.05 and observations >= 5:
                msg += f"• {description}: {prob:.0%} случаев "
                msg += f"(среднее время: {avg_time:.1f}ч, n={observations})\n"
        
        msg += "\n<i>→ Только статистика на основе истории. Не прогноз.</i>"
        
        return msg
    
    def _generate_footer(self, cluster: AnomalyCluster) -> str:
        """Generate minimal footer with status."""
        msg = "\n<b>Статус:</b> "
        
        if cluster.level == 1:
            msg += "Наблюдение без действий"
        elif cluster.level == 2:
            msg += "Повышенное внимание"
        elif cluster.level >= 3:
            msg += "Активное наблюдение"
        
        return msg
