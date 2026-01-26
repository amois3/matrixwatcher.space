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
        1: "🟡",  # Local deviation
        2: "🟠",  # Synchronization
        3: "🔴",  # Anomalous cluster
        4: "🟣",  # Global disturbance
        5: "⚫"   # Critical synchronicity
    }

    # Level names (honest and calibrated)
    LEVEL_NAMES = {
        1: "Local Deviation",
        2: "Temporal Synchronization",  # 2 sources in 30s
        3: "Multiple Correlation",  # 3 sources in 30s
        4: "System Disturbance",  # 4 sources in 30s
        5: "Critical Synchronicity"  # 5+ sources in 30s (extremely rare)
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
        level_name = self.LEVEL_NAMES.get(cluster.level, "Observation")

        msg = f"🕒 <b>{timestamp.strftime('%d %b · %H:%M')}</b>\n"
        msg += f"Level: {emoji} <b>{level_name}</b>"
        
        return msg

    def _generate_sources_list(self, anomalies: list[AnomalyEvent]) -> str:
        """Generate factual list of observed sources."""
        sources = set(a.sensor_source for a in anomalies)
        
        msg = "\n<b>Sources:</b>\n"
        
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
        msg = "\n<b>System Comment:</b>\n"

        if cluster.level == 1:
            msg += "Short-term deviation recorded in one source. "
            msg += "Such fluctuations occur regularly and stay within background noise."

        elif cluster.level == 2:
            msg += "Several independent sources showed deviations in close time window. "
            msg += "Short-term process synchronization recorded."

        elif cluster.level == 3:
            msg += "Stable cluster of deviations recorded across several independent domains. "
            msg += "Observed behavior exceeds normal background."

        elif cluster.level == 4:
            msg += "Synchronous anomalies detected in physical, digital and probabilistic sources. "
            msg += "State exceeds standard operating modes."

        elif cluster.level == 5:
            msg += "Rare configuration of synchronous anomalies recorded across multiple domains. "
            msg += "Such events stand out against entire observation history."
        
        return msg
    
    def _generate_statistical_context(self, cluster: AnomalyCluster, snapshot: AnomalyIndexSnapshot) -> str:
        """Generate statistical context (baseline comparison)."""
        msg = "\n<b>Statistical Context:</b>\n"

        # Anomaly Index
        msg += f"• Anomaly Index: {snapshot.index:.0f}/100\n"

        # Baseline comparison
        if snapshot.baseline_ratio > 1.2:
            msg += f"• Background deviation: {snapshot.baseline_ratio:.1f}x\n"
        else:
            msg += f"• Within normal background\n"

        # Rarity indicator (honest, qualitative)
        if cluster.level == 2:
            msg += f"• Frequency: regular (2 sources)\n"
        elif cluster.level == 3:
            msg += f"• Frequency: periodic (3 sources)\n"
        elif cluster.level == 4:
            msg += f"• Frequency: rare (4 sources)\n"
        elif cluster.level >= 5:
            msg += f"• Frequency: very rare (5+ sources)\n"
        
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
        
        msg = f"🎲 <b>Quantum RNG: {score:.0f} points</b>\n"

        # Get values
        randomness = meta.get("randomness_score", anomaly.value)
        expected = meta.get("expected", 0.95)
        source = meta.get("source", "unknown")

        # Source explanation
        source_text = {
            "anu_quantum": "quantum vacuum (Australia)",
            "random_org_atmospheric": "atmospheric noise",
            "local_entropy": "local entropy"
        }.get(source, source)

        msg += f"Randomness: {randomness:.1%} (normal {expected:.1%})\n"
        msg += f"Source: {source_text}\n"

        # Explanation
        if randomness < 0.90:
            msg += "→ <i>Quantum numbers showing patterns</i>\n"
            msg += "→ <i>Possible \"glitch\" in randomness</i>"
        elif randomness < 0.93:
            msg += "→ <i>Small deviation from ideal randomness</i>"

        # Additional details
        if "autocorrelation" in meta:
            autocorr = meta["autocorrelation"]
            if abs(autocorr) > 0.1:
                msg += f"\n→ <i>Numbers correlate (r={autocorr:.2f})</i>"

        if "bit_balance" in meta:
            balance = meta["bit_balance"]
            if abs(balance - 0.5) > 0.05:
                msg += f"\n→ <i>Bit imbalance ({balance:.1%} ones)</i>"
        
        return msg
    
    def _format_crypto(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Crypto anomaly with explanation."""
        meta = anomaly.metadata or {}
        
        msg = f"💰 <b>Crypto: {score:.0f} points</b>\n"

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
            msg += f"→ <i>Sharp {'rise' if change_pct > 0 else 'drop'} in short time</i>\n"
        else:
            msg += f"→ <i>Significant price change</i>\n"

        # Volume
        if "volume_spike" in meta and meta["volume_spike"]:
            msg += "→ <i>Trading volume spiked</i>"
        
        return msg
    
    def _format_earthquake(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Earthquake anomaly with explanation."""
        meta = anomaly.metadata or {}
        
        msg = f"🌍 <b>Earthquake: {score:.0f} points</b>\n"

        magnitude = anomaly.value
        location = meta.get("location", "Unknown")
        depth = meta.get("depth_km", 0)

        msg += f"Magnitude {magnitude} in {location}\n"
        msg += f"Depth: {depth} km "

        # Depth explanation
        if depth < 70:
            msg += "(shallow, more dangerous)\n"
        else:
            msg += "(deep)\n"

        # Magnitude explanation
        if magnitude >= 7.0:
            msg += "→ <i>Very strong earthquake</i>\n"
            msg += "→ <i>May cause tsunami</i>"
        elif magnitude >= 6.0:
            msg += "→ <i>Strong earthquake</i>\n"
            msg += "→ <i>Possible damage</i>"
        elif magnitude >= 5.0:
            msg += "→ <i>Moderate earthquake</i>"
        
        return msg

    
    def _format_space_weather(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Space Weather anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"☀️ <b>Space Weather: {score:.0f} points</b>\n"

        kp_index = meta.get("kp_index", 0)
        flare_class = meta.get("max_flare_class", "A")

        msg += f"Kp index: {kp_index} "

        if kp_index >= 7:
            msg += "(strong geomagnetic storm)\n"
        elif kp_index >= 5:
            msg += "(geomagnetic storm)\n"
        else:
            msg += "\n"

        if flare_class in ["X", "M"]:
            msg += f"Solar flare class {flare_class}\n"
            msg += "→ <i>May affect electronics</i>"
        
        return msg
    
    def _format_weather(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Weather anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"🌤️ <b>Weather: {score:.0f} points</b>\n"

        temp = meta.get("temperature", 0)
        change = meta.get("change_percent", 0)

        msg += f"Temperature: {temp}°C "

        if abs(change) > 10:
            msg += f"({'rise' if change > 0 else 'drop'} by {abs(change):.1f}%)\n"
            msg += "→ <i>Sharp weather change</i>"
        else:
            msg += "\n"
        
        return msg
    
    def _format_news(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format News anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"📰 <b>News: {score:.0f} points</b>\n"

        count = meta.get("headline_count", 0)
        normal = meta.get("normal_count", 0)

        msg += f"Headlines: {count} (usually {normal})\n"

        if count > normal * 2:
            msg += "→ <i>Sharp news spike</i>\n"
            msg += "→ <i>Possibly important event</i>"
        
        return msg
    
    def _format_blockchain(self, anomaly: AnomalyEvent, score: float) -> str:
        """Format Blockchain anomaly."""
        meta = anomaly.metadata or {}
        
        msg = f"⛓️ <b>Blockchain: {score:.0f} points</b>\n"

        network = meta.get("network", "Unknown")
        block_time = meta.get("block_time", 0)

        msg += f"{network}: block time {block_time}s\n"
        msg += "→ <i>Block speed anomaly</i>"
        
        return msg
    
    def _generate_correlation_explanation(self, cluster: AnomalyCluster) -> str:
        """Generate explanation of correlation."""
        msg = "🔗 <b>Possible Connection:</b>\n"

        sources = [a.sensor_source for a in cluster.anomalies]
        explanations = []

        # Generate smart explanation based on combination
        if "quantum_rng" in sources and "earthquake" in sources:
            explanations.append("Quantum fluctuations before geophysical event")
            explanations.append("→ Possible quantum-level influence")

        if "crypto" in sources and "earthquake" in sources:
            if explanations:
                explanations.append("")  # Empty line
            explanations.append("Market reaction to natural event")
            explanations.append("→ Investors reacting to news")

        if "quantum_rng" in sources and "crypto" in sources:
            if explanations:
                explanations.append("")  # Empty line
            explanations.append("Quantum anomaly + market volatility")
            explanations.append("→ Unexplained correlation")

        if "space_weather" in sources:
            if explanations:
                explanations.append("")  # Empty line
            explanations.append("Solar activity may affect other systems")
            explanations.append("→ Geomagnetic effects")

        if len(sources) >= 3 and not explanations:
            explanations.append("Multiple systems showing anomalies")
            explanations.append("→ Possibly global event")
        
        msg += "\n".join(explanations)
        return msg
    
    def _generate_probabilistic_estimates(self, probabilities: dict[str, dict]) -> str:
        """Generate probabilistic estimates section (calm, factual)."""
        if not probabilities:
            return ""
        
        msg = "<b>Historically after similar conditions:</b>\n"

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
                msg += f"• {description}: {prob:.0%} of cases "
                msg += f"(avg time: {avg_time:.1f}h, n={observations})\n"

        msg += "\n<i>→ Statistics based on history only. Not a prediction.</i>"
        
        return msg
    
    def _generate_footer(self, cluster: AnomalyCluster) -> str:
        """Generate minimal footer with status."""
        msg = "\n<b>Status:</b> "

        if cluster.level == 1:
            msg += "Observation, no action"
        elif cluster.level == 2:
            msg += "Increased attention"
        elif cluster.level >= 3:
            msg += "Active observation"

        return msg
