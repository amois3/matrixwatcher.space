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

 msg = f"{self.LEVEL_EMOJI[1]} <b>ANOMALY</b>\n\n"
 msg += f"{emoji} <b>{self._get_sensor_name(anomaly.sensor_source)}</b>\n"
 msg += f"{desc}\n\n"
 msg += f"🕐 {self._format_time(anomaly.timestamp)}\n"
 msg += f"📊 Monitoring other systems..."

 return msg

 def _generate_level2(self, cluster: AnomalyCluster) -> str:
 """Level 2: Two systems - medium message."""
 msg = f"{self.LEVEL_EMOJI[2]} <b>CORRELATION (2 systems)</b>\n\n"

 for i, anomaly in enumerate(cluster.anomalies, 1):
 emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
 desc = self._get_anomaly_description(anomaly)
 msg += f"{i}. {emoji} {desc}\n"

 msg += f"\n⏱️ Time difference: {self._get_time_diff(cluster.anomalies)}\n"
 msg += f"🤔 Probability of chance: {cluster.probability:.2f}%\n\n"
 msg += "Possible coincidence, but interesting.\n"
 msg += "Continuing observation."

 return msg

 def _generate_level3(self, cluster: AnomalyCluster) -> str:
 """Level 3: Three systems - detailed message."""
 msg = f"{self.LEVEL_EMOJI[3]} <b>CLUSTER (3 systems)</b>\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

 for i, anomaly in enumerate(cluster.anomalies, 1):
 emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
 desc = self._get_anomaly_description(anomaly)
 msg += f"{i}. {emoji} {desc}\n"

 msg += f"\n⏱️ All within {self._get_time_diff(cluster.anomalies)}\n"
 msg += f"🤔 Probability of chance: {cluster.probability:.3f}%\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "💭 <b>Possible explanations:</b>\n"
 msg += self._get_explanations(cluster.anomalies)
 msg += "\n\nThis is interesting! 🧐"

 return msg

 def _generate_level4(self, cluster: AnomalyCluster) -> str:
 """Level 4: Four+ systems - full analysis."""
 msg = f"{self.LEVEL_EMOJI[4]} <b>CRITICAL ANOMALY LEVEL 4</b>\n\n"
 msg += "⚡ <b>SYNCHRONIZATION OF UNRELATED SYSTEMS</b>\n\n"
 msg += f"🕐 {self._format_time(cluster.timestamp)}\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "📊 <b>WHAT HAPPENED:</b>\n\n"
 msg += f"Within {self._get_time_diff(cluster.anomalies)}, "
 msg += f"{len(cluster.anomalies)} independent systems triggered simultaneously:\n\n"

 for i, anomaly in enumerate(cluster.anomalies, 1):
 emoji = self.SENSOR_EMOJI.get(anomaly.sensor_source, "🔍")
 desc = self._get_anomaly_description(anomaly)
 msg += f"{i}. {emoji} {desc}\n"
 msg += f" {self._get_anomaly_details(anomaly)}\n\n"

 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "🤔 <b>ANALYSIS:</b>\n\n"
 msg += "These events should NOT be connected:\n"
 msg += self._get_independence_explanation(cluster.anomalies)
 msg += "\n\nBut they occurred SIMULTANEOUSLY.\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "🔬 <b>POSSIBLE EXPLANATIONS:</b>\n\n"
 msg += self._get_detailed_explanations(cluster)
 msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += f"🎯 <b>STATISTICS:</b>\n\n"
 msg += f"Probability of this cluster: 1 in {int(1/max(cluster.probability/100, 0.0001)):,}\n"
 msg += f"Criticality level: HIGH\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "💭 <b>CONCLUSION:</b>\n\n"
 msg += "Either we observe a rare coincidence,\n"
 msg += "or there exists an unknown connection\n"
 msg += "between these systems.\n\n"
 msg += "Continuing observation. 👁️"

 return msg

 def _generate_level5(self, cluster: AnomalyCluster) -> str:
 """Level 5: Precursor - special message."""
 precursor = cluster.precursor_event
 event = cluster.anomalies[-1]

 time_diff = event.timestamp - precursor.timestamp

 msg = f"{self.LEVEL_EMOJI[5]} <b>PRECURSOR DETECTED</b>\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

 emoji1 = self.SENSOR_EMOJI.get(precursor.sensor_source, "🔍")
 emoji2 = self.SENSOR_EMOJI.get(event.sensor_source, "🔍")

 msg += f"{emoji1} {self._get_anomaly_description(precursor)}\n"
 msg += f" 🕐 {self._format_time(precursor.timestamp)}\n\n"
 msg += " ⬇️\n"
 msg += f" ⏱️ {int(time_diff/60)} min later\n"
 msg += " ⬇️\n\n"
 msg += f"{emoji2} {self._get_anomaly_description(event)}\n"
 msg += f" 🕐 {self._format_time(event.timestamp)}\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "🤔 <b>WHAT THIS MEANS:</b>\n\n"
 msg += "A system in an <b>unrelated</b> domain showed\n"
 msg += "an anomaly BEFORE the main event.\n\n"
 msg += "Possible explanations:\n"
 msg += "1. Random coincidence\n"
 msg += "2. Common hidden cause\n"
 msg += "3. Retrocausality (future affects past)\n"
 msg += "4. Quantum entanglement of systems\n\n"
 msg += f"🎯 Probability of chance: {cluster.probability:.2f}%\n\n"
 msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
 msg += "🔮 This may be a sign that\n"
 msg += "systems are connected at a deeper level\n"
 msg += "than we understand.\n\n"
 msg += "Continuing monitoring. 👁️"

 return msg

 def _get_sensor_name(self, source: str) -> str:
 """Get human-readable sensor name."""
 names = {
 "crypto": "Crypto",
 "earthquake": "Earthquakes",
 "space_weather": "Space weather",
 "quantum_rng": "Quantum randomness",
 "weather": "Weather",
 "news": "News",
 "blockchain": "Blockchain"
 }
 return names.get(source, source)

 def _get_anomaly_description(self, anomaly: AnomalyEvent) -> str:
 """Get short description of anomaly."""
 source = anomaly.sensor_source

 if source == "crypto":
 return f"BTC changed by {abs(anomaly.value):.1f}%"
 elif source == "earthquake":
 return f"Earthquake M{anomaly.value:.1f}"
 elif source == "space_weather":
 return "Solar flare"
 elif source == "quantum_rng":
 return "Quantum RNG showed pattern"
 elif source == "weather":
 return "Sudden weather change"
 elif source == "news":
 return "News spike"
 else:
 return f"{source}: anomaly"

 def _get_anomaly_details(self, anomaly: AnomalyEvent) -> str:
 """Get detailed info about anomaly."""
 if anomaly.metadata and "reason" in anomaly.metadata:
 return anomaly.metadata["reason"]
 return f"Value: {anomaly.value:.2f}"

 def _format_time(self, timestamp: float) -> str:
 """Format timestamp."""
 dt = datetime.fromtimestamp(timestamp)
 return dt.strftime("%d.%m.%Y, %H:%M:%S")

 def _get_time_diff(self, anomalies: list[AnomalyEvent]) -> str:
 """Get time difference between first and last anomaly."""
 if len(anomalies) < 2:
 return "instantly"

 times = [a.timestamp for a in anomalies]
 diff = max(times) - min(times)

 if diff < 1:
 return f"{int(diff*1000)}ms"
 elif diff < 60:
 return f"{int(diff)}s"
 else:
 return f"{int(diff/60)}min {int(diff%60)}s"

 def _get_explanations(self, anomalies: list[AnomalyEvent]) -> str:
 """Get possible explanations for cluster."""
 msg = "1. Random coincidence\n"
 msg += "2. Common hidden cause\n"
 msg += "3. System synchronization"
 return msg

 def _get_detailed_explanations(self, cluster: AnomalyCluster) -> str:
 """Get detailed explanations."""
 prob = cluster.probability

 msg = f"1. <b>RANDOMNESS</b> ({prob:.3f}% probability)\n"
 msg += " An incredible coincidence\n\n"
 msg += "2. <b>COMMON CAUSE</b>\n"
 msg += " Solar activity -> magnetic field ->\n"
 msg += " affects tectonics + electronics +\n"
 msg += " quantum processes\n"
 msg += " Problem: mechanism unknown to science\n\n"
 msg += "3. <b>RETROCAUSALITY</b>\n"
 msg += " Event sent signal to the past\n"
 msg += " via quantum entanglement\n"
 msg += " Problem: violates causality\n\n"
 msg += "4. <b>SYSTEMS SYNCHRONIZATION</b>\n"
 msg += " Universe is a unified system\n"
 msg += " All processes connected at deep level\n"
 msg += " Problem: contradicts locality\n\n"
 msg += "5. <b>SIMULATION GLITCH</b> 👁️\n"
 msg += " If we are in a simulation, such synchronizations\n"
 msg += " may be bugs in the code of reality"

 return msg

 def _get_independence_explanation(self, anomalies: list[AnomalyEvent]) -> str:
 """Explain why systems should be independent."""
 sources = [a.sensor_source for a in anomalies]

 explanations = {
 "crypto": "• Crypto = human economy",
 "earthquake": "• Earthquake = plate tectonics",
 "space_weather": "• Sun = space",
 "quantum_rng": "• Quantum RNG = foundation of reality",
 "weather": "• Weather = atmosphere",
 "news": "• News = human events"
 }

 return "\n".join(explanations.get(s, f"• {s}") for s in set(sources))
