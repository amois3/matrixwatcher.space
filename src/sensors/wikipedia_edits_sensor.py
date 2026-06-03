"""Wikipedia Edits Sensor — global human edit rate on Wikipedia (all languages).

A more objective replacement-in-waiting for the RSS `news` sensor. headline_count
depends on a handful of editors at BBC/NYT and jumps when their shift starts — an
editorial artifact, not a real-world signal. The global Wikipedia edit rate is
instead a decentralised reaction of hundreds of thousands of people; when an event
breaks, the crowd pours into the relevant articles, so a sharp surge is an
objective "shaking" of the human information field (exactly the framing the
independent reviewer proposed).

Source: https://stream.wikimedia.org/v2/stream/recentchange  (Wikimedia EventStreams
SSE, public, no key; requires a descriptive User-Agent). The stream carries ALL
Wikimedia projects, so we keep only Wikipedia proper — server_name ending in
".wikipedia.org" — and drop Wikidata / Commons / Wiktionary, which are largely
structured-data maintenance and dilute the event-driven human signal. Independent
infrastructure from our news/crypto/space feeds (source-diversity hygiene).

Two honest design choices the raw stream forces:
  1. BOTS DOMINATE. We count only human (bot == false) edits.
  2. The edit rate has a strong diurnal/weekly cycle. We do NOT model it here; we
     emit the instantaneous human edits/sec and let the adaptive detector judge it
     against its ~100-minute rolling robust baseline, which tracks the slow swing
     so only SHARP departures fire (not the predictable daily peak). Hence
     adaptive-only (no fixed-threshold named event).

Architecture: the stream is push (SSE), our scheduler is pull. To avoid a
persistent task, collect() samples the stream for a bounded window (~8 s) each
poll and reports the rate; high event volume makes a short sample a stable estimate.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.event_bus import EventBus
from ..core.types import SensorReading

logger = logging.getLogger(__name__)

_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
_HEADERS = {
    "User-Agent": "MatrixWatcher/1.0 (https://matrixwatcher.space; anomaly research) edit-rate-sensor",
    "Accept": "text/event-stream",
}


class WikipediaEditsSensor(BaseSensor):
    """Global human (non-bot) Wikipedia edit rate sampled from Wikimedia EventStreams."""

    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None):
        super().__init__("wikipedia_edits", config, event_bus)
        params = (config.custom_params if config and config.custom_params else {}) or {}
        self.sample_seconds = float(params.get("sample_seconds", 8.0))

    async def collect(self) -> SensorReading:
        total = bots = human_edits = non_wikipedia = 0
        t0 = time.time()
        timeout = aiohttp.ClientTimeout(total=self.sample_seconds + 12.0)
        try:
            async with aiohttp.ClientSession(headers=_HEADERS) as session:
                async with session.get(_STREAM_URL, timeout=timeout) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Wikipedia stream returned HTTP {resp.status}")
                    async for raw in resp.content:
                        line = raw.decode("utf-8", "ignore").strip()
                        if not line.startswith("data:"):
                            continue
                        try:
                            d = json.loads(line[5:].strip())
                        except (ValueError, json.JSONDecodeError):
                            continue
                        total += 1
                        is_bot = bool(d.get("bot"))
                        if is_bot:
                            bots += 1
                        is_wikipedia = str(d.get("server_name", "")).endswith(".wikipedia.org")
                        if not is_wikipedia:
                            non_wikipedia += 1
                        if d.get("type") in ("edit", "new") and not is_bot and is_wikipedia:
                            human_edits += 1
                        if time.time() - t0 >= self.sample_seconds:
                            break
        except asyncio.TimeoutError as e:
            raise RuntimeError("Wikipedia stream sample timed out") from e

        elapsed = max(0.5, time.time() - t0)
        if total == 0:
            raise RuntimeError("Wikipedia stream yielded no events in the sample window")

        return SensorReading.create(
            source="wikipedia_edits",
            data={
                "edits_per_sec": float(human_edits / elapsed),
                "human_edits": int(human_edits),
                "events_total": int(total),
                "bot_fraction": float(bots / total) if total else 0.0,
                "non_wikipedia_dropped": int(non_wikipedia),
                "sample_seconds": float(elapsed),
            },
        )

    def get_schema(self) -> dict[str, type]:
        return {
            "edits_per_sec": float,
            "human_edits": int,
            "events_total": int,
            "bot_fraction": float,
        }
