"""Wikipedia Edits Sensor — human edit rate across Wikipedia languages.

This measures changes in human editing activity. It cannot establish why an
edit happened or whether an external event caused it.

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

Wikimedia supports ``since`` replay. When enabled, each bounded poll consumes
the interval since the previous persisted reading with an event-time watermark.
Replay is capped to avoid expensive unbounded historical requests. A skipped
interval is marked partial and never enters live anomaly detection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.event_bus import EventBus
from ..core.types import SensorReading

logger = logging.getLogger(__name__)

_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
_HEADERS = {
    "User-Agent": "MatrixWatcher/1.0 (+https://matrixwatcher.space; contact: amois3@users.noreply.github.com)",
    "Accept": "text/event-stream",
}
_REPLAY_HEADERS = {**_HEADERS, "Accept": "application/json"}


def _event_time(value: dict) -> float | None:
    dt = (value.get("meta") or {}).get("dt")
    if not isinstance(dt, str):
        return None
    try:
        parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except ValueError:
        return None


def _is_human_wikipedia_edit(value: dict) -> bool:
    return (value.get("type") in ("edit", "new") and not value.get("bot")
            and str(value.get("server_name", "")).endswith(".wikipedia.org"))


class WikipediaEditsSensor(BaseSensor):
    """Global human (non-bot) Wikipedia edit rate sampled from Wikimedia EventStreams."""

    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None,
                 log_dir: Path = Path("logs/wikipedia_edits")):
        super().__init__("wikipedia_edits", config, event_bus)
        params = (config.custom_params if config and config.custom_params else {}) or {}
        self.sample_seconds = max(1.0, min(50.0, float(params.get("sample_seconds", 45.0))))
        self.resume_history = bool(params.get("resume_history", False))
        self.max_replay_seconds = max(60.0, min(3600.0, float(params.get("max_replay_seconds", 600.0))))
        self.watermark_lag_seconds = max(10.0, min(30.0, float(params.get("watermark_lag_seconds", 15.0))))
        self._last_covered_at = self._load_checkpoint(log_dir) if self.resume_history else None

    @staticmethod
    def _load_checkpoint(log_dir: Path) -> float | None:
        for path in sorted(log_dir.glob("*.jsonl"), reverse=True)[:2]:
            try:
                with path.open("rb") as stream:
                    stream.seek(0, 2)
                    stream.seek(max(0, stream.tell() - 65536))
                    lines = stream.read().splitlines()
                for line in reversed(lines):
                    try:
                        record = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    ts = record.get("covered_until_at", record.get("timestamp"))
                    if isinstance(ts, (int, float)):
                        return float(ts)
            except OSError:
                continue
        return None

    def confirm_persisted(self, reading: SensorReading) -> None:
        """Advance the in-memory cursor only after the raw record is durable."""
        covered = reading.data.get("covered_until_at")
        if isinstance(covered, (int, float)):
            self._last_covered_at = float(covered)

    async def collect(self) -> SensorReading:
        if self.resume_history:
            return await self._collect_resumed()
        return await self._collect_sample()

    async def _collect_resumed(self) -> SensorReading:
        now = time.time()
        end = now - self.watermark_lag_seconds
        previous = self._last_covered_at if self._last_covered_at is not None else end - 60.0
        if previous > end:
            raise RuntimeError("Wikipedia replay interval has not advanced")
        start = max(previous, end - self.max_replay_seconds)
        skipped = max(0.0, start - previous)
        since = datetime.fromtimestamp(max(0, start - 5), timezone.utc).isoformat(
            timespec="milliseconds").replace("+00:00", "Z")
        total = bots = human_edits = non_wikipedia = 0
        ids: set[str] = set()
        reached_watermark = False
        try:
            async with aiohttp.ClientSession(headers=_REPLAY_HEADERS) as session:
                async with session.get(_STREAM_URL, params={"since": since},
                                       timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Wikipedia replay returned HTTP {resp.status}")
                    async for raw in resp.content:
                        if not raw.strip():
                            continue
                        try:
                            value = json.loads(raw)
                        except (ValueError, UnicodeDecodeError):
                            continue
                        ts = _event_time(value)
                        if ts is None:
                            continue
                        if ts >= end + 5.0:
                            reached_watermark = True
                            break
                        if ts < start or ts >= end:
                            continue
                        meta = value.get("meta") or {}
                        if meta.get("domain") == "canary":
                            continue
                        event_id = meta.get("id")
                        if isinstance(event_id, str):
                            if event_id in ids:
                                continue
                            ids.add(event_id)
                        total += 1
                        is_bot = bool(value.get("bot"))
                        bots += is_bot
                        is_wikipedia = str(value.get("server_name", "")).endswith(".wikipedia.org")
                        non_wikipedia += not is_wikipedia
                        human_edits += _is_human_wikipedia_edit(value)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("Wikipedia replay did not reach its watermark") from exc
        if not reached_watermark or total == 0:
            raise RuntimeError("Wikipedia replay incomplete or empty")
        duration = end - start
        quality = ({"complete": False, "missing_fields": [f"{round(skipped)} s of stream history skipped"]}
                   if skipped >= 1 else {"complete": True})
        return SensorReading(
            timestamp=end, source="wikipedia_edits",
            data={"edits_per_sec": float(human_edits / duration),
                  "human_edits": int(human_edits), "events_total": int(total),
                  "bot_fraction": float(bots / total),
                  "non_wikipedia_dropped": int(non_wikipedia),
                  "sample_seconds": float(duration),
                  "covered_start_at": float(start), "covered_until_at": float(end),
                  "replay_skipped_seconds": float(skipped),
                  "stream_mode": "resumed_history", "quality": quality},
        )

    async def _collect_sample(self) -> SensorReading:
        total = bots = human_edits = non_wikipedia = 0
        t0 = None
        timeout = aiohttp.ClientTimeout(total=self.sample_seconds + 12.0)
        try:
            async with aiohttp.ClientSession(headers=_HEADERS) as session:
                async with session.get(_STREAM_URL, timeout=timeout) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Wikipedia stream returned HTTP {resp.status}")
                    t0 = time.time()
                    async for raw in resp.content:
                        line = raw.decode("utf-8", "ignore").strip()
                        if not line.startswith("data:"):
                            continue
                        try:
                            d = json.loads(line[5:].strip())
                        except (ValueError, json.JSONDecodeError):
                            continue
                        if (d.get("meta") or {}).get("domain") == "canary":
                            continue
                        total += 1
                        is_bot = bool(d.get("bot"))
                        if is_bot:
                            bots += 1
                        is_wikipedia = str(d.get("server_name", "")).endswith(".wikipedia.org")
                        if not is_wikipedia:
                            non_wikipedia += 1
                        if _is_human_wikipedia_edit(d):
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
