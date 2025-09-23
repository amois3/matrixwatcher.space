"""Volcanic Activity Sensor — Smithsonian / USGS Weekly Volcanic Activity Report.

Pulls the public RSS feed published every Thursday by the Smithsonian's
Global Volcanism Program in cooperation with USGS. Each ``<item>`` is one
volcano with a status tag in the title ("New Eruptive Activity",
"Continuing Eruptive Activity", "New Unrest", ...). We summarise the week
into a few numeric parameters so the threshold detector can fire on changes.

Source: https://volcano.si.edu/news/WeeklyVolcanoRSS.xml
"""

from __future__ import annotations

import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading
from ..core.event_bus import EventBus

logger = logging.getLogger(__name__)

FEED_URL = "https://volcano.si.edu/news/WeeklyVolcanoRSS.xml"


def parse_volcano_feed(xml_text: str | bytes) -> dict[str, Any]:
    """Parse the Smithsonian weekly RSS and return summary numeric fields.

    Returns a dict with:
      - active_count: int — number of volcanoes in the weekly report
      - new_eruptions: int — items tagged "New Eruptive Activity"
      - new_unrest: int — items tagged "New Unrest"
      - continuing_eruptions: int — items tagged "Continuing Eruptive Activity"
      - feed_pub_date_unix: float — UTC epoch of the feed's pubDate (or 0)
      - volcanoes: list[str] — names of the active volcanoes (for storage,
        not threshold-matched)
    """
    summary: dict[str, Any] = {
        "active_count": 0,
        "new_eruptions": 0,
        "new_unrest": 0,
        "continuing_eruptions": 0,
        "feed_pub_date_unix": 0.0,
        "volcanoes": [],
    }
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning("Volcanic feed parse error: %s", e)
        return summary

    channel = root.find("channel")
    if channel is None:
        return summary

    pub = channel.findtext("pubDate")
    if pub:
        # RFC 822 format like "Thu, 21 May 2026 05:11:06 -0500"
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"):
            try:
                summary["feed_pub_date_unix"] = datetime.strptime(pub, fmt).timestamp()
                break
            except ValueError:
                continue

    titles: list[str] = []
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        titles.append(title)
        summary["active_count"] += 1
        # The report tag follows the last ' - '
        tag = title.rsplit(" - ", 1)[-1].lower()
        if "new eruptive activity" in tag:
            summary["new_eruptions"] += 1
        elif "new unrest" in tag:
            summary["new_unrest"] += 1
        elif "continuing eruptive activity" in tag:
            summary["continuing_eruptions"] += 1

    # Volcano names = first segment before " (Country)"
    summary["volcanoes"] = sorted({re.split(r"\s*\(", t, maxsplit=1)[0] for t in titles})
    return summary


class VolcanicActivitySensor(BaseSensor):
    """Smithsonian Weekly Volcanic Activity Report aggregator."""

    def __init__(self, config: SensorConfig | None = None, event_bus: EventBus | None = None):
        super().__init__("volcanic_activity", config, event_bus)
        # The Smithsonian feed is a WEEKLY digest (updated Thursdays). We poll it
        # hourly, so the same report is re-read ~24×/day. To avoid firing the same
        # "eruption"/"unrest" anomaly on every re-read, we expose *_fresh fields
        # that are non-zero ONLY on the first poll after the report's pubDate
        # changes. Threshold rules fire on the _fresh fields, not the raw levels.
        #
        # Seed from the most recent log on startup so a process RESTART does not
        # treat an already-seen weekly report as "new" (that bug produced a
        # spurious volcanic anomaly on every restart).
        self._last_feed_pub_date: float | None = self._load_last_feed_pub_date()

    @staticmethod
    def _load_last_feed_pub_date() -> float | None:
        """Read the newest volcanic log record's feed_pub_date_unix, if any."""
        import glob
        import os
        try:
            files = sorted(glob.glob("logs/volcanic_activity/*.jsonl"))
            if not files:
                return None
            last_line = None
            with open(files[-1]) as f:
                for line in f:
                    if line.strip():
                        last_line = line
            if last_line:
                rec = json.loads(last_line)
                fpd = rec.get("feed_pub_date_unix")
                return float(fpd) if fpd else None
        except Exception:
            return None
        return None

    async def collect(self) -> SensorReading:
        async with aiohttp.ClientSession() as session:
            async with session.get(FEED_URL, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"volcanic feed HTTP {resp.status}")
                # The Smithsonian feed is encoded ISO-8859-1; pass raw bytes so
                # ElementTree honours the declaration instead of mis-decoding.
                raw = await resp.read()

        summary = parse_volcano_feed(raw)
        feed_pub = float(summary["feed_pub_date_unix"])

        # Is this a genuinely new weekly report (vs a re-read of the same one)?
        is_new_report = feed_pub > 0 and feed_pub != self._last_feed_pub_date
        self._last_feed_pub_date = feed_pub

        # _fresh fields carry the counts ONLY on a new report; otherwise 0 so the
        # threshold rules stay quiet between weekly updates.
        fresh_eruptions = int(summary["new_eruptions"]) if is_new_report else 0
        fresh_unrest = int(summary["new_unrest"]) if is_new_report else 0

        # Promote numeric fields to top-level so the threshold detector can iterate them.
        return SensorReading.create(
            source="volcanic_activity",
            data={
                # Raw weekly levels — kept for storage/display (NOT used by rules).
                "active_count": int(summary["active_count"]),
                "new_eruptions": int(summary["new_eruptions"]),
                "new_unrest": int(summary["new_unrest"]),
                "continuing_eruptions": int(summary["continuing_eruptions"]),
                "feed_pub_date_unix": feed_pub,
                # Anomaly signals — non-zero only when a NEW weekly report lands.
                "new_eruptions_fresh": fresh_eruptions,
                "new_unrest_fresh": fresh_unrest,
                "report_is_new": 1 if is_new_report else 0,
                "volcanoes": summary["volcanoes"],
                "fetched_at_utc": datetime.now(tz=timezone.utc).isoformat(),
            },
        )

    def get_schema(self) -> dict[str, type]:
        return {
            "active_count": int,
            "new_eruptions": int,
            "new_unrest": int,
            "continuing_eruptions": int,
            "feed_pub_date_unix": float,
            "new_eruptions_fresh": int,
            "new_unrest_fresh": int,
            "report_is_new": int,
        }
