"""Add explicit clocks to a stored anomaly without changing its legacy timestamp.

Only a source timestamp actually supplied by the feed is called an event or
publication time. A collector observation must never be silently backdated to
an earlier physical event, particularly in prospective forecasting.
"""

from __future__ import annotations

import math
from numbers import Real

from ...core.types import AnomalyEvent, Event


def _source_time(value: object, observed_at: float) -> float | None:
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    ts = float(value)
    return ts if math.isfinite(ts) and 0 < ts <= observed_at + 120 else None


def anomaly_clocks(anomaly: AnomalyEvent, observation: Event,
                   detected_at: float) -> dict[str, float | str | None]:
    """Return four UTC clocks; absent upstream times remain null, not guessed."""
    observed_at = float(observation.timestamp)
    source_event_at = None
    published_at = None
    if anomaly.sensor_source == "earthquake":
        source_event_at = _source_time(
            (anomaly.metadata or {}).get("event_time"), observed_at)
    elif anomaly.sensor_source == "volcanic_activity":
        # Channel pubDate is the weekly *report* time, not eruption onset.
        published_at = _source_time(
            observation.payload.get("feed_pub_date_unix"), observed_at)
    return {
        "source_event_at": source_event_at,
        "published_at": published_at,
        "observed_at": observed_at,
        "detected_at": float(detected_at),
        "timestamp_basis": "source_event" if source_event_at is not None else "collector_observation",
    }
