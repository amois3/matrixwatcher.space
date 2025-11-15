"""Daily digest generator - turns a day's raw observations into an honest narrative.

The digest is built strictly from recorded data (cluster anomaly logs and the
predictions file). It reports counts, the peak synchronicity reached, which
sources were most active, and the day's standing predictions — with no
causal claims and no invented numbers. A quiet day is reported as a quiet day.
"""

from __future__ import annotations

import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any


# Levels we consider a real multi-domain cluster (matches the public threshold)
MIN_CLUSTER_LEVEL = 3

LEVEL_NAME = {
    3: "Multiple Correlation",
    4: "Strong Correlation",
    5: "Critical Synchronicity",
}

# Plain-language names for sources, used in the human-readable narrative.
SOURCE_LABEL = {
    "crypto": "crypto markets",
    "blockchain": "blockchain activity",
    "weather": "local weather",
    "news": "world news",
    "earthquake": "earthquakes",
    "space_weather": "space weather",
    "quantum_rng": "quantum randomness",
    "solar_activity": "solar activity",
    "volcanic_activity": "volcanic activity",
}


def _label(src: str | None) -> str:
    """Human-friendly name for a sensor source."""
    if not src:
        return "several sources"
    return SOURCE_LABEL.get(src, src.replace("_", " "))


def _cluster_sources(record: dict[str, Any]) -> list[str]:
    """Distinct sensor sources participating in a cluster record."""
    seen: list[str] = []
    for a in record.get("cluster", {}).get("anomalies", []):
        src = a.get("sensor_source")
        if src and src not in seen:
            seen.append(src)
    return seen


def build_digest(
    date_str: str,
    clusters: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    generated_at: float | None = None,
) -> dict[str, Any]:
    """Build an honest daily digest from raw cluster records and predictions.

    Args:
        date_str: The day being summarised (YYYY-MM-DD, UTC).
        clusters: Anomaly-log records for the day (each may carry a
            ``cluster`` block with ``level`` and ``anomalies``).
        predictions: Active prediction entries (from predictions/current.json).
        generated_at: Optional fixed timestamp (for reproducible output/tests).

    Returns:
        Structured digest dict including a human-readable ``narrative``.
    """
    generated_at = generated_at if generated_at is not None else time.time()

    # Keep only genuine multi-domain clusters
    real = [c for c in clusters if c.get("cluster", {}).get("level", 0) >= MIN_CLUSTER_LEVEL]

    by_level: Counter[int] = Counter(c["cluster"]["level"] for c in real)
    source_activity: Counter[str] = Counter()
    for c in real:
        for src in _cluster_sources(c):
            source_activity[src] += 1

    highest_level = max((c["cluster"]["level"] for c in real), default=0)

    # Sources of the strongest cluster of the day (first one that reached the peak)
    highest_level_sources: list[str] = []
    peak_index = 0.0
    peak_status = "normal"
    for c in real:
        idx = c.get("index", {}).get("value", 0) or 0
        if idx > peak_index:
            peak_index = idx
            peak_status = c.get("index", {}).get("status", "normal")
        if c["cluster"]["level"] == highest_level and not highest_level_sources:
            highest_level_sources = _cluster_sources(c)

    most_active_source = source_activity.most_common(1)[0][0] if source_activity else None

    # Best standing prediction (highest probability, then most observations)
    top_prediction = None
    if predictions:
        top = max(
            predictions,
            key=lambda p: (p.get("probability", 0), p.get("observations", 0)),
        )
        top_prediction = {
            "event": top.get("event"),
            "description": top.get("description", top.get("event")),
            "probability": top.get("probability"),
            "observations": top.get("observations"),
            "occurrences": top.get("occurrences"),
            "avg_time_hours": top.get("avg_time_hours"),
        }

    digest = {
        "date": date_str,
        "generated_at": generated_at,
        "clusters_total": len(real),
        "by_level": {str(lvl): by_level.get(lvl, 0) for lvl in (3, 4, 5)},
        "highest_level": highest_level,
        "highest_level_name": LEVEL_NAME.get(highest_level),
        "highest_level_sources": highest_level_sources,
        "most_active_source": most_active_source,
        "source_activity": dict(source_activity),
        "peak_index": round(peak_index, 1),
        "peak_status": peak_status,
        "predictions_count": len(predictions),
        "top_prediction": top_prediction,
        "narrative": _narrative(
            date_str, real, by_level, highest_level, highest_level_sources,
            most_active_source, source_activity, top_prediction,
        ),
    }
    return digest


def _narrative(
    date_str: str,
    real: list[dict[str, Any]],
    by_level: Counter,
    highest_level: int,
    highest_level_sources: list[str],
    most_active_source: str | None,
    source_activity: Counter,
    top_prediction: dict | None,
) -> str:
    """Compose a plain-language narrative a non-technical visitor can read.

    No jargon ("L3", "cluster", "synchronicity"), no leading date, no scary
    percentages — just an honest, calm description of the day in human words.
    """
    if not real:
        return (
            "A calm day. Every source we watch — from crypto markets to "
            "earthquakes, space weather and quantum randomness — stayed within "
            "its normal range, and nothing unusual lined up across different "
            "domains."
        )

    n = len(real)
    occasion = "occasion" if n == 1 else "occasions"

    lead = (
        f"We watched every source through the day. On {n} brief {occasion}, "
        f"three or more unrelated sources showed unusual readings within the "
        f"same half-minute"
    )
    if most_active_source:
        lead += f" — most often involving {_label(most_active_source)}"
    lead += "."

    # If the strongest moment pulled in even more sources, name them plainly.
    if highest_level >= 4 and highest_level_sources:
        names = [_label(s) for s in highest_level_sources]
        if len(names) > 1:
            joined = ", ".join(names[:-1]) + " and " + names[-1]
        else:
            joined = names[0]
        lead += f" The strongest of these brought together {joined}."

    tail = (
        " Coincidences like these also happen by chance, so we record them "
        "honestly without claiming any connection."
    )
    return lead + tail


def date_str_utc(ts: float | None = None) -> str:
    """Return the UTC date string (YYYY-MM-DD) for a timestamp (default: now)."""
    dt = datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")
