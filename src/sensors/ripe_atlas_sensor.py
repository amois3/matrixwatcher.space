"""Fixed RIPE Atlas K-root ping panel: network context, not anomaly evidence.

Measurement 1001 is an IPv4 ping of anycast k.root-servers.net every 240 s.
These probes are hosted by independent volunteers, but all share a target and
the public RIPE API. Regional observations are therefore not independent
global-outage votes. Probe IDs and regions are frozen here for auditability.
"""

from __future__ import annotations

import statistics
import time

import aiohttp

from .base import BaseSensor, SensorConfig
from ..core.types import SensorReading

MEASUREMENT_ID = 1001
API_URL = f"https://atlas.ripe.net/api/v2/measurements/{MEASUREMENT_ID}/latest/"
MAX_AGE_SECONDS = 900  # 240 s measurement interval plus up to 5 min API cache.
PROBES = {
    "Germany": (1017807, 1017804, 1017780),
    "United States": (1017838, 1017836, 1017788),
    "Brazil": (1017693, 1017629, 1017331),
    "South Africa": (1017682, 1017588, 1017566),
    "Japan": (1017728, 1017691, 1017681),
    "Australia": (1017761, 1017714, 1017700),
}


def summarize_results(payload: list | dict, now: float) -> dict:
    """Handle observed list format and the documented keyed format."""
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = [item for values in payload.values() if isinstance(values, list)
                for item in values[:1]]
    else:
        raise ValueError("Unexpected RIPE Atlas response shape")
    by_probe = {row.get("prb_id"): row for row in rows if isinstance(row, dict)}
    regions = {}
    missing_regions = []
    total_fresh = 0
    for name, probe_ids in PROBES.items():
        probes = []
        latencies = []
        for probe_id in probe_ids:
            row = by_probe.get(probe_id)
            ts = row.get("timestamp") if row else None
            age = max(0, now - ts) if isinstance(ts, (int, float)) else None
            if row is None or row.get("msm_id") != MEASUREMENT_ID:
                probes.append({"id": probe_id, "status": "missing"})
                continue
            if age is None or age > MAX_AGE_SECONDS or ts > now + 60:
                probes.append({"id": probe_id, "status": "stale", "age_seconds": round(age) if age is not None else None})
                continue
            sent = row.get("sent")
            received = row.get("rcvd")
            if not isinstance(sent, int) or sent <= 0 or not isinstance(received, int):
                probes.append({"id": probe_id, "status": "invalid"})
                continue
            latency = row.get("avg")
            valid_latency = bool(received and isinstance(latency, (int, float)) and latency >= 0)
            if valid_latency:
                latencies.append(float(latency))
            probes.append({"id": probe_id, "status": "ok", "age_seconds": round(age),
                           "latency_ms": round(latency, 2) if valid_latency else None,
                           "packet_loss": round(max(0, min(1, 1 - received / sent)), 3)})
        fresh = sum(probe["status"] == "ok" for probe in probes)
        total_fresh += fresh
        if fresh < 2:
            missing_regions.append(name)
        regions[name] = {"fresh_probes": fresh, "expected_probes": len(probe_ids),
                         "median_latency_ms": round(statistics.median(latencies), 2) if latencies else None,
                         "probes": probes}
    return {"regions": regions, "fresh_probes": total_fresh,
            "expected_probes": sum(map(len, PROBES.values())),
            "quality": {"complete": not missing_regions,
                        "missing_fields": [f"{name}: fewer than 2 fresh probes" for name in missing_regions]}}


class RipeAtlasSensor(BaseSensor):
    def __init__(self, config: SensorConfig | None = None):
        # No event bus: this is observational context until a separate study
        # specifies baselines, controls and prospectively frozen event rules.
        super().__init__("ripe_atlas", config, event_bus=None)

    async def collect(self) -> SensorReading:
        ids = ",".join(str(probe) for probes in PROBES.values() for probe in probes)
        async with aiohttp.ClientSession(headers={"User-Agent": "MatrixWatcher/1.0 (matrixwatcher.space)"}) as session:
            async with session.get(API_URL, params={"probe_ids": ids},
                                   timeout=aiohttp.ClientTimeout(total=20)) as response:
                if response.status != 200:
                    raise RuntimeError(f"RIPE Atlas API HTTP {response.status}")
                payload = await response.json()
        summary = summarize_results(payload, time.time())
        if not summary["fresh_probes"]:
            raise RuntimeError("RIPE Atlas returned no fresh measurements")
        return SensorReading.create("ripe_atlas", {
            **summary, "context_only": True, "measurement_id": MEASUREMENT_ID,
            "target": "k.root-servers.net (anycast IPv4)",
        })

    def get_schema(self) -> dict[str, type]:
        return {"regions": dict, "fresh_probes": int, "context_only": bool}
