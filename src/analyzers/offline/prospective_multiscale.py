"""Frozen, coverage-gated future screen of *observation-time* coincidences.

This is an exploratory screen, not a test of physical simultaneity or a claim
about simulation. No interim significance is published. The day-level coverage
gate avoids placing control events on unobserved days; it cannot account for
within-day outages or upstream publication delays, so independent replication
is required even if the endpoint screen looks promising.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .evidence import _holm_adjust, count_episodes
from .quality_atlas import scan_day
from .shuffle_test import load_anomalies_from_logs

VERSION = "observation-coincidence-v2-20260925"
START_AT = datetime(2026, 9, 25, tzinfo=timezone.utc).timestamp()
END_AT = START_AT + 120 * 86400
WINDOWS = (30, 300, 900)
MIN_DAILY_POLL_COVERAGE = 0.30
MIN_ELIGIBLE_DAYS = 90
MIN_MONTH_DAYS_FOR_SHIFT = 4
ITERATIONS = 2000
SEED = 20260924

# Weekly volcanic reports have no eruption-onset time. The remaining events
# are compared by collector observation time, not asserted physical event time.
DOMAIN_SOURCES = {
    "markets": ("crypto",),
    "blockchain": ("blockchain",),
    "geophysics": ("earthquake",),
    "atmosphere": ("weather",),
    "heliophysics": ("solar_activity", "solar_wind", "space_weather"),
    "human_activity": ("news", "wikipedia_edits"),
    "quantum": ("quantum_rng",),
}
SOURCE_DOMAINS = {source: domain for domain, sources in DOMAIN_SOURCES.items()
                  for source in sources}


def _day(ts: float) -> date:
    return datetime.fromtimestamp(ts, timezone.utc).date()


def usable_domain_days(logs: Path, config: dict) -> dict[str, set[date]]:
    """Require every stream in a domain to have substantial usable daily bins."""
    sensor_days: dict[str, set[date]] = {}
    begin = _day(START_AT)
    for source in SOURCE_DOMAINS:
        interval = float(config.get("sensors", {}).get(source, {}).get("interval_seconds", 300))
        good = set()
        for offset in range(120):
            day = begin + timedelta(days=offset)
            start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
            row = scan_day(logs / source / f"{day.isoformat()}.jsonl", source, interval, start)
            if row["poll_coverage"] >= MIN_DAILY_POLL_COVERAGE:
                good.add(day)
        sensor_days[source] = good
    return {domain: set.intersection(*(sensor_days[source] for source in sources))
            for domain, sources in DOMAIN_SOURCES.items()}


def eligible_observations(records: list[dict], days: dict[str, set[date]]) -> tuple[list[tuple[float, str]], dict]:
    """Use only prospectively stored collector clocks on eligible source days."""
    events = []
    excluded = defaultdict(int)
    for record in records:
        if "cluster" in record:
            continue
        source = record.get("sensor_source")
        if source not in SOURCE_DOMAINS:
            continue
        ts = record.get("observed_at")
        if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not math.isfinite(ts):
            excluded["missing_observation_clock"] += 1
            continue
        ts = float(ts)
        if not START_AT <= ts < END_AT:
            continue
        domain = SOURCE_DOMAINS[source]
        if _day(ts) not in days[domain]:
            excluded["ineligible_day"] += 1
            continue
        if source == "quantum_rng" and (record.get("metadata") or {}).get("measurement_source") != "anu_quantum":
            excluded["unverified_quantum"] += 1
            continue
        if source == "earthquake":
            origin = record.get("source_event_at")
            if not isinstance(origin, (int, float)) or not 0 <= ts - origin <= 900:
                excluded["late_or_unknown_earthquake_origin"] += 1
                continue
        events.append((ts, domain))
    return sorted(events), dict(excluded)


def _complete_month_days(days: dict[str, set[date]]) -> dict[tuple[str, int, int], list[date]]:
    groups: dict[tuple[str, int, int], list[date]] = defaultdict(list)
    for domain, good in days.items():
        for day in good:
            groups[(domain, day.year, day.month)].append(day)
    return {key: sorted(group) for key, group in groups.items()
            if len(group) >= MIN_MONTH_DAYS_FOR_SHIFT}


def rotate_eligible_days(events: list[tuple[float, str]], days: dict[str, set[date]],
                         rng: random.Random) -> list[tuple[float, str]]:
    """Rotate whole UTC day profiles among the domain's eligible days/month."""
    groups = _complete_month_days(days)
    offsets = {key: rng.randrange(1, len(group)) for key, group in groups.items()}
    positions = {key: {day: i for i, day in enumerate(group)} for key, group in groups.items()}
    shifted = []
    for ts, domain in events:
        day = _day(ts)
        key = (domain, day.year, day.month)
        if key not in groups or day not in positions[key]:
            continue
        target = groups[key][(positions[key][day] + offsets[key]) % len(groups[key])]
        target_start = datetime(target.year, target.month, target.day, tzinfo=timezone.utc).timestamp()
        source_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
        shifted.append((target_start + ts - source_start, domain))
    return sorted(shifted)


def endpoint_analysis(events: list[tuple[float, str]], days: dict[str, set[date]],
                      iterations: int = ITERATIONS, seed: int = SEED) -> dict:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    eligible = {domain for domain, good in days.items() if len(good) >= MIN_ELIGIBLE_DAYS}
    month_days = _complete_month_days(days)
    events = [(ts, domain) for ts, domain in events
              if domain in eligible and _day(ts) in month_days.get((domain, _day(ts).year, _day(ts).month), ())]
    if len(eligible) < 3 or len({domain for _, domain in events}) < 3:
        return {"status": "insufficient_coverage_or_events_no_test",
                "eligible_domains": sorted(eligible), "eligible_events": len(events)}
    observed = [count_episodes(events, window) for window in WINDOWS]
    samples = [[] for _ in WINDOWS]
    rng = random.Random(seed)
    for _ in range(iterations):
        shifted = rotate_eligible_days(events, days, rng)
        for i, window in enumerate(WINDOWS):
            samples[i].append(count_episodes(shifted, window))
    raw_p = [(1 + sum(n >= count for n in null)) / (iterations + 1)
             for count, null in zip(observed, samples)]
    adjusted = _holm_adjust(raw_p)
    return {"status": "exploratory_endpoint_screen_not_validated",
            "eligible_domains": sorted(eligible), "eligible_events": len(events),
            "iterations": iterations, "seed": seed,
            "windows": [{"seconds": window, "observed_episodes": count,
                         "null_mean_episodes": round(sum(null) / iterations, 2),
                         "p_value": round(p, 4), "p_holm": round(adj, 4)}
                        for window, count, null, p, adj in zip(WINDOWS, observed, samples, raw_p, adjusted)]}


def build_report(now: float, logs: Path, anomaly_logs: Path, config: dict,
                 final_path: Path) -> dict:
    common = {
        "study_version": VERSION, "generated_at": now,
        "start_at": START_AT, "end_at": END_AT,
        "complete_days": max(0, min(120, int((now - START_AT) // 86400))),
        "time_basis": "collector observed_at, never the mixed legacy timestamp",
        "windows_seconds": list(WINDOWS),
        "quality_gate": {"daily_usable_poll_bins_min": MIN_DAILY_POLL_COVERAGE,
                         "eligible_days_per_domain_min": MIN_ELIGIBLE_DAYS,
                         "eligible_days_per_month_min": MIN_MONTH_DAYS_FOR_SHIFT},
        "exclusions": "weekly volcano reports and context feeds; earthquakes reported >15 minutes after origin; unverified quantum samples",
        "caveat": "Day-level coverage matching does not remove within-day outages, publication lag or shared upstream failures. An endpoint excess needs independent replication.",
    }
    if now < END_AT:
        return {**common, "status": "collecting_no_interim_test"}
    try:
        return json.loads(final_path.read_text())
    except (OSError, json.JSONDecodeError):
        pass
    days = usable_domain_days(logs, config)
    scan_days = max(123, math.ceil((now - START_AT) / 86400) + 2)
    records = load_anomalies_from_logs(anomaly_logs, scan_days)
    events, excluded = eligible_observations(records, days)
    result = {**common, "coverage_days": {name: len(good) for name, good in days.items()},
              "excluded_records": excluded, "analysis": endpoint_analysis(events, days)}
    result["status"] = result["analysis"]["status"]
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = final_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, separators=(",", ":")))
    temporary.replace(final_path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", type=Path, default=Path("logs"))
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    parser.add_argument("--out", type=Path, default=Path("logs/research/prospective-multiscale.json"))
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    report = build_report(time.time(), args.logs, args.logs / "anomalies", config,
                          args.out.with_name("prospective-multiscale-final.json"))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.out.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, separators=(",", ":")))
    temporary.replace(args.out)
    print(json.dumps({"study_version": VERSION, "status": report["status"]}))


if __name__ == "__main__":
    main()
