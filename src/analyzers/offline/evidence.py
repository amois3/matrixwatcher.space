"""Exploratory, domain-aware coincidence analysis across fixed time windows.

The null shifts entire domain event trains by whole UTC days within each month.
This preserves each domain's bursts, polling phase and time-of-day structure.
The windows and method are frozen here, but past data inspected during design
remain exploratory. This module never promotes a result to a forecast.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from ..online.cluster_detector import source_domain
from .shuffle_test import extract_anomalies, load_anomalies_from_logs

WINDOWS = (30, 300, 900)
DETECTOR_EPOCH = 1780254840.0


def domain_events(records: list[dict], after: float = DETECTOR_EPOCH) -> list[tuple[float, str]]:
    return sorted((ts, source_domain(source)) for ts, source in extract_anomalies(records) if ts >= after)


def count_episodes(events: list[tuple[float, str]], window: float) -> int:
    """Count transitions into a >=3-domain overlap, not every repeated poll."""
    queue: deque[tuple[float, str]] = deque()
    domains: Counter[str] = Counter()
    in_episode = False
    episodes = 0
    for ts, domain in events:
        while queue and ts - queue[0][0] >= window:
            _, old_domain = queue.popleft()
            domains[old_domain] -= 1
            if domains[old_domain] == 0:
                del domains[old_domain]
        if len(domains) < 3:
            in_episode = False
        queue.append((ts, domain))
        domains[domain] += 1
        if len(domains) >= 3 and not in_episode:
            episodes += 1
            in_episode = True
    return episodes


def shift_within_months(events: list[tuple[float, str]], rng: random.Random) -> list[tuple[float, str]]:
    """Shift each domain by an independent number of whole days per UTC month."""
    groups: dict[tuple[int, int], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for ts, domain in events:
        dt = datetime.fromtimestamp(ts, timezone.utc)
        groups[(dt.year, dt.month)][domain].append(ts)
    shifted = []
    for (year, month), by_domain in groups.items():
        all_times = [ts for times in by_domain.values() for ts in times]
        month_start = min(int(ts // 86400) for ts in all_times) * 86400
        days = max(1, max(int(ts // 86400) for ts in all_times) - int(month_start // 86400) + 1)
        span = days * 86400
        for domain, times in by_domain.items():
            offset = rng.randrange(days) * 86400
            shifted.extend((month_start + ((ts - month_start + offset) % span), domain) for ts in times)
    shifted.sort()
    return shifted


def _holm_adjust(p_values: list[float]) -> list[float]:
    adjusted = [1.0] * len(p_values)
    running = 0.0
    for rank, index in enumerate(sorted(range(len(p_values)), key=p_values.__getitem__)):
        running = max(running, p_values[index] * (len(p_values) - rank))
        adjusted[index] = min(1.0, running)
    return adjusted


def analyze(events: list[tuple[float, str]], iterations: int = 500, seed: int = 20260923) -> dict:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    events = sorted(events)
    observed = [count_episodes(events, window) for window in WINDOWS]
    samples = [[] for _ in WINDOWS]
    rng = random.Random(seed)
    for _ in range(iterations):
        shifted = shift_within_months(events, rng)
        for index, window in enumerate(WINDOWS):
            samples[index].append(count_episodes(shifted, window))
    raw_p = [(1 + sum(value >= count for value in null)) / (iterations + 1)
             for count, null in zip(observed, samples)]
    adjusted = _holm_adjust(raw_p)
    return {
        "generated_at": time.time(),
        "period_start": events[0][0] if events else None,
        "period_end": events[-1][0] if events else None,
        "anomaly_records": len(events),
        "domains": dict(Counter(domain for _, domain in events)),
        "method": "whole-day circular shifts per domain within each UTC month",
        "status": "exploratory_not_validated",
        "windows": [
            {"seconds": window, "observed_episodes": count,
             "null_mean_episodes": round(sum(null) / iterations, 2),
             "p_value": round(p, 4), "p_holm": round(p_adj, 4)}
            for window, count, null, p, p_adj in zip(WINDOWS, observed, samples, raw_p, adjusted)
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", default="logs/anomalies")
    parser.add_argument("--out", default="logs/evidence/current.json")
    parser.add_argument("--days", type=int, default=120)
    parser.add_argument("--iterations", type=int, default=500)
    args = parser.parse_args()
    records = load_anomalies_from_logs(args.logs, args.days)
    events = domain_events(records, after=max(DETECTOR_EPOCH, time.time() - args.days * 86400))
    report = analyze(events, iterations=args.iterations)
    destination = Path(args.out)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    temporary.write_text(json.dumps(report, indent=2))
    temporary.replace(destination)
    print(json.dumps({"anomaly_records": report["anomaly_records"], "windows": report["windows"]}))


if __name__ == "__main__":
    main()
