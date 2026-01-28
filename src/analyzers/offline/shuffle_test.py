"""Shuffle test (control group) for cluster synchronicity.

Scientific purpose: decide whether the multi-domain clusters the system reports
are more frequent than expected by chance. We compare the observed number of
Level 3+ moments (>=3 distinct sources anomalous within the cluster window)
against a null distribution produced by *circularly time-shifting each source
independently*.

Why circular shift: it preserves each stream's own anomaly rate and internal
temporal structure (autocorrelation, bursts) and destroys ONLY the cross-source
phase alignment. So it isolates exactly the thing we claim to detect —
synchronization between independent streams — without giving the null an unfair
disadvantage. This is the honest, conservative control.

If observed ≈ null  -> the clusters are what randomness produces (no signal).
If observed >> null  -> synchronization exceeds chance (a provisional finding).

Run on real logs:
    python -m src.analyzers.offline.shuffle_test --days 30 --iter 500
"""

from __future__ import annotations

import json
import math
import random
import statistics
from collections import Counter, deque
from pathlib import Path
from typing import Any


def extract_anomalies(records: list[dict[str, Any]]) -> list[tuple[float, str]]:
    """Pull (timestamp, sensor_source) from individual anomaly records.

    Cluster summary records (those with a ``cluster`` key) are skipped — we want
    the raw per-sensor anomaly events.
    """
    events: list[tuple[float, str]] = []
    for r in records:
        if "cluster" in r:
            continue
        src = r.get("sensor_source")
        ts = r.get("timestamp")
        if src and isinstance(ts, (int, float)):
            events.append((float(ts), src))
    events.sort(key=lambda e: e[0])
    return events


def count_l3plus(events: list[tuple[float, str]], window: float) -> tuple[int, Counter]:
    """Count Level 3+ moments using event time as the clock.

    Mirrors the live detector: for each anomaly, look at the trailing ``window``
    and count distinct sources present. The level for that moment is that count
    (capped at 5). Returns (number of L3+ moments, Counter of level -> moments).

    O(n) via a sliding window over chronologically sorted events.
    """
    win: deque[tuple[float, str]] = deque()
    in_window: Counter = Counter()
    distinct = 0
    by_level: Counter = Counter()
    l3plus = 0

    for ts, src in events:
        # evict events older than the window
        while win and ts - win[0][0] >= window:
            old_ts, old_src = win.popleft()
            in_window[old_src] -= 1
            if in_window[old_src] == 0:
                distinct -= 1
        # add current event
        if in_window[src] == 0:
            distinct += 1
        in_window[src] += 1
        win.append((ts, src))

        level = min(distinct, 5)
        by_level[level] += 1
        if level >= 3:
            l3plus += 1

    return l3plus, by_level


def circular_shift(events: list[tuple[float, str]], rng: random.Random) -> list[tuple[float, str]]:
    """Circularly shift each source's anomaly times by an independent random offset.

    Preserves per-source count and inter-event structure; randomizes only the
    phase relative to other sources.
    """
    if not events:
        return []
    t_min = events[0][0]
    t_max = events[-1][0]
    span = t_max - t_min
    if span <= 0:
        return list(events)

    by_source: dict[str, list[float]] = {}
    for ts, src in events:
        by_source.setdefault(src, []).append(ts)

    shifted: list[tuple[float, str]] = []
    for src, times in by_source.items():
        offset = rng.uniform(0, span)
        for ts in times:
            new_ts = t_min + ((ts - t_min + offset) % span)
            shifted.append((new_ts, src))

    shifted.sort(key=lambda e: e[0])
    return shifted


# Polling intervals of the enabled sensors. The schedule-aware null uses these
# to shift each source by an *integer multiple* of its polling period — so a
# source remains on its sampling grid (and any sources that were polling in
# lockstep stay locked in the null too).
SENSOR_PERIODS: dict[str, float] = {
    "crypto":        2.0,
    "blockchain":   10.0,
    "earthquake":   60.0,
    "space_weather": 300.0,
    "quantum_rng":   300.0,
    "news":         900.0,
    "weather":     1800.0,
}


def schedule_aware_shift(
    events: list[tuple[float, str]],
    rng: random.Random,
    periods: dict[str, float] | None = None,
) -> list[tuple[float, str]]:
    """Per-source circular shift by an *integer multiple* of the sampling period.

    Difference from :func:`circular_shift`: each source is shifted by ``k * p``
    where ``p`` is the source's polling period and ``k`` is a random integer.
    This keeps each source on its original sampling grid. Crucially, two sources
    with the SAME period that were polling in lockstep originally remain in
    lockstep in the null. Any L3+ excess that survives THIS null can no longer
    be blamed on synchronized polling — it would point to a real cross-domain
    coincidence above and beyond the scheduling artefact.

    A source whose period is missing from ``periods`` falls back to the
    continuous ``circular_shift`` behaviour for that source only.
    """
    if not events:
        return []
    periods = periods or SENSOR_PERIODS
    t_min = events[0][0]
    t_max = events[-1][0]
    span = t_max - t_min
    if span <= 0:
        return list(events)

    by_source: dict[str, list[float]] = {}
    for ts, src in events:
        by_source.setdefault(src, []).append(ts)

    shifted: list[tuple[float, str]] = []
    for src, times in by_source.items():
        p = periods.get(src)
        if p is None or p <= 0 or p >= span:
            # unknown / huge period — fall back to continuous shift
            offset = rng.uniform(0, span)
        else:
            # use the largest multiple of p that fits inside the span so that
            # the wrap-around lands back on the same sampling grid
            n_periods = int(span // p)
            k = rng.randint(0, max(1, n_periods - 1))
            offset = k * p
        for ts in times:
            new_ts = t_min + ((ts - t_min + offset) % span)
            shifted.append((new_ts, src))

    shifted.sort(key=lambda e: e[0])
    return shifted


def run_shuffle_test(
    events: list[tuple[float, str]],
    window: float = 30.0,
    n_iter: int = 500,
    seed: int = 12345,
    null_mode: str = "circular",
) -> dict[str, Any]:
    """Compare observed L3+ count against a shuffle null distribution.

    ``null_mode``:
      - ``"circular"`` (default): each source is shifted by a continuous random
        offset. Tests "is there any cross-source synchronisation?".
      - ``"schedule_aware"``: each source is shifted by an integer multiple of
        its sampling period. Sources that share a period (and were polled in
        lockstep) STAY in lockstep in the null, so any L3+ excess that survives
        cannot be the scheduling artefact.
    """
    rng = random.Random(seed)

    observed, by_level = count_l3plus(events, window)

    if null_mode == "schedule_aware":
        shift_fn = schedule_aware_shift
    elif null_mode == "circular":
        shift_fn = circular_shift
    else:
        raise ValueError(f"unknown null_mode: {null_mode!r}")

    null_samples: list[int] = []
    for _ in range(n_iter):
        shuffled = shift_fn(events, rng)
        null_samples.append(count_l3plus(shuffled, window)[0])

    null_mean = statistics.fmean(null_samples) if null_samples else 0.0
    null_std = statistics.pstdev(null_samples) if len(null_samples) > 1 else 0.0
    # empirical one-sided p-value: P(null >= observed)
    ge = sum(1 for s in null_samples if s >= observed)
    p_value = (ge + 1) / (n_iter + 1)
    z = (observed - null_mean) / null_std if null_std > 0 else math.inf if observed > null_mean else 0.0

    sources = Counter(src for _, src in events)
    span_days = (events[-1][0] - events[0][0]) / 86400 if len(events) > 1 else 0.0

    if observed > null_mean and p_value < 0.05:
        verdict = "exceeds chance (provisional signal — warrants deeper analysis)"
    elif observed < null_mean and p_value > 0.95:
        verdict = "below chance (clusters rarer than random timing would give)"
    else:
        verdict = "consistent with chance (no detectable synchronization signal)"

    return {
        "window_seconds": window,
        "n_iterations": n_iter,
        "seed": seed,
        "null_mode": null_mode,
        "period_days": round(span_days, 2),
        "n_anomalies": len(events),
        "sources": dict(sources),
        "observed_l3plus": observed,
        "observed_by_level": {str(k): v for k, v in sorted(by_level.items())},
        "null_mean": round(null_mean, 2),
        "null_std": round(null_std, 2),
        "null_min": min(null_samples) if null_samples else 0,
        "null_max": max(null_samples) if null_samples else 0,
        "z_score": round(z, 2) if math.isfinite(z) else None,
        "p_value": round(p_value, 5),
        "verdict": verdict,
    }


def load_anomalies_from_logs(logs_dir: str = "logs/anomalies", days: int = 30) -> list[dict]:
    """Load the most recent ``days`` daily anomaly logs."""
    path = Path(logs_dir)
    if not path.exists():
        return []
    records: list[dict] = []
    for log_file in sorted(path.glob("*.jsonl"), reverse=True)[:days]:
        with open(log_file) as f:
            for line in f:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def _format_report(r: dict[str, Any]) -> str:
    lines = [
        "=== Matrix Watcher — Shuffle Test (cluster synchronicity) ===",
        f"Period analysed : {r['period_days']} days, {r['n_anomalies']} anomalies",
        f"Sources         : {r['sources']}",
        f"Cluster window  : {r['window_seconds']}s | null iters: {r['n_iterations']} (seed {r['seed']})",
        "",
        f"Observed L3+ moments : {r['observed_l3plus']}",
        f"Null (random timing) : {r['null_mean']} ± {r['null_std']}  [range {r['null_min']}..{r['null_max']}]",
        f"z-score              : {r['z_score']}",
        f"p-value (one-sided)  : {r['p_value']}",
        "",
        f"VERDICT: {r['verdict']}",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Shuffle test for cluster synchronicity")
    ap.add_argument("--days", type=int, default=30, help="recent days of logs to analyse")
    ap.add_argument("--window", type=float, default=30.0, help="cluster window seconds")
    ap.add_argument("--iter", type=int, default=500, help="null iterations")
    ap.add_argument("--seed", type=int, default=12345)
    ap.add_argument("--logs", default="logs/anomalies")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of text")
    ap.add_argument(
        "--null",
        choices=["circular", "schedule_aware"],
        default="circular",
        help="null distribution: 'circular' (default) shifts continuously; "
             "'schedule_aware' shifts by multiples of each source's polling period",
    )
    args = ap.parse_args()

    recs = load_anomalies_from_logs(args.logs, args.days)
    events = extract_anomalies(recs)
    if not events:
        print("No anomalies found in logs — nothing to test.")
        raise SystemExit(0)

    report = run_shuffle_test(
        events, window=args.window, n_iter=args.iter, seed=args.seed, null_mode=args.null,
    )
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(_format_report(report))
