"""Out-of-sample backtest for condition -> crypto-move predictions.

The honest question is not "is our Brier score low?" but "do our predictions beat
the base rate?". A forecaster that always outputs the long-run frequency already
gets a decent Brier score; skill means doing better than that, out of sample.

Method (strict walk-forward, no lookahead):
  - Walk conditions (anomaly clusters) in chronological order.
  - For each condition, predict P(event) using ONLY earlier conditions of the
    same key (its observed rate so far). Before any history exists, fall back to
    the running global rate.
  - The actual outcome is measured from real BTC prices: did BTC move by at least
    ``threshold_pct`` (absolute) within ``horizon_hours`` after the condition?
  - Score the system with the Brier score and compare to a baseline that always
    predicts the running global base rate.

  skill = 1 - Brier_system / Brier_baseline
      > 0  : the conditions carry predictive information beyond the base rate
      <= 0 : no skill — predictions are not better than guessing the base rate

Run on real logs:
    python -m src.analyzers.offline.backtest --days 14 --horizon 1 --threshold 2.0
"""

from __future__ import annotations

import bisect
import json
import math
import random
from pathlib import Path
from typing import Any, Callable


# ----------------------------- data loading --------------------------------

def load_price_series(logs_dir: str = "logs/crypto", days: int = 14, symbol: str = "BTCUSDT") -> list[tuple[float, float]]:
    """Stream crypto logs into a sorted [(timestamp, price)] series (memory-light)."""
    path = Path(logs_dir)
    series: list[tuple[float, float]] = []
    if not path.exists():
        return series
    for log_file in sorted(path.glob("*.jsonl"), reverse=True)[:days]:
        with open(log_file) as f:
            for line in f:
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = d.get("timestamp")
                for p in d.get("pairs", []):
                    if p.get("symbol") == symbol and p.get("price", 0) > 0 and isinstance(ts, (int, float)):
                        series.append((float(ts), float(p["price"])))
                        break
    series.sort(key=lambda x: x[0])
    return series


def load_conditions(
    logs_dir: str = "logs/anomalies",
    days: int = 14,
    min_level: int = 2,
    exclude_sources: set[str] | None = None,
) -> list[tuple[float, str]]:
    """Stream cluster records into sorted [(timestamp, condition_key)].

    condition_key = L{level}_{sorted distinct sources} — the same keying the live
    pattern tracker uses.

    ``exclude_sources``: drop any cluster that contains one of these sources.
    Use ``{"crypto"}`` to test the real cross-domain thesis (does a cluster of
    NON-crypto streams predict a crypto move?) without the trivial
    crypto-anomaly-predicts-crypto-move circularity.
    """
    exclude_sources = exclude_sources or set()
    path = Path(logs_dir)
    conditions: list[tuple[float, str]] = []
    if not path.exists():
        return conditions
    for log_file in sorted(path.glob("*.jsonl"), reverse=True)[:days]:
        with open(log_file) as f:
            for line in f:
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cluster = d.get("cluster")
                if not cluster:
                    continue
                level = cluster.get("level", 0)
                if level < min_level:
                    continue
                ts = d.get("timestamp")
                if not isinstance(ts, (int, float)):
                    continue
                sources = sorted(set(
                    a.get("sensor_source", "") for a in cluster.get("anomalies", [])
                ))
                if exclude_sources & set(sources):
                    continue
                conditions.append((float(ts), f"L{level}_{'_'.join(sources)}"))
    conditions.sort(key=lambda x: x[0])
    return conditions


# ----------------------------- outcome model -------------------------------

def make_btc_move_outcome(
    price_series: list[tuple[float, float]],
    horizon_hours: float,
    threshold_pct: float,
) -> Callable[[float], int | None]:
    """Return outcome(t) -> 1 if BTC moved >= threshold within horizon, else 0.

    Returns None when there isn't enough future price data to judge (so the
    condition is excluded from scoring rather than counted as a miss).
    """
    times = [t for t, _ in price_series]
    horizon_s = horizon_hours * 3600.0

    def outcome(t: float) -> int | None:
        i = bisect.bisect_right(times, t) - 1
        if i < 0:
            return None
        base_price = price_series[i][1]
        end = t + horizon_s
        # need price coverage up to the horizon
        if times[-1] < end:
            return None
        moved = 0
        j = i + 1
        while j < len(price_series) and price_series[j][0] <= end:
            change = abs((price_series[j][1] - base_price) / base_price) * 100.0
            if change >= threshold_pct:
                moved = 1
                break
            j += 1
        return moved

    return outcome


# ----------------------------- symmetric outcome (any source) ---------------

def load_anomaly_events_by_source(
    logs_dir: str = "logs/anomalies", days: int = 200,
) -> dict[str, list[float]]:
    """Stream individual anomaly records and return ``{source: sorted_timestamps}``.

    Used as the universe of "did source X have any anomaly within horizon" —
    the symmetric replacement for the BTC-only outcome. Cluster summary records
    are skipped; only the per-sensor anomaly events count.
    """
    path = Path(logs_dir)
    if not path.exists():
        return {}
    by_source: dict[str, list[float]] = {}
    for log_file in sorted(path.glob("*.jsonl"), reverse=True)[:days]:
        with open(log_file) as f:
            for line in f:
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "cluster" in d:
                    continue
                src = d.get("sensor_source")
                ts = d.get("timestamp")
                if src and isinstance(ts, (int, float)):
                    by_source.setdefault(src, []).append(float(ts))
    for src in by_source:
        by_source[src].sort()
    return by_source


def make_anomaly_outcome(
    events_by_source: dict[str, list[float]],
    target_source: str,
    horizon_hours: float,
) -> Callable[[float], int | None]:
    """``outcome(t) -> 1`` if ``target_source`` had any anomaly in ``(t, t+horizon]``.

    This is the cross-domain symmetric outcome: every source can be a target.
    Returns ``None`` when the event timeline doesn't cover the horizon (so the
    condition is excluded from scoring rather than counted as a miss).
    """
    target_times = events_by_source.get(target_source, [])
    horizon_s = horizon_hours * 3600.0
    last_ts = max((max(ts) for ts in events_by_source.values() if ts), default=0.0)

    def outcome(t: float) -> int | None:
        end = t + horizon_s
        if end > last_ts:
            return None  # not enough future coverage to score honestly
        if not target_times:
            return 0
        # any target event strictly after t and within horizon?
        i = bisect.bisect_right(target_times, t)
        return 1 if i < len(target_times) and target_times[i] <= end else 0

    return outcome


# ----------------------------- walk-forward --------------------------------

def block_bootstrap_skill(
    sys_sq: list[float],
    base_sq: list[float],
    block_size: int,
    n_boot: int,
    seed: int = 2024,
) -> dict[str, Any]:
    """Moving-block bootstrap CI for the skill score.

    Resamples *contiguous blocks* of the per-condition squared-error pairs to
    respect time autocorrelation (an ordinary i.i.d. bootstrap would understate
    the variance and overstate significance). Returns a 95% CI for the skill and
    the fraction of resamples with skill <= 0.
    """
    n = len(sys_sq)
    if n == 0:
        return {"skill_ci_low": None, "skill_ci_high": None, "p_value": None, "significant": False}
    block_size = max(1, min(block_size, n))
    n_blocks = math.ceil(n / block_size)
    max_start = n - block_size
    rng = random.Random(seed)

    skills: list[float] = []
    for _ in range(n_boot):
        s_sum = 0.0
        b_sum = 0.0
        for _ in range(n_blocks):
            start = rng.randint(0, max_start) if max_start > 0 else 0
            s_sum += sum(sys_sq[start:start + block_size])
            b_sum += sum(base_sq[start:start + block_size])
        if b_sum > 0:
            skills.append(1 - s_sum / b_sum)
    if not skills:
        return {"skill_ci_low": None, "skill_ci_high": None, "p_value": None, "significant": False}

    skills.sort()
    lo = skills[int(0.025 * len(skills))]
    hi = skills[min(len(skills) - 1, int(0.975 * len(skills)))]
    p_value = sum(1 for s in skills if s <= 0) / len(skills)
    return {
        "skill_ci_low": round(lo, 4),
        "skill_ci_high": round(hi, 4),
        "p_value": round(p_value, 4),
        "significant": lo > 0,
        "block_size": block_size,
        "n_boot": n_boot,
    }


def walk_forward(
    conditions: list[tuple[float, str]],
    outcome_fn: Callable[[float], int | None],
    bootstrap: int = 0,
) -> dict[str, Any]:
    """Strict walk-forward scoring: system (per-key rate) vs base-rate baseline.

    If ``bootstrap`` > 0, also report a moving-block bootstrap CI for the skill.
    """
    key_hits: dict[str, int] = {}
    key_n: dict[str, int] = {}
    global_hits = 0
    global_n = 0

    sys_sq: list[float] = []   # per-condition squared error, system
    base_sq: list[float] = []  # per-condition squared error, baseline
    hits_total = 0

    for ts, key in conditions:
        o = outcome_fn(ts)
        if o is None:
            continue  # not enough future data to judge

        # prediction uses ONLY information available before this condition
        if key_n.get(key, 0) > 0:
            p_sys = key_hits[key] / key_n[key]
        elif global_n > 0:
            p_sys = global_hits / global_n
        else:
            p_sys = 0.5  # no information yet
        p_base = (global_hits / global_n) if global_n > 0 else 0.5

        sys_sq.append((p_sys - o) ** 2)
        base_sq.append((p_base - o) ** 2)
        hits_total += o

        # update history AFTER predicting (no lookahead)
        key_hits[key] = key_hits.get(key, 0) + o
        key_n[key] = key_n.get(key, 0) + 1
        global_hits += o
        global_n += 1

    scored = len(sys_sq)
    brier_sys = (sum(sys_sq) / scored) if scored else None
    brier_base = (sum(base_sq) / scored) if scored else None
    skill = (1 - brier_sys / brier_base) if (brier_sys is not None and brier_base) else None

    if skill is None:
        verdict = "insufficient data to score"
    elif skill > 0.02:
        verdict = "positive skill — conditions beat the base rate (provisional)"
    elif skill < -0.02:
        verdict = "negative skill — conditions predict worse than the base rate"
    else:
        verdict = "no skill — predictions are not better than guessing the base rate"

    report = {
        "conditions_total": len(conditions),
        "conditions_scored": scored,
        "event_base_rate": round(hits_total / scored, 4) if scored else None,
        "brier_system": round(brier_sys, 5) if brier_sys is not None else None,
        "brier_baseline": round(brier_base, 5) if brier_base is not None else None,
        "skill_score": round(skill, 4) if skill is not None else None,
        "distinct_condition_keys": len(key_n),
        "verdict": verdict,
    }

    if bootstrap > 0 and scored > 0:
        # block size ~ sqrt(n): a standard moving-block choice
        block = max(1, int(scored ** 0.5))
        boot = block_bootstrap_skill(sys_sq, base_sq, block_size=block, n_boot=bootstrap)
        report["bootstrap"] = boot
        if boot.get("skill_ci_low") is not None:
            if boot["significant"]:
                report["verdict"] += f" | SIGNIFICANT: 95% CI [{boot['skill_ci_low']}, {boot['skill_ci_high']}] excludes 0"
            else:
                report["verdict"] += (
                    f" | NOT significant: 95% CI [{boot['skill_ci_low']}, {boot['skill_ci_high']}] includes 0"
                )

    return report


def run_backtest(
    logs_root: str = "logs",
    days: int = 14,
    horizon_hours: float = 1.0,
    threshold_pct: float = 2.0,
    min_level: int = 2,
    bootstrap: int = 0,
    exclude_sources: set[str] | None = None,
    target_source: str | None = None,
) -> dict[str, Any]:
    """Run a single backtest.

    ``target_source`` selects the outcome model:
      - ``None`` (default) → legacy BTC-price-move outcome (uses
        ``logs/crypto`` + ``threshold_pct``). Kept for backward compatibility.
      - any sensor name (``"earthquake"``, ``"quantum_rng"``, ...) → symmetric
        outcome "did this source have any anomaly in the horizon window?". This
        is the cross-domain test: the same condition stream is scored against
        every possible target source, not just BTC.
    """
    conditions = load_conditions(
        f"{logs_root}/anomalies", days=days, min_level=min_level, exclude_sources=exclude_sources
    )
    if not conditions:
        return {"error": "no condition data", "n_conditions": 0}

    extra: dict[str, Any] = {
        "days": days,
        "horizon_hours": horizon_hours,
        "min_cluster_level": min_level,
        "excluded_sources": sorted(exclude_sources) if exclude_sources else [],
        "target_source": target_source,
    }

    if target_source is None:
        # legacy: BTC price-move outcome
        price = load_price_series(f"{logs_root}/crypto", days=days)
        if not price:
            return {"error": "missing price data", "n_price": 0}
        outcome_fn = make_btc_move_outcome(price, horizon_hours, threshold_pct)
        extra["threshold_pct"] = threshold_pct
        extra["n_price_points"] = len(price)
    else:
        events_by_src = load_anomaly_events_by_source(f"{logs_root}/anomalies", days=days)
        if target_source not in events_by_src or not events_by_src[target_source]:
            return {
                "error": f"no anomaly events for target_source={target_source!r}",
                "target_source": target_source,
                "available_sources": sorted(events_by_src.keys()),
            }
        outcome_fn = make_anomaly_outcome(events_by_src, target_source, horizon_hours)
        extra["n_target_events"] = len(events_by_src[target_source])
        extra["available_target_sources"] = sorted(events_by_src.keys())

    report = walk_forward(conditions, outcome_fn, bootstrap=bootstrap)
    report.update(extra)

    if report.get("conditions_scored", 0) < 100:
        report["warning"] = (
            f"only {report.get('conditions_scored', 0)} conditions scored — too few to interpret; "
            "accumulate more (clean, post-fix) data before drawing conclusions"
        )
    return report


def run_backtest_matrix(
    logs_root: str = "logs",
    days: int = 200,
    horizon_hours: float = 1.0,
    min_level: int = 2,
    bootstrap: int = 0,
    exclude_sources: set[str] | None = None,
    target_sources: list[str] | None = None,
) -> dict[str, Any]:
    """Run the symmetric backtest across every available target source.

    Produces one row per target — the long-missing cross-domain matrix.
    Significance is judged by ``bootstrap`` on each row.
    """
    events_by_src = load_anomaly_events_by_source(f"{logs_root}/anomalies", days=days)
    if not events_by_src:
        return {"error": "no anomaly events found", "logs_root": logs_root}

    targets = target_sources or sorted(events_by_src.keys())
    rows: dict[str, dict[str, Any]] = {}
    for tgt in targets:
        rep = run_backtest(
            logs_root=logs_root,
            days=days,
            horizon_hours=horizon_hours,
            min_level=min_level,
            bootstrap=bootstrap,
            exclude_sources=exclude_sources,
            target_source=tgt,
        )
        rows[tgt] = rep

    # extract a compact comparison table
    table = []
    for tgt, rep in rows.items():
        if "error" in rep:
            table.append({"target": tgt, "error": rep["error"]})
            continue
        boot = rep.get("bootstrap") or {}
        table.append({
            "target": tgt,
            "n_events": rep.get("n_target_events"),
            "conditions_scored": rep.get("conditions_scored"),
            "event_base_rate": rep.get("event_base_rate"),
            "skill_score": rep.get("skill_score"),
            "ci_low": boot.get("skill_ci_low"),
            "ci_high": boot.get("skill_ci_high"),
            "significant": boot.get("significant"),
        })

    return {
        "horizon_hours": horizon_hours,
        "min_cluster_level": min_level,
        "excluded_sources": sorted(exclude_sources) if exclude_sources else [],
        "bootstrap_iters": bootstrap,
        "table": table,
        "details": rows,
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Out-of-sample backtest of condition->BTC-move predictions")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--horizon", type=float, default=1.0, help="prediction horizon (hours)")
    ap.add_argument("--threshold", type=float, default=2.0, help="BTC move %% that counts as an event")
    ap.add_argument("--min-level", type=int, default=2)
    ap.add_argument("--bootstrap", type=int, default=0, help="block-bootstrap iterations for skill significance (e.g. 1000)")
    ap.add_argument("--exclude", default="", help="comma-separated sources to exclude from conditions (e.g. crypto)")
    ap.add_argument("--target-source", default=None,
                    help="single target sensor (e.g. 'earthquake'); default: legacy BTC-move outcome")
    ap.add_argument("--all-targets", action="store_true",
                    help="run symmetric matrix backtest across every sensor as target")
    ap.add_argument("--logs", default="logs")
    args = ap.parse_args()

    excl = {s.strip() for s in args.exclude.split(",") if s.strip()}
    if args.all_targets:
        report = run_backtest_matrix(
            logs_root=args.logs, days=args.days,
            horizon_hours=args.horizon, min_level=args.min_level,
            bootstrap=args.bootstrap, exclude_sources=excl,
        )
    else:
        report = run_backtest(
            logs_root=args.logs, days=args.days,
            horizon_hours=args.horizon, threshold_pct=args.threshold, min_level=args.min_level,
            bootstrap=args.bootstrap, exclude_sources=excl,
            target_source=args.target_source,
        )
    print(json.dumps(report, indent=2))
