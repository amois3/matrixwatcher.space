"""Tests for scheduler start-phase decorrelation (anti-lockstep polling)."""

import time
import statistics

from src.core.scheduler import Scheduler


def _noop():
    return None


def test_same_interval_tasks_get_different_phases():
    sch = Scheduler()
    now = time.time()
    sch.register_task("a", _noop, interval=300.0)
    sch.register_task("b", _noop, interval=300.0)

    na = sch._tasks["a"]._next_run
    nb = sch._tasks["b"]._next_run
    # phases must differ (not registered in lockstep)
    assert na != nb
    # start spread is capped at 30s so the first reading stays prompt
    assert now <= na <= now + 30.0 + 1
    assert now <= nb <= now + 30.0 + 1


def test_phases_spread_within_cap():
    sch = Scheduler()
    now = time.time()
    for i in range(60):
        sch.register_task(f"t{i}", _noop, interval=300.0)
    phases = [sch._tasks[f"t{i}"]._next_run - now for i in range(60)]
    # phases spread out (not bunched) but stay within the 30s cap
    assert statistics.pstdev(phases) > 4.0
    assert all(0.0 <= p <= 31.0 for p in phases)
    assert max(phases) - min(phases) > 15.0
