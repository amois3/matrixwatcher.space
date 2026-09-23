from src.core import scheduler as scheduler_module
from src.core.scheduler import Scheduler


def test_interval_is_measured_between_poll_starts(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(scheduler_module.time, "time", lambda: clock[0])
    monkeypatch.setattr(scheduler_module.random, "uniform", lambda _a, _b: 0.0)

    def work():
        clock[0] += 3.0

    scheduler = Scheduler()
    scheduler.register_task("short", work, interval=5)
    task = scheduler._tasks["short"]
    task._next_run = 100.0
    scheduler._run_task(task)
    assert task._next_run == 105.0


def test_slow_poll_never_schedules_before_completion(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(scheduler_module.time, "time", lambda: clock[0])
    monkeypatch.setattr(scheduler_module.random, "uniform", lambda _a, _b: 0.0)

    def work():
        clock[0] += 7.0

    scheduler = Scheduler()
    scheduler.register_task("slow", work, interval=5)
    task = scheduler._tasks["slow"]
    task._next_run = 100.0
    scheduler._run_task(task)
    assert task._next_run >= 107.1
