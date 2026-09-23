"""SIGTERM must finish the active event-loop turn before cleanup starts."""

import signal

from main import MatrixWatcher


def test_signal_defers_shutdown_until_event_loop_returns(monkeypatch):
    handlers = {}
    monkeypatch.setattr(signal, "signal", lambda number, handler: handlers.__setitem__(number, handler))

    class Loop:
        active = False

        def run_until_complete(self, awaitable):
            self.active = True
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            awaitable.close()
            self.active = False

    class Watcher:
        _running = False
        _loop = Loop()
        stops = 0

        def start(self):
            self._running = True

        def stop(self):
            assert not self._loop.active
            self.stops += 1

    watcher = Watcher()
    MatrixWatcher.run(watcher)
    assert watcher.stops == 1
