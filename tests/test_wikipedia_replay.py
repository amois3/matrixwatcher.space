import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.analyzers.offline.quality_atlas import scan_day
from src.sensors.base import SensorConfig
from src.sensors import wikipedia_edits_sensor as wiki


def _line(ts, *, server="en.wikipedia.org", bot=False, event_id="x", canary=False):
    return (json.dumps({
        "meta": {"dt": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
                 "id": event_id, "domain": "canary" if canary else server},
        "server_name": server, "type": "edit", "bot": bot,
    }) + "\n").encode()


class FakeResponse:
    status = 200

    def __init__(self, lines):
        self.content = self._lines(lines)

    async def _lines(self, lines):
        for line in lines:
            yield line

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass


class FakeSession:
    def __init__(self, lines, requests):
        self.lines, self.requests = lines, requests

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return FakeResponse(self.lines)


@pytest.mark.asyncio
async def test_replay_covers_gap_and_commits_cursor_after_persistence(monkeypatch, tmp_path):
    requests = []
    lines = [
        _line(920, event_id="old"),
        _line(930, event_id="human"),
        _line(930, event_id="human"),
        _line(940, bot=True, event_id="bot"),
        _line(950, server="commons.wikimedia.org", event_id="commons"),
        _line(960, event_id="canary", canary=True),
        _line(991, event_id="watermark"),
    ]
    monkeypatch.setattr(wiki, "time", SimpleNamespace(time=lambda: 1000.0))
    monkeypatch.setattr(wiki.aiohttp, "ClientSession",
                        lambda **_kwargs: FakeSession(lines, requests))
    sensor = wiki.WikipediaEditsSensor(
        SensorConfig(custom_params={"resume_history": True}), log_dir=tmp_path)
    sensor._last_covered_at = 925.0
    reading = await sensor.collect()
    assert reading.timestamp == 985.0
    assert reading.data["human_edits"] == 1
    assert reading.data["events_total"] == 3
    assert reading.data["quality"]["complete"] is True
    assert reading.data["sample_seconds"] == 60.0
    assert requests[0][1]["params"]["since"] == "1970-01-01T00:15:20.000Z"
    assert sensor._last_covered_at == 925.0
    sensor.confirm_persisted(reading)
    assert sensor._last_covered_at == 985.0


@pytest.mark.asyncio
async def test_long_outage_is_marked_partial(monkeypatch, tmp_path):
    lines = [_line(400, event_id="first"), _line(991, event_id="watermark")]
    monkeypatch.setattr(wiki, "time", SimpleNamespace(time=lambda: 1000.0))
    monkeypatch.setattr(wiki.aiohttp, "ClientSession",
                        lambda **_kwargs: FakeSession(lines, []))
    sensor = wiki.WikipediaEditsSensor(
        SensorConfig(custom_params={"resume_history": True, "max_replay_seconds": 600}),
        log_dir=tmp_path)
    sensor._last_covered_at = 25.0
    reading = await sensor.collect()
    assert reading.data["replay_skipped_seconds"] == 360.0
    assert reading.data["quality"]["complete"] is False
    assert reading.data["covered_start_at"] == 385.0


def test_atlas_uses_actual_replayed_interval(tmp_path):
    day = datetime(2026, 9, 23, tzinfo=timezone.utc).timestamp()
    path = tmp_path / "wikipedia_edits" / "2026-09-23.jsonl"
    path.parent.mkdir()
    path.write_text(json.dumps({"timestamp": day + 90, "source": "wikipedia_edits",
                                "sample_seconds": 45,
                                "covered_start_at": day + 50,
                                "covered_until_at": day + 60}) + "\n")
    row = scan_day(path, "wikipedia_edits", 60, day)
    assert row["sampled_seconds"] == 10
    assert row["sampling_opportunity"]["30"] == round(40 / 86400, 4)
