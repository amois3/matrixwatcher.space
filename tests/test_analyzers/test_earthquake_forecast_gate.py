"""Legacy earthquake association counts must never become location forecasts."""

import time

from src.analyzers.online.historical_pattern_tracker import (
    Condition,
    HistoricalPatternTracker,
    Pattern,
)


def test_legacy_earthquake_pattern_is_not_emitted_as_forecast(tmp_path):
    tracker = HistoricalPatternTracker(storage_path=str(tmp_path / "patterns"))
    assert not any(name.startswith("earthquake_") for name in tracker._event_definitions)
    assert tracker.check_events({"source": "earthquake", "max_magnitude": 6.2}) == []
    condition = Condition(time.time(), 1, ["weather"], 20, 1)
    earthquake = Pattern(condition.to_key(), "earthquake_strong")
    earthquake.condition_count = 35
    earthquake.event_after_count = 4
    earthquake.avg_time_to_event = 7200
    earthquake.min_time_to_event = 3600
    earthquake.max_time_to_event = 10800
    earthquake.event_locations = [(38.9, 16.6)] * 4
    earthquake.update_probability()
    tracker._patterns[condition.to_key()]["earthquake_strong"] = earthquake

    assert tracker.get_probabilities(condition, category_filter="earthquake") == {}
    assert tracker.get_probabilities(condition) == {}
