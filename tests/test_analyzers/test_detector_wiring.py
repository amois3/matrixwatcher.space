"""Audit regression tests: every ENABLED sensor's detector rules must match the
real field names it emits, and the previously-dead rules must now fire.

These lock in the fixes from the deep audit (weather/blockchain/news were
silently unable to ever produce an anomaly because of field-name / window bugs).
"""

import time

from src.analyzers.online.threshold_detector import ThresholdDetector
from src.core.types import Event, EventType


def _detector():
    return ThresholdDetector(event_bus=None, enable_calibration_tracking=False)


def _evt(source, payload, ts):
    return Event(timestamp=ts, source=source, event_type=EventType.DATA, payload=payload)


def test_weather_temperature_absolute_change_fires():
    """A ≥5 °C swing on the real field `temperature_celsius` must trigger."""
    d = _detector()
    t0 = 1_000_000.0
    # baseline then +6 °C 20 min later (within the 1800s lookback)
    d.process(_evt("weather", {"temperature_celsius": 10.0, "pressure_hpa": 1013.0}, t0))
    anomalies = d.process(_evt("weather", {"temperature_celsius": 16.5, "pressure_hpa": 1013.0}, t0 + 1200))
    params = [a.parameter for a in anomalies]
    assert "weather.temperature_celsius" in params


def test_weather_small_change_does_not_fire():
    d = _detector()
    t0 = 2_000_000.0
    d.process(_evt("weather", {"temperature_celsius": 10.0, "pressure_hpa": 1013.0}, t0))
    anomalies = d.process(_evt("weather", {"temperature_celsius": 11.0, "pressure_hpa": 1014.0}, t0 + 1200))
    assert anomalies == []  # 1°C / 1hPa is below thresholds


def test_weather_pressure_absolute_change_fires():
    d = _detector()
    t0 = 3_000_000.0
    d.process(_evt("weather", {"temperature_celsius": 10.0, "pressure_hpa": 1013.0}, t0))
    anomalies = d.process(_evt("weather", {"temperature_celsius": 10.0, "pressure_hpa": 1003.0}, t0 + 3600))
    params = [a.parameter for a in anomalies]
    assert "weather.pressure_hpa" in params


def test_blockchain_any_anomalous_flag_fires():
    """blockchain emits a top-level `any_anomalous` flag; the rule must use it."""
    d = _detector()
    anomalies = d.process(_evt("blockchain", {"networks_count": 2, "any_anomalous": 1}, 4_000_000.0))
    params = [a.parameter for a in anomalies]
    assert "blockchain.any_anomalous" in params


def test_blockchain_normal_does_not_fire():
    d = _detector()
    anomalies = d.process(_evt("blockchain", {"networks_count": 2, "any_anomalous": 0}, 4_100_000.0))
    assert anomalies == []


def test_news_publication_burst_is_observable():
    """The rule uses a reachable count of newly seen stories, not a >40 target."""
    d = _detector()
    t0 = 5_000_000.0
    d.process(_evt("news", {"new_items_count": 0}, t0))
    anomalies = d.process(_evt("news", {"new_items_count": 8}, t0 + 300))
    params = [a.parameter for a in anomalies]
    assert "news.new_items_count" in params


def test_no_dead_rules_for_enabled_sensors():
    """Every rule for an ENABLED sensor must target a field the sensor actually
    emits — i.e. the rule's source prefix is a real sensor and the pattern is not
    one of the known-dead historical patterns."""
    d = _detector()
    dead_patterns = {
        "weather.temperature", "weather.pressure",
        "blockchain.*.block_time_seconds",
    }
    live_patterns = {r.parameter_pattern for r in d._rules}
    assert not (dead_patterns & live_patterns), f"dead rules still present: {dead_patterns & live_patterns}"
    # the corrected patterns must be present
    for needed in ("weather.temperature_celsius", "weather.pressure_hpa", "blockchain.any_anomalous"):
        assert needed in live_patterns, f"missing corrected rule: {needed}"


# ---- Edge-triggering: one ongoing event = one anomaly, not one per poll ----

def test_persistent_threshold_event_fires_once():
    """A single earthquake stays in USGS's 'last hour' feed for ~60 polls. The
    rule must fire ONCE on the rising edge, not 60 times."""
    d = _detector()
    t = 1_000_000.0
    fires = 0
    for i in range(60):
        a = d.process(_evt("earthquake", {"max_magnitude": 5.1, "count": 1}, t + i * 60))
        fires += sum(1 for x in a if x.parameter == "earthquake.max_magnitude")
    assert fires == 1, f"expected 1 edge-triggered anomaly, got {fires}"


def test_threshold_refires_after_condition_clears():
    """Once the condition returns to normal, the next genuine occurrence fires again."""
    d = _detector()
    t = 2_000_000.0
    d.process(_evt("earthquake", {"max_magnitude": 5.1, "count": 1}, t))            # edge -> fire
    d.process(_evt("earthquake", {"max_magnitude": 5.1, "count": 1}, t + 60))       # latched -> silent
    d.process(_evt("earthquake", {"max_magnitude": 0.0, "count": 0}, t + 120))      # clears -> unlatch
    a = d.process(_evt("earthquake", {"max_magnitude": 6.0, "count": 1}, t + 180))  # edge -> fire
    assert any(x.parameter == "earthquake.max_magnitude" for x in a)


def test_change_rule_does_not_refire_while_latched():
    """A sustained crypto jump should fire once, not on every poll inside the window."""
    d = _detector()
    t = 3_000_000.0
    d.process(_evt("crypto", {"btcusdt.price": 60000.0}, t))
    fires = 0
    # price jumps to 63000 (+5%) and stays there for several polls within lookback
    for i in range(1, 6):
        a = d.process(_evt("crypto", {"btcusdt.price": 63000.0}, t + i * 5))
        fires += sum(1 for x in a if x.parameter == "crypto.btcusdt.price")
    assert fires == 1, f"sustained jump should fire once, got {fires}"
