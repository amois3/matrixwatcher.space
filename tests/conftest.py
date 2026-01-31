"""Shared pytest fixtures and Hypothesis strategies for Matrix Watcher tests."""

import pytest
from hypothesis import strategies as st
from datetime import datetime, date
import tempfile
import os

# ============================================================================
# Hypothesis Strategies for Property-Based Testing
# ============================================================================

# Timestamp strategies
timestamp_strategy = st.floats(min_value=0, max_value=2**31, allow_nan=False, allow_infinity=False)

# Percentage strategies (0-100)
percentage_strategy = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)

# Latency strategies (ms)
latency_strategy = st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False)

# Price strategies
price_strategy = st.floats(min_value=0.0001, max_value=1_000_000.0, allow_nan=False, allow_infinity=False)

# Z-score strategies
z_score_strategy = st.floats(min_value=-20.0, max_value=20.0, allow_nan=False, allow_infinity=False)

# Priority strategies
priority_strategy = st.sampled_from(["high", "medium", "low"])

# Sensor name strategies
sensor_name_strategy = st.sampled_from([
    "system", "time_drift", "network", "random", 
    "crypto", "blockchain", "weather", "news"
])

# Event type strategies
event_type_strategy = st.sampled_from(["data", "anomaly", "error", "health"])

# Random bits strategy (1024 bits = 128 bytes)
random_bits_strategy = st.binary(min_size=128, max_size=128)

# Date strategies
date_strategy = st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))

# Interval strategies (seconds)
interval_strategy = st.floats(min_value=0.1, max_value=3600.0, allow_nan=False, allow_infinity=False)

# Window size strategies
window_size_strategy = st.integers(min_value=10, max_value=1000)

# Correlation strategies
correlation_strategy = st.floats(min_value=-1.0, max_value=1.0, allow_nan=False, allow_infinity=False)

# Lag strategies (seconds)
lag_strategy = st.integers(min_value=-60, max_value=60)


# ============================================================================
# Composite Strategies
# ============================================================================

@st.composite
def sensor_config_strategy(draw):
    """Generate valid SensorConfig objects."""
    return {
        "enabled": draw(st.booleans()),
        "interval_seconds": draw(interval_strategy),
        "priority": draw(priority_strategy),
        "custom_params": draw(st.dictionaries(
            st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
            st.one_of(st.integers(), st.floats(allow_nan=False), st.text(max_size=50), st.booleans()),
            max_size=5
        ))
    }


@st.composite
def event_strategy(draw):
    """Generate valid Event objects."""
    return {
        "timestamp": draw(timestamp_strategy),
        "source": draw(sensor_name_strategy),
        "event_type": draw(event_type_strategy),
        "payload": draw(st.dictionaries(
            st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
            st.one_of(st.integers(), st.floats(allow_nan=False, allow_infinity=False), st.text(max_size=50)),
            max_size=10
        ))
    }


@st.composite
def sensor_reading_strategy(draw):
    """Generate valid SensorReading objects."""
    return {
        "timestamp": draw(timestamp_strategy),
        "source": draw(sensor_name_strategy),
        "data": draw(st.dictionaries(
            st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
            st.one_of(st.integers(), st.floats(allow_nan=False, allow_infinity=False), st.text(max_size=50)),
            min_size=1,
            max_size=15
        ))
    }


@st.composite
def anomaly_event_strategy(draw):
    """Generate valid AnomalyEvent objects."""
    mean = draw(st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False))
    std = draw(st.floats(min_value=0.01, max_value=100, allow_nan=False, allow_infinity=False))
    z = draw(z_score_strategy)
    value = mean + z * std
    
    return {
        "timestamp": draw(timestamp_strategy),
        "parameter": draw(st.text(min_size=1, max_size=30, alphabet=st.characters(whitelist_categories=("L", "N", "Pd")))),
        "value": value,
        "mean": mean,
        "std": std,
        "z_score": z,
        "sensor_source": draw(sensor_name_strategy)
    }


# ============================================================================
# Pytest Fixtures
# ============================================================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def temp_logs_dir(temp_dir):
    """Create a temporary logs directory structure."""
    logs_dir = os.path.join(temp_dir, "logs")
    os.makedirs(logs_dir)
    
    # Create sensor subdirectories
    for sensor in ["system", "time_drift", "network", "random", "crypto", "blockchain", "weather", "news", "anomalies", "all_events"]:
        os.makedirs(os.path.join(logs_dir, sensor))
    
    yield logs_dir


@pytest.fixture
def sample_config():
    """Return a sample configuration dictionary."""
    return {
        "sensors": {
            "system": {"enabled": True, "interval_seconds": 1.0, "priority": "high"},
            "time_drift": {"enabled": True, "interval_seconds": 2.0, "priority": "high"},
            "network": {"enabled": True, "interval_seconds": 5.0, "priority": "medium"},
            "random": {"enabled": True, "interval_seconds": 5.0, "priority": "medium"},
            "crypto": {"enabled": True, "interval_seconds": 2.0, "priority": "high"},
            "blockchain": {"enabled": True, "interval_seconds": 10.0, "priority": "low"},
            "weather": {"enabled": True, "interval_seconds": 300.0, "priority": "low"},
            "news": {"enabled": True, "interval_seconds": 900.0, "priority": "low"},
        },
        "storage": {
            "base_path": "logs",
            "compression": False,
            "max_file_size_mb": 100,
            "buffer_size": 1000,
        },
        "analysis": {
            "window_size": 100,
            "z_score_threshold": 4.0,
            "lag_range_seconds": 60,
            "cluster_window_seconds": 3.0,
        },
        "alerting": {
            "enabled": False,
            "webhook_url": None,
            "cooldown_seconds": 300,
            "alert_on_clusters": True,
            "alert_on_correlations": True,
            "min_cluster_sensors": 3,
        },
        "api_keys": {
            "openweathermap": "",
            "random_org": "",
            "etherscan": "",
        }
    }


@pytest.fixture
def sample_system_record():
    """Return a sample system sensor record."""
    return {
        "timestamp": 1733912193.555,
        "source": "system",
        "local_time_unix": 1733912193.555,
        "loop_interval_ms": 1000.5,
        "loop_drift_ms": 0.5,
        "cpu_usage_percent": 25.3,
        "ram_usage_percent": 45.2,
        "cpu_temperature": 55.0,
        "process_pid": 12345,
        "process_uptime_seconds": 3600.0,
    }


@pytest.fixture
def sample_anomaly_records():
    """Return sample anomaly records for cluster testing."""
    base_time = 1733912193.0
    return [
        {"timestamp": base_time, "parameter": "cpu_usage", "sensor_source": "system", "z_score": 4.5},
        {"timestamp": base_time + 1.0, "parameter": "latency", "sensor_source": "network", "z_score": 5.2},
        {"timestamp": base_time + 2.0, "parameter": "price_delta", "sensor_source": "crypto", "z_score": 4.8},
        {"timestamp": base_time + 10.0, "parameter": "random_pvalue", "sensor_source": "random", "z_score": 6.1},
        {"timestamp": base_time + 11.0, "parameter": "block_interval", "sensor_source": "blockchain", "z_score": 4.2},
    ]
