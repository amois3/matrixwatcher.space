"""Tests for the Open-Meteo WeatherSensor.

Pure-function tests (no network) cover the WMO code mapping and the
Open-Meteo response parsing. A network-gated integration test verifies a
real end-to-end fetch when connectivity is available.
"""

import asyncio
import socket

import pytest

from src.sensors.weather_sensor import WeatherSensor, wmo_to_text, parse_open_meteo


def _online() -> bool:
    try:
        socket.create_connection(("api.open-meteo.com", 443), timeout=4).close()
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def test_wmo_to_text_known_codes():
    assert wmo_to_text(0) == ("Clear", "clear sky")
    assert wmo_to_text(3) == ("Clouds", "overcast")
    assert wmo_to_text(95) == ("Thunderstorm", "thunderstorm")


def test_wmo_to_text_unknown_and_none():
    assert wmo_to_text(None) == ("", "")
    main, desc = wmo_to_text(1234)
    assert main == "Unknown"
    assert "1234" in desc


def test_parse_open_meteo_maps_all_fields():
    payload = {
        "current": {
            "temperature_2m": 12.345,
            "apparent_temperature": 10.0,
            "relative_humidity_2m": 80,
            "surface_pressure": 1013.27,
            "cloud_cover": 75,
            "wind_speed_10m": 4.56,
            "wind_direction_10m": 200,
            "weather_code": 3,
        }
    }
    out = parse_open_meteo(payload, "London", "GB")

    assert out["location"] == "London"
    assert out["country"] == "GB"
    assert out["temperature_celsius"] == 12.3      # rounded to 1 dp
    assert out["feels_like_celsius"] == 10.0
    assert out["humidity_percent"] == 80
    assert out["pressure_hpa"] == 1013.3
    assert out["clouds_percent"] == 75
    assert out["wind_speed_ms"] == 4.6
    assert out["wind_direction_deg"] == 200
    assert out["weather_code"] == 3
    assert out["weather_main"] == "Clouds"
    assert out["weather_description"] == "overcast"


def test_parse_open_meteo_handles_missing_current():
    out = parse_open_meteo({}, "Nowhere", "")
    # No fabricated numbers — missing values stay None
    assert out["temperature_celsius"] is None
    assert out["pressure_hpa"] is None
    assert out["weather_code"] is None
    assert out["location"] == "Nowhere"


# ---------------------------------------------------------------------------
# No-key contract: sensor must NOT require an API key
# ---------------------------------------------------------------------------

def test_sensor_constructs_without_api_key():
    sensor = WeatherSensor(location="New York,US")
    assert sensor._place_name == "New York"
    # api_key path is gone — collect() must not short-circuit on a missing key.


# ---------------------------------------------------------------------------
# Live integration (skipped when offline)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _online(), reason="no network to api.open-meteo.com")
def test_live_collect_returns_real_data():
    sensor = WeatherSensor(location="New York,US")
    reading = asyncio.run(sensor.collect())

    assert reading.data["error"] is None, f"fetch failed: {reading.data.get('error')}"
    assert reading.data["from_cache"] is False
    temp = reading.data["temperature_celsius"]
    assert temp is not None and -90.0 < temp < 60.0
    assert reading.data["pressure_hpa"] is not None
