"""The weather grid must expose model time, gaps and non-independence."""

import asyncio

from src.core.types import SensorReading
from src.sensors.weather_grid_sensor import LOCATIONS, summarize_weather_grid
from src.sensors.weather_sensor import WeatherSensor


def _row(index, now):
    return {
        "location_id": index,
        "current_units": {"time": "unixtime", "temperature_2m": "°C", "surface_pressure": "hPa"},
        "current": {"time": now - 900, "interval": 900,
                    "temperature_2m": 20 + index, "surface_pressure": 1010 + index,
                    "relative_humidity_2m": 50, "weather_code": 3},
    }


def test_grid_preserves_fixed_place_order_and_model_time():
    now = 1_800_000_000
    result = summarize_weather_grid([_row(i, now) for i in range(len(LOCATIONS))], now)
    assert result["quality"]["complete"] is True
    assert result["fresh_locations"] == 6
    assert [cell["id"] for cell in result["cells"]] == [place[0] for place in LOCATIONS]
    assert all(cell["retrieval_lag_seconds"] == 900 for cell in result["cells"])
    assert all(cell["status"] == "ok" for cell in result["cells"])


def test_grid_marks_stale_or_reordered_cells_missing():
    now = 1_800_000_000
    rows = [_row(i, now) for i in range(len(LOCATIONS))]
    rows[1]["current"]["time"] = now - 7200
    rows[2]["location_id"] = 4
    result = summarize_weather_grid(rows, now)
    assert result["fresh_locations"] == 4
    assert result["quality"]["complete"] is False
    assert result["cells"][1]["temperature_celsius"] is None
    assert result["cells"][2]["status"] == "missing"


def test_old_weather_cache_never_publishes_as_new_anomaly_data():
    class Bus:
        def __init__(self):
            self.events = []

        def publish(self, event):
            self.events.append(event)

    bus = Bus()
    sensor = WeatherSensor(event_bus=bus, location="New York,US")

    async def cached():
        return SensorReading.create("weather", {"from_cache": True, "error": "upstream unavailable",
                                                "quality": {"complete": False}})

    sensor.collect = cached
    assert asyncio.run(sensor.safe_collect()) is not None
    assert bus.events == []
