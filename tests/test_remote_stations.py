"""Physical reports need identity, source time and unit checks before display."""

from datetime import datetime, timezone

from src.sensors.remote_stations_sensor import (
    STATIONS, RemoteStationsSensor, summarize_station_observations,
)


def _report(station: str, now: float) -> dict:
    stamp = datetime.fromtimestamp(now - 1200, timezone.utc).isoformat()
    return {
        "id": f"https://api.weather.gov/stations/{station}/observations/{stamp}",
        "properties": {
            "timestamp": stamp,
            "temperature": {"unitCode": "wmoUnit:degC", "value": 20},
            "barometricPressure": {"unitCode": "wmoUnit:Pa", "value": 101325},
            "relativeHumidity": {"unitCode": "wmoUnit:percent", "value": 50},
        },
    }


def test_station_report_exposes_source_time_and_converts_pressure():
    now = 1_800_000_000
    payloads = {station: _report(station, now) for station, _ in STATIONS}
    result = summarize_station_observations(payloads, now)
    assert result["fresh_stations"] == 3
    assert result["quality"]["complete"] is True
    assert all(row["reporting_lag_seconds"] == 1200 for row in result["stations"])
    assert all(row["barometric_pressure_hpa"] == 1013.2 for row in result["stations"])
    assert result["context_only"] is True
    assert RemoteStationsSensor().event_bus is None


def test_wrong_station_stale_report_and_wrong_unit_are_missing():
    now = 1_800_000_000
    payloads = {station: _report(station, now) for station, _ in STATIONS}
    payloads["KJFK"]["id"] = payloads["KLAX"]["id"]
    payloads["KLAX"]["properties"]["timestamp"] = datetime.fromtimestamp(
        now - 7200, timezone.utc).isoformat()
    payloads["KSEA"]["properties"]["barometricPressure"]["unitCode"] = "wmoUnit:hPa"
    result = summarize_station_observations(payloads, now)
    assert result["fresh_stations"] == 0
    assert result["quality"]["complete"] is False
    assert all(row["temperature_celsius"] is None for row in result["stations"])


def test_missing_one_station_is_explicitly_partial():
    now = 1_800_000_000
    result = summarize_station_observations({"KJFK": _report("KJFK", now)}, now)
    assert result["fresh_stations"] == 1
    assert result["stations"][1]["status"] == "missing"
    assert len(result["quality"]["missing_fields"]) == 2
