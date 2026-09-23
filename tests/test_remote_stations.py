"""Global METAR reports need stable identity, event time and completeness."""

from src.sensors.remote_stations_sensor import (
    STATIONS, RemoteStationsSensor, summarize_station_observations,
)


def _report(station: tuple, now: float) -> dict:
    station_id, _, _, latitude, longitude = station
    return {
        "icaoId": station_id, "obsTime": now - 1200,
        "temp": 20, "dewp": 12, "altim": 1013.2,
        "lat": latitude, "lon": longitude,
        "rawOb": f"METAR {station_id} 231800Z 00000KT CAVOK 20/12 Q1013",
    }


def test_global_panel_exposes_six_regions_and_source_time():
    now = 1_800_000_000
    reports = [_report(station, now) for station in reversed(STATIONS)]
    result = summarize_station_observations(reports, now)
    assert result["fresh_stations"] == 6
    assert result["quality"]["complete"] is True
    assert [row["id"] for row in result["stations"]] == [station[0] for station in STATIONS]
    assert len({row["region"] for row in result["stations"]}) == 6
    assert all(row["reporting_lag_seconds"] == 1200 for row in result["stations"])
    assert all(row["altimeter_hpa"] == 1013.2 for row in result["stations"])
    assert result["context_only"] is True
    assert RemoteStationsSensor().event_bus is None


def test_newest_report_is_selected_without_duplication():
    now = 1_800_000_000
    older = _report(STATIONS[0], now)
    older["obsTime"] -= 600
    older["temp"] = 99
    result = summarize_station_observations([_report(STATIONS[0], now), older], now)
    assert result["fresh_stations"] == 1
    assert result["stations"][0]["temperature_celsius"] == 20


def test_stale_missing_and_mismatched_station_are_visible():
    now = 1_800_000_000
    reports = [_report(station, now) for station in STATIONS]
    reports[0]["icaoId"] = "XXXX"
    reports[1]["obsTime"] = now - 7200
    reports[2]["lat"] += 2
    reports[3]["altim"] = None
    result = summarize_station_observations(reports, now)
    assert result["fresh_stations"] == 2
    assert result["quality"]["complete"] is False
    assert all(row["temperature_celsius"] is None for row in result["stations"][:4])
    assert len(result["quality"]["missing_fields"]) == 4
