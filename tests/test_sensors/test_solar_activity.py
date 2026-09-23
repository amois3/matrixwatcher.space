"""Tests for solar_activity sensor parsing — no network."""

from src.sensors.solar_activity_sensor import (
    _xray_flare_class,
    parse_f107,
    parse_proton,
    parse_xray_peak,
)


def test_xray_flare_class_thresholds():
    assert _xray_flare_class(None) == "none"
    assert _xray_flare_class(0) == "none"
    assert _xray_flare_class(5e-8) == "A"
    assert _xray_flare_class(5e-7) == "B"
    assert _xray_flare_class(5e-6) == "C"
    assert _xray_flare_class(5e-5) == "M"
    assert _xray_flare_class(5e-4) == "X"


def test_parse_f107_handles_newest_first_records():
    """NOAA returns f107 records newest-first; sensor must sort and pick latest."""
    records = [
        {"time_tag": "2026-05-27T22:00:00", "flux": 142.0},
        {"time_tag": "2026-05-27T17:00:00", "flux": 140.0},
        {"time_tag": "2026-05-27T00:00:00", "flux": 130.0},
    ]
    latest, pct = parse_f107(records)
    assert latest == 142.0
    # previous (in chronological order) is 140.0 -> pct = (142-140)/140 * 100
    assert abs(pct - 1.4286) < 0.01


def test_parse_f107_handles_empty():
    assert parse_f107([]) == (None, None)


def test_parse_f107_ignores_records_without_flux():
    records = [
        {"time_tag": "2026-05-27T22:00:00"},  # missing flux
        {"time_tag": "2026-05-27T17:00:00", "flux": 142.0},
    ]
    latest, pct = parse_f107(records)
    assert latest == 142.0
    assert pct is None  # only one valid record


def test_parse_xray_picks_correct_band_and_returns_class():
    records = [
        {"energy": "0.05-0.4nm", "flux": 5e-5, "time_tag": "x"},  # WRONG band — ignored
        {"energy": "0.1-0.8nm", "flux": 3e-6, "time_tag": "x"},
        {"energy": "0.1-0.8nm", "flux": 1.2e-5, "time_tag": "x"},  # peak -> M-class
        {"energy": "0.1-0.8nm", "flux": 5e-7, "time_tag": "x"},
    ]
    peak, cls = parse_xray_peak(records)
    assert peak == 1.2e-5
    assert cls == "M"


def test_parse_xray_skips_electron_contamination():
    records = [
        {"energy": "0.1-0.8nm", "flux": 1e-3, "electron_contaminaton": True},  # contaminated
        {"energy": "0.1-0.8nm", "flux": 2e-6},
    ]
    peak, cls = parse_xray_peak(records)
    assert peak == 2e-6
    assert cls == "C"


def test_parse_xray_empty():
    assert parse_xray_peak([]) == (None, "none")


def test_parse_proton_picks_latest_at_threshold():
    records = [
        {"energy": ">=10 MeV", "flux": 1.0, "time_tag": "2026-05-27T16:00:00Z"},
        {"energy": ">=10 MeV", "flux": 4.0, "time_tag": "2026-05-27T17:00:00Z"},
        {"energy": ">=100 MeV", "flux": 0.1, "time_tag": "2026-05-27T17:00:00Z"},
    ]
    assert parse_proton(records, ">=10 MeV") == 4.0
    assert parse_proton(records, ">=100 MeV") == 0.1
    assert parse_proton(records, ">=500 MeV") is None
