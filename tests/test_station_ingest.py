"""Physical readings must have known provenance and honest clock quality."""

import pytest

from src.instruments.station_ingest import ingest, validate_reading


def fixtures(now=1_800_000_000):
    registry = {"stations": {"station_a": {
        "enabled": True, "site_code": "north_site", "calibration_id": "cal-2026-a",
        "calibration_valid_until": now + 86400}}}
    payload = {"protocol_version": 1, "station_id": "station_a", "sequence": 1,
               "observed_at": now - 30, "ntp_offset_ms": 5,
               "clock_uncertainty_ms": 10, "calibration_id": "cal-2026-a",
               "temperature_celsius": 17.2, "humidity_percent": 48,
               "pressure_hpa": 1012.4}
    return registry, payload


def test_station_ingest_keeps_real_provenance_and_sequence(tmp_path):
    now = 1_800_000_000
    registry, payload = fixtures(now)
    row = ingest(payload, registry, tmp_path, now)
    assert row["data_kind"] == "physical_station_observation"
    assert row["context_only"] is True
    assert row["reporting_delay_seconds"] == 30
    assert list((tmp_path / "stations" / "station_a").glob("*.jsonl"))
    with pytest.raises(ValueError, match="duplicate"):
        ingest(payload, registry, tmp_path, now)


@pytest.mark.parametrize("field,value,error", [
    ("station_id", "../station_b", "not commissioned"),
    ("calibration_id", "unknown", "calibration"),
    ("ntp_offset_ms", 150, "clock offset"),
    ("observed_at", 1_799_998_000, "delayed"),
    ("pressure_hpa", 0, "pressure"),
])
def test_invalid_station_sample_is_rejected(field, value, error):
    now = 1_800_000_000
    registry, payload = fixtures(now)
    payload[field] = value
    with pytest.raises(ValueError, match=error):
        validate_reading(payload, registry, now)
