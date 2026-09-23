from src.sensors.ripe_atlas_sensor import MEASUREMENT_ID, PROBES, RipeAtlasSensor, summarize_results


def rows(timestamp=900):
    return [{"prb_id": probe, "timestamp": timestamp, "msm_id": MEASUREMENT_ID,
             "sent": 3, "rcvd": 3, "avg": 20.0}
            for probes in PROBES.values() for probe in probes]


def test_all_regions_fresh_but_shared_target_is_context_only():
    payload = rows()
    payload[0]["rcvd"] = 0
    payload[0]["avg"] = -1
    result = summarize_results(payload, now=1000)
    assert result["fresh_probes"] == 18
    assert result["quality"]["complete"] is True
    assert result["regions"]["Germany"]["probes"][0]["packet_loss"] == 1
    assert result["regions"]["Germany"]["median_latency_ms"] == 20
    assert RipeAtlasSensor().event_bus is None


def test_stale_probes_degrade_region_without_faking_zero_latency():
    payload = rows()
    australian = set(PROBES["Australia"])
    for row in payload:
        if row["prb_id"] in australian:
            row["timestamp"] = 0
    result = summarize_results(payload, now=1000)
    assert result["fresh_probes"] == 15
    assert result["regions"]["Australia"]["median_latency_ms"] is None
    assert result["quality"]["complete"] is False
    assert result["quality"]["missing_fields"] == ["Australia: fewer than 2 fresh probes"]


def test_documented_keyed_response_shape_is_supported():
    payload = {str(row["prb_id"]): [row] for row in rows()}
    result = summarize_results(payload, now=1000)
    assert result["fresh_probes"] == 18
