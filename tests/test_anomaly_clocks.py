from src.analyzers.online.timing import anomaly_clocks
from src.core.types import AnomalyEvent, Event, EventType


def _anomaly(source, timestamp, metadata=None):
    return AnomalyEvent(timestamp=timestamp, parameter="example", value=1,
                        mean=0, std=1, z_score=4, sensor_source=source,
                        metadata=metadata)


def _observation(source, timestamp, payload=None):
    return Event(timestamp=timestamp, source=source, event_type=EventType.DATA,
                 payload=payload or {})


def test_usgs_origin_and_observation_are_distinct():
    origin, observed, detected = 1000.0, 1800.0, 1800.5
    clocks = anomaly_clocks(_anomaly("earthquake", origin,
                                    {"event_time": origin}),
                            _observation("earthquake", observed), detected)
    assert clocks == {
        "source_event_at": origin, "published_at": None,
        "observed_at": observed, "detected_at": detected,
        "timestamp_basis": "source_event",
    }


def test_weekly_report_date_is_not_eruption_time():
    clocks = anomaly_clocks(_anomaly("volcanic_activity", 1800),
                            _observation("volcanic_activity", 1800,
                                         {"feed_pub_date_unix": 1000}), 1801)
    assert clocks["source_event_at"] is None
    assert clocks["published_at"] == 1000
    assert clocks["observed_at"] == 1800


def test_unknown_or_invalid_upstream_time_is_not_invented():
    clocks = anomaly_clocks(_anomaly("news", 1800),
                            _observation("news", 1800), 1801)
    assert clocks["source_event_at"] is None
    assert clocks["published_at"] is None
    assert clocks["timestamp_basis"] == "collector_observation"
    bad = anomaly_clocks(_anomaly("earthquake", 1900, {"event_time": float("nan")}),
                         _observation("earthquake", 1800), 1801)
    assert bad["source_event_at"] is None
