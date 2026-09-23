"""Regression checks for gaps found in the September 2026 production audit."""

import json
import asyncio
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from main import MatrixWatcher
from src.analyzers.online.cluster_detector import AnomalyCluster, ClusterDetector
from src.analyzers.online.historical_pattern_tracker import Condition, HistoricalPatternTracker, Pattern
from src.analyzers.online.hybrid_detector import HybridDetector
from src.core.types import AnomalyEvent, Event, EventType
from src.monitoring.coverage import get_coverage
from src.monitoring.health_monitor import HealthMonitor
from src.storage.storage_manager import StorageManager
from src.sensors.space_weather_sensor import SpaceWeatherSensor
from src.sensors.solar_wind_sensor import _parse_product


def anomaly(source: str, ts: float) -> AnomalyEvent:
    return AnomalyEvent(ts, "test", 1, 0, 1, 4, source)


def test_related_solar_feeds_count_as_one_domain():
    detector = ClusterDetector(cluster_window_seconds=30)
    ts = time.time()
    for source in ("solar_activity", "solar_wind", "space_weather"):
        cluster = detector.add_anomaly(anomaly(source, ts), now=ts)
    assert cluster.level == 1
    assert cluster.source_count == 3
    assert cluster.domains == ("heliophysics",)
    cluster = detector.add_anomaly(anomaly("crypto", ts), now=ts)
    assert cluster.level == 2


def test_qualified_pattern_no_longer_crashes_on_undefined_temporal_flag():
    with TemporaryDirectory() as directory:
        tracker = HistoricalPatternTracker(storage_path=directory)
        condition = Condition(time.time(), 1, ["solar_wind"], 40, 1)
        tracker._patterns[condition.to_key()]["btc_dump_1h"] = Pattern(
            condition_key=condition.to_key(), event_type="btc_dump_1h",
            condition_count=40, event_after_count=8, actual_probability=.2,
            avg_time_to_event=3600,
        )
        result = tracker.get_probabilities(condition)
        assert result["btc_dump_1h"]["probability"] == .2
        assert "temporal_pattern" not in result["btc_dump_1h"]
        assert tracker.get_calibration_stats()["avg_brier_score"] is None


def test_cluster_is_durable_before_optional_pattern_analysis():
    with TemporaryDirectory() as directory:
        watcher = object.__new__(MatrixWatcher)
        watcher.storage = StorageManager(base_path=directory, buffer_size=1000)
        item = anomaly("crypto", time.time())
        cluster = AnomalyCluster(1, [item], item.timestamp, 1, "Single", domains=("markets",), source_count=1)
        watcher.smart_analyzer = SimpleNamespace(record_anomaly=lambda _: None)
        watcher.cluster_detector = SimpleNamespace(add_anomaly=lambda _: cluster)
        watcher.anomaly_index = SimpleNamespace(calculate=lambda _: SimpleNamespace(
            index=1, baseline_ratio=1, status="normal", breakdown={}))
        watcher.pattern_tracker = SimpleNamespace(
            record_condition=lambda _: None,
            get_probabilities=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("pattern failure")),
        )
        watcher._pipeline_stats = {"clusters_persisted": 0}
        try:
            watcher._handle_anomaly(item)
        except RuntimeError:
            pass
        else:
            assert False, "expected synthetic pattern failure"
        files = list((Path(directory) / "anomalies").glob("*.jsonl"))
        assert files
        assert json.loads(files[0].read_text().splitlines()[-1])["cluster"]["level"] == 1
        assert watcher._pipeline_stats["clusters_persisted"] == 1


def test_event_pipeline_records_measurements_and_reports_analysis_failure():
    with TemporaryDirectory() as directory:
        watcher = object.__new__(MatrixWatcher)
        watcher.storage = StorageManager(base_path=directory, buffer_size=1000)
        item = anomaly("crypto", time.time())
        cluster = AnomalyCluster(1, [item], item.timestamp, 1, "Single", domains=("markets",), source_count=1)
        watcher.smart_analyzer = SimpleNamespace(record_event=lambda _: None, record_anomaly=lambda _: None)
        watcher.anomaly_detector = SimpleNamespace(process=lambda _: [item], mark_persisted=lambda _: None)
        watcher.cluster_detector = SimpleNamespace(add_anomaly=lambda _: cluster)
        watcher.anomaly_index = SimpleNamespace(calculate=lambda _: SimpleNamespace(
            index=1, baseline_ratio=1, status="normal", breakdown={}))
        watcher.pattern_tracker = SimpleNamespace(
            check_events=lambda _: [], record_condition=lambda _: None,
            get_probabilities=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("pattern failure")),
        )
        watcher._pipeline_stats = {"events_processed": 0, "anomalies_detected": 0,
                                   "clusters_persisted": 0, "errors": 0,
                                   "last_error": None, "last_error_at": None, "last_event_at": None}
        watcher._write_pipeline_status = lambda force=False: None
        watcher._process_data_event(Event.create("crypto", EventType.DATA, {"source": "crypto"}))
        records = [json.loads(line) for path in (Path(directory) / "anomalies").glob("*.jsonl")
                   for line in path.read_text().splitlines()]
        assert len(records) == 2
        assert any("cluster" in record for record in records)
        assert watcher._pipeline_stats["errors"] == 1
        assert watcher._pipeline_stats["clusters_persisted"] == 1


def test_news_burst_is_detectable_only_with_enough_feeds():
    detector = HybridDetector()
    ts = time.time()
    missing = Event(ts, "news", EventType.DATA, {"new_items_count": 12, "feeds_successful": 2})
    assert detector.process(missing) == []
    good = Event(ts + 1, "news", EventType.DATA, {"new_items_count": 8, "feeds_successful": 4})
    assert any(a.sensor_source == "news" for a in detector.process(good))


def test_coverage_exposes_missing_btc_despite_fresh_eth():
    with TemporaryDirectory() as directory:
        logs = Path(directory) / "logs"
        crypto = logs / "crypto"
        crypto.mkdir(parents=True)
        now = time.time()
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        (crypto / f"{day}.jsonl").write_text(json.dumps({
            "timestamp": now, "pairs": [{"symbol": "ETHUSDT", "price": 100}],
        }) + "\n")
        config = Path(directory) / "config.json"
        config.write_text(json.dumps({"sensors": {"crypto": {"interval_seconds": 5}}}))
        result = get_coverage(logs, config, now=now)
        assert result["sensors"]["crypto"]["status"] == "partial"
        assert "Missing BTCUSDT price" in result["sensors"]["crypto"]["issues"]


def test_noaa_wind_selects_current_active_row_and_real_field_names():
    now = time.time()
    stamp = lambda offset: time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now + offset))
    rows = [
        {"time_tag": stamp(0), "active": False, "source": "IMAP", "proton_speed": 290, "proton_density": 8},
        {"time_tag": stamp(-60), "active": True, "source": "SOLAR1", "proton_speed": 281, "proton_density": 3.1},
        {"time_tag": stamp(-86400), "active": True, "source": "ACE", "proton_speed": 305, "proton_density": .2},
    ]

    class Response:
        status = 200
        async def __aenter__(self): return self
        async def __aexit__(self, *_): pass
        async def json(self): return rows

    class Session:
        def get(self, *_args, **_kwargs): return Response()

    result = asyncio.run(SpaceWeatherSensor()._get_solar_wind(Session()))
    assert result["speed"] == 281
    assert result["density"] == 3.1
    assert result["source"] == "SOLAR1"


def test_noaa_solar_wind_does_not_carry_old_value_forward():
    rows = [["time_tag", "speed"], ["old", 300], ["new", None]]
    assert _parse_product(rows, "speed") is None


def test_partial_reading_is_not_reported_as_healthy():
    monitor = HealthMonitor()
    monitor.register_sensor("crypto")
    monitor.record_degraded("crypto", "BTC unavailable")
    status = monitor.get_all_status()
    assert status["status"] == "degraded"
    assert status["sensors"]["crypto"]["status"] == "degraded"
