"""Atmospheric and local entropy must never masquerade as quantum evidence."""

import asyncio

import pytest

from src.analyzers.offline.evidence import domain_events
from src.analyzers.online.hybrid_detector import HybridDetector
from src.core.types import Event, EventType
from src.monitoring.coverage import _issues
from src.sensors.quantum_rng_sensor import QuantumRNGSensor


def test_failed_anu_request_does_not_fallback():
    sensor = QuantumRNGSensor(sample_size=128)

    async def failed_anu():
        raise RuntimeError("ANU unavailable")

    sensor._fetch_anu_quantum = failed_anu
    with pytest.raises(RuntimeError, match="ANU unavailable"):
        asyncio.run(sensor.collect())


def test_invalid_anu_sample_is_rejected():
    sensor = QuantumRNGSensor(sample_size=128)

    async def short_sample():
        return [42] * 127

    sensor._fetch_anu_quantum = short_sample
    with pytest.raises(ValueError, match="incomplete"):
        asyncio.run(sensor.collect())


def test_fallback_record_is_partial_and_not_analyzed():
    record = {"source": "random_org_atmospheric", "randomness_score": 0.1}
    assert _issues("quantum_rng", record)
    event = Event(timestamp=1780300000, source="quantum_rng",
                  event_type=EventType.DATA, payload=record)
    assert HybridDetector().process(event) == []


def test_historical_quantum_needs_provenance():
    records = [
        {"timestamp": 1780300000.0, "sensor_source": "quantum_rng"},
        {"timestamp": 1780300001.0, "sensor_source": "quantum_rng",
         "metadata": {"measurement_source": "anu_quantum"}},
        {"timestamp": 1780300002.0, "sensor_source": "crypto"},
    ]
    assert domain_events(records) == [(1780300001.0, "quantum"),
                                      (1780300002.0, "markets")]
