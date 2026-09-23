"""The network reference window must stay fixed and reject sparse probes."""

import json
from pathlib import Path

from src.analyzers.offline.ripe_baseline import (
    START_AT, END_AT, build_baseline, write_report,
)
from src.sensors.ripe_atlas_sensor import PROBES, TARGETS


def _record(day):
    ts = START_AT + day * 86400 + 3600
    return {"timestamp": ts, "targets": {
        key: {"measurement_id": mid,
              "regions": {region: {"probes": [
                  {"id": probe, "status": "ok", "age_seconds": 10,
                   "latency_ms": 20.0, "packet_loss": 0}
                  for probe in probes]} for region, probes in PROBES.items()}}
        for key, (mid, _) in TARGETS.items()}}


def test_sparse_probe_history_cannot_be_declared_a_frozen_baseline(tmp_path):
    path = tmp_path / "ripe_atlas" / "2026-09-24.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_record(0)) + "\n")
    before = build_baseline(tmp_path, START_AT + 2 * 86400)
    assert before["status"] == "collecting_reference"
    assert before["problems"] == []
    after = build_baseline(tmp_path, END_AT + 1)
    assert after["status"] == "insufficient_frozen_reference"
    assert after["problems"]


def test_reference_is_written_once_after_endpoint(tmp_path):
    path = tmp_path / "ripe_atlas" / "2026-09-24.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_record(0)) + "\n")
    out = tmp_path / "research" / "ripe-baseline.json"
    first = write_report(tmp_path, out, END_AT + 1)
    assert out.exists() and first["status"] == "insufficient_frozen_reference"
    path.write_text("")
    second = write_report(tmp_path, out, END_AT + 500)
    assert second == first
