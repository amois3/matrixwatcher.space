"""Web Push opt-in, safety, and delivery semantics."""

import base64

import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

from src.monitoring import web_push


def subscription(host="fcm.googleapis.com"):
    key = ec.generate_private_key(ec.SECP256R1()).public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=").decode()
    return {"endpoint": f"https://{host}/fcm/send/test-token",
            "keys": {"p256dh": encode(key), "auth": encode(b"0123456789abcdef")}}


def test_rejects_private_or_untrusted_push_endpoints():
    for host in ("127.0.0.1", "localhost", "fcm.googleapis.com.attacker.test"):
        with pytest.raises(ValueError):
            web_push.validate_subscription(subscription(host))
    assert web_push.validate_subscription(subscription())["endpoint"].startswith("https://")


def test_opt_in_topics_and_unsubscribe_are_durable(tmp_path, monkeypatch):
    monkeypatch.setattr(web_push, "PUSH_DIR", tmp_path / "push")
    item = subscription()
    web_push.subscribe(item, ["research"])
    assert web_push.subscription_topics(item["endpoint"]) == ["research"]
    web_push.subscribe(item, ["research", "observations"])
    assert web_push.subscription_topics(item["endpoint"]) == ["observations", "research"]
    web_push.unsubscribe(item["endpoint"])
    assert web_push.subscription_topics(item["endpoint"]) is None


def test_dispatch_only_new_opted_events_and_deduplicates(tmp_path, monkeypatch):
    monkeypatch.setattr(web_push, "PUSH_DIR", tmp_path / "push")
    item = subscription()
    monkeypatch.setattr(web_push.time, "time", lambda: 1000.0)
    web_push.subscribe(item, ["research"])
    events = [
        web_push.PushEvent("before", "research", "Old", "Old report", "/", 999.0),
        web_push.PushEvent("observation", "observations", "Coincidence", "Observation", "/", 1001.0),
        web_push.PushEvent("new", "research", "Report ready", "Review required", "/", 1001.0),
    ]
    monkeypatch.setattr(web_push, "collect_events", lambda now: events)
    sent = []
    monkeypatch.setattr(web_push, "_send", lambda sub, payload: sent.append(payload) or True)
    first = web_push.dispatch_once(1002.0)
    second = web_push.dispatch_once(1003.0)
    assert first["sent"] == 1
    assert second["sent"] == 0
    assert [payload["title"] for payload in sent] == ["Report ready"]


def test_key_stays_stable_and_private(tmp_path, monkeypatch):
    monkeypatch.setattr(web_push, "PUSH_DIR", tmp_path / "push")
    first = web_push.init_keys()
    assert web_push.init_keys() == first
    assert len(base64.urlsafe_b64decode(first + "=" * (-len(first) % 4))) == 65
    assert (tmp_path / "push" / "vapid-private.pem").stat().st_mode & 0o777 == 0o600


def test_transient_delivery_is_retried_without_duplicate_success(tmp_path, monkeypatch):
    monkeypatch.setattr(web_push, "PUSH_DIR", tmp_path / "push")
    item = subscription()
    monkeypatch.setattr(web_push.time, "time", lambda: 1000.0)
    web_push.subscribe(item, ["research"])
    monkeypatch.setattr(web_push, "collect_events", lambda now: [
        web_push.PushEvent("report", "research", "Report", "Review it", "/", 1001.0)
    ])
    attempts = []
    monkeypatch.setattr(web_push, "_send", lambda sub, payload: (attempts.append(1), len(attempts) > 1)[1])
    assert web_push.dispatch_once(1002.0)["failed"] == 1
    assert web_push.dispatch_once(1030.0)["sent"] == 0
    assert web_push.dispatch_once(1062.0)["sent"] == 1
    assert web_push.dispatch_once(2000.0)["sent"] == 0
    assert len(attempts) == 2


def test_collect_events_keeps_interim_study_quiet(tmp_path, monkeypatch):
    import json
    import web.api

    monkeypatch.chdir(tmp_path)
    report_path = tmp_path / "logs" / "research" / "prospective-multiscale.json"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(json.dumps({"status": "collecting_no_interim_test",
                                       "study_version": "test-v1", "generated_at": 1000}))
    monkeypatch.setattr(web.api, "get_active_predictions", lambda use_cache=False: [])
    monkeypatch.setattr(web.api, "get_cached_levels", lambda hours: [])
    assert web_push.collect_events(2000) == []
    report_path.write_text(json.dumps({"status": "exploratory_endpoint_screen_not_validated",
                                       "study_version": "test-v1", "generated_at": 2000}))
    events = web_push.collect_events(2001)
    assert len(events) == 1
    assert "review" in events[0].body.lower()
    assert "not a confirmed discovery" in events[0].body.lower()


def test_api_requires_site_origin_and_can_unsubscribe(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from web.api import app

    monkeypatch.setattr(web_push, "PUSH_DIR", tmp_path / "push")
    web_push.init_keys()
    item = subscription()
    with TestClient(app) as client:
        assert client.get("/api/push/config").json()["enabled"] is True
        assert client.post("/api/push/subscriptions", json={
            "subscription": item, "topics": ["research"]}).status_code == 403
        headers = {"Origin": "https://matrixwatcher.space"}
        added = client.post("/api/push/subscriptions", json={
            "subscription": item, "topics": ["research"]}, headers=headers)
        assert added.status_code == 200
        status = client.post("/api/push/subscriptions/status", json={
            "endpoint": item["endpoint"]}, headers=headers).json()
        assert status == {"subscribed": True, "topics": ["research"]}
        removed = client.request("DELETE", "/api/push/subscriptions", json={
            "endpoint": item["endpoint"]}, headers=headers)
        assert removed.status_code == 200
        assert web_push.subscription_topics(item["endpoint"]) is None


def test_published_finding_requires_independent_evidence(tmp_path, monkeypatch):
    import json
    import web.api

    monkeypatch.chdir(tmp_path)
    registry = tmp_path / "research" / "findings.json"
    registry.parent.mkdir()
    finding = {"id": "example", "title": "Example finding",
               "status": "independently_replicated", "published_at": 2000,
               "report_url": "https://matrixwatcher.space/report",
               "replication_url": "https://example.org/replication"}
    registry.write_text(json.dumps({"schema_version": 1, "findings": [{**finding, "replication_url": ""}]}))
    assert web_push.public_findings() == []
    registry.write_text(json.dumps({"schema_version": 1, "findings": [finding]}))
    monkeypatch.setattr(web.api, "get_active_predictions", lambda use_cache=False: [])
    monkeypatch.setattr(web.api, "get_cached_levels", lambda hours: [])
    events = web_push.collect_events(2001)
    assert len(events) == 1
    assert events[0].key == "finding:example"
    assert "independently replicated" in events[0].title.lower()
