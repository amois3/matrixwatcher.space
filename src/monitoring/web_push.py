"""Opt-in Web Push delivery for observations and research milestones.

Subscriptions and delivery receipts are stored outside Git in logs/push/. A
delivery is never evidence of a discovery: the message links to the public
report for review. Run ``python -m src.monitoring.web_push init`` once, then
``python -m src.monitoring.web_push dispatch`` from a systemd timer.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from pywebpush import WebPushException, webpush

PUSH_DIR = Path(os.environ.get("MATRIX_WATCHER_PUSH_DIR", "logs/push"))
TOPICS = frozenset({"research", "observations"})
CONTACT = "https://matrixwatcher.space"


def public_findings() -> list[dict]:
    """Only manually published, documented independent replications count."""
    try:
        value = json.loads(Path("research/findings.json").read_text())
    except (OSError, ValueError):
        return []
    if not isinstance(value, dict) or value.get("schema_version") != 1 or not isinstance(value.get("findings"), list):
        return []
    results = []
    seen = set()
    for finding in value["findings"]:
        if not isinstance(finding, dict) or finding.get("status") != "independently_replicated":
            continue
        if not isinstance(finding.get("id"), str) or not isinstance(finding.get("title"), str):
            continue
        if not finding["id"] or finding["id"] in seen:
            continue
        if not isinstance(finding.get("published_at"), (int, float)):
            continue
        if not all(isinstance(finding.get(key), str) and finding[key].startswith("https://")
                   for key in ("report_url", "replication_url")):
            continue
        results.append({key: finding[key] for key in
                        ("id", "title", "status", "published_at", "report_url", "replication_url")})
        seen.add(finding["id"])
    return results


def _ensure_dir() -> None:
    PUSH_DIR.mkdir(parents=True, exist_ok=True)
    PUSH_DIR.chmod(0o700)


def _private_path() -> Path:
    return PUSH_DIR / "vapid-private.pem"


def public_key() -> str | None:
    try:
        private = serialization.load_pem_private_key(_private_path().read_bytes(), password=None)
        raw = private.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    except (OSError, ValueError, TypeError):
        return None


def init_keys() -> str:
    """Create a stable P-256 VAPID identity with a private 0600 key."""
    _ensure_dir()
    path = _private_path()
    if not path.exists():
        private = ec.generate_private_key(ec.SECP256R1())
        pem = private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(fd, "wb") as output:
                output.write(pem)
    path.chmod(0o600)
    key = public_key()
    if not key:
        raise RuntimeError("VAPID private key could not be read")
    return key


def _decoded_key(value: str, expected: int) -> bool:
    try:
        data = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        if len(data) != expected:
            return False
        if expected == 65:
            ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), data)
        return True
    except (ValueError, TypeError):
        return False


def validate_subscription(value: dict) -> dict:
    """Reject malformed or private-network destinations before any HTTP send."""
    if not isinstance(value, dict):
        raise ValueError("Invalid subscription")
    endpoint = value.get("endpoint")
    keys = value.get("keys")
    if not isinstance(endpoint, str) or len(endpoint) > 2048 or not isinstance(keys, dict):
        raise ValueError("Invalid subscription endpoint or keys")
    url = urlsplit(endpoint)
    host = (url.hostname or "").lower()
    if url.scheme != "https" or not host or url.username or url.password or url.fragment:
        raise ValueError("Push endpoint must be HTTPS")
    try:
        port = url.port
    except ValueError as error:
        raise ValueError("Invalid push endpoint port") from error
    if port not in (None, 443):
        raise ValueError("Unexpected push endpoint port")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError("IP push endpoints are not accepted")
    allowed = (
        host == "fcm.googleapis.com"
        or host == "updates.push.services.mozilla.com"
        or host == "web.push.apple.com"
        or host.endswith(".push.apple.com")
        or host.endswith(".notify.windows.com")
    )
    if not allowed:
        raise ValueError("Unsupported push service")
    p256dh, auth = keys.get("p256dh"), keys.get("auth")
    if not isinstance(p256dh, str) or not _decoded_key(p256dh, 65):
        raise ValueError("Invalid encryption key")
    if not isinstance(auth, str) or not _decoded_key(auth, 16):
        raise ValueError("Invalid authentication secret")
    return {"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth}}


@contextmanager
def _db():
    _ensure_dir()
    path = PUSH_DIR / "subscriptions.sqlite3"
    connection = sqlite3.connect(path, timeout=10)
    path.chmod(0o600)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("""CREATE TABLE IF NOT EXISTS subscriptions (
        endpoint TEXT PRIMARY KEY, p256dh TEXT NOT NULL, auth TEXT NOT NULL,
        topics TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL,
        last_test_at REAL NOT NULL DEFAULT 0
    )""")
    connection.execute("""CREATE TABLE IF NOT EXISTS deliveries (
        endpoint TEXT NOT NULL, event_key TEXT NOT NULL, state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at REAL NOT NULL DEFAULT 0,
        sent_at REAL, PRIMARY KEY (endpoint, event_key)
    )""")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def subscribe(value: dict, topics: list[str]) -> None:
    subscription = validate_subscription(value)
    if not isinstance(topics, list) or not topics or set(topics) - TOPICS:
        raise ValueError("Invalid notification topics")
    now = time.time()
    with _db() as db:
        db.execute("""INSERT INTO subscriptions
            (endpoint, p256dh, auth, topics, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(endpoint) DO UPDATE SET p256dh=excluded.p256dh,
              auth=excluded.auth, topics=excluded.topics, updated_at=excluded.updated_at""",
            (subscription["endpoint"], subscription["keys"]["p256dh"],
             subscription["keys"]["auth"], json.dumps(sorted(set(topics))), now, now))


def unsubscribe(endpoint: str) -> None:
    if not isinstance(endpoint, str) or len(endpoint) > 2048:
        raise ValueError("Invalid subscription endpoint")
    with _db() as db:
        db.execute("DELETE FROM subscriptions WHERE endpoint=?", (endpoint,))
        db.execute("DELETE FROM deliveries WHERE endpoint=?", (endpoint,))


def subscription_topics(endpoint: str) -> list[str] | None:
    if not isinstance(endpoint, str) or len(endpoint) > 2048:
        raise ValueError("Invalid subscription endpoint")
    with _db() as db:
        row = db.execute("SELECT topics FROM subscriptions WHERE endpoint=?", (endpoint,)).fetchone()
    return json.loads(row[0]) if row else None


def test_subscription(endpoint: str) -> bool:
    """One test per subscription per five minutes, initiated by its browser."""
    now = time.time()
    with _db() as db:
        row = db.execute("SELECT p256dh, auth, last_test_at FROM subscriptions WHERE endpoint=?",
                         (endpoint,)).fetchone()
        if not row:
            raise ValueError("Subscription not found")
        if now - row[2] < 300:
            raise ValueError("Please wait five minutes before another test")
        db.execute("UPDATE subscriptions SET last_test_at=? WHERE endpoint=?", (now, endpoint))
    return _send({"endpoint": endpoint, "keys": {"p256dh": row[0], "auth": row[1]}},
                 {"title": "Matrix Watcher notifications are on",
                  "body": "Test delivered. Research reports still require scientific review.",
                  "url": "/", "tag": "matrix-watcher-test"})


def _send(subscription: dict, payload: dict) -> bool:
    if not public_key():
        raise RuntimeError("VAPID key is not configured")
    response = webpush(
        subscription_info=subscription,
        data=json.dumps(payload, separators=(",", ":")),
        vapid_private_key=str(_private_path()),
        vapid_claims={"sub": CONTACT},
        ttl=3600, timeout=10,
    )
    return response.status_code in (200, 201, 202)


@dataclass(frozen=True)
class PushEvent:
    key: str
    topic: str
    title: str
    body: str
    url: str
    happened_at: float

    def payload(self) -> dict:
        return {"title": self.title, "body": self.body, "url": self.url,
                "tag": "matrix-watcher-" + hashlib.sha256(self.key.encode()).hexdigest()[:20]}


def collect_events(now: float | None = None) -> list[PushEvent]:
    """Only published observations or endpoint reports become push events."""
    now = now or time.time()
    events: list[PushEvent] = []
    for finding in public_findings():
        when = float(finding["published_at"])
        if 0 < when <= now:
            events.append(PushEvent(
                "finding:" + finding["id"], "research",
                "An independently replicated finding was published",
                finding["title"] + ". Read the report and independent replication.",
                "/#findingsPanel", when,
            ))
    reports = (
        ("coincidence", Path("logs/research/prospective-multiscale.json"),
         "exploratory_endpoint_screen_not_validated", "/#prospectiveMultiscale"),
        ("lag", Path("logs/research/prospective-final.json"),
         "prospective_screen_completed_not_replication", "/#lagLab"),
    )
    for name, path, complete_status, url in reports:
        try:
            report = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        status = report.get("status")
        if status not in (complete_status, "insufficient_coverage_or_events_no_test"):
            continue
        when = float(report.get("generated_at") or report.get("end_at") or 0)
        if not 0 < when <= now:
            continue
        events.append(PushEvent(
            f"report:{name}:{report.get('study_version')}:{status}", "research",
            "Matrix Watcher study report is ready",
            ("The frozen future-data report needs coverage and control review; "
             "this is not a confirmed discovery." if status == complete_status else
             "The endpoint lacked enough eligible data for this timing test."),
            url, when,
        ))
    # Use the same public gate as the Validated Signals panel. A signal is not
    # claimed to have independent replication merely because it passed that gate.
    from web.api import get_active_predictions, get_cached_levels
    for signal in get_active_predictions(use_cache=False):
        when = float(signal.get("timestamp") or 0)
        if not 0 < when <= now:
            continue
        identity = str(signal.get("id") or f"{signal.get('event')}:{signal.get('condition')}:{when}")
        events.append(PushEvent(
            "signal:" + identity, "research", "A future-data-tested signal is listed",
            "Review its base rate, misses and independent replication before interpreting it.",
            "/#predictionsPanel", when,
        ))
    for level in get_cached_levels(24):
        when = float(level.get("timestamp") or 0)
        if not 0 < when <= now:
            continue
        domains = int(level.get("domain_count") or level.get("level") or 0)
        if domains < 3:
            continue
        events.append(PushEvent(
            "observation:" + str(level.get("id") or when), "observations",
            "Unusual observations coincided",
            f"{domains} domains within 30 seconds. This is an observation, not a discovery.",
            "/#levelsPanel", when,
        ))
    return events


def dispatch_once(now: float | None = None) -> dict[str, int]:
    """Retry transient failures; de-duplicate successful deliveries per browser."""
    now = now or time.time()
    events = collect_events(now)
    results = {"subscriptions": 0, "events": len(events), "sent": 0, "failed": 0}
    with _db() as db:
        subscribers = db.execute(
            "SELECT endpoint, p256dh, auth, topics, created_at FROM subscriptions"
        ).fetchall()
    results["subscriptions"] = len(subscribers)
    for endpoint, p256dh, auth, topics_json, created_at in subscribers:
        topics = set(json.loads(topics_json))
        for event in events:
            if event.topic not in topics or event.happened_at < created_at:
                continue
            with _db() as db:
                db.execute("""INSERT OR IGNORE INTO deliveries
                    (endpoint, event_key, state) VALUES (?, ?, 'pending')""",
                    (endpoint, event.key))
                row = db.execute("""SELECT state, attempts, next_attempt_at FROM deliveries
                    WHERE endpoint=? AND event_key=?""", (endpoint, event.key)).fetchone()
            if row[0] != "pending" or row[2] > now:
                continue
            try:
                sent = _send({"endpoint": endpoint, "keys": {"p256dh": p256dh, "auth": auth}},
                             event.payload())
            except WebPushException as error:
                code = getattr(getattr(error, "response", None), "status_code", None)
                if code in (404, 410):
                    unsubscribe(endpoint)
                    break
                sent = False
            except Exception:
                sent = False
            with _db() as db:
                db.execute("""UPDATE deliveries SET state=?, attempts=?, next_attempt_at=?,
                    sent_at=? WHERE endpoint=? AND event_key=?""",
                    ("sent" if sent else "pending", row[1] + 1,
                     0 if sent else now + min(3600, 60 * 2 ** min(row[1], 6)),
                     now if sent else None, endpoint, event.key))
            results["sent" if sent else "failed"] += 1
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "dispatch"))
    args = parser.parse_args()
    if args.action == "init":
        init_keys()
        print("Web Push VAPID identity ready")
    else:
        print(json.dumps(dispatch_once()))


if __name__ == "__main__":
    main()
