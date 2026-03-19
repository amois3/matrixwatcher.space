"""Matrix Watcher Web API - FastAPI backend with WebSocket support."""

import asyncio
import bisect
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from src.analyzers.online.digest_generator import build_digest, date_str_utc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Matrix Watcher API", version="1.0.0")

# Simple cache to avoid reading files on every request
_cache = {
    "predictions": {"data": [], "timestamp": 0},
    "levels": {},  # keyed by hours window: {hours: {"data": [...], "timestamp": ts}}
}
CACHE_TTL = 5  # seconds

# A prediction is only worth showing if it beats the event's base rate by at
# least this margin (percentage points). Our 168-day out-of-sample backtest
# found no condition does — so honestly, the panel stays empty until one does.
SKILL_THRESHOLD_PP = 10.0

_base_rates_cache = {"data": None, "mtime": 0.0}


def _load_base_rates() -> dict:
    """Load logs/base_rates.json (event -> base rate 0..1 over the tracker's
    72h matching window). Cached, reloaded when the file changes."""
    path = Path("logs/base_rates.json")
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {}
    if _base_rates_cache["data"] is None or mtime != _base_rates_cache["mtime"]:
        try:
            with open(path) as f:
                _base_rates_cache["data"] = json.load(f).get("base_rate", {})
            _base_rates_cache["mtime"] = mtime
        except Exception:
            _base_rates_cache["data"] = {}
    return _base_rates_cache["data"] or {}


def annotate_skill(predictions: list[dict]) -> list[dict]:
    """Attach base_rate (%) and skill (pp) to each prediction.

    skill = shown probability − base rate of the same event over the same 72h
    window. A near-zero or negative skill means the condition adds nothing over
    "this happens anyway", which is the honest reality for every pattern we have.
    """
    base = _load_base_rates()
    for p in predictions:
        br = base.get(p.get("event"))
        if br is None:
            p["base_rate"] = None
            p["skill"] = None
        else:
            br_pct = round(br * 100, 1)
            p["base_rate"] = br_pct
            p["skill"] = round(p.get("probability", 0) - br_pct, 1)
    return predictions

# CORS for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# HTTPS Redirect middleware for SEO (HTTP -> HTTPS)
class HTTPSRedirectMiddleware(BaseHTTPMiddleware):
    """Redirect HTTP to HTTPS for proper SEO canonical handling."""

    async def dispatch(self, request: Request, call_next):
        # Check X-Forwarded-Proto header (set by Cloudflare/Railway)
        forwarded_proto = request.headers.get("x-forwarded-proto", "https")
        if forwarded_proto == "http" and "localhost" not in str(request.url):
            https_url = str(request.url).replace("http://", "https://", 1)
            return RedirectResponse(https_url, status_code=301)
        return await call_next(request)


app.add_middleware(HTTPSRedirectMiddleware)

# WebSocket connections
connected_clients: list[WebSocket] = []

# Event queues for real-time updates
predictions_queue: asyncio.Queue = asyncio.Queue()
levels_queue: asyncio.Queue = asyncio.Queue()


class ConnectionManager:
    """Manage WebSocket connections."""
    
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Total: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Send message to all connected clients."""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)


manager = ConnectionManager()


def load_recent_anomalies(hours: int = 24) -> list[dict]:
    """Load recent anomalies from logs (only level >= 3, deduplicated by minute)."""
    anomalies = []
    logs_path = Path("logs/anomalies")
    
    if not logs_path.exists():
        return anomalies
    
    cutoff = time.time() - (hours * 3600)
    
    # Read more files for longer periods
    days_to_read = max(3, (hours // 24) + 2)
    
    seen_keys = set()  # For deduplication
    
    for log_file in sorted(logs_path.glob("*.jsonl"), reverse=True)[:days_to_read]:
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        level = data.get("cluster", {}).get("level", 0)
                        ts = data.get("timestamp", 0)
                        
                        if level >= 3 and ts > cutoff:
                            # Create dedup key: minute + level + sorted sources
                            minute = int(ts // 60)
                            sources = sorted(set(
                                a.get("sensor_source", "") 
                                for a in data.get("cluster", {}).get("anomalies", [])
                            ))
                            dedup_key = f"{minute}_{level}_{'-'.join(sources)}"
                            
                            if dedup_key not in seen_keys:
                                seen_keys.add(dedup_key)
                                anomalies.append(data)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error reading {log_file}: {e}")
    
    anomalies.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    return anomalies[:100]


def load_anomalies_for_date(date_str: str) -> list[dict]:
    """Load Level 3+ cluster records for a specific UTC date (YYYY-MM-DD).

    Reads the day's anomaly log (deduplicated by minute/level/sources) so any
    past day can be summarised reproducibly from raw data.
    """
    log_file = Path("logs/anomalies") / f"{date_str}.jsonl"
    if not log_file.exists():
        return []

    seen_keys: set[str] = set()
    records: list[dict] = []
    try:
        with open(log_file, "r") as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue
                level = data.get("cluster", {}).get("level", 0)
                if level < 3:
                    continue
                ts = data.get("timestamp", 0)
                minute = int(ts // 60)
                sources = sorted(set(
                    a.get("sensor_source", "")
                    for a in data.get("cluster", {}).get("anomalies", [])
                ))
                dedup_key = f"{minute}_{level}_{'-'.join(sources)}"
                if dedup_key not in seen_keys:
                    seen_keys.add(dedup_key)
                    records.append(data)
    except Exception as e:
        logger.error(f"Error reading anomalies for {date_str}: {e}")
    return records


def load_patterns() -> dict:
    """Load pattern statistics."""
    patterns_file = Path("logs/patterns/patterns.json")
    
    if not patterns_file.exists():
        return {}
    
    try:
        with open(patterns_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading patterns: {e}")
        return {}


def get_active_predictions(use_cache: bool = True) -> list[dict]:
    """Get active predictions from file (real-time sync with main.py).

    HONEST SYSTEM: Show only the BEST prediction for each event.
    - Group by event type
    - Select the freshest and most specific
    - Show ITS data without manipulation
    """
    # Check cache first
    if use_cache and time.time() - _cache["predictions"]["timestamp"] < CACHE_TTL:
        return _cache["predictions"]["data"]

    predictions_file = Path("logs/predictions/current.json")

    if not predictions_file.exists():
        return []

    try:
        with open(predictions_file, 'r') as f:
            data = json.load(f)

        predictions = data.get("predictions", [])
        last_update = data.get("last_update", 0)

        # Filter out old predictions (older than 24 hours)
        # AND predictions with no meaningful lead time (< 30 min)
        # A prediction of "0.0 hours" is not a prediction - it's current state
        cutoff = time.time() - (24 * 3600)
        MIN_LEAD_TIME = 0.5  # hours (30 minutes minimum for honest predictions)
        active_predictions = [
            p for p in predictions
            if p.get("timestamp", 0) > cutoff
            and p.get("avg_time_hours", 0) >= MIN_LEAD_TIME
        ]

        # DEDUPLICATION: One prediction per event
        # Group by event type
        by_event = {}
        for p in active_predictions:
            event = p.get("event", "unknown")
            if event not in by_event:
                by_event[event] = []
            by_event[event].append(p)

        # Select the BEST for each event
        best_predictions = []
        for event, preds in by_event.items():
            # Sort by priority:
            # 1. More sources in condition (more specific pattern)
            # 2. Fresher (recently created)
            # 3. More observations (more reliable)
            def score_prediction(p):
                condition = p.get("condition", "")
                source_count = condition.count("_") + 1  # L2_crypto_quantum = 2 sources
                timestamp = p.get("timestamp", 0)
                observations = p.get("observations", 0)

                # Priority: specificity > freshness > reliability
                return (
                    source_count,  # More sources = more interesting
                    timestamp,     # Fresher = more relevant
                    min(observations, 1000)  # More reliable, but don't let huge numbers dominate
                )

            best = max(preds, key=score_prediction)
            best_predictions.append(best)

        # Annotate every prediction with its base rate and skill, then keep only
        # those that genuinely beat chance. Honest by construction: if nothing
        # clears the bar (the current reality), the panel shows an empty state
        # rather than impressive-looking numbers that are really just base rates.
        annotate_skill(best_predictions)
        informative = [
            p for p in best_predictions
            if p.get("skill") is not None and p["skill"] >= SKILL_THRESHOLD_PP
        ]
        informative.sort(key=lambda p: p.get("skill", 0), reverse=True)

        # Cache result
        _cache["predictions"]["data"] = informative
        _cache["predictions"]["timestamp"] = time.time()

        return informative

    except Exception as e:
        logger.error(f"Error loading predictions from file: {e}")
        return _cache["predictions"]["data"]  # Return cached on error


def _flare_class(flux: float) -> str:
    """W/m² → NOAA flare class like 'M1.2' (X≥1e-4, M≥1e-5, C≥1e-6, B≥1e-7, else A)."""
    import math
    if flux is None or flux <= 0:
        return ""
    letter, base = ("A", 1e-8)
    for L, b in (("X", 1e-4), ("M", 1e-5), ("C", 1e-6), ("B", 1e-7), ("A", 1e-8)):
        if flux >= b:
            letter, base = L, b
            break
    return f"{letter}{flux / base:.1f}"


_eq_place_cache: dict[str, tuple[float, list]] = {}  # date -> (mtime, [(ts, place)...])


def _earthquake_place_at(ts: float) -> str:
    """USGS place of the strongest quake near ``ts`` (looked up from the raw
    earthquake feed, since the anomaly record only carries the magnitude).
    Returns e.g. '106 km SW of Turpan, China', or '' if unavailable."""
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
    path = Path(f"logs/earthquake/{date_str}.jsonl")
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return ""
    cached = _eq_place_cache.get(date_str)
    if not cached or cached[0] != mtime:
        rows = []
        try:
            with open(path) as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    t = d.get("timestamp")
                    eqs = d.get("earthquakes") or []
                    if isinstance(t, (int, float)) and eqs and eqs[0].get("place"):
                        rows.append((float(t), eqs[0]["place"]))
        except OSError:
            return ""
        rows.sort()
        _eq_place_cache[date_str] = (mtime, rows)
        cached = _eq_place_cache[date_str]

    rows = cached[1]
    if not rows:
        return ""
    times = [r[0] for r in rows]
    i = bisect.bisect_left(times, ts)
    best, best_dt = "", 180.0  # accept only within 3 minutes of the cluster
    for j in (i - 1, i):
        if 0 <= j < len(rows):
            dt = abs(rows[j][0] - ts)
            if dt <= best_dt:
                best_dt, best = dt, rows[j][1]
    return best


def _concise_fact(parameter: str, value, reason: str) -> str:
    """A short, concrete fact for a cluster source — the actual value, not a
    restated rule. e.g. 'M5.3', 'M1.2 flare', '0.94', 'Kp 6', '↓2.1%'."""
    p = parameter or ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = None

    def pct_from_reason() -> str:
        m = re.search(r"(increased|decreased) by ([\d.]+)%", reason)
        if not m:
            return ""
        arrow = "↑" if m.group(1) == "increased" else "↓"
        return f"{arrow}{float(m.group(2)):.1f}%"

    def delta_from_reason(unit: str) -> str:
        m = re.search(r"(increased|decreased) by ([\d.]+)", reason)
        if not m:
            return ""
        sign = "+" if m.group(1) == "increased" else "−"
        return f"{sign}{float(m.group(2)):.1f}{unit}"

    if v is not None:
        if p == "earthquake.max_magnitude":
            return f"M{v:.1f}"
        if p == "earthquake.count":
            return f"{int(v)} quakes/h"
        if p == "quantum_rng.randomness_score":
            return f"{v:.2f} randomness"
        if p == "space_weather.kp_index":
            return f"Kp {v:.0f}"
        if p == "space_weather.flare_count":
            return f"{int(v)} flares/24h"
        if p == "solar_activity.xray_peak_long_flux":
            fc = _flare_class(v)
            return f"{fc} flare" if fc else ""
        if p == "solar_activity.proton_flux_10mev":
            return f"{v:.0f} pfu protons"
        if p == "solar_activity.f107_flux":
            return f"F10.7 = {v:.0f}"
        if p.startswith("volcanic_activity"):
            return f"{int(v)} new this week"

    if p.endswith(".price") or "volume" in p or p == "news.headline_count":
        return pct_from_reason()
    if p == "weather.temperature_celsius":
        return delta_from_reason("°C")
    if p == "weather.pressure_hpa":
        return delta_from_reason(" hPa")
    if p == "blockchain.any_anomalous":
        return "irregular block time"
    return ""


def format_level_event(anomaly: dict) -> dict | None:
    """Format anomaly for level display - detailed like Telegram but in English."""
    cluster = anomaly.get("cluster", {})
    index_data = anomaly.get("index", {})
    
    level = cluster.get("level", 0)
    if level < 3:  # Only show Level 3+ (significant correlations)
        return None
    
    timestamp = anomaly.get("timestamp", time.time())
    
    # Get sources with icons (kept in sync with the digest's DIGEST_SOURCE map)
    source_icons = {
        "crypto": "💰",
        "quantum_rng": "🎲",
        "space_weather": "🛰️",
        "solar_activity": "☀️",
        "volcanic_activity": "🌋",
        "weather": "🌦️",
        "earthquake": "🌍",
        "blockchain": "⛓️",
        "news": "📰",
    }
    
    # Per source, show the ACTUAL fact — concrete value, not a restated rule.
    # e.g. "🌍 Earthquake — M5.3", "☀️ Solar Activity — M1.2 flare", "🎲 Quantum — 0.94".
    # First occurrence of each source wins.
    sources = []
    source_detail: dict[str, str] = {}
    for a in cluster.get("anomalies", []):
        src = a.get("sensor_source", "unknown")
        if src not in sources:
            sources.append(src)
            detail = _concise_fact(
                a.get("parameter", ""),
                a.get("value"),
                (a.get("metadata") or {}).get("reason", "") or "",
            )
            # For earthquakes, add WHERE (USGS place isn't stored on the anomaly,
            # so look it up from the raw feed by timestamp): "M5.3 · Turpan, China".
            if src == "earthquake" and detail.startswith("M"):
                place = _earthquake_place_at(a.get("timestamp", anomaly.get("timestamp", 0)))
                if place:
                    detail = f"{detail} · {place}"
            source_detail[src] = detail

    def _fmt_source(s: str) -> str:
        icon = source_icons.get(s, "📊")
        name = s.replace("_", " ").title()
        detail = source_detail.get(s, "")
        if detail:
            return f"{icon} {name}<span style=\"opacity:.55\"> — {detail}</span>"
        return f"{icon} {name}"

    sources_formatted = [_fmt_source(s) for s in sources]
    
    # Level descriptions (must match digest_generator.LEVEL_NAME exactly)
    level_names = {
        3: "Multiple Correlation",
        4: "Strong Correlation",
        5: "Critical Synchronicity"
    }

    level_icons = {3: "🔴", 4: "🔴🔴", 5: "🚨"}

    # Deviation = how many times above the normal background (the honest ratio
    # the anomaly index already computed). The old code used index/5, an
    # arbitrary divisor that overstated the deviation (e.g. showed 4.3x when the
    # real figure was 1.4x).
    index_val = index_data.get("value", 0)
    deviation = round(index_data.get("baseline_ratio", 1.0), 1)
    
    # Format time in UTC
    dt = datetime.utcfromtimestamp(timestamp)
    today = datetime.utcnow().date()
    if dt.date() == today:
        time_str = dt.strftime("%H:%M UTC")
        date_str = "Today"
    else:
        time_str = dt.strftime("%H:%M UTC")
        date_str = dt.strftime("%d %b")
    
    # System comment based on level
    comments = {
        3: "Stable cluster of deviations detected across multiple independent domains. Observed behavior exceeds normal background.",
        4: "Strong correlation pattern emerging. Multiple sensors showing synchronized anomalous readings.",
        5: "Critical anomaly state. Unprecedented correlation across monitoring systems. Maximum observation priority."
    }
    
    return {
        "id": f"level_{timestamp}",
        "level": level,
        "level_name": level_names.get(level, "Anomaly"),
        "level_icon": level_icons.get(level, "⚠️"),
        "sources": sources,
        "sources_formatted": sources_formatted,
        "sources_str": " + ".join(sources),
        "index": round(index_val, 1),
        "deviation": deviation,
        "status": index_data.get("status", "normal"),
        "timestamp": timestamp,
        "time_str": time_str,
        "date_str": date_str,
        "comment": comments.get(level, "Anomaly detected."),
        "source_count": len(sources)
    }


@app.get("/")
async def root():
    """Serve PWA."""
    return FileResponse("web/static/index.html")


@app.get("/sitemap.xml")
async def sitemap():
    """Serve sitemap at root URL (Google standard)."""
    return FileResponse("web/static/sitemap.xml", media_type="application/xml")


@app.get("/robots.txt")
async def robots():
    """Serve robots.txt at root URL."""
    return FileResponse("web/static/robots.txt", media_type="text/plain")


@app.get("/llms.txt")
async def llms_txt():
    """Serve llms.txt at the root URL (llmstxt.org convention) for AI assistants."""
    return FileResponse("web/static/llms.txt", media_type="text/markdown; charset=utf-8")


@app.get("/api/health")
async def health():
    """Health check."""
    return {"status": "ok", "timestamp": time.time()}


@app.get("/api/predictions")
async def get_predictions():
    """Get active predictions."""
    return {"predictions": get_active_predictions()}


def get_cached_levels(hours: int = 24) -> list[dict]:
    """Get Level 3+ events over the last ``hours``, cached per time window.

    The time-range buttons (1H/24H/3D/7D) pass ``hours``; we must honour it and
    cache per window, otherwise every button returns the same (24h) data.
    """
    # Clamp to the windows the UI actually offers (1h..7d) to avoid silly inputs.
    hours = max(1, min(int(hours), 168))
    slot = _cache["levels"].get(hours)
    if slot and time.time() - slot["timestamp"] < CACHE_TTL:
        return slot["data"]

    anomalies = load_recent_anomalies(hours)
    levels = []
    for a in anomalies:
        formatted = format_level_event(a)
        if formatted:
            levels.append(formatted)
    levels = levels[:50]

    _cache["levels"][hours] = {"data": levels, "timestamp": time.time()}
    return levels


@app.get("/api/levels")
async def get_levels(hours: int = 24):
    """Get recent Level 3+ events over the last ``hours`` (default 24)."""
    return {"levels": get_cached_levels(hours)}


@app.get("/api/stats")
async def get_stats():
    """Get system statistics."""
    patterns = load_patterns()
    
    total_patterns = 0
    crypto_patterns = 0
    
    for cond_key, events in patterns.items():
        for event_type, pattern in events.items():
            if pattern["condition_count"] >= 5:
                total_patterns += 1
                if any(x in event_type for x in ["btc", "eth", "blockchain"]):
                    crypto_patterns += 1
    
    return {
        "total_patterns": total_patterns,
        "crypto_patterns": crypto_patterns,
        "pattern_groups": len(patterns),
        "timestamp": time.time()
    }


@app.get("/api/digest")
async def get_digest(date: str | None = None):
    """Daily digest: an honest narrative summary of a day's observations.

    Query param ``date`` (YYYY-MM-DD, UTC) selects a past day; defaults to today.
    Built straight from the raw anomaly log and current predictions.
    """
    date_str = date or date_str_utc()
    clusters = load_anomalies_for_date(date_str)
    # Predictions are only meaningful for "today"; omit for historical dates.
    predictions = get_active_predictions() if date_str == date_str_utc() else []
    return build_digest(date_str, clusters, predictions)


@app.get("/api/all")
async def get_all_data(hours: int = 72):
    """Get all data in one request. ``hours`` selects the levels time window
    (the 1H/24H/3D/7D buttons)."""
    return {
        "predictions": get_active_predictions(),
        "levels": get_cached_levels(hours),
        "timestamp": time.time()
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates."""
    await manager.connect(websocket)

    try:
        # Send initial data (using cache)
        await websocket.send_json({
            "type": "init",
            "predictions": get_active_predictions(),
            "levels": get_cached_levels()
        })

        # Keep connection alive and listen for messages
        while True:
            try:
                # Wait for message with timeout (60 sec to reduce load)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=60)

                if data == "ping":
                    await websocket.send_json({"type": "pong"})
                elif data == "refresh":
                    await websocket.send_json({
                        "type": "refresh",
                        "predictions": get_active_predictions(),
                        "levels": get_cached_levels()
                    })
            except asyncio.TimeoutError:
                # Send lightweight heartbeat
                await websocket.send_json({"type": "heartbeat"})
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# Broadcast function for main.py to call
async def broadcast_prediction(prediction: dict):
    """Broadcast new prediction to all clients."""
    await manager.broadcast({
        "type": "prediction",
        "data": prediction
    })


async def broadcast_level(level: dict):
    """Broadcast new level event to all clients."""
    await manager.broadcast({
        "type": "level",
        "data": level
    })


# Mount static files
app.mount("/static", StaticFiles(directory="web/static"), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)
