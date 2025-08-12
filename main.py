#!/usr/bin/env python3
"""Matrix Watcher v1.0 - Main Entry Point.

System for discovering hidden patterns and anomalies in digital reality.
"""

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

from src.config import ConfigManager
from src.core.event_bus import EventBus
from src.core.scheduler import Scheduler
from src.core.types import EventType, AnomalyEvent
from src.storage import StorageManager
from src.analyzers.online.anomaly_detector import OnlineAnomalyDetector
from src.analyzers.online.threshold_detector import ThresholdDetector
from src.analyzers.online.smart_analyzer import SmartAnalyzer
from src.analyzers.online.cluster_detector import ClusterDetector
from src.analyzers.online.anomaly_index import AnomalyIndexCalculator
from src.analyzers.online.enhanced_message_generator import EnhancedMessageGenerator
from src.analyzers.online.historical_pattern_tracker import HistoricalPatternTracker, Condition
from src.sensors import (
    SystemSensor,
    TimeDriftSensor,
    NetworkSensor,
    RandomSensor,
    CryptoSensor,
    BlockchainSensor,
    WeatherSensor,
    NewsSensor,
)
from src.sensors.earthquake_sensor import EarthquakeSensor
from src.sensors.space_weather_sensor import SpaceWeatherSensor
from src.sensors.solar_activity_sensor import SolarActivitySensor
from src.sensors.volcanic_activity_sensor import VolcanicActivitySensor
from src.sensors.quantum_rng_sensor import QuantumRNGSensor
from src.monitoring import HealthMonitor, AlertingSystem
from src.monitoring.auto_calibrator import get_auto_calibrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("matrix_watcher.log")
    ]
)
logger = logging.getLogger(__name__)


class MatrixWatcher:
    """Main application class."""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load()
        
        self.event_bus = EventBus()
        self.scheduler = Scheduler()
        self.storage = StorageManager(
            base_path=self.config.storage.base_path,
            compression=self.config.storage.compression,
            buffer_size=self.config.storage.buffer_size
        )
        # Use threshold detector instead of z-score detector
        self.anomaly_detector = ThresholdDetector(event_bus=self.event_bus)
        self.smart_analyzer = SmartAnalyzer(
            lookback_seconds=getattr(self.config.analysis, "precursor_lookback_seconds", 60),
            correlation_threshold=self.config.analysis.correlation_threshold,
            precursor_threshold=self.config.analysis.precursor_threshold
        )
        self.cluster_detector = ClusterDetector(
            cluster_window_seconds=self.config.analysis.cluster_window_seconds,
            precursor_window_seconds=3600.0
        )
        # Enhanced system with Anomaly Index
        self.anomaly_index = AnomalyIndexCalculator(baseline_window_hours=24)
        self.enhanced_message_gen = EnhancedMessageGenerator()
        
        # Historical Pattern Tracker for probabilistic estimates
        self.pattern_tracker = HistoricalPatternTracker(storage_path="logs/patterns")
        
        # Auto-calibrator for automatic threshold optimization
        # auto_apply=True: system auto-applies changes with high confidence and notifies
        self.auto_calibrator = get_auto_calibrator(auto_apply=False)
        
        self.health_monitor = HealthMonitor(
            port=8080,
            failure_threshold=3,
            on_sensor_disabled=self._on_sensor_disabled
        )
        self.alerting = AlertingSystem(cooldown_seconds=300)
        
        self._running = False
        self._sensors = {}
        self._loop = None
        
        # Anomaly tracking for clusters
        self._recent_anomalies: list[dict] = []
        self._cluster_window = self.config.analysis.cluster_window_seconds

    def _on_sensor_disabled(self, sensor_name: str):
        """Handle sensor being disabled — log only (no external notifications)."""
        logger.warning(f"Sensor {sensor_name} was disabled due to failures")
    
    def _save_patterns(self):
        """Save pattern tracker data periodically."""
        try:
            self.pattern_tracker.save()
            
            # Log calibration stats
            stats = self.pattern_tracker.get_calibration_stats()
            if stats["total_patterns"] > 0:
                logger.info(
                    f"Pattern Tracker: {stats['total_patterns']} patterns, "
                    f"Brier score: {stats['avg_brier_score']:.3f}, "
                    f"Well calibrated: {stats['well_calibrated_percent']:.0f}%"
                )
        except Exception as e:
            logger.error(f"Error saving patterns: {e}")
    
    def _refresh_predictions_file(self):
        """Refresh predictions file with current active conditions.
        
        This ensures PWA always shows fresh predictions even without new anomalies.
        Runs every 60 seconds to keep predictions up-to-date.
        """
        try:
            import json
            from pathlib import Path
            from datetime import datetime
            
            predictions_dir = Path("logs/predictions")
            predictions_file = predictions_dir / "current.json"
            
            # If file doesn't exist or is old, regenerate from recent conditions
            if not predictions_file.exists():
                return
            
            # Load current file
            with open(predictions_file, 'r') as f:
                data = json.load(f)
            
            predictions = data.get("predictions", [])
            
            # Filter out old predictions (older than 24 hours)
            current_time = time.time()
            cutoff = current_time - (24 * 3600)
            
            active_predictions = [
                p for p in predictions 
                if p.get("timestamp", 0) > cutoff
                and p.get("event") != "earthquake_moderate"  # Remove M5.0+ (too frequent)
            ]
            
            removed_count = len(predictions) - len(active_predictions)
            logger.info(f"Refreshed predictions: {len(predictions)} → {len(active_predictions)} (removed {removed_count} old/M5.0+)")
            
            # Update file with filtered predictions
            data["predictions"] = active_predictions
            data["last_update"] = current_time
            data["last_update_str"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            with open(predictions_file, "w") as f:
                json.dump(data, f, indent=2)
            
            logger.debug(f"Refreshed predictions file: {len(active_predictions)} active predictions")
            
        except Exception as e:
            logger.error(f"Error refreshing predictions file: {e}")
    
    def _save_predictions_to_file(self, condition: Condition, probabilities: dict):
        """Save current predictions to file for PWA real-time access.
        
        The PWA reads this file directly; it is the single source of truth
        for predictions. Accumulates predictions from multiple conditions
        instead of overwriting.
        """
        try:
            import json
            from pathlib import Path
            from datetime import datetime
            
            logger.info(f"💾 Saving predictions: {len(probabilities)} events for {condition.to_key()}")
            
            predictions_dir = Path("logs/predictions")
            predictions_dir.mkdir(parents=True, exist_ok=True)
            predictions_file = predictions_dir / "current.json"
            
            # Load existing predictions
            existing_predictions = []
            if predictions_file.exists():
                try:
                    with open(predictions_file, 'r') as f:
                        data = json.load(f)
                        existing_predictions = data.get("predictions", [])
                        logger.info(f"💾 Loaded {len(existing_predictions)} existing predictions")
                except Exception:
                    pass
            
            # Deduplicate sources
            unique_sources = list(dict.fromkeys(condition.sources))
            
            # Format NEW predictions for this condition
            new_predictions = []
            for event_type, pred in probabilities.items():
                # Icons and colors by event type
                if "pump" in event_type:
                    icon = "📈"
                    color = "#00ff88"
                elif "dump" in event_type:
                    icon = "📉"
                    color = "#ff4444"
                elif "volatility" in event_type:
                    icon = "⚡"
                    color = "#ffaa00"
                elif "blockchain" in event_type:
                    icon = "⛓️"
                    color = "#8888ff"
                elif "earthquake" in event_type:
                    icon = "🌍"
                    color = "#ff9500"
                elif "solar" in event_type:
                    icon = "☀️"
                    color = "#ffee00"
                else:
                    icon = "📊"
                    color = "#8888ff"
                
                pred_id = f"{condition.to_key()}_{event_type}"

                prediction_data = {
                    "id": pred_id,
                    "condition": condition.to_key(),
                    "condition_level": condition.level,
                    "condition_sources": unique_sources,
                    "event": event_type,
                    "description": pred.get("description", event_type),
                    "probability": round(pred["probability"] * 100),
                    "avg_time_hours": round(pred["avg_time_hours"], 1),
                    "observations": pred["observations"],
                    "occurrences": pred.get("occurrences", 0),
                    "category": pred.get("category", "other"),
                    "icon": icon,
                    "color": color,
                    "timestamp": condition.timestamp
                }

                # Add temporal info if available
                if pred.get("temporal_pattern"):
                    prediction_data["temporal_pattern"] = True
                    prediction_data["time_bucket"] = pred.get("time_bucket", "")
                    prediction_data["is_weekend"] = pred.get("is_weekend", False)

                # Add region for earthquake events
                if pred.get("region"):
                    prediction_data["region"] = pred.get("region")

                new_predictions.append(prediction_data)
            
            # Merge: remove old predictions with same ID, add new ones
            existing_ids = {p["id"] for p in new_predictions}
            merged_predictions = [p for p in existing_predictions if p["id"] not in existing_ids]
            merged_predictions.extend(new_predictions)
            
            logger.info(f"💾 Merged: {len(existing_predictions)} existing + {len(new_predictions)} new = {len(merged_predictions)} total")
            
            # Filter out old predictions (older than 24 hours) AND M5.0+
            current_time = time.time()
            cutoff = current_time - (24 * 3600)
            active_predictions = [
                p for p in merged_predictions 
                if p.get("timestamp", 0) > cutoff
                and p.get("event") != "earthquake_moderate"  # Remove M5.0+ (too frequent)
            ]
            
            logger.info(f"💾 After filtering: {len(active_predictions)} active predictions")
            
            # Count by category
            crypto_count = sum(1 for p in active_predictions if p.get("category") == "crypto")
            earthquake_count = sum(1 for p in active_predictions if p.get("category") == "earthquake")
            logger.info(f"💾 Categories: {crypto_count} crypto, {earthquake_count} earthquake")
            
            # Sort by probability (desc), then by time (asc)
            active_predictions.sort(key=lambda x: (-x["probability"], x["avg_time_hours"]))
            
            # Save to file
            data = {
                "predictions": active_predictions[:50],  # Top 50 (increased from 20)
                "last_update": current_time,
                "last_update_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            with open(predictions_file, "w") as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"💾 Saved {len(active_predictions)} predictions to file (added {len(new_predictions)} new)")
            
        except Exception as e:
            logger.error(f"Error saving predictions to file: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _auto_calibration_loop(self):
        """Periodically check and perform auto-calibration."""
        while self._running:
            try:
                # Wait 24 hours between checks
                await asyncio.sleep(86400)
                
                logger.info("🔬 Running auto-calibration check...")
                
                # Check and calibrate
                results = self.auto_calibrator.check_and_calibrate()
                
                if results["status"] == "not_ready":
                    logger.info(f"Auto-calibration: {results['message']}")
                
                elif results["status"] == "waiting":
                    logger.info(f"Auto-calibration: {results['message']}")
                
                elif results["status"] == "completed":
                    logger.info(f"✅ Auto-calibration completed: {results['summary']}")
                
            except Exception as e:
                logger.error(f"Error in auto-calibration loop: {e}")
    
    def _log_anomaly_index(self):
        """Log current Anomaly Index periodically."""
        logger.debug("_log_anomaly_index: Starting")
        try:
            # Get recent anomalies from cluster detector
            recent_anomalies = []
            current_time = time.time()
            
            logger.debug(f"_log_anomaly_index: Checking {len(self.cluster_detector._recent_anomalies)} recent anomalies")
            
            # Get all anomalies from _recent_anomalies deque
            for item in self.cluster_detector._recent_anomalies:
                if current_time - item["timestamp"] < 300:  # Last 5 minutes
                    recent_anomalies.append(item["anomaly"])
            
            # Calculate index
            if recent_anomalies:
                snapshot = self.anomaly_index.calculate(recent_anomalies)
            else:
                # No recent anomalies - create empty snapshot
                snapshot = self.anomaly_index.calculate([])
            
            # Log to storage
            self.storage.write_record("anomaly_index", {
                "timestamp": snapshot.timestamp,
                "source": "anomaly_index",
                "index": snapshot.index,
                "breakdown": snapshot.breakdown,
                "baseline_ratio": snapshot.baseline_ratio,
                "status": snapshot.status,
                "active_anomalies_count": len(recent_anomalies)
            })
            
            # Log to console if elevated or every 10 runs
            if snapshot.status != "normal":
                logger.info(f"Anomaly Index: {snapshot.index:.1f}/100 ({snapshot.status})")
        
        except Exception as e:
            logger.error(f"Error logging anomaly index: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _handle_anomaly(self, anomaly: AnomalyEvent):
        """Run cluster / pattern / prediction analysis for an anomaly.

        Always runs — independent of any external notifier. The PWA reads the
        files written here (``logs/anomalies/*.jsonl``, ``logs/predictions/current.json``)
        as the single source of truth.
        """
        # Record for smart analysis
        self.smart_analyzer.record_anomaly(anomaly)

        # Detect cluster (returns level 1-5)
        cluster = self.cluster_detector.add_anomaly(anomaly)
        if not cluster:
            return

        # Anomaly Index across the cluster's anomalies
        index_snapshot = self.anomaly_index.calculate(cluster.anomalies)

        # Record condition for pattern tracking (unique sources only)
        unique_sources = sorted(set(a.sensor_source for a in cluster.anomalies))
        condition = Condition(
            timestamp=cluster.timestamp,
            level=cluster.level,
            sources=unique_sources,
            anomaly_index=index_snapshot.index,
            baseline_ratio=index_snapshot.baseline_ratio,
        )
        self.pattern_tracker.record_condition(condition)

        # Probabilistic estimates for all public event categories
        probabilities = self.pattern_tracker.get_probabilities(condition, category_filter=None)

        # Persist predictions for the PWA (single source of truth)
        if probabilities:
            self._save_predictions_to_file(condition, probabilities)

        # Save detailed cluster log with index and probabilities
        self.storage.write_anomaly({
            "source": "anomalies",
            "cluster": {
                "level": cluster.level,
                "timestamp": cluster.timestamp,
                "probability": cluster.probability,
                "anomalies": [a.to_dict() for a in cluster.anomalies]
            },
            "index": {
                "value": index_snapshot.index,
                "status": index_snapshot.status,
                "baseline_ratio": index_snapshot.baseline_ratio,
                "breakdown": index_snapshot.breakdown
            },
            "probabilities": probabilities,
            "timestamp": time.time()
        })
    
    def _setup_sensors(self):
        """Initialize and register all sensors."""
        # System Sensor
        sensor_cfg = self.config.sensors.get("system")
        if sensor_cfg and sensor_cfg.enabled:
            system_sensor = SystemSensor(event_bus=self.event_bus)
            self._sensors["system"] = system_sensor
            self.health_monitor.register_sensor("system")
            
            def collect_system(s=system_sensor):
                try:
                    data = s.collect_data()
                    timestamp = data["local_time_unix"]
                    self.storage.write_record("system", {"timestamp": timestamp, "source": "system", **data})
                    # Publish to event bus for anomaly detection
                    from src.core.types import Event, EventType
                    event = Event(timestamp=timestamp, source="system", event_type=EventType.DATA, payload=data)
                    self.event_bus.publish(event)
                    self.health_monitor.record_success("system")
                except Exception as e:
                    logger.error(f"System sensor error: {e}")
                    self.health_monitor.record_failure("system", str(e))
            
            self.scheduler.register_task("system", collect_system, interval=sensor_cfg.interval_seconds)
        
        # Time Drift Sensor
        sensor_cfg = self.config.sensors.get("time_drift")
        if sensor_cfg and sensor_cfg.enabled:
            time_drift_sensor = TimeDriftSensor(event_bus=self.event_bus)
            self._sensors["time_drift"] = time_drift_sensor
            self.health_monitor.register_sensor("time_drift")
            
            async def collect_time_drift(s=time_drift_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("time_drift", {"timestamp": reading.timestamp, "source": "time_drift", **reading.data})
                        self.health_monitor.record_success("time_drift")
                    else:
                        self.health_monitor.record_failure("time_drift", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("time_drift", str(e))
            
            self.scheduler.register_task("time_drift", lambda f=collect_time_drift: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Network Sensor
        sensor_cfg = self.config.sensors.get("network")
        if sensor_cfg and sensor_cfg.enabled:
            network_sensor = NetworkSensor(event_bus=self.event_bus)
            self._sensors["network"] = network_sensor
            self.health_monitor.register_sensor("network")
            
            async def collect_network(s=network_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("network", {"timestamp": reading.timestamp, "source": "network", **reading.data})
                        self.health_monitor.record_success("network")
                    else:
                        self.health_monitor.record_failure("network", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("network", str(e))
            
            self.scheduler.register_task("network", lambda f=collect_network: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Random Sensor
        sensor_cfg = self.config.sensors.get("random")
        if sensor_cfg and sensor_cfg.enabled:
            random_sensor = RandomSensor(event_bus=self.event_bus)
            self._sensors["random"] = random_sensor
            self.health_monitor.register_sensor("random")
            
            async def collect_random(s=random_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("random", {"timestamp": reading.timestamp, "source": "random", **reading.data})
                        self.health_monitor.record_success("random")
                    else:
                        self.health_monitor.record_failure("random", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("random", str(e))
            
            self.scheduler.register_task("random", lambda f=collect_random: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Crypto Sensor
        sensor_cfg = self.config.sensors.get("crypto")
        if sensor_cfg and sensor_cfg.enabled:
            crypto_sensor = CryptoSensor(event_bus=self.event_bus)
            self._sensors["crypto"] = crypto_sensor
            self.health_monitor.register_sensor("crypto")
            
            async def collect_crypto(s=crypto_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("crypto", {"timestamp": reading.timestamp, "source": "crypto", **reading.data})
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("crypto")
                    else:
                        self.health_monitor.record_failure("crypto", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("crypto", str(e))
            
            self.scheduler.register_task("crypto", lambda f=collect_crypto: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Blockchain Sensor
        sensor_cfg = self.config.sensors.get("blockchain")
        if sensor_cfg and sensor_cfg.enabled:
            blockchain_sensor = BlockchainSensor(event_bus=self.event_bus)
            self._sensors["blockchain"] = blockchain_sensor
            self.health_monitor.register_sensor("blockchain")
            
            async def collect_blockchain(s=blockchain_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("blockchain", {"timestamp": reading.timestamp, "source": "blockchain", **reading.data})
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("blockchain")
                    else:
                        self.health_monitor.record_failure("blockchain", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("blockchain", str(e))
            
            self.scheduler.register_task("blockchain", lambda f=collect_blockchain: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Weather Sensor
        sensor_cfg = self.config.sensors.get("weather")
        if sensor_cfg and sensor_cfg.enabled:
            # Get API key from api_keys section or custom_params
            api_key = self.config.api_keys.get("openweathermap") or (
                sensor_cfg.custom_params.get("api_key") if sensor_cfg.custom_params else None
            )
            location = sensor_cfg.custom_params.get("location") if sensor_cfg.custom_params else None
            weather_sensor = WeatherSensor(event_bus=self.event_bus, api_key=api_key, location=location)
            self._sensors["weather"] = weather_sensor
            self.health_monitor.register_sensor("weather")
            
            async def collect_weather(s=weather_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("weather", {"timestamp": reading.timestamp, "source": "weather", **reading.data})
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("weather")
                    else:
                        self.health_monitor.record_failure("weather", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("weather", str(e))
            
            self.scheduler.register_task("weather", lambda f=collect_weather: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # News Sensor
        sensor_cfg = self.config.sensors.get("news")
        if sensor_cfg and sensor_cfg.enabled:
            news_sensor = NewsSensor(event_bus=self.event_bus)
            self._sensors["news"] = news_sensor
            self.health_monitor.register_sensor("news")
            
            async def collect_news(s=news_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("news", {"timestamp": reading.timestamp, "source": "news", **reading.data})
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("news")
                    else:
                        self.health_monitor.record_failure("news", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("news", str(e))
            
            self.scheduler.register_task("news", lambda f=collect_news: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Earthquake Sensor
        sensor_cfg = self.config.sensors.get("earthquake")
        if sensor_cfg and sensor_cfg.enabled:
            min_mag = sensor_cfg.custom_params.get("min_magnitude", 4.5) if sensor_cfg.custom_params else 4.5
            earthquake_sensor = EarthquakeSensor(event_bus=self.event_bus, min_magnitude=min_mag)
            self._sensors["earthquake"] = earthquake_sensor
            self.health_monitor.register_sensor("earthquake")
            
            async def collect_earthquake(s=earthquake_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("earthquake", {"timestamp": reading.timestamp, "source": "earthquake", **reading.data})
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("earthquake")
                    else:
                        self.health_monitor.record_failure("earthquake", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("earthquake", str(e))
            
            self.scheduler.register_task("earthquake", lambda f=collect_earthquake: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Space Weather Sensor
        sensor_cfg = self.config.sensors.get("space_weather")
        if sensor_cfg and sensor_cfg.enabled:
            space_sensor = SpaceWeatherSensor(event_bus=self.event_bus)
            self._sensors["space_weather"] = space_sensor
            self.health_monitor.register_sensor("space_weather")
            
            async def collect_space(s=space_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record("space_weather", {"timestamp": reading.timestamp, "source": "space_weather", **reading.data})
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("space_weather")
                    else:
                        self.health_monitor.record_failure("space_weather", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("space_weather", str(e))
            
            self.scheduler.register_task("space_weather", lambda f=collect_space: asyncio.run(f()), interval=sensor_cfg.interval_seconds)

        # Solar Activity Sensor (F10.7 + GOES X-ray + integral protons)
        sensor_cfg = self.config.sensors.get("solar_activity")
        if sensor_cfg and sensor_cfg.enabled:
            solar_sensor = SolarActivitySensor(event_bus=self.event_bus)
            self._sensors["solar_activity"] = solar_sensor
            self.health_monitor.register_sensor("solar_activity")

            async def collect_solar(s=solar_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record(
                            "solar_activity",
                            {"timestamp": reading.timestamp, "source": "solar_activity", **reading.data},
                        )
                        self.health_monitor.record_success("solar_activity")
                    else:
                        self.health_monitor.record_failure("solar_activity", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("solar_activity", str(e))

            self.scheduler.register_task(
                "solar_activity",
                lambda f=collect_solar: asyncio.run(f()),
                interval=sensor_cfg.interval_seconds,
            )

        # Volcanic Activity Sensor (Smithsonian/USGS weekly RSS)
        sensor_cfg = self.config.sensors.get("volcanic_activity")
        if sensor_cfg and sensor_cfg.enabled:
            volcanic_sensor = VolcanicActivitySensor(event_bus=self.event_bus)
            self._sensors["volcanic_activity"] = volcanic_sensor
            self.health_monitor.register_sensor("volcanic_activity")

            async def collect_volcanic(s=volcanic_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        self.storage.write_record(
                            "volcanic_activity",
                            {"timestamp": reading.timestamp, "source": "volcanic_activity", **reading.data},
                        )
                        self.health_monitor.record_success("volcanic_activity")
                    else:
                        self.health_monitor.record_failure("volcanic_activity", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("volcanic_activity", str(e))

            self.scheduler.register_task(
                "volcanic_activity",
                lambda f=collect_volcanic: asyncio.run(f()),
                interval=sensor_cfg.interval_seconds,
            )

        # Quantum RNG Sensor
        sensor_cfg = self.config.sensors.get("quantum_rng")
        if sensor_cfg and sensor_cfg.enabled:
            sample_size = sensor_cfg.custom_params.get("sample_size", 1024) if sensor_cfg.custom_params else 1024
            anu_api_key = self.config.api_keys.get("anu_qrng")
            qrng_sensor = QuantumRNGSensor(event_bus=self.event_bus, sample_size=sample_size, anu_api_key=anu_api_key)
            self._sensors["quantum_rng"] = qrng_sensor
            self.health_monitor.register_sensor("quantum_rng")
            
            async def collect_qrng(s=qrng_sensor):
                try:
                    reading = await s.safe_collect()
                    if reading:
                        # Don't overwrite 'source' field from reading.data (it contains actual source: anu_quantum, random_org, etc)
                        record = {"timestamp": reading.timestamp, **reading.data}
                        self.storage.write_record("quantum_rng", record)
                        # Event already published by BaseSensor.safe_collect()
                        self.health_monitor.record_success("quantum_rng")
                    else:
                        self.health_monitor.record_failure("quantum_rng", "Collection returned None")
                except Exception as e:
                    self.health_monitor.record_failure("quantum_rng", str(e))
            
            self.scheduler.register_task("quantum_rng", lambda f=collect_qrng: asyncio.run(f()), interval=sensor_cfg.interval_seconds)
        
        # Register Anomaly Index logging task (every 60 seconds)
        self.scheduler.register_task("anomaly_index_logger", self._log_anomaly_index, interval=60.0)
        
        # Register Pattern Tracker save task (every 5 minutes)
        self.scheduler.register_task("pattern_tracker_save", self._save_patterns, interval=300.0)
        
        # Register Predictions refresh task (every 60 seconds)
        # This ensures PWA always has fresh predictions even without new anomalies
        self.scheduler.register_task("predictions_refresh", self._refresh_predictions_file, interval=60.0)
        
        logger.info(f"Registered {len(self._sensors)} sensors + anomaly index logger + pattern tracker + predictions refresh")

    def _setup_event_handlers(self):
        """Set up event bus handlers."""
        def on_data(event):
            # Record event for smart analysis
            self.smart_analyzer.record_event(event)
            
            # Check for pattern events (for predictions)
            # Event has 'payload' not 'data'
            payload = getattr(event, 'payload', None)
            if payload:
                source = payload.get('source', getattr(event, 'source', 'unknown'))
                
                # Log for debugging
                if source == 'earthquake':
                    mag = payload.get('max_magnitude', 0)
                    logger.info(f"📊 Earthquake: mag={mag}")
                
                # Log quantum_rng data
                if 'randomness_score' in payload:
                    rs = payload.get('randomness_score', 1.0)
                    logger.info(f"🎲 Quantum RNG: randomness={rs:.3f}")
                
                detected_events = self.pattern_tracker.check_events(payload)
                for evt in detected_events:
                    logger.info(f"🎯 Pattern event detected: {evt.event_type} ({evt.severity})")
            
            # Detect anomalies
            anomalies = self.anomaly_detector.process(event)
            for anomaly in anomalies:
                self.storage.write_anomaly(anomaly.to_dict())
                # Run cluster / pattern / prediction analysis (always — file-driven, no notifier gating)
                if self._loop:
                    asyncio.run_coroutine_threadsafe(
                        self._handle_anomaly(anomaly),
                        self._loop
                    )
        
        self.event_bus.subscribe(on_data, event_types=[EventType.DATA])
    
    async def start_async(self):
        """Start Matrix Watcher asynchronously."""
        logger.info("Starting Matrix Watcher v1.0")
        
        self._setup_sensors()
        self._setup_event_handlers()
        
        # Start health monitor
        await self.health_monitor.start()
        
        self._running = True
        self.scheduler.start()
        
        # Start auto-calibration check (runs once per day)
        asyncio.create_task(self._auto_calibration_loop())

        logger.info("Matrix Watcher is running. Press Ctrl+C to stop.")
        logger.info(f"Health endpoint: http://localhost:{self.health_monitor.port}/health")
        
        # Log calibration status
        cal_status = self.auto_calibrator.get_calibration_status()
        if cal_status["ready_for_calibration"]:
            logger.info("🔬 Auto-calibration: READY")
        else:
            days_left = cal_status["days_needed"] - cal_status["days_collecting"]
            logger.info(f"🔬 Auto-calibration: {days_left:.1f} days until ready")
    
    def start(self):
        """Start Matrix Watcher."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.start_async())
    
    async def stop_async(self):
        """Stop Matrix Watcher asynchronously."""
        logger.info("Stopping Matrix Watcher...")
        self._running = False
        self.scheduler.stop()
        await self.health_monitor.stop()
        self.storage.flush_all()
        self.storage.close()
        
        # Save pattern tracker data
        self.pattern_tracker.save()
        logger.info("Pattern tracker data saved")

        logger.info("Matrix Watcher stopped.")
    
    def stop(self):
        """Stop Matrix Watcher."""
        if self._loop:
            self._loop.run_until_complete(self.stop_async())
    
    def run(self):
        """Run Matrix Watcher until interrupted."""
        def signal_handler(sig, frame):
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        self.start()
        
        try:
            while self._running:
                self._loop.run_until_complete(asyncio.sleep(1))
                self.health_monitor.log_health()
        except KeyboardInterrupt:
            self.stop()


def main():
    """Main entry point."""
    watcher = MatrixWatcher()
    watcher.run()


if __name__ == "__main__":
    main()
