"""Configuration schema definitions for Matrix Watcher.

This module defines dataclasses for all configuration sections:
- SensorConfig: Per-sensor configuration
- StorageConfig: Data storage settings
- AnalysisConfig: Analysis parameters
- AlertingConfig: Alerting/notification settings
- Config: Root configuration object
"""

from dataclasses import dataclass, field, asdict
from typing import Any
import json


@dataclass
class SensorConfig:
    """Configuration for a single sensor.
    
    Attributes:
        enabled: Whether the sensor is active
        interval_seconds: How often to collect data
        priority: Execution priority (high, medium, low)
        custom_params: Sensor-specific parameters
    """
    enabled: bool = True
    interval_seconds: float = 5.0
    priority: str = "medium"
    custom_params: dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.interval_seconds < 0.1:
            self.interval_seconds = 0.1
        if self.interval_seconds > 3600:
            self.interval_seconds = 3600
        if self.priority not in ("high", "medium", "low"):
            self.priority = "medium"
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SensorConfig":
        return cls(
            enabled=data.get("enabled", True),
            interval_seconds=data.get("interval_seconds", 5.0),
            priority=data.get("priority", "medium"),
            custom_params=data.get("custom_params", {})
        )


@dataclass
class StorageConfig:
    """Configuration for data storage.
    
    Attributes:
        base_path: Root directory for log files
        compression: Whether to use gzip compression
        max_file_size_mb: Maximum file size before rotation
        buffer_size: Number of records to buffer before flush
    """
    base_path: str = "logs"
    compression: bool = False
    max_file_size_mb: int = 100
    buffer_size: int = 1000
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.max_file_size_mb < 1:
            self.max_file_size_mb = 1
        if self.max_file_size_mb > 10000:
            self.max_file_size_mb = 10000
        if self.buffer_size < 1:
            self.buffer_size = 1
        if self.buffer_size > 100000:
            self.buffer_size = 100000
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StorageConfig":
        return cls(
            base_path=data.get("base_path", "logs"),
            compression=data.get("compression", False),
            max_file_size_mb=data.get("max_file_size_mb", 100),
            buffer_size=data.get("buffer_size", 1000)
        )


@dataclass
class AnalysisConfig:
    """Configuration for analysis parameters.
    
    Attributes:
        window_size: Size of sliding window for online analysis
        z_score_threshold: Threshold for anomaly detection
        lag_range_seconds: Range for lag-correlation analysis
        cluster_window_seconds: Time window for clustering anomalies
        correlation_threshold: Minimum correlation to flag as significant
        precursor_threshold: Minimum frequency to flag as precursor
    """
    window_size: int = 100
    z_score_threshold: float = 4.0
    lag_range_seconds: int = 60
    cluster_window_seconds: float = 3.0
    correlation_threshold: float = 0.7
    precursor_threshold: float = 0.3
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.window_size < 10:
            self.window_size = 10
        if self.window_size > 10000:
            self.window_size = 10000
        if self.z_score_threshold < 1.0:
            self.z_score_threshold = 1.0
        if self.z_score_threshold > 10.0:
            self.z_score_threshold = 10.0
        if self.lag_range_seconds < 1:
            self.lag_range_seconds = 1
        if self.lag_range_seconds > 3600:
            self.lag_range_seconds = 3600
        if self.cluster_window_seconds < 0.1:
            self.cluster_window_seconds = 0.1
        if self.cluster_window_seconds > 60.0:
            self.cluster_window_seconds = 60.0
        if self.correlation_threshold < 0.0:
            self.correlation_threshold = 0.0
        if self.correlation_threshold > 1.0:
            self.correlation_threshold = 1.0
        if self.precursor_threshold < 0.0:
            self.precursor_threshold = 0.0
        if self.precursor_threshold > 1.0:
            self.precursor_threshold = 1.0
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AnalysisConfig":
        return cls(
            window_size=data.get("window_size", 100),
            z_score_threshold=data.get("z_score_threshold", 4.0),
            lag_range_seconds=data.get("lag_range_seconds", 60),
            cluster_window_seconds=data.get("cluster_window_seconds", 3.0),
            correlation_threshold=data.get("correlation_threshold", 0.7),
            precursor_threshold=data.get("precursor_threshold", 0.3)
        )


@dataclass
class TelegramConfig:
    """Configuration for Telegram bot.
    
    Attributes:
        enabled: Whether Telegram notifications are active
        token: Bot API token from @BotFather
        chat_id: Chat ID to send messages to
        cooldown_seconds: Minimum time between similar messages
    """
    enabled: bool = False
    token: str = ""
    chat_id: str = ""
    cooldown_seconds: int = 60
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.cooldown_seconds < 0:
            self.cooldown_seconds = 0
        if self.cooldown_seconds > 86400:
            self.cooldown_seconds = 86400
    
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TelegramConfig":
        return cls(
            enabled=data.get("enabled", False),
            token=data.get("token", ""),
            chat_id=data.get("chat_id", ""),
            cooldown_seconds=data.get("cooldown_seconds", 60)
        )


@dataclass
class AlertingConfig:
    """Configuration for alerting system.
    
    Attributes:
        enabled: Whether alerting is active
        webhook_url: URL for webhook notifications
        cooldown_seconds: Minimum time between duplicate alerts
        alert_on_clusters: Alert on multi-source anomaly clusters
        alert_on_correlations: Alert on significant correlations
        alert_on_sensor_failure: Alert on sensor failures
        min_cluster_sensors: Minimum sensors for cluster alert
        telegram: Telegram bot configuration
    """
    enabled: bool = False
    webhook_url: str | None = None
    cooldown_seconds: int = 300
    alert_on_clusters: bool = True
    alert_on_correlations: bool = True
    alert_on_sensor_failure: bool = True
    min_cluster_sensors: int = 3
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.cooldown_seconds < 0:
            self.cooldown_seconds = 0
        if self.cooldown_seconds > 86400:
            self.cooldown_seconds = 86400
        if self.min_cluster_sensors < 2:
            self.min_cluster_sensors = 2
        if self.min_cluster_sensors > 10:
            self.min_cluster_sensors = 10
    
    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        # Handle nested TelegramConfig
        if isinstance(self.telegram, TelegramConfig):
            result["telegram"] = self.telegram.to_dict()
        return result
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AlertingConfig":
        telegram_data = data.get("telegram", {})
        return cls(
            enabled=data.get("enabled", False),
            webhook_url=data.get("webhook_url"),
            cooldown_seconds=data.get("cooldown_seconds", 300),
            alert_on_clusters=data.get("alert_on_clusters", True),
            alert_on_correlations=data.get("alert_on_correlations", True),
            alert_on_sensor_failure=data.get("alert_on_sensor_failure", True),
            min_cluster_sensors=data.get("min_cluster_sensors", 3),
            telegram=TelegramConfig.from_dict(telegram_data)
        )


@dataclass
class Config:
    """Root configuration object for Matrix Watcher.
    
    Attributes:
        sensors: Per-sensor configurations
        storage: Storage settings
        analysis: Analysis parameters
        alerting: Alerting settings
        api_keys: API keys for external services
    """
    sensors: dict[str, SensorConfig] = field(default_factory=dict)
    storage: StorageConfig = field(default_factory=StorageConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    alerting: AlertingConfig = field(default_factory=AlertingConfig)
    api_keys: dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize default sensor configs if not provided."""
        default_sensors = {
            "system": SensorConfig(enabled=True, interval_seconds=1.0, priority="high"),
            "time_drift": SensorConfig(enabled=True, interval_seconds=2.0, priority="high"),
            "network": SensorConfig(enabled=True, interval_seconds=5.0, priority="medium"),
            "random": SensorConfig(enabled=True, interval_seconds=5.0, priority="medium"),
            "crypto": SensorConfig(enabled=True, interval_seconds=2.0, priority="high"),
            "blockchain": SensorConfig(enabled=True, interval_seconds=10.0, priority="low"),
            "weather": SensorConfig(enabled=True, interval_seconds=300.0, priority="low"),
            "news": SensorConfig(enabled=True, interval_seconds=900.0, priority="low"),
        }
        
        # Merge with defaults
        for name, default_config in default_sensors.items():
            if name not in self.sensors:
                self.sensors[name] = default_config
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "sensors": {name: cfg.to_dict() for name, cfg in self.sensors.items()},
            "storage": self.storage.to_dict(),
            "analysis": self.analysis.to_dict(),
            "alerting": self.alerting.to_dict(),
            "api_keys": self.api_keys
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        """Create Config from dictionary."""
        sensors = {}
        for name, sensor_data in data.get("sensors", {}).items():
            sensors[name] = SensorConfig.from_dict(sensor_data)
        
        return cls(
            sensors=sensors,
            storage=StorageConfig.from_dict(data.get("storage", {})),
            analysis=AnalysisConfig.from_dict(data.get("analysis", {})),
            alerting=AlertingConfig.from_dict(data.get("alerting", {})),
            api_keys=data.get("api_keys", {})
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> "Config":
        """Create Config from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def default(cls) -> "Config":
        """Create default configuration."""
        return cls(
            api_keys={
                "openweathermap": "",
                "random_org": "",
                "etherscan": "",
                "newsapi": "",
            }
        )
    
    def get_enabled_sensors(self) -> list[str]:
        """Get list of enabled sensor names."""
        return [name for name, cfg in self.sensors.items() if cfg.enabled]
