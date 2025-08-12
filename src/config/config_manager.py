"""Configuration manager for Matrix Watcher.

Handles loading, validation, saving, and hot-reloading of configuration.
"""

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .schema import Config, SensorConfig, StorageConfig, AnalysisConfig, AlertingConfig

logger = logging.getLogger(__name__)


class ConfigValidationError:
    """Represents a configuration validation error."""
    
    def __init__(self, path: str, message: str, value: Any = None):
        self.path = path
        self.message = message
        self.value = value
    
    def __str__(self) -> str:
        if self.value is not None:
            return f"{self.path}: {self.message} (got: {self.value})"
        return f"{self.path}: {self.message}"


class ConfigManager:
    """Manages configuration loading, validation, and hot-reload.
    
    Attributes:
        config_path: Path to the configuration file
        config: Current configuration object
        _lock: Thread lock for safe access
        _last_modified: Last modification time of config file
        _reload_callbacks: Callbacks to invoke on config reload
    """
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize ConfigManager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = Path(config_path)
        self.config: Config = Config.default()
        self._lock = threading.RLock()
        self._last_modified: float = 0
        self._reload_callbacks: list[Callable[[Config], None]] = []
        self._validation_errors: list[ConfigValidationError] = []
    
    def load(self, path: str | None = None) -> Config:
        """Load configuration from file.
        
        Args:
            path: Optional path override
            
        Returns:
            Loaded configuration object
        """
        config_path = Path(path) if path else self.config_path
        
        with self._lock:
            if not config_path.exists():
                logger.warning(f"Config file not found: {config_path}, creating default")
                self.save_default(str(config_path))
                return self.config
            
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                self._validation_errors = self.validate(data)
                if self._validation_errors:
                    for error in self._validation_errors:
                        logger.warning(f"Config validation: {error}")
                
                self.config = Config.from_dict(data)
                self._last_modified = config_path.stat().st_mtime
                logger.info(f"Configuration loaded from {config_path}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in config file: {e}")
                self._validation_errors = [
                    ConfigValidationError("", f"Invalid JSON: {e}")
                ]
                # Keep existing config or use default
                if self.config is None:
                    self.config = Config.default()
            except Exception as e:
                logger.error(f"Error loading config: {e}")
                self._validation_errors = [
                    ConfigValidationError("", f"Load error: {e}")
                ]
                if self.config is None:
                    self.config = Config.default()
        
        return self.config
    
    def validate(self, data: dict[str, Any]) -> list[ConfigValidationError]:
        """Validate configuration data.
        
        Args:
            data: Configuration dictionary to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors: list[ConfigValidationError] = []
        
        # Validate sensors section
        if "sensors" in data:
            if not isinstance(data["sensors"], dict):
                errors.append(ConfigValidationError(
                    "sensors", "must be a dictionary", type(data["sensors"]).__name__
                ))
            else:
                for name, sensor_data in data["sensors"].items():
                    errors.extend(self._validate_sensor_config(f"sensors.{name}", sensor_data))
        
        # Validate storage section
        if "storage" in data:
            errors.extend(self._validate_storage_config("storage", data["storage"]))
        
        # Validate analysis section
        if "analysis" in data:
            errors.extend(self._validate_analysis_config("analysis", data["analysis"]))
        
        # Validate alerting section
        if "alerting" in data:
            errors.extend(self._validate_alerting_config("alerting", data["alerting"]))
        
        # Validate api_keys section
        if "api_keys" in data:
            if not isinstance(data["api_keys"], dict):
                errors.append(ConfigValidationError(
                    "api_keys", "must be a dictionary", type(data["api_keys"]).__name__
                ))
        
        return errors
    
    def _validate_sensor_config(self, path: str, data: Any) -> list[ConfigValidationError]:
        """Validate sensor configuration."""
        errors: list[ConfigValidationError] = []
        
        if not isinstance(data, dict):
            errors.append(ConfigValidationError(path, "must be a dictionary", type(data).__name__))
            return errors
        
        if "enabled" in data and not isinstance(data["enabled"], bool):
            errors.append(ConfigValidationError(
                f"{path}.enabled", "must be boolean", data["enabled"]
            ))
        
        if "interval_seconds" in data:
            val = data["interval_seconds"]
            if not isinstance(val, (int, float)):
                errors.append(ConfigValidationError(
                    f"{path}.interval_seconds", "must be a number", val
                ))
            elif val < 0.1 or val > 3600:
                errors.append(ConfigValidationError(
                    f"{path}.interval_seconds", "must be between 0.1 and 3600", val
                ))
        
        if "priority" in data:
            val = data["priority"]
            if val not in ("high", "medium", "low"):
                errors.append(ConfigValidationError(
                    f"{path}.priority", "must be 'high', 'medium', or 'low'", val
                ))
        
        return errors
    
    def _validate_storage_config(self, path: str, data: Any) -> list[ConfigValidationError]:
        """Validate storage configuration."""
        errors: list[ConfigValidationError] = []
        
        if not isinstance(data, dict):
            errors.append(ConfigValidationError(path, "must be a dictionary", type(data).__name__))
            return errors
        
        if "max_file_size_mb" in data:
            val = data["max_file_size_mb"]
            if not isinstance(val, int) or val < 1 or val > 10000:
                errors.append(ConfigValidationError(
                    f"{path}.max_file_size_mb", "must be integer between 1 and 10000", val
                ))
        
        if "buffer_size" in data:
            val = data["buffer_size"]
            if not isinstance(val, int) or val < 1 or val > 100000:
                errors.append(ConfigValidationError(
                    f"{path}.buffer_size", "must be integer between 1 and 100000", val
                ))
        
        return errors
    
    def _validate_analysis_config(self, path: str, data: Any) -> list[ConfigValidationError]:
        """Validate analysis configuration."""
        errors: list[ConfigValidationError] = []
        
        if not isinstance(data, dict):
            errors.append(ConfigValidationError(path, "must be a dictionary", type(data).__name__))
            return errors
        
        if "window_size" in data:
            val = data["window_size"]
            if not isinstance(val, int) or val < 10 or val > 10000:
                errors.append(ConfigValidationError(
                    f"{path}.window_size", "must be integer between 10 and 10000", val
                ))
        
        if "z_score_threshold" in data:
            val = data["z_score_threshold"]
            if not isinstance(val, (int, float)) or val < 1.0 or val > 10.0:
                errors.append(ConfigValidationError(
                    f"{path}.z_score_threshold", "must be number between 1.0 and 10.0", val
                ))
        
        return errors
    
    def _validate_alerting_config(self, path: str, data: Any) -> list[ConfigValidationError]:
        """Validate alerting configuration."""
        errors: list[ConfigValidationError] = []
        
        if not isinstance(data, dict):
            errors.append(ConfigValidationError(path, "must be a dictionary", type(data).__name__))
            return errors
        
        if "cooldown_seconds" in data:
            val = data["cooldown_seconds"]
            if not isinstance(val, int) or val < 0 or val > 86400:
                errors.append(ConfigValidationError(
                    f"{path}.cooldown_seconds", "must be integer between 0 and 86400", val
                ))
        
        if "min_cluster_sensors" in data:
            val = data["min_cluster_sensors"]
            if not isinstance(val, int) or val < 2 or val > 10:
                errors.append(ConfigValidationError(
                    f"{path}.min_cluster_sensors", "must be integer between 2 and 10", val
                ))
        
        return errors
    
    def save_default(self, path: str | None = None) -> None:
        """Save default configuration to file.
        
        Args:
            path: Optional path override
        """
        config_path = Path(path) if path else self.config_path
        
        with self._lock:
            self.config = Config.default()
            
            # Ensure directory exists
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(self.config.to_json(indent=2))
            
            self._last_modified = config_path.stat().st_mtime
            logger.info(f"Default configuration saved to {config_path}")
    
    def reload(self) -> Config:
        """Reload configuration from file.
        
        Returns:
            Reloaded configuration object
        """
        old_config = self.config
        new_config = self.load()
        
        # Notify callbacks if config changed
        if old_config != new_config:
            for callback in self._reload_callbacks:
                try:
                    callback(new_config)
                except Exception as e:
                    logger.error(f"Error in reload callback: {e}")
        
        return new_config
    
    def check_for_changes(self) -> bool:
        """Check if config file has been modified.
        
        Returns:
            True if file was modified since last load
        """
        if not self.config_path.exists():
            return False
        
        current_mtime = self.config_path.stat().st_mtime
        return current_mtime > self._last_modified
    
    def on_reload(self, callback: Callable[[Config], None]) -> None:
        """Register a callback for configuration reload.
        
        Args:
            callback: Function to call with new config on reload
        """
        self._reload_callbacks.append(callback)
    
    def get_validation_errors(self) -> list[ConfigValidationError]:
        """Get validation errors from last load.
        
        Returns:
            List of validation errors
        """
        return self._validation_errors.copy()
    
    def get_sensor_config(self, sensor_name: str) -> SensorConfig | None:
        """Get configuration for a specific sensor.
        
        Args:
            sensor_name: Name of the sensor
            
        Returns:
            SensorConfig or None if not found
        """
        with self._lock:
            return self.config.sensors.get(sensor_name)
    
    def update_sensor_config(self, sensor_name: str, **kwargs) -> None:
        """Update configuration for a specific sensor.
        
        Args:
            sensor_name: Name of the sensor
            **kwargs: Configuration values to update
        """
        with self._lock:
            if sensor_name in self.config.sensors:
                sensor_cfg = self.config.sensors[sensor_name]
                for key, value in kwargs.items():
                    if hasattr(sensor_cfg, key):
                        setattr(sensor_cfg, key, value)
