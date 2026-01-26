"""Storage Manager for Matrix Watcher.

Orchestrates data storage with buffering, retry logic, and DataFrame export.
"""

import logging
import threading
import time
from collections import deque
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .base import StorageBackend, StorageError
from .jsonl_storage import JSONLStorage
from .parquet_export import ParquetExporter
from ..core.types import Event, SensorReading

logger = logging.getLogger(__name__)


class StorageManager:
    """Orchestrates data storage operations.
    
    Features:
    - Buffering for batch writes
    - Retry logic on failures
    - DataFrame export for analysis
    - Parquet export support
    - Thread-safe operations
    
    Example:
        manager = StorageManager(base_path="logs")
        manager.write_event(event)
        
        df = manager.read_records("system", start_date, end_date)
        manager.export_parquet("system", start_date, end_date, "export.parquet")
    """
    
    def __init__(
        self,
        base_path: str = "logs",
        compression: bool = False,
        max_file_size_mb: int = 100,
        buffer_size: int = 1000,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """Initialize StorageManager.
        
        Args:
            base_path: Root directory for log files
            compression: Whether to use gzip compression
            max_file_size_mb: Maximum file size before rotation
            buffer_size: Number of records to buffer before flush
            max_retries: Maximum retry attempts on failure
            retry_delay: Delay between retries in seconds
        """
        self.storage = JSONLStorage(
            base_path=base_path,
            compression=compression,
            max_file_size_mb=max_file_size_mb
        )
        self.parquet_exporter = ParquetExporter()
        
        self.buffer_size = buffer_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self._buffers: dict[str, deque] = {}
        self._lock = threading.RLock()
        self._stats = {
            "writes": 0,
            "buffered": 0,
            "flushed": 0,
            "retries": 0,
            "failures": 0
        }
    
    def write_record(self, sensor_name: str, record: dict[str, Any]) -> None:
        """Write a single record with buffering.
        
        Args:
            sensor_name: Name of the sensor
            record: Data record to store
        """
        with self._lock:
            if sensor_name not in self._buffers:
                self._buffers[sensor_name] = deque()
            
            self._buffers[sensor_name].append(record)
            self._stats["buffered"] += 1
            
            # Flush if buffer is full
            if len(self._buffers[sensor_name]) >= self.buffer_size:
                self._flush_buffer(sensor_name)
    
    def write_event(self, event: Event) -> None:
        """Write an Event to storage.
        
        Args:
            event: Event to store
        """
        record = event.to_dict()
        self.write_record(event.source, record)
    
    def write_reading(self, reading: SensorReading) -> None:
        """Write a SensorReading to storage.
        
        Args:
            reading: SensorReading to store
        """
        record = reading.to_dict()
        self.write_record(reading.source, record)
    
    def write_anomaly(self, anomaly_record: dict[str, Any]) -> None:
        """Write an anomaly record to the anomalies log.
        
        Args:
            anomaly_record: Anomaly data to store
        """
        self.write_record("anomalies", anomaly_record)
    
    def flush(self, sensor_name: str | None = None) -> int:
        """Flush buffered records to storage.
        
        Args:
            sensor_name: Specific sensor to flush (None = all)
            
        Returns:
            Number of records flushed
        """
        total_flushed = 0
        
        with self._lock:
            if sensor_name:
                if sensor_name in self._buffers:
                    total_flushed = self._flush_buffer(sensor_name)
            else:
                for name in list(self._buffers.keys()):
                    total_flushed += self._flush_buffer(name)
        
        return total_flushed
    
    def flush_all(self) -> int:
        """Flush all buffered records."""
        return self.flush()
    
    def _flush_buffer(self, sensor_name: str) -> int:
        """Flush buffer for a specific sensor with retry logic.
        
        Args:
            sensor_name: Name of the sensor
            
        Returns:
            Number of records flushed
        """
        if sensor_name not in self._buffers or not self._buffers[sensor_name]:
            return 0
        
        records = list(self._buffers[sensor_name])
        self._buffers[sensor_name].clear()
        
        for attempt in range(self.max_retries):
            try:
                written = self.storage.write_batch(sensor_name, records)
                self._stats["writes"] += written
                self._stats["flushed"] += written
                return written
            except StorageError as e:
                self._stats["retries"] += 1
                logger.warning(f"Write failed for {sensor_name} (attempt {attempt + 1}): {e}")
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    # Put records back in buffer
                    self._buffers[sensor_name].extend(records)
                    self._stats["failures"] += 1
                    logger.error(f"Failed to write {len(records)} records for {sensor_name}")
                    raise
        
        return 0
    
    def read_records(
        self,
        sensor_name: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """Read records as a pandas DataFrame.
        
        Args:
            sensor_name: Name of the sensor
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            DataFrame with records
        """
        # Flush any buffered records first
        self.flush(sensor_name)
        
        records = list(self.storage.read(sensor_name, start_date, end_date))
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # Convert timestamp to datetime if present
        if "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
        
        return df
    
    def export_parquet(
        self,
        sensor_name: str,
        start_date: date,
        end_date: date,
        output_path: str
    ) -> int:
        """Export records to Parquet format.
        
        Args:
            sensor_name: Name of the sensor
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            output_path: Path for output Parquet file
            
        Returns:
            Number of records exported
        """
        # Flush any buffered records first
        self.flush(sensor_name)
        
        return self.parquet_exporter.export_from_storage(
            self.storage,
            sensor_name,
            start_date,
            end_date,
            output_path
        )
    
    def export_csv(
        self,
        sensor_name: str,
        start_date: date,
        end_date: date,
        output_path: str
    ) -> int:
        """Export records to CSV format.
        
        Args:
            sensor_name: Name of the sensor
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            output_path: Path for output CSV file
            
        Returns:
            Number of records exported
        """
        df = self.read_records(sensor_name, start_date, end_date)
        
        if df.empty:
            return 0
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        return len(df)
    
    def get_all_sensors(self) -> list[str]:
        """Get list of all sensors with data.
        
        Returns:
            List of sensor names
        """
        base_path = Path(self.storage.base_path)
        
        if not base_path.exists():
            return []
        
        sensors = []
        for item in base_path.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                sensors.append(item.name)
        
        return sorted(sensors)
    
    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics.
        
        Returns:
            Dictionary with stats
        """
        with self._lock:
            stats = self._stats.copy()
            stats["buffer_counts"] = {
                name: len(buf) for name, buf in self._buffers.items()
            }
            stats["total_buffered"] = sum(len(buf) for buf in self._buffers.values())
        
        return stats
    
    def get_size(self, sensor_name: str) -> int:
        """Get storage size for a sensor in bytes.
        
        Args:
            sensor_name: Name of the sensor
            
        Returns:
            Size in bytes
        """
        return self.storage.get_size(sensor_name)
    
    def close(self) -> None:
        """Close the storage manager, flushing all buffers."""
        self.flush_all()
        self.storage.close()
