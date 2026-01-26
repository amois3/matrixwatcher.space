"""Data Export module for Matrix Watcher.

Provides CSV export and data replay functionality.
"""

import asyncio
import csv
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Iterator

import pandas as pd

from .jsonl_storage import JSONLStorage

logger = logging.getLogger(__name__)


def _parse_date(date_str: str | None, default: date) -> date:
    """Parse date string or return default."""
    if date_str is None:
        return default
    return datetime.strptime(date_str, "%Y-%m-%d").date()


class DataExporter:
    """Export data to various formats.
    
    Supports:
    - CSV export with date range filtering
    - Configurable column selection
    - Streaming export for large datasets
    
    Example:
        exporter = DataExporter(storage)
        exporter.export_csv("system", "output.csv", start_date="2024-01-01")
    """
    
    def __init__(self, storage: JSONLStorage):
        """Initialize exporter.
        
        Args:
            storage: JSONL storage instance
        """
        self.storage = storage
    
    def export_csv(
        self,
        sensor_name: str,
        output_path: str | Path,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None
    ) -> int:
        """Export sensor data to CSV.
        
        Args:
            sensor_name: Sensor name to export
            output_path: Output CSV file path
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            columns: Columns to include (None = all)
            
        Returns:
            Number of records exported
        """
        # Parse dates with defaults
        start = _parse_date(start_date, date(2020, 1, 1))
        end = _parse_date(end_date, date.today() + timedelta(days=1))
        
        # Read records (generator to list)
        records = list(self.storage.read(sensor_name, start, end))
        
        if not records:
            logger.warning(f"No records found for {sensor_name}")
            return 0
        
        # Filter columns if specified
        if columns:
            records = [{k: r.get(k) for k in columns if k in r} for r in records]
        
        # Get all unique keys
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        
        # Sort keys with timestamp first
        sorted_keys = sorted(all_keys)
        if "timestamp" in sorted_keys:
            sorted_keys.remove("timestamp")
            sorted_keys = ["timestamp"] + sorted_keys
        
        # Write CSV
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)
        
        logger.info(f"Exported {len(records)} records to {output_path}")
        return len(records)
    
    def export_all_sensors_csv(
        self,
        output_dir: str | Path,
        start_date: str | None = None,
        end_date: str | None = None
    ) -> dict[str, int]:
        """Export all sensors to separate CSV files.
        
        Args:
            output_dir: Output directory
            start_date: Start date filter
            end_date: End date filter
            
        Returns:
            Dictionary mapping sensor name to record count
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results = {}
        
        # Get all sensor directories
        logs_dir = self.storage.base_path
        if not logs_dir.exists():
            return results
        
        for sensor_dir in logs_dir.iterdir():
            if sensor_dir.is_dir():
                sensor_name = sensor_dir.name
                output_path = output_dir / f"{sensor_name}.csv"
                count = self.export_csv(
                    sensor_name, output_path, start_date, end_date
                )
                results[sensor_name] = count
        
        return results
    
    def export_merged_csv(
        self,
        sensor_names: list[str],
        output_path: str | Path,
        start_date: str | None = None,
        end_date: str | None = None,
        resample_interval: str | None = None
    ) -> int:
        """Export multiple sensors merged by timestamp.
        
        Args:
            sensor_names: List of sensors to merge
            output_path: Output CSV file path
            start_date: Start date filter
            end_date: End date filter
            resample_interval: Pandas resample interval (e.g., '1s', '5s')
            
        Returns:
            Number of records exported
        """
        start = _parse_date(start_date, date(2020, 1, 1))
        end = _parse_date(end_date, date.today() + timedelta(days=1))
        
        dfs = []
        
        for sensor_name in sensor_names:
            records = list(self.storage.read(sensor_name, start, end))
            if records:
                df = pd.DataFrame(records)
                # Prefix columns with sensor name
                df = df.rename(columns={
                    c: f"{sensor_name}_{c}" if c != "timestamp" else c
                    for c in df.columns
                })
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')
                df = df.set_index("timestamp")
                dfs.append(df)
        
        if not dfs:
            return 0
        
        # Merge on timestamp
        merged = pd.concat(dfs, axis=1)
        
        # Resample if requested
        if resample_interval:
            merged = merged.resample(resample_interval).mean()
        
        # Export
        merged.to_csv(output_path)
        logger.info(f"Exported merged data to {output_path}")
        
        return len(merged)


class DataReplayer:
    """Replay historical data through analyzers.
    
    Feeds historical data at configurable speed for testing
    and analysis of past events.
    
    Example:
        replayer = DataReplayer(storage)
        async for record in replayer.replay("system", speed=10.0):
            process(record)
    """
    
    def __init__(self, storage: JSONLStorage):
        """Initialize replayer.
        
        Args:
            storage: JSONL storage instance
        """
        self.storage = storage
        self._running = False
        self._paused = False
    
    async def replay(
        self,
        sensor_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
        speed: float = 1.0,
        callback: Callable[[dict[str, Any]], None] | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        """Replay sensor data asynchronously.
        
        Args:
            sensor_name: Sensor to replay
            start_date: Start date filter
            end_date: End date filter
            speed: Playback speed (1.0 = real-time, 10.0 = 10x faster)
            callback: Optional callback for each record
            
        Yields:
            Records in chronological order with timing
        """
        start = _parse_date(start_date, date(2020, 1, 1))
        end = _parse_date(end_date, date.today() + timedelta(days=1))
        
        records = list(self.storage.read(sensor_name, start, end))
        
        if not records:
            logger.warning(f"No records to replay for {sensor_name}")
            return
        
        # Sort by timestamp
        records.sort(key=lambda r: r.get("timestamp", 0))
        
        self._running = True
        last_timestamp = None
        
        for record in records:
            if not self._running:
                break
            
            while self._paused:
                await asyncio.sleep(0.1)
            
            timestamp = record.get("timestamp", 0)
            
            # Calculate delay
            if last_timestamp is not None and speed > 0:
                delay = (timestamp - last_timestamp) / speed
                if delay > 0:
                    await asyncio.sleep(delay)
            
            last_timestamp = timestamp
            
            if callback:
                callback(record)
            
            yield record
        
        self._running = False
    
    async def replay_all_sensors(
        self,
        sensor_names: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        speed: float = 1.0,
        callback: Callable[[str, dict[str, Any]], None] | None = None
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        """Replay multiple sensors merged by timestamp.
        
        Args:
            sensor_names: Sensors to replay
            start_date: Start date filter
            end_date: End date filter
            speed: Playback speed
            callback: Callback receiving (sensor_name, record)
            
        Yields:
            Tuples of (sensor_name, record) in chronological order
        """
        start = _parse_date(start_date, date(2020, 1, 1))
        end = _parse_date(end_date, date.today() + timedelta(days=1))
        
        # Collect all records with sensor name
        all_records = []
        
        for sensor_name in sensor_names:
            records = list(self.storage.read(sensor_name, start, end))
            for record in records:
                all_records.append((sensor_name, record))
        
        if not all_records:
            return
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x[1].get("timestamp", 0))
        
        self._running = True
        last_timestamp = None
        
        for sensor_name, record in all_records:
            if not self._running:
                break
            
            while self._paused:
                await asyncio.sleep(0.1)
            
            timestamp = record.get("timestamp", 0)
            
            if last_timestamp is not None and speed > 0:
                delay = (timestamp - last_timestamp) / speed
                if delay > 0:
                    await asyncio.sleep(delay)
            
            last_timestamp = timestamp
            
            if callback:
                callback(sensor_name, record)
            
            yield sensor_name, record
        
        self._running = False
    
    def pause(self) -> None:
        """Pause replay."""
        self._paused = True
    
    def resume(self) -> None:
        """Resume replay."""
        self._paused = False
    
    def stop(self) -> None:
        """Stop replay."""
        self._running = False
    
    @property
    def is_running(self) -> bool:
        """Check if replay is running."""
        return self._running
    
    @property
    def is_paused(self) -> bool:
        """Check if replay is paused."""
        return self._paused
