"""JSONL (JSON Lines) storage backend for Matrix Watcher.

Stores sensor data in JSONL format with:
- File path: logs/{sensor_name}/{YYYY-MM-DD}.jsonl
- Automatic file rotation at 100MB
- Optional gzip compression
- Pretty-printer for human-readable output
- Record validation on read
"""

import gzip
import json
import logging
import os
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

from .base import StorageBackend, StorageError

logger = logging.getLogger(__name__)


class JSONLStorage(StorageBackend):
    """JSONL file-based storage backend.
    
    Features:
    - Stores data in logs/{sensor_name}/{YYYY-MM-DD}.jsonl
    - Automatic file rotation when exceeding max_file_size_mb
    - Optional gzip compression
    - Thread-safe writes
    - Pretty-printer for human-readable output
    
    Example:
        storage = JSONLStorage(base_path="logs", compression=False)
        storage.write("system", {"timestamp": 1234567890.0, "source": "system", "cpu": 50.0})
        
        for record in storage.read("system", date(2024, 1, 1), date(2024, 1, 31)):
            print(record)
    """
    
    def __init__(
        self,
        base_path: str = "logs",
        compression: bool = False,
        max_file_size_mb: int = 100
    ):
        """Initialize JSONL storage.
        
        Args:
            base_path: Root directory for log files
            compression: Whether to use gzip compression
            max_file_size_mb: Maximum file size before rotation
        """
        self.base_path = Path(base_path)
        self.compression = compression
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()
        self._file_counters: dict[str, int] = {}  # For rotation
        
        # Ensure base directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _get_lock(self, sensor_name: str) -> threading.Lock:
        """Get or create a lock for a sensor."""
        with self._global_lock:
            if sensor_name not in self._locks:
                self._locks[sensor_name] = threading.Lock()
            return self._locks[sensor_name]
    
    def _get_file_path(self, sensor_name: str, target_date: date, rotation_index: int = 0) -> Path:
        """Generate file path for a sensor and date.
        
        Args:
            sensor_name: Name of the sensor
            target_date: Date for the file
            rotation_index: Rotation index (0 = first file, 1+ = rotated files)
            
        Returns:
            Path to the JSONL file
        """
        sensor_dir = self.base_path / sensor_name
        sensor_dir.mkdir(parents=True, exist_ok=True)
        
        date_str = target_date.strftime("%Y-%m-%d")
        
        if rotation_index == 0:
            filename = f"{date_str}.jsonl"
        else:
            filename = f"{date_str}.{rotation_index}.jsonl"
        
        if self.compression:
            filename += ".gz"
        
        return sensor_dir / filename
    
    def _get_current_file_path(self, sensor_name: str) -> Path:
        """Get the current file path for writing, handling rotation."""
        today = date.today()
        key = f"{sensor_name}:{today}"
        
        # Get current rotation index
        rotation_index = self._file_counters.get(key, 0)
        file_path = self._get_file_path(sensor_name, today, rotation_index)
        
        # Check if rotation is needed
        if file_path.exists() and file_path.stat().st_size >= self.max_file_size_bytes:
            rotation_index += 1
            self._file_counters[key] = rotation_index
            file_path = self._get_file_path(sensor_name, today, rotation_index)
            logger.info(f"Rotated log file for {sensor_name}: {file_path}")
        
        return file_path
    
    def write(self, sensor_name: str, record: dict[str, Any]) -> None:
        """Write a single record to storage."""
        self._validate_record(record)
        
        lock = self._get_lock(sensor_name)
        with lock:
            file_path = self._get_current_file_path(sensor_name)
            
            try:
                line = json.dumps(record, ensure_ascii=False) + "\n"
                
                if self.compression:
                    with gzip.open(file_path, "at", encoding="utf-8") as f:
                        f.write(line)
                else:
                    with open(file_path, "a", encoding="utf-8") as f:
                        f.write(line)
                        
            except Exception as e:
                raise StorageError(f"Failed to write record: {e}", sensor_name, e)
    
    def write_batch(self, sensor_name: str, records: list[dict[str, Any]]) -> int:
        """Write multiple records to storage."""
        if not records:
            return 0
        
        for record in records:
            self._validate_record(record)
        
        lock = self._get_lock(sensor_name)
        written = 0
        
        with lock:
            file_path = self._get_current_file_path(sensor_name)
            
            try:
                lines = [json.dumps(r, ensure_ascii=False) + "\n" for r in records]
                content = "".join(lines)
                
                if self.compression:
                    with gzip.open(file_path, "at", encoding="utf-8") as f:
                        f.write(content)
                else:
                    with open(file_path, "a", encoding="utf-8") as f:
                        f.write(content)
                
                written = len(records)
                
            except Exception as e:
                raise StorageError(f"Failed to write batch: {e}", sensor_name, e)
        
        return written
    
    def read(
        self,
        sensor_name: str,
        start_date: date,
        end_date: date
    ) -> Iterator[dict[str, Any]]:
        """Read records from storage within date range."""
        sensor_dir = self.base_path / sensor_name
        
        if not sensor_dir.exists():
            return
        
        # Iterate through dates
        current_date = start_date
        while current_date <= end_date:
            # Find all files for this date (including rotated ones)
            date_str = current_date.strftime("%Y-%m-%d")
            pattern = f"{date_str}*.jsonl*"
            
            for file_path in sorted(sensor_dir.glob(pattern)):
                yield from self._read_file(file_path)
            
            current_date = date(
                current_date.year,
                current_date.month,
                current_date.day + 1
            ) if current_date.day < 28 else self._next_date(current_date)
    
    def _next_date(self, d: date) -> date:
        """Get the next date, handling month/year boundaries."""
        try:
            return date(d.year, d.month, d.day + 1)
        except ValueError:
            # End of month
            if d.month == 12:
                return date(d.year + 1, 1, 1)
            else:
                return date(d.year, d.month + 1, 1)
    
    def _read_file(self, file_path: Path) -> Iterator[dict[str, Any]]:
        """Read records from a single file."""
        try:
            if file_path.suffix == ".gz" or str(file_path).endswith(".jsonl.gz"):
                opener = lambda: gzip.open(file_path, "rt", encoding="utf-8")
            else:
                opener = lambda: open(file_path, "r", encoding="utf-8")
            
            with opener() as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        if self._is_valid_record(record):
                            yield record
                        else:
                            logger.warning(f"Invalid record at {file_path}:{line_num}")
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON parse error at {file_path}:{line_num}: {e}")
                        
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
    
    def get_size(self, sensor_name: str) -> int:
        """Get total storage size for a sensor in bytes."""
        sensor_dir = self.base_path / sensor_name
        
        if not sensor_dir.exists():
            return 0
        
        total_size = 0
        for file_path in sensor_dir.glob("*.jsonl*"):
            total_size += file_path.stat().st_size
        
        return total_size
    
    def get_record_count(self, sensor_name: str, target_date: date | None = None) -> int:
        """Get number of records for a sensor."""
        sensor_dir = self.base_path / sensor_name
        
        if not sensor_dir.exists():
            return 0
        
        count = 0
        
        if target_date:
            date_str = target_date.strftime("%Y-%m-%d")
            pattern = f"{date_str}*.jsonl*"
        else:
            pattern = "*.jsonl*"
        
        for file_path in sensor_dir.glob(pattern):
            count += self._count_lines(file_path)
        
        return count
    
    def _count_lines(self, file_path: Path) -> int:
        """Count lines in a file."""
        try:
            if file_path.suffix == ".gz" or str(file_path).endswith(".jsonl.gz"):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    return sum(1 for line in f if line.strip())
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    return sum(1 for line in f if line.strip())
        except Exception:
            return 0
    
    def delete(self, sensor_name: str, before_date: date) -> int:
        """Delete records before a specific date."""
        sensor_dir = self.base_path / sensor_name
        
        if not sensor_dir.exists():
            return 0
        
        deleted = 0
        
        for file_path in sensor_dir.glob("*.jsonl*"):
            # Extract date from filename
            try:
                date_str = file_path.stem.split(".")[0]
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                if file_date < before_date:
                    count = self._count_lines(file_path)
                    file_path.unlink()
                    deleted += count
                    logger.info(f"Deleted {file_path} ({count} records)")
            except (ValueError, IndexError):
                continue
        
        return deleted
    
    def close(self) -> None:
        """Close the storage backend."""
        # No persistent connections to close for file-based storage
        pass
    
    def _validate_record(self, record: dict[str, Any]) -> None:
        """Validate that a record has required fields."""
        if "timestamp" not in record:
            raise StorageError("Record missing 'timestamp' field")
        if "source" not in record:
            raise StorageError("Record missing 'source' field")
    
    def _is_valid_record(self, record: dict[str, Any]) -> bool:
        """Check if a record is valid."""
        return isinstance(record, dict) and "timestamp" in record and "source" in record
    
    @staticmethod
    def pretty_print(record: dict[str, Any], indent: int = 2) -> str:
        """Format a record for human-readable output.
        
        Args:
            record: Record to format
            indent: Indentation level
            
        Returns:
            Formatted string
        """
        # Format timestamp
        if "timestamp" in record:
            ts = record["timestamp"]
            dt = datetime.fromtimestamp(ts)
            formatted = record.copy()
            formatted["_datetime"] = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            formatted = record
        
        return json.dumps(formatted, indent=indent, ensure_ascii=False)
    
    def get_available_dates(self, sensor_name: str) -> list[date]:
        """Get list of dates with data for a sensor.
        
        Args:
            sensor_name: Name of the sensor
            
        Returns:
            Sorted list of dates
        """
        sensor_dir = self.base_path / sensor_name
        
        if not sensor_dir.exists():
            return []
        
        dates = set()
        for file_path in sensor_dir.glob("*.jsonl*"):
            try:
                date_str = file_path.stem.split(".")[0]
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                dates.add(file_date)
            except (ValueError, IndexError):
                continue
        
        return sorted(dates)
