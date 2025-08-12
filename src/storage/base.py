"""Abstract storage backend interface for Matrix Watcher.

Defines the contract for all storage implementations (JSONL, MongoDB, etc.)
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Iterator


class StorageBackend(ABC):
    """Abstract base class for storage backends.
    
    All storage implementations must implement these methods to ensure
    consistent behavior across different storage types.
    """
    
    @abstractmethod
    def write(self, sensor_name: str, record: dict[str, Any]) -> None:
        """Write a single record to storage.
        
        Args:
            sensor_name: Name of the sensor (determines storage location)
            record: Data record to store (must include 'timestamp' and 'source')
            
        Raises:
            StorageError: If write fails
        """
        pass
    
    @abstractmethod
    def write_batch(self, sensor_name: str, records: list[dict[str, Any]]) -> int:
        """Write multiple records to storage.
        
        Args:
            sensor_name: Name of the sensor
            records: List of data records to store
            
        Returns:
            Number of records successfully written
            
        Raises:
            StorageError: If write fails
        """
        pass
    
    @abstractmethod
    def read(
        self,
        sensor_name: str,
        start_date: date,
        end_date: date
    ) -> Iterator[dict[str, Any]]:
        """Read records from storage within date range.
        
        Args:
            sensor_name: Name of the sensor
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Yields:
            Data records matching the criteria
            
        Raises:
            StorageError: If read fails
        """
        pass
    
    @abstractmethod
    def get_size(self, sensor_name: str) -> int:
        """Get total storage size for a sensor in bytes.
        
        Args:
            sensor_name: Name of the sensor
            
        Returns:
            Size in bytes
        """
        pass
    
    @abstractmethod
    def get_record_count(self, sensor_name: str, target_date: date | None = None) -> int:
        """Get number of records for a sensor.
        
        Args:
            sensor_name: Name of the sensor
            target_date: Optional specific date (None = all dates)
            
        Returns:
            Number of records
        """
        pass
    
    @abstractmethod
    def delete(self, sensor_name: str, before_date: date) -> int:
        """Delete records before a specific date.
        
        Args:
            sensor_name: Name of the sensor
            before_date: Delete records before this date
            
        Returns:
            Number of records deleted
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the storage backend and release resources."""
        pass


class StorageError(Exception):
    """Exception raised for storage-related errors."""
    
    def __init__(self, message: str, sensor_name: str | None = None, cause: Exception | None = None):
        self.sensor_name = sensor_name
        self.cause = cause
        super().__init__(message)
