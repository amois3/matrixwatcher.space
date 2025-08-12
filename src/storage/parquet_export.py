"""Parquet export functionality for Matrix Watcher.

Exports JSONL data to Parquet format for efficient offline analysis.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

logger = logging.getLogger(__name__)


class ParquetExporter:
    """Export sensor data to Parquet format.
    
    Parquet provides:
    - Columnar storage for efficient analytics
    - Compression for reduced file size
    - Fast read performance for large datasets
    
    Example:
        exporter = ParquetExporter()
        exporter.export(
            records=storage.read("system", start, end),
            output_path="exports/system_2024.parquet"
        )
    """
    
    def __init__(self, compression: str = "snappy"):
        """Initialize Parquet exporter.
        
        Args:
            compression: Compression algorithm (snappy, gzip, brotli, none)
        """
        self.compression = compression if compression != "none" else None
    
    def export(
        self,
        records: Iterator[dict[str, Any]],
        output_path: str,
        chunk_size: int = 10000
    ) -> int:
        """Export records to Parquet file.
        
        Args:
            records: Iterator of records to export
            output_path: Path for output Parquet file
            chunk_size: Number of records to process at a time
            
        Returns:
            Number of records exported
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        total_records = 0
        chunks = []
        current_chunk = []
        
        for record in records:
            current_chunk.append(record)
            
            if len(current_chunk) >= chunk_size:
                chunks.append(pd.DataFrame(current_chunk))
                total_records += len(current_chunk)
                current_chunk = []
        
        # Handle remaining records
        if current_chunk:
            chunks.append(pd.DataFrame(current_chunk))
            total_records += len(current_chunk)
        
        if not chunks:
            logger.warning(f"No records to export to {output_path}")
            return 0
        
        # Concatenate all chunks
        df = pd.concat(chunks, ignore_index=True)
        
        # Write to Parquet
        df.to_parquet(
            output_path,
            compression=self.compression,
            index=False
        )
        
        logger.info(f"Exported {total_records} records to {output_path}")
        return total_records
    
    def export_from_storage(
        self,
        storage,  # JSONLStorage
        sensor_name: str,
        start_date: date,
        end_date: date,
        output_path: str
    ) -> int:
        """Export data from storage to Parquet.
        
        Args:
            storage: JSONLStorage instance
            sensor_name: Name of sensor to export
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            output_path: Path for output Parquet file
            
        Returns:
            Number of records exported
        """
        records = storage.read(sensor_name, start_date, end_date)
        return self.export(records, output_path)
    
    @staticmethod
    def read_parquet(file_path: str) -> pd.DataFrame:
        """Read Parquet file into DataFrame.
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            DataFrame with records
        """
        return pd.read_parquet(file_path)
    
    @staticmethod
    def parquet_to_records(file_path: str) -> Iterator[dict[str, Any]]:
        """Read Parquet file and yield records.
        
        Args:
            file_path: Path to Parquet file
            
        Yields:
            Records as dictionaries
        """
        df = pd.read_parquet(file_path)
        for _, row in df.iterrows():
            yield row.to_dict()


def export_to_parquet(
    records: Iterator[dict[str, Any]],
    output_path: str,
    compression: str = "snappy"
) -> int:
    """Convenience function to export records to Parquet.
    
    Args:
        records: Iterator of records
        output_path: Output file path
        compression: Compression algorithm
        
    Returns:
        Number of records exported
    """
    exporter = ParquetExporter(compression=compression)
    return exporter.export(records, output_path)


def import_from_parquet(file_path: str) -> list[dict[str, Any]]:
    """Convenience function to import records from Parquet.
    
    Args:
        file_path: Path to Parquet file
        
    Returns:
        List of records
    """
    return list(ParquetExporter.parquet_to_records(file_path))
