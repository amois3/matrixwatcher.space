"""Time utility functions for Matrix Watcher."""

import time
from datetime import datetime


def get_current_timestamp() -> float:
    """Get current Unix timestamp."""
    return time.time()


def format_timestamp(ts: float, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format Unix timestamp to string."""
    return datetime.fromtimestamp(ts).strftime(fmt)


def timestamp_to_datetime(ts: float) -> datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.fromtimestamp(ts)


def datetime_to_timestamp(dt: datetime) -> float:
    """Convert datetime to Unix timestamp."""
    return dt.timestamp()
