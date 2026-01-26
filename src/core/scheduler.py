"""Task Scheduler for Matrix Watcher.

Manages periodic execution of sensor tasks with:
- Configurable intervals (1 second to 1 hour)
- Priority-based execution (high, medium, low)
- Prevention of execution overlap
- Execution timing and drift logging
"""

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Any
from enum import Enum

from .types import Priority, TaskStats

logger = logging.getLogger(__name__)


class TaskState(str, Enum):
    """State of a scheduled task."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class ScheduledTask:
    """Represents a scheduled task.
    
    Attributes:
        name: Unique task name
        callback: Function to execute
        interval: Execution interval in seconds
        priority: Task priority
        state: Current task state
        stats: Execution statistics
        _running: Flag to prevent overlap
        _lock: Thread lock for state changes
    """
    name: str
    callback: Callable[[], Any]
    interval: float
    priority: Priority
    state: TaskState = TaskState.PENDING
    stats: TaskStats = field(default_factory=lambda: TaskStats(name=""))
    _running: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _next_run: float = 0.0
    
    def __post_init__(self):
        self.stats = TaskStats(name=self.name)
        self._next_run = time.time()


class Scheduler:
    """Task scheduler with priority-based execution.
    
    Features:
    - Configurable intervals per task
    - Priority levels (high, medium, low)
    - Prevents concurrent execution of same task
    - Tracks execution timing and drift
    
    Example:
        scheduler = Scheduler()
        
        def collect_data():
            return {"value": 42}
        
        scheduler.register_task("sensor", collect_data, interval=5.0, priority="high")
        scheduler.start()
        # ... later ...
        scheduler.stop()
    """
    
    def __init__(self, max_concurrent: int = 10):
        """Initialize Scheduler.
        
        Args:
            max_concurrent: Maximum concurrent task executions
        """
        self._tasks: dict[str, ScheduledTask] = {}
        self._lock = threading.RLock()
        self._running = False
        self._thread: threading.Thread | None = None
        self._max_concurrent = max_concurrent
        self._semaphore = threading.Semaphore(max_concurrent)
        self._stop_event = threading.Event()
    
    def register_task(
        self,
        name: str,
        callback: Callable[[], Any],
        interval: float,
        priority: str | Priority = Priority.MEDIUM
    ) -> None:
        """Register a task for periodic execution.
        
        Args:
            name: Unique task name
            callback: Function to execute
            interval: Execution interval in seconds (0.1 to 3600)
            priority: Task priority (high, medium, low)
        """
        # Validate interval
        interval = max(0.1, min(3600.0, interval))
        
        # Convert priority
        if isinstance(priority, str):
            priority = Priority(priority.lower())
        
        task = ScheduledTask(
            name=name,
            callback=callback,
            interval=interval,
            priority=priority
        )
        
        with self._lock:
            self._tasks[name] = task
            logger.info(f"Registered task: {name} (interval={interval}s, priority={priority.value})")
    
    def unregister_task(self, name: str) -> bool:
        """Unregister a task.
        
        Args:
            name: Task name
            
        Returns:
            True if task was found and removed
        """
        with self._lock:
            if name in self._tasks:
                del self._tasks[name]
                logger.info(f"Unregistered task: {name}")
                return True
        return False
    
    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return
        
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started")
    
    def stop(self, timeout: float = 5.0) -> None:
        """Stop the scheduler.
        
        Args:
            timeout: Maximum time to wait for tasks to complete
        """
        if not self._running:
            return
        
        self._running = False
        self._stop_event.set()
        
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None
        
        logger.info("Scheduler stopped")
    
    def pause_task(self, name: str) -> bool:
        """Pause a task.
        
        Args:
            name: Task name
            
        Returns:
            True if task was found and paused
        """
        with self._lock:
            if name in self._tasks:
                self._tasks[name].state = TaskState.PAUSED
                logger.info(f"Paused task: {name}")
                return True
        return False
    
    def resume_task(self, name: str) -> bool:
        """Resume a paused task.
        
        Args:
            name: Task name
            
        Returns:
            True if task was found and resumed
        """
        with self._lock:
            if name in self._tasks:
                task = self._tasks[name]
                if task.state == TaskState.PAUSED:
                    task.state = TaskState.PENDING
                    task._next_run = time.time()  # Run immediately
                    logger.info(f"Resumed task: {name}")
                    return True
        return False
    
    def get_stats(self) -> dict[str, TaskStats]:
        """Get statistics for all tasks.
        
        Returns:
            Dictionary of task name to TaskStats
        """
        with self._lock:
            return {name: task.stats for name, task in self._tasks.items()}
    
    def get_task_stats(self, name: str) -> TaskStats | None:
        """Get statistics for a specific task.
        
        Args:
            name: Task name
            
        Returns:
            TaskStats or None if not found
        """
        with self._lock:
            if name in self._tasks:
                return self._tasks[name].stats
        return None
    
    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self._running and not self._stop_event.is_set():
            now = time.time()
            
            # Get tasks ready to run, sorted by priority
            ready_tasks = self._get_ready_tasks(now)
            
            # Execute ready tasks
            for task in ready_tasks:
                if not self._running:
                    break
                self._execute_task(task)
            
            # Sleep briefly to avoid busy-waiting
            self._stop_event.wait(timeout=0.1)
    
    def _get_ready_tasks(self, now: float) -> list[ScheduledTask]:
        """Get tasks ready to run, sorted by priority.
        
        Args:
            now: Current timestamp
            
        Returns:
            List of ready tasks, highest priority first
        """
        priority_order = {Priority.HIGH: 0, Priority.MEDIUM: 1, Priority.LOW: 2}
        ready = []
        
        with self._lock:
            for task in self._tasks.values():
                if task.state == TaskState.PAUSED:
                    continue
                if task._running:
                    continue
                if task._next_run <= now:
                    ready.append(task)
        
        # Sort by priority (high first)
        ready.sort(key=lambda t: priority_order.get(t.priority, 1))
        return ready
    
    def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a task in a separate thread.
        
        Args:
            task: Task to execute
        """
        # Check if already running (prevent overlap)
        with task._lock:
            if task._running:
                logger.debug(f"Task {task.name} already running, skipping")
                return
            task._running = True
            task.state = TaskState.RUNNING
        
        # Execute in thread pool
        thread = threading.Thread(
            target=self._run_task,
            args=(task,),
            daemon=True
        )
        thread.start()
    
    def _run_task(self, task: ScheduledTask) -> None:
        """Run a task and update statistics.
        
        Args:
            task: Task to run
        """
        scheduled_time = task._next_run
        start_time = time.time()
        drift_ms = (start_time - scheduled_time) * 1000
        
        try:
            # Acquire semaphore to limit concurrency
            if not self._semaphore.acquire(timeout=1.0):
                logger.warning(f"Task {task.name} could not acquire semaphore")
                return
            
            try:
                task.callback()
                
                # Update stats on success
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                
                task.stats.run_count += 1
                task.stats.last_run = start_time
                task.stats.last_drift_ms = drift_ms
                task.stats.consecutive_failures = 0
                
                # Update average duration
                if task.stats.avg_duration_ms == 0:
                    task.stats.avg_duration_ms = duration_ms
                else:
                    task.stats.avg_duration_ms = (
                        task.stats.avg_duration_ms * 0.9 + duration_ms * 0.1
                    )
                
                if task.stats.run_count % 10 == 1:  # Log every 10th run
                    logger.info(
                        f"Task {task.name} completed: duration={duration_ms:.1f}ms, drift={drift_ms:.1f}ms, runs={task.stats.run_count}"
                    )
                
            finally:
                self._semaphore.release()
                
        except Exception as e:
            task.stats.error_count += 1
            task.stats.consecutive_failures += 1
            import traceback
            logger.error(f"Task {task.name} failed: {e}\n{traceback.format_exc()}")
        
        finally:
            # Schedule next run
            with task._lock:
                task._running = False
                task.state = TaskState.PENDING
                task._next_run = time.time() + task.interval
                task.stats.next_run = task._next_run
    
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running
    
    def get_task_count(self) -> int:
        """Get number of registered tasks."""
        with self._lock:
            return len(self._tasks)
