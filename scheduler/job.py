"""Job model: what a unit of work looks like."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional


class JobStatus(Enum):
    PENDING = "PENDING"    # waiting for dependencies or its turn
    RUNNING = "RUNNING"    # currently executing
    SUCCESS = "SUCCESS"    # finished without error
    FAILED = "FAILED"      # failed after all retries
    SKIPPED = "SKIPPED"    # never ran because a dependency failed


@dataclass
class Job:
    job_id: str
    func: Callable[[], Any]                       # the work to run
    priority: int = 0                             # higher number = runs earlier
    dependencies: List[str] = field(default_factory=list)  # job_ids that must succeed first
    max_retries: int = 0                          # extra attempts after the first failure

    # runtime state (filled in by the scheduler)
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    result: Any = None
    error: Optional[str] = None
