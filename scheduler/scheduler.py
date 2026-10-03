"""Mini job scheduler.

Core ideas:
  * Jobs form a DAG (directed acyclic graph): an edge A -> B means "B depends on A".
  * Kahn's algorithm (topological sort) tells us when a job becomes ready and
    also detects cycles.
  * A max-priority queue (heapq) picks the most important job among the READY ones.
  * Failed jobs are retried with exponential backoff; if they still fail,
    everything downstream is SKIPPED.
"""
import heapq
import itertools
import logging
import time
from collections import defaultdict, deque
from typing import Callable, Dict, List, Tuple

from .job import Job, JobStatus

logger = logging.getLogger("scheduler")


class SchedulerError(Exception):
    """Base class for scheduler errors."""


class CycleError(SchedulerError):
    """Raised when jobs depend on each other in a loop."""


class MissingDependencyError(SchedulerError):
    """Raised when a job depends on a job_id that was never added."""


class JobScheduler:
    def __init__(self, backoff_seconds: float = 0.0, sleep: Callable[[float], None] = time.sleep):
        self._jobs: Dict[str, Job] = {}
        self._backoff = backoff_seconds
        self._sleep = sleep                    # injectable so tests don't really wait
        self._counter = itertools.count()      # tie-breaker: equal priority -> first added runs first
        self.execution_order: List[str] = []

    # ---------- building the job set ----------
    def add_job(self, job: Job) -> None:
        if job.job_id in self._jobs:
            raise ValueError(f"Duplicate job id: {job.job_id}")
        self._jobs[job.job_id] = job

    def _build_graph(self) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
        """Return (dependents, in_degree).

        dependents[A] = jobs that wait on A
        in_degree[B]  = number of unfinished dependencies of B
        """
        dependents: Dict[str, List[str]] = defaultdict(list)
        in_degree: Dict[str, int] = {}
        for job in self._jobs.values():
            deps = set(job.dependencies)           # set() ignores duplicate deps
            in_degree[job.job_id] = len(deps)
            for dep in deps:
                if dep not in self._jobs:
                    raise MissingDependencyError(
                        f"Job '{job.job_id}' depends on unknown job '{dep}'"
                    )
                dependents[dep].append(job.job_id)
        return dependents, in_degree

    def validate(self) -> None:
        """Detect missing dependencies and cycles using Kahn's algorithm."""
        dependents, in_degree = self._build_graph()
        queue = deque(j for j, d in in_degree.items() if d == 0)
        visited = 0
        while queue:
            node = queue.popleft()
            visited += 1
            for child in dependents[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
        if visited != len(self._jobs):
            stuck = sorted(j for j, d in in_degree.items() if d > 0)
            raise CycleError(f"Cycle detected involving jobs: {stuck}")

    # ---------- running ----------
    def _push_ready(self, heap: list, job: Job) -> None:
        # heapq is a MIN-heap, so negate priority to get highest priority first
        heapq.heappush(heap, (-job.priority, next(self._counter), job.job_id))

    def run(self) -> Dict[str, JobStatus]:
        self.validate()
        dependents, in_degree = self._build_graph()

        ready: list = []
        for job_id, degree in in_degree.items():
            if degree == 0:
                self._push_ready(ready, self._jobs[job_id])

        while ready:
            _, _, job_id = heapq.heappop(ready)
            job = self._jobs[job_id]
            self.execution_order.append(job_id)

            if self._execute(job):
                for child_id in dependents[job_id]:
                    in_degree[child_id] -= 1
                    if in_degree[child_id] == 0:
                        self._push_ready(ready, self._jobs[child_id])
            else:
                self._skip_descendants(job_id, dependents)

        return {job_id: job.status for job_id, job in self._jobs.items()}

    def _execute(self, job: Job) -> bool:
        """Run one job with retries. Returns True on success."""
        job.status = JobStatus.RUNNING
        total_attempts = job.max_retries + 1
        for attempt in range(1, total_attempts + 1):
            job.attempts = attempt
            logger.info("Starting job '%s' (attempt %d/%d)", job.job_id, attempt, total_attempts)
            try:
                job.result = job.func()
                job.error = None
                job.status = JobStatus.SUCCESS
                logger.info("Job '%s' succeeded", job.job_id)
                return True
            except Exception as exc:  # a job can fail with anything; scheduler must survive
                job.error = f"{type(exc).__name__}: {exc}"
                logger.warning("Job '%s' failed on attempt %d: %s", job.job_id, attempt, job.error)
                if attempt < total_attempts:
                    delay = self._backoff * (2 ** (attempt - 1))   # exponential backoff
                    self._sleep(delay)
        job.status = JobStatus.FAILED
        logger.error("Job '%s' FAILED after %d attempts", job.job_id, total_attempts)
        return False

    def _skip_descendants(self, failed_id: str, dependents: Dict[str, List[str]]) -> None:
        """BFS over everything downstream of a failed job and mark it SKIPPED."""
        queue = deque(dependents[failed_id])
        while queue:
            child_id = queue.popleft()
            child = self._jobs[child_id]
            if child.status == JobStatus.SKIPPED:
                continue                               # already handled (diamond-shaped graphs)
            child.status = JobStatus.SKIPPED
            child.error = f"Skipped because dependency '{failed_id}' failed"
            logger.warning("Job '%s' skipped (upstream '%s' failed)", child_id, failed_id)
            queue.extend(dependents[child_id])

    def get_job(self, job_id: str) -> Job:
        return self._jobs[job_id]
