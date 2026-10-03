# Mini Job Scheduler (Python)

A small learning project that runs jobs in the right order, the way a data/compute
platform schedules work. It is **not** production-scale; it is built to demonstrate
core data structures and reliability ideas.

## Features
- Jobs with **priorities** and **dependencies** (job B runs only after job A succeeds)
- **Cycle detection** and missing-dependency detection (topological sort, Kahn's algorithm)
- **Priority queue (heap)** to choose the next job among those ready to run
- **Retries with exponential backoff** for transient failures
- **Failure isolation**: if a job fails, only its downstream jobs are skipped
- **Structured logging** of every start, retry, failure and skip
- **12 unit tests** (pytest)

## Run
```bash
pip install pytest
python main.py          # demo pipeline
python -m pytest -q     # tests
```

## Example
```python
from scheduler import Job, JobScheduler

s = JobScheduler(backoff_seconds=0.5)
s.add_job(Job("extract", lambda: "data", priority=5))
s.add_job(Job("transform", lambda: "clean", dependencies=["extract"], max_retries=2))
s.add_job(Job("load", lambda: "done", dependencies=["transform"]))
print(s.run())
```

## Complexity
V = jobs, E = dependencies. Validation and ordering: O(V + E). Heap operations add
O(V log V). Space: O(V + E).

## Limitations / future work
Runs jobs sequentially (no parallel workers), keeps state in memory only, no
timeouts or persistence. Possible next steps: ThreadPoolExecutor workers, job
timeouts, saving state to disk, a priority-aging rule to prevent starvation.
