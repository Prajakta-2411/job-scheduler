# Interview Guide: Mini Job Scheduler

## 1. The 30-second pitch (memorise the idea, not the words)
"I built a small job scheduler in Python to learn how platforms like Spark
infrastructure run work reliably. Jobs have priorities and dependencies. I model
dependencies as a directed graph, use topological sort to find which jobs are
ready and to detect cycles, and a heap to pick the highest-priority ready job.
It retries transient failures with exponential backoff, and if a job still fails
it skips only the jobs downstream of it. It has unit tests and logging. It's a
learning project, not production scale."

## 2. Why this project connects to the Microsoft role
The team runs infrastructure that executes huge numbers of Spark jobs. Any such
system must answer: what runs first, what must wait, what happens on failure, how
do we debug it. This project is a tiny version of those four questions.
Say clearly that it is a simplified learning model. Never claim it resembles
Spark's real scheduler.

## 3. The three core concepts

### a) Graph / DAG
Each job is a node; "B depends on A" is an edge A -> B. It must be a DAG
(directed acyclic graph): if A needs B and B needs A, nothing can ever start.

### b) Topological sort (Kahn's algorithm)
1. Compute in-degree = number of unfinished dependencies for each job.
2. Jobs with in-degree 0 are ready.
3. Run one; for each job that depends on it, subtract 1 from its in-degree.
   If it hits 0, that job is now ready.
4. If the number of jobs processed is less than the total, a cycle exists.
Time O(V + E).

### c) Heap / priority queue
Among the ready jobs we want the highest priority. Python's heapq is a MIN-heap,
so we push (-priority, counter, job_id). Push and pop are O(log n).
The counter breaks ties so equal-priority jobs run in the order they were added
(and avoids comparing job objects, which would raise an error).

## 4. Walkthrough of the code
- job.py: Job dataclass + JobStatus enum (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED).
- scheduler.py:
  - add_job: stores jobs in a dict (O(1) lookup), rejects duplicate ids.
  - _build_graph: builds `dependents` (who waits on me) and `in_degree`
    (how many I wait on). Raises MissingDependencyError for unknown ids.
  - validate: Kahn's algorithm only to count; if visited != total -> CycleError.
  - run: validates, seeds the heap with in-degree-0 jobs, loops pop -> execute ->
    on success release dependents, on failure skip descendants.
  - _execute: attempts = max_retries + 1; catches Exception; sleeps
    backoff * 2^(attempt-1) between attempts (1s, 2s, 4s...).
  - _skip_descendants: BFS from the failed job marking everything downstream SKIPPED.
- tests/: 12 tests covering ordering, priorities, ties, cycles, self-dependency,
  missing deps, retries, skipping, diamond graph, backoff delays.

## 5. Design decisions (interviewers love "why")
- **Why a dict for jobs?** O(1) lookup by id.
- **Why validate before running?** Fail fast; better to reject a bad graph than
  run half of it.
- **Why exponential backoff?** If a service is overloaded, hammering it every
  second makes things worse. Doubling the wait gives it time to recover. Real
  systems also add random jitter so many clients don't retry at the same instant.
- **Why skip downstream jobs instead of stopping everything?** Failure isolation:
  unrelated work still completes.
- **Why inject `sleep`?** So tests run instantly and I can assert the exact delays.
  (Dependency injection for testability.)
- **Why catch broad `Exception`?** A scheduler must survive any user job crashing.
  I deliberately don't catch BaseException (KeyboardInterrupt should still stop it).

## 6. Complexity cheat sheet
- Build graph: O(V + E)
- Cycle check: O(V + E)
- Whole run: O(V + E + V log V) plus job execution time
- Space: O(V + E)

## 7. Questions you should expect (and answer outline)

1. **How do you detect a cycle?** Kahn's: if not all nodes get processed, the rest
   are in or behind a cycle. Alternative: DFS with a "currently visiting" colour.
2. **Why not just sort by priority?** A high-priority job may depend on a
   low-priority one; dependencies must win. The heap only holds *ready* jobs.
3. **What if two jobs have the same priority?** Counter tie-breaker, FIFO.
4. **Can a low-priority job starve?** Yes, if high-priority jobs keep arriving.
   Fix: priority aging (raise priority the longer a job waits).
5. **How would you run jobs in parallel?** Use a worker pool (ThreadPoolExecutor
   or processes). Independent ready jobs can run concurrently; protect shared
   state (in-degree map, heap) with a lock or let one coordinator thread own it.
6. **What if the scheduler crashes midway?** Currently state is in memory, so
   everything is lost. Fix: persist job status (database or file), and make jobs
   idempotent so re-running is safe.
7. **What is idempotency and why does it matter with retries?** Running a job twice
   gives the same result as once. Retries can cause duplicates otherwise
   (e.g. charging a customer twice).
8. **Which errors should be retried?** Transient ones (network timeouts, throttling).
   Not permanent ones (bad input, bug); retrying those just wastes time.
9. **How would you add timeouts?** Run the job in a worker and wait with a timeout;
   treat a timeout as a failure. Python threads cannot be force-killed, so
   processes are safer for hard timeouts.
10. **How would you test it?** Unit tests as in the repo, plus failure injection
    (jobs that fail N times), plus larger random DAGs to check ordering invariants.
11. **How would you monitor it in production?** Metrics: queue length, job latency,
    failure and retry rates; logs with job ids; alerts when failures spike.
12. **How is this different from real Spark/YARN/Kubernetes schedulers?** Those
    handle resources (CPU/memory), distributed workers, node failures, and
    fairness across users. Mine only handles ordering and retry logic in one process.
    (Being honest about this scores well.)
13. **What was the hardest part?** Be genuine. Common real answers: handling the
    diamond-shaped graph when skipping descendants, or realising a high-priority
    job still has to wait for its dependencies.

## 8. Live-coding warm-ups (practice these yourself)
1. Write `topological_sort(graph)` from scratch using Kahn's algorithm.
2. Detect a cycle in a directed graph with DFS.
3. Find the top K most frequent items with a heap.
4. Explain why `heapq` needs `(-priority, counter, item)` tuples.

## 9. Exercises to make the project truly yours (do at least 3)
1. Add a `timeout` field to Job.
2. Add jitter to the backoff (random 0-20%).
3. Print the execution plan (order) without running it: `dry_run()`.
4. Run independent ready jobs in parallel with ThreadPoolExecutor.
5. Save final statuses to a JSON file after run().
6. Add priority aging to avoid starvation.
7. Write 3 new tests for each feature you add.

Commit each exercise separately so your GitHub history shows real progress.

## 10. Resume line (use once you can explain everything above)
> Built a job scheduler in Python using a graph (topological sort) and priority
> queue (heap) to run prioritized, dependent jobs with cycle detection,
> exponential-backoff retries, failure isolation, logging and 12 unit tests.
