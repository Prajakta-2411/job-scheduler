"""Demo: a tiny data pipeline."""
import logging

from scheduler import Job, JobScheduler

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

flaky_calls = {"count": 0}


def flaky_job():
    flaky_calls["count"] += 1
    if flaky_calls["count"] < 3:
        raise ConnectionError("temporary network error")
    return "ok after retries"


def always_fails():
    raise RuntimeError("bad input data")


def main():
    s = JobScheduler(backoff_seconds=0.1)
    s.add_job(Job("extract_sales", lambda: "sales rows", priority=5))
    s.add_job(Job("extract_users", flaky_job, priority=5, max_retries=3))
    s.add_job(Job("transform", lambda: "joined", priority=10,
                  dependencies=["extract_sales", "extract_users"]))
    s.add_job(Job("load_warehouse", lambda: "loaded", priority=10, dependencies=["transform"]))
    s.add_job(Job("send_report", lambda: "sent", priority=1, dependencies=["load_warehouse"]))
    s.add_job(Job("cleanup_temp", lambda: "cleaned", priority=1))
    s.add_job(Job("validate_schema", always_fails, priority=3, max_retries=1))
    s.add_job(Job("publish_schema", lambda: "published", dependencies=["validate_schema"]))

    results = s.run()
    print("\nExecution order:", s.execution_order)
    print("\nFinal status:")
    for job_id, status in results.items():
        print(f"  {job_id:16} {status.value}")


if __name__ == "__main__":
    main()
