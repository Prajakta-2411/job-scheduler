import pytest

from scheduler import (
    CycleError, Job, JobScheduler, JobStatus, MissingDependencyError,
)


def make(**kw):
    return JobScheduler(sleep=lambda _: None, **kw)   # no real waiting in tests


def ok():
    return "done"


def test_dependencies_run_before_dependents():
    s = make()
    s.add_job(Job("c", ok, dependencies=["b"]))
    s.add_job(Job("b", ok, dependencies=["a"]))
    s.add_job(Job("a", ok))
    s.run()
    assert s.execution_order == ["a", "b", "c"]


def test_higher_priority_runs_first_among_ready_jobs():
    s = make()
    s.add_job(Job("low", ok, priority=1))
    s.add_job(Job("high", ok, priority=9))
    s.add_job(Job("mid", ok, priority=5))
    s.run()
    assert s.execution_order == ["high", "mid", "low"]


def test_equal_priority_is_first_come_first_served():
    s = make()
    for name in ["x", "y", "z"]:
        s.add_job(Job(name, ok, priority=1))
    s.run()
    assert s.execution_order == ["x", "y", "z"]


def test_dependency_beats_priority():
    s = make()
    s.add_job(Job("important", ok, priority=100, dependencies=["boring"]))
    s.add_job(Job("boring", ok, priority=0))
    s.run()
    assert s.execution_order == ["boring", "important"]


def test_cycle_is_detected():
    s = make()
    s.add_job(Job("a", ok, dependencies=["b"]))
    s.add_job(Job("b", ok, dependencies=["a"]))
    with pytest.raises(CycleError):
        s.run()


def test_self_dependency_is_a_cycle():
    s = make()
    s.add_job(Job("a", ok, dependencies=["a"]))
    with pytest.raises(CycleError):
        s.validate()


def test_missing_dependency_is_reported():
    s = make()
    s.add_job(Job("a", ok, dependencies=["ghost"]))
    with pytest.raises(MissingDependencyError):
        s.run()


def test_duplicate_job_id_rejected():
    s = make()
    s.add_job(Job("a", ok))
    with pytest.raises(ValueError):
        s.add_job(Job("a", ok))


def test_retry_then_success():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("boom")
        return "fine"

    s = make()
    s.add_job(Job("flaky", flaky, max_retries=2))
    result = s.run()
    job = s.get_job("flaky")
    assert result["flaky"] == JobStatus.SUCCESS
    assert job.attempts == 3 and job.result == "fine" and job.error is None


def test_failure_after_retries_skips_downstream():
    def bad():
        raise RuntimeError("nope")

    s = make()
    s.add_job(Job("bad", bad, max_retries=1))
    s.add_job(Job("child", ok, dependencies=["bad"]))
    s.add_job(Job("grandchild", ok, dependencies=["child"]))
    s.add_job(Job("independent", ok))
    result = s.run()
    assert result["bad"] == JobStatus.FAILED
    assert s.get_job("bad").attempts == 2
    assert result["child"] == JobStatus.SKIPPED
    assert result["grandchild"] == JobStatus.SKIPPED
    assert result["independent"] == JobStatus.SUCCESS      # unrelated work still runs


def test_diamond_graph_runs_join_once_after_both_parents():
    s = make()
    s.add_job(Job("a", ok))
    s.add_job(Job("b", ok, dependencies=["a"]))
    s.add_job(Job("c", ok, dependencies=["a"]))
    s.add_job(Job("d", ok, dependencies=["b", "c"]))
    s.run()
    assert s.execution_order[0] == "a"
    assert s.execution_order[-1] == "d"
    assert s.execution_order.count("d") == 1


def test_backoff_delays_double():
    delays = []
    s = JobScheduler(backoff_seconds=1.0, sleep=delays.append)

    def bad():
        raise RuntimeError("x")

    s.add_job(Job("bad", bad, max_retries=3))
    s.run()
    assert delays == [1.0, 2.0, 4.0]
