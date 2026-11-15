"""Quart responsiveness, bounded execution, cancellation and context propagation."""
from __future__ import annotations

import asyncio
import contextvars
import importlib.util
from pathlib import Path
import threading
import time

import pytest
from quart import Quart, jsonify


ROOT = Path(__file__).resolve().parents[1]


def dispatcher(workers, waiting, admission_waiting=20):
    path = ROOT / "candidate-server/paperflow_bounded_executor.py"
    assert path.is_file(), "the bounded request executor is not implemented"
    spec = importlib.util.spec_from_file_location("candidate_bounded_executor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.BoundedExecutor(workers=workers, max_waiting=waiting,
                                  max_admission_waiting=admission_waiting)


async def until(predicate, timeout=3):
    end = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= end:
            raise AssertionError("condition not reached before deadline")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("workers", [2, 4])
async def test_quart_remains_responsive_and_only_bounded_workers_execute(workers):
    runner = dispatcher(workers, waiting=0)
    gate = threading.Event()
    active, peak = 0, 0
    mutex = threading.Lock()
    app = Quart(__name__)

    def blocked_work():
        nonlocal active, peak
        with mutex:
            active += 1
            peak = max(peak, active)
        try:
            assert gate.wait(5)
            return "done"
        finally:
            with mutex:
                active -= 1

    @app.post("/write")
    async def write():
        return jsonify(status=await runner.run(blocked_work))

    @app.get("/health")
    async def health():
        return jsonify(status="ok")

    client = app.test_client()
    requests = [asyncio.create_task(client.post("/write")) for _ in range(20)]
    try:
        await until(lambda: runner.snapshot()["executing"] == workers)
        await until(lambda: runner.snapshot()["waiting_to_admit"] == 20 - workers)
        start = time.monotonic()
        response = await client.get("/health")
        assert (await response.get_json())["status"] == "ok"
        assert time.monotonic() - start < 0.5
        assert peak == workers
    finally:
        gate.set()
        await asyncio.gather(*requests, return_exceptions=True)
        await runner.aclose()
    assert runner.snapshot() == {"executing": 0, "queued": 0, "waiting_to_admit": 0}


@pytest.mark.asyncio
async def test_cancelled_request_does_not_free_running_worker_slot_early():
    runner = dispatcher(2, waiting=1)
    gate, started = threading.Event(), threading.Event()
    context = contextvars.ContextVar("request_user")
    seen = []

    def blocked():
        seen.append(context.get())
        started.set()
        assert gate.wait(5)
        return "done"

    context.set("first-user")
    first = asyncio.create_task(runner.run(blocked))
    context.set("second-user")
    second = asyncio.create_task(runner.run(blocked))
    context.set("third-user")
    third = asyncio.create_task(runner.run(blocked))
    try:
        await until(lambda: runner.snapshot()["executing"] == 2 and runner.snapshot()["queued"] == 1)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.sleep(0.05)
        assert runner.snapshot()["executing"] == 2 and runner.snapshot()["queued"] == 1
        assert set(seen) == {"first-user", "second-user"}
    finally:
        gate.set()
        await asyncio.gather(second, third, return_exceptions=True)
        await runner.aclose()
    assert set(seen) == {"first-user", "second-user", "third-user"}


@pytest.mark.asyncio
async def test_cancelling_queued_and_admission_waiters_preserves_accurate_counts():
    runner = dispatcher(2, waiting=1)
    gate = threading.Event()

    def blocked():
        assert gate.wait(5)

    jobs = [asyncio.create_task(runner.run(blocked)) for _ in range(4)]
    try:
        await until(lambda: runner.snapshot() == {"executing": 2, "queued": 1, "waiting_to_admit": 1})
        jobs[2].cancel()
        jobs[3].cancel()
        await asyncio.gather(jobs[2], jobs[3], return_exceptions=True)
        await until(lambda: runner.snapshot() == {"executing": 2, "queued": 0, "waiting_to_admit": 0})
    finally:
        gate.set()
        await asyncio.gather(*jobs, return_exceptions=True)
        await runner.aclose()


@pytest.mark.asyncio
async def test_admission_backlog_is_bounded_and_rejects_before_work():
    runner = dispatcher(2, waiting=1, admission_waiting=2)
    gate = threading.Event()
    entered = []

    def blocked(value):
        entered.append(value)
        assert gate.wait(5)

    jobs = [asyncio.create_task(runner.run(blocked, i)) for i in range(5)]
    try:
        await until(lambda: runner.snapshot() == {"executing": 2, "queued": 1, "waiting_to_admit": 2})
        with pytest.raises(RuntimeError, match="admission"):
            await runner.run(blocked, "overflow")
        assert "overflow" not in entered
        assert runner.snapshot() == {"executing": 2, "queued": 1, "waiting_to_admit": 2}
        jobs[4].cancel()
        await asyncio.gather(jobs[4], return_exceptions=True)
        await until(lambda: runner.snapshot()["waiting_to_admit"] == 1)
        jobs.append(asyncio.create_task(runner.run(blocked, "replacement")))
        await until(lambda: runner.snapshot()["waiting_to_admit"] == 2)
        assert "replacement" not in entered
    finally:
        gate.set()
        outcomes = await asyncio.gather(*jobs, return_exceptions=True)
        await runner.aclose()
    assert "replacement" in entered
    assert all(not isinstance(result, BaseException) for i, result in enumerate(outcomes) if i != 4)


@pytest.mark.asyncio
async def test_zero_admission_wait_allows_free_workers_and_rejects_overflow():
    runner = dispatcher(2, waiting=0, admission_waiting=0)
    gate = threading.Event()
    started = []

    def blocked(value):
        started.append(value)
        assert gate.wait(5)

    jobs = [asyncio.create_task(runner.run(blocked, i)) for i in range(2)]
    try:
        await until(lambda: runner.snapshot()["executing"] == 2)
        with pytest.raises(RuntimeError, match="admission"):
            await runner.run(blocked, "overflow")
        assert len(started) == 2
    finally:
        gate.set()
        await asyncio.gather(*jobs, return_exceptions=True)
        await runner.aclose()
