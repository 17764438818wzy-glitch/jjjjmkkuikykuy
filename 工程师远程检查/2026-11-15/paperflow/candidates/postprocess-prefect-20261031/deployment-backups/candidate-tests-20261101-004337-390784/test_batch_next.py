import asyncio
from contextlib import nullcontext
from pathlib import Path
import sqlite3
import sys
import threading
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from batch_next import run_batch, snapshot_waiting, make_batch_worker, guard_prior_failures
from stage_runtime import _job_id, build_task
from test_stage_runtime import context


def populate(context, count):
    items = [context.identity]
    with sqlite3.connect(context.db) as db:
        db.execute('CREATE TABLE postprocessing_claims(task_id TEXT,group_id TEXT)')
        db.execute('CREATE TABLE postprocessing_results(task_id TEXT,group_id TEXT)')
        for number in range(1, count):
            item = dict(context.identity, group_id=f'{number:032x}', paper_id=f'paper-{number}',
                        receipt_id=f'{number:064x}', attempt_id=f'attempt-{number}', task_run_id=str(uuid4()))
            db.execute('INSERT INTO llm_folder_queue VALUES (?,?,?,?,?,?,?,?,?,?)',
                       tuple(item.values()) + ('c'*64, 'waiting_postprocessing', 1))
            db.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?)',
                       (item['task_id'], item['task_run_id'], 'body-' + str(number), 'raw_complete', 'body'))
            items.append(item)
    return items


def test_snapshot_excludes_unknown_previously_reserved_and_deleted(context):
    items = populate(context, 3)
    with sqlite3.connect(context.db) as db:
        db.execute("update llm_request_admissions set state='unknown' where task_run_id=?", (items[1]['task_run_id'],))
        db.execute('insert into postprocessing_results values (?,?)', (items[2]['task_id'], items[2]['group_id']))
    selected = snapshot_waiting(context.db, context.identity['task_id'], 10)
    assert selected == [(context.identity, 1)]


@pytest.mark.xfail(strict=True, reason='暂停恢复代次修正待与上游及阶段入口同步')
def test_snapshot_uses_current_task_generation_after_resume(context):
    populate(context, 1)
    with sqlite3.connect(context.db) as db:
        db.execute('update tasks set start_generation=2')
    # The frozen LLM result remains generation 1; new admission uses the
    # current task generation, while preserving the same LLM history.
    assert snapshot_waiting(context.db, context.identity['task_id'], 10) == [(context.identity, 2)]


def test_batch_worker_keys_match_only_the_chosen_jobs(monkeypatch):
    import prefect.task_worker
    class FakeWorker:
        def __init__(self, *stages, limit):
            self.task_keys = {stage.task_key for stage in stages}
            self.tasks = [SimpleNamespace(task_key='recomputed') for _ in stages]
            self.limit = limit
    monkeypatch.setattr(prefect.task_worker, 'TaskWorker', FakeWorker)
    ids = [str(uuid4()), str(uuid4())]
    stages = [build_task(lambda: None, job_id) for job_id in ids]
    worker = make_batch_worker(stages, ids)
    assert worker.limit == 2
    assert {stage.task_key for stage in worker.tasks} == worker.task_keys == {stage.task_key for stage in stages}
    with pytest.raises(ValueError): make_batch_worker(stages, ids + [ids[0]])


def test_batch_gate_rechecks_prior_failures_at_admission_boundary():
    state = {'job-1': 'submitted'}
    coordinator = SimpleNamespace(postprocess_is_open=lambda: True,
        job=lambda job_id: {'state': state[job_id]})
    worker = SimpleNamespace(finished_task_runs={'key-1': 0})
    with guard_prior_failures(coordinator, [('job-1', 'key-1')], worker):
        assert coordinator.postprocess_is_open() is True
        state['job-1'] = 'execution_unknown'
        assert coordinator.postprocess_is_open() is False
    assert coordinator.postprocess_is_open() is True


class FakeWorker:
    def __init__(self, stages):
        self.finished_task_runs = {stage.task_key: 0 for stage in stages}
    async def __aenter__(self): return self
    async def __aexit__(self, *_): return None
    async def astart(self): await asyncio.Event().wait()


def test_no_more_than_eight_keys_are_subscribed_at_once(context):
    identities = populate(context, 10)
    stage_map, worker_sizes = {}, []
    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, identity, generation):
            job_id = _job_id(identity['task_run_id'])
            row = {'job_id': job_id, 'state': 'reserved'}; stage_map[job_id] = row
            return dict(row)
        def submit(self, job_id, client, task): stage_map[job_id]['state'] = 'finished'
        def job(self, job_id): return dict(stage_map[job_id])
    def worker(stages, ids):
        worker_sizes.append(len(stages)); return FakeWorker(stages)
    result = asyncio.run(run_batch(Coordinator(), [(item, 1) for item in identities],
        client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
        worker_factory=worker, batch_size=8, poll_seconds=.01))
    assert worker_sizes == [8, 2]
    assert len(result['completed']) == 10 and result['stopped'] is False


def test_first_observed_unknown_stops_later_dispatch(context):
    identities = populate(context, 5)
    stages, dispatched = {}, []
    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, identity, generation):
            job_id = _job_id(identity['task_run_id']); stages[job_id] = {'job_id': job_id, 'state': 'reserved'}
            return dict(stages[job_id])
        def submit(self, job_id, client, task):
            dispatched.append(job_id)
            stages[job_id]['state'] = 'execution_unknown' if len(dispatched) == 2 else 'finished'
        def job(self, job_id): return dict(stages[job_id])
    result = asyncio.run(run_batch(Coordinator(), [(item, 1) for item in identities],
        client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
        worker_factory=lambda stages, ids: FakeWorker(stages), batch_size=5, poll_seconds=.01))
    assert len(dispatched) == 2
    assert result['stopped'] is True
    assert result['remaining'] == 3


def test_ambiguous_submission_is_not_resent_or_followed_by_another(context):
    identities = populate(context, 3)
    stages, submitted = {}, []
    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, identity, generation):
            job_id = _job_id(identity['task_run_id']); stages[job_id] = {'job_id': job_id, 'state': 'reserved'}
            return dict(stages[job_id])
        def submit(self, job_id, client, task):
            submitted.append(job_id); stages[job_id]['state'] = 'submission_unknown'
            raise TimeoutError('server outcome unknown')
        def job(self, job_id): return dict(stages[job_id])
    result = asyncio.run(run_batch(Coordinator(), [(item, 1) for item in identities],
        client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
        worker_factory=lambda stages, ids: FakeWorker(stages), batch_size=3, poll_seconds=.01))
    assert len(submitted) == 1
    assert result['stopped'] is True
    assert result['detail']['job_id'] == submitted[0]


def test_lost_submit_response_waits_for_any_already_running_work(context):
    identities = populate(context, 2)
    stages, submitted, finished = {}, [], threading.Event()
    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, identity, generation):
            job_id = _job_id(identity['task_run_id']); stages[job_id] = {'job_id': job_id, 'state': 'reserved'}
            return dict(stages[job_id])
        def submit(self, job_id, client, task):
            submitted.append(job_id); stages[job_id]['state'] = 'executing'
            def finish(): stages[job_id]['state'] = 'finished'; finished.set()
            threading.Timer(.08, finish).start()
            raise TimeoutError('reply lost after worker started')
        def job(self, job_id): return dict(stages[job_id])
    result = asyncio.run(run_batch(Coordinator(), [(item, 1) for item in identities],
        client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
        worker_factory=lambda stages, ids: FakeWorker(stages), batch_size=2, poll_seconds=.01))
    assert finished.is_set()
    assert len(submitted) == 1 and result['stopped'] is True


def test_worker_start_failure_stops_without_reserving_any_folder(context):
    calls = []
    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, *args): calls.append('reserve')
    class BrokenWorker(FakeWorker):
        async def astart(self): raise RuntimeError('subscription unavailable')
    result = asyncio.run(run_batch(Coordinator(), [(context.identity, 1)],
        client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
        worker_factory=lambda stages, ids: BrokenWorker(stages), batch_size=1, poll_seconds=.01))
    assert result['stopped'] is True and result['detail']['reason'] == 'worker_stopped'
    assert calls == []
