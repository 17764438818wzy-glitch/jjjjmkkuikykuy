import time
from collections import Counter
from threading import Event, Lock

import pytest

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_contracts import spec


@pytest.mark.parametrize('poll_limit', [1, 6])
def test_active_polls_diagnostics_and_uncertain_writes_all_make_progress(tmp_path, poll_limit):
    queue = QueueStore(tmp_path / 'queue.sqlite3')
    calls = Counter()
    lock = Lock()
    active_ids = []
    for number in range(8):
        active_ids.append(queue.enqueue(
            spec(source_id=f'active-{number}', remote_name=f'active-{number}.md'),
            {'status': 'running', 'remote_id': f'active-{number}'}))
    pending = queue.enqueue(spec(source_id='diagnostic', remote_name='diagnostic.md'),
                            {'status': 'outcome_unresolved', 'remote_id': 'diagnostic',
                             'failure_kind': 'diagnostic_pending'})
    uncertain = queue.enqueue(spec(source_id='uncertain', remote_name='uncertain.md'))
    queue.transition(uncertain, 'upload_waiting', 'parse_intent',
                     remote_id='uncertain', lease_until=0)

    class Gateway:
        def lookup(self, task):
            with lock:
                calls[task['id']] += 1
            return [{'id': task['remote_id'], 'name': task['remote_name'],
                     'pipeline_id': task['pipeline_id'],
                     'run': ('FAIL' if task['id'] == pending else
                             'UNSTART' if task['id'] == uncertain else 'RUNNING')}]

        def parse(self, task):
            pytest.fail('An uncertain write must never be resubmitted')

    def diagnostic(task, remote):
        return {'source': 'execution_node', 'component_id': 'CodeExec:EducationGate',
                'kind': 'quality_decision', 'outcome': 'quality_hold',
                'reason': 'Missing implementation evidence', 'log_id': 'current-log',
                'remote_id': remote['id'], 'pipeline_id': task['pipeline_id'],
                'attempt': task['execution_attempt']}

    pool = WorkPool(queue, Gateway, lambda *_: 'outcome_unresolved',
                    diagnostics_reader=diagnostic, poll_limit=poll_limit, parse_limit=8)
    try:
        # Bounded public ticks must reach every active task and both maintenance
        # categories, even when active papers outnumber available read workers.
        for _ in range(40):
            pool.tick()
            if (all(calls[task_id] for task_id in active_ids)
                    and queue.get(pending)['status'] == 'quality_hold'
                    and queue.get(uncertain)['status'] == 'parse_unknown'):
                break
            time.sleep(.01)
        assert all(calls[task_id] for task_id in active_ids)
        assert queue.get(pending)['status'] == 'quality_hold'
        assert queue.get(uncertain)['status'] == 'parse_unknown'
        assert all(queue.get(task_id)['status'] == 'running' for task_id in active_ids)
    finally:
        pool.close()


def test_blocked_reads_do_not_queue_more_than_shared_read_budget(tmp_path):
    queue = QueueStore(tmp_path / 'queue.sqlite3')
    for number in range(8):
        queue.enqueue(spec(source_id=f'p{number}', remote_name=f'p{number}.md'),
                      {'status': 'running', 'remote_id': f'p{number}'})
    release = Event()
    entered = Event()
    calls = []
    lock = Lock()

    class Gateway:
        def lookup(self, task):
            with lock:
                calls.append(task['id'])
                if len(calls) == 3:
                    entered.set()
            assert release.wait(5)
            return [{'id': task['remote_id'], 'run': 'RUNNING'}]

    pool = WorkPool(queue, Gateway, lambda *_: 'accepted', poll_limit=3, parse_limit=8)
    try:
        pool.tick()
        assert entered.wait(3)
        for _ in range(5):
            pool.tick()
    finally:
        release.set()
        pool.close()
    # close() drains submitted futures, so this also detects work queued behind
    # the three blocked requests, not merely the number of simultaneous threads.
    assert len(calls) == 3
