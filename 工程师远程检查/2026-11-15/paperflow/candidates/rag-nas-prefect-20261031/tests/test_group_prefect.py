from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import UUID

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'prefect-paperflow'))

from test_group_stage import control
from group_stage import FIELDS, prefect_task_identity, reserve_group


class Client:
    def __init__(self, lose=False):
        self.calls = 0
        self.lose = lose
    def create_task_run(self, *, task, flow_run_id, id, dynamic_key, name, state):
        self.calls += 1
        if self.lose:
            raise TimeoutError('create outcome unknown')
        return SimpleNamespace(id=id, task_key=task.task_key, dynamic_key=dynamic_key,
                               tags=task.tags, flow_run_id=flow_run_id)


def fake_task(stage, task_id):
    identity = prefect_task_identity(stage, task_id)
    return SimpleNamespace(task_key=identity['task_key'], tags={identity['tag']})


def actual(stage, task_id):
    identity = prefect_task_identity(stage, task_id)
    return SimpleNamespace(id=UUID(task_id), task_key=identity['task_key'],
                           dynamic_key=identity['dynamic_key'], tags={identity['tag']}, flow_run_id=None)


def read(database):
    with closing(sqlite3.connect(database)) as db:
        db.row_factory = sqlite3.Row
        return dict(db.execute('SELECT * FROM paperflow_rag_nas_jobs').fetchone())


def evidence(tmp_path, ticket, job):
    from paperflow_prefect.records import sha_file
    copy = tmp_path / 'copy.json'
    copy.write_text(json.dumps({'schema': 'paperflow-isolated-test-copy/1',
        **{key: ticket[key] for key in ('group_id', 'paper_id', 'receipt_id')},
        'files': [{'file_id': 'one', 'kind': 'body', 'sha256': 'a' * 64,
                   'relative': 'one.docx', 'path': str(tmp_path / 'one.docx'), 'bytes': 10}]}), encoding='utf8')
    body = tmp_path / 'body.json'
    body.write_text(json.dumps({'body_verified': True, 'binding': {'source_sha256': 'a' * 64,
                                'manifest_sha256': 'b' * 64, 'run_id': 'test-run',
                                'journal_id': 'test-journal'}}), encoding='utf8')
    rag = tmp_path / 'rag.json'
    postprocess = {key: job[key] for key in ('claim_id', 'postprocess_job_id', 'postprocess_receipt_sha256',
                                             'upstream_manifest_sha256', 'source_generation')}
    rag.write_text(json.dumps({'schema': 'paperflow-isolated-group-rag/1', 'status': 'verified', 'group_count': 1,
        'binding': {'ticket': ticket, 'copy_receipt_sha256': sha_file(copy), 'manifest_sha256': 'b' * 64,
                    'run_id': 'test-run', 'journal_id': 'test-journal',
                    'postprocess': postprocess}, 'body_receipt_path': str(body),
        'body_receipt_sha256': sha_file(body), 'attachments': []}), encoding='utf8')
    return copy, rag


def test_one_folder_transitions_from_rag_to_nas_only_after_group_receipt(control, tmp_path):
    from group_prefect import advance_stage, claim_stage, finish_rag, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    api = Client()
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    assert read(database)['state'] == 'running'
    copy, receipt = evidence(tmp_path, ticket, job)
    finish_rag(database, ticket['task_id'], ticket['group_id'], receipt, copy_receipt_path=copy)
    assert (read(database)['stage'], read(database)['state']) == ('rag', 'running')
    run = actual('rag', job['rag_task_run_id'])
    run.state = SimpleNamespace(is_completed=lambda: True)
    advance_stage(database, ticket['task_id'], ticket['group_id'], 'rag', run)
    row = read(database)
    assert (row['stage'], row['state']) == ('nas', 'waiting')
    assert row['rag_started_at_ns'] <= row['rag_completed_at_ns']
    assert api.calls == 1


def test_uncertain_prefect_submission_is_not_resent(control):
    from group_prefect import submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    api = Client(lose=True)
    task = fake_task('rag', job['rag_task_run_id'])
    with pytest.raises(TimeoutError): submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, task)
    assert read(database)['state'] == 'unknown'
    with pytest.raises(ValueError): submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, task)
    assert api.calls == 1


def test_unread_disclosure_is_recorded_on_its_folder_without_rag_completion(control):
    from contextlib import nullcontext
    from group_prefect import HandoffNeedsReview, _run_attempt, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    def operation(*_):
        raise HandoffNeedsReview('UnreadReviewRequired')
    with pytest.raises(HandoffNeedsReview):
        _run_attempt(database, ticket['task_id'], ticket['group_id'], 'rag',
                     actual('rag', job['rag_task_run_id']), operation, lambda _: nullcontext())
    row = read(database)
    assert (row['stage'], row['state'], row['error_type']) == ('rag', 'needs_review', 'UnreadReviewRequired')
    assert row['rag_receipt_path'] is None and row['nas_receipt_path'] is None


def test_paused_parent_does_not_submit_stage(control):
    from group_prefect import submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused'")
    api = Client()
    with pytest.raises(InterruptedError):
        submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, fake_task('rag', job['rag_task_run_id']))
    assert api.calls == 0


def test_late_result_cannot_revive_deleted_parent(control, tmp_path):
    from group_prefect import claim_stage, finish_rag, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('INSERT INTO task_deletions VALUES(?)', (ticket['task_id'],))
    copy, receipt = evidence(tmp_path, ticket, job)
    with pytest.raises(InterruptedError):
        finish_rag(database, ticket['task_id'], ticket['group_id'], receipt, copy_receipt_path=copy)
    assert read(database)['stage'] == 'rag'


def test_complete_stage_requires_archive_and_deleted_test_copy_evidence(control, tmp_path):
    from group_prefect import advance_stage, claim_stage, finish_nas, finish_rag, submit_stage
    from paperflow_prefect.records import sha_file
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    api = Client()
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', api, fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    copy, rag = evidence(tmp_path, ticket, job)
    finish_rag(database, ticket['task_id'], ticket['group_id'], rag, copy_receipt_path=copy)
    run = actual('rag', job['rag_task_run_id']); run.state = SimpleNamespace(is_completed=lambda: True)
    advance_stage(database, ticket['task_id'], ticket['group_id'], 'rag', run)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'nas', api, fake_task('nas', job['nas_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'nas', actual('nas', job['nas_task_run_id']))
    archive = tmp_path / 'archive.json'
    archive.write_text(json.dumps({'schema': 'paperflow-isolated-test-archive/1', 'readback_verified': True,
                                   **{key: ticket[key] for key in ('group_id', 'paper_id', 'receipt_id')}}), encoding='utf8')
    cleanup = tmp_path / 'cleanup.json'
    state = {'schema': 'paperflow-isolated-cleanup/1', 'status': 'pending', 'group_id': ticket['group_id'],
             'binding': {'copy_receipt_sha256': sha_file(copy), 'rag_receipt_sha256': sha_file(rag),
                         'archive_receipt_sha256': sha_file(archive)},
             'files': {'one': {'state': 'deleted'}}}
    cleanup.write_text(json.dumps(state), encoding='utf8')
    with pytest.raises(ValueError):
        finish_nas(database, ticket['task_id'], ticket['group_id'], archive, cleanup)
    assert read(database)['stage'] == 'nas'
    state['status'] = 'completed'
    cleanup.write_text(json.dumps(state), encoding='utf8')
    with pytest.raises(ValueError):
        finish_nas(database, ticket['task_id'], ticket['group_id'], archive, cleanup)
    archive_data = json.loads(archive.read_bytes())
    archive_data['files'] = [{'name': 'one.docx', 'sha256': 'a' * 64, 'bytes': 10}]
    archive.write_text(json.dumps(archive_data), encoding='utf8')
    state['binding']['archive_receipt_sha256'] = sha_file(archive)
    state['files']['one'].update(path=str(tmp_path / 'one.docx'), sha256='a' * 64)
    cleanup.write_text(json.dumps(state), encoding='utf8')
    finish_nas(database, ticket['task_id'], ticket['group_id'], archive, cleanup)
    assert (read(database)['stage'], read(database)['state']) == ('nas', 'running')
    run = actual('nas', job['nas_task_run_id']); run.state = SimpleNamespace(is_completed=lambda: True)
    advance_stage(database, ticket['task_id'], ticket['group_id'], 'nas', run)
    row = read(database)
    assert (row['stage'], row['state']) == ('completed', 'completed')
    assert row['rag_completed_at_ns'] <= row['nas_started_at_ns'] <= row['completed_at_ns']


def test_paused_folder_resumes_same_prefect_task_and_records_two_active_intervals(control):
    from group_prefect import claim_stage, pause_stage, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused'")
    pause_stage(database, ticket['task_id'], ticket['group_id'], 'rag')
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    assert read(database)['run_generation'] == 6
    with closing(sqlite3.connect(database)) as db:
        intervals = db.execute('SELECT started_at_ns,ended_at_ns FROM paperflow_rag_nas_intervals ORDER BY id').fetchall()
    assert len(intervals) == 2
    assert intervals[0][1] is not None and intervals[1][1] is None


def test_cannot_advance_until_the_actual_prefect_task_is_completed(control, tmp_path):
    from group_prefect import advance_stage, claim_stage, finish_rag, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    copy, receipt = evidence(tmp_path, ticket, job)
    finish_rag(database, ticket['task_id'], ticket['group_id'], receipt, copy_receipt_path=copy)
    run = actual('rag', job['rag_task_run_id']); run.state = SimpleNamespace(is_completed=lambda: False)
    with pytest.raises(ValueError, match='not completed'):
        advance_stage(database, ticket['task_id'], ticket['group_id'], 'rag', run)
    assert read(database)['stage'] == 'rag'


def test_other_postprocessing_job_cannot_complete_this_folder(control, tmp_path):
    from group_prefect import claim_stage, finish_rag, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    copy, receipt = evidence(tmp_path, ticket, job)
    value = json.loads(receipt.read_bytes())
    value['binding']['postprocess']['postprocess_job_id'] = 'another-job'
    receipt.write_text(json.dumps(value), encoding='utf8')
    with pytest.raises(ValueError, match='RAG receipt'):
        finish_rag(database, ticket['task_id'], ticket['group_id'], receipt, copy_receipt_path=copy)
    assert read(database)['stage'] == 'rag'


def test_successful_submission_after_parent_delete_is_recorded_but_not_started(control):
    from group_prefect import submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    class LateClient(Client):
        def create_task_run(self, **kwargs):
            with closing(sqlite3.connect(database)) as db, db:
                db.execute('INSERT INTO task_deletions VALUES(?)', (ticket['task_id'],))
            return super().create_task_run(**kwargs)
    with pytest.raises(InterruptedError):
        submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', LateClient(),
                     fake_task('rag', job['rag_task_run_id']))
    row = read(database)
    assert row['rag_submission'] == 'submitted'
    assert (row['stage'], row['state']) == ('rag', 'waiting')


def test_unknown_submission_can_be_claimed_only_by_its_actual_prefect_task(control):
    from group_prefect import claim_stage, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    with pytest.raises(TimeoutError):
        submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(lose=True),
                     fake_task('rag', job['rag_task_run_id']))
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    assert (read(database)['state'], read(database)['rag_submission']) == ('running', 'submitted')


def test_unknown_submission_actual_run_can_claim_after_parent_resumes(control):
    from group_prefect import claim_stage, submit_stage
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    with pytest.raises(TimeoutError):
        submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(lose=True),
                     fake_task('rag', job['rag_task_run_id']))
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
    claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']))
    assert (read(database)['state'], read(database)['run_generation']) == ('running', 6)


def test_drained_unknown_effect_is_preserved_when_parent_is_deleted(control):
    from contextlib import nullcontext
    from group_prefect import _run_attempt, submit_stage, observed_concurrency
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    def operation(_stage, _job):
        with closing(sqlite3.connect(database)) as db, db:
            db.execute('INSERT INTO task_deletions VALUES(?)', (ticket['task_id'],))
        raise TimeoutError('unknown remote outcome')
    with pytest.raises(TimeoutError):
        _run_attempt(database, ticket['task_id'], ticket['group_id'], 'rag', actual('rag', job['rag_task_run_id']),
                     operation, lambda _: nullcontext())
    assert read(database)['state'] == 'unknown'
    assert observed_concurrency(database, ticket['task_id'], 'rag')['open_intervals'] == 0


def test_pause_while_waiting_for_a_prefect_slot_keeps_the_same_task(control, tmp_path):
    import asyncio
    from contextlib import contextmanager
    from group_prefect import controlled_executor, submit_stage
    from task_effect_admission import TaskNotRunning
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    copy, receipt = evidence(tmp_path, ticket, job)
    waits = []
    @contextmanager
    def slot(_stage):
        if not waits:
            with closing(sqlite3.connect(database)) as db, db:
                db.execute("UPDATE tasks SET desired_state='paused'")
            waits.append(1)
            raise TaskNotRunning('paused before acquiring the slot')
        waits.append(1)
        yield
    def operation(_stage, _job):
        return {'rag_receipt_path': str(receipt), 'copy_receipt_path': str(copy)}
    async def run():
        execute = controlled_executor(database, ticket['task_id'], ticket['group_id'], operation, slot)
        pending = asyncio.create_task(execute('rag', actual('rag', job['rag_task_run_id'])))
        while not waits:
            await asyncio.sleep(.02)
        assert read(database)['state'] == 'waiting'
        with closing(sqlite3.connect(database)) as db, db:
            db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
        await asyncio.wait_for(pending, 5)
    asyncio.run(run())
    assert len(waits) == 2
    assert (read(database)['rag_submission'], read(database)['run_generation']) == ('finished', 6)


def test_observed_concurrency_uses_group_intervals_not_specialty_count(control):
    from group_prefect import observed_concurrency
    database, ticket, verified = control
    reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        db.executemany('INSERT INTO paperflow_rag_nas_intervals(task_id,group_id,stage,started_at_ns,ended_at_ns) '
                       'VALUES(?,?,?,?,?)', [(ticket['task_id'], 'one', 'rag', 10, 30),
                                            (ticket['task_id'], 'two', 'rag', 20, 40),
                                            (ticket['task_id'], 'three', 'rag', 40, 50)])
    result = observed_concurrency(database, ticket['task_id'], 'rag')
    assert result == {'peak': 2, 'intervals': 3, 'open_intervals': 0}
