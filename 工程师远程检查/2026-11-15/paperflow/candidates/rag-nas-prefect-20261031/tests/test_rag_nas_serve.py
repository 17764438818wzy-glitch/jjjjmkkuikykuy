import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from test_group_stage import control
from group_stage import reserve_group


def add_group(db, group):
    for table in ('postprocessing_claims', 'paperflow_postprocess_stage_jobs', 'postprocessing_results'):
        columns = [row[1] for row in db.execute('PRAGMA table_info(' + table + ')')]
        copied = list(db.execute('SELECT * FROM ' + table).fetchone())
        copied[columns.index('group_id')] = group
        db.execute('INSERT INTO ' + table + ' VALUES(' + ','.join('?' for _ in copied) + ')', copied)


def test_rag_snapshot_waits_for_both_postprocessing_records(control):
    from rag_nas_serve import snapshot
    database, ticket, _ = control
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='executing'")
        db.execute("UPDATE postprocessing_results SET state='processing'")
    assert snapshot(database, ticket['task_id'])['rag'] == []
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='finished'")
    assert snapshot(database, ticket['task_id'])['rag'] == []
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE postprocessing_results SET state='passed'")
    assert snapshot(database, ticket['task_id'])['rag'] == [ticket['group_id']]


def test_nas_can_continue_with_rag_barrier_closed_but_parent_pause_stops_admission(control):
    from rag_nas_serve import snapshot
    database, ticket, verified = control
    reserve_group(database, verified, 5)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE paperflow_rag_nas_jobs SET stage='nas',state='waiting'")
        db.execute("UPDATE paperflow_queue_barriers SET blocked=1 WHERE name='rag_upload'")
    state = snapshot(database, ticket['task_id'])
    assert state['rag'] == []
    assert [row['group_id'] for row in state['nas']] == [ticket['group_id']]
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused'")
    assert snapshot(database, ticket['task_id'])['nas'] == []


def test_unread_group_does_not_block_another_folder(control):
    from rag_nas_serve import Bridge
    database, ticket, _ = control
    other = 'b' * 32
    with closing(sqlite3.connect(database)) as db, db:
        add_group(db, other)
    attempts, started, logs = [], [], []
    def prepare(group, generation):
        attempts.append(group)
        if group == ticket['group_id']:
            raise ValueError('unread disclosure requires review')
        return {'task_id': ticket['task_id'], 'group_id': group}, {}
    async def execute(job, stage, context):
        started.append((job['group_id'], stage))
        await asyncio.Event().wait()
    bridge = Bridge(database, ticket['task_id'], prepare, execute, lambda job: {},
                    lambda event, **fields: logs.append((event, fields)))

    async def run():
        for _ in range(15):
            await bridge.tick()
            if started and any(fields.get('group_id') == ticket['group_id'] for _, fields in logs):
                break
            if bridge.pending():
                await asyncio.wait(bridge.pending(), timeout=1, return_when=asyncio.FIRST_COMPLETED)
        assert set(attempts) == {ticket['group_id'], other}
        assert started == [(other, 'rag')]
        assert any(fields.get('group_id') == ticket['group_id'] for _, fields in logs)
        for task in bridge.pending():
            task.cancel()
        await asyncio.gather(*bridge.pending(), return_exceptions=True)

    asyncio.run(run())


def test_different_folders_prepare_concurrently(control):
    from rag_nas_serve import Bridge
    database, ticket, _ = control
    other = 'b' * 32
    with closing(sqlite3.connect(database)) as db, db:
        add_group(db, other)
    started = []
    async def run():
        release, finish = threading.Event(), asyncio.Event()
        def prepare(group, generation):
            started.append(group)
            assert release.wait(5)
            return {'task_id': ticket['task_id'], 'group_id': group}, {}
        async def execute(job, stage, context):
            await finish.wait()
        bridge = Bridge(database, ticket['task_id'], prepare, execute, lambda _: {}, lambda *_a, **_k: None)
        try:
            await bridge.tick()
            for _ in range(100):
                if len(started) == 2:
                    break
                await asyncio.sleep(.01)
            assert set(started) == {ticket['group_id'], other}
        finally:
            release.set()
        await asyncio.gather(*bridge.pending())
        await bridge.tick()
        finish.set()
        await asyncio.gather(*bridge.pending(), return_exceptions=True)

    asyncio.run(run())


def test_bridge_allows_twenty_rag_groups_at_a_time(control):
    from rag_nas_serve import Bridge
    database, ticket, _ = control
    with closing(sqlite3.connect(database)) as db, db:
        for index in range(20):
            add_group(db, f'{index:032x}')
    started = []
    async def run():
        finish = asyncio.Event()
        def prepare(group, generation):
            return {'task_id': ticket['task_id'], 'group_id': group}, {}
        async def execute(job, stage, context):
            started.append(job['group_id'])
            await finish.wait()
        bridge = Bridge(database, ticket['task_id'], prepare, execute, lambda _: {}, lambda *_a, **_k: None)
        for _ in range(100):
            await bridge.tick()
            if len(started) == 20:
                break
            await asyncio.sleep(.01)
        assert len(started) == 20
        assert len(bridge.pending()) == 20
        finish.set()
        await asyncio.gather(*bridge.pending(), return_exceptions=True)
    asyncio.run(run())


def test_completed_isolated_groups_attach_to_original_tickets_without_resending(control, monkeypatch):
    from prefect.testing.utilities import prefect_test_harness
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] /
                           'candidates/postprocess-prefect-20261031/server_release_next'))
    from read_only_rpc import UnreadReviewRequired
    import isolated_group_prepare
    from group_stage import FIELDS
    from rag_nas_serve import build

    database, _, _ = control
    batch = Path(__file__).resolve().parents[1] / 'live-tests/batch-309-fc240b92da874cca9a05f9630aeb073e'
    groups = ['723f1c8cac67472ff77fa7f27e8caa6d', '4ede358f59b08e033e7983f11aca1ff7']
    maps = {}
    for group in groups:
        rag = json.loads((batch / ('prefect-single-' + group) / group / 'rag/rag-group.json').read_text(encoding='utf8'))
        ticket, source = rag['binding']['ticket'], rag['binding']['postprocess']
        maps[group] = {'identity': ticket, 'job': {'job_id': source['postprocess_job_id'],
                       'claim_id': source['claim_id'], 'generation': source['source_generation'],
                       'manifest_sha256': source['upstream_manifest_sha256']},
                       'receipt_canonical_sha256': source['postprocess_receipt_sha256']}
    task_id = maps[groups[0]]['identity']['task_id']
    unread = '0' * 32
    template = maps[groups[0]]
    maps[unread] = {'identity': {**template['identity'], 'group_id': unread,
                                 'task_run_id': str(uuid4()), 'paper_id': 'unread-paper'},
                    'job': {**template['job'], 'job_id': str(uuid4()), 'claim_id': str(uuid4())},
                    'receipt_canonical_sha256': template['receipt_canonical_sha256']}
    with closing(sqlite3.connect(database)) as db, db:
        for table in ('tasks', 'postprocessing_claims', 'paperflow_postprocess_stage_jobs',
                      'postprocessing_results', 'postprocessing_receipts', 'llm_folder_queue'):
            db.execute('DELETE FROM ' + table)
        parent = maps[groups[0]]['identity']['parent_run_id']
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)', (task_id, parent, 'v2', 'running', 0, 7, 'complete'))
        for value in maps.values():
            ticket, job, receipt = value['identity'], value['job'], value['receipt_canonical_sha256']
            fields = [ticket[key] for key in FIELDS]
            db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (*fields, job['generation'], 'waiting_rag', job['claim_id'], job['manifest_sha256']))
            db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (*fields, job['job_id'], 'finished', job['manifest_sha256'], job['generation']))
            db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (*fields, job['claim_id'], 'passed', 'passed', receipt, job['manifest_sha256'], job['generation']))
            db.execute('INSERT INTO postprocessing_receipts VALUES(?,?)', (job['claim_id'], receipt))
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                       (*fields, 'waiting_postprocessing', job['manifest_sha256']))
    monkeypatch.setattr(isolated_group_prepare, 'prepare_group',
                        lambda *a, **k: pytest.fail('a completed group must not be prepared or sent again'))
    def inspect(_task, group):
        if group == unread:
            raise UnreadReviewRequired('missing unread-range disclosure')
        return maps[group]
    access = SimpleNamespace(database=database, artifact_map=inspect)
    logs = []
    runtime = SimpleNamespace(original_runtime={'database': database})

    async def run(bridge):
        deadline = time.monotonic() + 75
        while time.monotonic() < deadline:
            await bridge.tick()
            with closing(sqlite3.connect(database)) as db:
                done = db.execute("SELECT COUNT(*) FROM paperflow_rag_nas_jobs WHERE state='completed'").fetchone()[0]
                review = db.execute("SELECT COUNT(*) FROM paperflow_rag_nas_jobs WHERE state='needs_review'").fetchone()[0]
            if done == 2 and review == 1 and not bridge.pending():
                return
            if bridge.pending():
                await asyncio.wait(bridge.pending(), timeout=1, return_when=asyncio.FIRST_COMPLETED)
            else:
                await asyncio.sleep(.1)
        pytest.fail(f'two recorded groups did not finish: {logs}')

    with prefect_test_harness():
        batch_config = json.loads((Path(__file__).resolve().parents[1] / 'batch-309.json').read_text(encoding='utf8'))
        bridge = build(runtime, access, lambda event, **fields: logs.append((event, fields)), batch_config=batch_config)
        asyncio.run(run(bridge))
    with closing(sqlite3.connect(database)) as db:
        rows = db.execute('SELECT group_id,stage,state FROM paperflow_rag_nas_jobs ORDER BY group_id').fetchall()
    assert rows == [(unread, 'rag', 'needs_review')] + [
        (group, 'completed', 'completed') for group in sorted(groups)]
