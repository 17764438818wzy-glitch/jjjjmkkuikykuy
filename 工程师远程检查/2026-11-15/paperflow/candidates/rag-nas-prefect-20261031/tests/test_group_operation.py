"""One synthetic folder through the same RAG, archive and cleanup primitives."""
from contextlib import closing, nullcontext
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from test_group_stage import control
from test_group_prefect import Client, actual, fake_task, read
from test_isolated_group_rag import case
from group_stage import reserve_group


def test_completed_isolated_group_reuses_evidence_without_any_new_writes(monkeypatch):
    import isolated_group_operation as module
    def unexpected(*_args, **_kwargs):
        raise AssertionError('completed group must not upload or archive again')
    monkeypatch.setattr(module, 'publish_group', unexpected)
    monkeypatch.setattr(module, 'archive_test_copy', unexpected)
    monkeypatch.setattr(module, 'cleanup_test_copy', unexpected)
    context = {'reuse_receipts': {'rag': 'rag.json', 'copy': 'copy.json',
                                  'archive': 'archive.json', 'cleanup': 'cleanup.json'}}
    assert module.execute_group_stage('rag', {}, context)['reused_isolated_test'] is True
    assert module.execute_group_stage('nas', {}, context)['reused_isolated_test'] is True


def test_group_archive_pause_resumes_without_reupload_or_deleting_original(control, case, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import isolated_group_operation as operation_module
    from group_prefect import _run_attempt, advance_stage, submit_stage
    from paperflow_prefect.settings import Settings
    from paperflow_prefect.records import read_json

    database, ticket, verified = control
    ticket.update(group_id=case['ticket']['group_id'], paper_id=case['ticket']['paper_id'],
                  receipt_id=case['ticket']['receipt_id'])
    with closing(sqlite3.connect(database)) as db, db:
        for table in ('postprocessing_claims', 'paperflow_postprocess_stage_jobs',
                      'postprocessing_results', 'llm_folder_queue'):
            db.execute(f'UPDATE {table} SET group_id=?,paper_id=?,receipt_id=?',
                       (ticket['group_id'], ticket['paper_id'], ticket['receipt_id']))
    job = reserve_group(database, verified, 5)
    settings = Settings.load()
    context = {'database': database, 'copy_receipt_path': case['copied']['receipt_path'],
               'prepared': case['prepared'], 'packet': case['packet'], 'plan': case['plan'],
               'folder': case['folder'], 'attachments': [case['item']], 'profile_sha256': 'd' * 64,
               'raw': case['raw'], 'journal': case['journal'],
               'require_runtime': lambda: {'ready': True, 'ready_valid_until': __import__('time').time() + 10},
               'config': case['config'], 'runtime': read_json(settings.path('runtime_config')),
               'paperflow_root': settings.path('release_root') / 'core',
               'test_root': tmp_path / 'copies', 'nas_root': tmp_path / 'nas', 'work_root': tmp_path / 'work'}
    operation = lambda stage, row: operation_module.execute_group_stage(stage, row, context)
    slot = lambda _: nullcontext()
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', Client(), fake_task('rag', job['rag_task_run_id']))
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(_run_attempt, database, ticket['task_id'], ticket['group_id'], 'rag',
                    actual('rag', job['rag_task_run_id']), operation, slot).result()
    rag_run = actual('rag', job['rag_task_run_id']); rag_run.state = SimpleNamespace(is_completed=lambda: True)
    advance_stage(database, ticket['task_id'], ticket['group_id'], 'rag', rag_run)
    assert len(case['raw'].native) == 4
    assert all(Path(row['path']).exists() for row in case['copied']['files'])

    original_archive = operation_module.archive_test_copy
    admissions = []
    def interrupted_archive(*args, admit):
        def pause_during_copy():
            admissions.append(1)
            if len(admissions) == 4:
                with closing(sqlite3.connect(database)) as db, db:
                    db.execute("UPDATE tasks SET desired_state='paused'")
            return admit()
        return original_archive(*args, admit=pause_during_copy)
    monkeypatch.setattr(operation_module, 'archive_test_copy', interrupted_archive)
    submit_stage(database, ticket['task_id'], ticket['group_id'], 'nas', Client(), fake_task('nas', job['nas_task_run_id']))
    _run_attempt(database, ticket['task_id'], ticket['group_id'], 'nas', actual('nas', job['nas_task_run_id']), operation, slot)
    assert read(database)['state'] == 'paused'
    assert all(Path(row['path']).exists() for row in case['copied']['files'])

    monkeypatch.setattr(operation_module, 'archive_test_copy', original_archive)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
    _run_attempt(database, ticket['task_id'], ticket['group_id'], 'nas', actual('nas', job['nas_task_run_id']), operation, slot)
    nas_run = actual('nas', job['nas_task_run_id']); nas_run.state = SimpleNamespace(is_completed=lambda: True)
    advance_stage(database, ticket['task_id'], ticket['group_id'], 'nas', nas_run)
    assert read(database)['state'] == 'completed'
    assert len(case['raw'].native) == 4
    assert all(not Path(row['path']).exists() for row in case['copied']['files'])
    assert all(Path(row['source_delivery_path']).exists() for row in case['copied']['files'])
