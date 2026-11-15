"""An unconfirmed server job must not become a RAG-ready folder."""
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_execution_unknown_is_not_ready_or_retried(tmp_path):
    fixtures = load('release_unknown_fixtures', ROOT / 'remote_projection_next' / 'tests' /
                    'test_server_projection.py')
    server = load('release_unknown_projection', ROOT / 'server_release_next' / 'server_projection.py')
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.helpers.add_folder(db, 1, job='execution_unknown', claim='unknown',
                                            outcome=('unresolved', 'outcome_unknown'), receipt=False)
    inspector, calls = fixtures.make_inspector(db)
    probes = []
    value = server.snapshot(db.path, ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, verify_scope=lambda _: True,
        probe_server=lambda _: probes.append(True) or True)
    assert value['stage']['counts']['waiting_rag_upload'] is None
    assert value['stage']['counts']['postprocessing_in_progress'] is None
    assert value['stage']['counts']['postprocessing_unresolved'] >= 1
    assert value['results']['counts']['passed'] is None
    assert calls == [] and probes == [True]
    with __import__('sqlite3').connect(db.path) as conn:
        assert conn.execute('SELECT state FROM paperflow_postprocess_stage_jobs WHERE task_run_id=?',
                            (identity['task_run_id'],)).fetchone()[0] == 'execution_unknown'
