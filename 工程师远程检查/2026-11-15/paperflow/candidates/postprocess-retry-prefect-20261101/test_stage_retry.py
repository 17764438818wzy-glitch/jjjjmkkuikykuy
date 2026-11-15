import importlib.util
from pathlib import Path
import sqlite3
import sys
import pytest

root = Path(__file__).resolve().parents[2]
candidate = Path(__file__).resolve().parent
server = root / 'candidates/postprocess-server-20261101'
sys.path[:0] = [str(candidate), str(server / 'tests'), str(server),
                str(root / 'candidates/postprocess-prefect-20261031/runtime_next'),
                str(root / 'candidates/postprocess-prefect-20261031/results_next'),
                str(root / 'candidates/postprocess-prefect-20261031'), str(root / 'prefect-paperflow')]
spec = importlib.util.spec_from_file_location('stage_runtime', candidate / 'stage_runtime.py')
stage_runtime = importlib.util.module_from_spec(spec)
sys.modules['stage_runtime'] = stage_runtime
spec.loader.exec_module(stage_runtime)

from test_remote_runtime_adapter import sample


def test_new_postprocessing_attempt_keeps_original_folder_and_previous_attempt(sample):
    base, database = sample['base'], sample['database']
    identity = sample['evidence'][0]['identity']
    old = base.reserve(identity, 1)
    result_id = sample['results'].reserve(identity, 1, old['manifest_sha256'])
    claim = sample['stage_store'].claim(identity, 1, old['manifest_sha256'])
    sample['stage_store'].mark_unknown(identity, claim['claim_id'], 'synthetic earlier unknown')
    sample['results'].record_unresolved(identity, result_id, code='outcome_unknown')
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown',error_type='TransportError'")
        db.execute("UPDATE tasks SET desired_state='running',start_generation=2 WHERE task_id=?", (identity['task_id'],))
    new = base.reserve(identity, 2, retry_job_id=old['job_id'])
    assert new['job_id'] != old['job_id']
    assert new['job_id'] == stage_runtime._job_id(identity['task_run_id'], old['job_id'])
    assert new['folder_run_id'] == old['folder_run_id'] == identity['task_run_id']
    assert new['source_llm_generation'] == old['source_llm_generation'] == 1
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT job_id FROM postprocessing_attempt_history').fetchone()[0] == old['job_id']


def test_failed_retry_reservation_leaves_original_attempt_visible(sample):
    base, database = sample['base'], sample['database']
    identity = sample['evidence'][0]['identity']
    old = base.reserve(identity, 1)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown' WHERE job_id=?", (old['job_id'],))
        db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (identity['task_id'],))
        db.execute('DELETE FROM llm_request_admissions WHERE task_id=?', (identity['task_id'],))
    with pytest.raises(stage_runtime.StageConflict):
        base.reserve(identity, 2, retry_job_id=old['job_id'])
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT job_id,state FROM paperflow_postprocess_stage_jobs').fetchone() == (old['job_id'], 'execution_unknown')
        assert db.execute('SELECT COUNT(*) FROM postprocessing_attempt_history').fetchone()[0] == 0
