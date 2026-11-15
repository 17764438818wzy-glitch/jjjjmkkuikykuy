from concurrent.futures import ThreadPoolExecutor
import json
import os
import sqlite3
import subprocess
import sys

import pytest

from remote_jobs import CapacityFull, RemoteJobs
from test_remote_jobs import job
from test_server_termination_proof import case


def ledger_with_recorded_termination(tmp_path, capacity=1):
    item, run, worker, entry, config = case()
    store = RemoteJobs(tmp_path / 'ledger.sqlite3', capacity=capacity)
    store.initialize()
    store.reserve(item)
    store.start(item['job_id'], item['claim_id'])
    store.mark_unknown(item['job_id'], item['claim_id'])
    with sqlite3.connect(store.database) as db:
        db.execute('CREATE TABLE supervisor_jobs(job_id TEXT PRIMARY KEY, frozen_json TEXT, scope_json TEXT, '
                   'process_json TEXT, config_json TEXT, clock_json TEXT)')
        db.execute('CREATE TABLE service_jobs(job_id TEXT PRIMARY KEY, frozen_json TEXT, phase TEXT, '
                   'config_sha256 TEXT, detail_json TEXT)')
        db.execute('INSERT INTO supervisor_jobs VALUES(?,?,?,?,?,?)', (item['job_id'], worker['frozen_json'],
            worker['scope_json'], worker['process_json'], worker['config_json'], worker['clock_json']))
        db.execute('INSERT INTO service_jobs VALUES(?,?,?,?,?)', (item['job_id'], entry['frozen_json'],
            entry['phase'], entry['config_sha256'], entry['detail_json']))
    return store, item, config


def test_confirmed_stopped_process_frees_only_capacity_without_changing_unknown_or_retry(tmp_path):
    store, item, config = ledger_with_recorded_termination(tmp_path)
    second = job(2)
    assert store.occupied() == 1
    with pytest.raises(CapacityFull): store.reserve(second)
    store.initialize_capacity_reconciliation()
    proof = store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    assert proof['result_state'] == 'unknown' and proof['process_terminated'] is True
    assert store.status(item['job_id'])['state'] == 'unknown' and store.occupied() == 0
    assert store.reserve(item) == {'may_start': False, 'state': 'unknown'}
    assert store.reserve(second)['may_start'] is True


def test_unproved_termination_keeps_its_capacity_slot(tmp_path):
    store, item, config = ledger_with_recorded_termination(tmp_path)
    store.initialize_capacity_reconciliation()
    with sqlite3.connect(store.database) as db:
        db.execute("UPDATE service_jobs SET detail_json='{}' WHERE job_id=?", (item['job_id'],))
    with pytest.raises(ValueError):
        store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    assert store.occupied() == 1 and store.status(item['job_id'])['state'] == 'unknown'
    with pytest.raises(CapacityFull): store.reserve(job(2))


def test_competing_new_jobs_still_share_one_real_capacity(tmp_path):
    store, item, config = ledger_with_recorded_termination(tmp_path)
    store.initialize_capacity_reconciliation()
    store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    def reserve(n):
        try: return store.reserve(job(n))['may_start']
        except CapacityFull: return False
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(2, 10)))
    assert sum(results) == 1 and store.occupied() == 1


@pytest.mark.parametrize('mutation', [
    "UPDATE supervisor_jobs SET process_json='{}'",
    "UPDATE service_jobs SET detail_json='{}'",
    "UPDATE terminated_capacity_releases SET evidence_sha256='invalid'",
    "UPDATE terminated_capacity_releases SET config_json='{}'",
    "DELETE FROM supervisor_jobs",
])
def test_changed_or_incomplete_proof_never_opens_a_capacity_slot(tmp_path, mutation):
    store, item, config = ledger_with_recorded_termination(tmp_path)
    store.initialize_capacity_reconciliation()
    store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    assert store.occupied() == 0
    with sqlite3.connect(store.database) as db:
        db.execute(mutation)
    # Invalid evidence must never make this unknown worker disappear from the
    # capacity calculation, including on the path that reserves a new job.
    assert store.occupied() == 1
    with pytest.raises(CapacityFull):
        store.reserve(job(2))


@pytest.mark.skipif(not os.environ.get('PAPERFLOW_OLD_APP'), reason='requires a separately installed old app')
def test_old_and_new_app_share_real_atomic_capacity_after_release(tmp_path):
    store, item, config = ledger_with_recorded_termination(tmp_path, capacity=2)
    store.initialize_capacity_reconciliation()
    store.release_terminated_capacity(item['job_id'], item['claim_id'], config)
    old_app, new_app = os.environ['PAPERFLOW_OLD_APP'], str(__import__('pathlib').Path(__file__).parents[1])
    script = ('import json,sys; from remote_jobs import RemoteJobs,CapacityFull; '
              'r=json.load(sys.stdin); s=RemoteJobs(r["db"],capacity=2,ledger_id=r["ledger"]); '
              'exec("try:\\n print(int(s.reserve(r[\'job\'])[\'may_start\']))\\n'
              'except CapacityFull:\\n print(0)")')
    def attempt(number):
        env = dict(os.environ, PYTHONPATH=old_app if number % 2 else new_app)
        request = dict(db=str(store.database), ledger=store.ledger_id, job=job(number))
        result = subprocess.run([sys.executable, '-c', script], input=json.dumps(request), text=True,
                                env=env, cwd=tmp_path, capture_output=True, timeout=30, check=True)
        return int(result.stdout.strip())
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(2, 10)))
    assert sum(results) == 2
    assert store.occupied() == 2
    assert store.reserve(item) == {'may_start': False, 'state': 'unknown'}
