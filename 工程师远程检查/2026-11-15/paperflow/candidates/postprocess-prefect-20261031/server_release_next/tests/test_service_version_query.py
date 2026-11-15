"""Only temporary server ledger evidence; no network or real jobs."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from uuid import uuid4

import pytest

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent


def load(name, path):
    assert path.is_file(), 'the isolated server version query is missing'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures = load('synthetic_version_query_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_server_projection.py')


@pytest.fixture
def sample(tmp_path):
    from remote_jobs import RemoteJobs, _validated
    local = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.add_complete_pass(local, 1)
    inspector, _ = fixtures.make_inspector(local)
    with sqlite3.connect(local.path) as db:
        raw, claim = db.execute('SELECT receipt_json,claim_id FROM postprocessing_receipts').fetchone()
    request = inspector._request(json.loads(raw), identity, claim)
    request['job'].update(generation=1, snapshot_sha256='a' * 64)
    request['nonce'] = str(uuid4())
    ledger = RemoteJobs(tmp_path / 'server.sqlite3', capacity=8, ledger_id=inspector.server['ledger_id'])
    ledger.initialize()
    ledger.reserve(request['job'])
    digest = request['receipt_sha256']
    with sqlite3.connect(ledger.database) as db:
        db.execute('CREATE TABLE supervisor_jobs(job_id TEXT PRIMARY KEY, frozen_json TEXT, receipt_json TEXT, held_sha256 TEXT)')
        db.execute('CREATE TABLE service_jobs(job_id TEXT PRIMARY KEY, frozen_json TEXT, config_sha256 TEXT, phase TEXT)')
        db.execute('INSERT INTO supervisor_jobs VALUES(?,?,?,?)',
                   (request['job']['job_id'], _validated(request['job']), request['receipt_json'], digest))
        db.execute('INSERT INTO service_jobs VALUES(?,?,?,?)',
                   (request['job']['job_id'], _validated(request['job']), 'b' * 64, 'finished'))
        db.execute("UPDATE executor_jobs SET state='finished',receipt_sha256=? WHERE job_id=?",
                   (digest, request['job']['job_id']))
    return ledger, request, {'server_identity': inspector.server}


def test_one_finished_exact_receipt_returns_only_its_original_service_hash(sample, monkeypatch):
    ledger, request, config = sample
    module = load('synthetic_service_version', HERE / 'server_version_query.py')
    import server_job_service as service
    monkeypatch.setattr(service, '_ledger', lambda _: ledger)
    before = hashlib.sha256(ledger.database.read_bytes()).hexdigest()
    response = module.lookup(config, request)
    assert response == {'schema': 'paperflow-server-job-config/1', 'server_identity': config['server_identity'],
                        'job_id': request['job']['job_id'], 'receipt_sha256': request['receipt_sha256'],
                        'nonce': request['nonce'], 'service_config_canonical_sha256': 'b' * 64}
    assert hashlib.sha256(ledger.database.read_bytes()).hexdigest() == before


@pytest.mark.parametrize('changed', ['receipt', 'job', 'server', 'nonce', 'unfinished', 'service_frozen',
                                      'worker_frozen', 'worker_receipt', 'config_hash', 'ledger_identity'])
def test_version_lookup_rejects_mismatched_or_unfinished_evidence(sample, monkeypatch, changed):
    ledger, request, config = sample
    request = copy.deepcopy(request)
    module = load('synthetic_service_version_' + changed, HERE / 'server_version_query.py')
    import server_job_service as service
    monkeypatch.setattr(service, '_ledger', lambda _: ledger)
    if changed == 'receipt': request['receipt_sha256'] = 'f' * 64
    elif changed == 'job': request['job']['snapshot_sha256'] = 'f' * 64
    elif changed == 'server': request['server_identity']['server_id'] = 'another-server'
    elif changed == 'nonce': request['nonce'] = 'invalid'
    else:
        with sqlite3.connect(ledger.database) as db:
            if changed == 'unfinished': db.execute("UPDATE service_jobs SET phase='running'")
            elif changed == 'service_frozen': db.execute("UPDATE service_jobs SET frozen_json='{}'")
            elif changed == 'worker_frozen': db.execute("UPDATE supervisor_jobs SET frozen_json='{}'")
            elif changed == 'worker_receipt': db.execute("UPDATE supervisor_jobs SET receipt_json='{}'")
            elif changed == 'config_hash': db.execute("UPDATE service_jobs SET config_sha256='bad'")
            elif changed == 'ledger_identity': db.execute("UPDATE executor_settings SET value=? WHERE name='ledger_id'", (str(uuid4()),))
    with pytest.raises(ValueError): module.lookup(config, request)


def test_lookup_script_imports_from_explicit_app_directory_when_installed_separately(sample, tmp_path):
    ledger, request, config = sample
    app = tmp_path / 'reviewed-app'
    app.mkdir()
    import remote_jobs
    frozen = remote_jobs._validated(request['job'])
    (app / 'remote_jobs.py').write_text(
        'def _uuid(value): return True\ndef _validated(job): return ' + repr(frozen) + '\n', encoding='utf8')
    (app / 'server_job_service.py').write_text(
        'from pathlib import Path\nfrom types import SimpleNamespace\n'
        'def load_config(path, sha): return ' + repr(config) + '\n'
        'def _ledger(config): return SimpleNamespace(database=Path(' + repr(str(ledger.database)) +
        '), busy_timeout=1, capacity=8, ledger_id=' + repr(ledger.ledger_id) + ')\n', encoding='utf8')
    isolated = tmp_path / 'observer'
    isolated.mkdir()
    helper = isolated / 'server_version_query.py'
    helper.write_bytes((HERE / 'server_version_query.py').read_bytes())
    result = subprocess.run([sys.executable, '-B', str(helper), str(app), '/unused-config', 'a' * 64],
        input=json.dumps(request), capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['service_config_canonical_sha256'] == 'b' * 64
