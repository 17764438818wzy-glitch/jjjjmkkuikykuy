"""Synthetic old and new receipts use the same current reader."""
from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest

here = Path(__file__).resolve().parent
root = here.parents[1]
sys.path[:0] = [str(here), str(root / 'candidates/postprocess-server-20261101')]
spec = importlib.util.spec_from_file_location('candidate_server_receipt_query', here / 'server_receipt_query.py')
query = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query)


def test_current_reader_selects_recorded_config_for_old_and_new_receipts(tmp_path, monkeypatch):
    database = tmp_path / 'jobs.sqlite3'
    current = {'server_identity': {'server_id': 'same', 'ledger_id': 'same'}}
    old = {'server_identity': current['server_identity'], 'version': 'old-data'}
    old_raw = json.dumps(old, sort_keys=True, separators=(',', ':')).encode()
    old_path = tmp_path / 'old-config.json'
    old_path.write_bytes(old_raw)
    old_sha = hashlib.sha256(old_raw).hexdigest()
    current_sha = hashlib.sha256(query._json(current).encode()).hexdigest()
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE service_jobs(job_id TEXT,config_sha256 TEXT)')
        db.executemany('INSERT INTO service_jobs VALUES(?,?)', [('old-job', old_sha), ('new-job', current_sha)])
    ledger = SimpleNamespace(_connect=lambda: sqlite3.connect(database))
    monkeypatch.setattr(query, 'HISTORICAL_CONFIGS', {old_sha: str(old_path)})
    monkeypatch.setattr(query.service, 'load_config', lambda path, sha: json.loads(Path(path).read_bytes()))
    assert query.source_config(ledger, {'job_id': 'old-job'}, current) == old
    assert query.source_config(ledger, {'job_id': 'new-job'}, current) is current
    monkeypatch.setattr(query, 'HISTORICAL_CONFIGS', {})
    with pytest.raises(query.ReceiptQueryError, match='original service configuration'):
        query.source_config(ledger, {'job_id': 'old-job'}, current)


def test_inspection_uses_original_evidence_with_current_reader(monkeypatch):
    current = {'server_identity': {'server_id': 'same'}}
    original = {'server_identity': current['server_identity'], 'version': 'recorded'}
    job = {'job_id': 'old-job', 'code_sha256': 'old-code'}
    receipt = {'identity': 'synthetic'}
    raw = json.dumps(receipt)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    ledger, seen = object(), []
    monkeypatch.setattr(query.service, '_config', lambda config: None)
    monkeypatch.setattr(query.service, '_ledger', lambda config: ledger)
    monkeypatch.setattr(query, '_validated', lambda value: None)
    monkeypatch.setattr(query, 'source_config', lambda actual, item, config: original)
    monkeypatch.setattr(query, '_finished', lambda actual, item, config: (seen.append(config) or (raw, digest, receipt)))
    monkeypatch.setattr(query.service, 'verify_job', lambda ident, config:
                        (seen.append(config) or {'job': job, 'snapshot': 'snapshot',
                                                'manifest': {'output_root_windows': 'output'}}))
    monkeypatch.setattr(query, '_verify_code', lambda item, config, snapshot: seen.append(config))
    monkeypatch.setattr(query.service, '_business_receipt', lambda *args: True)
    monkeypatch.setattr(query, 'inspect_receipt', lambda **kwargs:
                        {'integrity_status': 'passed', 'job': job, 'receipt_canonical_sha256': digest})
    request = {'job': job, 'server_identity': current['server_identity'], 'receipt_json': raw, 'receipt_sha256': digest}
    result = query.inspect_requests(current, [request])
    assert result[0]['business_code_sha256'] == 'old-code'
    assert seen == [original, original, original]
