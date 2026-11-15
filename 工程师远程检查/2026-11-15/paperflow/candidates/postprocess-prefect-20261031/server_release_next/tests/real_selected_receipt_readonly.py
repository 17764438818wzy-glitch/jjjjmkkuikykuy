"""Explicitly inspect one already-finished receipt; never claim or execute work.

The selected runtime authorizes only this job's remote materials. The local
ledger projection reads metadata globally, but this script sends only the
selected accepted receipt to the server's read-only inspection operation.
"""
from contextlib import closing
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import time


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
SERVER = HERE.parents[1] / 'postprocess-server-20261101'
FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    exec(compile(path.read_bytes(), str(path), 'exec', dont_inherit=True), module.__dict__)
    return module


def selected(database, task_id, group_id):
    with closing(sqlite3.connect(Path(database).resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        job = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                         (task_id, group_id)).fetchone()
        claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                           (task_id, group_id)).fetchone()
        result = db.execute('SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?',
                            (task_id, group_id)).fetchone()
        receipt = None if claim is None else db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?',
                                                        (claim['claim_id'],)).fetchone()
        states = dict(db.execute('SELECT state,COUNT(*) FROM paperflow_postprocess_stage_jobs GROUP BY state'))
    if not all(row is not None for row in (job, claim, result, receipt)):
        raise ValueError('selected registration is incomplete')
    return dict(job=dict(job), claim=dict(claim), result=dict(result), receipt=dict(receipt), states=states)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--config-sha256', required=True)
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--group-id', required=True)
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    sys.pycache_prefix = str(HERE / '.selected-readonly-no-bytecode')
    if Path(sys.pycache_prefix).exists():
        raise ValueError('private bytecode prefix must not exist')
    sys.path.insert(0, str(SERVER))
    from remote_live_launch import build_from_config
    runtime = build_from_config(args.config, args.config_sha256)
    from paperflow_prefect.llm_queue.folder_completion import component_id
    ledger = load('selected_readonly_ledger', ROOT / 'ledger_projection_next' / 'ledger_projection.py')
    database = runtime.original_runtime['database']
    initial = selected(database, args.task_id, args.group_id)
    before = ledger.snapshot(database, result_filter='passed', page=1, page_size=100, component_id_fn=component_id)
    claim, result, row = initial['claim'], initial['result'], initial['receipt']
    raw = row['receipt_json']
    digest = hashlib.sha256(raw.encode('utf8')).hexdigest()
    if (initial['job']['state'] != 'finished' or claim['state'] != 'waiting_rag'
            or result['state'] != 'passed' or row['receipt_sha256'] != digest
            or result['stage_receipt_sha256'] != digest or result['claim_id'] != claim['claim_id']):
        raise ValueError('selected durable records do not form a passed receipt')
    identity = {field: claim[field] for field in FIELDS}
    receipt = json.loads(raw)
    inspector = runtime.remote.stage_store.inspect_receipt
    inspector.journal._ready(runtime.remote.stage_store)
    started = time.monotonic()
    mappings = inspector.inspect_many([(receipt, identity, claim['claim_id'])], retain_maps=True)
    runtime.check_configuration()
    after = ledger.snapshot(database, result_filter='passed', page=1, page_size=100, component_id_fn=component_id)
    if selected(database, args.task_id, args.group_id) != initial or before.get('revision') != after.get('revision'):
        raise ValueError('registration changed during the observation')
    if not isinstance(mappings, list) or len(mappings) != 1 or mappings[0].get('integrity_status') != 'passed':
        raise ValueError('selected server receipt is not verified')
    items = before.get('results', {}).get('list', {}).get('items', [])
    present = any(item.get('task_id') == args.task_id and item.get('group_id') == args.group_id for item in items)
    if not present:
        raise ValueError('selected folder is absent from the local passed projection')
    print(json.dumps({'selected_stage': initial['job']['state'], 'selected_claim': claim['state'],
        'selected_result': result['state'], 'selected_receipt_sha256_matches': True,
        'selected_server_receipt_inspection': 'passed', 'selected_in_local_passed_list': present,
        'registered_passed': before['results']['counts']['passed'],
        'local_unresolved': before['stage']['counts']['postprocessing_unresolved'],
        'execution_unknown_count': initial['states'].get('execution_unknown', 0),
        'global_results_partial': before['results']['partial'],
        'server_inspection_seconds': round(time.monotonic() - started, 3),
        'global_source_scope_verified': False, 'business_work_started': False, 'rag_upload_started': False},
        ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'readonly_observation': 'unavailable', 'error_type': type(exc).__name__,
                          'business_work_started': False, 'rag_upload_started': False}, ensure_ascii=False))
        raise SystemExit(1)
