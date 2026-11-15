"""Explicitly observe named original receipts; never claim, run, retry or upload.

Outputs only identities, bounded observations and durations. This is not the
full page/source/Prefect check and never grants RAG admission.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import time
from uuid import UUID


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SERVER = ROOT.parent / 'postprocess-server-20261101'
WORKSPACE = ROOT.parents[1]
SELECTED_RUNS = frozenset({
    'bf190daa-263c-4786-adc3-2cbafff20c1b',
    '35f35e94-610f-40c7-830d-11cc641597d2',
    '12ef8b9a-a0f0-486b-9285-183544dfdc9a',
})
for path in (SERVER, ROOT, ROOT / 'runtime_next', ROOT / 'results_next', WORKSPACE / 'prefect-paperflow', HERE):
    sys.path.insert(0, str(path))

from release_guard import ReleaseGuard
from remote_coordinator import RemoteSubmissionJournal
from remote_runtime_adapter import RemoteReceiptInspector
from observation_diagnostics import safe_exception_type, wrap
from read_only_rpc import (ExistingInspectVersionRouter, GuardedAppDelegate, PinnedAccessBindings, ReadOnlyRPC,
                           UnreadDisclosureGate, UnreadReviewRequired, _read_pinned, command_for)
from verified_ssh_transport import build


def _read_only(path):
    return sqlite3.connect(Path(path).resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=5)


def _new_candidate_file(path):
    path = Path(path)
    if not path.is_absolute() or not path.parent.resolve(strict=True).is_relative_to(HERE) or path.exists():
        raise ValueError('a new explicit candidate output is required')
    return path


class _QuietParser(argparse.ArgumentParser):
    def error(self, _message):
        raise ValueError('invalid observation arguments')


def main(guard=None, argv=None):
    if type(guard) is not ReleaseGuard or guard._approved is None:
        raise ValueError('a separately reviewed, verified observation inventory is required')
    parser = _QuietParser(add_help=False)
    parser.add_argument('--run-id', action='append', required=True)
    parser.add_argument('--report', required=True)
    parser.add_argument('--timeout', type=float, default=30)
    args = parser.parse_args(argv)
    run_ids = [str(UUID(value)) for value in args.run_id]
    if len(run_ids) != 3 or set(run_ids) != SELECTED_RUNS or not 0 < args.timeout <= 30:
        raise ValueError('the three explicitly selected folder runs and a bounded timeout are required')
    report = _new_candidate_file(args.report)
    guard.verify()

    history = HERE / 'history-registry-candidate-bb7d5d9ae45b.json'
    access = HERE / 'history-access-candidate-ed0ef92b075f.json'
    bundle = HERE / 'guard-bundle-candidate-20261101-v2.json'
    history_sha = '7fc35333f9d59094d388c4d54b30aed3db8b2b2e96b79dd5bf7d7da76a93ccb2'
    access_sha = 'e1f3444842540ab5fb8f48a6c45ec8fc503600f42ef066f680fe8bff1ea31599'
    bundle_sha = '9b304d0f21f8650e8534b9de2c1067a778f81f82d473cfc09962e936f511db13'
    registry = json.loads(_read_pinned(history, history_sha).decode('utf8'))
    diagnostics = []
    bindings = PinnedAccessBindings(access, access_sha, server_identity=registry['server_identity'])
    bindings.resolve = wrap('access', bindings.resolve, diagnostics)
    base_source = bindings._sources()['base']
    config = json.loads(_read_pinned(base_source['config_path'], base_source['config_sha256']).decode('utf-8-sig'))
    connection = bindings.resolve('base')
    transport = config['transport_config']
    if connection != transport['connection']:
        raise ValueError('the selected observation connection changed')
    journal_path = Path(config['journal_database']).resolve(strict=True)
    with _read_only(journal_path) as db:
        settings = dict(db.execute('SELECT name,value FROM journal_settings'))
    if settings.get('journal_id') != config['journal_id']:
        raise ValueError('the original journal differs from the selected configuration')
    control = Path(settings['control_database']).resolve(strict=True)
    journal = RemoteSubmissionJournal(journal_path, control_database=control, admission_gate=lambda *_: None,
                                      journal_id=config['journal_id'])
    inspector = RemoteReceiptInspector(journal=journal, server_identity=registry['server_identity'],
                                       inspect_batch=lambda _: None, max_summary_receipts=3)
    inspector._accepted = wrap('accept', inspector._accepted, diagnostics)
    files = (SERVER / 'ssh_rpc_client.py', SERVER / 'ssh_rpc_process.py')
    call = wrap('transport', build(guard, *files), diagnostics)
    bundle_value = json.loads(_read_pinned(bundle, bundle_sha).decode('utf8'))
    disclosure = UnreadDisclosureGate(bundle_value['unread_path'], bundle_value['unread_sha256'])
    router = ExistingInspectVersionRouter(history, history_sha, server_identity=registry['server_identity'],
                                          access_bindings=bindings, disclosure_gate=disclosure)
    guarded = GuardedAppDelegate(call, router, bundle, bundle_sha)
    rpc = ReadOnlyRPC(wrap('guarded_command', guarded, diagnostics))
    rpc.bind(database=control, inspector=inspector, transport_config=transport, version_router=router)
    rpc._registered = wrap('registration', rpc._registered, diagnostics)

    observations, saved_requests = [], {}
    for run_id in run_ids:
        diagnostics.clear()
        started, phase = time.monotonic(), 'registered_receipt'
        item = {'task_run_id': run_id, 'status': 'unknown'}
        try:
            with _read_only(control) as db:
                rows = db.execute('SELECT c.claim_id,r.receipt_json FROM postprocessing_claims c '
                    'JOIN postprocessing_receipts r ON r.claim_id=c.claim_id WHERE c.task_run_id=?', (run_id,)).fetchall()
            if len(rows) != 1:
                raise ValueError('the selected original receipt is unavailable')
            claim_id, raw = rows[0]
            receipt = json.loads(raw)
            identity = receipt['identity']
            if identity.get('task_run_id') != run_id:
                raise ValueError('the original folder identity differs')
            request = inspector._request(receipt, identity, claim_id)
            saved_requests[run_id] = request
            phase = 'server_receipt_and_unread'
            result = rpc(connection, command_for(transport, 'inspect'), {'requests': [request]},
                         timeout=args.timeout, max_bytes=16 * 1024 * 1024)
            if len(result) != 1:
                raise ValueError('the selected server result is incomplete')
            inspector._accepted(request, result[0])
            item['status'] = 'verified_receipt_and_specific_unread_check'
        except UnreadReviewRequired:
            item['status'] = 'review_required_unlocated_unread'
        except Exception as exc:
            item.update(phase=phase, error_type=safe_exception_type(exc))
        item['elapsed_seconds'] = round(time.monotonic() - started, 3)
        item['observed_at_utc'] = datetime.now(timezone.utc).isoformat()
        item['failure_categories'] = list(diagnostics)
        observations.append(item)
    try:
        for item in observations:
            if item['status'] in {'verified_receipt_and_specific_unread_check', 'review_required_unlocated_unread'}:
                try:
                    rpc._registered(saved_requests[item['task_run_id']])
                except Exception:
                    item['status'] = 'unknown_registration_changed_after_observation'
        # Recheck pinned version/access and guard bundle after all observations.
        router._versions()
        for reference in bindings._sources():
            bindings.resolve(reference)
        guarded._bindings()
        guard.verify()
        source_stable = True
    except Exception:
        source_stable = False
        for item in observations:
            item['status'] = 'unknown_source_changed_during_observation'
    value = dict(schema='paperflow-selected-readonly-observation/1', observations=observations,
                 observation_inventory_sha256=guard.sha256, source_stable=source_stable,
                 whole_page_or_prefect_scope_verified=False, processing_started=False, rag_upload=False)
    with report.open('x', encoding='utf8', newline='\n') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps({'report_written': True, **value}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
