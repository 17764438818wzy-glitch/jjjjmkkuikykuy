"""One bounded global observation; remote inspection is restricted to one job.

This reads existing local handoff/material/receipt evidence. It never scans,
claims, modifies a queue, starts a task, requests a model or uploads to RAG.
It must only be invoked with explicit authority to read the global local scope.
"""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
CANDIDATES = HERE.parents[1]
SERVER = CANDIDATES / 'postprocess-server-20261101'
INTAKE = CANDIDATES / 'local-intake-live-link-20261031'
PHASE = 'setup'


def child(args):
    global PHASE
    sys.dont_write_bytecode = True
    sys.pycache_prefix = str(HERE / '.global-readonly-no-bytecode')
    if Path(sys.pycache_prefix).exists():
        raise ValueError('private bytecode prefix must not exist')
    for path in (HERE, INTAKE, SERVER):
        sys.path.insert(0, str(path))
    import release_guard
    import release_layout
    PHASE = 'release_inventory'
    roots, required = release_layout.layout(HERE / 'site')
    guard = release_guard.ReleaseGuard(args.manifest, args.manifest_sha256, roots=roots,
        required_files=required, required_packages=release_layout.PACKAGES)
    guard.verify()
    PHASE = 'pinned_imports'
    sys.meta_path.insert(0, release_guard.PinnedFinder(guard))
    guard.load_source('_paperflow_first_step_receipt_reader',
                      CANDIDATES / 'first-step-control-20261029' / 'paperflow_prefect' / '__init__.py')
    from ssh_rpc_client import call_bounded
    remote_calls = []
    def selected_inspection_only(connection, command, payload, **kwargs):
        if shlex.split(command)[-1] != 'inspect':
            raise ValueError('only the selected read-only receipt inspection is permitted')
        requests = payload.get('requests') if isinstance(payload, dict) else None
        if (not isinstance(requests, list) or len(requests) != 1
                or requests[0].get('job', {}).get('identity', {}).get('task_id') != args.task_id
                or requests[0].get('job', {}).get('identity', {}).get('group_id') != args.group_id):
            raise ValueError('remote inspection exceeds the explicitly selected folder')
        remote_calls.append('selected_inspection')
        return call_bounded(connection, command, payload, **kwargs)
    import remote_live_launch
    PHASE = 'selected_runtime'
    runtime = remote_live_launch.build_from_config(args.config, args.config_sha256, rpc_call=selected_inspection_only)
    from read_only_queue import open_queue
    waiting_cli = guard.load_source('_global_readonly_waiting_cli', HERE / 'waiting_cli.py')
    PHASE = 'open_readonly_queue'
    queue = open_queue(args.queue_db)
    PHASE = 'configure_observer'
    observer = waiting_cli.configured_orders(queue, guard=guard)
    if observer is None:
        raise ValueError('original Prefect observer is unavailable')
    import page_assembly
    PHASE = 'page_assembly'
    _app, registered = page_assembly.build_page(loaded_runtime=runtime, release_guard=guard,
                                                 queue=queue, observer=observer)
    if args.partial:
        from paperflow_prefect.llm_queue.folder_completion import component_id
        partial = guard.load_source('_real_partial_observation', HERE / 'partial_projection.py')
        ledger = sys.modules['_release_page_registered_ledger']
        server_helpers = sys.modules['_release_page_server_projection']
        started = time.monotonic()
        PHASE = 'selected_partial_observation'
        value = partial.observe_one(runtime.original_runtime['database'], (args.task_id, args.group_id),
            ledger_snapshot=ledger.snapshot, component_id_fn=component_id,
            inspector=runtime.remote.stage_store.inspect_receipt, stage_store=runtime.remote.stage_store,
            scope=registered.keywords['verify_scope'], server_helpers=server_helpers)
        remaining = (round(max(0, value['valid_until_monotonic'] - time.monotonic()), 3)
                     if value.get('available') is True else None)
        print(json.dumps({'partial_observation_available': value.get('available'),
            'reason': value.get('reason'), 'elapsed_seconds': round(time.monotonic() - started, 3),
            'freshness_remaining_seconds': remaining, 'global_counts_available': value.get('global_counts_available'),
            'selected_only': value.get('scope') == 'verified_registered_subset',
            'remote_selected_inspection_calls': len(remote_calls),
            'business_work_started': False, 'rag_upload_started': False}, ensure_ascii=False, sort_keys=True))
        return
    # Capture only exception types, not local material or paths, while still
    # invoking the actual ScopeVerifier built by the candidate page assembly.
    scope = registered.keywords['verify_scope']
    scope_errors = []
    def checked_scope(request):
        try:
            return scope.verify(request)
        except Exception as exc:
            scope_errors.append(type(exc).__name__)
            return False
    registered.keywords['verify_scope'] = checked_scope
    PHASE = 'global_observation'
    started = time.monotonic()
    value = registered(result_filter='passed', page=1, page_size=100)
    observed = value.get('server_observation') or {}
    stage, results = value.get('stage') or {}, value.get('results') or {}
    listing = results.get('list') or {}
    selected_present = any(item.get('task_id') == args.task_id and item.get('group_id') == args.group_id
                           for item in listing.get('items', []))
    print(json.dumps({'global_projection_available': value.get('available'),
        'server_observation_available': observed.get('available'), 'reason': observed.get('reason'),
        'scope_failure_types': scope_errors, 'elapsed_seconds': round(time.monotonic() - started, 3),
        'waiting_postprocessing': stage.get('counts', {}).get('waiting_postprocessing'),
        'waiting_rag_upload': stage.get('counts', {}).get('waiting_rag_upload'),
        'unresolved': stage.get('counts', {}).get('postprocessing_unresolved'),
        'registered_passed': results.get('counts', {}).get('passed'),
        'partial': results.get('partial'), 'selected_in_passed_list': selected_present,
        'remote_selected_inspection_calls': len(remote_calls),
        'business_work_started': False, 'rag_upload_started': False}, ensure_ascii=False, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    for name in ('config', 'config-sha256', 'manifest', 'manifest-sha256', 'queue-db', 'task-id', 'group-id'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--child', action='store_true')
    parser.add_argument('--partial', action='store_true')
    args = parser.parse_args()
    if args.child:
        try:
            child(args)
        except Exception as exc:
            print(json.dumps({'readonly_observation': 'unavailable', 'phase': PHASE, 'error_type': type(exc).__name__,
                              'business_work_started': False, 'rag_upload_started': False}))
            raise SystemExit(1)
        return
    try:
        result = subprocess.run([sys.executable, '-B', str(Path(__file__).resolve()), '--child', *sys.argv[1:]],
            capture_output=True, text=True, encoding='utf8', errors='replace', timeout=45)
        print(result.stdout.strip() or json.dumps({'readonly_observation': 'unavailable', 'error_type': 'no_output'}))
        raise SystemExit(result.returncode)
    except subprocess.TimeoutExpired:
        print(json.dumps({'readonly_observation': 'unavailable', 'error_type': 'timeout_45s',
                          'business_work_started': False, 'rag_upload_started': False}))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
