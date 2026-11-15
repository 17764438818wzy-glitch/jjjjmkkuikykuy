"""Observe exactly three existing folders through the candidate aggregator.

No service is started and no task, model request or RAG upload is dispatched.
The original local intake and Prefect metadata are read for scope verification.
"""
import hashlib
import hmac
import importlib.util
import json
import os
from concurrent.futures import Future
from pathlib import Path
import sqlite3
import sys
from threading import Lock
import time
from uuid import uuid4


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
CANDIDATES = HERE.parents[1]
SERVER = CANDIDATES / 'postprocess-server-20261101'
INTAKE = CANDIDATES / 'local-intake-live-link-20261031'
BOOTSTRAP_SHA = 'a41014d0952d23836e2e20a7b96f113bd8350380d2db0d676cd420f7965505d5'
GUARD_SHA = '9343b0d7367926fd4365a4672ee51617b60d4e77f30e7f2642e481dbb7e8d2be'
LAYOUT_SHA = '76e10389f88c377746112fd63f7959e09be1f36179aab9ebdb57b001a002b26a'
MANIFEST = HERE / 'release-inventory-three-receipt-probe-20261101-06.json'
MANIFEST_SHA = 'b1235269a03b29c97b60c3f88c6e4b1a385573505b5cb0ff5d6c7511a307ed6e'
RUNS = frozenset({'bf190daa-263c-4786-adc3-2cbafff20c1b', '35f35e94-610f-40c7-830d-11cc641597d2',
                  '12ef8b9a-a0f0-486b-9285-183544dfdc9a'})
PHASE = 'bootstrap'


def main(*, source_only=False):
    global PHASE
    sys.dont_write_bytecode = True
    sys.pycache_prefix = str(HERE / ('.page-observation-no-bytecode-' + uuid4().hex))
    source = HERE / 'selected_probe_bootstrap.py'
    raw = source.read_bytes()
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), BOOTSTRAP_SHA):
        raise ValueError('bootstrap changed')
    spec = importlib.util.spec_from_file_location('_selected_page_bootstrap', source)
    bootstrap = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(source), 'exec', dont_inherit=True), bootstrap.__dict__)
    release = bootstrap._bootstrap('release_guard', HERE / 'release_guard.py', GUARD_SHA)
    layout = bootstrap._bootstrap('release_layout', HERE / 'release_layout.py', LAYOUT_SHA)
    roots, required = layout.layout()
    guard = release.ReleaseGuard(MANIFEST, MANIFEST_SHA, roots=roots, required_files=required,
                                 required_packages=layout.PACKAGES)
    guard.verify()
    finder = bootstrap._SourceOnlyFinder(guard, release)
    for name, module in list(sys.modules.items()):
        if name in {'release_guard', 'release_layout'}:
            continue
        filename = getattr(module, '__file__', None)
        if filename and (name in finder.protected or finder._managed(Path(os.path.abspath(filename)))):
            raise ValueError('candidate module preloaded')
    sys.meta_path.insert(0, finder)
    for path in (SERVER, INTAKE, ROOT, HERE):
        sys.path.insert(0, str(path))
    guard.load_source('_paperflow_first_step_receipt_reader',
                      CANDIDATES / 'first-step-control-20261029' / 'paperflow_prefect' / '__init__.py')
    PHASE = 'setup'
    from read_only_rpc import (ExistingInspectVersionRouter, GuardedAppDelegate, PinnedAccessBindings,
                               ReadOnlyRPC, UnreadDisclosureGate, _read_pinned, forbid_transfer)
    from verified_ssh_transport import build
    history = HERE / 'history-registry-candidate-bb7d5d9ae45b.json'
    history_sha = '7fc35333f9d59094d388c4d54b30aed3db8b2b2e96b79dd5bf7d7da76a93ccb2'
    access = HERE / 'history-access-candidate-ed0ef92b075f.json'
    access_sha = 'e1f3444842540ab5fb8f48a6c45ec8fc503600f42ef066f680fe8bff1ea31599'
    bundle = HERE / 'guard-bundle-candidate-20261101-v2.json'
    bundle_sha = '9b304d0f21f8650e8534b9de2c1067a778f81f82d473cfc09962e936f511db13'
    registry = json.loads(_read_pinned(history, history_sha))
    bindings = PinnedAccessBindings(access, access_sha, server_identity=registry['server_identity'])
    base = bindings._sources()['base']
    call = build(guard, SERVER / 'ssh_rpc_client.py', SERVER / 'ssh_rpc_process.py')
    rpc = ReadOnlyRPC(call)
    import remote_live_launch
    loaded = remote_live_launch.build_from_config(base['config_path'], base['config_sha256'],
                                                   stage_call=forbid_transfer, rpc_call=rpc)
    control = Path(loaded.original_runtime['database']).resolve(strict=True)
    with sqlite3.connect(control.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        rows = db.execute('SELECT task_id,group_id,task_run_id FROM postprocessing_claims '
                          'WHERE task_run_id IN (?,?,?)', tuple(sorted(RUNS))).fetchall()
    if len(rows) != 3 or {row[2] for row in rows} != RUNS:
        raise ValueError('selected original claims are unavailable')
    keys = {(row[0], row[1]): row[2] for row in rows}
    if len(keys) != 3:
        raise ValueError('selected folders are not unique')
    bundle_data = json.loads(_read_pinned(bundle, bundle_sha))
    gate = UnreadDisclosureGate(bundle_data['unread_path'], bundle_data['unread_sha256'])
    router = ExistingInspectVersionRouter(history, history_sha, server_identity=registry['server_identity'],
                                         access_bindings=bindings, disclosure_gate=gate)
    guarded = GuardedAppDelegate(call, router, bundle, bundle_sha)
    calls = []
    def selected_only(connection, command, payload, **kwargs):
        if source_only:
            raise ValueError('server calls are disabled for source-only diagnosis')
        if isinstance(payload, dict) and set(payload) == {'requests'}:
            requests = payload['requests']
            if not isinstance(requests, list) or len(requests) != 1:
                raise ValueError('one selected receipt required')
            request = requests[0]
        elif isinstance(payload, dict) and set(payload) == {'job', 'server_identity', 'receipt_json', 'receipt_sha256', 'nonce'}:
            request = payload
        else:
            raise ValueError('only selected receipt and unread checks are allowed')
        identity = request.get('job', {}).get('identity', {})
        if keys.get((identity.get('task_id'), identity.get('group_id'))) != identity.get('task_run_id'):
            raise ValueError('outside selected folders')
        calls.append(identity['task_run_id'])
        return guarded(connection, command, payload, **kwargs)
    rpc.delegate = selected_only
    stage = loaded.remote.stage_store
    rpc.bind(database=control, inspector=stage.inspect_receipt, transport_config=loaded.remote.transport._config,
             version_router=router)
    from read_only_queue import open_queue
    queue = open_queue(HERE.parents[2] / 'local-intake-runtime' / 'waiting-pool.sqlite3')
    cli = guard.load_source('_selected_page_cli', HERE / 'waiting_cli.py')
    observer = cli.configured_orders(queue, guard=guard)
    if observer is None:
        raise ValueError('original observer unavailable')
    PHASE = 'page_assembly'
    import page_assembly
    app, registered = page_assembly.build_page(loaded_runtime=loaded, release_guard=guard, queue=queue,
                                               observer=observer, partial_observation=True)
    aggregator = registered.__self__
    # This isolated observation uses the real aggregation logic but executes
    # its bounded reads synchronously. No executor work can outlive the report.
    aggregator._pool.shutdown(wait=True, cancel_futures=True)
    class SynchronousPool:
        def submit(self, callback, *args):
            future = Future()
            try:
                future.set_result(callback(*args))
            except BaseException as exc:
                future.set_exception(exc)
            return future
        def shutdown(self, **_kwargs):
            return None
    aggregator._pool = SynchronousPool()
    source_outcomes = {}
    if source_only:
        # Block the server in two places, even if source verification succeeds.
        def no_server(_requests):
            raise ValueError('server calls are disabled for source-only diagnosis')
        stage.inspect_receipt._inspect_batch = no_server
        scope = aggregator.scope
        original_scope = scope.verify_selected
        known = {
            'receiver source unavailable': 'receiver_unavailable',
            'original large ticket differs': 'large_ticket_mismatch',
            'original folder scope or generation changed': 'folder_scope_changed',
            'selected folder is outside the original scope': 'folder_not_in_scope',
            'orphan stage or result': 'orphan_stage_or_result',
            'original retained handoff differs': 'retained_handoff_mismatch',
            'folder stage is not waiting for postprocessing': 'prefect_folder_state_mismatch',
            'postprocessing Prefect stage identity changed': 'prefect_stage_identity_mismatch',
            'postprocessing Prefect stage is unavailable': 'prefect_stage_unavailable',
            'finished postprocessing stage is not confirmed': 'prefect_stage_not_finished',
            'manual retry source contract is unresolved': 'manual_retry_unresolved',
            'LLM stage has not settled': 'llm_stage_unsettled',
            'frozen source or registered receipt changed': 'frozen_source_changed',
            'Prefect source changed during observation': 'prefect_source_changed',
            'original control or source changed during observation': 'control_or_source_changed',
        }
        def diagnosed_scope(request, selected):
            key = next(iter(selected))
            try:
                answer = original_scope(request, selected)
                source_outcomes[key] = 'verified' if answer is True else 'unavailable'
                return answer
            except Exception as exc:
                detail = exc.args[0] if exc.args and type(exc.args[0]) is str else None
                source_outcomes[key] = known.get(detail, 'other_source_error')
                raise
        scope.verify_selected = diagnosed_scope
    partial = aggregator.partial
    original_candidates, original_observe = partial.registered_candidates, partial.observe_one
    def selected_candidates(*args, **kwargs):
        snapshot, candidates = original_candidates(*args, **kwargs)
        return snapshot, [row for row in candidates if (row.get('task_id'), row.get('group_id')) in keys]
    outcomes, started, protect = {}, set(), Lock()
    def observe_once(database, selected, **kwargs):
        with protect:
            if selected not in keys or selected in started:
                return {'available': False, 'reason': 'one_observation_limit'}
            started.add(selected)
        result = original_observe(database, selected, **kwargs)
        with protect:
            outcomes[selected] = {'available_at_observation': result.get('available') is True,
                                  'reason_at_observation': (result.get('reason')
                                      if result.get('available') is not True else 'verified'),
                                  'observed_at_utc': result.get('as_of')}
        return result
    partial.registered_candidates, partial.observe_one = selected_candidates, observe_once
    PHASE = 'selected_aggregation'
    deadline = time.monotonic() + 60
    result, result_revision = None, None
    try:
        while time.monotonic() < deadline:
            revision_before = app.state.postprocessing_revision.current()
            result = registered(result_filter='passed', page=1, page_size=20)
            revision_after = app.state.postprocessing_revision.current()
            if revision_before != revision_after:
                continue
            result_revision = revision_after
            with protect:
                if len(outcomes) == 3 and not aggregator._inflight:
                    break
            time.sleep(.2)
        PHASE = 'final_consistency'
        guard.verify()
        guarded._bindings()
        if not isinstance(result, dict) or result.get('available') is not True:
            raise ValueError('candidate aggregation unavailable')
        started_at = getattr(result, '_server_observed_monotonic', None)
        valid_for = getattr(result, '_server_valid_for_seconds', None)
        source_revision = result.get('server_observation', {}).get('source_revision')
        fresh_ledger = aggregator.ledger_snapshot(control, component_id_fn=aggregator.component_id_fn)
        if (result_revision is None or app.state.postprocessing_revision.current() != result_revision
                or fresh_ledger.get('available') is not True or fresh_ledger.get('revision') != source_revision
                or type(started_at) not in (int, float) or type(valid_for) not in (int, float)
                or not started_at <= time.monotonic() < started_at + valid_for):
            raise ValueError('selected observation expired or changed')
        listing = result.get('results', {}).get('list', {}).get('items', [])
        statuses = {}
        for item in listing:
            key = (item.get('task_id'), item.get('group_id'))
            if key in keys:
                statuses[keys[key]] = ('pending' if item.get('verification_pending') is True else 'verified')
        output = {'schema': 'paperflow-selected-page-observation/1', 'scope': 'three_selected_folders',
                  'item_statuses': statuses,
                  'first_attempts': {keys[key]: value for key, value in outcomes.items()},
                  'verified_count': result.get('results', {}).get('counts', {}).get('passed'),
                  'waiting_rag_count': result.get('stage', {}).get('counts', {}).get('waiting_rag_upload'),
                  'selected_request_count': len(calls), 'source_stable': True,
                  'full_live_page_verified': False, 'processing_started': False, 'rag_upload': False}
        if source_only:
            output['server_calls_disabled'] = True
            output['source_diagnostics'] = {keys[key]: value for key, value in source_outcomes.items()}
        report = HERE / ('selected-page-source-diagnosis-20261101-01.json' if source_only
                         else 'selected-page-observation-20261101-01.json')
        with report.open('x', encoding='utf8', newline='\n') as stream:
            json.dump(output, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write('\n')
        print(json.dumps(output, ensure_ascii=False))
    finally:
        aggregator.close()
        app.state.postprocessing_revision.close()


if __name__ == '__main__':
    try:
        if sys.argv[1:] not in ([], ['--source-only']):
            raise ValueError('fixed observation mode required')
        main(source_only=sys.argv[1:] == ['--source-only'])
    except BaseException:
        print(json.dumps({'status': 'observation_unavailable', 'phase': PHASE,
                          'processing_started': False, 'rag_upload': False}))
        raise SystemExit(1) from None
