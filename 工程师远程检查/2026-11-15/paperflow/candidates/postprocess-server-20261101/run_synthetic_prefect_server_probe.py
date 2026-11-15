"""Explicit one-folder Prefect Task.fn -> real server synthetic probe.

The control DB, upstream evidence and Prefect execution context are synthetic.
The server must be a fresh, capacity-one synthetic service and the caller must
provide its short-lived SSH configuration on stdin. No live queues are read.
"""
import asyncio
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parent
UPSTREAM = ROOT.parent / 'postprocess-prefect-20261031'
for directory in (ROOT, ROOT / 'tests', UPSTREAM, UPSTREAM / 'tests',
                  UPSTREAM / 'runtime_next', UPSTREAM / 'results_next',
                  UPSTREAM.parent.parent / 'prefect-paperflow'):
    sys.path.insert(0, str(directory))

from prefect.context import TaskRunContext
from live_runtime import ResultsHooks
from remote_prefect_entry import RemoteCoordinatorFactory
from remote_runtime_adapter import make_remote_receipt_stores
from remote_server_transport import RemoteServerTransport
from stage_runtime import build_task
from test_remote_runtime_adapter import sample
from test_snapshot_collect import bind_catalog, fixture
import test_synthetic_mainline_integration as mainline


def canonical_sha(value):
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(text.encode('utf8')).hexdigest()


def main():
    supplied = json.load(sys.stdin)
    transport_config = supplied['transport_config']
    if (transport_config['server_identity']['server_id'] != 'synthetic-integration-only'
            or '/service-integration-' not in transport_config['service_config']
            or '/service-integration-' not in transport_config['staging_root']):
        raise ValueError('only a fresh isolated synthetic server service is permitted')
    root = ROOT / 'synthetic-prefect-server' / uuid4().hex
    root.mkdir(parents=True)
    data = sample.__wrapped__(root)
    catalog = data['catalog']
    original_body_fixture = mainline.body_fixture

    def body_with_catalog(*args, **kwargs):
        body, original_catalog = original_body_fixture(*args, **kwargs)
        bind_catalog({'body': body}, catalog, original_catalog)
        return body, original_catalog

    # The catalog participates in the frozen request plan, so bind it before
    # the synthetic producer freezes its two-body-part plan.
    with patch.object(mainline, 'body_fixture', body_with_catalog):
        evidence = fixture(root / 'integration-evidence', 2, rich=True, delivered=True)
    if any(supplied.get(key) is True for key in
           ('exercise_reading_scope', 'exercise_unread_scope', 'exercise_unlocated_unread_scope')):
        # Only these freshly generated synthetic replies are modified. One
        # part leaves reading unspecified; the other uses a real namespaced
        # block ID. Neither change touches an existing user ticket.
        from test_body_adapter import change_response
        for number, row in enumerate(evidence['body']['responses'], 1):
            refs = [] if number == 1 else [f'main:B{number:03d}']
            unread = (['main:unlocated-example'] if number == 1 and supplied.get('exercise_unlocated_unread_scope') is True
                      else ['B001'] if number == 1 and supplied.get('exercise_unread_scope') is True else [])
            change_response(row, lambda value, refs=refs, unread=unread: value['source']['reading_scope'].update(
                read_refs=refs, unread_refs=unread))
    previous = data['evidence'][0]
    old = previous['identity']
    new = evidence['identity']
    data['evidence'][0] = evidence
    with sqlite3.connect(data['database']) as db:
        db.execute('UPDATE tasks SET task_id=?,prefect_run_id=? WHERE task_id=?',
                   (new['task_id'], new['parent_run_id'], old['task_id']))
        db.execute('UPDATE llm_folder_queue SET task_id=?,group_id=?,paper_id=?,receipt_id=?,'
                   'attempt_id=?,parent_run_id=?,task_run_id=?,manifest_sha256=? WHERE task_id=?',
                   (new['task_id'], new['group_id'], new['paper_id'], new['receipt_id'],
                    new['attempt_id'], new['parent_run_id'], new['task_run_id'],
                    evidence['manifest_sha256'], old['task_id']))
        db.execute('UPDATE llm_request_admissions SET task_id=?,task_run_id=? WHERE task_id=?',
                   (new['task_id'], new['task_run_id'], old['task_id']))

    catalog_value = json.loads(catalog.read_bytes())
    catalog_hash = hashlib.sha256(catalog.read_bytes()).hexdigest()
    transport_config['allowed_roots'] = [str(root)]
    transport_config['trusted_files'] = [dict(path=str(catalog), sha256=catalog_hash)]
    transport_kwargs = {}
    if supplied.get('diagnostic_stage') is True:
        # This synthetic-only helper makes one staging attempt with the same
        # production function, but retains its exception traceback locally.
        def diagnostic_stage(connection, command, manifest, *, allowed_roots, timeout,
                             verify_locally, trusted_files):
            request = dict(connection=connection, command=command, manifest=manifest,
                allowed_roots=allowed_roots, io_timeout=min(timeout, 30),
                verify_locally=verify_locally, trusted_files=trusted_files)
            source = ("import sys,json,traceback;sys.path.insert(0," + repr(str(ROOT)) + ");"
                      "from ssh_stage_process import run;"
                      "exec('try:\\n r=run(json.load(sys.stdin))\\n print(json.dumps(r))"
                      "\\nexcept BaseException:\\n traceback.print_exc()\\n sys.exit(1)')")
            child = subprocess.run([sys.executable, '-B', '-c', source],
                input=json.dumps(request, ensure_ascii=False), text=True, encoding='utf8',
                capture_output=True, timeout=timeout)
            if child.returncode != 0:
                print('synthetic staging diagnostic:', child.stderr[-6000:], file=sys.stderr)
                raise ValueError('the single synthetic staging attempt failed')
            return json.loads(child.stdout)
        transport_kwargs['stage_call'] = diagnostic_stage
    transport_type = RemoteServerTransport
    admission_wait = None
    busy_count = [0]
    if supplied.get('capacity_wait_test') is True:
        from capacity_aware_transport import CapacityAwareTransport
        signal = Path(supplied['capacity_busy_signal'])
        if (not signal.is_absolute() or signal.parent != ROOT / 'synthetic-prefect-server'
                or signal.exists() or signal.is_symlink()):
            raise ValueError('a fresh synthetic capacity signal path is required')
        def wait_for_test_capacity():
            if not signal.exists():
                with signal.open('x', encoding='utf8') as stream:
                    stream.write('synthetic capacity busy')
            time.sleep(.5)
        admission_wait = dict(wait=wait_for_test_capacity, max_attempts=240)
        transport_type = CapacityAwareTransport
    transport = transport_type(transport_config, config_sha256=canonical_sha(transport_config),
                               journal=data['journal'], **transport_kwargs)
    if admission_wait is not None:
        original_attempt = transport.submit_attempt
        def tracked_attempt(*args):
            response = original_attempt(*args)
            if response.get('reason') == 'capacity_full':
                busy_count[0] += 1
            return response
        transport.submit_attempt = tracked_attempt
    server = transport_config['server_identity']
    stage_store, results = make_remote_receipt_stores(database=data['database'],
        admission_gate=data['task_gate'], journal=data['journal'], server_identity=server,
        observe_run=data['stage_store'].observe_run, inspect_batch=transport.inspect_batch,
        max_summary_receipts=10)
    base = data['base']
    base.verify_execution = lambda item, context: str(context.id) == item['job_id']
    remote_base = copy.copy(base)
    remote_base.results = ResultsHooks(results)
    counter = supplied['counter_contract']

    def load_inputs(identity, generation, item, context):
        if identity != new or generation != 1 or str(context.id) != item['job_id']:
            raise ValueError('synthetic Prefect task identity mismatch')
        return dict(inputs=copy.deepcopy(evidence), counter_contract=copy.deepcopy(counter),
                    trusted_catalog_path=catalog)

    snapshot_options = dict(catalog=catalog_value, code_sha256=supplied['code_sha256'],
        runtime_template_sha256=supplied['runtime_template_sha256'],
        paperflow_root_windows=supplied['paperflow_root_windows'],
        business_workspace_windows=supplied['business_workspace_windows'],
        output_root_windows=supplied['output_root_windows'], allowed_roots=[str(root)],
        max_json_file_bytes=8 * 1024 * 1024, max_total_json_bytes=64 * 1024 * 1024)
    parts = dict(base=remote_base, stage_store=stage_store, journal=data['journal'],
                 load_inputs=load_inputs, snapshot_options=snapshot_options)
    factory = RemoteCoordinatorFactory(server_identity=server, control_database=data['database'],
        journal_database=data['journal'].database, journal_id=data['journal'].journal_id,
        original_base=base, components_factory=lambda: dict(parts),
        transport=transport.coordinator_callbacks(), admission_wait=admission_wait)
    job = factory().reserve(new, 1)
    with sqlite3.connect(data['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?",
                   (job['job_id'],))
    context = SimpleNamespace(task_run=SimpleNamespace(id=UUID(job['job_id'])))
    with patch.object(TaskRunContext, 'get', classmethod(lambda cls: context)):
        result = asyncio.run(build_task(factory, job['job_id']).fn())
    with sqlite3.connect(data['database']) as db:
        result_states = db.execute('SELECT state FROM postprocessing_results').fetchall()
    with sqlite3.connect(data['journal'].database) as db:
        intents = [db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                   for table in ('remote_transfer_intents', 'remote_start_intents')]
    if result['state'] != 'waiting_rag' or result_states != [('passed',)] or intents != [1, 1]:
        raise ValueError('synthetic task, durable intents or existing result ledger did not finish')
    if admission_wait is not None and busy_count[0] < 1:
        raise ValueError('the expected synthetic capacity busy response was not observed')
    print(json.dumps(dict(status='passed', input_kind='synthetic', body_parts=2, attachments=1,
        reading_scope_exercised=supplied.get('exercise_reading_scope') is True,
        unread_scope_exercised=supplied.get('exercise_unread_scope') is True, synthetic_job_id=job['job_id'],
        unlocated_unread_exercised=supplied.get('exercise_unlocated_unread_scope') is True,
        capacity_busy_responses=busy_count[0],
        existing_prefect_task_function=True, synthetic_prefect_execution_context=True,
        synthetic_control_database=True, durable_host_intents=intents, server_transport='real_ssh',
        result_state=result['state'], llm_and_rag_entrypoints='not_used',
        offline_counter_inference='checked_zero_by_service'), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
