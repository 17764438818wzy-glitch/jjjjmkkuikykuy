"""Synthetic-only tests for server-verified folder observations."""
import copy
from contextlib import nullcontext
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
import sqlite3
import sys
import time
from uuid import uuid4, uuid5

import pytest

ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT.parent / 'postprocess-server-20261101'
for path in (SERVER, ROOT, ROOT / 'runtime_next', ROOT / 'results_next',
             ROOT.parents[1] / 'prefect-paperflow'):
    sys.path.insert(0, str(path))

from remote_coordinator import NAMESPACE, RemoteSubmissionJournal
from remote_runtime_adapter import RemoteFolderStageStore, RemoteReceiptInspector


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


helpers = load('server_projection_ledger_test_helpers',
               ROOT / 'ledger_projection_next' / 'tests' / 'test_ledger_projection.py')


@pytest.fixture
def db(tmp_path):
    return helpers.db.__wrapped__(tmp_path)


def candidate():
    return load('server_projection_candidate', ROOT / 'remote_projection_next' / 'server_projection.py')


def synthetic_seals(receipt):
    package = PureWindowsPath(receipt['body']['package_path'])
    members = []
    for row in receipt['artifacts']:
        try:
            relative = PureWindowsPath(row['path']).relative_to(package)
        except ValueError:
            continue
        name = PurePosixPath(*relative.parts).as_posix()
        if name.casefold() in {'manifest.json', 'ready.json'}:
            continue
        members.append({'path': name, 'sha256': row['sha256'], 'bytes': 17})
    ident = {'task_id': 'synthetic-task', 'source_id': 'synthetic-source', 'source_sha256': 'f' * 64}
    excluded = {'split-plan.json', *receipt.get('_synthetic_excluded_publications', [])}
    manifest = dict(schema='paperflow/1.0', **ident, files=members,
                    publication_files=[row['path'] for row in members if row['path'] not in excluded])
    raw_manifest = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    manifest_hash = hashlib.sha256(raw_manifest.encode('utf8')).hexdigest()
    ready = dict(status='ready', manifest='manifest.json', manifest_sha256=manifest_hash, **ident)
    raw_ready = json.dumps(ready, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return raw_manifest, raw_ready


def add_complete_pass(db, n, *, excluded_publications=()):
    identity = helpers.add_folder(db, n, job='finished', claim='waiting_rag', outcome=('passed', 'passed'))
    def add_members(value):
        if excluded_publications:
            value['_synthetic_excluded_publications'] = list(excluded_publications)
        package = PureWindowsPath(value['body']['package_path'])
        for index, name in enumerate(('profile.json', 'tree.json', 'assets.json', 'chunks.json',
                                      'split-plan.json', 'source-map.json', 'assets/figure.png'), 1):
            value['artifacts'].append({'path': str(package / name), 'sha256': format(index, 'x') * 64})
        manifest, ready = synthetic_seals(value)
        hashes = {'manifest.json': hashlib.sha256(manifest.encode('utf8')).hexdigest(),
                  'ready.json': hashlib.sha256(ready.encode('utf8')).hexdigest()}
        value['body']['manifest_sha256'] = hashes['manifest.json']
        for row in value['artifacts']:
            name = PureWindowsPath(row['path']).name.casefold()
            if name in hashes:
                row['sha256'] = hashes[name]
    helpers.set_receipt_attachments(db, [], group_id=identity['group_id'], mutate=add_members)
    return identity


def make_inspector(db, *, tamper=None, on_inspect=None, max_rows=100):
    server = {'server_id': 'synthetic-server', 'ledger_id': str(uuid4())}
    gate = lambda _: nullcontext()
    journal = RemoteSubmissionJournal(db.path.parent / ('synthetic-journal-' + str(uuid4()) + '.sqlite3'),
                                      control_database=db.path, admission_gate=gate)
    journal.initialize()
    calls = []

    def intent(job_id):
        with sqlite3.connect(db.path) as conn:
            rows = conn.execute('SELECT receipt_json FROM postprocessing_receipts').fetchall()
        for (raw,) in rows:
            value = json.loads(raw)
            identity = value['identity']
            if str(uuid5(NAMESPACE, identity['task_run_id'])) == job_id:
                return {'server_identity': copy.deepcopy(server), 'job': {
                    'job_id': job_id, 'identity': identity, 'claim_id': value['claim_id'],
                    'manifest_sha256': value['manifest_sha256'], 'code_sha256': 'a' * 64}}
        return None

    journal.get = intent

    def inspect_batch(requests):
        calls.append(copy.deepcopy(requests))
        if tamper == 'offline':
            raise TimeoutError('synthetic connection unavailable')
        output = []
        for request in requests:
            receipt = json.loads(request['receipt_json'])
            manifest, ready = synthetic_seals(receipt)
            seal_sizes = {'manifest.json': len(manifest.encode('utf8')), 'ready.json': len(ready.encode('utf8'))}
            files = [dict(path=row['path'], linux_path='/synthetic/output/' + str(i),
                          sha256=row['sha256'], bytes=seal_sizes.get(PureWindowsPath(row['path']).name.casefold(), 17))
                     for i, row in enumerate(receipt['artifacts'])]
            by_path = {str(PureWindowsPath(row['path'])).casefold(): row for row in files}
            package_path = PureWindowsPath(receipt['body']['package_path'])
            package = []
            for row in files:
                try:
                    relative = PureWindowsPath(row['path']).relative_to(package_path)
                except ValueError:
                    continue
                package.append(dict(relative_path=PurePosixPath(*relative.parts).as_posix(), **row))
            attachments = []
            for item in receipt['attachments']:
                attachments.append(dict(attachment_id=item['attachment_id'],
                    source=copy.deepcopy(by_path[str(PureWindowsPath(item['source_path'])).casefold()]),
                    markdown=copy.deepcopy(by_path[str(PureWindowsPath(item['md_path'])).casefold()])))
            mapping = dict(schema='paperflow-linux-artifact-map/1', integrity_status='passed',
                job=copy.deepcopy(request['job']), identity=copy.deepcopy(request['job']['identity']),
                receipt_canonical_sha256=request['receipt_sha256'], files=files, body_package=package,
                body_manifest_json=manifest, body_ready_json=ready,
                body_publications=[{key: row[key] for key in ('path', 'linux_path', 'sha256', 'bytes')}
                                   for row in package if row['relative_path'] in
                                   json.loads(manifest)['publication_files']],
                attachments=attachments, rag_admission='not_granted',
                semantic_validation='depends_on_reviewed_business_processor',
                privacy_validation='depends_on_reviewed_business_processor',
                worker_termination='caller_must_verify', original_receipt_rewritten=False)
            value = dict(schema='paperflow-authenticated-remote-result/1',
                server_identity=copy.deepcopy(server), job=copy.deepcopy(request['job']),
                receipt_json=request['receipt_json'], receipt_sha256=request['receipt_sha256'],
                ledger_state='finished', worker_terminated=True, business_validation='processor_reported_pass',
                business_code_sha256=request['job']['code_sha256'], artifact_map=mapping)
            if tamper == 'server': value['server_identity']['server_id'] = 'wrong-server'
            if tamper == 'artifact': value['artifact_map']['files'][0]['sha256'] = '0' * 64
            if tamper == 'missing': value['artifact_map']['body_package'] = []
            if tamper == 'package_omission':
                value['artifact_map']['body_package'] = [row for row in package
                    if row['relative_path'] != 'article.redacted.md']
            if tamper == 'publications_missing': value['artifact_map']['body_publications'] = []
            if tamper == 'publication_asset_omission':
                value['artifact_map']['body_publications'] = [row for row in value['artifact_map']['body_publications']
                    if not row['path'].replace('\\', '/').endswith('/assets/figure.png')]
            output.append(value)
        if on_inspect:
            on_inspect()
        return output

    inspector = RemoteReceiptInspector(journal=journal, server_identity=server,
        inspect_batch=inspect_batch, max_summary_receipts=max_rows)
    inspector.synthetic_stage_store = RemoteFolderStageStore(db.path, observe_run=lambda _: True,
        observe_activity=None, admission_gate=gate, inspect_receipt=inspector)
    return inspector, calls


def observe(db, *, inspector=None, verify_scope=lambda _: True, **kwargs):
    return candidate().snapshot(db.path, ledger_snapshot=helpers.projection.snapshot,
        component_id_fn=helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store if inspector else None, verify_scope=verify_scope, **kwargs)


def test_empty_queue_does_not_hide_changed_original_intent_journal(db):
    inspector, _ = make_inspector(db)
    with sqlite3.connect(inspector.journal.database) as conn:
        conn.execute("UPDATE journal_settings SET value=? WHERE name='journal_id'", (str(uuid4()),))
    probes = []
    value = observe(db, inspector=inspector, probe_server=lambda _: probes.append(True) or True)
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None
    assert probes == []


def test_intent_journal_change_during_probe_invalidates_empty_observation(db):
    inspector, _ = make_inspector(db)
    def probe(_):
        with sqlite3.connect(inspector.journal.database) as conn:
            conn.execute("UPDATE journal_settings SET value=? WHERE name='journal_id'", (str(uuid4()),))
        return True
    value = observe(db, inspector=inspector, probe_server=probe)
    assert value['server_observation']['reason'] == 'journal_recheck_unavailable'
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_multiple_components_still_count_as_one_original_folder(db):
    ident = add_complete_pass(db, 1)
    first = helpers.add_attachment_admission(db, ident, 'attachment-1', 'a' * 64)
    second = helpers.add_attachment_admission(db, ident, 'attachment-2', 'b' * 64)
    helpers.set_receipt_attachments(db, [first, second])
    inspector, calls = make_inspector(db)
    scope = []
    value = observe(db, inspector=inspector, verify_scope=lambda request: scope.append(request) or True)
    assert value['server_observation']['available'] is True
    assert value['stage']['counts']['waiting_rag_upload'] == 1
    assert value['stage']['counts']['postprocessing_in_progress'] is None
    assert value['results']['counts']['passed'] == 1
    assert value['results']['counts']['failed'] is None
    assert value['current_delivery_verified'] is False
    assert len(calls) == 1 and len(calls[0]) == 1
    assert scope[0]['folders'][0]['identity'] == ident


def test_receipt_and_mapping_both_omitting_required_package_files_are_unknown(db):
    # The inherited fixture intentionally has only article/manifest/READY.
    helpers.add_folder(db, 1, job='finished', claim='waiting_rag', outcome=('passed', 'passed'))
    inspector, _ = make_inspector(db)
    value = observe(db, inspector=inspector)
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_publication_mapping_follows_the_sealed_manifest_and_keeps_full_package(db):
    add_complete_pass(db, 1, excluded_publications=('assets/figure.png',))
    inspector, _ = make_inspector(db)
    value = observe(db, inspector=inspector)
    # The image still exists in the complete body_package and original receipt.
    # Only its separate publication-list membership follows the sealed manifest.
    assert value['server_observation']['available'] is True
    assert value['stage']['counts']['waiting_rag_upload'] == 1


@pytest.mark.parametrize('case', ['missing_inspector', 'offline', 'server', 'artifact', 'missing',
                                  'package_omission', 'publications_missing', 'publication_asset_omission', 'scope'])
def test_unverified_server_or_source_never_counts_as_waiting_rag(db, case):
    add_complete_pass(db, 1)
    inspector, _ = make_inspector(db, tamper=case)
    value = observe(db, inspector=None if case == 'missing_inspector' else inspector,
                    verify_scope=(lambda _: False) if case == 'scope' else (lambda _: True))
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None
    assert value['stage']['counts']['postprocessing_in_progress'] is None
    assert value['results']['counts']['passed'] is None
    assert value['current_delivery_verified'] is False


def test_empty_ledger_cannot_infer_server_or_source_health(db):
    inspector, _ = make_inspector(db)
    value = observe(db, inspector=inspector, verify_scope=lambda _: False)
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_empty_ledger_requires_an_actual_server_probe(db):
    inspector, calls = make_inspector(db, tamper='offline')
    value = observe(db, inspector=inspector, verify_scope=lambda _: True)
    assert calls == []  # inspect_many([]) does not contact the server
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_empty_ledger_is_zero_only_after_bounded_server_probe(db):
    inspector, calls = make_inspector(db)
    observed = []
    value = observe(db, inspector=inspector, verify_scope=lambda _: True,
                    probe_server=lambda server: observed.append(copy.deepcopy(server)) or True)
    assert observed == [inspector.server]
    assert calls == []
    assert value['stage']['counts']['waiting_rag_upload'] == 0
    assert value['server_observation']['available'] is True


def test_unverified_source_cannot_claim_waiting_postprocessing_count(db):
    helpers.add_folder(db, 1)
    inspector, _ = make_inspector(db)
    value = observe(db, inspector=inspector, verify_scope=lambda _: False)
    assert value['stage']['counts']['waiting_postprocessing'] is None
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_database_change_during_remote_inspection_discards_observation(db):
    add_complete_pass(db, 1)
    def mutate():
        with sqlite3.connect(db.path) as conn:
            conn.execute("UPDATE tasks SET name='changed during inspection'")
    inspector, _ = make_inspector(db, on_inspect=mutate)
    value = observe(db, inspector=inspector)
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_source_change_during_remote_inspection_discards_observation(db):
    add_complete_pass(db, 1)
    source = {'available': True, 'checks': 0}
    def check(_):
        source['checks'] += 1
        return source['available']
    inspector, _ = make_inspector(db, on_inspect=lambda: source.update(available=False))
    value = observe(db, inspector=inspector, verify_scope=check)
    assert source['checks'] >= 2
    assert value['server_observation']['available'] is False
    assert value['stage']['counts']['waiting_rag_upload'] is None


def test_observation_larger_than_the_inspector_bound_is_unknown(db):
    for n in (1, 2):
        add_complete_pass(db, n)
    inspector, calls = make_inspector(db, max_rows=1)
    value = observe(db, inspector=inspector)
    assert value['stage']['counts']['waiting_rag_upload'] is None
    assert value['results']['counts']['passed'] is None
    assert calls == []


def test_a_slow_observation_expires_from_its_start(db):
    add_complete_pass(db, 1)
    inspector, _ = make_inspector(db)
    projection = candidate()
    values = iter((100.0, 131.0))
    projection.time = type('Clock', (), {'monotonic': staticmethod(lambda: next(values))})
    result = projection.snapshot(db.path, ledger_snapshot=helpers.projection.snapshot,
        component_id_fn=helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, verify_scope=lambda _: True)
    assert result['stage']['counts']['waiting_rag_upload'] is None
    assert result['server_observation']['available'] is False


def test_registered_remote_execution_is_not_server_liveness(db):
    helpers.add_folder(db, 1, job='executing')
    inspector, _ = make_inspector(db)
    value = observe(db, inspector=inspector)
    assert value['stage']['counts']['postprocessing_in_progress'] is None


@pytest.mark.parametrize('state', ['reserved', 'submitted', 'executing'])
def test_unconfirmed_inflight_cannot_be_reported_as_empty_queue(db, state):
    helpers.add_folder(db, 1, job=state)
    inspector, _ = make_inspector(db)
    value = observe(db, inspector=inspector, probe_server=lambda _: True)
    assert value['stage']['counts']['postprocessing_in_progress'] is None
    assert value['stage']['counts']['waiting_postprocessing'] is None
    assert value['stage']['counts']['waiting_rag_upload'] is None
    assert value['stage']['partial'] is True


def test_remote_result_pagination_rejects_changed_ledger(db):
    for n in (1, 2):
        add_complete_pass(db, n)
    inspector, _ = make_inspector(db)
    first = observe(db, inspector=inspector, result_filter='passed', page=1, page_size=1)
    assert first['results']['list']['has_next'] is True
    second = observe(db, inspector=inspector, result_filter='passed', page=2, page_size=1,
                     expected_revision=first['revision'])
    assert second['available'] is True and second['revision'] == first['revision']
    helpers.add_folder(db, 3)
    changed = observe(db, inspector=inspector, result_filter='passed', page=2, page_size=1,
                      expected_revision=first['revision'])
    assert changed['available'] is False and changed['reason'] == 'snapshot_changed'


def test_268_synthetic_pass_receipts_use_bounded_batches(db):
    # This is deliberately not an authoritative source check or real SSH.
    for n in range(1, 269):
        add_complete_pass(db, n)
    inspector, calls = make_inspector(db, max_rows=300)
    # Index the synthetic lookup, mirroring the real journal's indexed job_id
    # lookup instead of timing the fixture's intentionally simple full scan.
    with sqlite3.connect(db.path) as conn:
        receipts = [json.loads(row[0]) for row in conn.execute('SELECT receipt_json FROM postprocessing_receipts')]
    intents = {}
    for value in receipts:
        identity = value['identity']
        job_id = str(uuid5(NAMESPACE, identity['task_run_id']))
        intents[job_id] = {'server_identity': copy.deepcopy(inspector.server), 'job': {
            'job_id': job_id, 'identity': identity, 'claim_id': value['claim_id'],
            'manifest_sha256': value['manifest_sha256'], 'code_sha256': 'a' * 64}}
    inspector.journal.get = intents.get
    started = time.perf_counter()
    value = observe(db, inspector=inspector)
    elapsed = time.perf_counter() - started
    print(f'268 synthetic passing receipts; {len(calls)} synthetic batches; elapsed={elapsed:.3f}s')
    assert value['stage']['counts']['waiting_rag_upload'] == 268
    assert value['results']['counts']['passed'] == 268
    assert len(calls) == 9 and all(len(batch) <= 32 for batch in calls)
