"""Synthetic callback/SSH boundaries; no real SSH, papers, LLM or RAG."""
import copy
import hashlib
import importlib
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent),
               str(Path(__file__).resolve().parents[2] / 'postprocess-prefect-20261031')]
from snapshot_wire import snapshot_sha
from test_snapshot_wire import make_manifest
from test_remote_coordinator import sample


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def module():
    try:
        return importlib.import_module('remote_server_transport')
    except ImportError:
        pytest.fail('the isolated server transport adapter is not implemented')


@pytest.fixture
def setup(tmp_path):
    m = module()
    manifest = make_manifest(); manifest['binary_hash_scope'] = 'server'
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation', 'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    server = dict(server_id='synthetic', ledger_id=str(uuid4()))
    snapshot = dict(snapshot_sha256=job['snapshot_sha256'], code_sha256=job['code_sha256'],
                    server_identity=server, payload=dict(kind='in-process-frozen-manifest/1'))
    config = dict(schema='paperflow-server-transport/1', server_identity=server,
        connection=dict(host='synthetic.invalid', port=22, username='synthetic',
            private_key=str(tmp_path / 'never-read-private-key'), server_key_sha256='1' * 64),
        server_python='/isolated/venv/bin/python', staging_script='/isolated/code/server_staging_cli.py',
        rpc_script='/isolated/code/server_rpc.py', service_config='/isolated/code/server-job-service.json',
        service_config_sha256='2' * 64, staging_root='/isolated/staging', allowed_roots=[str(tmp_path)],
        staging_timeout_seconds=60, metadata_timeout_seconds=5, admit_timeout_seconds=2,
        wait_timeout_seconds=20, poll_seconds=.001)
    records, start, calls = {}, {}, []
    journal = SimpleNamespace(journal_id=str(uuid4()), get_transfer=lambda key: records.get(key), get=lambda key: start.get(key))
    records[job['job_id']] = dict(job=job, snapshot=snapshot, server_identity=server, journal_id=journal.journal_id)
    output = tmp_path / 'artifact'; output.write_bytes(b'synthetic validated output')
    receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=job['identity'], claim_id=job['claim_id'],
        manifest_sha256=job['manifest_sha256'], validation_status='passed',
        body=dict(package_path=str(tmp_path / 'package'), manifest_sha256='f' * 64), attachments=[],
        artifacts=[dict(path=str(output), sha256=hashlib.sha256(output.read_bytes()).hexdigest())])
    raw = encoded(receipt); digest = hashlib.sha256(raw.encode()).hexdigest()
    evidence = dict(schema='paperflow-authenticated-remote-result/1', job=job, server_identity=server,
        receipt_json=raw, receipt_sha256=digest, ledger_state='finished', worker_terminated=True,
        business_validation='processor_reported_pass', business_code_sha256=job['code_sha256'],
        artifact_map=dict(schema='paperflow-linux-artifact-map/1', integrity_status='passed', job=job,
            receipt_canonical_sha256=digest, rag_admission='not_granted'))
    result = dict(job=job, server_identity=server, state='finished', server_validation='passed',
                  receipt=receipt, receipt_json=raw, receipt_sha256=digest, server_receipt=evidence)
    def stage(connection, command, supplied, **kwargs):
        calls.append(('stage', kwargs))
        assert connection == config['connection'] and supplied == manifest
        assert shlex.split(command) == [config['server_python'], '-B', config['staging_script'], config['staging_root']]
        return dict(schema='paperflow-remote-snapshot-verified/1', **job, files=len(manifest['files']),
                    evidence_scope='declared_files_only', dependency_closure_verified=False)
    def rpc(connection, command, payload, **kwargs):
        operation = shlex.split(command)[-1]; calls.append((operation, copy.deepcopy(payload), kwargs))
        assert connection == config['connection']
        assert shlex.split(command)[:-1] == [config['server_python'], '-B', config['rpc_script'],
            config['service_config'], config['service_config_sha256']]
        if operation == 'descriptor': return dict(job=job, server_identity=server, descriptor_sha256='3' * 64)
        if operation == 'preflight': return dict(job=job, server_identity=server, preflight_id=str(uuid4()), preflight_sha256='4' * 64)
        if operation == 'admit': return dict(job=job, server_identity=server, accepted=True)
        if operation == 'status': return dict(job=job, server_identity=server, state='finished', ledger_state='finished',
            receipt_ready=True, worker_liveness='not_independently_observed')
        if operation == 'result': return copy.deepcopy(result)
        if operation == 'inspect': return [copy.deepcopy(evidence) for _ in payload['requests']]
        raise AssertionError(operation)
    def construct(**changes):
        return m.RemoteServerTransport(config, config_sha256=hashlib.sha256(encoded(config).encode()).hexdigest(),
            journal=journal, stage_call=changes.get('stage_call', stage), rpc_call=changes.get('rpc_call', rpc))
    adapter = construct()
    def prepared():
        staged = adapter.transfer_snapshot(job, snapshot, manifest, verify_locally=False)
        frozen = copy.deepcopy(staged)
        assert adapter.validate_staging(job, snapshot, staged) is True
        assert staged == frozen
        start[job['job_id']] = dict(records[job['job_id']], staging=copy.deepcopy(staged))
        return staged
    return locals()


def test_full_chain_preserves_original_ack_and_preflights_before_small_admission(setup):
    x = setup; a = x['adapter']; staged = x['prepared']()
    assert [row[0] for row in x['calls']] == ['stage', 'descriptor', 'preflight']
    ack = a.submit_once(x['job'], x['snapshot'], staged)
    assert ack['accepted'] is True
    result = a.wait_for_result(x['job'])
    assert a.validate_result(x['job'], result) is True
    assert result['receipt_json'] == x['raw']
    assert [row[0] for row in x['calls']] == ['stage', 'descriptor', 'preflight', 'admit', 'status', 'result', 'inspect']
    admit = x['calls'][3]
    assert set(admit[1]) == {'job', 'preflight_id', 'preflight_sha256'}
    assert admit[2]['timeout'] == 2 and admit[2]['max_bytes'] == 65536
    assert set(a.coordinator_callbacks()) == {'transfer_snapshot', 'validate_staging', 'submit_once', 'wait_for_result', 'validate_result'}


def test_missing_or_changed_transfer_intent_prevents_any_upload(setup):
    x = setup; x['records'].clear()
    with pytest.raises(x['m'].TransportError):
        x['adapter'].transfer_snapshot(x['job'], x['snapshot'], x['manifest'], verify_locally=False)
    assert x['calls'] == []


def test_disconnect_is_terminal_for_this_upload_and_not_retried(setup):
    x = setup; sends = []
    def disconnected(*args, **kwargs):
        sends.append(True); raise TimeoutError('synthetic SSH exec did not acknowledge')
    a = x['construct'](stage_call=disconnected)
    for _ in range(2):
        with pytest.raises(x['m'].TransportError):
            a.transfer_snapshot(x['job'], x['snapshot'], x['manifest'], verify_locally=False)
    assert sends == [True] and x['calls'] == []


def test_mismatched_authenticated_descriptor_cannot_reach_preflight_or_admit(setup):
    x = setup
    def changed(connection, command, payload, **kwargs):
        response = x['rpc'](connection, command, payload, **kwargs)
        response['server_identity'] = dict(x['server'], ledger_id=str(uuid4()))
        return response
    a = x['construct'](rpc_call=changed)
    with pytest.raises(x['m'].TransportError):
        a.transfer_snapshot(x['job'], x['snapshot'], x['manifest'], verify_locally=False)
    assert [row[0] for row in x['calls']] == ['stage', 'descriptor']


def test_preflight_cannot_mutate_staging_or_repeat_after_unknown(setup):
    x = setup; a = x['adapter']
    staged = a.transfer_snapshot(x['job'], x['snapshot'], x['manifest'], verify_locally=False)
    staged['staging_receipt']['files'] += 1
    with pytest.raises(x['m'].TransportError):
        a.validate_staging(x['job'], x['snapshot'], staged)
    assert [row[0] for row in x['calls']] == ['stage', 'descriptor']


def test_admit_ack_loss_consumes_context_and_cannot_resubmit_or_recover_by_reconstruction(setup):
    x = setup; staged = x['prepared'](); a = x['adapter']; real = a._rpc_call
    def lost(connection, command, payload, **kwargs):
        real(connection, command, payload, **kwargs)
        raise TimeoutError('synthetic ACK lost after server accepted')
    a._rpc_call = lost
    for instance in (a, a, x['construct']()):
        with pytest.raises(x['m'].TransportError):
            instance.submit_once(x['job'], x['snapshot'], staged)
    assert [row[0] for row in x['calls']].count('admit') == 1


def test_unknown_status_stops_without_result_retry_or_restart(setup):
    x = setup; staged = x['prepared'](); a = x['adapter']; a.submit_once(x['job'], x['snapshot'], staged)
    real = a._rpc_call
    def unknown(connection, command, payload, **kwargs):
        response = real(connection, command, payload, **kwargs)
        response['state'] = 'unknown'; response['receipt_ready'] = False
        return response
    a._rpc_call = unknown
    with pytest.raises(x['m'].TransportError): a.wait_for_result(x['job'])
    assert [row[0] for row in x['calls']].count('status') == 1
    assert not any(row[0] == 'result' for row in x['calls'])


def test_changed_original_receipt_is_not_accepted_as_successful_exit(setup):
    x = setup; staged = x['prepared'](); a = x['adapter']; a.submit_once(x['job'], x['snapshot'], staged)
    x['result']['receipt_sha256'] = '0' * 64
    with pytest.raises(x['m'].TransportError): a.wait_for_result(x['job'])


def test_changed_inspection_rejects_the_previously_fetched_result(setup):
    x = setup; staged = x['prepared'](); a = x['adapter']; a.submit_once(x['job'], x['snapshot'], staged)
    result = a.wait_for_result(x['job'])
    x['evidence']['worker_terminated'] = False
    with pytest.raises(x['m'].TransportError): a.validate_result(x['job'], result)


def test_whole_staging_deadline_kills_a_helper_stuck_before_ack(tmp_path, monkeypatch):
    m = module(); marker = tmp_path / 'late-write'
    helper = tmp_path / 'ssh_stage_process.py'
    helper.write_text('import pathlib,time; time.sleep(1); pathlib.Path(' + repr(str(marker)) + ').write_text("late")')
    monkeypatch.setattr(m, '__file__', str(tmp_path / 'remote_server_transport.py'))
    started = time.monotonic()
    with pytest.raises(m.TransportError):
        m.stage_bounded({}, '/synthetic', {}, allowed_roots=[str(tmp_path)], timeout=.15, verify_locally=False)
    assert time.monotonic() - started < 1.5
    time.sleep(.15)
    assert not marker.exists()


def test_runtime_cannot_expand_exact_file_authority_without_independently_frozen_configuration(setup):
    x = setup
    trusted = [dict(path=str(x['tmp_path'] / 'catalog.json'), sha256='5' * 64)]
    with pytest.raises(x['m'].TransportError):
        x['adapter'].transfer_snapshot(x['job'], x['snapshot'], x['manifest'],
                                       verify_locally=False, trusted_files=trusted)
    assert x['calls'] == []


def test_exact_catalog_authority_crosses_child_boundary_without_authorizing_its_parent(setup):
    x = setup; trusted = [dict(path=str(x['tmp_path'] / 'catalog.json'), sha256='5' * 64)]
    x['config']['allowed_roots'] = [str(x['tmp_path'] / 'papers')]
    x['config']['trusted_files'] = trusted
    a = x['construct']()
    a.transfer_snapshot(x['job'], x['snapshot'], x['manifest'], verify_locally=False, trusted_files=trusted)
    supplied = x['calls'][0][1]
    assert supplied['trusted_files'] == trusted
    assert supplied['allowed_roots'] == [str(x['tmp_path'] / 'papers')]


@pytest.mark.parametrize('failure', ['none', 'upload_ack_lost', 'admit_ack_lost', 'paused_after_upload'])
def test_original_coordinator_gate_and_durable_intents_control_the_actual_callback_chain(sample, setup, failure):
    import sqlite3
    from remote_jobs import NAMESPACE
    from uuid import uuid5
    x = setup; calls, holder = [], {}
    config = copy.deepcopy(x['config']); config['server_identity'] = sample['server']
    def prepare(claim):
        sample['prepare'](claim)
        manifest = make_manifest()
        manifest.update(identity=sample['identity'], claim_id=claim['claim_id'], generation=2,
            job_id=str(uuid5(NAMESPACE, sample['identity']['task_run_id'])), binary_hash_scope='server')
        holder['manifest'] = manifest
        return dict(snapshot_sha256=snapshot_sha(manifest), code_sha256=manifest['code_sha256'], payload={'synthetic': True})
    def stage(connection, command, manifest, **kwargs):
        calls.append(('upload', sample['gate_depth'][0]))
        if failure == 'upload_ack_lost': raise TimeoutError('synthetic transfer ACK lost')
        if failure == 'paused_after_upload':
            with sqlite3.connect(sample['database']) as db:
                db.execute("UPDATE tasks SET desired_state='paused'")
        job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation', 'manifest_sha256', 'code_sha256')}
        return dict(schema='paperflow-remote-snapshot-verified/1', **job, snapshot_sha256=snapshot_sha(manifest),
            files=len(manifest['files']), evidence_scope='declared_files_only', dependency_closure_verified=False)
    def rpc(connection, command, payload, **kwargs):
        op = shlex.split(command)[-1]; calls.append((op, sample['gate_depth'][0]))
        job = payload.get('job')
        if op == 'descriptor': return dict(job=job, server_identity=sample['server'], descriptor_sha256='3' * 64)
        if op == 'preflight': return dict(job=job, server_identity=sample['server'], preflight_id=str(uuid4()), preflight_sha256='4' * 64)
        if op == 'admit':
            assert sample['journal'].get(job['job_id'])['job'] == job
            if failure == 'admit_ack_lost': raise TimeoutError('synthetic server accepted, ACK lost')
            return dict(job=job, server_identity=sample['server'], accepted=True)
        if op == 'status': return dict(job=job, server_identity=sample['server'], state='finished', receipt_ready=True)
        if op == 'result':
            result = sample['wait'](job)
            result['server_receipt'] = dict(x['evidence'], job=job, server_identity=sample['server'],
                receipt_json=result['receipt_json'], receipt_sha256=result['receipt_sha256'],
                business_code_sha256=job['code_sha256'], artifact_map=dict(x['evidence']['artifact_map'], job=job,
                    receipt_canonical_sha256=result['receipt_sha256']))
            holder['result'] = result
            return result
        if op == 'inspect': return [holder['result']['server_receipt']]
        raise AssertionError(op)
    def build():
        return x['m'].RemoteServerTransport(config, config_sha256=hashlib.sha256(encoded(config).encode()).hexdigest(),
            journal=sample['journal'], stage_call=stage, rpc_call=rpc)
    a = build()
    def run(adapter):
        callbacks = adapter.coordinator_callbacks()
        callbacks['transfer_snapshot'] = lambda job, snap: adapter.transfer_snapshot(job, snap, holder['manifest'], verify_locally=False)
        return sample['run'](prepare_snapshot=prepare, **callbacks)
    if failure == 'none':
        assert run(a)['state'] == 'waiting_rag'
    else:
        with pytest.raises((x['m'].TransportError, sample['module'].StageConflict)):
            run(a)
        assert sample['state']() == 'unknown'
    before = copy.deepcopy(calls)
    with pytest.raises(sample['module'].StageConflict): run(build())
    assert calls == before
    assert sum(op == 'upload' for op, _ in calls) == 1
    assert all(depth == (1 if op == 'admit' else 0) for op, depth in calls)
    assert sum(op == 'admit' for op, _ in calls) == (0 if failure in {'upload_ack_lost', 'paused_after_upload'} else 1)


@pytest.mark.parametrize('disconnected', [False, True])
def test_staging_helper_uses_only_pinned_key_auth_and_closes_on_each_outcome(setup, monkeypatch, disconnected):
    import paramiko
    import ssh_stage_process
    from ssh_rpc_process import PinnedHostKey
    x = setup; observed = {}
    class Client:
        def set_missing_host_key_policy(self, policy): observed['policy'] = policy
        def connect(self, host, **kwargs): observed['connection'] = (host, kwargs)
        def close(self): observed['closed'] = True
    client = Client()
    monkeypatch.setattr(paramiko, 'SSHClient', lambda: client)
    trusted = [dict(path=str(x['tmp_path'] / 'catalog.json'), sha256='5' * 64)]
    def stage(actual_client, command, manifest, **kwargs):
        assert actual_client is client and command == '/fixed/staging' and manifest == x['manifest']
        assert kwargs['trusted_files'] == trusted and kwargs['allowed_roots'] == x['config']['allowed_roots']
        assert kwargs['verify_locally'] is False
        if disconnected: raise OSError('synthetic disconnected staging')
        return {'original_ack': True}
    monkeypatch.setattr(ssh_stage_process, 'stage_over_ssh', stage)
    request = dict(connection=x['config']['connection'], io_timeout=2, command='/fixed/staging',
        manifest=x['manifest'], allowed_roots=x['config']['allowed_roots'], trusted_files=trusted, verify_locally=False)
    if disconnected:
        with pytest.raises(OSError): ssh_stage_process.run(request)
    else:
        assert ssh_stage_process.run(request) == {'original_ack': True}
    assert isinstance(observed['policy'], PinnedHostKey)
    assert observed['policy'].expected == x['config']['connection']['server_key_sha256']
    host, options = observed['connection']
    assert host == 'synthetic.invalid' and options['look_for_keys'] is False and options['allow_agent'] is False
    assert options['key_filename'] == x['config']['connection']['private_key'] and 'password' not in options
    assert observed['closed'] is True


def test_unpinned_credentials_and_modified_frozen_config_are_rejected_before_network(setup):
    x = setup; pinned = hashlib.sha256(encoded(x['config']).encode()).hexdigest()
    x['config']['connection']['server_key_sha256'] = ''
    with pytest.raises(x['m'].TransportError):
        x['m'].RemoteServerTransport(x['config'], config_sha256=pinned, journal=x['journal'])
    with pytest.raises(x['m'].TransportError): x['construct']()
    assert x['calls'] == []
