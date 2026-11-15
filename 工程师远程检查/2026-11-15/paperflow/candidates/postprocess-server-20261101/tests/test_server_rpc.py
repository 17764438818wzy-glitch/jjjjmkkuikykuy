"""One bounded metadata request; no real service, LLM or RAG invocation."""
import json
import hashlib
from pathlib import Path
import sqlite3
import sys
import time
from uuid import uuid4

import pytest

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
import server_rpc
from remote_jobs import RemoteJobs, _validated
from server_receipt_query import ReceiptQueryError, _finished
from ssh_rpc_client import RemoteCallError, call_once, _run_bounded
from ssh_rpc_process import PinnedHostKey
from snapshot_wire import snapshot_sha
from test_snapshot_wire import make_manifest


def test_rpc_dispatches_only_explicit_operation_and_rejects_extra_fields(monkeypatch):
    calls = []
    monkeypatch.setattr(server_rpc.service, 'preflight_job', lambda job, cfg: calls.append(job) or {'ready': True})
    assert server_rpc.execute('preflight', {'job': {'synthetic': 1}}, {}) == {'ready': True}
    assert calls == [{'synthetic': 1}]
    with pytest.raises(ValueError):
        server_rpc.execute('preflight', {'job': {}, 'unexpected': 1}, {})
    with pytest.raises(ValueError):
        server_rpc.execute('upload-rag', {}, {})
    assert len(calls) == 1


def test_admit_rpc_preserves_keyword_only_preflight_binding(monkeypatch):
    calls = []
    def admit(job, config, *, preflight_id, preflight_sha256):
        calls.append((job, config, preflight_id, preflight_sha256))
        return {'accepted': True}
    monkeypatch.setattr(server_rpc.service, 'admit_job', admit)
    request = {'job': {'synthetic': 1}, 'preflight_id': 'id', 'preflight_sha256': 'a' * 64}
    assert server_rpc.execute('admit', request, {'synthetic': 2}) == {'accepted': True}
    assert calls == [({'synthetic': 1}, {'synthetic': 2}, 'id', 'a' * 64)]


def test_result_reads_original_held_receipt_then_revalidates_before_returning(monkeypatch):
    manifest = make_manifest()
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                          'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=job['identity'],
                   claim_id=job['claim_id'], manifest_sha256=job['manifest_sha256'], validation_status='passed')
    raw = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(raw.encode()).hexdigest()
    config = dict(server_identity=dict(server_id='synthetic', ledger_id=str(uuid4())))
    ledger, calls = object(), []
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda _: ledger)
    def held(actual_ledger, actual_job, actual_config):
        assert actual_ledger is ledger and actual_job == job and actual_config == config
        calls.append('held'); return raw, digest, receipt
    def inspect(actual_config, requests):
        assert calls == ['held'] and actual_config == config
        assert requests == [dict(job=job, server_identity=config['server_identity'],
                                 receipt_json=raw, receipt_sha256=digest)]
        calls.append('inspect')
        return [dict(schema='paperflow-authenticated-remote-result/1', job=job,
            server_identity=config['server_identity'], receipt_json=raw, receipt_sha256=digest,
            ledger_state='finished', worker_terminated=True, business_validation='processor_reported_pass',
            business_code_sha256=job['code_sha256'], artifact_map={'synthetic': True})]
    monkeypatch.setattr(server_rpc, '_finished', held)
    monkeypatch.setattr(server_rpc, 'inspect_requests', inspect)
    result = server_rpc.execute('result', {'job': job}, config)
    assert calls == ['held', 'inspect']
    assert result['receipt'] == receipt and result['receipt_json'] == raw and result['receipt_sha256'] == digest
    assert result['server_receipt']['worker_terminated'] is True and result['server_validation'] == 'passed'
    assert result['job'] == job and result['server_identity'] == config['server_identity']


def test_result_never_returns_when_finished_scope_evidence_is_unknown(monkeypatch):
    job = {'synthetic': 1}
    monkeypatch.setattr(server_rpc, '_validated', lambda _: None)
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda _: None)
    def unknown(*args):
        raise ReceiptQueryError('synthetic live or unknown scope')
    monkeypatch.setattr(server_rpc, '_finished', unknown)
    monkeypatch.setattr(server_rpc, 'inspect_requests', lambda *args: pytest.fail('inspected without finished proof'))
    with pytest.raises(ReceiptQueryError):
        server_rpc.execute('result', {'job': job}, {})


def test_finished_ledger_waits_for_service_receipt_proof(tmp_path, monkeypatch):
    path = tmp_path / 'status.sqlite'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE service_jobs(job_id TEXT, phase TEXT)')
        db.execute("INSERT INTO service_jobs VALUES('job', 'running')")
    job = dict(job_id='job', claim_id='claim', identity=dict(task_id='task', group_id='group', task_run_id='run'))
    class Ledger:
        def status(self, _):
            return dict(state='finished', frozen_json='frozen', claim_id='claim',
                        task_id='task', group_id='group', task_run_id='run')
        def _connect(self):
            db = sqlite3.connect(path); db.row_factory = sqlite3.Row; return db
    ledger, inspected = Ledger(), []
    monkeypatch.setattr(server_rpc, '_validated', lambda _: 'frozen')
    monkeypatch.setattr(server_rpc.service, '_ledger', lambda _: ledger)
    monkeypatch.setattr(server_rpc, '_finished', lambda *args: inspected.append(args))
    config = {'server_identity': {'server_id': 'synthetic'}}
    request = {'job': job}
    pending = server_rpc.execute('status', request, config)
    assert pending['state'] == 'finishing' and pending['receipt_ready'] is False and not inspected
    with sqlite3.connect(path) as db: db.execute("UPDATE service_jobs SET phase='finished'")
    done = server_rpc.execute('status', request, config)
    assert done['state'] == 'finished' and done['receipt_ready'] is True and len(inspected) == 1


class Channel:
    def __init__(self, *, fail=False):
        self.fail, self.sent, self.closed = fail, [], False
        self.response = b'{"accepted":true}'
    def settimeout(self, value): self.timeout = value
    def exec_command(self, command): self.command = command
    def sendall(self, data):
        self.sent.append(data)
        if self.fail: raise OSError('synthetic disconnected transfer')
    def shutdown_write(self): pass
    def exit_status_ready(self): return True
    def recv_ready(self): return bool(self.response)
    def recv_stderr_ready(self): return False
    def recv(self, size):
        data, self.response = self.response[:size], self.response[size:]
        return data
    def recv_exit_status(self): return 0
    def close(self): self.closed = True


class Transport:
    def __init__(self, channel): self.channel, self.opens = channel, 0
    def is_active(self): return True
    def is_authenticated(self): return True
    def open_session(self, timeout): self.opens += 1; return self.channel


class Client:
    def __init__(self, transport): self.transport = transport
    def get_transport(self): return self.transport


def test_ssh_metadata_call_is_once_only_on_success_and_disconnect():
    channel = Channel()
    transport = Transport(channel)
    result = call_once(Client(transport), '/fixed/server-rpc', {'test': '合成'}, io_timeout=2)
    assert result == {'accepted': True} and transport.opens == 1 and channel.closed
    assert json.loads(channel.sent[0]) == {'test': '合成'}
    lost = Channel(fail=True)
    transport = Transport(lost)
    with pytest.raises(RemoteCallError, match='unknown'):
        call_once(Client(transport), '/fixed/server-rpc', {'test': 1}, io_timeout=2)
    assert transport.opens == 1 and len(lost.sent) == 1 and lost.closed


def test_whole_call_deadline_kills_helper_before_it_can_send_late(tmp_path):
    late = tmp_path / 'must-not-be-created'
    script = "import pathlib,sys,time; time.sleep(1); pathlib.Path(sys.argv[1]).write_text('late')"
    before = time.monotonic()
    with pytest.raises(RemoteCallError, match='unknown'):
        _run_bounded([sys.executable, '-c', script, str(late)], {}, timeout=.15, max_bytes=1024)
    assert time.monotonic() - before < 1.5
    time.sleep(.15)
    assert not late.exists()


def test_explicit_host_key_pin_rejects_a_different_server():
    import paramiko
    class Key:
        def __init__(self, data): self.data = data
        def asbytes(self): return self.data
    policy = PinnedHostKey(hashlib.sha256(b'expected-key').hexdigest())
    assert policy.missing_host_key(None, 'synthetic', Key(b'expected-key')) is None
    with pytest.raises(paramiko.SSHException):
        policy.missing_host_key(None, 'synthetic', Key(b'different-key'))


@pytest.mark.parametrize('change', ['none', 'unfinished', 'different_receipt', 'live_scope', 'wrong_config', 'wrong_scope', 'wrong_index'])
def test_receipt_query_requires_durable_finished_job_and_empty_scope(tmp_path, change):
    manifest = make_manifest()
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                           'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    ledger = RemoteJobs(tmp_path / 'remote.sqlite', capacity=1)
    ledger.initialize(); ledger.reserve(job)
    receipt = dict(identity=job['identity'], claim_id=job['claim_id'], manifest_sha256=job['manifest_sha256'])
    encode = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    raw = encode(receipt)
    digest = hashlib.sha256(raw.encode('utf8')).hexdigest()
    config = {'synthetic': 1, 'worker_uid': 1000, 'worker_gid': 1000}
    config_sha = hashlib.sha256(encode(config).encode('utf8')).hexdigest()
    boot = str(uuid4())
    scope = dict(populated=False, boot_id=boot, scope_id=[1, 2])
    process = dict(boot_id=boot, pid=23, start_ticks=45)
    launch = dict(parent='/sys/fs/cgroup/paperflow-pp-' + job['job_id'] + '.service',
                  uid=1000, gid=1000, clock_basis='linux-monotonic-boot/1')
    clock = dict(boot_id=boot, started_monotonic=123.0)
    with sqlite3.connect(ledger.database) as db:
        db.execute('CREATE TABLE supervisor_jobs(job_id TEXT, frozen_json TEXT, scope_json TEXT, '
                   'process_json TEXT, receipt_json TEXT, held_sha256 TEXT, config_json TEXT, clock_json TEXT)')
        db.execute('CREATE TABLE service_jobs(job_id TEXT, frozen_json TEXT, config_sha256 TEXT, '
                   'phase TEXT, detail_json TEXT)')
        db.execute('INSERT INTO supervisor_jobs VALUES(?,?,?,?,?,?,?,?)',
                   (job['job_id'], _validated(job), encode(scope), encode(process), raw, digest,
                    encode(launch), encode(clock)))
        cleanup = dict(terminated=change != 'live_scope', scope_removed=True, scope_identity=scope,
            process_identity=process, final_scope=dict(scope, scope_id=[9, 9]) if change == 'wrong_scope' else scope,
            parent=launch['parent'], observed_monotonic=124.0, boot_id=boot)
        db.execute('INSERT INTO service_jobs VALUES(?,?,?,?,?)',
                   (job['job_id'], _validated(job), config_sha, 'finished', encode({'cleanup': cleanup})))
        db.execute('UPDATE executor_jobs SET state=?,receipt_sha256=? WHERE job_id=?',
                   ('running' if change == 'unfinished' else 'finished',
                    '0' * 64 if change == 'different_receipt' else digest, job['job_id']))
        if change == 'wrong_index':
            db.execute("UPDATE executor_jobs SET group_id='different-group' WHERE job_id=?", (job['job_id'],))
    if change == 'wrong_config': config = dict(config, synthetic=2)
    if change == 'none':
        assert _finished(ledger, job, config) == (raw, digest, receipt)
    else:
        with pytest.raises(ReceiptQueryError):
            _finished(ledger, job, config)
