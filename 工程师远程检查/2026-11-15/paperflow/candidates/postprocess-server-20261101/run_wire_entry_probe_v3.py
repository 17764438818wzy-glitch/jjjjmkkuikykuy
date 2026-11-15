"""Exercise the actual Wine business entry on an SSH-staged synthetic folder.

This is not a production dispatcher. It never reads the live control database,
uses a one-off ledger and a dedicated Wine prefix, and calls no LLM or RAG.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import shutil
import sys
import time

from remote_jobs import RemoteJobs, _validated
from server_release import prepare_environment
from server_receipt_inspect import inspect_receipt
from server_supervisor import ServerSupervisor
from snapshot_import import SnapshotImporter
from snapshot_wire import snapshot_sha
from windows_snapshot import _no_links


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    base = Path('/home/wzy/paperflow-postprocess/candidate-20261101')
    if len(sys.argv) != 3 or not re.fullmatch(r'wire-transfer-tests-v[0-9]+', sys.argv[1]):
        raise ValueError('one synthetic staging directory and job ID are required')
    staging, job_id = sys.argv[1:3]
    snapshot = base / staging / job_id
    raw = (snapshot / 'transport-manifest.json').read_bytes()
    manifest = json.loads(raw)
    marker = json.loads((snapshot / 'INPUT_VERIFIED.json').read_bytes())
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                           'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    _validated(job)
    if (job['job_id'] != job_id or marker.get('snapshot_sha256') != job['snapshot_sha256']
            or marker.get('claim_id') != job['claim_id'] or marker.get('evidence_scope') != 'declared_files_only'):
        raise ValueError('staged synthetic identity changed')

    source = base / 'synthetic-real-entry-v3'
    code_manifest = source / 'source-code-manifest.json'
    if sha(code_manifest) != job['code_sha256']:
        raise ValueError('synthetic code manifest changed')
    code_rows = json.loads(code_manifest.read_bytes())
    def check_inputs():
        importer = SnapshotImporter(snapshot, manifest['files'])
        try:
            return importer.verify_complete()
        finally:
            importer.close()
    check_inputs()

    prefix = base / ('wire-prefix-' + staging + '-' + job_id)
    prepared = prepare_environment(job=job, snapshot=snapshot, code_root=source, code_manifest=code_manifest,
                                   template=base / 'wine-test-prefix4', prefix=prefix,
                                   template_manifest=base / 'wine-template-manifest-v1.json')
    if prepared['verified_code_files'] != len(code_rows) or not prepared['template_identity_verified']:
        raise ValueError('synthetic code manifest was not fully checked')
    fdrive = snapshot / 'drives/f'
    with (fdrive / 'wire-manifest.json').open('xb') as out:
        out.write(raw)

    # The service entry itself must be one of the pinned source files.
    entry_rows = [row for row in code_rows if PureWindowsPath(row['source']).name == 'server_wine_entry.py']
    if len(entry_rows) != 1:
        raise ValueError('pinned business entry is ambiguous')
    cgroup = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
    ledger = RemoteJobs(snapshot / 'synthetic-ledger.sqlite', capacity=1)
    ledger.initialize()
    ledger.reserve(job)
    result_file = fdrive / 'wire-result.json'

    def validate(actual, receipt):
        if actual != job or not check_inputs() or not result_file.is_file():
            return False
        value = json.loads(result_file.read_bytes())
        encoded = json.dumps(receipt, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        if (value.get('server_validation') != 'passed' or value.get('receipt') != receipt
                or value.get('receipt_json') != encoded
                or value.get('receipt_sha256') != hashlib.sha256(encoded.encode('utf8')).hexdigest()):
            return False
        if len(receipt.get('attachments', [])) != len(manifest['inputs']['attachments']):
            return False
        mapping = inspect_receipt(job=job, manifest=manifest, receipt=receipt, snapshot_root=snapshot,
                                  output_root_windows=manifest['output_root_windows'])
        if mapping.get('integrity_status') != 'passed' or mapping.get('rag_admission') != 'not_granted':
            return False
        for row in receipt.get('artifacts', []):
            name = PureWindowsPath(row['path'])
            if name.drive.lower() != 'f:' or '..' in name.parts:
                return False
            path = fdrive.joinpath(*name.parts[1:])
            _no_links(path)
            if not path.is_file() or sha(path) != row['sha256']:
                return False
        return bool(receipt.get('artifacts'))

    supervisor = ServerSupervisor(ledger, cgroup_parent=cgroup, worker_uid=os.getuid(), worker_gid=os.getgid(),
                                  receipt_validator=validate)
    supervisor.initialize()
    supervisor.launch(job, [sys.executable, str(base / 'server_wine_worker.py'), str(base), str(snapshot),
                            str(prefix), '160'], timeout_seconds=180, cwd=str(base), env=dict(os.environ))
    deadline = time.monotonic() + 170
    wine_seen, receipt = False, None
    while time.monotonic() < deadline:
        current = supervisor.poll(job, receipt=receipt)
        if current['state'] == 'finished':
            break
        if current['state'] != 'running':
            raise RuntimeError('synthetic execution became unknown')
        for pid in (cgroup / job_id / 'cgroup.procs').read_text().split():
            try:
                if 'wineserver' in Path('/proc', pid, 'comm').read_text().lower():
                    wine_seen = True
            except FileNotFoundError:
                pass
        if current['leader_alive'] is False and receipt is None:
            if not result_file.is_file():
                worker_error = snapshot / 'service-worker.stderr'
                detail = worker_error.read_text(errors='replace')[-1500:] if worker_error.exists() else ''
                raise RuntimeError('synthetic worker exited without a receipt: ' + detail)
            receipt = json.loads(result_file.read_bytes())['receipt']
        time.sleep(.1)
    else:
        raise TimeoutError('synthetic execution did not finish')
    if supervisor.held_receipt(job) is None or ledger.occupied() != 0:
        raise RuntimeError('synthetic durable receipt is unavailable')
    check_inputs()
    for row in code_rows:
        if sha(snapshot.joinpath(*PurePosixPath(row['relative']).parts)) != row['sha256']:
            raise RuntimeError('synthetic code changed during execution')
    print(json.dumps(dict(actual_server_entry_to_receipt='passed', body_parts=len(manifest['inputs']['body']['responses']),
        attachments=len(receipt['attachments']), artifacts=len(receipt['artifacts']),
        original_claim_preserved=receipt['claim_id'] == job['claim_id'],
        code_files_verified=len(code_rows), cgroup_observed_wineserver=wine_seen,
        gpu_inference_requests=0, dependency_closure_scope='supported_synthetic_only')))


if __name__ == '__main__':
    main()
