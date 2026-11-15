"""Server-side staging descriptor preserves one job and its actual receipt."""
import io
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

import pytest

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from server_service_descriptor import DescriptorError, create_descriptor
from snapshot_wire import receive_snapshot, snapshot_sha
from test_snapshot_wire import make_manifest


pytestmark = pytest.mark.skipif(os.name != 'posix', reason='Linux private directories required')


def fixture(root):
    staging, jobs = root / 'staging', root / 'jobs'
    staging.mkdir(mode=0o700)
    jobs.mkdir(mode=0o700)
    manifest = make_manifest()
    job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                           'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    receipt = receive_snapshot(staging / job['job_id'], manifest, job['snapshot_sha256'], io.BytesIO(b'{}reply'))
    server = dict(server_id='synthetic-server', ledger_id=str(uuid4()))
    return staging, jobs, job, server, receipt


def create(staging, jobs, job, server, receipt):
    return create_descriptor(job=job, server_identity=server, staging_root=staging,
                             jobs_root=jobs, staging_receipt=receipt)


def test_staged_receipt_becomes_one_nonoverwriting_service_descriptor(tmp_path):
    staging, jobs, job, server, receipt = fixture(tmp_path)
    actual = create(staging, jobs, job, server, receipt)
    directory = jobs / job['job_id']
    descriptor = json.loads((directory / 'STAGING.json').read_bytes())
    assert json.loads((directory / 'job.json').read_bytes()) == job
    assert descriptor == dict(schema='paperflow-service-staging/1', job=job, server_identity=server,
                              snapshot_root=str(staging / job['job_id']), staging_receipt=receipt)
    assert actual['job'] == job and (staging / job['job_id'] / 'INPUT_VERIFIED.json').exists()
    before = (directory / 'STAGING.json').read_bytes()
    with pytest.raises(DescriptorError):
        create(staging, jobs, job, server, receipt)
    assert (directory / 'STAGING.json').read_bytes() == before


@pytest.mark.parametrize('change', ['receipt', 'job', 'marker', 'overlap'])
def test_mismatched_staging_or_roots_never_create_service_job(tmp_path, change):
    staging, jobs, job, server, receipt = fixture(tmp_path)
    if change == 'receipt':
        receipt = dict(receipt, files=receipt['files'] + 1)
    elif change == 'job':
        job = dict(job, claim_id=str(uuid4()))
    elif change == 'marker':
        marker = staging / job['job_id'] / 'INPUT_VERIFIED.json'
        marker.chmod(0o600)
        marker.write_text('{}', encoding='utf8')
        marker.chmod(0o400)
    else:
        jobs = staging / 'nested'
        jobs.mkdir(mode=0o700)
    with pytest.raises(DescriptorError):
        create(staging, jobs, job, server, receipt)
    assert not (jobs / job['job_id']).exists()


def test_job_root_ancestor_swap_cannot_redirect_descriptor(tmp_path, monkeypatch):
    staging, jobs, job, server, receipt = fixture(tmp_path)
    moved = tmp_path / 'moved-jobs'
    other = tmp_path / 'other-jobs'
    other.mkdir(mode=0o700)
    mkdir, swapped = os.mkdir, []
    def replace(path, mode=0o777, *, dir_fd=None):
        if str(path) == job['job_id'] and not swapped:
            swapped.append(True)
            jobs.rename(moved)
            jobs.symlink_to(other, target_is_directory=True)
        return mkdir(path, mode, dir_fd=dir_fd)
    monkeypatch.setattr(os, 'mkdir', replace)
    with pytest.raises(DescriptorError):
        create(staging, jobs, job, server, receipt)
    assert swapped and not (other / job['job_id']).exists()
