import importlib.util
import hashlib
from pathlib import Path

import pytest


SPEC = importlib.util.spec_from_file_location('bootstrap_service_revision',
    Path(__file__).parents[1] / 'bootstrap_service_revision.py')
revision = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(revision)


@pytest.fixture
def roots(tmp_path):
    base = tmp_path / 'runtime'
    old = base / 'old-service'
    app, jobs, staging = (old / name for name in ('app', 'jobs', 'staging'))
    for path in (app, jobs, staging): path.mkdir(parents=True)
    return base, app, jobs, staging


def test_revision_target_is_a_fresh_sibling(roots):
    base, app, jobs, staging = roots
    assert revision.safe_revision_target(base / 'new-revision', base, app, jobs, staging) == base / 'new-revision'


@pytest.mark.parametrize('protected', ['app', 'jobs', 'staging'])
def test_revision_cannot_be_inside_existing_app_or_job_data(roots, protected):
    base, app, jobs, staging = roots
    parent = {'app': app, 'jobs': jobs, 'staging': staging}[protected]
    with pytest.raises(ValueError):
        revision.safe_revision_target(parent / 'new-revision', base, app, jobs, staging)


def test_revision_parent_symlink_is_rejected(roots):
    base, app, jobs, staging = roots
    link = base / 'job-link'
    try:
        link.symlink_to(jobs, target_is_directory=True)
    except OSError:
        pytest.skip('directory symlink creation is unavailable')
    with pytest.raises(ValueError):
        revision.safe_revision_target(link / 'new-revision', base, app, jobs, staging)


def test_revision_source_replacements_are_exactly_named_and_hash_pinned(tmp_path):
    replacements = {}
    for name in ('remote_jobs.py', 'server_job_service.py', 'server_termination_proof.py', 'server_admission.py'):
        path = tmp_path / name
        raw = ('synthetic ' + name).encode()
        path.write_bytes(raw)
        replacements[name] = dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())
    actual = revision.verified_source_replacements(replacements)
    assert set(actual) == set(replacements)
    assert all(actual[name] == Path(spec['path']).read_bytes() for name, spec in replacements.items())
    (tmp_path / 'remote_jobs.py').write_bytes(b'changed')
    with pytest.raises(ValueError):
        revision.verified_source_replacements(replacements)


def test_revision_rejects_incomplete_or_unrelated_source_replacements(tmp_path):
    with pytest.raises(ValueError):
        revision.verified_source_replacements({'remote_jobs.py': {'path': str(tmp_path / 'x'), 'sha256': 'a' * 64}})
    assert revision.verified_source_replacements(None) == {}
