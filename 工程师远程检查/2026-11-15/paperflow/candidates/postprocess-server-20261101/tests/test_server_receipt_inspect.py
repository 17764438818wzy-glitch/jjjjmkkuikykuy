"""Synthetic Linux snapshot integrity tests; no network or business dispatch."""
import copy
import hashlib
import importlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import sys
from uuid import uuid4, uuid5

import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from remote_jobs import NAMESPACE
from snapshot_wire import snapshot_sha


def sha(raw): return hashlib.sha256(raw).hexdigest()
def canonical(value): return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
def hashed(value): return sha(canonical(value))


def case(root, *, output_root=r'F:\out'):
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic-paper',
        receipt_id='b' * 64, attempt_id='synthetic', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    root.mkdir(mode=0o700)
    files = []
    def save(name, value, *, source=False):
        name = str(name); raw = value if isinstance(value, bytes) else canonical(value)
        physical = root / 'drives' / 'f' / Path(*PureWindowsPath(name).parts[1:])
        physical.parent.mkdir(parents=True, exist_ok=True); physical.write_bytes(raw)
        ref = dict(path=name, sha256=sha(raw))
        if source: files.append(dict(ref, bytes=len(raw)))
        return ref
    source = save(r'F:\input\body.docx', b'synthetic body', source=True)
    response = save(r'F:\input\model\response.txt', b'synthetic saved response', source=True)
    attachment_source = save(r'F:\input\archive.zip', b'synthetic archive', source=True)
    input_receipt = save(r'F:\input\receipt.json', dict(schema='paperflow-receipt/1',
        **{k:identity[k] for k in ('group_id','paper_id','receipt_id')}, inventory=dict(
            body=dict(source, file_id='BODY_one'), attachments=[dict(attachment_source, file_id='ATT_one')])), source=True)
    inputs = dict(identity=identity, manifest_sha256='c' * 64,
        body=dict(schema='body-postprocess-input/1', identity=identity, source=source, receipt=input_receipt, responses=[dict(response=response)]),
        attachments=[dict(schema='attachment-postprocess-input/1', ticket=identity, attachment_id='ATT_one',
            receipt_ref=input_receipt, model_refs=dict(response=response))])
    manifest = dict(schema='paperflow-remote-snapshot/1', identity=identity, claim_id=str(uuid4()),
        job_id=str(uuid5(NAMESPACE, identity['task_run_id'])), generation=1, manifest_sha256='c' * 64,
        code_sha256='d' * 64, inputs=inputs, files=files,
        paperflow_root_windows=r'F:\program\release\core',
        catalog=dict(schema='specialty-routing/1.0', majors=[dict(code='synthetic', name='Synthetic')]))
    job = {k: manifest[k] for k in ('identity','claim_id','job_id','generation','manifest_sha256','code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    output = PureWindowsPath(output_root)
    folder = output / hashed(identity)
    package = folder / 'body/publication'
    task = dict(task_id='PRE_synthetic', source_id='synthetic', source_sha256=source['sha256'], revision=1)
    member = save(package / 'article.redacted.md', b'synthetic MD')
    manifest_body = dict(schema='paperflow/1.0', **{k:v for k,v in task.items() if k != 'revision'}, revision='r001',
        files=[dict(path='article.redacted.md', sha256=member['sha256'], bytes=12)], publication_files=['article.redacted.md'])
    body_manifest = save(package / 'manifest.json', manifest_body)
    ready = save(package / 'READY.json', dict(status='ready', manifest='manifest.json', manifest_sha256=body_manifest['sha256'],
        **{k:v for k,v in task.items() if k != 'revision'}))
    private = save(folder / 'body/private/candidate.json', {'synthetic': True})
    body_result = save(folder / 'body/body-result.json', dict(schema='body-postprocess-result/1', status='body_package_verified',
        identity=identity, package_path=str(package), task=task, manifest_sha256=body_manifest['sha256'],
        input_fingerprint=hashed(dict(evidence=inputs['body'], catalog=manifest['catalog'],
            paperflow_root=manifest['paperflow_root_windows'])),
        private_artifacts=dict(candidate=private), original_responses_retained=True, model_requests_added=0,
        whole_paper_semantic_synthesis=False, redaction_policy='best_effort'))
    attachment_dir = folder / 'attachments' / hashed('ATT_one')
    md = save(attachment_dir / 'cards/version/attachment.md', b'attachment MD')
    delivery = dict(paper_id=identity['paper_id'], attachment_id='ATT_one', source_path=attachment_source['path'],
        source_sha256=attachment_source['sha256'], md_path=md['path'], md_sha256=md['sha256'], delivery_version='version')
    description = save(attachment_dir / 'description.json', {'kind': 'synthetic'})
    delivery_file = save(attachment_dir / 'cards/version/delivery.json', delivery)
    attachment_result = save(attachment_dir / 'receipt.json', dict(schema='attachment-postprocess-result/1', status='complete',
        ticket=identity, delivery=delivery, raw_response=response,
        binding=dict(schema='attachment-postprocess-binding/1', ticket=identity, handoff=inputs['attachments'][0],
            attachment_id='ATT_one', source_path=attachment_source['path'], source_sha256=attachment_source['sha256']),
        artifacts=dict(description=description, md=md, delivery=delivery_file), model_requests_added=0,
        privacy_redaction='not_performed'))
    receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=identity, claim_id=job['claim_id'],
        manifest_sha256=job['manifest_sha256'], validation_status='passed',
        body=dict(package_path=str(package), manifest_sha256=body_manifest['sha256']), attachments=[delivery],
        artifacts=[body_result, body_manifest, ready, member, attachment_result, attachment_source, description, md, delivery_file])
    return dict(job=job, manifest=manifest, receipt=receipt, snapshot_root=root, output_root_windows=str(output)), save


def inspect(args):
    return importlib.import_module('server_receipt_inspect').inspect_receipt(**args)


def add_package_members(args, save, members):
    """Add synthetic members and reseal their enclosing synthetic references."""
    package = PureWindowsPath(args['receipt']['body']['package_path'])
    root = args['snapshot_root'] / 'drives/f' / Path(*package.parts[1:])
    manifest = json.loads((root / 'manifest.json').read_bytes())
    for relative, content in members:
        reference = save(package.joinpath(*PurePosixPath(relative).parts), content)
        manifest['files'].append(dict(path=relative, sha256=reference['sha256'], bytes=len(content)))
        if relative != 'split-plan.json':
            manifest['publication_files'].append(relative)
        args['receipt']['artifacts'].append(reference)
    manifest_ref = save(package / 'manifest.json', manifest)
    args['receipt']['artifacts'][1] = manifest_ref
    args['receipt']['body']['manifest_sha256'] = manifest_ref['sha256']
    ready = json.loads((root / 'READY.json').read_bytes())
    ready['manifest_sha256'] = manifest_ref['sha256']
    args['receipt']['artifacts'][2] = save(package / 'READY.json', ready)
    body_path = package.parent / 'body-result.json'
    body = json.loads((root.parent / 'body-result.json').read_bytes())
    body['manifest_sha256'] = manifest_ref['sha256']
    args['receipt']['artifacts'][0] = save(body_path, body)


@pytest.fixture
def linux_case(tmp_path):
    if os.name != 'posix': pytest.skip('Linux no-follow inspector required')
    return case(tmp_path / 'snapshot')


def test_same_job_and_input_identity_are_required_before_io(tmp_path):
    args, _ = case(tmp_path / 'snapshot')
    args['receipt']['claim_id'] = str(uuid4())
    with pytest.raises(ValueError, match='identity'): inspect(args)


@pytest.mark.parametrize('root', [r'C:\out', 'F:\\', r'F:\input', r'F:\out\..\escape', r'\\host\share\out'])
def test_invalid_or_overlapping_windows_output_root_rejected(tmp_path, root):
    args, _ = case(tmp_path / 'snapshot'); args['output_root_windows'] = root
    with pytest.raises(ValueError): inspect(args)


def test_all_files_and_publications_map_to_real_linux_paths_without_receipt_rewrite(linux_case):
    args, _ = linux_case; before = copy.deepcopy(args['receipt'])
    result = inspect(args)
    assert result['integrity_status'] == 'passed'
    assert result['identity'] == args['job']['identity']
    assert args['receipt'] == before
    assert result['rag_admission'] == 'not_granted'
    assert result['semantic_validation'] == 'depends_on_reviewed_business_processor'
    assert result['privacy_validation'] == 'depends_on_reviewed_business_processor'
    for row in result['files']:
        assert row['linux_path'].startswith(str(args['snapshot_root']) + '/')
        assert sha(Path(row['linux_path']).read_bytes()) == row['sha256']
    assert len(result['body_publications']) == 1
    assert result['attachments'][0]['attachment_id'] == 'ATT_one'


def test_original_body_seals_are_returned_as_exact_hash_bound_utf8_metadata(linux_case):
    args, _ = linux_case
    result = inspect(args)
    raw_manifest = result['body_manifest_json'].encode('utf8')
    raw_ready = result['body_ready_json'].encode('utf8')
    assert sha(raw_manifest) == args['receipt']['body']['manifest_sha256']
    ready_ref = next(ref for ref in args['receipt']['artifacts'] if ref['path'].endswith('READY.json'))
    assert sha(raw_ready) == ready_ref['sha256']
    assert json.loads(raw_manifest)['publication_files'] == ['article.redacted.md']
    assert json.loads(raw_ready)['manifest_sha256'] == sha(raw_manifest)


def test_body_package_lists_every_member_and_seal_without_private_or_inputs(linux_case):
    args, save = linux_case
    add_package_members(args, save, [
        ('split-plan.json', b'synthetic split plan'),
        ('assets/figure.png', b'synthetic image'),
    ])
    result = inspect(args)
    package = result['body_package']
    assert [entry['relative_path'] for entry in package] == [
        'article.redacted.md', 'split-plan.json', 'assets/figure.png', 'manifest.json', 'READY.json']
    assert [PureWindowsPath(entry['path']).relative_to(
        PureWindowsPath(args['receipt']['body']['package_path'])).as_posix()
        for entry in result['body_publications']] == ['article.redacted.md', 'assets/figure.png']
    package_root = args['snapshot_root'] / 'drives/f' / Path(
        *PureWindowsPath(args['receipt']['body']['package_path']).parts[1:])
    for entry in package:
        expected = package_root.joinpath(*PurePosixPath(entry['relative_path']).parts)
        content = expected.read_bytes()
        assert entry['linux_path'] == str(expected)
        assert entry['path'] == str(PureWindowsPath(args['receipt']['body']['package_path']).joinpath(
            *PurePosixPath(entry['relative_path']).parts))
        assert entry['bytes'] == len(content)
        assert entry['sha256'] == hashlib.sha256(content).hexdigest()
    assert result['rag_admission'] == 'not_granted'
    assert result['privacy_validation'] == 'depends_on_reviewed_business_processor'


def test_package_rejects_windows_file_directory_collision(linux_case):
    args, save = linux_case
    add_package_members(args, save, [
        ('collision', b'synthetic file'),
        ('Collision/child.json', b'synthetic child'),
    ])
    with pytest.raises(ValueError, match='conflict'):
        inspect(args)


def test_private_output_participates_in_windows_collision_check(linux_case):
    args, save = linux_case
    folder = PureWindowsPath(args['output_root_windows']) / hashed(args['job']['identity'])
    body_path = folder / 'body/body-result.json'
    physical = args['snapshot_root'] / 'drives/f' / Path(*body_path.parts[1:])
    body = json.loads(physical.read_bytes())
    body['private_artifacts']['conflicting'] = save(folder / 'body/private/CANDIDATE.json/child.json', b'{}')
    args['receipt']['artifacts'][0] = save(body_path, body)
    with pytest.raises(ValueError, match='conflict'):
        inspect(args)


def test_input_and_output_share_one_windows_collision_check(tmp_path):
    if os.name != 'posix': pytest.skip('Linux no-follow inspector required')
    args, save = case(tmp_path / 'snapshot', output_root=r'F:\out\pub')
    save(r'F:\OUT', b'synthetic input', source=True)
    args['job']['snapshot_sha256'] = snapshot_sha(args['manifest'])
    with pytest.raises(ValueError, match='conflict'):
        inspect(args)


@pytest.mark.parametrize('change', ['input', 'output', 'missing', 'ready', 'private'])
def test_missing_or_changed_bytes_cannot_pass(linux_case, change):
    args, _ = linux_case
    if change == 'input': ref = args['manifest']['files'][0]
    elif change == 'output': ref = args['receipt']['artifacts'][3]
    elif change == 'ready': ref = args['receipt']['artifacts'][2]
    elif change == 'private':
        folder = PureWindowsPath(args['output_root_windows']) / hashed(args['job']['identity'])
        ref = dict(path=str(folder / 'body/private/candidate.json'))
    else: ref = args['receipt']['artifacts'][0]
    physical = args['snapshot_root'] / 'drives/f' / Path(*PureWindowsPath(ref['path']).parts[1:])
    if change == 'missing': physical.unlink()
    else: physical.write_bytes(b'tampered')
    with pytest.raises(ValueError): inspect(args)


@pytest.mark.parametrize('change', ['missing_attachment', 'duplicate_artifact', 'duplicate_input', 'unlisted_input', 'foreign_job_output'])
def test_incomplete_or_foreign_coverage_fails(linux_case, change):
    args, save = linux_case
    if change == 'missing_attachment': args['receipt']['attachments'] = []
    elif change == 'duplicate_artifact': args['receipt']['artifacts'].append(copy.deepcopy(args['receipt']['artifacts'][0]))
    elif change == 'duplicate_input':
        args['manifest']['files'].append(copy.deepcopy(args['manifest']['files'][0]))
        args['job']['snapshot_sha256'] = snapshot_sha(args['manifest'])
    elif change == 'unlisted_input':
        args['manifest']['inputs']['body']['source']['path'] = r'F:\input\unregistered.docx'
        args['job']['snapshot_sha256'] = snapshot_sha(args['manifest'])
    else:
        args['receipt']['artifacts'].append(save(r'F:\out\other-job\secret.txt', b'foreign'))
    with pytest.raises(ValueError): inspect(args)


@pytest.mark.parametrize('kind', ['symlink', 'hardlink', 'directory_symlink'])
def test_links_are_never_used_as_result_evidence(linux_case, kind):
    args, _ = linux_case
    path = args['snapshot_root'] / 'drives/f' / Path(*PureWindowsPath(args['receipt']['artifacts'][3]['path']).parts[1:])
    if kind == 'hardlink': os.link(path, path.with_name('alias.md'))
    elif kind == 'symlink':
        original = path.with_name('saved.md'); path.rename(original); path.symlink_to(original)
    else:
        original = path.parent.with_name('saved-publication'); path.parent.rename(original); path.parent.symlink_to(original, target_is_directory=True)
    with pytest.raises(ValueError): inspect(args)


def test_manifest_and_ready_cannot_be_self_resealed_to_another_task(linux_case):
    args, save = linux_case
    package = PureWindowsPath(args['receipt']['body']['package_path'])
    original = args['snapshot_root'] / 'drives/f' / Path(*package.parts[1:]) / 'READY.json'
    ready = json.loads(original.read_bytes()); ready['task_id'] = 'another-task'
    changed = save(package / 'READY.json', ready)
    args['receipt']['artifacts'][2] = changed
    with pytest.raises(ValueError, match='package'): inspect(args)


def test_fingerprint_uses_actual_body_adapter_context_algorithm(tmp_path):
    args, _ = case(tmp_path / 'snapshot')
    manifest = args['manifest']
    module = importlib.import_module('server_receipt_inspect')
    assert hasattr(module, '_body_input_fingerprint'), 'missing actual body adapter fingerprint calculation'
    expected = hashed(dict(evidence=manifest['inputs']['body'], catalog=manifest['catalog'],
        paperflow_root=manifest['paperflow_root_windows']))
    assert module._body_input_fingerprint(manifest) == expected
    assert expected != hashed(manifest['inputs']['body'])


@pytest.mark.parametrize('field', ['catalog', 'paperflow_root_windows'])
def test_missing_frozen_processor_context_is_rejected_before_file_io(tmp_path, field):
    args, _ = case(tmp_path / 'snapshot')
    args['manifest'].pop(field)
    args['job']['snapshot_sha256'] = snapshot_sha(args['manifest'])
    with pytest.raises(ValueError, match='processor context'): inspect(args)


@pytest.mark.parametrize('root', [
    r'C:\program\core', r'F:program\core', r'F:\program\..\core', r'F:\program\.\core',
    'F:/program/core', r'f:\program\core', 'F:\\program\\core\\',
    r'\\host\share\core', r'F:\program\desktop',
])
def test_noncanonical_frozen_core_path_is_rejected_before_file_io(tmp_path, root):
    args, _ = case(tmp_path / 'snapshot')
    args['manifest']['paperflow_root_windows'] = root
    args['job']['snapshot_sha256'] = snapshot_sha(args['manifest'])
    with pytest.raises(ValueError, match='processor context'): inspect(args)


@pytest.mark.parametrize('field', ['catalog', 'paperflow_root_windows'])
def test_processor_context_cannot_change_under_original_snapshot_hash(tmp_path, field):
    args, _ = case(tmp_path / 'snapshot')
    if field == 'catalog': args['manifest'][field]['majors'][0]['name'] = 'Changed'
    else: args['manifest'][field] = r'F:\another\core'
    with pytest.raises(ValueError, match='manifest identity'): inspect(args)


@pytest.mark.parametrize('field', ['catalog', 'paperflow_root_windows'])
def test_resealed_context_still_cannot_match_original_body_result(linux_case, field):
    args, _ = linux_case
    if field == 'catalog': args['manifest'][field]['majors'][0]['name'] = 'Changed'
    else: args['manifest'][field] = r'F:\another\core'
    args['job']['snapshot_sha256'] = snapshot_sha(args['manifest'])
    with pytest.raises(ValueError, match='body result identity'): inspect(args)
