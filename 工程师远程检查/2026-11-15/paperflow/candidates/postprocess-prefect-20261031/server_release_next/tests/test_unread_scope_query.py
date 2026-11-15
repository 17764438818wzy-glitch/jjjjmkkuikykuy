"""Synthetic receipt-linked evidence; no real paper, server, or model."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path, PureWindowsPath
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest

HERE = Path(__file__).resolve().parents[1]
PLAN = {'schema': 'paperflow-body-input-plan/1', 'mode': 'partitioned', 'part_count': 2,
        'parts': [{'id': 'part-001'}, {'id': 'part-002'}]}


def load():
    path = HERE / 'server_unread_scope_query.py'
    assert path.is_file(), 'the independent unread-scope evidence check is missing'
    spec = importlib.util.spec_from_file_location('synthetic_unread_scope_query', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def evidence():
    root = PureWindowsPath('F:/synthetic/job/body')
    package = root / 'publication'
    identity = {'task_id': 'synthetic-task', 'group_id': 'synthetic-folder'}
    files = {}
    def store(path, value):
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()
        ref = {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}
        files[str(path).casefold()] = raw
        return ref
    empty = {'valid': True, 'compatibility_report': []}
    first = store(root / 'private/part-001-validation.json', empty)
    second = store(root / 'private/part-002-validation.json', empty)
    profile = store(package / 'profile.json', {'migration_boundary': {'global_limits': []}})
    body = store(root / 'body-result.json', {'schema': 'body-postprocess-result/1',
        'status': 'body_package_verified', 'identity': identity, 'package_path': str(package),
        'manifest_sha256': 'a' * 64, 'assembly': {'mode': 'program_merge', 'pointer_maps': [{'part': 1}, {'part': 2}]},
        'private_artifacts': {'part_1_validation': first, 'part_2_validation': second}})
    receipt = {'identity': identity, 'body': {'package_path': str(package), 'manifest_sha256': 'a' * 64},
               'artifacts': [body, profile]}
    def read(ref):
        raw = files[str(PureWindowsPath(ref['path'])).casefold()]
        if hashlib.sha256(raw).hexdigest() != ref['sha256']:
            raise ValueError('synthetic evidence changed')
        return json.loads(raw)
    return receipt, files, read, store


def test_unlocated_unread_must_be_disclosed_in_the_public_profile(evidence):
    receipt, files, read, store = evidence
    module = load()
    assert module.check_related_files(receipt, read, PLAN)['status'] == 'passed'
    body = read(receipt['artifacts'][0])
    path = PureWindowsPath(body['private_artifacts']['part_2_validation']['path'])
    report = {'valid': True, 'compatibility_report': [dict(code='normalized_unlocated_reading_declaration',
        path='/source/reading_scope/unread_refs', original=['unlocated-source'])]}
    body['private_artifacts']['part_2_validation'] = store(path, report)
    receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    result = module.check_related_files(receipt, read, PLAN)
    assert result == {'status': 'review_required', 'reason': 'unlocated_unread_missing_public_limit'}
    profile_path = PureWindowsPath(receipt['artifacts'][1]['path'])
    receipt['artifacts'][1] = store(profile_path, {'migration_boundary': {'global_limits': [module.REQUIRED_LIMIT]}})
    assert module.check_related_files(receipt, read, PLAN)['status'] == 'passed'


def test_unlocated_read_declaration_is_not_mistaken_for_unread(evidence):
    receipt, _, read, store = evidence
    module = load()
    body = read(receipt['artifacts'][0])
    path = PureWindowsPath(body['private_artifacts']['part_1_validation']['path'])
    body['private_artifacts']['part_1_validation'] = store(path, {'valid': True,
        'compatibility_report': [dict(code='normalized_unlocated_reading_declaration',
            path='/source/reading_scope/read_refs', original=['unlocated-source'])]})
    receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    assert module.check_related_files(receipt, read, PLAN)['status'] == 'passed'


def test_recorded_unlocated_unread_is_conservative_even_without_original_detail(evidence):
    receipt, _, read, store = evidence
    module = load()
    body = read(receipt['artifacts'][0])
    path = PureWindowsPath(body['private_artifacts']['part_1_validation']['path'])
    body['private_artifacts']['part_1_validation'] = store(path, {'valid': True,
        'compatibility_report': [dict(code='normalized_unlocated_reading_declaration',
            path='/source/reading_scope/unread_refs', original=[])]})
    receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    assert module.check_related_files(receipt, read, PLAN)['status'] == 'review_required'


def test_two_parts_cannot_reuse_one_validation_report(evidence):
    receipt, _, read, store = evidence
    module = load()
    body = read(receipt['artifacts'][0])
    body['private_artifacts']['part_2_validation'] = copy.deepcopy(body['private_artifacts']['part_1_validation'])
    receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    with pytest.raises(ValueError):
        module.check_related_files(receipt, read, PLAN)


def test_assembly_cannot_hide_a_part_present_in_the_frozen_plan(evidence):
    receipt, _, read, store = evidence
    module = load()
    body = read(receipt['artifacts'][0])
    body['private_artifacts'].pop('part_2_validation')
    body['assembly']['pointer_maps'].pop()
    receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    with pytest.raises(ValueError):
        module.check_related_files(receipt, read, PLAN)


@pytest.mark.parametrize('plan', [None, {}, {'schema': 'paperflow-body-input-plan/1',
    'mode': 'partitioned', 'part_count': 2, 'parts': [{'id': 'part-001'}]}])
def test_missing_or_incomplete_frozen_plan_cannot_produce_a_pass(evidence, plan):
    receipt, _, read, _ = evidence
    with pytest.raises(ValueError):
        load().check_related_files(receipt, read, plan)


def test_single_response_matches_the_frozen_single_plan(evidence):
    receipt, _, read, store = evidence
    body = read(receipt['artifacts'][0])
    body['private_artifacts'].pop('part_2_validation')
    body['assembly'] = {'mode': 'single_saved_response'}
    receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    plan = {'schema': 'paperflow-body-input-plan/1', 'mode': 'single', 'part_count': 1,
            'parts': [{'id': 'part-001'}]}
    assert load().check_related_files(receipt, read, plan)['status'] == 'passed'


def test_server_check_reuses_the_held_receipt_and_reads_only_its_linked_output_evidence(evidence, monkeypatch):
    receipt, files, _, _ = evidence
    module = load()
    raw = json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(raw.encode()).hexdigest()
    job = {'job_id': str(uuid4())}
    server = {'server_id': 'synthetic', 'ledger_id': str(uuid4())}
    read_paths = []
    class Snapshot:
        def __init__(self, root): assert root == '/synthetic/frozen'
        def read(self, path, ref, *, json_value):
            assert json_value == 'raw'
            data = files[str(PureWindowsPath(path)).casefold()]
            assert hashlib.sha256(data).hexdigest() == ref['sha256']
            read_paths.append(path)
            return {}, data.decode()
        def visible(self): pass
        def close(self): pass
    monkeypatch.setitem(sys.modules, 'remote_jobs', SimpleNamespace(_uuid=lambda _: True))
    monkeypatch.setitem(sys.modules, 'server_receipt_inspect', SimpleNamespace(_LinuxSnapshot=Snapshot))
    monkeypatch.setitem(sys.modules, 'server_receipt_query', SimpleNamespace(
        _finished=lambda ledger, actual_job, config: (raw, digest, copy.deepcopy(receipt))))
    monkeypatch.setitem(sys.modules, 'server_job_service', SimpleNamespace(
        _ledger=lambda config: 'synthetic-ledger',
        verify_job=lambda job_id, config: {'job': job, 'snapshot': '/synthetic/frozen',
            'manifest': {'inputs': {'body': {'plan_identity': PLAN}}}}))
    request = dict(job=job, server_identity=server, receipt_json=raw, receipt_sha256=digest, nonce=str(uuid4()))
    result = module.execute({'server_identity': server}, request)
    assert result['status'] == 'passed'
    assert len(read_paths) == 4
    assert all('synthetic' in path for path in read_paths)


@pytest.mark.parametrize('change', ['missing_profile', 'missing_part', 'missing_last_part', 'outside_part', 'changed_report',
                                      'missing_compatibility', 'wrong_body_identity'])
def test_missing_changed_or_unrelated_evidence_never_produces_a_pass(evidence, change):
    receipt, files, read, store = evidence
    module = load()
    body = read(receipt['artifacts'][0])
    if change == 'missing_profile': receipt['artifacts'].pop()
    elif change == 'missing_part': body['private_artifacts'].pop('part_1_validation')
    elif change == 'missing_last_part': body['private_artifacts'].pop('part_2_validation')
    elif change == 'outside_part': body['private_artifacts']['part_1_validation']['path'] = 'F:/other/foreign.json'
    elif change == 'changed_report':
        files[body['private_artifacts']['part_1_validation']['path'].casefold()] = b'{}'
    elif change == 'missing_compatibility':
        path = PureWindowsPath(body['private_artifacts']['part_1_validation']['path'])
        body['private_artifacts']['part_1_validation'] = store(path, {'valid': True})
    elif change == 'wrong_body_identity': body['identity'] = {'task_id': 'another-task'}
    if change not in {'missing_profile', 'changed_report'}:
        receipt['artifacts'][0] = store(PureWindowsPath(receipt['artifacts'][0]['path']), body)
    with pytest.raises((ValueError, KeyError)):
        module.check_related_files(receipt, read, PLAN)
