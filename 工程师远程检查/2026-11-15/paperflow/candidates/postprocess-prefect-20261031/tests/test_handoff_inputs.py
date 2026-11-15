"""Verify the bridge consumes real upstream identities and raw-evidence layout."""
import copy
import hashlib
import json
from pathlib import Path
import sys

import pytest

CANDIDATE = Path(__file__).resolve().parents[1]
UPSTREAM = CANDIDATE.parent / 'llm-new-prefect-20261031'
sys.path[:0] = [str(CANDIDATE), str(UPSTREAM)]

from folder_completion import expected_components, inspect_folder
import handoff_inputs


def compact(value, *, sort=False):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=sort,
        separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, ensure_ascii=False).encode('utf8') if isinstance(value, dict) else value
    path.write_bytes(raw)
    return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}


def fixture(root):
    identity = dict(task_id='task-1', group_id='group-1', paper_id='paper-1',
                    receipt_id='a' * 64, attempt_id='attempt-1', parent_run_id='parent-1', task_run_id='folder-1')
    source = save(root / 'received/body.txt', b'synthetic body')
    attachment_source = save(root / 'received/attachment.zip', b'synthetic attachment')
    receipt = save(root / 'receipt.json', {'example': 1})
    material = save(root / 'material.json', {'example': 2})
    reading = save(root / 'reading.json', {'example': 3})
    work = root / 'body-work'
    attachment_work = root / 'attachment-work'
    body = {'file_id': 'body-id', **source}
    attachment = {'file_id': 'attachment-id', **attachment_source}
    request = {'messages': [{'role': 'user', 'content': 'body'}]}
    part = {'id': 'part-001', 'request_sha256': compact(request)}
    plan_identity = {'schema': 'paperflow-body-input-plan/1', 'source_sha256': source['sha256'],
        'reading_sha256': reading['sha256'], 'plan_sha256': 'b' * 64, 'coverage_sha256': 'c' * 64,
        'mode': 'single', 'parts': [part], 'part_count': 1}
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1',
        **{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        body=body, attachments=[attachment], receipt=receipt, material=material,
        reading_material=reading, body_work=str(work),
        capacity={'status': 'planned', 'plan_identity': plan_identity, 'parts': 1, 'mode': 'single'})
    paper = dict(paper_id=identity['paper_id'], body=body, attachments=[attachment])
    rows = []
    for component in expected_components(manifest, identity['task_run_id']):
        is_body = component['kind'] == 'body'
        current_work = work if is_body else attachment_work
        model = current_work / 'model'
        current_request = request if is_body else {'messages': [{'role': 'user', 'content': 'attachment'}]}
        request_ref = save(model / 'request.json', current_request)
        response_ref = save(model / 'response.txt', b'{"synthetic": true}')
        save(model / 'response-events.jsonl', b'{"synthetic": true}\n')
        save(model / 'request-state.json', {'status': 'response_complete', 'finish_reason': 'stop',
             'request_sha256': compact(current_request), 'response_sha256': response_ref['sha256']})
        if not is_body: save(current_work / 'evidence.json', {'synthetic': True})
        rows.append(dict(task_id=identity['task_id'], task_run_id=identity['task_run_id'],
            receipt_id=identity['receipt_id'], component_id=component['component_id'],
            kind=component['kind'], source_sha256=component['source_sha256'], state='raw_complete',
            request_sha256=compact(current_request), response_sha256=response_ref['sha256'],
            work_dir=str(current_work), model_dir=str(model)))
    verified = {'manifest': manifest, 'paper': paper}

    def observer(model):
        state = json.loads((Path(model) / 'request-state.json').read_text('utf8'))
        return 'response_complete', 'postprocess', dict(request_sha256=state['request_sha256'],
                                                        response_sha256=state['response_sha256'])

    def invoke(**overrides):
        opts = dict(identity=identity, verified=verified, admissions=rows, manifest_sha256=compact(manifest, sort=True),
            expected_components=expected_components, inspect_folder=inspect_folder,
            observe_request=observer, frozen_plan=lambda w, p: {'synthetic_plan_ref': str(w)},
            attachment_work=lambda p, r: attachment_work)
        opts.update(overrides)
        return handoff_inputs.build_inputs(**opts)
    return invoke, identity, verified, rows


def test_collects_complete_body_and_attachment_from_admitted_rows(tmp_path):
    build, identity, verified, rows = fixture(tmp_path)
    result = build()
    assert result['identity'] == identity
    assert result['body']['plan'] == {'synthetic_plan_ref': verified['manifest']['body_work']}
    assert len(result['body']['responses']) == 1
    assert result['body']['responses'][0]['response']['sha256'] == rows[0]['response_sha256']
    assert [row['attachment_id'] for row in result['attachments']] == ['attachment-id']
    assert result['attachments'][0]['model_refs']['response']['sha256'] == rows[1]['response_sha256']
    assert result['attachments'][0]['ticket'] == identity


def test_partial_or_unknown_reply_never_creates_inputs(tmp_path):
    build, _, _, rows = fixture(tmp_path)
    rows.pop()
    with pytest.raises(ValueError): build()


@pytest.mark.parametrize('change', ['source', 'response', 'request', 'identity', 'directory', 'manifest'])
def test_changed_evidence_or_wrong_owner_rejected(tmp_path, change):
    build, _, verified, rows = fixture(tmp_path)
    if change == 'source': Path(verified['manifest']['body']['path']).write_bytes(b'changed')
    if change == 'response': (Path(rows[0]['model_dir']) / 'response.txt').write_bytes(b'changed')
    if change == 'request': save(Path(rows[0]['model_dir']) / 'request.json', {'changed': True})
    if change == 'identity': rows[1]['task_run_id'] = 'other-flow'
    if change == 'directory': rows[1]['work_dir'] = rows[0]['work_dir']
    if change == 'manifest':
        with pytest.raises(ValueError): build(manifest_sha256='d' * 64)
        return
    with pytest.raises(ValueError): build()
