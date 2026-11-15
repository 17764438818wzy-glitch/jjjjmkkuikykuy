import copy
import errno
import os
from importlib import import_module
from pathlib import Path

import pytest


@pytest.fixture
def delivery(tmp_path):
    source, copies, work, nas = [tmp_path / name for name in ('source', 'copies', 'work', 'nas')]
    for folder in (source, copies, work, nas):
        folder.mkdir()
    for path in (source / 'body.docx', source / 'attachment.zip', source / 'trash.txt',
                 copies / 'body.docx', copies / 'attachment.zip', work / 'material.json', work / 'receipt.json',
                 nas / 'body.docx', nas / 'attachment.zip'):
        path.write_bytes(b'synthetic')
    paper = {'paper_id': 'p', 'group_id': 'g', 'person_dir': str(source), 'excluded': ['trash.txt'],
             'body': {'file_id': 'body', 'sha256': 'body-sha'},
             'attachments': [{'file_id': 'attachment', 'sha256': 'attachment-sha'}],
             'archive_files': [
                 {'file_id': 'body', 'kind': 'body', 'path': str(copies / 'body.docx'),
                  'source_path': str(source / 'body.docx')},
                 {'file_id': 'attachment', 'kind': 'attachment', 'path': str(copies / 'attachment.zip'),
                  'source_path': str(source / 'attachment.zip')},
             ]}
    identity = {'task_id': 't', 'group_id': 'g', 'paper_id': 'p', 'receipt_id': 'r',
                'attempt_id': 'a', 'parent_run_id': 'parent', 'task_run_id': 'child'}
    handoff = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': dict(identity),
               'validation_status': 'passed', 'body': {'package_path': 'pkg', 'manifest_sha256': 'm'},
               'attachments': [{'attachment_id': 'attachment'}]}
    packet = {'routes': [{'code': c, 'paper_dataset_id': 'paper-' + c, 'image_dataset_id': 'asset-' + c}
                         for c in ['a', 'b', 'c']]}
    body = {c: {'body_delivery_state': 'complete', 'verified': True, 'retrieval_verified': True,
                'formal_route': copy.deepcopy(next(route for route in packet['routes'] if route['code'] == c)),
                'ticket_identity': dict(identity),
                'body': {'dataset_id': 'paper-' + c, 'document_id': 'doc-' + c,
                         'identity': {'paperflow_source_sha256': 'body-sha', 'paperflow_specialty': c}}}
            for c in ['a', 'b', 'c']}
    attachments = {'attachment': {'status': 'verified', 'current_md_sha256': 'md',
                                 'ticket_identity': dict(identity),
                                 'binding': {'paper_id': 'p', 'attachment_id': 'attachment',
                                             'source_sha256': 'attachment-sha'},
                                 'publications': {'md': {c: {'status': 'verified', 'dataset_id': 'asset-' + c}
                                                         for c in ['a', 'b', 'c']}}}}
    archived = {'identity': dict(identity), 'status': 'complete', 'files': {
        'body': {'status': 'complete', 'destination': str(nas / 'body.docx')},
        'attachment': {'status': 'complete', 'destination': str(nas / 'attachment.zip')},
    }}
    extras = [str(work / 'material.json'), str(work / 'receipt.json')]
    return paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas


def test_ready_paper_cleans_exact_original_copy_and_work_files_only(delivery):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    events = []

    def server_save(record):
        events.append(copy.deepcopy(record))
        return f'artifact-{len(events)}'

    result = module.cleanup_paper(paper, identity, handoff, packet, body, attachments, archived, extras,
                                  {}, server_save, lambda: True)
    assert result['status'] == 'complete'
    assert all(not p.exists() for p in [source / 'body.docx', source / 'attachment.zip',
                                        copies / 'body.docx', copies / 'attachment.zip',
                                        work / 'material.json', work / 'receipt.json'])
    assert (source / 'trash.txt').is_file()
    assert (nas / 'body.docx').is_file() and (nas / 'attachment.zip').is_file()
    assert events[0]['body_targets'] == body and events[0]['archive'] == archived


@pytest.mark.parametrize('condition', ['rag_failed', 'attachment_failed', 'nas_missing', 'server_unavailable'])
def test_incomplete_delivery_or_no_server_receipt_keeps_local_files(delivery, condition):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    if condition == 'rag_failed':
        body['c']['body_delivery_state'] = 'failed'
    elif condition == 'attachment_failed':
        attachments['attachment']['publications']['md']['b']['status'] = 'failed'
    elif condition == 'nas_missing':
        archived['files']['attachment']['status'] = 'unknown'
    server_save = (lambda _: None) if condition == 'server_unavailable' else (lambda _: 'artifact-1')
    with pytest.raises(ValueError):
        module.cleanup_paper(paper, identity, handoff, packet, body, attachments, archived, extras,
                             {}, server_save, lambda: True)
    assert all(p.is_file() for p in [source / 'body.docx', source / 'attachment.zip', copies / 'body.docx',
                                    copies / 'attachment.zip', work / 'material.json', work / 'receipt.json'])


def test_pause_after_first_delete_stops_before_second(delivery):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    running = {'value': True}

    def save(record):
        if any(row['status'] == 'complete' for row in record.get('files', {}).values()):
            running['value'] = False
        return 'server-artifact'

    state = {}
    with pytest.raises(InterruptedError):
        module.cleanup_paper(paper, identity, handoff, packet, body, attachments, archived, extras,
                             state, save, lambda: running['value'])
    assert sum(not p.exists() for p in [source / 'body.docx', source / 'attachment.zip',
                                        copies / 'body.docx', copies / 'attachment.zip',
                                        work / 'material.json', work / 'receipt.json']) == 1
    assert (source / 'trash.txt').is_file()


def test_trash_or_nas_file_in_extra_list_never_deleted(delivery):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    for protected in (source / 'trash.txt', nas / 'body.docx'):
        with pytest.raises(ValueError):
            module.cleanup_paper(paper, identity, handoff, packet, body, attachments, archived,
                                 extras + [str(protected)], {}, lambda _: 'server-artifact', lambda: True)
        assert protected.is_file()


def test_permission_failure_requires_manual_retry_and_does_not_repeat_success(delivery, monkeypatch):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    failed_path = copies / 'body.docx'
    real_unlink, attempted = Path.unlink, []

    def unlink_once(path, *args, **kwargs):
        attempted.append(path)
        if path == failed_path and attempted.count(path) == 1:
            raise PermissionError('synthetic locked file')
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'unlink', unlink_once)
    state = {}
    args = (paper, identity, handoff, packet, body, attachments, archived, extras, state,
            lambda _: 'server-artifact', lambda: True)
    with pytest.raises(PermissionError):
        module.cleanup_paper(*args)
    assert not (source / 'body.docx').exists() and failed_path.exists()
    assert state['files'][os.path.normcase(str(failed_path))]['status'] == 'failed'
    with pytest.raises(ValueError, match='retry'):
        module.cleanup_paper(*args)
    module.cleanup_paper(*args, retry_failed=True)
    assert attempted.count(source / 'body.docx') == 1
    assert attempted.count(failed_path) == 2
    assert state['status'] == 'complete' and (source / 'trash.txt').is_file()


def test_lost_server_receipt_after_unlink_stays_unknown_even_on_retry(delivery):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    state = {}

    def save(value):
        return None if any(row['status'] == 'complete' for row in value.get('files', {}).values()) else 'artifact'

    args = (paper, identity, handoff, packet, body, attachments, archived, extras, state, save, lambda: True)
    with pytest.raises(ValueError, match='server'):
        module.cleanup_paper(*args)
    assert not (source / 'body.docx').exists()
    assert state['files'][os.path.normcase(str(source / 'body.docx'))]['status'] == 'in_flight'
    with pytest.raises(ValueError, match='unknown'):
        module.cleanup_paper(*args, retry_failed=True)
    assert (copies / 'body.docx').exists()


def test_ambiguous_unlink_error_cannot_be_retried_without_reconciliation(delivery, monkeypatch):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    real_unlink = Path.unlink

    def ambiguous(path, *args, **kwargs):
        if path == source / 'body.docx':
            raise OSError(errno.EIO, 'synthetic ambiguous I/O error')
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'unlink', ambiguous)
    state = {}
    args = (paper, identity, handoff, packet, body, attachments, archived, extras, state,
            lambda _: 'server-artifact', lambda: True)
    with pytest.raises(OSError):
        module.cleanup_paper(*args)
    assert state['files'][os.path.normcase(str(source / 'body.docx'))]['status'] == 'in_flight'
    with pytest.raises(ValueError, match='unknown'):
        module.cleanup_paper(*args, retry_failed=True)


@pytest.mark.parametrize('wrong_evidence', ['body', 'attachment', 'archive'])
def test_receipts_from_another_child_cannot_authorize_deletion(delivery, wrong_evidence):
    module = import_module('paperflow_v3.delivery.cleanup')
    paper, identity, handoff, packet, body, attachments, archived, extras, source, copies, work, nas = delivery
    target = body['a'] if wrong_evidence == 'body' else (
        attachments['attachment'] if wrong_evidence == 'attachment' else archived)
    key = 'identity' if wrong_evidence == 'archive' else 'ticket_identity'
    target[key]['task_run_id'] = 'other-child'
    with pytest.raises(ValueError):
        module.cleanup_paper(paper, identity, handoff, packet, body, attachments, archived, extras,
                             {}, lambda _: 'server-artifact', lambda: True)
    assert (source / 'body.docx').is_file()


