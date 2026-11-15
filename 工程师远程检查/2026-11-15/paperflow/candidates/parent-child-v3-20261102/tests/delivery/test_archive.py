import copy
from importlib import import_module
from pathlib import Path

import pytest


@pytest.fixture
def archive_input(tmp_path):
    source = tmp_path / 'delivery'
    source.mkdir()
    (source / 'body.docx').write_bytes(b'synthetic paper')
    (source / 'attachment.zip').write_bytes(b'synthetic attachment')
    paper = {'paper_id': 'paper-1', 'group_id': 'group-1',
             'body': {'file_id': 'body-1'}, 'attachments': [{'file_id': 'attachment-1'}],
             'archive_files': [
                 {'file_id': 'body-1', 'kind': 'body', 'path': str(source / 'body.docx'), 'relative': '正文/body.docx'},
                 {'file_id': 'attachment-1', 'kind': 'attachment', 'path': str(source / 'attachment.zip'),
                  'relative': '附件/attachment.zip'},
             ]}
    identity = {'task_id': 't', 'group_id': 'group-1', 'paper_id': 'paper-1', 'receipt_id': 'r',
                'attempt_id': 'a', 'parent_run_id': 'p', 'task_run_id': 'c'}
    handoff = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': dict(identity),
               'validation_status': 'passed', 'body': {'package_path': 'pkg', 'manifest_sha256': 'm'},
               'attachments': [{'attachment_id': 'attachment-1'}]}
    return paper, identity, handoff, tmp_path / 'nas' / 'paper-1'


def test_one_archive_contains_original_body_and_attachment(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    state, snapshots = {}, []
    def save(value):
        snapshots.append(copy.deepcopy(value))
        return 'server-artifact'

    module.save_archive(paper, identity, handoff, destination, state, save, lambda: True)
    assert (destination / '正文/body.docx').read_bytes() == b'synthetic paper'
    assert (destination / '附件/attachment.zip').read_bytes() == b'synthetic attachment'
    assert state['status'] == 'complete'
    assert set(state['files']) == {'body-1', 'attachment-1'}
    assert all(row['status'] == 'complete' for row in state['files'].values())


def test_missing_source_never_marks_archive_complete(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    Path(paper['archive_files'][1]['path']).unlink()
    state = {}
    with pytest.raises(FileNotFoundError):
        module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True)
    assert state.get('status') != 'complete'
    assert Path(paper['archive_files'][0]['path']).is_file()


def test_pause_after_one_file_stops_before_next_archive(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    state = {}
    running = {'value': True}

    def save(snapshot):
        if snapshot.get('files', {}).get('body-1', {}).get('status') == 'complete':
            running['value'] = False
        return 'artifact'

    with pytest.raises(InterruptedError):
        module.save_archive(paper, identity, handoff, destination, state, save, lambda: running['value'])
    assert (destination / '正文/body.docx').is_file()
    assert not (destination / '附件/attachment.zip').exists()
    assert state.get('status') != 'complete'


def test_unknown_archive_result_is_not_copied_again(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    state = {'identity': dict(identity), 'files': {'body-1': {
        'status': 'in_flight', 'destination': str(destination / '正文/body.docx')}}}
    with pytest.raises(ValueError, match='unknown'):
        module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True)
    assert not destination.exists()


def test_unconfirmed_server_intent_prevents_archive_write(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    with pytest.raises(ValueError, match='server'):
        module.save_archive(paper, identity, handoff, destination, {}, lambda _: None, lambda: True)
    assert not (destination / '正文/body.docx').exists()


def test_lost_server_completion_ack_keeps_archive_outcome_unknown(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    state = {}

    def save(value):
        entry = value.get('files', {}).get('body-1', {})
        return None if entry.get('status') == 'complete' else 'artifact'

    with pytest.raises(ValueError, match='server'):
        module.save_archive(paper, identity, handoff, destination, state, save, lambda: True)
    assert (destination / '正文/body.docx').is_file()
    assert state['files']['body-1']['status'] == 'in_flight'
    assert state.get('status') != 'complete'


def test_manual_retry_replaces_only_the_known_failed_partial_archive(archive_input, monkeypatch):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    state, copied = {}, []
    original_copy = module.shutil.copyfileobj

    def fail_attachment_once(source, target, *args, **kwargs):
        copied.append(Path(source.name).name)
        if copied.count('attachment.zip') == 1:
            target.write(b'partial')
            raise OSError('synthetic interrupted copy')
        return original_copy(source, target, *args, **kwargs)

    monkeypatch.setattr(module.shutil, 'copyfileobj', fail_attachment_once)
    with pytest.raises(OSError):
        module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True)
    assert state['files']['body-1']['status'] == 'complete'
    assert state['files']['attachment-1']['status'] == 'failed'
    assert (destination / '正文/body.docx').is_file()
    assert not (destination / '附件/attachment.zip').exists()
    partial = destination / '附件/attachment.zip.paperflow-part'
    assert partial.read_bytes() == b'partial'
    with pytest.raises(ValueError, match='retry'):
        module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True)
    module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True,
                        retry_failed=True)
    assert copied == ['body.docx', 'attachment.zip', 'attachment.zip']
    assert (destination / '附件/attachment.zip').read_bytes() == b'synthetic attachment'
    assert not partial.exists()
    assert state['status'] == 'complete'


def test_unconfirmed_failure_receipt_cannot_authorize_retry(archive_input, monkeypatch):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    state = {}

    def broken_copy(source, target, *args, **kwargs):
        target.write(b'partial')
        raise OSError('synthetic failed copy')

    def save(value):
        return None if value.get('files', {}).get('body-1', {}).get('status') == 'failed' else 'artifact'

    monkeypatch.setattr(module.shutil, 'copyfileobj', broken_copy)
    with pytest.raises(ValueError, match='server'):
        module.save_archive(paper, identity, handoff, destination, state, save, lambda: True)
    assert state['files']['body-1']['status'] == 'in_flight'
    with pytest.raises(ValueError, match='unknown'):
        module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True,
                            retry_failed=True)


def test_retry_cannot_delete_a_different_file_using_a_tampered_partial_path(archive_input):
    module = import_module('paperflow_v3.delivery.archive')
    paper, identity, handoff, destination = archive_input
    other = destination.parent / 'unrelated.txt'
    other.parent.mkdir(parents=True)
    other.write_bytes(b'keep')
    state = {'identity': dict(identity), 'files': {'body-1': {
        'status': 'failed', 'destination': str(destination / '正文/body.docx'),
        'partial_path': str(other), 'partial_owned': True}}}
    with pytest.raises(ValueError):
        module.save_archive(paper, identity, handoff, destination, state, lambda _: 'artifact', lambda: True,
                            retry_failed=True)
    assert other.read_bytes() == b'keep'
