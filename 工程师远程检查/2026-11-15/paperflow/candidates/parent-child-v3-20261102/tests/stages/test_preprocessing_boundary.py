"""Preprocessing completion and explicit failure stay separate per paper."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys
from zipfile import BadZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder, save


@pytest.fixture
def setup_pipeline(tmp_path, monkeypatch):
    from paperflow_v3.stages import pipeline
    identity, manifest, paper, _admissions, catalog, _attachment_work = synthetic_folder(tmp_path)
    manifest_ref = save(tmp_path / 'handoff.json', manifest)
    events, calls = [], []

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    class Admission:
        async def wait(self):
            pass
        async def running(self):
            return True
        def running_sync(self):
            return True

    class Backend:
        pass

    async def stop_after_preprocessing(**_kwargs):
        raise RuntimeError('stop after preprocessing')

    monkeypatch.setattr(pipeline, 'read_receipts', read_receipts)
    monkeypatch.setattr(pipeline, 'save_receipt', save_receipt)
    monkeypatch.setattr(pipeline, 'admitted_slot', slot)
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, _record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'run_single_body', stop_after_preprocessing)
    monkeypatch.setattr(pipeline, 'run_attachment', stop_after_preprocessing)
    registered = {'paper': paper, **{key: identity[key] for key in
        ('task_id', 'group_id', 'paper_id', 'receipt_id')},
        'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    options = dict(identity=identity, registered=registered, settings=None, connection={}, admission=Admission(),
        limits={name: ['synthetic'] for name in ('preprocessing', 'body', 'attachment', 'postprocessing')},
        catalog=catalog, paperflow_root=tmp_path, counter_factory=None, output_root=tmp_path / 'output')
    prepared = {'status': 'preprocessing_complete', **{key: identity[key] for key in
        ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']}
    return pipeline, identity, manifest, manifest_ref, events, calls, options, prepared


def test_file_access_failure_requires_explicit_retry_and_success_is_reused(setup_pipeline, monkeypatch):
    pipeline, identity, _manifest, _ref, events, calls, options, prepared = setup_pipeline

    def prepare(*_args, **_kwargs):
        calls.append('prepare')
        if len(calls) == 1:
            raise PermissionError('synthetic working file is locked')
        return prepared

    monkeypatch.setattr(pipeline, 'prepare_registered_folder', prepare)
    with pytest.raises(PermissionError):
        asyncio.run(pipeline.process_paper(**options))
    failed = next(row['data'] for row in events if row['kind'] == 'preprocessing_failed')
    assert failed['retryable'] is True and failed['failure_reason'] == 'local_io'
    assert 'locked' in failed['reason']
    assert not any(row['kind'] == 'preprocessing_complete' for row in events)
    with pytest.raises(RuntimeError, match='人工重试'):
        asyncio.run(pipeline.process_paper(**options))
    assert calls == ['prepare']
    events.append({'kind': 'manual_retry', 'data': {'state': 'scheduled', 'retry_id': 'manual-pre'}})
    with pytest.raises(RuntimeError, match='stop after preprocessing'):
        asyncio.run(pipeline.process_paper(**options, retry_id='manual-pre'))
    assert calls == ['prepare', 'prepare']
    assert any(row['kind'] == 'preprocessing_complete' for row in events)
    with pytest.raises(RuntimeError, match='stop after preprocessing'):
        asyncio.run(pipeline.process_paper(**options))
    assert calls == ['prepare', 'prepare']


def test_pipeline_passes_first_step_root_to_preprocessing(setup_pipeline, monkeypatch, tmp_path):
    pipeline, _identity, _manifest, _ref, _events, calls, options, prepared = setup_pipeline
    first_root = tmp_path / 'first-step'
    options['first_step_work_root'] = first_root

    def prepare(_settings, _registered, *, attempt_id, first_step_work_root):
        calls.append(first_step_work_root)
        return prepared

    monkeypatch.setattr(pipeline, 'prepare_registered_folder', prepare)
    with pytest.raises(RuntimeError, match='stop after preprocessing'):
        asyncio.run(pipeline.process_paper(**options))
    assert calls == [first_root]


def _reviewed_preprocessing_events(identity, registered):
    keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')
    folder_identity = {key: identity[key] for key in keys}
    paper = registered['paper']
    source = [row['sha256'] for row in (paper['body'], *paper['attachments'])]
    archive = [row['sha256'] for row in paper['archive_files']]
    failure = {'kind': 'preprocessing_failed', 'event_id': 'original-failure',
        'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'data': {**folder_identity, 'processing_attempt_id': 'initial',
                 'failure_reason': 'preprocessing_unconfirmed', 'retryable': False,
                 'reason': '原转换结果待核对'}}
    review = {'kind': 'preprocessing_reviewed', 'event_id': 'review-1',
        'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'data': {'identity': folder_identity, 'failure_event_id': 'original-failure',
            'processing_attempt_id': 'initial', 'receipt_sha256': registered['receipt_sha256'],
            'source_sha256': source, 'delivery_sha256': source, 'archive_sha256': archive,
            'cached_docx_sha256': '3' * 64, 'observed_at': '2026-11-02T13:30:00+00:00',
            'conversion_observation': 'no_owned_process_observed',
            'local_request_history': 'none_observed', 'downstream_events': 'none_observed'}}
    manual = {'kind': 'manual_retry', 'data': {'state': 'running', 'stage': 'preprocessing',
        'retry_id': 'manual-pre', 'review_event_id': 'review-1',
        'failure_event_id': 'original-failure'}}
    return failure, review, manual


def test_reviewed_failure_with_matching_manual_retry_reaches_preprocessing(setup_pipeline, monkeypatch):
    pipeline, identity, _manifest, _ref, events, calls, options, prepared = setup_pipeline
    failure, review, manual = _reviewed_preprocessing_events(identity, options['registered'])
    events.extend((failure, review, manual))
    monkeypatch.setattr(pipeline, 'prepare_registered_folder',
                        lambda *_args, **_kwargs: calls.append('prepare') or prepared)
    with pytest.raises(RuntimeError, match='stop after preprocessing'):
        asyncio.run(pipeline.process_paper(**options, retry_id='manual-pre'))
    assert calls == ['prepare']
    assert failure in events and failure['data']['retryable'] is False
    assert any(row['kind'] == 'preprocessing_complete' for row in events)


def test_stage_consumes_manual_authorization_without_rechecking_review(setup_pipeline, monkeypatch):
    pipeline, identity, _manifest, _ref, events, calls, options, prepared = setup_pipeline
    failure, _review, manual = _reviewed_preprocessing_events(identity, options['registered'])
    # The API has already checked the review before saving this authorization.
    events.extend((failure, manual))
    monkeypatch.setattr(pipeline, 'prepare_registered_folder',
                        lambda *_args, **_kwargs: calls.append('prepare') or prepared)
    with pytest.raises(RuntimeError, match='stop after preprocessing'):
        asyncio.run(pipeline.process_paper(**options, retry_id='manual-pre'))
    assert calls == ['prepare']


@pytest.mark.parametrize('case', [
    'no_manual', 'missing_review_ref', 'missing_failure_ref', 'missing_failure_event',
    'wrong_failure', 'wrong_retry', 'wrong_stage',
])
def test_reviewed_failure_does_not_bypass_identity_or_manual_authorization(setup_pipeline, monkeypatch, case):
    pipeline, identity, _manifest, _ref, events, calls, options, _prepared = setup_pipeline
    failure, review, manual = _reviewed_preprocessing_events(identity, options['registered'])
    if case == 'missing_review_ref': manual['data'].pop('review_event_id')
    if case == 'missing_failure_ref': manual['data'].pop('failure_event_id')
    if case == 'missing_failure_event': failure.pop('event_id')
    if case == 'wrong_failure': manual['data']['failure_event_id'] = 'another-failure'
    if case == 'wrong_retry': manual['data']['retry_id'] = 'another-retry'
    if case == 'wrong_stage': manual['data']['stage'] = 'rag'
    events.extend((failure, review, *([] if case == 'no_manual' else [manual])))
    monkeypatch.setattr(pipeline, 'prepare_registered_folder',
                        lambda *_args, **_kwargs: calls.append('unexpected'))
    with pytest.raises(RuntimeError):
        asyncio.run(pipeline.process_paper(**options, retry_id='manual-pre'))
    assert calls == []
    assert failure in events and failure['data']['retryable'] is False


@pytest.mark.parametrize('error,reason', [
    (BadZipFile('synthetic damaged document'), 'local_parse'),
    (ValueError('synthetic identity mismatch'), 'preprocessing_unconfirmed'),
])
def test_parse_and_identity_failures_are_not_automatically_retryable(setup_pipeline, monkeypatch, error, reason):
    pipeline, _identity, _manifest, _ref, events, calls, options, _prepared = setup_pipeline
    def prepare(*_args, **_kwargs):
        calls.append('prepare')
        raise error
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', prepare)
    with pytest.raises(type(error)):
        asyncio.run(pipeline.process_paper(**options))
    failed = next(row['data'] for row in events if row['kind'] == 'preprocessing_failed')
    assert failed['failure_reason'] == reason and failed['retryable'] is False
    assert not any(row['kind'] == 'preprocessing_complete' for row in events)


def test_unavailable_plan_is_not_preprocessing_success(setup_pipeline, monkeypatch):
    pipeline, _identity, manifest, reference, events, calls, options, prepared = setup_pipeline
    manifest['capacity'] = {'status': 'unavailable', 'error_type': 'RequestCapacityError'}
    reference.update(save(Path(reference['path']), manifest))
    prepared['manifest_sha256'] = reference['sha256']
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: prepared)
    with pytest.raises(ValueError, match='容量或分段计划不可用'):
        asyncio.run(pipeline.process_paper(**options))
    failed = next(row['data'] for row in events if row['kind'] == 'preprocessing_failed')
    assert failed['failure_reason'] == 'plan_unavailable' and failed['retryable'] is False
    assert failed['plan_status'] == 'unavailable'
    assert not any(row['kind'] == 'preprocessing_complete' for row in events)


def test_pause_does_not_become_preprocessing_failure(setup_pipeline, monkeypatch):
    pipeline, _identity, _manifest, _ref, events, _calls, options, _prepared = setup_pipeline
    def paused(*_args, **_kwargs):
        raise InterruptedError('synthetic pause')
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', paused)
    with pytest.raises(InterruptedError):
        asyncio.run(pipeline.process_paper(**options))
    assert not any(row['kind'] in {'preprocessing_failed', 'preprocessing_complete'} for row in events)


def test_changed_registered_source_is_not_claimed_retryable(setup_pipeline, monkeypatch):
    pipeline, _identity, _manifest, _ref, events, _calls, options, _prepared = setup_pipeline
    Path(options['registered']['paper']['body']['path']).write_bytes(b'changed synthetic source')
    def unreadable(*_args, **_kwargs):
        raise PermissionError('synthetic file access failure')
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', unreadable)
    with pytest.raises(PermissionError):
        asyncio.run(pipeline.process_paper(**options))
    failed = next(row['data'] for row in events if row['kind'] == 'preprocessing_failed')
    assert failed['retryable'] is False
    assert not any(row['kind'] == 'preprocessing_complete' for row in events)


def test_missing_plan_identity_cannot_be_marked_preprocessing_complete(setup_pipeline, monkeypatch):
    pipeline, _identity, manifest, reference, events, _calls, options, prepared = setup_pipeline
    manifest['capacity'].pop('plan_identity')
    reference.update(save(Path(reference['path']), manifest))
    prepared['manifest_sha256'] = reference['sha256']
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: prepared)
    with pytest.raises(ValueError):
        asyncio.run(pipeline.process_paper(**options))
    assert any(row['kind'] == 'preprocessing_failed' and row['data']['retryable'] is False for row in events)
    assert not any(row['kind'] == 'preprocessing_complete' for row in events)


def test_missing_prepared_material_cannot_be_marked_preprocessing_complete(setup_pipeline, monkeypatch):
    pipeline, _identity, manifest, reference, events, _calls, options, prepared = setup_pipeline
    manifest['material']['path'] = str(Path(reference['path']).parent / 'missing-material.json')
    reference.update(save(Path(reference['path']), manifest))
    prepared['manifest_sha256'] = reference['sha256']
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: prepared)
    with pytest.raises(OSError):
        asyncio.run(pipeline.process_paper(**options))
    assert any(row['kind'] == 'preprocessing_failed' and row['data']['retryable'] is False for row in events)
    assert not any(row['kind'] == 'preprocessing_complete' for row in events)


def test_changed_completed_handoff_is_recorded_as_unconfirmed(setup_pipeline, monkeypatch):
    pipeline, _identity, _manifest, reference, events, calls, options, prepared = setup_pipeline
    def prepare(*_args, **_kwargs):
        calls.append('prepare')
        return prepared
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', prepare)
    with pytest.raises(RuntimeError, match='stop after preprocessing'):
        asyncio.run(pipeline.process_paper(**options))
    Path(reference['path']).write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='交接记录待核对'):
        asyncio.run(pipeline.process_paper(**options))
    assert calls == ['prepare']
    assert events[-1]['kind'] == 'preprocessing_failed'
    assert events[-1]['data']['retryable'] is False
