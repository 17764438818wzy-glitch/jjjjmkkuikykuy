"""Exercise W2 manual recovery through the actual W3 delivery boundary."""

import asyncio
from contextlib import asynccontextmanager
import io
import json
from pathlib import Path
import sys
import urllib.request
from urllib.error import HTTPError

import pytest

STAGES_TESTS = Path(__file__).resolve().parents[1] / 'stages'
sys.path.insert(0, str(STAGES_TESTS))

from test_partitioned_pipeline import test_partitioned_pipeline_submits_all_parts_then_one_global_ranking as exercise_w2
from test_server_handoff import server_handoff


def deliver_synthetic(identity, paper, records, catalog, tmp_path):
    from paperflow.package import validate_package
    from paperflow_v3.delivery.attachments import publish_attachments
    from paperflow_v3.delivery.publication import publish_body_targets
    from paperflow_v3.delivery.server_handoff import read_complete_handoff
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient
    from preextract_publish import _Budget
    from test_attachment_publish import Counter, FakeStore
    from test_transport_journal import event_store

    class JsonRag(FakeStore):
        def request(self, method, endpoint, body=None):
            result = super().request(method, endpoint, body)
            if method == 'PATCH' and isinstance(result, dict):
                return {'id': result['id']}
            return result

    evidence = read_complete_handoff(identity, paper, records)
    body = evidence['body_result']
    packet = validate_package(body['package_path'], body['task'], catalog, strict_seal=True)
    prepared = {'task': body['task'], 'validation': body['validation'], 'catalog': catalog}
    saved, save_event = event_store()
    raw = JsonRag()
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(identity, saved, save_event))
    targets, attachments = {}, {}
    publish_body_targets(paper, identity, evidence['handoff'], prepared, packet, client, targets,
                         lambda _: 'synthetic-artifact', lambda: True)
    publish_attachments(paper, identity, evidence['handoff'], packet, targets, client,
                        tmp_path / 'w3-receipts', _Budget(Counter()), 'folder-main', True,
                        attachments, lambda _: 'synthetic-artifact', lambda: True)
    assert targets and all(value['body_delivery_state'] == 'complete' for value in targets.values())
    assert len(attachments) == len(paper['attachments'])
    assert all(value['status'] == 'verified' for value in attachments.values())
    assert raw.sent


def forbid_network(*_args, **_kwargs):
    raise AssertionError('this integration uses synthetic transports only')


@pytest.mark.parametrize('failed_part', [1, 2, 3])
def test_partition_or_ranking_retry_reaches_the_real_delivery_publisher(
        tmp_path, monkeypatch, failed_part, managed_counter_factory):
    from paperflow_v3.stages import llm_handoff, pipeline

    monkeypatch.setattr(urllib.request.OpenerDirector, 'open', forbid_network)
    original = pipeline.process_paper
    delivered = []

    async def process_and_deliver(**options):
        result = await original(**options)
        if delivered:
            return result
        identity, paper = options['identity'], options['registered']['paper']
        records = await llm_handoff.read_receipts(identity['task_run_id'])
        failures = [row['data'] for row in records if row.get('kind') == 'llm_request_failed']
        assert failures and failures[0]['retryable'] is True
        retried = [row['data'] for row in records if row.get('kind') == 'llm_response_saved'
                   and row.get('data', {}).get('retry_of') == failures[0]['request_id']]
        assert len(retried) == 1 and retried[0]['request_id'] != failures[0]['request_id']
        assert retried[0]['component_id'] == failures[0]['component_id']
        assert retried[0]['retry_id'] == 'manual-part-2'
        deliver_synthetic(identity, paper, records, options['catalog'], tmp_path)
        delivered.append((failures[0], retried[0]))
        return result

    monkeypatch.setattr(pipeline, 'process_paper', process_and_deliver)
    exercise_w2(tmp_path, monkeypatch, failed_part, managed_counter_factory)
    assert len(delivered) == 1


@pytest.mark.parametrize(('failed_kind', 'failure_count'), [('body', 1), ('attachment', 1), ('body', 2)])
def test_single_body_or_attachment_retry_reaches_the_real_delivery_publisher(
        tmp_path, monkeypatch, failed_kind, failure_count, managed_counter_factory):
    from paperflow_prefect.attachments import has_request_history
    from paperflow_prefect.body_clean_handoff import save_projection
    from paperflow_prefect.records import read_json
    from paperflow_prefect.request_admission import confirm_first_request
    from paperflow_v3.stages import llm_handoff, llm_operations, llm_request, pipeline, postprocessing
    from test_body_adapter import RELEASE
    from test_llm_request import Admission, Response, WORKSPACE
    from test_synthetic_mainline_integration import save, synthetic_folder

    monkeypatch.setattr(urllib.request.OpenerDirector, 'open', forbid_network)
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    save_projection(manifest['body_work'], read_json(manifest['material']['path']),
                    read_json(manifest['reading_material']['path']))
    failed_row = next(row for row in admissions if row['kind'] == failed_kind)
    failed_model = Path(failed_row['model_dir'])
    answer = (failed_model / 'response.txt').read_bytes().decode('utf8')
    for name in ('request.json', 'request-state.json', 'response.txt', 'response-events.jsonl'):
        (failed_model / name).unlink()
    manifest_ref = save(tmp_path / 'handoff.json', manifest)
    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
               'kind': 'llm_response_saved', 'data': {
                   'component_id': row['component_id'], 'request_id': row['component_id'],
                   'status': 'raw_response_saved', 'kind': row['kind'], 'request_sha256': row['request_sha256'],
                   'response_sha256': row['response_sha256'],
                   'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
                   'response_path': str(Path(row['model_dir']) / 'response.txt')}}
              for row in admissions if row is not failed_row]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    monkeypatch.setattr(llm_operations, 'read_receipts_sync', lambda _: list(reversed(events)))

    @asynccontextmanager
    async def slot(_admission, _limits):
        yield

    class Settings:
        def path(self, key):
            assert key == 'template_path'
            return WORKSPACE.parent / '程序/resources/论文预提取_空白模板_v1.md'

    class Parent(Admission):
        async def wait(self):
            pass

    class Backend:
        root = tmp_path / 'backend'
        settings = Settings()

        def body_work(self, _paper):
            return Path(manifest['body_work'])

        def attachment_work(self, _paper, _row):
            return Path(attachment_work)

        def paper_root(self, _paper):
            return tmp_path

        def confirm_history(self, _paper, row):
            work = self.body_work(_paper) if row['kind'] == 'body' else self.attachment_work(_paper, row)
            if not has_request_history(work) and not (work / 'first-request.json').exists():
                self.root.mkdir(parents=True, exist_ok=True)
                confirm_first_request(_paper, row, work, history_roots=[self.root])

    calls = []

    class Transport:
        def open(self, request, timeout):
            calls.append(json.loads(request.data))
            if len(calls) <= failure_count:
                raise HTTPError(request.full_url, 401, 'synthetic', {}, io.BytesIO(b'complete failure'))
            return Response(json.dumps({'choices': [{'message': {'content': answer},
                'finish_reason': 'stop'}]}, ensure_ascii=False).encode('utf8'))

    for module in (llm_request, pipeline, llm_operations, llm_handoff, postprocessing):
        if hasattr(module, 'read_receipts'):
            monkeypatch.setattr(module, 'read_receipts', read_receipts)
        if hasattr(module, 'save_receipt'):
            monkeypatch.setattr(module, 'save_receipt', save_receipt)
        if hasattr(module, 'admitted_slot'):
            monkeypatch.setattr(module, 'admitted_slot', slot)
    prepared = {'status': 'preprocessing_complete',
                **{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']}
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: prepared)
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, _record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    registered = {'paper': paper, **{key: identity[key] for key in
                                   ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    options = dict(identity=identity, registered=registered, settings=None,
        connection={'model': 'synthetic-never-sent', 'base_url': 'http://synthetic.invalid',
                    'api_key': 'synthetic', 'max_tokens': 2048},
        admission=Parent(), limits={name: ['synthetic'] for name in
            ('preprocessing', 'body', 'attachment', 'postprocessing')}, catalog=catalog,
        paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory, output_root=tmp_path / 'result',
        opener=Transport())
    with pytest.raises(HTTPError):
        asyncio.run(pipeline.process_paper(**options))
    original_files = {path: path.read_bytes() for path in failed_model.iterdir() if path.is_file()}
    for attempt in range(1, failure_count + 1):
        retry_id = 'manual-single-' + str(attempt)
        events.append({'task_id': identity['task_id'], 'folder_id': identity['group_id'],
                       'kind': 'manual_retry', 'data': {'state': 'scheduled', 'retry_id': retry_id}})
        if attempt < failure_count:
            with pytest.raises(HTTPError):
                asyncio.run(pipeline.process_paper(**options, retry_id=retry_id))
        else:
            result = asyncio.run(pipeline.process_paper(**options, retry_id=retry_id))
    assert result['validation_status'] == 'passed' and len(calls) == failure_count + 1
    assert all(call == calls[0] for call in calls)
    assert all(path.read_bytes() == value for path, value in original_files.items())
    failures = [row['data'] for row in events if row['kind'] == 'llm_request_failed']
    assert len(failures) == failure_count
    assert all((Path(failure['model_dir']) / 'http-error.txt').is_file() for failure in failures)
    failure = failures[-1]
    retried = next(row['data'] for row in events if row['kind'] == 'llm_response_saved'
                   and row.get('data', {}).get('retry_of') == failure['request_id'])
    assert retried['component_id'] == failure['component_id']
    assert retried['request_id'] != failure['request_id'] and retried['retry_id'] == retry_id
    deliver_synthetic(identity, paper, list(reversed(events)), catalog, tmp_path)


def test_actual_w2_unknown_request_still_blocks_w3_even_with_another_saved_response(
        tmp_path, monkeypatch, server_handoff):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff
    from test_llm_request import Admission, dependencies

    monkeypatch.setattr(urllib.request.OpenerDirector, 'open', forbid_network)
    identity, paper, completed_records, _, _ = server_handoff
    component = next(row['data']['component_id'] for row in completed_records
                     if row['kind'] == 'llm_response_saved')
    module, generated = dependencies(monkeypatch)
    admission, calls = Admission(), []

    class Timeout:
        def open(self, request, timeout):
            calls.append('sent')
            raise TimeoutError('synthetic unknown outcome')

    work = tmp_path / 'unknown-request'
    connection = {'model': 'synthetic', 'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic'}
    with pytest.raises(TimeoutError):
        asyncio.run(module.run_raw_component(identity=identity, component_id=component,
            request_id='unknown-attempt', kind='body',
            request_body_factory=lambda: {'model': 'synthetic', 'messages': []}, connection=connection,
            work=work, admission=admission, limit_names=['synthetic'], check_history=lambda: None,
            check_source=lambda: None, opener=Timeout()))
    unknown = next(row for row in generated if row['kind'] == 'llm_request_unknown')
    combined = [*reversed(generated), *completed_records]
    with pytest.raises(ValueError, match='unknown'):
        read_complete_handoff(identity, paper, combined)
    generated.append({'kind': 'manual_retry', 'data': {'state': 'scheduled', 'retry_id': 'manual-unknown'}})
    with pytest.raises(RuntimeError, match='待核对'):
        asyncio.run(module.retry_raw_component(identity=identity, component_id=component,
            retry_id='manual-unknown', kind='body', original_work=work, original_model=work / 'model',
            expected_request_sha256=unknown['data']['request_sha256'], connection=connection,
            admission=admission, limit_names=['synthetic'], check_source=lambda: None, opener=Timeout()))
    assert calls == ['sent']
