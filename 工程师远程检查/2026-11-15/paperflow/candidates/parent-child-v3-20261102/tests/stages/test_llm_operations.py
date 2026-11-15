"""Actual single-body and attachment request preparation using saved fixtures."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder


def test_real_body_and_attachment_adapters_reuse_saved_replies(tmp_path, monkeypatch):
    try:
        from paperflow_v3.stages import llm_operations, llm_request
    except ImportError:
        pytest.fail('The concrete body and attachment request adapters are missing')
    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    from paperflow_prefect.body_clean_handoff import save_projection
    from paperflow_prefect.records import read_json
    save_projection(manifest['body_work'], read_json(manifest['material']['path']),
                    read_json(manifest['reading_material']['path']))
    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'kind': 'llm_response_saved', 'data': {'component_id': row['component_id'],
            'request_id': row['component_id'], 'kind': row['kind'], 'status': 'raw_response_saved',
            'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
            'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**data):
        events.append(data)

    @asynccontextmanager
    async def isolated_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    monkeypatch.setattr(llm_request, 'read_receipts', read_receipts)
    monkeypatch.setattr(llm_operations, 'read_receipts', read_receipts)
    monkeypatch.setattr(llm_request, 'save_receipt', save_receipt)
    monkeypatch.setattr(llm_request, 'admitted_slot', isolated_slot)

    class Admission:
        def running_sync(self):
            return True

        async def wait(self):
            pass

    class Backend:
        root = tmp_path / 'backend'
        def body_work(self, _paper):
            return Path(manifest['body_work'])
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)
        def confirm_history(self, _paper, _row):
            pass

    class NoTransport:
        def open(self, *_args, **_kwargs):
            pytest.fail('saved replies must never be sent again')

    body = next(row for row in admissions if row['kind'] == 'body')
    attachment = next(row for row in admissions if row['kind'] == 'attachment')
    options = dict(identity=identity, manifest=manifest, paper=paper, backend=Backend(),
                   connection={'model': 'synthetic-never-sent', 'base_url': 'http://synthetic.invalid',
                               'api_key': 'synthetic'}, admission=Admission(), limit_names=['synthetic'],
                   opener=NoTransport())
    result_body = asyncio.run(llm_operations.run_single_body(
        **options, request_id=body['component_id']))
    result_attachment = asyncio.run(llm_operations.run_attachment(
        **options, row=paper['attachments'][0], request_id=attachment['component_id']))
    assert result_body['status'] == 'raw_response_saved'
    assert result_attachment['status'] == 'raw_response_saved'
    projection = Path(manifest['body_work']) / 'private/prefect-reading-projection.json'
    projection.unlink()  # Synthetic-only simulation of missing original evidence.
    with pytest.raises(ValueError, match='缺少绑定'):
        asyncio.run(llm_operations.run_single_body(**options, request_id=body['component_id']))
    assert not projection.exists()
