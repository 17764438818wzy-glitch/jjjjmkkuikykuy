"""Use W2's actual synthetic stage results as W3's server handoff input."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys

WORKSPACE = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(WORKSPACE / 'candidates' / 'postprocess-prefect-20261031' / 'tests'))

from paperflow_prefect.body_clean_handoff import save_projection
from paperflow_prefect.records import read_json
from test_body_adapter import RELEASE
from test_synthetic_mainline_integration import synthetic_folder, save


def test_processing_records_are_accepted_by_the_real_delivery_boundary(tmp_path, monkeypatch, managed_counter_factory):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff
    from paperflow_v3.stages import pipeline, llm_request, llm_operations, llm_handoff, postprocessing

    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    save_projection(manifest['body_work'], read_json(manifest['material']['path']),
                    read_json(manifest['reading_material']['path']))
    manifest_ref = save(tmp_path / 'preprocessing-handoff.json', manifest)
    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
               'kind': 'llm_response_saved', 'data': {'component_id': row['component_id'],
                   'request_id': row['component_id'], 'kind': row['kind'], 'status': 'raw_response_saved',
                   'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
                   'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
                   'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    class Admission:
        async def running(self): return True
        def running_sync(self): return True
        async def wait(self): return None

    @asynccontextmanager
    async def isolated_slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        yield

    class Backend:
        root = tmp_path / 'backend'
        def paper_root(self, _paper): return tmp_path
        def body_work(self, _paper): return Path(manifest['body_work'])
        def attachment_work(self, _paper, _row): return Path(attachment_work)
        def confirm_history(self, _paper, _row): pass

    class NoTransport:
        def open(self, *_args, **_kwargs):
            raise AssertionError('saved synthetic responses must not be sent')

    for module in (pipeline, llm_request, llm_operations, llm_handoff, postprocessing):
        if hasattr(module, 'save_receipt'):
            monkeypatch.setattr(module, 'save_receipt', save_receipt)
        if hasattr(module, 'read_receipts'):
            monkeypatch.setattr(module, 'read_receipts', read_receipts)
        if hasattr(module, 'admitted_slot'):
            monkeypatch.setattr(module, 'admitted_slot', isolated_slot)
    prepared = {'status': 'preprocessing_complete',
                **{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                'manifest_path': manifest_ref['path'], 'manifest_sha256': manifest_ref['sha256']}
    monkeypatch.setattr(pipeline, 'prepare_registered_folder', lambda *_args, **_kwargs: prepared)
    monkeypatch.setattr(pipeline, 'settings_for_verified_receipt', lambda settings, record: settings)
    monkeypatch.setattr(pipeline, 'UnifiedBackend', lambda *_args, **_kwargs: Backend())
    monkeypatch.setattr(pipeline, 'stage_workspace', lambda *_args: tmp_path)
    registered = {'paper': paper, **{key: identity[key] for key in
                                   ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'receipt_path': manifest['receipt']['path'], 'receipt_sha256': manifest['receipt']['sha256']}
    asyncio.run(pipeline.process_paper(identity=identity, registered=registered, settings=None,
        connection={'model': 'synthetic', 'base_url': 'http://synthetic.invalid', 'api_key': 'synthetic'},
        admission=Admission(), limits={name: ['synthetic'] for name in
            ('preprocessing', 'body', 'attachment', 'postprocessing')},
        catalog=catalog, paperflow_root=RELEASE / 'core', counter_factory=managed_counter_factory,
        output_root=tmp_path / 'postprocessing', opener=NoTransport()))
    evidence = read_complete_handoff(identity, paper, list(reversed(events)))
    assert evidence['handoff']['validation_status'] == 'passed'
    assert evidence['body_result']['status'] == 'body_package_verified'
    assert evidence['stage_work_files']
