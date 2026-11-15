"""Two real frozen request parts, with synthetic model transport only."""
import asyncio
from contextlib import contextmanager
import io
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_body_adapter import fixture, save
from paperflow_prefect.body_clean_handoff import save_projection
from paperflow_prefect.records import read_json
from paperflow_prefect.llm_queue.frozen_body_plan import _metadata
from preextract_contract import load_template
from preextract_partition_run import create_plan


class Response(io.BytesIO):
    headers = {'Content-Type': 'application/json'}


def test_partitioned_body_resumes_after_pause_without_resending_completed_part(tmp_path, monkeypatch):
    from paperflow_v3.stages import llm_operations
    if not hasattr(llm_operations, 'run_partitioned_body'):
        pytest.fail('The concrete partitioned-body adapter is missing')
    evidence, catalog = fixture(tmp_path, parts=2)
    identity = evidence['identity']
    source = dict(file_id='body-file', kind='body', relative='body.txt', **evidence['source'])
    paper = {'paper_id': identity['paper_id'], 'body': source, 'attachments': []}
    work = tmp_path / 'partition-work'
    work.mkdir()
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1',
        **{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        body=source, attachments=[], body_work=str(work), material=evidence['material'],
        reading_material=evidence['reading_material'],
        capacity={'status': 'planned', 'mode': 'partitioned', 'parts': 2,
                  'plan_identity': evidence['plan_identity']})
    plan = read_json(evidence['plan']['path'])
    material = read_json(evidence['material']['path'])
    reading = read_json(evidence['reading_material']['path'])
    template, _ = load_template()
    connection = {'model': 'synthetic-never-sent', 'base_url': 'http://synthetic.invalid',
                  'api_key': 'synthetic'}
    save(work / 'private/llm-frozen-plan-metadata.json', _metadata(plan, evidence['plan_identity']))
    save_projection(work, material, reading)
    create_plan(plan, material, reading, work, template, connection)
    events = []

    async def read_receipts(_child):
        return list(reversed(events))

    def save_sync(**row):
        events.append(row)

    async def save_async(**row):
        events.append(row)

    @contextmanager
    def isolated_slot(admission, _limits):
        if hasattr(admission, 'admit_sync'):
            assert admission.admit_sync()
        if not admission.running_sync():
            raise InterruptedError('paused')
        yield

    monkeypatch.setattr(llm_operations, 'read_receipts', read_receipts)
    monkeypatch.setattr(llm_operations,'read_receipts_sync',lambda _child:list(reversed(events)))
    monkeypatch.setattr(llm_operations, 'save_receipt_sync', save_sync)
    monkeypatch.setattr(llm_operations, 'save_receipt', save_async)
    monkeypatch.setattr(llm_operations, 'admitted_sync_slot', isolated_slot)

    class Admission:
        active = True
        def running_sync(self):
            return self.active

    admission = Admission()

    class Settings:
        def path(self, key):
            assert key == 'template_path'
            return WORKSPACE.parent / '程序/resources/论文预提取_空白模板_v1.md'

    class Backend:
        root = tmp_path / 'backend'
        settings = Settings()
        def body_work(self, _paper):
            return work
        def confirm_history(self, _paper, _row):
            pass

    calls = []
    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            if len(calls) == 1:
                admission.active = False
            return Response(json.dumps({'choices': [{'message': {'content': f'part-{len(calls)}'},
                'finish_reason': 'stop'}]}).encode())

    options = dict(identity=identity, manifest=manifest, paper=paper, backend=Backend(),
                   connection=connection, admission=admission, limit_names=['synthetic'], opener=Opener())
    with pytest.raises(InterruptedError):
        asyncio.run(llm_operations.run_partitioned_body(**options))
    assert len(calls) == 1
    assert (work / 'model/part-001/response.txt').read_text(encoding='utf8') == 'part-1'
    admission.active = True
    result = asyncio.run(llm_operations.run_partitioned_body(**options))
    assert result['status'] == 'body_parts_raw_saved'
    assert len(calls) == 2
    assert (work / 'model/part-001/response.txt').read_text(encoding='utf8') == 'part-1'
    assert (work / 'model/part-002/response.txt').read_text(encoding='utf8') == 'part-2'
    for event in (row for row in events if row['kind'] == 'llm_response_saved'):
        assert event['data']['response_text'].encode('utf8') == Path(event['data']['response_path']).read_bytes()
    started = [row for row in events if row['kind'] == 'llm_request_started']
    assert len(started) == 2 and all(row['data']['kind'] == 'body' for row in started)
