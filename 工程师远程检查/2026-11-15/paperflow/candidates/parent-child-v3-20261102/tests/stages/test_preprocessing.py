"""Synthetic H2.1 input through the existing local document parser only."""
import io
import json
from pathlib import Path
import sys

import pytest
from docx import Document

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'prefect-paperflow' / 'tests')]

from paperflow_prefect.intake_handoff import receive_handoff
from paperflow_prefect.receipts import ReceiptStore
from paperflow_prefect.records import read_json, sha_file, write_json
from paperflow_prefect.settings import Settings
from paperflow_prefect.stage3_handoff import read_receipt
from test_receipt_h21 import handoff


@pytest.fixture
def folder(tmp_path, monkeypatch):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    monkeypatch.setattr(socket.socket, 'connect_ex', lambda *a, **k: pytest.fail('network forbidden'))
    library = tmp_path / 'library'
    library.mkdir()
    document = Document()
    document.add_heading('1 合成材料', 1)
    document.add_paragraph('用于验证单篇前处理的合成内容。')
    buffer = io.BytesIO()
    document.save(buffer)
    upstream = handoff(library, library / 'one', body=buffer.getvalue(),
                       attachments=[('附件/sample.txt', b'synthetic attachment')])
    handoff_path = tmp_path / 'handoff.json'
    write_json(handoff_path, upstream)
    received = receive_handoff(handoff_path, ReceiptStore(tmp_path / 'receipts'))
    receipt_path = Path(received['record_path'])
    receipt, paper = read_receipt(receipt_path)
    model = tmp_path / 'model.json'
    catalog = tmp_path / 'catalog.json'
    write_json(model, {'text_model': 'synthetic'})
    write_json(catalog, {'schema': 'specialty-routing/1.0', 'majors': []})
    settings = Settings(dict(Settings.load().values, execution_enabled=True,
        library_root=str(library), data_root=str(tmp_path / 'data'),
        model_config=str(model), catalog_path=str(catalog), history_roots=[]))
    record = dict(task_id='synthetic-parent', group_id=receipt['group_id'],
                  paper_id=receipt['paper_id'], receipt_id=receipt['receipt_id'],
                  receipt_path=str(receipt_path), receipt_sha256=sha_file(receipt_path),
                  library_root=str(library), paper=paper)
    return settings, record


def planner(material, work, backend):
    # The tokenizer is outside this test; the existing plan contract is real.
    plan = dict(schema='preextract-input-plan/1', mode='single',
        parts=[dict(id='part-001', body={'model': 'synthetic'}, bundle={},
                    ranges=[], asset_ids=[], capacity={})],
        coverage=dict(complete=True, ranges=[], asset_ids=[], block_count=0, character_count=0))
    return material, [], {'status': 'planned', 'mode': 'single', 'parts': 1}, plan


def test_one_registered_folder_uses_real_parser_and_retains_all_attachments(folder, monkeypatch):
    try:
        from paperflow_v3.stages.preprocessing import prepare_registered_folder
    except ModuleNotFoundError:
        pytest.fail('The candidate single-folder preprocessing entry is missing')
    from paperflow_v3.stages import preprocessing
    monkeypatch.setattr(preprocessing, '_capacity_and_projection', planner)
    settings, record = folder
    result = prepare_registered_folder(settings, record, attempt_id='synthetic-attempt')
    manifest = read_json(result['manifest_path'])
    assert manifest['schema'] == 'paperflow-local-preprocessing-handoff/1'
    assert manifest['paper_id'] == record['paper_id']
    assert manifest['task_id'] == record['task_id']
    assert manifest['group_id'] == record['group_id']
    assert manifest['body'] == record['paper']['body']
    assert manifest['attachments'] == record['paper']['attachments']
    assert manifest['capacity']['plan_identity']['mode'] == 'single'
    assert Path(manifest['material']['path']).is_file()
    assert result['manifest_sha256'] == sha_file(result['manifest_path'])
    assert prepare_registered_folder(settings, record, attempt_id='synthetic-attempt') == result


def test_saved_preprocessing_is_reused_after_llm_work_has_started(folder, monkeypatch):
    from paperflow_v3.stages import preprocessing
    monkeypatch.setattr(preprocessing, '_capacity_and_projection', planner)
    settings, record = folder
    result = preprocessing.prepare_registered_folder(settings, record, attempt_id='synthetic-attempt')
    manifest = read_json(result['manifest_path'])
    model = Path(manifest['body_work']) / 'model'
    model.mkdir(exist_ok=True)
    (model / 'request-state.json').write_text(json.dumps({'status': 'request_in_flight'}), encoding='utf8')
    monkeypatch.setattr(preprocessing.Backend, 'prepare',
                        lambda *a: pytest.fail('completed preprocessing must not run again'))
    assert preprocessing.prepare_registered_folder(settings, record, attempt_id='synthetic-attempt') == result
    assert read_json(model / 'request-state.json')['status'] == 'request_in_flight'
