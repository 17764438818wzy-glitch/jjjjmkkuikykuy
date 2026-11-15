"""Reuse only a registered folder's own valid first-step conversion."""
import io
from pathlib import Path
import socket

import pytest
from docx import Document

from paperflow_prefect.intake_handoff import receive_handoff
from paperflow_prefect.receipts import ReceiptStore
from paperflow_prefect.records import identity, read_json, sha_file, write_json
from paperflow_prefect.settings import Settings
from paperflow_prefect.stage3_handoff import read_receipt
from test_preprocessing import planner
from test_receipt_h21 import handoff


def _docx(text):
    document = Document()
    document.add_heading('1 合成正文', 1)
    document.add_paragraph(text)
    buffer = io.BytesIO()
    document.save(buffer)
    return buffer.getvalue()


@pytest.mark.parametrize('case, kind', [
    ('libreoffice', 'doc'), ('wps', 'doc'), ('missing', 'doc'),
    ('other_group', 'doc'), ('other_source', 'doc'), ('corrupt', 'doc'),
    ('libreoffice', 'rtf'), ('wps', 'wps'), ('libreoffice', 'odt'),
])
def test_registered_original_uses_only_its_valid_cached_conversion(tmp_path, monkeypatch, case, kind):
    from paperflow_v3.stages import preprocessing
    import direct_source
    import engine

    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    monkeypatch.setattr(socket.socket, 'connect_ex', lambda *a, **k: pytest.fail('network forbidden'))
    monkeypatch.setattr(preprocessing, '_capacity_and_projection', planner)
    # The parser's real Office format detector is outside this synthetic
    # conversion-handoff test; no actual Office process is started.
    monkeypatch.setattr(direct_source, 'detect_format', lambda _source: (kind, 'fixture'))

    library = tmp_path / 'library'
    library.mkdir()
    original_bytes = b'synthetic original legacy Word document'
    upstream = handoff(library, library / 'one', body=original_bytes)
    body = upstream['body']
    source = Path(body['path']).with_suffix('.' + kind)
    Path(body['path']).rename(source)
    relative = '正文/original.' + kind
    body.update(path=str(source), relative=relative,
                file_id=identity([upstream['group_id'], relative])[:24])
    upstream['handoff_id'] = identity(['group-handoff/1', upstream['group_id'],
        [(body['relative'], body['bytes'], body['sha256'])]])[:32]
    upstream['result_version'] = identity({k: v for k, v in upstream.items() if k != 'result_version'})[:32]

    data_root = tmp_path / 'data'
    first_root = data_root / 'paperflow-v3' / 'synthetic-parent' / 'first-step'
    group_root = first_root / upstream['group_id']
    handoff_path = tmp_path / 'handoff.json'
    write_json(handoff_path, upstream)
    received = receive_handoff(handoff_path, ReceiptStore(group_root / 'registration'))
    receipt_path = Path(received['record_path'])
    receipt, paper = read_receipt(receipt_path)
    model, catalog = tmp_path / 'model.json', tmp_path / 'catalog.json'
    write_json(model, {'text_model': 'synthetic'})
    write_json(catalog, {'schema': 'specialty-routing/1.0', 'majors': []})
    settings = Settings(dict(Settings.load().values, execution_enabled=True,
        library_root=str(library), data_root=str(data_root), model_config=str(model),
        catalog_path=str(catalog), history_roots=[]))
    registered = dict(task_id='synthetic-parent', group_id=receipt['group_id'],
        paper_id=receipt['paper_id'], receipt_id=receipt['receipt_id'],
        receipt_path=str(receipt_path), receipt_sha256=sha_file(receipt_path),
        library_root=str(library), paper=paper)

    cached_text = '这是第一步已有的合成转换内容。'
    fallback_text = '这是现有转换入口返回的合成内容。'
    converted = _docx(cached_text)
    if case != 'missing':
        owner = first_root / 'another-group' if case == 'other_group' else group_root
        converter = 'wps' if case == 'wps' else 'libreoffice'
        cache = owner / 'conversion/office/private/material-conversion/cache' / converter / 'existing-key'
        cache.mkdir(parents=True)
        (cache / 'content.docx').write_bytes(converted)
        write_json(cache / 'identity.json', {'engine': converter,
            'source_sha256': '0' * 64 if case == 'other_source' else body['sha256'],
            'sha256': sha_file(cache / 'content.docx')})
        if case == 'corrupt':
            (cache / 'content.docx').write_bytes(b'damaged cache')

    fallback_calls = []
    def existing_conversion(_source, output):
        fallback_calls.append(True)
        output = Path(output)
        output.mkdir(parents=True, exist_ok=True)
        path = output / 'converted.docx'
        path.write_bytes(_docx(fallback_text))
        return path
    monkeypatch.setattr(engine, 'convert_office', existing_conversion)
    result = preprocessing.prepare_registered_folder(settings, registered,
        attempt_id='synthetic-attempt', first_step_work_root=first_root)
    manifest = read_json(result['manifest_path'])
    material = read_json(manifest['material']['path'])
    text = Path(material['source']['text_file']).read_text(encoding='utf8')
    hit = case in {'libreoffice', 'wps'}
    assert (cached_text if hit else fallback_text) in text
    assert len(fallback_calls) == (0 if hit else 1)
    assert manifest['body'] == paper['body']
    assert paper['body']['path'].endswith('.' + kind)
    assert material['source']['source_sha256'] == body['sha256']
    assert source.read_bytes() == original_bytes
    assert any(row['path'].endswith('.' + kind) for row in paper['archive_files'])
