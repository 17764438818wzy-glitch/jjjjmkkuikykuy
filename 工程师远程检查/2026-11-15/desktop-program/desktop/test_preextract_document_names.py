"""Desktop title naming at the existing sealed-package delivery boundary."""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from auto_ragflow import handoff
from test_preextract_handoff import memory_client, prepared_batch
from test_preextract_publish import PAPERFLOW, fixture, prepare


def _files(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}


def test_desktop_sets_title_before_chunks_and_retry_reuses_documents(tmp_path, monkeypatch):
    store, item, prepared, work = prepared_batch(tmp_path, image=True)
    package_root = Path(prepared['package_path'])
    protected = _files(package_root)
    client = memory_client(monkeypatch)
    request = client.request
    observed = set()

    def checked(method, endpoint, body=None, content_type=None):
        parts = urlsplit(endpoint).path.strip('/').split('/')
        if method == 'POST' and parts[-1] == 'chunks':
            identity = (parts[1], parts[3])
            assert client.docs[identity]['name'].startswith('控制模型研究__')
            observed.add(identity)
        return request(method, endpoint, body, content_type)

    client.request = checked
    handoff(store, 'batch', client, {})
    receipt = json.loads((work / 'delivery-receipt.json').read_text(encoding='utf8'))
    assert receipt['status'] == 'complete'
    assert len(observed) == len(client.docs) == 4
    assert len({doc['name'] for doc in client.docs.values()}) == 4
    for target in receipt['targets'].values():
        for role, label in [('body', '正文'), ('images', '资料')]:
            obj = target[role]
            assert obj['upload_name'].startswith('PF_')
            assert obj['name'].startswith('控制模型研究__')
            assert obj['name'].endswith('_' + label + '.md')
            assert obj['name'] == client.docs[(obj['dataset_id'], obj['document_id'])]['name']
    writes = client.writes
    handoff(store, 'batch', client, {})
    assert client.writes == writes
    assert _files(package_root) == protected


def test_title_naming_uses_redacted_title_and_existing_filename_sanitizer(tmp_path):
    candidate, bundle, _ = fixture()
    candidate['profile']['article']['title'] = '张三:控制/模型研究?'
    candidate['redaction_plan'].append(dict(id='R2', entity_key='person1',
        scope='generated_field', action='replace', replacement='研究者甲',
        expected_text='张三', target_pointer='/profile/article/title', location_status='located'))
    original_name = bundle['source']['file']
    result = prepare(tmp_path, candidate, bundle)
    article = result['packet']['profile']['article']
    assert article['public_file_name'] == '研究者甲_控制_模型研究_.md'
    assert article['document_name_policy'] == 'paper-title/1'
    from paperflow.rag import RagPublisher
    upload, display = RagPublisher.document_names(result['task'], result['packet'], '080801', 'body')
    assert upload.startswith('PF_')
    assert display.startswith('研究者甲_控制_模型研究___')
    assert '张三' not in display
    assert bundle['source']['file'] == original_name


def test_existing_sealed_package_keeps_its_original_naming_policy(tmp_path, monkeypatch):
    from preextract_publish import _paperflow
    package = _paperflow(PAPERFLOW)
    with monkeypatch.context() as older:
        older.setattr(package, 'article_presentation', lambda *_: {})
        store, item, prepared, work = prepared_batch(tmp_path)
    root = Path(prepared['package_path'])
    protected = _files(root)
    client = memory_client(monkeypatch)
    handoff(store, 'batch', client, {})
    assert all(doc['name'].startswith('PF_') for doc in client.docs.values())
    assert _files(root) == protected
