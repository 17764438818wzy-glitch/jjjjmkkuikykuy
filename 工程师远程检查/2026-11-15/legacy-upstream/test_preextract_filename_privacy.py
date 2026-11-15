"""Original filenames stay local, including at the last delivery boundary."""
import copy
import json
from pathlib import Path

import pytest

from preextract_publish import deliver_publication
from test_preextract_handoff import memory_client
from test_preextract_publish import fixture, prepare


PRIVATE_NAME = '学号202600123_张三_控制模型研究.docx'


@pytest.mark.parametrize('field', ['title', 'summary', 'locator_hint', 'body'])
def test_original_basename_in_public_content_prevents_sealing(tmp_path, field):
    candidate, bundle, _ = fixture()
    bundle['source']['file'] = 'C:\\private\\' + PRIVATE_NAME
    if field in {'title', 'summary'}:
        candidate['profile']['article'][field] = PRIVATE_NAME
    elif field == 'locator_hint':
        candidate['profile']['article']['source_refs'][0]['locator_hint'] = PRIVATE_NAME
    else:
        bundle['blocks'][0]['text'] += '\n' + PRIVATE_NAME
    # The source-text redaction is already tested independently; here the
    # filename itself must be blocked without a model-generated filename plan.
    candidate['redaction_plan'] = []
    with pytest.raises(ValueError, match='private source'):
        prepare(tmp_path, candidate, bundle)
    assert not (tmp_path / 'public' / 'READY.json').exists()


def test_authoritative_original_filename_is_checked_when_source_is_a_private_copy(tmp_path):
    candidate, bundle, _ = fixture()
    candidate['redaction_plan'] = []
    bundle['source']['file'] = 'C:\\private\\original.docx'
    bundle['original_filename'] = PRIVATE_NAME
    candidate['profile']['article']['summary'] = PRIVATE_NAME
    with pytest.raises(ValueError, match='private source'):
        prepare(tmp_path, candidate, bundle)


@pytest.mark.parametrize('literal,blocked', [(PRIVATE_NAME, True), ('张三', True), ('等待时间', False)])
def test_best_effort_filename_plan_only_blocks_verified_original_identity(tmp_path, literal, blocked):
    from preextract_contract import validate_candidate
    from preextract_publish import prepare_publication
    from test_preextract_contract import fixture as contract_fixture
    from test_preextract_publish import Counter, PAPERFLOW
    candidate, bundle, catalog = contract_fixture()
    catalog['majors'][0].update(paper_dataset_id='paper_ds', image_dataset_id='image_ds')
    bundle['original_filename'] = PRIVATE_NAME
    candidate['profile']['article']['summary'] = literal
    candidate['redaction_plan'] = [dict(scope='filename', expected_text=literal)]
    validated = validate_candidate(candidate, bundle, catalog, redaction_policy='best_effort')
    assert validated['valid']

    def publish():
        return prepare_publication(validated['candidate'], bundle, tmp_path / 'public',
            paperflow_root=PAPERFLOW, catalog=catalog, counter=Counter(),
            redaction_policy='best_effort', candidate_validation=validated)

    if blocked:
        with pytest.raises(ValueError, match='private source'):
            publish()
        assert not (tmp_path / 'public' / 'READY.json').exists()
    else:
        assert publish()['validation']['embedding']['admission_verified']


def test_last_delivery_checks_material_before_any_remote_write(tmp_path, monkeypatch):
    candidate, bundle, _ = fixture()
    candidate['redaction_plan'] = []
    # Model an older sealed package made before the filename check existed.
    candidate['profile']['article']['summary'] = PRIVATE_NAME
    bundle['source']['file'] = 'C:\\private\\original.docx'
    prepared = prepare(tmp_path, candidate, bundle)
    material = copy.deepcopy(bundle)
    material['original_filename'] = PRIVATE_NAME
    (tmp_path / 'material.json').write_text(json.dumps(material, ensure_ascii=False), encoding='utf8')
    client = memory_client(monkeypatch)
    before = {p.name: p.read_bytes() for p in Path(prepared['package_path']).iterdir() if p.is_file()}
    with pytest.raises(ValueError, match='private source'):
        deliver_publication(prepared, config=None, receipt_path=tmp_path / 'delivery-receipt.json', client=client)
    assert client.docs == {} and client.writes == 0
    assert not (tmp_path / 'delivery-receipt.json').exists()
    assert before == {p.name: p.read_bytes() for p in Path(prepared['package_path']).iterdir() if p.is_file()}


def test_unicode_escaped_cached_metadata_cannot_leak_original_name(tmp_path, monkeypatch):
    from preextract_publish import _paperflow
    candidate, bundle, _ = fixture()
    candidate['redaction_plan'] = []
    candidate['profile']['article']['summary'] = PRIVATE_NAME
    bundle['source']['file'] = 'C:\\private\\original.docx'
    prepared = prepare(tmp_path, candidate, bundle)
    root = Path(prepared['package_path'])
    # Reproduce a valid older package serialized with JSON's default Unicode
    # escapes. The live publisher decodes these values before sending metadata.
    for path in root.rglob('*.json'):
        path.write_text(json.dumps(json.loads(path.read_text(encoding='utf8')), ensure_ascii=True), encoding='utf8')
    package = _paperflow(prepared['paperflow_root'])
    package.seal_package(root, prepared['task'], 1)
    packet = package.validate_package(root, prepared['task'], prepared['catalog'], strict_seal=True)
    prepared['validation']['manifest_sha256'] = packet['manifest_sha256']
    assert PRIVATE_NAME not in (root / 'profile.json').read_text(encoding='utf8')
    assert packet['profile']['article']['summary'] == PRIVATE_NAME
    material = copy.deepcopy(bundle)
    material['original_filename'] = PRIVATE_NAME
    (tmp_path / 'material.json').write_text(json.dumps(material, ensure_ascii=False), encoding='utf8')
    client = memory_client(monkeypatch)
    with pytest.raises(ValueError, match='private source'):
        deliver_publication(prepared, config=None, receipt_path=tmp_path / 'delivery-receipt.json', client=client)
    assert client.docs == {} and client.writes == 0
    assert not (tmp_path / 'delivery-receipt.json').exists()


@pytest.mark.parametrize('invalid', ['json', 'version', 'source', 'hash'])
def test_existing_invalid_private_material_cannot_bypass_delivery_guard(tmp_path, monkeypatch, invalid):
    prepared = prepare(tmp_path)
    _, bundle, _ = fixture()
    if invalid == 'version':
        bundle['version'] = 'unknown'
    elif invalid == 'source':
        bundle['source'] = []
    elif invalid == 'hash':
        bundle['source']['source_sha256'] = 'b' * 64
    raw = '{' if invalid == 'json' else json.dumps(bundle, ensure_ascii=False)
    (tmp_path / 'material.json').write_text(raw, encoding='utf8')
    client = memory_client(monkeypatch)
    with pytest.raises(ValueError):
        deliver_publication(prepared, config=None, receipt_path=tmp_path / 'delivery-receipt.json', client=client)
    assert client.docs == {} and client.writes == 0


def test_outgoing_documents_chunks_and_metadata_omit_original_filename(tmp_path, monkeypatch):
    candidate, bundle, _ = fixture()
    original_path = 'C:\\private\\' + PRIVATE_NAME
    bundle['source']['file'] = original_path
    bundle['original_filename'] = PRIVATE_NAME
    prepared = prepare(tmp_path, candidate, bundle)
    (tmp_path / 'material.json').write_text(json.dumps(bundle, ensure_ascii=False), encoding='utf8')
    client = memory_client(monkeypatch)
    payloads = []
    request, upload = client.request, client.upload_bytes

    def checked_request(method, endpoint, body=None, content_type=None):
        payloads.append(json.dumps(body, ensure_ascii=False))
        return request(method, endpoint, body, content_type)

    def checked_upload(dataset, name, blob):
        payloads.extend([name, blob.decode('utf8')])
        return upload(dataset, name, blob)

    client.request, client.upload_bytes = checked_request, checked_upload
    receipt = deliver_publication(prepared, config=None, receipt_path=tmp_path / 'delivery-receipt.json', client=client)
    assert receipt['status'] == 'complete'
    assert all(PRIVATE_NAME not in text and '202600123' not in text and '张三' not in text for text in payloads)
    assert all(doc['name'].startswith('控制模型研究__') for doc in client.docs.values())
    assert bundle['source']['file'] == original_path
