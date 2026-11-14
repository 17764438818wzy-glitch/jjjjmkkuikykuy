import struct

import pytest

from agent_client import AgentError
from auto_pipeline import AutoProcessor, lightweight
from engine import Store


def compound_file(path, names):
    header = bytearray(512)
    header[:8] = bytes.fromhex('d0cf11e0a1b11ae1')
    struct.pack_into('<H', header, 30, 9)
    entries = []
    for name in names:
        entry = bytearray(128)
        encoded = (name+'\0').encode('utf-16le')
        entry[:len(encoded)] = encoded
        struct.pack_into('<H', entry, 64, len(encoded))
        entry[66] = 2
        entries.append(entry)
    path.write_bytes(header+b''.join(entries))
    return path


def test_encrypted_docx_has_actionable_error_before_parser_or_model(tmp_path):
    source = compound_file(tmp_path/'encrypted.docx', ['EncryptionInfo', 'EncryptedPackage'])
    before = source.read_bytes()
    with pytest.raises(AgentError, match='加密'):
        lightweight(source)
    result = AutoProcessor(Store(tmp_path/'data'), {'auto_upload': False}).run([
        {'path': str(source), 'name': source.name}])
    item = result['items'][0]
    assert item['reason_kind'] == 'technical'
    assert '加密' in item['reason'] and '解密' in item['reason']
    assert 'BadZipFile' not in item['reason']
    assert source.read_bytes() == before
    assert not (tmp_path/'data/ai_usage.jsonl').exists()


def test_legacy_compound_file_is_not_misreported_as_encrypted(tmp_path):
    source = compound_file(tmp_path/'wrong_extension.docx', ['WordDocument'])
    with pytest.raises(AgentError, match='实际格式') as exc:
        lightweight(source)
    assert '加密' not in str(exc.value)


def test_invalid_docx_has_clear_container_error(tmp_path):
    source = tmp_path/'broken.docx'; source.write_bytes(b'not a DOCX')
    with pytest.raises(AgentError, match='DOCX') as exc:
        lightweight(source)
    assert 'BadZipFile' not in str(exc.value)


def test_valid_docx_is_still_parsed(tmp_path):
    from docx import Document
    source = tmp_path/'valid.docx'
    doc = Document(); doc.add_paragraph('Original research content'); doc.save(source)
    text, reliable = lightweight(source)
    assert 'Original research content' in text and reliable
