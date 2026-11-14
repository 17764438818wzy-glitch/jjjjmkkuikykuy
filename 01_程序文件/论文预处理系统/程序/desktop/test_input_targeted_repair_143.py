import io
import struct
import zipfile
import zlib
from pathlib import Path

import pytest

from test_archive_input_105 import block, vint


def rar_member(name, payload):
    encoded = name.encode('utf-8')
    header = vint(2) + vint(2) + vint(len(payload))
    header += vint(4) + vint(len(payload)) + vint(32) + struct.pack('<L', zlib.crc32(payload))
    header += vint(0) + vint(0) + vint(len(encoded)) + encoded
    return block(header, payload)


def make_rar(path, members):
    with path.open('wb') as stream:
        stream.write(b'Rar!\x1a\x07\x01\0' + block(b'\x01\x00\x00'))
        for name, payload in members:
            stream.write(rar_member(name, payload))
        stream.write(block(b'\x05\x00\x00'))
    return path


def test_large_video_only_rar_is_auxiliary_not_technical_failure(tmp_path):
    # A real stored member exceeds the document cap, as do the reported videos.
    from archive_input import extract_rar
    source = make_rar(tmp_path/'video.rar', [('lesson.mp4', b'\0'*(101*1024**2))])
    rows = extract_rar(source, tmp_path, tmp_path/'out')
    assert rows == []
    assert not (tmp_path/'out').exists()


def test_mixed_rar_preserves_thesis_but_does_not_extract_video(tmp_path):
    from archive_input import extract_rar
    source = make_rar(tmp_path/'mixed.rar', [('lesson.mp4', b'video'), ('paper.txt', b'original thesis evidence')])
    rows = extract_rar(source, tmp_path, tmp_path/'out')
    assert [row['inner_path'] for row in rows] == ['paper.txt']
    assert Path(rows[0]['extracted_path']).read_bytes() == b'original thesis evidence'
    assert not list((tmp_path/'out').rglob('*.mp4'))


def test_rar_never_screens_out_nested_archive_as_auxiliary(tmp_path):
    from archive_input import extract_rar
    source = make_rar(tmp_path/'nested.rar', [('paper.zip', b'not a decoded archive')])
    with pytest.raises(ValueError, match='嵌套'):
        extract_rar(source, tmp_path, tmp_path/'out')


def test_rar_still_rejects_oversized_thesis_instead_of_dropping_it(tmp_path):
    from archive_input import extract_rar
    source = make_rar(tmp_path/'large.rar', [('paper.txt', b'\0'*(101*1024**2))])
    with pytest.raises(ValueError, match='过大'):
        extract_rar(source, tmp_path, tmp_path/'out')


def test_auxiliary_paths_are_validated_even_when_not_extracted(tmp_path):
    from archive_input import extract_rar
    source = make_rar(tmp_path/'unsafe.rar', [('../lesson.mp4', b'video')])
    with pytest.raises(ValueError, match='不安全路径'):
        extract_rar(source, tmp_path, tmp_path/'out')
    assert not (tmp_path/'out').exists()


def docx_bytes():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, 'w') as archive:
        archive.writestr('word/document.xml', '<document/>')
    return stream.getvalue()


def compound_with_zip(encrypted=False):
    header = bytes.fromhex('d0cf11e0a1b11ae1') + bytes(504)
    records = []
    for name in (['EncryptionInfo', 'EncryptedPackage'] if encrypted else ['WordDocument']):
        record = bytearray(128)
        encoded = (name+'\0').encode('utf-16le')
        record[:len(encoded)] = encoded
        record[64:66] = len(encoded).to_bytes(2, 'little')
        record[66] = 2
        records.append(bytes(record))
    return header + b''.join(records) + docx_bytes()


def test_docx_validation_checks_container_magic_before_embedded_zip(tmp_path):
    from office_input import validate_docx_container
    from agent_client import AgentError
    source = tmp_path/'wrong.docx'
    source.write_bytes(compound_with_zip())
    with pytest.raises(AgentError, match='旧版 Office'):
        validate_docx_container(source)


def test_mislabeled_legacy_word_converts_only_private_copy(tmp_path, monkeypatch):
    from office_input import normalize_docx_container
    import engine
    source = tmp_path/'original.docx'
    before = compound_with_zip()
    source.write_bytes(before)
    # LibreOffice is the external boundary; its real conversion is also replayed
    # against the affected source in the saved diagnosis, with no source edits.
    def convert(private_source, output):
        assert private_source.suffix == '.doc'
        assert private_source != source and private_source.read_bytes() == before
        output.mkdir(parents=True)
        result = output/'source.docx'
        result.write_bytes(docx_bytes())
        return result
    monkeypatch.setattr(engine, 'convert_office', convert)
    result = normalize_docx_container(source, tmp_path/'private')
    assert result != source and result.read_bytes() == docx_bytes()
    assert source.read_bytes() == before


def test_encrypted_office_is_not_sent_to_conversion(tmp_path, monkeypatch):
    from office_input import normalize_docx_container
    from agent_client import AgentError
    import engine
    source = tmp_path/'encrypted.docx'
    before = compound_with_zip(encrypted=True)
    source.write_bytes(before)
    monkeypatch.setattr(engine, 'convert_office', lambda *_: pytest.fail('encrypted document must not be converted'))
    with pytest.raises(AgentError, match='加密'):
        normalize_docx_container(source, tmp_path/'private')
    assert source.read_bytes() == before
    assert not (tmp_path/'private').exists()


def test_normal_docx_does_not_need_conversion(tmp_path, monkeypatch):
    from office_input import normalize_docx_container
    import engine
    source = tmp_path/'paper.docx'
    source.write_bytes(docx_bytes())
    monkeypatch.setattr(engine, 'convert_office', lambda *_: pytest.fail('normal DOCX must not be converted'))
    assert normalize_docx_container(source, tmp_path/'private') == source
    assert not (tmp_path/'private').exists()
