from pathlib import Path


def test_doc_attachment_converts_temporarily_and_preserves_original(tmp_path, monkeypatch):
    from docx import Document
    import direct_source
    from attachment_preview import preview_attachment
    source = tmp_path/'attachment.doc'; source.write_bytes(b'original attachment')
    converted_paths = []
    def convert(actual, private, kind, conversion_dir):
        assert Path(actual) == source and kind == 'doc'
        path = Path(conversion_dir)/'converted.docx'
        doc = Document(); doc.add_paragraph('附件中的可用文字。'); doc.save(path)
        converted_paths.append(path)
        return path, 'docx'
    monkeypatch.setattr(direct_source, 'prepare_readable_source', convert)
    preview = preview_attachment(source, 'doc')
    assert preview['status'] == 'preview' and '附件中的可用文字' in preview['text']
    assert converted_paths and not converted_paths[0].exists()
    assert source.read_bytes() == b'original attachment'


def test_doc_attachment_conversion_failure_is_local_warning(tmp_path, monkeypatch):
    import direct_source
    from attachment_preview import preview_attachment
    source = tmp_path/'attachment.doc'; source.write_bytes(b'original attachment')
    def fail(*args):raise RuntimeError('cannot convert')
    monkeypatch.setattr(direct_source, 'prepare_readable_source', fail)
    preview = preview_attachment(source, 'doc')
    assert preview['status'] == 'unavailable'
    assert source.read_bytes() == b'original attachment'
