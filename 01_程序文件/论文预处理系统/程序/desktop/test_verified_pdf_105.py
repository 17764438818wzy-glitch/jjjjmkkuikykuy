import hashlib
import json
from pathlib import Path

import fitz
import pytest


def fixture(tmp_path, monkeypatch):
    import verified_pdf
    cache = tmp_path / 'private' / 'verified_pdf'
    monkeypatch.setattr(verified_pdf, 'CACHE_ROOT', cache)
    source = tmp_path / 'source.docx'
    source.write_bytes(b'original document bytes')
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    entry = cache / source_hash
    entry.mkdir(parents=True)
    with fitz.open() as pdf:
        pdf.new_page().insert_text((50, 50), 'Verified original research text')
        pdf.save(entry / 'source.pdf')
    metadata = {'source_sha256': source_hash,
                'pdf_sha256': hashlib.sha256((entry / 'source.pdf').read_bytes()).hexdigest(),
                'page_count': 1, 'renderer': 'WPS', 'original_unchanged': True,
                'verified': True}
    (entry / 'manifest.json').write_text(json.dumps(metadata), encoding='utf-8')
    return source, entry, metadata


def test_exact_source_uses_hash_checked_private_pdf_without_rerender(tmp_path, monkeypatch):
    import verified_pdf
    source, entry, metadata = fixture(tmp_path, monkeypatch)
    original = source.read_bytes()
    path = verified_pdf.copy_verified_pdf(source, tmp_path/'work')
    assert path == tmp_path/'work'/'source.pdf'
    assert hashlib.sha256(path.read_bytes()).hexdigest() == metadata['pdf_sha256']
    assert source.read_bytes() == original
    with fitz.open(path) as pdf:
        assert len(pdf) == 1
        assert 'Verified original research text' in pdf[0].get_text()


def test_changed_source_never_reuses_old_pdf(tmp_path, monkeypatch):
    import verified_pdf
    source, _, _ = fixture(tmp_path, monkeypatch)
    source.write_bytes(b'modified content')
    assert verified_pdf.copy_verified_pdf(source, tmp_path/'work') is None
    assert not (tmp_path/'work').exists()


@pytest.mark.parametrize('field,value', [('source_sha256','wrong'), ('page_count',2), ('verified',False)])
def test_changed_or_unverified_manifest_is_not_trusted(tmp_path, monkeypatch, field, value):
    import verified_pdf
    from agent_client import AgentError
    source, entry, metadata = fixture(tmp_path, monkeypatch)
    metadata[field] = value
    (entry/'manifest.json').write_text(json.dumps(metadata), encoding='utf-8')
    with pytest.raises(AgentError): verified_pdf.copy_verified_pdf(source, tmp_path/'work')
    assert not (tmp_path/'work'/'source.pdf').exists()


def test_changed_pdf_is_not_used(tmp_path, monkeypatch):
    import verified_pdf
    from agent_client import AgentError
    source, entry, _ = fixture(tmp_path, monkeypatch)
    with (entry/'source.pdf').open('ab') as stream: stream.write(b'changed')
    with pytest.raises(AgentError): verified_pdf.copy_verified_pdf(source, tmp_path/'work')
    assert not (tmp_path/'work'/'source.pdf').exists()


def test_pdf_adapter_uses_verified_docx_render_without_calling_office(tmp_path, monkeypatch):
    import evidence_tasks
    source, _, _ = fixture(tmp_path, monkeypatch)
    def forbidden(*args, **kwargs): raise AssertionError('Office rerendered exact verified source')
    monkeypatch.setattr(evidence_tasks.subprocess, 'run', forbidden)
    with evidence_tasks._pdf(source, tmp_path/'work') as pdf:
        assert 'Verified original research text' in pdf[0].get_text()


def mapping_fixture():
    import verified_pdf
    block = {'id':'block_0001', 'type':'paragraph', 'position':{'body_index':4},
             'graphic_members':[{'text':'Graph label', 'position':{'object_ref':'object_0001'}}]}
    issue = {'code':'embedded_object_review', 'position':{'body_index':4}}
    record = {**issue, 'block_id':block['id'], 'block_signature_sha256':verified_pdf.block_signature(block),
              'verified_pages':[3]}
    return {'verified_issue_pages':[record]}, issue, block


def test_exact_verified_object_mapping_retains_all_reviewed_pages():
    import verified_pdf
    info, issue, block = mapping_fixture()
    assert verified_pdf.verified_issue_pages(info, issue, [block], 8) == {2}


def test_changed_member_content_refuses_manual_object_mapping():
    import verified_pdf
    from agent_client import AgentError
    info, issue, block = mapping_fixture()
    block['graphic_members'].append({'text':'New member'})
    with pytest.raises(AgentError):
        verified_pdf.verified_issue_pages(info, issue, [block], 8)


@pytest.mark.parametrize('pages', [[0], [9], [1,2,3,4,5], [True]])
def test_verified_object_mapping_rejects_out_of_bounds_or_oversized_scope(pages):
    import verified_pdf
    from agent_client import AgentError
    info, issue, block = mapping_fixture()
    info['verified_issue_pages'][0]['verified_pages'] = pages
    with pytest.raises(AgentError):
        verified_pdf.verified_issue_pages(info, issue, [block], 8)


def test_unlisted_issue_uses_normal_locator():
    import verified_pdf
    info, issue, block = mapping_fixture()
    issue['position']['body_index'] = 5
    # Do not alias the issue position in the record.
    info['verified_issue_pages'][0]['position'] = {'body_index':4}
    assert verified_pdf.verified_issue_pages(info, issue, [block], 8) is None
