import pytest
from attachment_notes import enrich

def test_note_reads_explicit_raw_identity_without_guessing_parent():
    doc={'id':'note','name':'att_abcd_note.md','meta':{}}
    rows=[{'content_with_weight':'# 附件说明\n图纸附件原件：设计图册.pdf。未解析。\n原件编号：file123\n原件SHA256：'+'a'*64+'\n关联状态：待关联\n'}]
    result=enrich(doc,rows)['meta']
    assert result['paperflow_role']=='attachment-note'
    assert result['original_file_id']=='file123'
    assert result['attachment_source_sha256']=='a'*64
    assert result['original_source_name']=='设计图册.pdf'
    assert result['parent_link_status']=='unresolved'
    assert 'parent_body_document_id' not in result
    assert doc['meta']=={}

def test_note_disagreement_cannot_override_structured_original_hash():
    doc={'id':'note','name':'PFA_MD_abc.md','meta':{'paperflow_role':'attachment-card','attachment_source_sha256':'b'*64}}
    with pytest.raises(ValueError,match='hash disagree'):
        enrich(doc,[{'content_with_weight':'原件SHA256：'+'a'*64+'\n'}])
