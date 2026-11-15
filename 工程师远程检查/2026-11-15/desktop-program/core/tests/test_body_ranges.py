from pathlib import Path
import pytest
from paperflow.package import PackageError,load,save,materialize_body_chunks


def test_program_executes_model_selected_line_boundaries_without_rewriting_text(tmp_path):
    article='# 方法\n\n第一步获取新材料。\n第二步解释关系。\n'
    (tmp_path/'article.redacted.md').write_text(article,encoding='utf-8')
    save(tmp_path/'chunks.json',dict(chunks=[
        dict(id='b1',kind='body',line_start=1,line_end=3,section_ids=['s1']),
        dict(id='b2',kind='body',line_start=4,line_end=4,section_ids=['s1']),
        dict(id='l1',kind='learning',content='LLM方法说明')]))
    materialize_body_chunks(tmp_path)
    rows=load(tmp_path/'chunks.json')['chunks']
    assert ''.join(r['content'] for r in rows if r['kind']=='body')==article
    assert rows[-1]['content']=='LLM方法说明'
    assert [r['id'] for r in rows]==['b1','b2','l1']


def test_missing_range_fails_instead_of_silently_inventing_a_boundary(tmp_path):
    (tmp_path/'article.redacted.md').write_text('one\ntwo\n',encoding='utf-8')
    save(tmp_path/'chunks.json',dict(chunks=[dict(id='b1',kind='body',line_start=2,line_end=2)]))
    with pytest.raises(PackageError,match='contiguous'):materialize_body_chunks(tmp_path)
