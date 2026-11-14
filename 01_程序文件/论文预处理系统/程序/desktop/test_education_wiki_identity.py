import pytest

from education_wiki_identity import bind_identity


DOC = '1' * 32
CHUNK = '2' * 16
PAPER = 'paper_' + '3' * 24
INFO = {DOC: {'stable_paper_id': PAPER, 'title': '课堂调查',
              'admission': '入库', 'example_tier': '普通本科'}}
PAGE = {'content_md_rendered': f'#### L4 范例论文\n\n稳定paper_id未获得，记为待核实。\n\n事实（SOURCE {CHUNK}）',
        'source_doc_ids': [DOC], 'source_chunk_ids': [CHUNK]}


def test_identity_binding_is_reproducible_and_replaces_unknown_claim():
    first = bind_identity(PAGE, INFO, {CHUNK: DOC})
    assert PAPER in first
    assert '稳定paper_id未获得' not in first
    assert '范例候选档位：普通本科' in first
    assert bind_identity({**PAGE, 'content_md_rendered': first}, INFO, {CHUNK: DOC}) == first


def test_identity_binding_requires_real_body_citation_and_admission():
    with pytest.raises(ValueError, match='不可作为证据'):
        bind_identity(PAGE, INFO, {})
    with pytest.raises(ValueError, match='未通过准入'):
        bind_identity(PAGE, {DOC: {**INFO[DOC], 'admission': '剔除'}}, {CHUNK: DOC})
    second_doc = '4' * 32
    with pytest.raises(ValueError, match='未被正文引文覆盖'):
        bind_identity({**PAGE, 'source_doc_ids': [DOC, second_doc]},
                      {**INFO, second_doc: {**INFO[DOC], 'stable_paper_id': 'paper_' + '5' * 24}},
                      {CHUNK: DOC})
