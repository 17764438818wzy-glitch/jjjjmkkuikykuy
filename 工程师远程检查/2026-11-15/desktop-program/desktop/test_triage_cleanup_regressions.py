"""Regressions for body loss and false rejections found in the real batch."""
import pytest


@pytest.mark.parametrize('separator',[' ', '\t'])
def test_embedded_supervisor_cover_field_is_not_a_research_title(separator):
    from content_rules import valid_title
    assert not valid_title('乡村教育现状研究 指导教师'+separator+'张某')

from content_rules import remove_acknowledgements, valid_title, detect_title
from evidence import retained_blocks
from human_view import _rebase


def paragraphs(*texts):
    return [{'type': 'paragraph', 'text': text, 'position': {'line': n}}
            for n, text in enumerate(texts)]


def test_late_summary_sentence_is_not_a_new_start_of_the_paper():
    blocks = paragraphs('同语构式研究', '研究设计与实际观察结果。',
                        '绪论部分，介绍了本文选题背景和意义。', '参考文献', '[1]相关文献')
    assert retained_blocks(blocks) == blocks


@pytest.mark.parametrize('heading', ['一、引言', '1绪论', '1.1研究背景'])
def test_bare_toc_acknowledgement_does_not_delete_following_chapter(heading):
    text = '摘要\n研究概述。\n目录\n致谢\n' + heading + '\n实际研究正文。\n致谢\n感谢导师张某。\n参考文献\n引用文献。'
    cleaned, _ = remove_acknowledgements(text)
    kept = '\n'.join(b['text'] for b in retained_blocks(paragraphs(*text.splitlines())))
    for value in [cleaned, kept]:
        assert heading in value and '实际研究正文。' in value
        assert '感谢导师张某。' not in value
        assert '引用文献。' in value


def test_colon_page_number_is_a_toc_entry_not_an_ack_section():
    text = '摘要\n研究概述。\n目录\n致谢：\t15\n1绪论\n研究正文。\n致谢：\n感谢家人。'
    cleaned, count = remove_acknowledgements(text)
    assert '研究正文。' in cleaned
    assert '感谢家人。' not in cleaned
    assert count == 1


def test_numbered_thanks_are_not_mistaken_for_new_body_chapters():
    text = '研究正文。\n致谢\n一、感谢导师张某\n1.1感谢家人\n感谢同学李某\n参考文献\n引用。'
    cleaned, _ = remove_acknowledgements(text)
    kept = '\n'.join(b['text'] for b in retained_blocks(paragraphs(*text.splitlines())))
    for value in [cleaned, kept]:
        assert '感谢' not in value and '张某' not in value and '李某' not in value
        assert '研究正文。' in value and '引用。' in value


def test_research_title_can_discuss_supervising_teachers():
    title = '幼儿园实习指导教师指导现状的调查研究'
    assert valid_title(title)
    assert detect_title('题目：'+title+'\n指导教师：张某\n摘要\n研究正文') == title


@pytest.mark.parametrize('title', ['指导教师：张某', '研究现状 指导教师：张某', '学生姓名：李某',
                                 '研究现状 学号：20200123456'])
def test_explicit_identity_fields_are_not_promoted_to_research_titles(title):
    assert not valid_title(title)


@pytest.mark.parametrize('text', [
    '[3](苏)苏霍姆林斯基.教育研究[M].',
    '[12](英)作者.绘本教学研究[M].',
    '文献研究[M](2020.5.1).',
    '汉语词典[增订五版](下).',
    '课程标准[^footnoteReference2](2011)提出要求。',
    '政策文件[EB/OL](2016-01-27).',
    '学生表达意愿[5](如“我想买”)时进行交流。',
])
def test_literal_citation_and_explanation_are_not_missing_resources(tmp_path, text):
    issues = []
    out = _rebase(text, tmp_path/'document.md', tmp_path/'view.md', tmp_path, issues)
    assert issues == []
    assert out.replace(r'\[', '[').replace(r'\]', ']') == text


@pytest.mark.parametrize('text', ['![3](苏)', '[原图](missing.png)', '[3](./苏)', '[3](<苏>)',
                                '[missing](missing-file)'])
def test_explicit_and_real_missing_evidence_is_still_reported(tmp_path, text):
    issues = []
    _rebase(text, tmp_path/'document.md', tmp_path/'view.md', tmp_path, issues)
    assert issues


def test_real_resource_overrides_citation_shape(tmp_path):
    (tmp_path/'苏').write_text('existing resource', encoding='utf8')
    issues = []
    out = _rebase('[3](苏)', tmp_path/'document.md', tmp_path/'view/x.md', tmp_path, issues)
    assert issues == []
    assert '../' in out
