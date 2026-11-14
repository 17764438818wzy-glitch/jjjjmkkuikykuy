"""Consumer-facing cases across Word styles, plain text and visual chart schemas."""
import copy
import json

import pytest

from evidence import export_package
from knowledge_package import knowledge_files
from test_knowledge_package import sample, read
from visual_knowledge import visual_fields
from agent_client import AgentError


def paragraph(text, level=0, **extra):
    return {'type': 'paragraph', 'text': text, 'heading_level': level, **extra}


def export_text(tmp_path, blocks):
    meta = sample(tmp_path)
    meta.update(resources=[], formulas=[], blocks=[
        dict(b, id=f'p{i}', position={'line': i + 1}) for i, b in enumerate(blocks)
    ])
    before = copy.deepcopy(meta['blocks'])
    export_package(tmp_path, meta)
    assert before == meta['blocks'], 'The source transcription must remain unchanged'
    knowledge_files(tmp_path)
    manifest = read(tmp_path, 'manifest.json')
    units = [read(tmp_path, u['path']) for u in manifest['writing_units']]
    return manifest, units


@pytest.mark.parametrize('entries', [
    ['1 引言\t1', '1.1背景\t2'],
    ['1 引言..........1', '1.1背景……2'],
    ['1 引言  1', '1.1背景  2'],
])
def test_toc_is_removed_from_deliverables_without_modifying_the_input(tmp_path, entries):
    blocks = [paragraph('摘  要'), paragraph('这是摘要原文。'), paragraph('Abstract'),
              paragraph('An abstract.'), paragraph('目  录')]
    blocks += [paragraph(t, 2) for t in entries]
    blocks += [paragraph('1 引言', 1), paragraph('1.1背景', 2), paragraph('真实的背景分析。')]
    manifest, units = export_text(tmp_path, blocks)
    titles = [u['title'] for u in units]
    assert len([t for t in titles if t.startswith('1.1')]) == 1
    assert not any('\t' in t or '……' in t or '..........' in t for t in titles)
    assert any('摘' in t for t in titles)
    assert not any('目录' == t.replace(' ', '') for t in titles)
    source = read(tmp_path, 'metadata/blocks.json')['blocks']
    assert [b['text'] for b in source] == [b['text'] for b in blocks if b['text'] not in entries+['目  录']]
    assert all(t not in json.dumps(read(tmp_path,'metadata.json'),ensure_ascii=False) for t in entries)
    ingest = (tmp_path/'ingest.md').read_text(encoding='utf-8')
    assert all(t not in ingest for t in entries)
    assert all(t not in (tmp_path/'document.md').read_text(encoding='utf-8') for t in entries)
    assert '真实的背景分析。' in ingest


def test_toc_styles_and_multiline_pdf_contents_without_heading_are_detected(tmp_path):
    _, units = export_text(tmp_path, [
        paragraph('Overview', style='TOC1'), paragraph('Details', style='TOC2'),
        paragraph('1 Introduction .... 1\n1.1 Background .... 2'),
        paragraph('1 Introduction'), paragraph('1.1 Background'), paragraph('Actual study.'),
    ])
    assert [u['title'] for u in units] == ['1 Introduction', '1.1 Background']


def test_page_like_prose_and_numbered_lists_do_not_disappear_or_become_headings(tmp_path):
    texts = ['本次目录结构分析保留了 20 个案例。', '2023.1 营业收入为91.30亿元',
             '1. 采购材料', '费用支出  120']
    _, units = export_text(tmp_path, [paragraph('1 研究结果'), paragraph(texts[0]),
        paragraph(texts[1]), paragraph(texts[2], list={'id': '1', 'level': 0, 'format': 'decimal', 'value': 1}),
        paragraph(texts[3])])
    assert len(units) == 1
    assert all(t in (tmp_path/'ingest.md').read_text(encoding='utf-8') for t in texts)


@pytest.mark.parametrize('prefix', ['', '## '])
def test_numbering_corrects_inconsistent_styles_and_unspaced_titles(tmp_path, prefix):
    _, units = export_text(tmp_path, [
        paragraph('1 理论研究', 1), paragraph('1.2 文献综述', 2),
        paragraph(prefix+'1.2.1 国外研究', 2), paragraph('国外研究正文。'),
        paragraph('1.2.2国内研究', 3), paragraph('国内研究正文。'),
        paragraph('1.2.3文献评述', 3), paragraph('评述正文。'),
        paragraph('2 研究设计', 1), paragraph('设计正文。'),
    ])
    parent = next(u for u in units if u['title'] == '1.2 文献综述')
    children = [u for u in units if u['id'] in parent['children']]
    assert len(children) == 3
    assert all(u['parent_id'] == parent['id'] for u in children)
    assert all(not u['children'] for u in children)
    assert '### 1.2.1 国外研究' in (tmp_path/'ingest.md').read_text(encoding='utf-8')


def test_numbered_section_never_attaches_to_unrelated_previous_chapter(tmp_path):
    _, units = export_text(tmp_path, [paragraph('第一章 绪论', 1), paragraph('正文。'),
        paragraph('2.1 研究样本', 2), paragraph('本节的上级标题在原稿中缺失。')])
    orphan = next(u for u in units if u['title'].startswith('2.1'))
    assert orphan['parent_id'] is None


def test_unnumbered_styled_headings_and_chinese_chapters_keep_hierarchy(tmp_path):
    _, units = export_text(tmp_path, [paragraph('第三章 研究设计'), paragraph('3.1样本选择'),
        paragraph('研究样本。'), paragraph('补充讨论', 2), paragraph('讨论正文。')])
    chapter = units[0]
    assert len(chapter['children']) == 2
    assert all(u['parent_id'] == chapter['id'] for u in units[1:])


def test_retrieval_text_has_nearby_stable_ids_without_bulk_block_catalogue(tmp_path):
    meta = sample(tmp_path)
    export_package(tmp_path, meta)
    m = read(tmp_path, 'manifest.json')
    ingest = (tmp_path/'ingest.md').read_text(encoding='utf-8')
    assert '内容块：' not in ingest
    assert '知识包导航' not in ingest
    assert 'block_' not in ingest
    assert m['paper_id'] in ingest
    unit = next(u for u in m['writing_units'] if u['title'] == '3.1 盈利规模')
    before_next = ingest.split('3.2 经营效率')[0]
    assert unit['id'] in before_next
    for item in m['tables'] + m['figures']:
        assert item['id'] in before_next
    assert '80.60' in ingest and '2023' in ingest
    assert m['handoff']['asset_upload'] == 'approved_images'


def test_visual_combination_records_per_series_types_and_keeps_unknowns():
    result = visual_fields({'chart_spec': {'chart_type': 'combination', 'series': [
        {'name': '收入', 'axis_side': 'left', 'chart_type': 'bar'},
        {'name': '增长率', 'axis_side': 'right', 'chart_type': 'line'},
        {'name': '不清楚的系列', 'axis_side': 'unknown', 'chart_type': None},
    ]}})
    assert [s['chart_type'] for s in result['chart_spec']['series']] == ['bar', 'line', None]


def test_legacy_visual_schema_remains_valid_without_guessing_combination_types():
    result = visual_fields({'chart_spec': {'chart_type': 'combination', 'series': [{'name': '收入'}]}})
    assert result['chart_spec']['series'][0].get('chart_type') is None
    with pytest.raises(AgentError):
        visual_fields({'chart_spec': {'chart_type': 'combination', 'series': [{'chart_type': 'line', 'values': [1]}]}})


def test_visual_recipe_survives_existing_single_call_and_export(tmp_path):
    from auto_evidence import media_privacy
    from test_visual_knowledge import visual_meta, answer
    meta = visual_meta(tmp_path)
    class Agent:
        calls = 0
        def call(self, task, payload, images=()):
            self.calls += 1
            result = answer()
            result['chart_spec']['series'][0]['chart_type'] = 'line'
            return result
    agent = Agent()
    media_privacy(meta, tmp_path, agent, lambda *a: None)
    assert agent.calls == 1
    assert meta['blocks'][0]['chart_spec']['series'][0]['chart_type'] == 'line'


@pytest.mark.parametrize('with_list', [False, True])
def test_adjacent_numbered_data_rows_are_never_removed_as_toc(tmp_path, with_list):
    extra={'list': {'format': 'decimal', 'value': 1, 'level': 0}} if with_list else {}
    lines=['1 低剂量组  20','2 高剂量组  30']
    _,units=export_text(tmp_path,[paragraph('3 研究结果',1), *[paragraph(t,**extra) for t in lines]])
    text=(tmp_path/'ingest.md').read_text(encoding='utf-8')
    assert all(t in text for t in lines)
    assert len(units)==1


@pytest.mark.parametrize('chapter', ['1. Introduction', '1、绪论', '研究设计'])
def test_common_chapter_styles_keep_numbered_child(tmp_path,chapter):
    _,units=export_text(tmp_path,[paragraph(chapter,1),paragraph('1.1 研究背景',2),paragraph('正文。')])
    assert units[1]['parent_id']==units[0]['id']


@pytest.mark.parametrize('heading,rows',[
    ('Contents',['Introduction ........ 1','Background ........ 2']),
    ('目录',['绪论……1','研究背景……2']),
])
def test_explicit_toc_with_unnumbered_dot_leader_entries_is_removed(tmp_path,heading,rows):
    _,units=export_text(tmp_path,[paragraph(heading),*[paragraph(t) for t in rows],
        paragraph('1 绪论'),paragraph('研究正文。')])
    assert len(units)==1
    assert all(t not in (tmp_path/'ingest.md').read_text(encoding='utf-8') for t in rows)


def test_financial_quantities_are_body_and_styled_question_is_a_heading(tmp_path):
    values=['15万元支出','1.5亿元收入','3.2%增长率']
    _,units=export_text(tmp_path,[paragraph('盈利能力为什么下降？',1),*[paragraph(t) for t in values]])
    assert [u['title'] for u in units]==['盈利能力为什么下降？']
    assert all(t in (tmp_path/'ingest.md').read_text(encoding='utf-8') for t in values)


@pytest.mark.parametrize('title',['3.1 人力资源管理','3.2 年度财务比较','3.1人力资源管理','3.2 元素分析'])
def test_normal_title_words_are_not_mistaken_for_measurement_units(tmp_path,title):
    _,units=export_text(tmp_path,[paragraph('第三章 企业情况'),paragraph(title),paragraph('真实分析。')])
    assert len(units)==2 and units[1]['parent_id']==units[0]['id']
