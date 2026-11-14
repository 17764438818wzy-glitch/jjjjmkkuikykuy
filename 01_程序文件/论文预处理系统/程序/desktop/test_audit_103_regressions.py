"""Prevent audited false rejections without relaxing evidence/privacy checks."""
import copy
from pathlib import Path

import pytest

from content_rules import valid_title, detect_title
from human_view import _rebase
from test_common_contracts import client, picture, repair


@pytest.mark.parametrize('title', ['ESG表现有助于提升企业创新吗？', 'ESG表现有助于降低企业债务融资成本吗？',
                                  'Does ESG performance improve corporate innovation?'])
def test_question_form_research_titles_keep_original_punctuation(title):
    assert valid_title(title)
    assert detect_title('题目：'+title+'\n摘要\n论文正文') == title


@pytest.mark.parametrize('text', ['学生姓名：这是哪位同学？', '请问？请回答？', '财务情况很好！',
                                 '201905170035这个学生是谁？', '本科毕业论文', '2026年9月20日'])
def test_identity_cover_and_multiple_sentences_still_not_titles(text):
    assert not valid_title(text)


def test_hausman_formula_is_literal_not_a_missing_resource(tmp_path):
    source = tmp_path/'document.md'
    target = tmp_path/'view/document.md'
    formula = "chi2(5) = (b-B)'[(V_b-V_B)^(-1)](b-B) = 11.36"
    issues = []
    text = _rebase(formula, source, target, tmp_path, issues)
    assert issues == []
    assert '本地证据缺失' not in text
    # Escaping must preserve the displayed formula, never remove a symbol.
    assert text.replace('\\[', '[').replace('\\]', ']') == formula


@pytest.mark.parametrize('body', ['![研究图](assets/missing.png)', '[原始证据](missing-file)',
                                '[a-b](missing-file)', '![公式](b-B)'])
def test_real_missing_links_are_still_rejected(tmp_path, body):
    issues = []
    _rebase(body, tmp_path/'document.md', tmp_path/'view.md', tmp_path, issues)
    assert issues


@pytest.mark.parametrize('field,value', [
    ('chart_spec', {'chart_type': 'invented'}),
    ('chart_spec', {'series': [{'name': '销售额', 'values': [1, 2]}]}),
    ('chart_spec', {'x_axis': {'label': ['not text']}}),
    ('structured_table', {'rows': [['2023', 100]]}),
    ('structured_table', {'rows': [['2023'], ['2024', '200']]})])
@pytest.mark.parametrize('task', ['privacy_image', 'repair'])
def test_visual_structure_is_corrected_with_local_schema_feedback(monkeypatch, task, field, value):
    if task == 'privacy_image':
        good = {'decision': 'pass', 'reason': '可辨认', 'research': True, 'text': '研究数据',
                'description': '原图', 'sensitive_boxes': [], 'chart_spec': None, 'structured_table': None}
    else:
        good = repair([.1, .1, .8, .8])
    bad = copy.deepcopy(good)
    entry = bad if task == 'privacy_image' else bad['insertions'][0]
    entry[field] = value
    adapter = client(monkeypatch, bad, good)
    assert adapter.call(task, {'page': 1}, [picture()]) == good
    assert len(adapter.opener.requests) == 2
    assert field in adapter.opener.requests[-1]['messages'][-1]['content']


def test_unfixable_visual_structure_has_bounded_retries(monkeypatch):
    from agent_client import AgentError
    bad = repair([.1, .1, .8, .8])
    bad['insertions'][0]['chart_spec'] = {'chart_type': 'invented'}
    adapter = client(monkeypatch, bad, bad, bad)
    with pytest.raises(AgentError, match='chart_spec'):
        adapter.call('repair', {'page': 1}, [picture()])
    assert len(adapter.opener.requests) == 3


def test_offpage_pdf_image_is_not_a_visible_research_object(tmp_path, monkeypatch):
    import fitz
    from evidence import parse_pdf
    source = tmp_path/'paper.pdf'
    pdf = fitz.open(); page = pdf.new_page(width=600, height=800)
    page.insert_text((50, 100), 'Visible research text remains.'); pdf.save(source); pdf.close()
    original = fitz.Page.get_text
    def text(page, option='text', *args, **kwargs):
        result = original(page, option, *args, **kwargs)
        if option == 'dict':
            result['blocks'].append({'type': 1, 'bbox': [36, 1023, 0, 800],
                'image': picture(), 'mask': b'mask', 'width': 12, 'height': 32,
                'transform': [9, 0, 0, 24, 36, 1023], 'ext': 'png'})
        return result
    monkeypatch.setattr(fitz.Page, 'get_text', text)
    resources = []
    def resource(*args):
        resources.append(args)
        return {'id': args[0]}
    blocks, images, _ = parse_pdf(source, lambda s:s, resource, lambda *args:None)
    assert not images and not resources
    assert any('Visible research text' in b.get('text', '') for b in blocks)


@pytest.mark.parametrize('rotation', [0, 90, 180, 270])
def test_pdf_visible_and_partially_visible_images_must_not_be_skipped(rotation):
    import fitz
    from evidence import _pdf_image_offpage
    doc = fitz.open(); page = doc.new_page(width=600, height=800); page.set_rotation(rotation)
    try:
        assert not _pdf_image_offpage({'transform': [100, 0, 0, 100, 550, 750]}, page)
        assert not _pdf_image_offpage({'transform': [100, 0, 0, 100, 50, 50]}, page)
        assert _pdf_image_offpage({'transform': [100, 0, 0, 100, 900, 1000]}, page)
        assert not _pdf_image_offpage({'transform': [float('nan'), 0, 0, 0, 0, 0]}, page)
    finally: doc.close()


@pytest.mark.parametrize('kind', ['原文对照报告', '简洁报告'])
def test_similarity_reports_are_rejected_before_length_or_ai(kind):
    from auto_pipeline import coarse_screen
    result = coarse_screen(kind+'\n总相似比：15.6%\n检测报告\n'+'报告内容'*1000, False,
                           source_name='论文_'+kind+'.pdf')
    assert result['decision'] == 'reject'
    assert '查重' in result['reason'] and '无需' in result['reason']


def test_report_header_can_follow_detection_metrics_in_pdf_text_order():
    from auto_pipeline import coarse_screen
    raw='复写率说明\n'+'检测字段\n'*30+'全文总相似比：\n15.6%\n报告编号：test\n说明\n说明\n简洁报告-大学生版'
    result=coarse_screen(raw, False, source_name='论文_简洁报告.pdf')
    assert result['decision']=='reject' and '查重' in result['reason']


def test_report_percentage_can_precede_its_label_due_to_pdf_column_order():
    from auto_pipeline import coarse_screen
    raw='14.35%\n全文总相似比：\n检测字符数：16756\n报告编号：fixture\n检测时间：2026-09-20\n简洁报告-大学生版'
    assert coarse_screen(raw,False,source_name='论文_简洁报告.pdf')['decision']=='reject'


@pytest.mark.parametrize('filename,body', [
    ('企业_原文对照报告.pdf', '题目：企业信息披露研究\n摘要\n'+ '论文正文'*1000+'\n参考文献'),
    ('查重报告研究.docx', '题目：高校查重报告使用研究\n摘要\n总相似比和简洁报告是本文研究对象'+ '研究正文'*1000),
    ('paper.docx', '题目：企业财务管理研究\n摘要\n'+ '企业正文'*1000+'\n附录\n简洁报告\n总相似比：15%')], ids=['name-only', 'research-about-reports', 'appendix'])
def test_paper_about_reports_or_filename_hint_alone_is_not_filtered(filename, body):
    from auto_pipeline import coarse_screen
    result = coarse_screen(body, True, source_name=filename)
    assert result['decision'] != 'reject'


@pytest.mark.parametrize('exception,code,expected', [
    (__import__('zipfile').BadZipFile("Bad CRC-32 for file 'word/media/image4.png'"), 'docx_media_crc', '内嵌图片'),
    (ValueError('SECRET_KEY_WITH_PRIVATE_DOCUMENT'), 'processing_value_error', 'ValueError')])
def test_failure_keeps_stage_and_safe_actionable_reason(tmp_path, monkeypatch, exception, code, expected):
    import auto_pipeline
    from engine import Store
    source = tmp_path/'paper.txt'
    source.write_text('企业财务管理研究\n摘要\n'+'企业经营数据与财务分析。'*400+'\n参考文献', encoding='utf-8')
    def broken(*args, **kwargs): raise exception
    monkeypatch.setattr(auto_pipeline, 'build_package', broken)
    result = auto_pipeline.AutoProcessor(Store(tmp_path/'data'), {'auto_upload': False}).run([{'path': str(source)}])
    item = result['items'][0]
    assert item.get('failure_code') == code
    assert item.get('failure_stage') == '图表提取'
    assert expected in item['reason']
    assert 'SECRET_KEY' not in item['reason']


def test_history_summary_counts_browse_failures_and_duplicates_separately():
    import auto_view
    manifest = {'status': 'completed', 'message': '通过 2，不通过 0', 'items': [
        {'id': 'a', 'status': 'pass', 'automatic_approval': True, 'title': '企业财务管理研究', 'major': '会计学'},
        {'id': 'b', 'status': 'pass', 'automatic_approval': True, 'title': '企业财务管理研究', 'major': '会计学'}],
        'events': [{'status': 'duplicate'}], 'human_view': {'issues': [{'id': 'b', 'reason': '真实图片缺失'}]}}
    assert hasattr(auto_view, 'batch_summary')
    summary = auto_view.batch_summary(manifest)
    assert '通过 1，不通过 1' in summary and '浏览异常 1' in summary and '重复跳过 1' in summary


def test_history_summary_includes_event_only_failures_without_double_counting():
    from auto_view import batch_summary
    manifest={'pipeline':'automatic','status':'completed','items':[
        {'id':'a','status':'reject','reason_kind':'technical','source_path':'C:/input/a.docx'}],
        'events':[{'status':'failed','source':'C:/input/a.docx'},
                  {'status':'failed','source':'C:/input/b.docx'},
                  {'status':'duplicate','source':'C:/input/c.docx'}]}
    assert '通过 0，不通过 2' in batch_summary(manifest)


def test_visible_pdf_with_invalid_geometry_keeps_page_and_requires_attention():
    import fitz
    from agent_client import AgentError
    from evidence import _pdf_image_payload
    from processing_errors import describe_failure
    pdf=fitz.open();page=pdf.new_page()
    try:
        with pytest.raises(AgentError,match='第 1 页') as raised:
            _pdf_image_payload({'image':picture(),'mask':b'mask','bbox':[50,50,10,10],
                'transform':[20,0,0,20,50,50],'width':20,'height':20},page)
        info=describe_failure(raised.value)
        assert info['code']=='pdf_image_geometry' and info['position']['page']==1
    finally:pdf.close()


def test_zip_crc_is_not_misreported_as_a_docx_image_error():
    from zipfile import BadZipFile
    from processing_errors import describe_failure
    error=describe_failure(BadZipFile("Bad CRC-32 for file 'paper.txt'"))
    assert error['code']=='archive_crc_error' and 'DOCX' not in error['message']
