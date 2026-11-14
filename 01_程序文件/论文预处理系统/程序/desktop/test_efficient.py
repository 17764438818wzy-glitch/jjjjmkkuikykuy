import copy
import io
import json
from pathlib import Path

import pytest
from PIL import Image

from agent_client import AgentError
from auto_evidence import Rejected, finish_evidence, media_privacy, text_privacy
from evidence import build_package


class TextAgent:
    def __init__(self): self.tasks = []
    def call(self, task, payload, images=()):
        self.tasks.append((task, payload, len(images)))
        if task == 'privacy_text': return {'decision': 'pass', 'reason': '无身份信息', 'sensitive_strings': []}
        if task == 'privacy_image': return {'decision': 'pass', 'reason': '已识别原图', 'research': True,
            'sensitive_boxes': [], 'text': '2022年收入100万元；2023年收入120万元。', 'description': '收入趋势图'}
        raise AssertionError('Unexpected redundant task: ' + task)


def doc_with_toc(path):
    from docx import Document
    from docx.oxml import OxmlElement
    doc = Document()
    doc.add_paragraph().add_run()._r.append(OxmlElement('w:object'))
    doc.add_heading('摘要', 1)
    doc.add_paragraph('本文分析企业财务数据。')
    sdt = OxmlElement('w:sdt'); content = OxmlElement('w:sdtContent')
    p = OxmlElement('w:p'); r = OxmlElement('w:r'); text = OxmlElement('w:t'); text.text = '目录：第一章 绪论'
    r.append(text); p.append(r); content.append(p); sdt.append(content)
    doc._element.body.insert(len(doc._element.body)-1, sdt)
    doc.add_paragraph('第一章 绪论：完整正文保留。')
    doc.add_heading('致谢', 1); doc.add_paragraph('感谢指导教师张小测。')
    doc.add_heading('参考文献', 1); doc.add_paragraph('[1] 研究文献。')
    doc.save(path)
    return path


def test_removed_cover_and_supported_toc_do_not_trigger_whole_document_scan(tmp_path, monkeypatch):
    source = doc_with_toc(tmp_path / 'source.docx'); package = tmp_path / 'package'
    meta = build_package(source, package, {'id': 'test', 'title': '企业财务数据研究'}, redact_content=False)
    assert not any(i['code'] in {'embedded_object_review', 'body_container'} for i in meta['issues'])
    import auto_evidence
    monkeypatch.setattr(auto_evidence, 'source_pages', lambda *a, **kw: pytest.fail('Native document must not render every page'))
    agent = TextAgent()
    finish_evidence(meta, package, agent, source, tmp_path / 'visual', 80, lambda *a: None)
    assert [task[0] for task in agent.tasks] == ['privacy_text']
    text = (package / 'ingest.md').read_text(encoding='utf-8')
    assert '完整正文保留' in text and '张小测' not in text and '[1] 研究文献' in text


def test_image_is_masked_locally_then_visible_pixels_are_rechecked(tmp_path):
    package = tmp_path / 'package'; (package / '_review').mkdir(parents=True); (package / 'assets').mkdir()
    image = package / '_review/image.png'; Image.new('RGB', (100, 80), 'white').save(image)
    before = image.read_bytes()
    meta = {'resources': [{'id': 'figure', 'kind': 'image', 'local_path': '_review/image.png', 'position': {'body_index': 2}}],
            'blocks': [{'id': 'figure', 'type': 'image', 'resource_id': 'figure', 'caption': '图1 收入趋势', 'position': {'body_index': 2}}]}
    class Agent(TextAgent):
        def call(self, task, payload, images=()):
            result = super().call(task, payload, images)
            result['sensitive_boxes'] = [] if payload.get('phase')=='verify_masked' else [[0, 0, .2, .2]]
            return result
    agent = Agent(); media_privacy(meta, package, agent, lambda *a: None)
    assert len(agent.tasks) == 2
    assert agent.tasks[1][1]['phase']=='verify_masked'
    assert agent.tasks[0][1]['caption'] == '图1 收入趋势'
    assert image.read_bytes() == before
    with Image.open(package / meta['resources'][0]['local_path']) as clean:
        assert clean.getpixel((5, 5)) == (0, 0, 0) and clean.getpixel((90, 70)) == (255, 255, 255)
    assert '100万元' in meta['blocks'][0]['ocr']['text']


def test_text_privacy_is_compact_once_and_preserves_numbers():
    meta = {'title': '企业财务数据研究', 'blocks': [
        {'type': 'paragraph', 'text': '由张小测整理的研究记录。'},
        {'type': 'table', 'cells': [{'text': '(10)'}, {'text': '-2.50%'}, {'text': ''}],
         'native_xml': '<table>' + '<cell>(10)</cell>' * 2000 + '</table>'}], 'resources': [], 'formulas': []}
    class Agent(TextAgent):
        def call(self, task, payload, images=()):
            result = super().call(task, payload, images)
            assert 'native_xml' not in payload['text'] and '<cell>' not in payload['text']
            result['sensitive_strings'] = ['张小测']
            return result
    agent = Agent(); text_privacy(meta, agent, lambda *a: None)
    assert len(agent.tasks) == 1
    assert '张小测' not in meta['blocks'][0]['text']
    assert [c['text'] for c in meta['blocks'][1]['cells']] == ['(10)', '-2.50%', '']


def test_invalid_native_grid_cannot_be_marked_complete(tmp_path):
    package = tmp_path / 'package'; package.mkdir()
    meta = {'id': 't', 'title': '财务数据研究', 'resources': [], 'issues': [], 'formulas': [], 'blocks': [
        {'id': 'table', 'type': 'table', 'rows': 2, 'columns': 2, 'cells': [{'row': 0, 'column': 0, 'rowspan': 1, 'colspan': 1, 'text': '100'}],
         'grid': [[0, 9], [0, 0]], 'position': {'body_index': 1}, 'header_rows': [0]}]}
    agent = TextAgent()
    with pytest.raises((Rejected, AgentError), match='表格'):
        finish_evidence(meta, package, agent, tmp_path / 'source.txt', tmp_path / 'visual', 80, lambda *a: None)
    assert not meta.get('completeness_verified')


def test_technical_failure_is_terminal_reject_with_original_preserved(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store, scan_sources
    from test_workbench import paper
    sources = tmp_path / 'in'; sources.mkdir(); source = sources / 'paper.txt'; source.write_text(paper(), encoding='utf-8')
    original = source.read_bytes()
    processor = AutoProcessor(Store(tmp_path / 'out'))
    class Broken:
        def call(self, *a, **kw): raise AgentError('接口响应格式无效；重试已用尽')
    processor.agent = Broken()
    manifest = processor.run(scan_sources(sources)); item = manifest['items'][0]
    assert item['status'] == 'reject' and item['reason_kind'] == 'technical'
    assert source.read_bytes() == original and not item['automatic_approval']
    assert '接口' in Path(item['path']).read_text(encoding='utf-8')


def test_clear_original_classification_is_reused_without_ai_reclassification(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store, scan_sources
    from test_workbench import paper
    sources = tmp_path / 'in'; sources.mkdir(); (sources / 'paper.txt').write_text(paper(), encoding='utf-8')
    processor = AutoProcessor(Store(tmp_path / 'out')); agent = TextAgent(); processor.agent = agent
    result = processor.run(scan_sources(sources)); item = result['items'][0]
    assert item['status'] == 'pass', item['reason']
    assert item['major'] == '学前教育' and item['classification']['method'] == 'original_rules'
    assert [task[0] for task in agent.tasks] == ['privacy_text']


def test_scanned_pdf_page_is_read_once_without_rescanning_text_pages(tmp_path):
    import fitz
    source=tmp_path/'scan.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page();page.insert_text((40,60),'Research introduction with native text.')
        # A scan has visible pixels but no text layer; a truly empty page is skipped.
        page=pdf.new_page();page.draw_line((40,60),(200,60))
        pdf.save(source)
    package=tmp_path/'package'
    meta=build_package(source,package,{'id':'pdf','title':'扫描页与文字页混合研究'},redact_content=False)
    class Agent(TextAgent):
        def call(self,task,payload,images=()):
            if task=='extract_page':
                self.tasks.append((task,payload,len(images)))
                assert payload['page']==2 and images
                return {'decision':'pass','reason':'完整转录本页','blocks':[
                    {'kind':'text','text':'扫描页完整正文。'},
                    {'kind':'table','rows':[['年份','收入'],['2023','100.00']], 'header_rows':[0], 'caption':'表1 收入', 'units':'万元', 'notes':[]}],
                    'sensitive_boxes':[]}
            return super().call(task,payload,images)
    agent=Agent()
    finish_evidence(meta,package,agent,source,tmp_path/'visual',80,lambda *a:None)
    assert [t[0] for t in agent.tasks]==['extract_page','privacy_text']
    text=(package/'ingest.md').read_text(encoding='utf-8')
    assert 'Research introduction' in text and '扫描页完整正文' in text and '100.00' in text


def test_missing_image_is_not_silently_resolved(tmp_path):
    source=tmp_path/'source.txt'; source.write_text('研究正文',encoding='utf-8')
    package=tmp_path/'package'; meta=build_package(source,package,{'id':'t','title':'缺失图像研究'})
    meta['issues'].append({'code':'image_missing','position':{'line':1},'message':'外链图片缺失','status':'open'})
    with pytest.raises(Rejected,match='图片'):
        finish_evidence(meta,package,TextAgent(),source,tmp_path/'visual',80,lambda *a:None)


@pytest.mark.parametrize('spans',[['作者\n张小测'],['作者\n张小测','作者']])
def test_cross_block_privacy_span_cannot_be_reported_removed_when_unchanged(spans):
    meta={'title':'财务数据研究','blocks':[{'type':'paragraph','text':'作者'},{'type':'paragraph','text':'张小测'}], 'resources':[], 'formulas':[]}
    class Agent(TextAgent):
        def call(self,*a,**kw): return {'decision':'pass','reason':'姓名','sensitive_strings':spans}
    with pytest.raises(Rejected,match='跨段|未能移除'):
        text_privacy(meta,Agent(),lambda *a:None)


def test_native_xml_revision_author_is_removed_without_altering_values():
    meta={'title':'财务数据研究','blocks':[{'type':'table','cells':[{'text':'100万元'}],
          'native_xml':'<table><ins author="张小测"><cell>100万元</cell></ins></table>'}], 'resources':[], 'formulas':[]}
    text_privacy(meta,TextAgent(),lambda *a:None)
    assert '张小测' not in json.dumps(meta,ensure_ascii=False)
    assert '>100万元<' in meta['blocks'][0]['native_xml']


@pytest.mark.parametrize('kind',['image','table_screenshot'])
def test_new_sensitive_image_text_cannot_leave_unmasked_pixels_approved(kind):
    meta={'title':'财务数据研究','blocks':[], 'formulas':[], 'resources':[
        {'id':'figure','kind':kind,'privacy_status':'approved','masked_regions':[],
         'ocr':{'text':'图1 制作人张小测'},'position':{'body_index':3}}]}
    class Agent(TextAgent):
        def call(self,*a,**kw): return {'decision':'pass','reason':'制作者姓名','sensitive_strings':['张小测']}
    with pytest.raises(Rejected,match='图片'):
        text_privacy(meta,Agent(),lambda *a:None)


def test_local_nested_table_recovery_preserves_outer_cells(tmp_path,monkeypatch):
    import fitz
    import evidence_tasks
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf: pdf.new_page();pdf.save(source)
    original={'id':'t','type':'table','position':{'page':1},'rows':2,'columns':2,
        'grid':[[0,1],[2,3]],'header_rows':[0], 'complex':True,
        'cells':[{'row':ri,'column':ci,'rowspan':1,'colspan':1,'text':v} for ri,row in enumerate([['项目','金额'],['原生外表收入','100万元']]) for ci,v in enumerate(row)]}
    meta={'blocks':[original], 'issues':[{'code':'nested_table','position':{'page':1},'status':'open'}], 'resources':[]}
    class Agent:
        def call(self,*a,**kw): return {'decision':'pass','reason':'补内表','sensitive_boxes':[],
            'insertions':[{'after_id':'t','kind':'table','rows':[['内表指标','数值'],['收入','50万元']]}]}
    evidence_tasks.resolve_scoped(meta,tmp_path,source,tmp_path/'work',80,Agent(),lambda *a:None)
    assert meta['blocks'][0] is original
    assert [c['text'] for c in original['cells']]==['项目','金额','原生外表收入','100万元']
    assert len(meta['blocks'])==2


def test_pdf_vector_graph_requires_scoped_recognition(tmp_path):
    import fitz
    source=tmp_path/'vector.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page();page.insert_text((40,60),'Original research introduction with native text.')
        page.draw_circle((100,150),30);page.draw_line((130,150),(250,220));pdf.save(source)
    package=tmp_path/'package'
    meta=build_package(source,package,{'id':'v','title':'矢量图保留研究'},redact_content=False)
    assert any(i['code']=='pdf_vector_review' for i in meta['issues'])
    class Agent(TextAgent):
        def call(self,task,payload,images=()):
            if task=='repair':
                self.tasks.append((task,payload,len(images)))
                target=next(b for b in payload['known_blocks'] if '矢量绘图' in b.get('text',''))
                return {'decision':'pass','reason':'已提取矢量图','sensitive_boxes':[],
                        'insertions':[{'after_id':target['id'],'kind':'image','bbox':[.1,.13,.5,.3],
                        'text':'原图未标数值','description':'一个圆形与一条连接线','caption':'矢量示意图'}]}
            if task=='privacy_image':
                self.tasks.append((task,payload,len(images)))
                return {'decision':'pass','reason':'复核实际裁图','research':True,'sensitive_boxes':[],
                        'text':'原图未标数值','description':'一个圆形与一条连接线'}
            return super().call(task,payload,images)
    agent=Agent();finish_evidence(meta,package,agent,source,tmp_path/'visual',80,lambda *a:None)
    assert [t[0] for t in agent.tasks]==['repair','privacy_image','privacy_image','privacy_text']
    assert len(meta['resources'])==1 and '一个圆形与一条连接线' in (package/'ingest.md').read_text(encoding='utf-8')


def test_unrelated_image_in_same_container_does_not_resolve_embedded_object(tmp_path,monkeypatch):
    import evidence_tasks
    meta={'blocks':[
        {'id':'unknown','type':'paragraph','text':'原文嵌入对象','position':{'part':'word/document.xml','body_index':2,'container_path':[0,0]}},
        {'id':'image','type':'image','position':{'part':'word/document.xml','body_index':2,'container_path':[0,1]}}],
        'issues':[{'code':'embedded_object_review','status':'open','position':{'part':'word/document.xml','body_index':2,'container_path':[0,0],'object_ref':'object_1'}}], 'resources':[]}
    def needs_original(*args): raise Rejected('必须提供此对象的原页证据')
    monkeypatch.setattr(evidence_tasks,'_pdf',needs_original)
    with pytest.raises(Rejected,match='原页证据'):
        evidence_tasks.resolve_scoped(meta,tmp_path,tmp_path/'source.docx',tmp_path/'visual',80,TextAgent(),lambda *a:None)
    assert meta['issues'][0]['status']=='open'


def test_replacing_merged_table_cannot_drop_continuation_cell_text(tmp_path):
    import fitz
    import evidence_tasks
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf: pdf.new_page();pdf.save(source)
    original={'id':'t','type':'table','position':{'page':1},'rows':2,'columns':1,'grid':[[0],[0]],
        'header_rows':[0],'complex':True,'cells':[{'row':0,'column':0,'rowspan':2,'colspan':1,
        'text':'收入','continuation_text':[{'row':1,'text':'100万元'}]}]}
    meta={'blocks':[original],'resources':[],'issues':[{'code':'merged_cell_content','position':{'page':1},'status':'open'}]}
    class Agent:
        def call(self,*a,**kw): return {'decision':'pass','reason':'恢复整表','sensitive_boxes':[],
            'insertions':[{'after_id':'t','operation':'replace_table','kind':'table','rows':[['收入']]}]}
    with pytest.raises(Rejected,match='遗漏或改变'):
        evidence_tasks.resolve_scoped(meta,tmp_path,source,tmp_path/'work',80,Agent(),lambda *a:None)
    assert meta['issues'][0]['status']=='open'


def test_one_parse_failure_does_not_stop_the_batch(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store,scan_sources
    from test_workbench import paper
    sources=tmp_path/'in';sources.mkdir()
    (sources/'a_bad.txt').write_bytes(b'\xff\xfe\xfa')
    (sources/'b_good.txt').write_text(paper(),encoding='utf-8')
    processor=AutoProcessor(Store(tmp_path/'out'));processor.agent=TextAgent()
    result=processor.run(scan_sources(sources))
    assert [i['status'] for i in result['items']]==['reject','pass']
    assert result['items'][0]['reason_kind']=='technical'
    assert (sources/'a_bad.txt').read_bytes()==b'\xff\xfe\xfa'


def test_tab_stop_properties_are_not_inserted_as_body_text(tmp_path):
    from docx import Document
    from docx.shared import Inches
    doc=Document();doc.add_heading('摘要',1)
    table=doc.add_table(rows=1,cols=1)
    paragraph=table.cell(0,0).paragraphs[0]
    paragraph.paragraph_format.tab_stops.add_tab_stop(Inches(1))
    run=paragraph.add_run('营业收入');run.add_tab();run.add_text('100.00')
    source=tmp_path/'tabs.docx';doc.save(source)
    meta=build_package(source,tmp_path/'package',{'id':'tabs','title':'财务数据研究'},redact_content=False)
    block=next(b for b in meta['blocks'] if b['type']=='table')
    assert block['cells'][0]['text']=='营业收入\t100.00'


def test_pdf_conversion_uses_short_temporary_profile_with_long_batch_path(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from urllib.parse import unquote,urlsplit
    import fitz
    import evidence_tasks
    source=tmp_path/'source.docx';source.write_bytes(b'source-preservation-test')
    work=tmp_path/'runs'/('batch_'+'a'*40)/'private'/'visual'
    calls=[]
    def convert(command,**kwargs):
        profile=unquote(urlsplit(next(v.split('=',1)[1] for v in command if v.startswith('-env:UserInstallation='))).path)
        calls.append(profile)
        # Reproduce the converter's silent first-start failure with deeply nested profiles.
        if '/runs/' in profile: return SimpleNamespace(returncode=0,stdout=b'',stderr=b'')
        output=Path(command[command.index('--outdir')+1])
        with fitz.open() as pdf:
            pdf.new_page();pdf.save(output/(Path(command[-1]).stem+'.pdf'))
        return SimpleNamespace(returncode=0,stdout=b'converted',stderr=b'')
    monkeypatch.setattr(evidence_tasks.subprocess,'run',convert)
    with evidence_tasks._pdf(source,work) as pdf: assert len(pdf)==1
    assert len(calls)==1 and '/runs/' not in calls[0]
    assert source.read_bytes()==b'source-preservation-test'
    assert (work/'source.pdf').is_file()
