"""Source-major identity must survive topic keywords, title repair and retry."""
import copy
import json
from pathlib import Path

import pytest

from auto_pipeline import AutoProcessor, VERSION, coarse_screen
from engine import Store, digest
from evidence import build_package
from ai_resume import save_prepared


def article(title, major_line=''):
    return ('题目：'+title+'\n'+major_line+'\n摘要\n本文考察制度的适用与解释。\n第一章 绪论\n'+
            '\n'.join('规范分析与案例比较说明了现有制度的适用范围，并从事实和法律依据提出改进建议。' for _ in range(150))+
            '\n关键原文必须从原生文件恢复。\n参考文献\n[1] 原始研究文献。')


class OfflineAgent:
    def __init__(self, classified_major='经济学'):
        self.tasks=[]
        self.classified_major=classified_major

    def call(self, task, payload, images=()):
        self.tasks.append((task, copy.deepcopy(payload)))
        if task=='privacy_text':
            return {'decision':'pass','reason':'无身份信息','sensitive_strings':[]}
        if task=='classify':
            return {'decision':'pass','reason':'仅补充题名','major':self.classified_major,'title':'企业内部控制有效性研究'}
        if task=='screen':
            return {'decision':'pass','reason':'研究论文'}
        raise AssertionError('Unexpected external task: '+task)


def run_article(tmp_path, directory, title, major_line='', record_fields=None):
    source=tmp_path/'inputs'/directory/'paper.txt'
    source.parent.mkdir(parents=True)
    source.write_text(article(title,major_line),encoding='utf-8')
    processor=AutoProcessor(Store(tmp_path/'output'))
    processor.agent=OfflineAgent()
    record={'path':str(source),'name':source.name,**(record_fields or {})}
    result=processor.run([record])
    return result['items'][0],processor.agent


@pytest.mark.parametrize('title',[
    '校园欺凌的刑法规制探究',
    '突发公共卫生事件中的个人信息保护',
    '人工智能生成内容的著作权保护',
    '人脸识别技术应用的法律规制',
])
def test_declared_law_directory_wins_over_research_topic(tmp_path,title):
    item,agent=run_article(tmp_path,'法学类/法学/2019法学2班',title)
    assert item['status']=='pass',item['reason']
    assert item['major']=='法学'
    assert item['classification']['source']=='source_directory'
    assert not any(task=='classify' for task,_ in agent.tasks)


def test_exact_political_education_directory_survives_general_teaching_title(tmp_path):
    item,_=run_article(tmp_path,'法学/马克思主义理论类/思想政治教育/2019思想政治教育班','高中课堂教学设计研究')
    assert item['major']=='思想政治教育'
    assert item['classification']['source']=='source_directory'


def test_cover_major_inside_native_content_control_is_not_lost_with_lightweight_text(tmp_path):
    from docx import Document
    from docx.oxml import OxmlElement
    source=tmp_path/'paper.docx'
    doc=Document()
    container=OxmlElement('w:sdt');content=OxmlElement('w:sdtContent')
    for value in ('专业：','思想政治教育'):
        p=OxmlElement('w:p');r=OxmlElement('w:r');text=OxmlElement('w:t');text.text=value
        r.append(text);p.append(r);content.append(p)
    container.append(content);doc._element.body.insert(0,container)
    for line in article('高中课堂教学设计研究').splitlines():doc.add_paragraph(line)
    doc.save(source)
    processor=AutoProcessor(Store(tmp_path/'output'));processor.agent=OfflineAgent()
    item=processor.run([{'path':str(source),'name':source.name}])['items'][0]
    assert item['major']=='思想政治教育'
    assert item['classification']['source']=='front_matter'


def test_conflicting_cover_and_major_directory_stops_before_any_ai(tmp_path):
    item,agent=run_article(tmp_path,'计算机科学与技术','人工智能应用的法律分析','专业：法学')
    assert item['status']=='reject'
    assert item['reason_kind']=='classification'
    assert item['classification']['source']=='source_conflict'
    assert agent.tasks==[]


def test_explicit_manual_classification_is_not_overwritten(tmp_path):
    selected={'major':'经济学','classification':{'major':'经济学','method':'manual','source':'manual','confidence':'high'}}
    item,_=run_article(tmp_path,'法学','人工智能应用的法律分析','专业：法学',selected)
    assert item['major']=='经济学'
    assert item['classification']['method']=='manual'


@pytest.mark.parametrize('confidence',[None,'human_confirmed'])
def test_manual_major_with_complete_title_needs_no_model_reclassification(tmp_path,confidence):
    classification={'major':'经济学','method':'manual'}
    if confidence is not None:classification['confidence']=confidence
    selected={'major':'经济学','classification':classification}
    item,agent=run_article(tmp_path,'法学','人工智能应用的法律分析','专业：法学',selected)
    assert item['status']=='pass',item['reason']
    assert item['major']=='经济学'
    assert item['classification']['source']=='manual'
    assert not any(task=='classify' for task,_ in agent.tasks)


def test_title_fallback_cannot_change_a_confirmed_major(tmp_path):
    item,agent=run_article(tmp_path,'法学','企业内部控制\n有效性\n研究','专业：法学')
    assert item['status']=='reject'
    assert item['reason_kind']=='classification'
    assert item['major']=='法学'
    requests=[payload for task,payload in agent.tasks if task=='classify']
    assert requests and requests[0]['majors']==['法学']


def saved_preparation(tmp_path, *, stale_parser=False, manual=False):
    source=tmp_path/'inputs/法学/2019法学2班/paper.txt'
    source.parent.mkdir(parents=True)
    raw=article('人工智能生成内容的著作权保护')
    source.write_text(raw,encoding='utf8')
    store=Store(tmp_path/'output')
    old_work=store.runs/'old/private/item';old_work.mkdir(parents=True)
    identity={'id':'old','title':'人工智能生成内容的著作权保护','major':'经济学' if manual else '人工智能',
              'category':'经管财会类' if manual else '计算机软件与数据类',
              'processing_version':VERSION,'original_source_sha256':digest(source)}
    package=old_work/'package'
    meta=build_package(source,package,identity,redact_content=False)
    if stale_parser:
        meta['parser_version']='evidence-old'
        for block in meta['blocks']:
            block['text']=block.get('text','').replace('关键原文必须从原生文件恢复。','')
    classification={'major':identity['major'],'title':identity['title'],
                    'method':'manual' if manual else 'original_rules',
                    'source':'manual' if manual else 'title','confidence':'high'}
    prepared={'raw':raw,'reliable':True,'screen':coarse_screen(raw,True),'hint_title':identity['title'],
              'hint_confidence':'high','meta':meta,
              'identity':{**{k:identity[k] for k in ('title','major','category')},'classification':classification}}
    checkpoint=save_prepared(old_work,source,package,{'source_hash':digest(source)},prepared,VERSION)
    return store,{'path':str(source),'name':source.name,'prepared_checkpoint':checkpoint}


def test_retry_reclassifies_old_automatic_identity_and_saves_new_checkpoint(tmp_path):
    store,record=saved_preparation(tmp_path)
    old_checkpoint=Path(record['prepared_checkpoint']['path'])
    old_bytes=old_checkpoint.read_bytes()
    processor=AutoProcessor(store);processor.agent=OfflineAgent()
    item=processor.run([record],operation='retry_ai')['items'][0]
    assert item['status']=='pass',item['reason']
    assert item['major']=='法学'
    new_checkpoint=Path(item['prepared_checkpoint']['path'])
    assert new_checkpoint!=old_checkpoint
    prepared=json.loads(new_checkpoint.read_text(encoding='utf8'))['preparation']
    assert prepared['identity']['major']==prepared['meta']['major']=='法学'
    assert old_checkpoint.read_bytes()==old_bytes


def test_retry_preserves_a_checkpoint_manual_major(tmp_path):
    store,record=saved_preparation(tmp_path,manual=True)
    processor=AutoProcessor(store);processor.agent=OfflineAgent()
    item=processor.run([record],operation='retry_ai')['items'][0]
    assert item['major']=='经济学'
    assert item['classification']['method']=='manual'


def test_retry_rebuilds_stale_parser_meta_from_verified_native_source(tmp_path):
    store,record=saved_preparation(tmp_path,stale_parser=True)
    processor=AutoProcessor(store);processor.agent=OfflineAgent()
    item=processor.run([record],operation='retry_ai')['items'][0]
    assert item['status']=='pass',item['reason']
    assert '关键原文必须从原生文件恢复。' in Path(item['path']).read_text(encoding='utf8')
    from evidence import VERSION as parser_version
    final_meta=json.loads((Path(item['package_path'])/'metadata.json').read_text(encoding='utf8'))
    assert final_meta['parser_version']==parser_version
