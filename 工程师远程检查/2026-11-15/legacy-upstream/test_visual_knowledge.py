import copy
import io
import json
from pathlib import Path

import pytest
from PIL import Image,ImageDraw
from auto_evidence import media_privacy, text_privacy, Rejected
from evidence_tasks import _transcribed_block, visible_text
from agent_client import AgentError


def visual_meta(tmp_path):
    (tmp_path/'assets').mkdir();(tmp_path/'_review').mkdir()
    figure=Image.new('RGB',(100,80),'white')
    ImageDraw.Draw(figure).line([(10,70),(50,40),(90,10)],fill='blue',width=2)
    figure.save(tmp_path/'_review/figure.png')
    return {'title':'企业经营分析','formulas':[],
        'blocks':[{'id':'figure','type':'image','resource_id':'r','caption':'图1 收入趋势','position':{'body_index':3}}],
        'resources':[{'id':'r','kind':'image','position':{'body_index':3},'local_path':'_review/figure.png'}]}


def answer():
    return {'decision':'pass','reason':'图形可辨认','research':True,'text':'年度收入趋势','description':'收入折线图','sensitive_boxes':[],
            'chart_spec':{'chart_type':'line','x_axis':{'label':'年份','unit':None},
                          'y_axes':[{'label':'收入','unit':'亿元','side':'left'}],
                          'series':[{'name':'营业收入','axis_side':'left'}],'legend':['营业收入'],'data_labels':True},
            'structured_table':None}


def test_single_image_call_carries_chart_recipe_and_new_fields_into_privacy(tmp_path):
    meta=visual_meta(tmp_path)
    class Agent:
        tasks=[]
        def call(self,task,payload,images=()):
            self.tasks.append(task)
            if task=='privacy_image':return answer()
            assert '亿元' in payload['text'] and '营业收入' in payload['text']
            return {'decision':'pass','reason':'无敏感内容','sensitive_strings':[]}
    agent=Agent();media_privacy(meta,tmp_path,agent,lambda *a:None);text_privacy(meta,agent,lambda *a:None)
    assert agent.tasks==['privacy_image','privacy_text']
    assert meta['blocks'][0]['chart_spec']['chart_type']=='line'
    assert meta['resources'][0]['chart_spec']['source']=='ai_visual_observation'
    assert meta['text_privacy_complete'] is True


def test_observed_label_with_new_identity_still_blocks_unmasked_pixels(tmp_path):
    meta=visual_meta(tmp_path)
    class Agent:
        def call(self,task,payload,images=()):
            if task=='privacy_image':
                result=answer();result['chart_spec']['series'][0]['name']='张测同学';return result
            return {'decision':'pass','reason':'姓名','sensitive_strings':['张测同学']}
    agent=Agent();media_privacy(meta,tmp_path,agent,lambda *a:None)
    with pytest.raises(Rejected):text_privacy(meta,agent,lambda *a:None)
    assert not meta.get('text_privacy_complete')


def test_scoped_page_crop_keeps_visual_recipe_and_literal_screenshot_rows(tmp_path):
    (tmp_path/'assets').mkdir();buf=io.BytesIO();figure=Image.new('RGB',(100,100),'white')
    ImageDraw.Draw(figure).line([(20,70),(40,40),(70,20)],fill='blue',width=2);figure.save(buf,format='PNG')
    entry=answer();entry.update(kind='image',bbox=[.1,.1,.8,.8],structured_table={
        'rows':[['年度','收入'],['2023','001.20'],['2024','']],'header_rows':[0],'units':'亿元','notes':[]})
    block,resource=_transcribed_block(entry,'img',{'page':1},tmp_path,buf.getvalue(),[])
    assert block['structured_table']['rows'][1][1]=='001.20'
    assert resource['chart_spec']['chart_type']=='line'
    assert '001.20' in visible_text({'blocks':[block],'resources':[resource]})


def test_visual_chart_cannot_smuggle_guessed_values(tmp_path):
    meta=visual_meta(tmp_path)
    class Agent:
        def call(self,*a,**kw):
            result=answer();result['chart_spec']['series'][0]['values']=[10,20];return result
    with pytest.raises(AgentError):media_privacy(meta,tmp_path,Agent(),lambda *a:None)


def test_full_pipeline_publishes_new_files_and_browse_links(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store,scan_sources
    from test_workbench import paper
    root=tmp_path/'in';root.mkdir();(root/'paper.txt').write_text(paper(),encoding='utf-8')
    processor=AutoProcessor(Store(tmp_path/'out'))
    class Agent:
        def call(self,task,payload,images=()):
            if task=='privacy_text':return {'decision':'pass','reason':'通过','sensitive_strings':[]}
            if task=='classify':return {'decision':'pass','reason':'通过','major':'会计学','title':'企业财务管理研究'}
            return {'decision':'pass','reason':'通过'}
    processor.agent=Agent();result=processor.run(scan_sources(root));item=result['items'][0]
    assert item['status']=='pass',item['reason']
    package=Path(item['package_path']);assert (package/'manifest.json').is_file()
    manifest=json.loads((package/'manifest.json').read_text(encoding='utf-8'))
    assert manifest['paper_id']=='paper_'+item['source_hash'][:24]
    for row in manifest['files']:assert (package/row['path']).is_file()
    assert '_review' not in {p.name for p in package.iterdir()}
    assert item['knowledge_manifest_path']==str(package/'manifest.json')
    assert 'manifest.json' in (package/'document.md').read_text(encoding='utf-8')
