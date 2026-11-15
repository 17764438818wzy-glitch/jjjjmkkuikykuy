import json
import pytest
from evidence_tasks import _locate
from auto_evidence import Rejected
from test_common_contracts import client,picture,repair


def test_split_contents_paragraph_with_leaders_keeps_its_own_page():
    pos={'part':'word/document.xml','body_index':54}
    blocks=[{'type':'paragraph','position':pos,'text':'3.2.4 '},
            {'type':'paragraph','position':{**pos,'object_ref':'object_1'},'text':'','graphic_object':True},
            {'type':'paragraph','position':pos,'text':'篮球运动损伤程度\t…………………………………..5'}]
    pages=['目录\n3.2.4 篮球运动损伤程度…………………………………..5','3.2.4 篮球运动损伤程度\n正文']
    assert _locate({'position':blocks[1]['position']},blocks,pages)=={0}
    with pytest.raises(Rejected):_locate({'position':blocks[1]['position']},blocks,[pages[0],pages[0]])


def test_incomplete_pdf_table_crop_gets_bounded_schema_feedback(monkeypatch):
    good=repair([.1,.2,.8,.9]);bad=repair([.1,.2,.8,.5])
    c=client(monkeypatch,bad,good)
    payload={'page':1,'known_blocks':[{'id':'b1','table_candidate':True,'table_bbox':[.1,.2,.8,.9],'candidate_pages':[1]}]}
    assert c.call('repair',payload,[picture()])==good
    assert len(c.opener.requests)==2
    assert 'table_bbox' in c.opener.requests[1]['messages'][-1]['content']


def test_merged_table_without_required_grid_gets_response_feedback(monkeypatch):
    bad=repair([.1,.2,.8,.9])
    good={**bad,'insertions':[{'kind':'table','after_id':'b1','page':1,'operation':'replace_table','rows':[['项目',''],['收入','100']]}]}
    c=client(monkeypatch,bad,good)
    payload={'page':1,'known_blocks':[{'id':'b1','type':'table','requires_table_replacement':True}]}
    assert c.call('repair',payload,[picture()])==good
    assert 'replace_table' in c.opener.requests[1]['messages'][-1]['content']


def test_table_continuation_cells_are_present_in_model_input(tmp_path):
    import fitz
    from evidence_tasks import resolve_scoped
    source=tmp_path/'table.pdf'
    with fitz.open() as pdf:pdf.new_page();pdf.save(source)
    position={'page':1}
    cell={'row':0,'column':0,'rowspan':2,'colspan':1,'text':'原始',
          'continuation_text':[{'row':1,'text':'续格文字'}]}
    meta={'blocks':[{'id':'b1','type':'table','position':position,'cells':[cell]}],
          'resources':[],'issues':[{'code':'merged_cell_content','position':position}]}
    class Probe:
        def call(self,task,payload,images=()):
            self.payload=payload
            return {'decision':'reject','reason':'fixture stop'}
    agent=Probe()
    with pytest.raises(Rejected):resolve_scoped(meta,tmp_path,source,tmp_path/'work',80,agent,lambda *a:None)
    target=agent.payload['known_blocks'][0]
    assert target['cells'][0]['continuation_text']==cell['continuation_text']
    assert target['requires_table_replacement'] is True
