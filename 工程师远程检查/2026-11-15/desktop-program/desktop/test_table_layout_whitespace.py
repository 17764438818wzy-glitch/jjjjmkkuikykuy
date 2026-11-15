"""Table replacement must preserve native cells, including display line wraps."""
import copy

import fitz
import pytest

from auto_evidence import Rejected
from evidence_tasks import resolve_scoped


def original_table(text='a1 未被同伴邀请能很快选择并加入\n游戏'):
    values=[['参与游戏方式','次数'],[text,'30'],['a2 在同伴邀请下加入','22']]
    cells=[]
    for row,values_row in enumerate(values):
        for column,value in enumerate(values_row):
            cells.append({'row':row,'column':column,'rowspan':1,'colspan':1,'text':value,
                          'missing_geometry':False,
                          'position':{'page':1,'bbox':[20+100*column,20+20*row,120+100*column,40+20*row]}})
    return {'id':'table_native','type':'table','rows':3,'columns':2,
            'position':{'page':1,'bbox':[20,20,220,80]},'grid':[[0,1],[2,3],[4,5]],
            'cells':cells,'header_rows':[0],'complex':True}


def run_replacement(tmp_path,original,rows):
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        pdf.new_page();pdf.save(source)
    before=source.read_bytes()
    meta={'blocks':[copy.deepcopy(original)],'resources':[],
          'issues':[{'code':'pdf_cell_relationship_review','position':copy.deepcopy(original['position']),'status':'open'}]}
    class Agent:
        def call(self,task,payload,images=()):
            assert task=='repair'
            return {'decision':'pass','reason':'按原页核对表格','sensitive_boxes':[],
                    'insertions':[{'after_id':'table_native','kind':'table','operation':'replace_table','page':1,'rows':rows}]}
    resolve_scoped(meta,tmp_path,source,tmp_path/'work',80,Agent(),lambda *args:None)
    assert source.read_bytes()==before
    return next(block for block in meta['blocks'] if block['type']=='table')


def native_rows(original):
    return [[original['cells'][index]['text'] for index in row] for row in original['grid']]


@pytest.mark.parametrize('native,observed',[
    ('加入\n游戏','加入游戏'),('加入\r游戏','加入游戏'),
    ('加入\r\n游戏','加入游戏'),('加入\r\n游戏','加入\n游戏'),
    ('加入游戏','加入\n游戏'),
    ('数值1\r\n0','数值1\n0'),
])
def test_layout_linebreak_only_uses_exact_native_utf8_bytes(tmp_path,native,observed):
    original=original_table('a1 '+native);snapshot=copy.deepcopy(original)
    rows=native_rows(original);rows[1][0]='a1 '+observed
    recovered=run_replacement(tmp_path,original,rows)
    assert recovered['grid']==original['grid']
    for row in range(original['rows']):
        for column in range(original['columns']):
            old=original['cells'][original['grid'][row][column]]
            new=recovered['cells'][recovered['grid'][row][column]]
            assert (new['row'],new['column'])==(old['row'],old['column'])
            assert new['text'].encode('utf-8')==old['text'].encode('utf-8')
    assert original==snapshot


@pytest.mark.parametrize('mutation',[
    'number','punctuation','words','space','tab','swap_rows','swap_columns','reshape',
    'merge_unknown','missing_geometry','missing_bbox','bad_native_grid','ai_source',
    'numeric_linebreak','numeric_operator_linebreak',
])
def test_replacement_does_not_normalize_content_or_unproved_cell_relationships(tmp_path,mutation):
    original=original_table();rows=native_rows(original)
    rows[1][0]=rows[1][0].replace('\n','')
    if mutation=='number':rows[1][1]='31'
    elif mutation=='punctuation':rows[1][0]+='。'
    elif mutation=='words':rows[1][0]=rows[1][0].replace('邀请','要求')
    elif mutation=='space':original['cells'][3]['text']='1 0';rows[1][1]='10'
    elif mutation=='tab':original['cells'][3]['text']='1\t0';rows[1][1]='10'
    elif mutation=='swap_rows':rows[1],rows[2]=rows[2],rows[1]
    elif mutation=='swap_columns':rows=[list(reversed(row)) for row in rows]
    elif mutation=='reshape':rows=[[cell] for row in rows for cell in row]
    elif mutation=='merge_unknown':original['cells'][2]['rowspan']=2
    elif mutation=='missing_geometry':original['cells'][2]['missing_geometry']=True
    elif mutation=='missing_bbox':original['cells'][2]['position'].pop('bbox')
    elif mutation=='bad_native_grid':original['grid'][1]=[4,3]
    elif mutation=='ai_source':original['source']='ai_transcription'
    elif mutation=='numeric_linebreak':original['cells'][3]['text']='1\n0';rows[1][1]='10'
    elif mutation=='numeric_operator_linebreak':original['cells'][3]['text']='1\n.0';rows[1][1]='1.0'
    with pytest.raises(Rejected,match='遗漏或改变'):
        run_replacement(tmp_path,original,rows)


def test_exact_cell_multiset_cannot_hide_row_swap(tmp_path):
    original=original_table();rows=native_rows(original);rows[1],rows[2]=rows[2],rows[1]
    with pytest.raises(Rejected,match='遗漏或改变'):
        run_replacement(tmp_path,original,rows)
