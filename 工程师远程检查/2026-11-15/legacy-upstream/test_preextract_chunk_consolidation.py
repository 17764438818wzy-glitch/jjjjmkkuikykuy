import copy
import hashlib
import json

from PIL import Image

from test_preextract_publish import Counter, fixture, prepare


def _records(result):
    from paperflow.rag import RagPublisher
    return RagPublisher(None).records(result['packet'])


def _tree(candidate):
    ref = candidate['profile']['learning_units'][0]['source_refs'][0]
    candidate['tree']['nodes'] = [
        dict(id='S1', title='第一章 研究方法', parent_id=None, order=0, function='完整研究安排', unit_ids=['U1']),
        dict(id='S2', title='1.1 控制实验', parent_id='S1', order=1, function='控制实验的父节', unit_ids=['U1']),
        dict(id='S3', title='1.1.1 参数选择', parent_id='S2', order=2, function='必须保留的参数选择依据',
             source_refs=[dict(ref, section_id='S3')], unit_ids=['U1']),
        dict(id='S4', title='1.1.1.1 稳定性', parent_id='S3', order=3, output='完整稳定性输出',
             connections=[dict(target='S5', relation='后续检验')], unit_ids=['U1']),
        dict(id='S5', title='1.2 对照检验', parent_id='S1', order=4, function='独立对照组', unit_ids=['U1']),
    ]


def test_chapter_descriptions_group_by_actual_second_level_and_keep_all_nodes(tmp_path):
    c, b, _ = fixture()
    _tree(c)
    original = copy.deepcopy(c)
    result = prepare(tmp_path, c, b)
    chapters = [r for r in result['packet']['chunks'] if r['kind'] == 'chapter']
    assert len(chapters) == 3
    grouped = next(r for r in chapters if 'S2' in r['section_ids'])
    assert grouped['section_ids'] == ['S2', 'S3', 'S4']
    assert grouped['unit_ids'] == ['U1']
    assert all(text in grouped['content'] for text in [
        '必须保留的参数选择依据', '完整稳定性输出', '后续检验', 'B1', 'source_refs', 'S3', 'S4'])
    assert result['packet']['tree']['nodes'] == original['tree']['nodes']
    assert result['packet']['profile']['learning_units'] == original['profile']['learning_units']
    assert c == original


def test_large_chapter_group_keeps_descendant_limits_and_all_text_across_parts(tmp_path):
    c, b, _ = fixture()
    _tree(c)
    paragraphs = [f'参数研究步骤{i:03d}及其成立条件。' for i in range(160)]
    c['tree']['nodes'][2]['approach'] = '\n'.join(paragraphs)
    c['gaps'] = [dict(id='G1', target_pointer='/tree/nodes/2',
                     use_limit='该参数尚未完成外部验证', source_refs=[])]
    result = prepare(tmp_path, c, b)
    rows = [r for r in result['packet']['chunks'] if r['kind'] == 'chapter' and 'S2' in r['section_ids']]
    assert len(rows) > 1
    assert all(r['section_ids'] == ['S2', 'S3', 'S4'] for r in rows)
    assert all('该参数尚未完成外部验证' in r['content'] for r in rows)
    text = '\n'.join(r['content'] for r in rows)
    assert all(p in text for p in paragraphs)
    assert '完整稳定性输出' in text


def test_unparented_node_does_not_guess_group_from_numbered_title(tmp_path):
    c, b, _ = fixture()
    c['tree']['nodes'].append(dict(id='orphan', title='9.9.9 未知父章节', parent_id=None,
                                   order=1, function='未知层级的实质说明', unit_ids=[]))
    result = prepare(tmp_path, c, b)
    rows = [r for r in result['packet']['chunks'] if r['kind'] == 'chapter']
    assert len(rows) == 2
    assert any(r['section_ids'] == ['orphan'] and '实质说明' in r['content'] for r in rows)


def _asset(candidate, *, meaning='学生字段通过主键建立关系。'):
    ref = copy.deepcopy(candidate['profile']['learning_units'][0]['source_refs'][0])
    ref.update(asset_id='A1', source_kind='figure')
    return dict(id='A1', kind='table', storage='description', caption='实体关系图',
                meaning=meaning, purpose='解释主键关系的用途', materials_needed=['实体与字段清单'],
                organization='实体与关联线', storage_reason='文字足以表达关系',
                section_ids=['S1'], unit_ids=['U1'], source_refs=[ref], limits=['不能由实体推出性能'])


def test_short_chart_uses_one_asset_record_with_full_sources_and_linked_limits(tmp_path):
    c, b, _ = fixture()
    c['assets']['items'] = [_asset(c)]
    c['gaps'] = [dict(id='G1', target_pointer='/assets/items/0', use_limit='图表依据需结合正文', source_refs=[])]
    result = prepare(tmp_path, c, b)
    rows = _records(result)['body']
    chart_records = [r for r in rows if r[0] == 'asset_A1' or r[0].startswith('chart_')]
    assert len(chart_records) == 1
    assert chart_records[0][0] == 'asset_A1'
    description = result['packet']['assets'][0]['description']
    assert all(value in description for value in ['学生字段通过主键建立关系', '实体与字段清单',
        '图表依据需结合正文', '不能由实体推出性能', 'source_refs', 'B1', 'S1', 'U1'])
    assert result['packet']['assets'][0]['source_refs'] == c['assets']['items'][0]['source_refs']


def test_long_chart_first_segment_and_continuations_do_not_repeat_payload(tmp_path):
    c, b, _ = fixture()
    paragraphs = [f'图表方法第{i:03d}步的材料、关系与适用条件。' for i in range(160)]
    c['assets']['items'] = [_asset(c, meaning='\n'.join(paragraphs))]
    result = prepare(tmp_path, c, b)
    asset = result['packet']['assets'][0]
    continuations = [r for r in result['packet']['chunks'] if r['kind'] == 'chart']
    assert continuations
    text = asset['description'] + '\n' + '\n'.join(r['content'] for r in continuations)
    assert all(text.count(p) == 1 for p in paragraphs)
    assert all(r['asset_ids'] == ['A1'] and r['unit_ids'] == ['U1'] for r in continuations)
    assert '不能由实体推出性能' in asset['description']
    assert all('不能由实体推出性能' in r['content'] for r in continuations)
    assert asset['meaning'] == '\n'.join(paragraphs)
    assert result['validation']['embedding']['admission_verified']


def test_same_image_pixels_keep_both_distinct_chart_descriptions_searchable(tmp_path):
    c, b, _ = fixture()
    path = tmp_path / 'original.png'
    Image.new('RGB', (30, 20), 'white').save(path)
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    b['assets'] = [dict(id='I1', image_path=str(path), sha256=source_hash, width=30, height=20)]
    one = _asset(c, meaning='第一种独有的机构解释。')
    one.update(storage='image', source_image_id='I1')
    two = copy.deepcopy(one)
    two.update(id='A2', caption='同一图片的第二种用途', meaning='第二种独有的操作解释。')
    two['source_refs'][0]['asset_id'] = 'A2'
    omitted = dict(id='A3', kind='image', storage='omit', meaning='装饰图应当省略', section_ids=['S1'])
    c['assets']['items'] = [one, two, omitted]
    result = prepare(tmp_path, c, b)
    records = _records(result)
    assert len(records['images']) == 1
    emitted = '\n'.join(r[1] for values in records.values() for r in values)
    assert '第一种独有的机构解释' in emitted and '第二种独有的操作解释' in emitted
    assert '装饰图应当省略' not in emitted
    assert any(r['kind'] == 'chart' and r['asset_ids'] == ['A2'] for r in result['packet']['chunks'])
    assert not any(r['kind'] == 'chart' and r['asset_ids'] == ['A1'] for r in result['packet']['chunks'])
    assert [a['storage'] for a in result['packet']['assets']] == ['image', 'image', 'omit']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == source_hash


def test_legacy_packet_still_emits_its_original_chart_and_asset_records():
    import sys
    from test_preextract_publish import PAPERFLOW
    if str(PAPERFLOW) not in sys.path:
        sys.path.insert(0, str(PAPERFLOW))
    from paperflow.rag import RagPublisher
    packet = dict(chunks=[dict(id='chart_legacy', content='旧图表完整说明')],
                  profile={}, tree=dict(nodes=[]),
                  assets=[dict(id='old_asset', kind='image', storage='description', description='旧资产说明')])
    records = RagPublisher(None).raw_records(packet)['body']
    assert {r[0] for r in records} == {'chart_legacy', 'asset_old_asset', '__profile', '__tree'}

