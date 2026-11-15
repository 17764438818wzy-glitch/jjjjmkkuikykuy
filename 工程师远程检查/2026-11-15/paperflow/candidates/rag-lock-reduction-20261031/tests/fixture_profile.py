"""Synthetic profile fixture reused from native test_preextract_pipeline.py."""
import copy
from types import SimpleNamespace
from preextract_contract import load_template
pipeline = SimpleNamespace(load_template=load_template)

def _empty(value):
    if isinstance(value, dict):
        return {key:_empty(item) for key,item in value.items()}
    return [] if isinstance(value, list) else copy.deepcopy(value)

def _candidate(env, bundle):
    _, skeleton = pipeline.load_template(env['template'])
    candidate = _empty(skeleton)
    candidate.update(record_kind='filled_candidate', source=copy.deepcopy(bundle['source']))
    block = next(row for row in bundle['blocks'] if '先按两种规则排序' in row['text'])
    ref = dict(file_ref='main', block_id=block['id'], section_id=block['section_id'], asset_id=None,
               start=None, end=None, source_kind='author_text', locator_hint=None)
    candidate['source']['reading_scope'].update(read_refs=['main', block['id']], notes='根据提供的完整文本提取')
    profile = candidate['profile']
    profile.update(material_type='论文正文')
    profile['material_disposition'].update(decision='keep', reason='有明确的比较研究过程', source_refs=[ref])
    profile['article'].update(title='排程研究', keywords=['排程', '等待时间'], summary='比较两个排程方案的等待时间', source_refs=[ref])
    profile['research_profile'].update(problem='如何减少作业等待时间', object='作业排程', overall_path='整理作业数据后比较两种方案', unit_sequence=['U001'], source_refs=[ref])
    unit = _empty(skeleton['profile']['learning_units'][0])
    unit.update(id='U001', title='比较排程方案', question='两种方案的等待时间如何比较', what_to_learn='组织输入并计算比较结果',
        method='对比实验', is_core=True, core_reason='承载比较研究过程', work_attribution='author_work',
        interpretation='比较得到两组等待时间', conditions=[], limitations=['新问题需重新采集作业数据'],
        section_ids=[block['section_id']], source_refs=[ref])
    unit['method_evidence'].update(status='documented', detail='原文明确列出排序、记录及比较步骤', source_refs=[ref])
    unit['materials'] = [dict(id='M001', type='作业数据', description='三个作业的到达时刻和持续时间', key_fields=['到达时刻', '持续时间'],
        time_scope=None, comparison_objects=[], organization=None, source_refs=[ref])]
    unit['steps'] = [dict(id='T001', operation='按两种规则排序，记录作业等待时间并比较平均值', input_refs=['M001'], output_refs=['O001'], source_refs=[ref])]
    unit['outputs'] = [dict(id='O001', type='比较结果', content='两组等待时间', source_refs=[ref])]
    profile['learning_units'] = [unit]
    profile['routing_candidates'] = [dict(code='080901', name='计算机科学与技术', reason='排程算法比较', unit_ids=['U001'], source_refs=[ref])]
    nodes = []
    for section in bundle['sections']:
        row = _empty(skeleton['tree']['nodes'][0])
        row.update(id=section['id'], title=section['title'], parent_id=section.get('parent_id'), order=section['order'],
                   source_refs=[dict(ref, block_id=None, section_id=section['id'])], unit_ids=['U001'])
        nodes.append(row)
    candidate['tree']['nodes'] = nodes
    return candidate
