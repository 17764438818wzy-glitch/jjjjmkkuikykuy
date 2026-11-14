"""Create an isolated education first-pass pipeline without structure compilers."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

from education_stream_ingest import write_json
from education_tree_optimization import _by_name

ROOT = Path(__file__).resolve().parent
PRIVATE = ROOT / 'data/private/education_pilot'
NAME = '教育论文研究范例链路 v5·方法复用快速首轮'
REMOVED = {'Compiler:PaperStructure', 'Compiler:PaperWiki'}
LEFT = 'VariableAssigner:EducationIndexInput'
RIGHT = 'CodeExec:ArtifactSources'


def fast_dsl(source: dict) -> dict:
    dsl = copy.deepcopy(source)
    components = dsl['components']
    if not REMOVED <= components.keys():
        raise ValueError('来源 Pipeline 缺少预期结构编译节点')
    if (components[LEFT]['downstream'] != ['Compiler:PaperStructure']
            or components[RIGHT]['upstream'] != ['Compiler:PaperWiki']
            or components[RIGHT]['obj']['params']['arguments']['compiled']
            != 'Compiler:PaperWiki@chunks'):
        raise ValueError('来源 Pipeline 结构编译连接已改变')
    for key in REMOVED:
        del components[key]
    components[LEFT]['downstream'] = [RIGHT]
    components[RIGHT]['upstream'] = [LEFT]
    components[RIGHT]['obj']['params']['arguments']['compiled'] = LEFT + '@chunks'
    graph = dsl['graph']
    graph['nodes'] = [row for row in graph['nodes'] if row['id'] not in REMOVED]
    for node in graph['nodes']:
        if node['id'] == RIGHT and 'data' in node:
            arguments = node['data']['form']['arguments']
            if arguments.get('compiled') != 'Compiler:PaperWiki@chunks':
                raise ValueError('Pipeline 图形参数与执行参数不一致')
            arguments['compiled'] = LEFT + '@chunks'
    graph['edges'] = [row for row in graph['edges']
                      if row['source'] not in REMOVED and row['target'] not in REMOVED]
    graph['edges'].append({'id': f'xy-edge__{LEFT}start-{RIGHT}end',
                           'source': LEFT, 'sourceHandle': 'start',
                           'target': RIGHT, 'targetHandle': 'end'})
    ids = set(components)
    if any(row['id'] not in ids for row in graph['nodes']) or any(
            row['source'] not in ids or row['target'] not in ids for row in graph['edges']):
        raise ValueError('快速 Pipeline 存在悬空节点')
    if any(key in json.dumps(dsl, ensure_ascii=False) for key in REMOVED):
        raise ValueError('快速 Pipeline 仍引用结构编译节点')
    return dsl


def deploy() -> dict:
    sys.path.insert(0, str(PRIVATE))
    from deploy_pilot import client, state

    receipt_path = PRIVATE / 'education_fast_pipeline.json'
    receipt = json.loads(receipt_path.read_text(encoding='utf-8')) if receipt_path.exists() else {}
    source_id = state['full_ingest_pipeline_id']
    source = client.request('GET', '/agents/' + source_id)
    if source.get('id') != source_id or source_id == state.get('finance_pipeline_id'):
        raise ValueError('教育来源 Pipeline 身份不一致')
    dsl = fast_dsl(source['dsl'])
    existing = _by_name(client, '/agents', 'canvas', 'title', NAME)
    if existing is None:
        existing = client.request('POST', '/agents', {
            'title': NAME,
            'description': '教育论文首轮标注、准入、Profile 和正文索引；目录及方法关系按正文证据生成。',
            'canvas_category': 'dataflow_canvas', 'canvas_type': '', 'dsl': dsl})
    remote = client.request('GET', '/agents/' + existing['id'])
    if remote.get('dsl') != dsl:
        raise ValueError('隔离教育快速 Pipeline 远端配置与预期不一致')
    receipt = {'source_pipeline_id': source_id, 'pipeline_id': existing['id'],
               'name': NAME, 'verified': True}
    write_json(receipt_path, receipt)
    return receipt


if __name__ == '__main__':
    print(json.dumps(deploy(), ensure_ascii=False, indent=2))
