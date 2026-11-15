"""Deploy a model-only education Pipeline clone; never mutate finance."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path


NODE = 'Extractor:EducationAnnotation'
SCHOOL_MODEL = 'afb95246b4d311f1b74c89b9f52f3fbe'
OFFICIAL_MODEL = 'e7e4a8e0b01511f199856dc9cfded896'
TITLE = '教育论文研究范例链路 v5·DeepSeek 官方补位'


def model_only_dsl(source: dict, source_model_id: str, target_model_id: str) -> dict:
    result = copy.deepcopy(source)
    bound = [key for key, value in result['components'].items()
             if value.get('obj', {}).get('params', {}).get('llm_id')]
    if bound != [NODE]:
        raise ValueError('Education generation topology changed')
    params = result['components'][NODE]['obj']['params']
    if params['llm_id'] != source_model_id:
        raise ValueError('School model binding changed')
    params['llm_id'] = target_model_id
    mirrored = 0
    for node in result.get('graph', {}).get('nodes', []):
        form = node.get('data', {}).get('form', {})
        if form.get('llm_id'):
            if node.get('id') != NODE or form['llm_id'] != source_model_id:
                raise ValueError('Pipeline form and execution binding disagree')
            form['llm_id'] = target_model_id
            mirrored += 1
    if mirrored != 1:
        raise ValueError('Education model form binding is missing or duplicated')
    if contract_hash(source) != contract_hash(result):
        raise ValueError('Non-model Pipeline fields changed')
    return result


OUTPUT_LIMIT_REMINDER = """

# 输出自检 education_output_limits/1.0
evidence 必须为对象数组，最多12项，通常用4至8条连续正文引文覆盖必填支持字段；一条引文可以支持多个确实有依据的字段，不为每个研究步骤重复添加 evidence。不能以对象、字符串或嵌套数组代替 evidence 数组。输出前检查数量、字段名称和原文连续性；保持原有准入和原文证据要求。
coverage_token 只填全文结束标记中给出的32位小写十六进制字符串，不加 coverage_token= 前缀、不附解释，逐字符核对；不要用 paper_id、正文哈希或先前文章的校验值。
研究方法只填规则指定的规范名：作者称“文献分析法”或“文献资料法”时，应按实际实施填写“文献研究法”，不要自行增加词表。
覆盖 research_subject 的证据应从正文研究对象、方法或结果章节复制，不用摘要、绪论计划或附录问卷替代实际研究对象证据。"""


def annotation_limits_dsl(source: dict) -> dict:
    result=copy.deepcopy(source)
    params=result['components'][NODE]['obj']['params']
    prompt=params.get('sys_prompt')
    if not isinstance(prompt,str) or not prompt: raise ValueError('Annotation prompt is missing')
    forms=[n['data']['form'] for n in result['graph']['nodes'] if n['id']==NODE]
    if len(forms)!=1 or forms[0].get('sys_prompt')!=prompt: raise ValueError('Annotation prompt form mismatch')
    if OUTPUT_LIMIT_REMINDER not in prompt:
        params['sys_prompt']=prompt+OUTPUT_LIMIT_REMINDER
        forms[0]['sys_prompt']=params['sys_prompt']
    return result


def contract_hash(dsl: dict) -> str:
    normalized = copy.deepcopy(dsl)
    def visit(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == 'llm_id':
                    value[key] = '<model-binding>'
                else:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)
    visit(normalized)
    encoded = json.dumps(normalized, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def prepare(client, state: dict, deploy: bool = False) -> dict:
    source_id = state['full_ingest_pipeline_id']
    if (source_id == '89ea3542b52a11f1b21a85c6855e5155'
            or state['paper_dataset_id'] != 'ecf88dc4b82b11f1a8846d29005ec004'):
        raise ValueError('Destination is not the authorized education Pipeline')
    models = {row['model_id']: row for row in client.request('GET', '/models')}
    official = models.get(OFFICIAL_MODEL, {})
    school = models.get(SCHOOL_MODEL, {})
    if (official.get('provider_name') != 'DeepSeek' or official.get('instance_name') != 'ds'
            or official.get('name') != 'deepseek-v4-flash'
            or school.get('instance_name') != 'ustc' or 'chat' not in official.get('model_type', [])):
        raise ValueError('Verified DeepSeek model registration changed')
    source = client.request('GET', '/agents/' + source_id)
    if source.get('id') != source_id:
        raise ValueError('Education source Pipeline identity mismatch')
    dsl = model_only_dsl(source['dsl'], SCHOOL_MODEL, OFFICIAL_MODEL)
    result = {'source_pipeline_id': source_id, 'contract_version': contract_hash(dsl),
              'school_model_id': SCHOOL_MODEL, 'official_model_id': OFFICIAL_MODEL,
              'verified_model_registration': True, 'deployed': False}
    if not deploy:
        return result
    from education_tree_optimization import _by_name
    existing = _by_name(client, '/agents', 'canvas', 'title', TITLE)
    if existing is None:
        existing = client.request('POST', '/agents', {
            'title': TITLE, 'description': '与学校教育快速链路共用契约；仅模型绑定不同。',
            'canvas_category': 'dataflow_canvas', 'canvas_type': '', 'dsl': dsl})
    remote = client.request('GET', '/agents/' + existing['id'])
    if remote.get('dsl') != dsl:
        raise ValueError('Official Pipeline does not match verified model-only clone')
    result.update(official_pipeline_id=remote['id'], deployed=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-root', type=Path, default=Path(__file__).parent/'data')
    parser.add_argument('--deploy', action='store_true')
    args = parser.parse_args()
    from auto_config import load_key
    from education_stream_ingest import write_json
    from ragflow_client import RagflowClient
    data = args.data_root.resolve()
    settings = json.loads((data/'settings.json').read_text(encoding='utf-8'))
    state = json.loads((data/'private/education_pilot/deployment.json').read_text(encoding='utf-8'))
    client = RagflowClient(settings['ragflow_url'], load_key(data, 'ragflow'))
    result = prepare(client, state, args.deploy)
    if args.deploy:
        write_json(data/'private/education_dual_channel/deployment.json', result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
