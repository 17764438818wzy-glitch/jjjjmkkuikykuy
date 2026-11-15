"""Bind this folder's saved raw responses for the existing postprocessor."""
import hashlib
from pathlib import Path

from paperflow_prefect.llm_queue.folder_completion import expected_components, inspect_folder
from paperflow_prefect.records import identity as fingerprint
from paperflow_v3.control.receipts import read_receipts, save_receipt
from paperflow_v3.control.stage_admission import unsettled_requests
from .body_adapter import frozen_plan_evidence
from .handoff_inputs import _saved_response, build_inputs


def _saved_inputs(*, identity, manifest, paper, backend, records):
    """Verify this child's saved replies without rereading or publishing history."""
    from preextract_scheduler_adapter import request_observation
    if manifest.get('body_plan_revision'):
        from .body_plan_revision import validate_revision
        validate_revision(records, identity, manifest)

    if unsettled_requests(records, 'llm'):
        raise RuntimeError('原模型请求结果待核对，未进入后处理')
    saved = {}
    for row in records:
        if row.get('kind') != 'llm_response_saved':
            continue
        data = row['data']
        response_text = data.get('response_text')
        if (not isinstance(response_text, str) or
                hashlib.sha256(response_text.encode('utf8')).hexdigest() != data['response_sha256']):
            raise ValueError('服务器未完整保存原始模型响应')
        key = data['component_id']
        evidence = {name: data[name] for name in
                    ('request_id', 'kind', 'request_sha256', 'response_sha256', 'response_path')}
        if key in saved and saved[key] != evidence:
            raise ValueError('同一资料的原始模型回复身份不一致')
        saved[key] = evidence

    components = expected_components(manifest, identity['task_run_id'])
    by_file = {row['file_id']: row for row in paper['attachments']}
    admissions = []
    for component in components:
        evidence = saved.get(component['component_id'])
        if evidence is None:
            continue
        if evidence['kind'] != component['kind']:
            raise ValueError('模型回复类型与资料不一致')
        work = (Path(manifest['body_work']) if component['kind'] == 'body' else
                Path(backend.attachment_work(paper, by_file[component['file_id']])))
        admissions.append(dict(task_id=identity['task_id'], task_run_id=identity['task_run_id'],
            receipt_id=identity['receipt_id'], component_id=component['component_id'],
            request_id=evidence['request_id'],
            kind=component['kind'], source_sha256=component['source_sha256'], state='raw_complete',
            request_sha256=evidence['request_sha256'], response_sha256=evidence['response_sha256'],
            work_dir=str(work), model_dir=str(Path(evidence['response_path']).parent)))

    digest = fingerprint(manifest)
    inputs = build_inputs(identity=identity, verified={'manifest': manifest, 'paper': paper},
        admissions=admissions, manifest_sha256=digest, expected_components=expected_components,
        inspect_folder=inspect_folder, observe_request=request_observation,
        frozen_plan=frozen_plan_evidence, attachment_work=backend.attachment_work)
    ranking_component = None
    if manifest['capacity']['plan_identity']['mode'] == 'partitioned':
        ranking = next((row['data'] for row in records if row.get('kind') == 'llm_whole_ranking_complete'), None)
        body_parts = [part for part in components if part['kind'] == 'body']
        part_responses = [{'part_id': part['part_id'],
                           'response_sha256': saved[part['component_id']]['response_sha256']}
                          for part in body_parts]
        if (not ranking or ranking.get('status') != 'raw_complete'
                or ranking.get('coverage') != [part['part_id'] for part in body_parts]
                or ranking.get('part_responses') != part_responses):
            raise ValueError('全文专业判定尚未完整保存')
        ranking_component = saved.get(ranking['component_id'])
        if (not ranking_component or any(ranking_component.get(key) != ranking.get(key)
                for key in ('request_id', 'request_sha256', 'response_sha256', 'response_path'))):
            raise ValueError('全文专业判定请求与回复身份不一致')
        refs = _saved_response({'model_dir': str(Path(ranking['response_path']).parent),
            'request_sha256': ranking['request_sha256'], 'response_sha256': ranking['response_sha256']},
            request_observation)
        inputs['body']['whole_paper_ranking'] = {'response': refs['response'], 'request': refs['request'],
            'state': refs['state'], 'events': refs['events'], 'coverage': ranking['coverage'],
            'part_responses': ranking['part_responses']}
    completion = {'status': 'raw_complete', 'ranking_complete': True,
            'manifest_sha256': digest,
            'components': [{'component_id': row['component_id'], 'request_sha256': row['request_sha256'],
                            'response_sha256': row['response_sha256']} for row in admissions],
            'whole_paper_ranking': ranking_component}
    return inputs, completion


def saved_postprocessing_inputs(*, identity, manifest, paper, backend, records):
    """Consume the bound completion; a package-phase retry never reruns the LLM."""
    inputs, expected = _saved_inputs(identity=identity, manifest=manifest, paper=paper, backend=backend, records=records)
    completion = next((row['data'] for row in records if row.get('kind') == 'llm_folder_complete'), {})
    if any(completion.get(key) != value for key, value in expected.items()):
        raise ValueError('已保存的模型完成凭据与当前资料不一致')
    return inputs


async def build_postprocessing_inputs(*, identity, manifest, paper, backend):
    """Publish LLM completion only after every required raw response is saved."""
    records = await read_receipts(identity['task_run_id'])
    inputs, completion = _saved_inputs(identity=identity, manifest=manifest, paper=paper, backend=backend, records=records)
    await save_receipt(task_id=identity['task_id'], folder_id=identity['group_id'],
        child_flow_run_id=identity['task_run_id'], event_id=identity['task_run_id'] + '-llm-complete',
        kind='llm_folder_complete', data=completion)
    return inputs
