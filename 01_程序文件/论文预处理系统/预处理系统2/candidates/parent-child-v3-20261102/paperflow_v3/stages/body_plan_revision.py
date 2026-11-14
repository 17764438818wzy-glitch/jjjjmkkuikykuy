"""Explicit, source-bound input-plan revisions on the same native folder ticket.

This module grants no automatic retry. The latest manual retry owns new sends;
stored revisions remain readable for downstream processing after that attempt.
"""
from pathlib import Path
from types import MethodType
from uuid import UUID

from paperflow_prefect.records import identity as digest, read_json, sha_file
from paperflow_prefect.llm_queue.folder_completion import expected_components

FIELD = 'body_plan_revision'
SCHEMA = 'paperflow-body-plan-revision/1'


def require(value, reason):
    if not value: raise ValueError('正文新计划核对失败：' + reason)


def _witness(records, reference):
    for row in records:
        if (row.get('_native_artifact_id') == reference['artifact_id']
                and row.get('event_id') == reference['event_id'] and row.get('kind') == reference['kind']
                and digest(row.get('data') or {}) == reference['data_sha256']):
            return row
        # The existing explicit-rerun view already checked these exact Native
        # records before replacing them with immutable acknowledgement markers.
        if row.get('kind') == '_user_authorized_model_replacement':
            for ref in row.get('data', {}).get('records', []):
                if (ref.get('source_artifact_id') == reference['artifact_id']
                        and ref.get('event_id') == reference['event_id'] and ref.get('event_kind') == reference['kind']
                        and ref.get('data_sha256') == reference['data_sha256']):
                    return row
    raise ValueError('正文新计划的原生历史凭据缺失或变化')


def validate_revision(records, identity, manifest, *, sending=False):
    manual = next((row for row in records if row.get('kind') == 'manual_retry'
                   and FIELD in row.get('data', {})), None)
    require(manual is not None, '缺少本工单明确的新计划授权')
    data, grant = manual['data'], manual['data'][FIELD]
    require(isinstance(grant, dict) and grant.get('schema') == SCHEMA, '授权格式')
    require(grant.get('identity') == identity and data.get('identity') == identity
        and manual.get('task_id') == identity['task_id'] and manual.get('folder_id') == identity['group_id'], '原工单身份')
    revision = grant.get('retry_id')
    try: UUID(revision)
    except (ValueError, TypeError, AttributeError): raise ValueError('正文新计划版本无效') from None
    require(data.get('retry_id') == revision and manifest.get(FIELD) == revision, '计划版本')
    if sending:
        latest = next((row.get('data') or {} for row in records if row.get('kind') == 'manual_retry'), {})
        require(latest.get('retry_id') == revision and latest.get('stage') == 'llm'
            and latest.get('state') in {'authorized', 'scheduled', 'running'}, '本次发送授权已变化')
    require(grant.get('reason') in {'output_length', 'image_decode_failure'}, '未经核对的重规划原因')
    for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'):
        require(manifest.get(key) == identity.get(key), '交接身份')
    new_ref, old_ref = grant['new_manifest'], grant['old_manifest']
    require(sha_file(new_ref['path']) == new_ref['sha256'] and read_json(new_ref['path']) == manifest, '新交接文件')
    require(sha_file(old_ref['path']) == old_ref['sha256'], '旧交接文件')
    old = read_json(old_ref['path'])
    for key in ('body', 'attachments', 'receipt', 'material', 'reading_material'):
        require(manifest.get(key) == old.get(key), '原材料绑定发生变化：' + key)
    for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'):
        require(old.get(key) == identity.get(key), '旧交接身份')
    old_work, work = Path(grant['old_body_work']).resolve(), Path(grant['new_body_work']).resolve()
    require(Path(old['body_work']).resolve() == old_work
        and work == old_work / 'body-plan-revisions' / revision
        and Path(manifest['body_work']).resolve() == work, '新工作目录')
    require(manifest.get('planning_context') == grant.get('planning_context'), '分段范围策略')
    require(isinstance(grant.get('protected_files'), dict) and grant['protected_files'], '缺少来源证据')
    for path, expected in grant['protected_files'].items():
        require(sha_file(path) == expected, '原文件或旧请求已变化')
    preprocessing = _witness(records, grant['old_preprocessing'])
    prepared = preprocessing.get('data', {})
    require(all(prepared.get(k) == identity[k] for k in ('task_id','group_id','paper_id','receipt_id','attempt_id')),
        '旧前处理与原交接不一致')
    if grant['reason']=='output_length':
        require(preprocessing.get('kind') == 'preprocessing_complete'
            and prepared.get('manifest_path') == old_ref['path'] and prepared.get('manifest_sha256') == old_ref['sha256'],
            '旧前处理完成凭据与交接不一致')
    else:
        require(preprocessing.get('kind')=='preprocessing_failed' and prepared.get('failure_reason')=='plan_unavailable'
            and old['capacity'].get('status')=='unavailable' and old['capacity'].get('error_type')=='SyntaxError',
            '原图片计划失败与交接不一致')
    failures = [_witness(records, reference) for reference in grant.get('old_failures', [])]
    old_ids = set(grant.get('old_body_components') or [])
    if grant['reason'] == 'output_length':
        require(old_ids == {row['component_id'] for row in expected_components(old, identity['task_run_id'])
                           if row['kind'] == 'body'} and bool(grant.get('old_failures')), '旧超限范围')
        from paperflow_prefect.llm_queue.raw_request import _body_hash
        for row in failures:
            failed = row.get('data', {})
            require(row.get('kind') == 'llm_request_failed' and failed.get('kind') == 'body'
                and failed.get('component_id') in old_ids
                and failed.get('failure_reason') == 'output_length_requires_changed_plan'
                and failed.get('retryable') is False, '并非旧正文输出超限')
            model = Path(failed['model_dir']).resolve()
            require(model.is_relative_to(old_work) and not model.is_relative_to(work), '旧请求目录')
            state = read_json(model / 'request-state.json')
            require(state.get('finish_reason') == 'length'
                and state.get('request_sha256') == failed['request_sha256'] == _body_hash(read_json(model/'request.json'))
                and sha_file(model/'request-state.json') == failed['request_state_sha256']
                and sha_file(model/'response.txt') == state.get('response_sha256') == failed['partial_response_sha256'],
                '截断请求或回答不一致')
            for name in ('request.json','request-state.json','response.txt','response-events.jsonl'):
                require(grant['protected_files'].get(str(model/name)) == sha_file(model/name), '旧请求清单不完整')
        pairs = {(row.get('data',{}).get('component_id'),row.get('data',{}).get('request_id')) for row in records
            if row.get('kind') in {'llm_request_intent','llm_request_started','llm_request_not_sent','llm_request_failed',
                'llm_request_unknown','llm_response_saved','_user_authorized_model_replacement'}
            and row.get('data',{}).get('component_id') in old_ids}
        require(pairs == {(r['component_id'],r['request_id']) for r in grant.get('old_requests',[])}, '遗漏旧正文请求')
        for row in grant.get('old_requests',[]):
            model = Path(row['model_dir']).resolve()
            require(model.is_relative_to(old_work) and not model.is_relative_to(work), '原请求范围')
            for name in ('request.json','request-state.json','response.txt','response-events.jsonl','http-error.txt'):
                path = model/name
                if path.is_file(): require(grant['protected_files'].get(str(path)) == sha_file(path), '旧历史文件未冻结')
    else:
        prior = records[records.index(preprocessing) + 1:]
        require(not old_ids and not any(row.get('kind', '').startswith('llm_') for row in prior),
            '前处理修复原本已有模型请求')
    require(not any(row.get('kind') == 'llm_response_saved'
        and row.get('data', {}).get('component_id') in old_ids for row in records),
        '已有完整正文结果须保留，不能按本入口重新全篇处理')
    expected_components(manifest, identity['task_run_id'])
    return grant


def check_revised_request(records, component_id, request_id, model_dir):
    model = Path(model_dir).resolve()
    if 'body-plan-revisions' not in model.parts:
        return
    manual = next((row for row in records if row.get('kind') == 'manual_retry'
                   and FIELD in row.get('data', {})), None)
    require(manual is not None, '新计划请求缺少明确授权')
    grant = manual['data'][FIELD]
    manifest = read_json(grant['new_manifest']['path'])
    saved = any(row.get('kind') == 'llm_response_saved'
        and row.get('data', {}).get('component_id') == component_id
        and row.get('data', {}).get('request_id') == request_id for row in records)
    validate_revision(records, grant['identity'], manifest, sending=not saved)
    work = Path(grant['new_body_work']).resolve()
    body = {row['component_id']: row for row in expected_components(manifest, grant['identity']['task_run_id'])
        if row['kind'] == 'body'}
    if component_id in body:
        normal = work / 'model'
        if manifest['capacity']['plan_identity']['mode'] == 'partitioned':
            normal = normal / body[component_id]['part_id']
        retry_root = work
    else:
        normal = work / 'whole-paper-ranking' / component_id / 'model'
        require((normal.parent/'ranking-plan.json').is_file(), '全文判断缺少本计划完整输入凭据')
        proof = read_json(normal.parent / 'ranking-plan.json')
        expected = ranking_component(grant['identity'], manifest, proof['level'], proof['number'], proof['final'])
        require(expected == component_id and proof['identity'] == grant['identity']
            and proof['revision'] == grant['retry_id'] and proof['source'] == manifest['reading_material'], '全文判断身份')
        from paperflow_prefect.llm_queue.raw_request import _body_hash
        require(_body_hash(read_json(normal.parent/'planned-request.json')) == proof['request_sha256'], '全文判断请求')
        from .handoff_inputs import _saved_response
        from preextract_scheduler_adapter import request_observation
        require(set(body) <= {row['component_id'] for row in proof['inputs']}, '全文判断缺少本计划正文')
        for row in proof['inputs']:
            require(any(r.get('kind') == 'llm_response_saved' and all(r.get('data',{}).get(k) == row[k]
                for k in ('component_id','request_id','request_sha256','response_sha256','response_path')) for r in records),
                '全文判断输入未完成')
            _saved_response({'model_dir':str(Path(row['response_path']).parent),
                'request_sha256':row['request_sha256'],'response_sha256':row['response_sha256']},request_observation)
        retry_root = normal.parent
    retry = retry_root / 'retries' / digest(component_id) / request_id / 'model'
    require(model in {normal, retry}, '请求目录不属于本计划')
    if model == normal: require(request_id == component_id, '原生新计划请求身份')


def ranking_component(identity, manifest, level, number, final):
    fields = ['whole-paper-specialty-ranking/1', identity['task_id'], identity['group_id'], identity['receipt_id'],
              'final' if final else 'intermediate', level, number]
    if manifest.get(FIELD): fields.extend((manifest[FIELD], manifest['capacity']['plan_identity']['plan_sha256']))
    return digest(fields)


def freeze_ranking(identity, manifest, component, body, inputs, level, number, final):
    from paperflow_prefect.records import write_json
    from paperflow_prefect.llm_queue.raw_request import _body_hash
    root = Path(manifest['body_work']) / 'whole-paper-ranking' / component
    value = dict(schema='paperflow-revised-ranking/1',identity=identity,revision=manifest[FIELD],level=level,
        number=number,final=final,source=manifest['reading_material'],request_sha256=_body_hash(body),
        inputs=[{k:row[k] for k in ('component_id','request_id','request_sha256','response_sha256','response_path')}
                for row in inputs])
    for name, data in (('planned-request.json',body),('ranking-plan.json',value)):
        path=root/name
        if path.exists(): require(read_json(path)==data,'全文判断已冻结计划变化')
        else: write_json(path,data)


def bind_revision_backend(backend, identity, manifest, paper, records):
    grant = validate_revision(records, identity, manifest)
    require(Path(backend.body_work(paper)).resolve() == Path(grant['old_body_work']).resolve(), '原正文工作绑定')
    def body_work(_self, current):
        require(current.get('paper_id') == paper['paper_id'] and current.get('body') == paper['body'], '另一份正文')
        return Path(grant['new_body_work'])
    backend.body_work = MethodType(body_work, backend)
    return backend
