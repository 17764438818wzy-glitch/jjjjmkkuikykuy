"""Durable child requests, using the workbench's existing injected invoke.

The parent is an assembly journal, never a fabricated one-request response.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from preextract_contract import parse_candidate,parse_saved_candidate,validate_candidate
from preextract_images import restore_request_images,_verified_image
from preextract_material import file_sha,write_json
from preextract_partition import digest,assemble_parts,apply_synthesis,_map_part
from preextract_request import build_request,RequestCapacityError

SCHEMA='preextract-partition-run/1'
SYNTHESIS_RULES='''本次仅统合已完成的分段提取，以下任务范围优先于原完整提取规则。
输入是各段原样保存的画像建议、学习索引与原文引用，不是新原稿。详细学习内容已冻结，不得重写或缩短。
只返回 JSON：{"profile":{全篇画像字段},"unit_links":[{"id":"已存在单元ID","upstream_unit_ids":[]}],
"section_links":[{"id":"章节ID","to_section_id":"章节ID","relation":"关系","basis":"依据","source_refs":[]}],
"gaps":[],"redaction_plan":[],"record_kind":"filled_candidate或partial_candidate"}。
profile 使用原 MD 字段，但不输出 learning_units。不得输出 tree、assets、segmentation 或新学习单元。
统一材料保留决定、真实题名、研究主线、迁移边界和最多3个最终专业；保留局部读取限制。
跨段关系只依据实际给出的材料、步骤、结果与引用；不足时记 gaps，不猜测。不得声称本次直接读过未提供图片。
unit_links 只使用输入学习单元编号，不能重复、合并或替换详细单元。局部依赖明确时补充 upstream_unit_ids。
全局生成字段如需脱敏，target_pointer 指向最终 MD 的 /profile/...；不得给未知的详细字段编造指针。
'''


def _request_hash(body):
    import hashlib
    return hashlib.sha256(json.dumps(body,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


def _read(path):return json.loads(Path(path).read_text(encoding='utf8'))


def _saved_reply(directory,expected):
    state_path=directory/'request-state.json'
    if not state_path.exists():return None
    state=_read(state_path)
    if state.get('status')!='response_complete':
        raise RuntimeError('该原稿已有失败或未决模型请求（分段）；已完成分段保留，不自动重发')
    if (state.get('request_sha256')!=expected or _request_hash(_read(directory/'request.json'))!=expected
            or file_sha(directory/'response.txt')!=state.get('response_sha256')):
        raise ValueError('保存的分段请求或响应发生变化')
    return (directory/'response.txt').read_text(encoding='utf8')


def _binding(connection):
    return {k:connection.get(k) for k in ('model','base_url','context_window','max_tokens','max_output_tokens',
        'max_images','max_total_image_bytes','max_single_image_bytes','max_image_side','max_request_bytes',
        'context_reserve_tokens','image_token_upper_bound','image_detail','supports_json_object',
        'max_input_parts','max_partition_requests','capacity_source')}


def _directory(model,state,ident):
    chosen=model/ident
    for recovery in state.get('recoveries',[]):
        if recovery['id']==ident:chosen=model/recovery['directory']
    if not chosen.resolve().is_relative_to((model/ident).resolve()):raise ValueError('分段恢复目录无效')
    return chosen


def create_partition_revision(source,parent,name,part_id,*,reason,recover_failed=False):
    """Explicit operator recovery of one child; the parent's plan stays frozen."""
    import re
    from filelock import FileLock
    if not re_safe_name(part_id) or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}',name) or not reason:
        raise ValueError('分段恢复需要明确编号、名称和原因')
    parent=Path(parent).resolve();model=parent/'model'
    with FileLock(str(parent/'operation.lock'),timeout=0):
        state=_read(model/'partition-state.json');plan=_read(model/'partition-plan.json')
        if (file_sha(model/'partition-plan.json')!=state.get('plan_sha256') or
                digest(_read(parent/'material.json'))!=plan.get('source_material_sha256') or
                file_sha(source)!=_read(parent/'material.json')['source']['source_sha256']):
            raise ValueError('分段恢复来源或计划发生变化')
        if not any(r['id']==part_id for r in state['requests']):raise ValueError('该分段尚无请求，不能创建重试')
        previous=_directory(model,state,part_id);old=_read(previous/'request-state.json')
        ended=(isinstance(old.get('finished_at'),(int,float)) and old['finished_at']>=old.get('started_at',float('inf')))
        allowed=old.get('status')=='partial_response' or (recover_failed and old.get('status')=='request_failed_or_uncertain')
        if not ended or not allowed:raise ValueError('分段尚未结束或已完成，不能自动重复请求')
        if len(state['requests'])+len(state.get('recoveries',[]))>=plan['max_model_requests']:
            raise RequestCapacityError('分段恢复达到原计划总请求预算')
        target=model/part_id/'revisions'/name;target.mkdir(parents=True,exist_ok=False)
        names=('request-state.json','request.json','response.txt','response-events.jsonl','http-error.txt')
        recovery=dict(id=part_id,directory=target.relative_to(model).as_posix(),previous=previous.relative_to(model).as_posix(),
            reason=reason,dispatch_started=False,evidence={n:file_sha(previous/n) if (previous/n).exists() else None for n in names})
        write_json(target/'revision.json',recovery)
        state.setdefault('recoveries',[]).append(recovery);state['status']='parts_pending'
        write_json(model/'partition-state.json',state)
        return parent


def create_plan(plan,original_bundle,adapted_bundle,work,template,connection):
    model=Path(work)/'model';model.mkdir(parents=True,exist_ok=True)
    if (model/'partition-state.json').exists():raise ValueError('已有输入分段记录，不能覆盖')
    parts=[]
    for part in plan['parts']:
        directory=model/part['id'];directory.mkdir(exist_ok=False)
        write_json(directory/'planned-request.json',part['body'])
        write_json(directory/'material.json',part['bundle'])
        parts.append(dict(id=part['id'],ranges=part['ranges'],asset_ids=part['asset_ids'],capacity=part['capacity'],
            request_sha256=_request_hash(part['body']),material_sha256=file_sha(directory/'material.json')))
    write_json(model/'partition-material.json',adapted_bundle)
    stored=dict(schema=SCHEMA,source_material_sha256=digest(original_bundle),template_sha256=digest(template),
        material_sha256=file_sha(model/'partition-material.json'),binding=_binding(connection),parts=parts,
        coverage=plan['coverage'],max_model_requests=int(connection.get('max_partition_requests',128)))
    write_json(model/'partition-plan.json',stored)
    state=dict(schema=SCHEMA,status='parts_pending',plan_sha256=file_sha(model/'partition-plan.json'),requests=[],
               aggregate_client_http_requests=0)
    write_json(model/'partition-state.json',state)


def run_partition(original_bundle,work,template,catalog,connection,invoke,*,stop=None,progress=lambda s:None,before_model=None):
    model=Path(work)/'model';state_path=model/'partition-state.json'
    state=_read(state_path);plan=_read(model/'partition-plan.json')
    if (state.get('schema')!=SCHEMA or state.get('plan_sha256')!=file_sha(model/'partition-plan.json')
            or plan.get('source_material_sha256')!=digest(original_bundle) or plan.get('template_sha256')!=digest(template)
            or file_sha(model/'partition-material.json')!=plan.get('material_sha256')):
        raise ValueError('已有输入分段计划、原文或模板发生变化')
    bundle=_read(model/'partition-material.json')
    for recovery in state.get('recoveries',[]):
        if not re_safe_name(recovery.get('id')):raise ValueError('分段恢复记录无效')
        previous=(model/recovery['previous']).resolve()
        if not previous.is_relative_to((model/recovery['id']).resolve()):raise ValueError('分段恢复目录无效')
        for name,expected in recovery['evidence'].items():
            path=previous/name
            if (file_sha(path) if path.is_file() else None)!=expected:raise ValueError('分段恢复原始证据发生变化')
    for asset in original_bundle.get('assets',[]):
        if asset.get('image_path'):_verified_image(asset)
    for part in plan['parts']:
        directory=model/part['id']
        if (file_sha(directory/'material.json')!=part['material_sha256']
                or _request_hash(_read(directory/'planned-request.json'))!=part['request_sha256']):
            raise ValueError('已有分段材料或计划请求发生变化')
        restore_request_images(_read(directory/'material.json'),directory,_read(directory/'planned-request.json'))
    # Check every real request before returning an already assembled candidate.
    for row in state['requests']:
        if not re_safe_name(row['id']):raise ValueError('分段请求目录无效')
        directory=_directory(model,state,row['id'])
        reply=_saved_reply(directory,row['request_sha256'])
        recovery=next((r for r in reversed(state.get('recoveries',[])) if r['id']==row['id']),None)
        fresh_recovery=(recovery is not None and recovery.get('dispatch_started') is False and
            not any((directory/n).exists() for n in ('request.json','response.txt','response-events.jsonl','http-error.txt')))
        if reply is None and (state['status']=='assembled' or not fresh_recovery):
            raise ValueError('已登记分段请求证据缺失或发生变化；不能将未决请求当成未发送')
    if state['status']=='assembled':
        if (not {p['id'] for p in plan['parts']} <= {r['id'] for r in state['requests']}
                or digest(state['requests'])!=state.get('request_index_sha256')):
            raise ValueError('已完成分段请求目录发生变化')
        path=model/'assembled-candidate.json'
        if file_sha(path)!=state.get('candidate_sha256'):raise ValueError('已装配学习内容发生变化')
        return path.read_text(encoding='utf8'),file_sha(path),bundle

    # A fully assembled cache returns above without reserving a model slot.
    # Unfinished authorized plans reserve before creating any new requests.
    if before_model:before_model()
    active_connection=None
    def get_connection():
        nonlocal active_connection
        if active_connection is None:
            active_connection=connection() if callable(connection) else connection
            if _binding(active_connection)!=plan['binding']:raise ValueError('输入分段模型或容量配置发生变化，旧计划保留')
        return active_connection

    def update_usage():
        usage={};calls=0;unknown=[]
        for row in state['requests']:
            attempts=[model/row['id']]+[model/r['directory'] for r in state.get('recoveries',[]) if r['id']==row['id']]
            for directory in attempts:
                path=directory/'request-state.json'
                if not path.exists():continue
                child=_read(path);calls+=child.get('client_http_requests',0)
                observed=child.get('usage') or {}
                if not observed:unknown.append(directory.relative_to(model).as_posix())
                for key in ('prompt_tokens','completion_tokens','total_tokens'):
                    if isinstance(observed.get(key),int):usage[key]=usage.get(key,0)+observed[key]
        state.update(aggregate_client_http_requests=calls,aggregate_usage=usage,missing_usage_requests=unknown)
        write_json(state_path,state)

    def call(ident,body):
        if stop is not None and stop.is_set():raise InterruptedError('分段读取已停止，完成部分保留')
        directory=_directory(model,state,ident);expected=_request_hash(body)
        row=next((r for r in state['requests'] if r['id']==ident),None)
        if row and row['request_sha256']!=expected:raise ValueError('已登记分段请求发生变化')
        saved=_saved_reply(directory,expected)
        if saved is not None:return parse_saved_candidate(saved)
        active=get_connection()
        if row is None:
            if len(state['requests'])+len(state.get('recoveries',[]))>=plan['max_model_requests']:raise RequestCapacityError('分段及全文统合达到总请求预算，已有结果保留')
            state['requests'].append(dict(id=ident,request_sha256=expected));update_usage()
        recovery=next((r for r in reversed(state.get('recoveries',[])) if r['id']==ident),None)
        if recovery is not None:
            # Seal dispatch before entering the old transport. A later lost
            # child intent cannot make this recovery eligible a second time.
            recovery['dispatch_started']=True;update_usage()
        try:
            text=invoke(body,active,directory,stop=stop,progress=progress)
            return parse_saved_candidate(text)
        finally:update_usage()

    candidates=[];subsets=[]
    for part in plan['parts']:
        progress('extracting_parts')
        directory=model/part['id'];subset=_read(directory/'material.json')
        candidate=call(part['id'],_read(directory/'planned-request.json'))
        validation=validate_candidate(candidate,subset,catalog,redaction_policy='best_effort',admission_policy='learning',
                                      partition_material=bundle)
        write_json(directory/'validation.json',validation)
        # Whole-paper requirements are checked only after assembly. Unsafe
        # containers/identity changes cannot be repaired by concatenating parts.
        fatal={'field_type','unknown_field','source_mismatch','source_identity','material_identity','source_reference'}
        if any(e['code'] in fatal for e in validation['errors']):raise ValueError('分段结构或来源校验失败，原响应及其他分段保留')
        candidates.append(validation['candidate']);subsets.append(subset)
    progress('assembling_parts')
    assembled,audit=assemble_parts(candidates,subsets,bundle)
    write_json(model/'parts-candidate.json',assembled);write_json(model/'assembly-audit.json',audit)
    state['status']='synthesis_pending';update_usage()
    records=[]
    for number,(candidate,subset) in enumerate(zip(candidates,subsets),1):
        mapped=_map_part(candidate,subset,f'P{number:03d}_');profile=mapped['profile']
        index=[{k:copy.deepcopy(u.get(k)) for k in ('id','title','question','method','materials','steps','outputs',
                 'interpretation','conditions','limitations','section_ids','source_refs','upstream_unit_ids')}
               for u in profile['learning_units']]
        # A chapter's extracted index can itself exceed one request. Individual
        # existing learning IDs, not an entire chapter, are the atomic records.
        proposal={k:v for k,v in profile.items() if k!='learning_units'}
        records.extend(dict(profile=proposal,unit_index=[unit]) for unit in index)
        if not index:records.append(dict(profile=proposal,unit_index=[]))

    def summary_body(group):
        ids={sid for r in group for u in r.get('unit_index',[]) for sid in (u.get('section_ids') or [])}
        material=dict(source=bundle['source'],blocks=[],assets=[],sections=[],issues=[],attachments=[],
            reading_context=dict(synthesis_input=group,outline=[{k:s.get(k) for k in ('id','title','parent_id')}
                          for s in bundle.get('sections',[]) if s['id'] in ids]))
        # Reading diagnostics were supplied to the extraction parts; they are
        # not repeated hundreds of times in a derived relation-only request.
        material['source']=copy.deepcopy(material['source']);material['source']['reading_scope']={}
        return build_request(material,template,catalog,get_connection(),instruction=SYNTHESIS_RULES,enforce=False)

    generation=0
    while True:
        generation+=1
        body,capacity=summary_body(records)
        if not capacity['violations']:
            progress('reconciling_parts')
            global_result=call(f'global-{generation:02d}-001',body)
            assembled=apply_synthesis(assembled,global_result);break
        groups=[];current=[]
        for record in records:
            _,cap=summary_body(current+[record])
            if cap['violations']:
                if not current:raise RequestCapacityError('不可再分的全文关系记录超过容量，详细学习内容已保存')
                groups.append(current);current=[]
                _,cap=summary_body([record])
                if cap['violations']:raise RequestCapacityError('不可再分的全文关系记录超过容量，详细学习内容已保存')
            current.append(record)
        if current:groups.append(current)
        next_records=[]
        for i,group in enumerate(groups,1):
            progress('reconciling_parts')
            result=call(f'global-{generation:02d}-{i:03d}',summary_body(group)[0])
            assembled=apply_synthesis(assembled,{k:v for k,v in result.items() if k!='profile'})
            next_records.append(dict(profile=result.get('profile',{}),unit_index=[
                {k:u.get(k) for k in ('id','title','section_ids')} for r in group for u in r.get('unit_index',[])]))
        if len(json.dumps(next_records))>=len(json.dumps(records)) or generation>=8:
            raise RequestCapacityError('全文关系索引仍超容量，达到有限统合边界，完整学习内容保留')
        records=next_records
    path=model/'assembled-candidate.json';write_json(path,assembled)
    state.update(status='assembled',candidate_sha256=file_sha(path),learning_units=len(assembled['profile']['learning_units']),
                 request_index_sha256=digest(state['requests']))
    update_usage()
    return path.read_text(encoding='utf8'),file_sha(path),bundle


def re_safe_name(name):
    import re
    return isinstance(name,str) and re.fullmatch(r'(part-\d{3}|global-\d{2}-\d{3})',name)
