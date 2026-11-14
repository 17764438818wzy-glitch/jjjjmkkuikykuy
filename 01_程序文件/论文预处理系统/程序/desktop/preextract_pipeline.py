"""Per-paper extraction stages called by the existing workbench batch runner."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import time
from pathlib import Path
from types import SimpleNamespace

from preextract_material import PARSER_VERSION, file_sha, prepare_material, write_json
from preextract_contract import load_template, parse_candidate, validate_candidate
from preextract_request import build_request, request_once, plan_requests
from preextract_images import image_limits, prepare_request_images, restore_request_images

ROOT=Path(__file__).resolve().parent
from project_paths import DATA_DIR, RUNTIME_DIR, TEMPLATE_PATH, CORE_ROOT, ROUTE_CONFIG
DEFAULT_RUNTIME=RUNTIME_DIR
DEFAULT_TEMPLATE=TEMPLATE_PATH
DEFAULT_PAPERFLOW=CORE_ROOT


_REVISION_REQUIRED={'material.json','model/request-state.json','model/request.json'}
_REVISION_OPTIONAL={'model/response.txt','model/response-events.jsonl','model/http-error.txt'}


def _ended_failure(state):
    started=state.get('started_at');finished=state.get('finished_at')
    return (state.get('status')=='request_failed_or_uncertain'
            and isinstance(started,(int,float)) and isinstance(finished,(int,float))
            and finished>=started>0)


def _revision(source, work):
    path=Path(work)/'revision.json'
    if not path.exists():return None
    meta=json.loads(path.read_text(encoding='utf8'));parent=Path(meta.get('parent','')).resolve()
    if (meta.get('schema')!='preextract-authorized-revision/1' or meta.get('mode') not in {'rerun','candidate_import'}
            or not meta.get('reason') or Path(work).resolve().parent!=parent/'revisions'
            or meta.get('source_sha256')!=file_sha(source)):
        raise ValueError('revision identity or explicit purpose is invalid')
    recovery=meta.get('recover_failed') is True
    expected=_REVISION_REQUIRED|(_REVISION_OPTIONAL if recovery else {'model/response.txt'})
    if set(meta.get('parent_hashes',{}))!=expected:
        raise ValueError('revision parent evidence is incomplete')
    for name,sha in meta['parent_hashes'].items():
        path=parent/name
        if sha is None:
            if not recovery or name not in _REVISION_OPTIONAL or path.exists():
                raise ValueError('revision parent evidence changed')
        elif not path.is_file() or file_sha(path)!=sha:
            raise ValueError('revision parent evidence changed')
    bundle=json.loads((parent/'material.json').read_text(encoding='utf8'))
    state=json.loads((parent/'model/request-state.json').read_text(encoding='utf8'))
    allowed=_ended_failure(state) if recovery else state.get('status') in {'partial_response','response_complete'}
    if bundle['source']['source_sha256']!=meta['source_sha256'] or not allowed:
        raise ValueError('revision cannot retry an unresolved or different source request')
    if not recovery and file_sha(parent/'model/response.txt')!=state.get('response_sha256'):
        raise ValueError('revision parent response identity is invalid')
    expected_material=meta['parent_hashes']['material.json']
    if meta.get('refresh_material') is True:
        if meta['mode']!='rerun' or not meta.get('refreshed_material_sha256'):
            raise ValueError('material refresh requires an explicitly bound rerun')
        expected_material=meta['refreshed_material_sha256']
    if file_sha(Path(work)/'material.json')!=expected_material:
        raise ValueError('revision must reuse the complete registered material')
    if meta.get('refresh_material') is True:
        refreshed=json.loads((Path(work)/'material.json').read_text(encoding='utf8'))
        if any(refreshed['source'].get(k)!=bundle['source'].get(k) for k in ('source_sha256','catalog_sha256')):
            raise ValueError('refreshed material changed the original source or catalog')
    return meta


def create_revision(source, parent, name, *, mode, reason, request_context=None, recover_failed=False, part_id=None,
                    refresh_material=False, catalog_path=None, max_pages=160):
    """Explicit operator recovery; never called by ordinary start/resume/retry."""
    if refresh_material and (mode!='rerun' or part_id is not None or catalog_path is None):
        raise ValueError('material refresh requires an explicit full rerun and its original catalog')
    if part_id is not None:
        if mode!='rerun' or request_context is not None:raise ValueError('分段恢复只重做指定失败请求，不改变冻结的输入计划')
        from preextract_partition_run import create_partition_revision
        return create_partition_revision(source,parent,name,part_id,reason=reason,recover_failed=recover_failed)
    parent=Path(parent).resolve()
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}',name) or mode not in {'rerun','candidate_import'} or not reason:
        raise ValueError('revision needs an explicit name, supported mode and reason')
    from filelock import FileLock
    with FileLock(str(parent/'operation.lock'),timeout=0):
        state=json.loads((parent/'model/request-state.json').read_text(encoding='utf8'))
        bundle=json.loads((parent/'material.json').read_text(encoding='utf8'))
        allowed=_ended_failure(state) if recover_failed is True else state.get('status') in {'partial_response','response_complete'}
        if not allowed or bundle['source']['source_sha256']!=file_sha(source):
            raise ValueError('revision cannot retry an unresolved or different source request')
        if refresh_material and file_sha(catalog_path)!=bundle['source']['catalog_sha256']:
            raise ValueError('material refresh must use the original catalog')
        names=_REVISION_REQUIRED|(_REVISION_OPTIONAL if recover_failed is True else {'model/response.txt'})
        hashes={p:file_sha(parent/p) if (parent/p).is_file() else None for p in sorted(names)}
        if any(hashes[p] is None for p in _REVISION_REQUIRED) or (mode=='candidate_import' and not hashes.get('model/response.txt')):
            raise ValueError('revision parent evidence is incomplete')
        meta=dict(schema='preextract-authorized-revision/1',parent=str(parent),mode=mode,reason=reason,
                  source_sha256=file_sha(source),parent_hashes=hashes,request_context=request_context)
        if recover_failed is True:meta['recover_failed']=True
        work=parent/'revisions'/name
        work.mkdir(parents=True,exist_ok=False)
        if refresh_material:
            prepare_material(source,work,catalog_path,max_pages=max_pages)
            meta.update(refresh_material=True,refreshed_material_sha256=file_sha(work/'material.json'))
        else:
            shutil.copyfile(parent/'material.json',work/'material.json')
        write_json(work/'revision.json',meta)
        _revision(source,work)
        return work


def save_candidate_import(work, candidate, *, evidence):
    """Keep imported corrections separate from actual model response journals."""
    work=Path(work);meta=json.loads((work/'revision.json').read_text(encoding='utf8'))
    if meta.get('mode')!='candidate_import' or not isinstance(candidate,dict):raise ValueError('candidate import mode required')
    parent=Path(meta['parent']).resolve()
    paths={Path(p).resolve() for p in evidence}|{parent/'model/response.txt'}
    if any(not p.is_relative_to(parent) for p in paths):raise ValueError('candidate import evidence outside its source history')
    record=dict(schema='preextract-candidate-import/1',candidate=candidate,
                evidence={str(p):file_sha(p) for p in sorted(paths)})
    path=work/'candidate-import.json'
    if path.exists():raise ValueError('candidate import already exists; keep prior correction')
    write_json(path,record)


def _import_candidate(work, revision):
    path=Path(work)/'candidate-import.json'
    if not path.is_file():raise ValueError('candidate import has not been supplied')
    record=json.loads(path.read_text(encoding='utf8'));proof=record.get('evidence',{})
    parent=Path(revision['parent']).resolve()
    if (record.get('schema')!='preextract-candidate-import/1' or not isinstance(record.get('candidate'),dict)
            or str(parent/'model/response.txt') not in proof or not proof
            or any(not Path(p).resolve().is_relative_to(parent) or not Path(p).is_file() or file_sha(p)!=sha for p,sha in proof.items())):
        raise ValueError('candidate import provenance is invalid or changed')
    return json.dumps(record['candidate'],ensure_ascii=False),file_sha(path),proof


def _active_workspace(source,parent):
    """Follow an explicitly activated recovery without replacing old identity."""
    parent=Path(parent).resolve();path=parent/'active-revision.json'
    if not path.exists():return parent
    active=json.loads(path.read_text(encoding='utf8'));work=Path(active.get('work_dir','')).resolve()
    if (active.get('schema')!='preextract-active-revision/1' or work.parent!=parent/'revisions'
            or active.get('source_sha256')!=file_sha(source)
            or not (work/'revision.json').is_file()
            or file_sha(work/'revision.json')!=active.get('revision_sha256')):
        raise ValueError('active revision identity is invalid; original history retained')
    revision=_revision(source,work)
    if Path(revision['parent']).resolve()!=parent:
        raise ValueError('active revision belongs to another source history')
    return work


def activate_revision(source,work):
    """An actually verified delivery may become the original entry's cache."""
    work=Path(work).resolve();revision=_revision(source,work)
    if not revision:return
    from filelock import FileLock
    from preextract_publish import _paperflow
    from preextract_handoff import _receipt_complete
    parent=Path(revision['parent'])
    with FileLock(str(parent/'operation.lock'),timeout=0):
        receipt_path=work/'delivery-receipt.json';prepared_path=work/'private/prepared-publication.json'
        if not receipt_path.is_file() or not prepared_path.is_file():
            raise ValueError('active revision requires verified delivery')
        prepared=json.loads(prepared_path.read_text(encoding='utf8'))
        receipt=json.loads(receipt_path.read_text(encoding='utf8'))
        validation=json.loads((work/'validation.json').read_text(encoding='utf8'))
        packet=_paperflow(prepared['paperflow_root']).validate_package(prepared['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
        if (not validation.get('valid') or not _receipt_complete(receipt,packet)
                or receipt.get('task')!=prepared['task']
                or receipt.get('manifest_sha256')!=packet['manifest_sha256']
                or prepared['validation'].get('manifest_sha256')!=packet['manifest_sha256']
                or prepared.get('candidate_sha256')!=file_sha(work/'private/candidate.json')):
            raise ValueError('active revision requires verified delivery and unchanged candidate')
        if revision['mode']=='candidate_import':_import_candidate(work,revision)
        active=dict(schema='preextract-active-revision/1',work_dir=str(work),
            source_sha256=revision['source_sha256'],revision_sha256=file_sha(work/'revision.json'),
            candidate_sha256=prepared['candidate_sha256'],manifest_sha256=packet['manifest_sha256'])
        path=parent/'active-revision.json'
        if path.exists() and json.loads(path.read_text(encoding='utf8'))!=active:
            from time import time_ns
            history=parent/'private/revision-activation-history';history.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(path,history/(str(time_ns())+'.json'))
        write_json(path,active)


def default_work_dir(source, template=DEFAULT_TEMPLATE):
    from filelock import FileLock
    source_sha=file_sha(source)
    identity=source_sha+'|'+file_sha(template)+'|'+PARSER_VERSION
    root=DATA_DIR/'single-request'
    root.mkdir(parents=True,exist_ok=True)
    registry=root/'source-index.json'
    # Windows cannot reliably open/replace the registry concurrently. Reuse
    # its writer lock for this short read; never hold it during paper work.
    with FileLock(str(registry)+'.lock',timeout=5):
        records=json.loads(registry.read_text(encoding='utf-8')) if registry.exists() else {}
    if identity in records:return _active_workspace(source,records[identity])
    for key,path in records.items():
        if key.split('|')[0]==source_sha and _has_model_history(path):return _active_workspace(source,path)
    return root/(source_sha[:16]+'-'+hashlib.sha256(identity.encode()).hexdigest()[:12])


def _has_model_history(path):
    return any((Path(path)/'model'/name).exists() for name in ('request-state.json','partition-state.json'))


def register_workspace(source, template, work):
    from filelock import FileLock
    registry=DATA_DIR/'single-request'/'source-index.json'
    registry.parent.mkdir(parents=True,exist_ok=True)
    with FileLock(str(registry)+'.lock',timeout=5):
        records=json.loads(registry.read_text(encoding='utf-8')) if registry.exists() else {}
        source_sha=file_sha(source)
        identity=source_sha+'|'+file_sha(template)+'|'+PARSER_VERSION
        revision=_revision(source,work)
        if revision:
            if not any(k.split('|')[0]==revision['source_sha256'] and Path(p).resolve()==Path(revision['parent']) for k,p in records.items()):
                raise ValueError('revision parent is not the registered source workspace')
            return  # Keep the original cache identity and every original receipt.
        for key,path in records.items():
            if key.split('|')[0]==source_sha and Path(path).resolve()!=Path(work).resolve() and _has_model_history(path):
                raise RuntimeError('该原稿已有模型请求记录；材料代码升级不会自动重发模型请求')
        old=records.get(identity)
        if old and Path(old).resolve()!=Path(work).resolve():
            if _has_model_history(old):raise RuntimeError('该原稿/模板版本已有模型请求，复用登记目录；不自动另开请求')
        records[identity]=str(Path(work).resolve());write_json(registry,records)


def load_runtime(runtime_dir=DEFAULT_RUNTIME):
    return json.loads((Path(runtime_dir)/'config.json').read_text(encoding='utf-8'))


def load_model_connection(runtime_dir=DEFAULT_RUNTIME):
    import yaml
    runtime_dir=Path(runtime_dir);runtime=load_runtime(runtime_dir)
    route=ROUTE_CONFIG
    settings=yaml.safe_load(route.read_text(encoding='utf-8'))
    providers=next(r['config']['providers'] for r in settings if 'providers' in r.get('config',{}))
    provider=providers[runtime['provider']]
    model=runtime['model']
    entry=next(r for r in provider['models'] if r['id']==model)
    if 'image' not in entry.get('input',[]):raise ValueError('当前模型路由未声明图像输入，未发送请求')
    credentials=Path.home()/'.dsh'/'.credentials.yaml'
    key=yaml.safe_load(credentials.read_text(encoding='utf-8'))['refs'][provider['apiKeyEnv']]
    # Read the existing provider catalog. Unknown capacity is never guessed.
    cache=json.loads((Path.home()/'.dsh'/'.workbuddy-ai-catalog.json').read_text(encoding='utf-8'))
    def find(value):
        if isinstance(value,dict):
            if model in (value.get('id'),value.get('model'),value.get('name')) and 'maxTokens' in value:return value
            if model in value and isinstance(value[model],dict) and 'maxTokens' in value[model]:return value[model]
            for v in value.values():
                found=find(v)
                if found:return found
        elif isinstance(value,list):
            for v in value:
                found=find(v)
                if found:return found
        return None
    limits=find(cache)
    if not limits or not limits.get('supportsImages'):raise ValueError('缺少当前模型图像与容量记录，未发送请求')
    windows=limits.get('supportedContextWindows') or [limits.get('contextWindow')]
    context=max(int(v) for v in windows if v)
    return dict(base_url=provider['baseURL'],api_key=key,model=model,timeout=runtime.get('model_timeout',1800),
                context_window=context,max_tokens=min(49152,int(limits['maxTokens'])),max_output_tokens=int(limits['maxTokens']),
                max_total_image_bytes=provider.get('maxRequestImageBytes',2*1024*1024),
                capacity_source='existing provider catalog '+str(Path.home()/'.dsh'/'.workbuddy-ai-catalog.json'))


def offline_counter(runtime, paperflow_root):
    import sys
    if str(paperflow_root) not in sys.path:sys.path.insert(0,str(paperflow_root))
    from paperflow.embedding_counter import OfflineCounter
    admission=runtime['embedding_admission'];policy=admission['policy']
    counter=OfflineCounter(**admission['counter'],unified=True,**{k:policy[k] for k in ('model_sha256','libllama_sha256','cl100k_sha256')})
    counter.model_budget=runtime.get('embedding_model_budget',2000)
    counter.cl_limit=min(policy['pipeline_limit'],policy['adapter_limit'])
    return counter


def run_single(source, *, work_dir=None, template_path=DEFAULT_TEMPLATE, runtime_dir=DEFAULT_RUNTIME,
               paperflow_root=DEFAULT_PAPERFLOW, stage='package', stop=None, progress=lambda s:None,
               connection=None, invoke=None, max_pages=160, before_postprocess=None, before_model=None,
               preparation_context=None, associated_document=None, associated_records=None):
    source=Path(source);template_path=Path(template_path);runtime_dir=Path(runtime_dir)
    from preextract_contract import is_preextract_artifact
    if is_preextract_artifact(source):
        raise ValueError('预提取中间产物不能作为新论文重复处理；请使用原稿的已有工作记录续跑')
    work=Path(work_dir) if work_dir else default_work_dir(source,template_path)
    work.mkdir(parents=True,exist_ok=True)
    register_workspace(source,template_path,work)
    from filelock import FileLock
    with FileLock(str(work/'operation.lock'),timeout=0):
        runtime=load_runtime(runtime_dir)
        catalog_path=Path(runtime.get('catalog_path') or runtime_dir/'catalog.json')
        catalog=json.loads(catalog_path.read_text(encoding='utf-8'))
        state=dict(schema='paper-preextract-run/1.0',stage='registered',work_dir=str(work),source_sha256=file_sha(source),completed=False,
                   stage_timings=[])
        stage_clock=[time.monotonic()]
        def advance(name,**values):
            now=time.monotonic()
            if state['stage_timings']:
                state['stage_timings'][-1].update(finished_at=time.time(),elapsed_seconds=round(now-stage_clock[0],6))
            state['stage_timings'].append(dict(stage=name,started_at=time.time()))
            stage_clock[0]=now
            state.update(stage=name,**values);write_json(work/'status.json',state);progress(name)
        try:
            advance('preparing')
            from contextlib import nullcontext
            with preparation_context() if preparation_context is not None else nullcontext():
                bundle=prepare_material(source,work,catalog_path,stop=stop,max_pages=max_pages,
                    associated_document=associated_document,associated_records=associated_records)
            template,_=load_template(template_path)
            model_dir=work/'model';saved=model_dir/'request-state.json'
            revision=_revision(source,work);origin='model_response';proof={}
            registered_bundle=bundle;image_report=None
            # Every new paper passes the same pure-program image preparation,
            # including "prepare only" with no model credentials/configuration.
            # Saved requests retain their exact historical image binding.
            if not _has_model_history(work) and not (revision and revision['mode']=='candidate_import'):
                advance('adapting_images')
                bundle,image_report=prepare_request_images(bundle,work,{},stop=stop)
                state.update(image_count=image_report['image_count'],adjusted_image_count=image_report['adjusted_count'])
            advance('material_prepared')
            if stage=='prepare':return state
            if stop is not None and stop.is_set():raise InterruptedError('已停止')
            if revision and revision['mode']=='candidate_import':
                response,response_hash,proof=_import_candidate(work,revision)
                parent=Path(revision['parent'])
                bundle=restore_request_images(bundle,parent,json.loads((parent/'model/request.json').read_text(encoding='utf8')))
                origin='candidate_import';advance('candidate_imported')
            elif (model_dir/'partition-state.json').exists():
                from preextract_partition_run import run_partition
                advance('reusing_saved_response')
                response,response_hash,bundle=run_partition(registered_bundle,work,template,catalog,
                    connection or (lambda:load_model_connection(runtime_dir)),invoke or request_once,
                    stop=stop,progress=advance,before_model=before_model)
                origin='partition_assembly'
            elif saved.exists():
                previous=json.loads(saved.read_text(encoding='utf-8'))
                if previous.get('status')!='response_complete':
                    advance('saved_model_request_incomplete')
                    raise RuntimeError('该原稿已有失败或未决模型请求，不自动重发')
                if file_sha(model_dir/'response.txt')!=previous.get('response_sha256'):raise ValueError('保存的模型响应发生变化')
                raw_request=(model_dir/'request.json').read_bytes()
                prior_request=json.loads(raw_request)
                digest=hashlib.sha256(json.dumps(prior_request,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
                if digest!=previous.get('request_sha256'):raise ValueError('请求记录发生变化')
                if not prior_request['messages'][0]['content'].endswith(template):raise ValueError('已有结果对应不同MD模板')
                prior_material=json.loads(prior_request['messages'][1]['content'][0]['text'])['material']['source']
                if any(prior_material.get(k)!=bundle['source'].get(k) for k in ('source_sha256','text_sha256','index_sha256','catalog_sha256')):
                    raise ValueError('已有结果对应不同材料版本')
                prior_inventory=json.loads(prior_request['messages'][1]['content'][0]['text'])['material'].get('associated_materials_sha256')
                if prior_inventory!=bundle.get('associated_materials_sha256'):
                    raise ValueError('已有结果对应不同关联资料版本；原请求保留')
                bundle=restore_request_images(bundle,work,prior_request)
                advance('reusing_saved_response')
                response=(model_dir/'response.txt').read_text(encoding='utf-8')
                response_hash=file_sha(model_dir/'response.txt')
            else:
                # Material and image adaptation are already durable. Wait before
                # constructing base64 requests, without occupying preparation.
                if before_model:before_model()
                connection=connection() if callable(connection) else connection or load_model_connection(runtime_dir)
                if image_limits(connection)!=(image_report['max_side'],image_report['max_bytes']):
                    advance('adapting_images')
                    bundle,image_report=prepare_request_images(registered_bundle,work,connection,stop=stop)
                advance('checking_request',image_count=image_report['image_count'],adjusted_image_count=image_report['adjusted_count'])
                context=revision.get('request_context') if revision else None
                advance('planning_input')
                plan=plan_requests(bundle,template,catalog,connection,request_context=context)
                write_json(work/'input-plan.json',dict(schema=plan['schema'],mode=plan['mode'],coverage=plan['coverage'],
                    part_count=len(plan['parts']),capacities=[p['capacity'] for p in plan['parts']]))
                if plan['mode']=='single':
                    body,capacity=plan['parts'][0]['body'],plan['parts'][0]['capacity']
                    write_json(work/'capacity.json',capacity)
                    advance('extracting_once')
                    response=(invoke or request_once)(body,connection,model_dir,stop=stop,progress=progress)
                    response_hash=file_sha(model_dir/'response.txt')
                else:
                    from preextract_partition_run import create_plan,run_partition
                    write_json(work/'capacity.json',dict(mode='partitioned',original=plan['original_capacity'],
                        parts=[p['capacity'] for p in plan['parts']]))
                    create_plan(plan,registered_bundle,bundle,work,template,connection)
                    response,response_hash,bundle=run_partition(registered_bundle,work,template,catalog,connection,
                        invoke or request_once,stop=stop,progress=advance)
                    origin='partition_assembly'
            # The complete reply/import/partition assembly is durable now. The
            # original batch runner may refill model admission before local
            # candidate parsing, without starting this paper a second time.
            advance('response_saved',response_sha256=response_hash)
            if before_postprocess:before_postprocess()
            if stop is not None and stop.is_set():raise InterruptedError('已停止，完整模型回答已保留')
            advance('candidate_received')
            syntax_repairs=[]
            from preextract_contract import parse_saved_candidate
            candidate=parse_saved_candidate(response,repairs=syntax_repairs)
            write_json(work/'normalization.json',dict(syntax_repairs=syntax_repairs,
                response_sha256=response_hash,candidate_origin=origin,import_evidence=proof))
            write_json(work/'private'/'candidate-raw.json',candidate)
            from associated_materials import add_inventory_decisions
            candidate=add_inventory_decisions(candidate,bundle.get('attachments',[]))
            advance('validating_candidate')
            validation=validate_candidate(candidate,bundle,catalog,redaction_policy='best_effort',admission_policy='learning')
            if revision and revision['mode']=='rerun' and revision.get('request_context'):
                preserved=revision['request_context']['content']
                changed=[key for key,value in preserved.items() if candidate.get(key)!=value]
                write_json(work/'preservation-comparison.json',dict(unchanged=not changed,changed_fields=changed,
                    original_fields_sha256=hashlib.sha256(json.dumps(preserved,sort_keys=True,ensure_ascii=False).encode()).hexdigest()))
                if changed:
                    validation['valid']=False
                    validation['errors'].extend(dict(code='preserved_content_changed',path='/'+key,
                        message='new reply changed completed prior content; both versions retained for reconciliation') for key in changed)
            write_json(work/'normalization.json',dict(syntax_repairs=syntax_repairs,
                compatibility_report=validation.get('compatibility_report',[]),
                response_sha256=response_hash,candidate_origin=origin,import_evidence=proof))
            write_json(work/'validation.json',validation)
            candidate=validation['candidate'];write_json(work/'private'/'candidate.json',candidate)
            (work/'private'/'论文预提取.md').write_text('# 论文预提取候选\n\n<!-- paper-preextract:data:start -->\n```json\n'+json.dumps(candidate,ensure_ascii=False,indent=2)+'\n```\n<!-- paper-preextract:data:end -->\n',encoding='utf-8')
            if not validation['valid']:
                advance('candidate_requires_attention',errors=validation['errors'],warnings=validation['warnings']);return state
            if (candidate['profile']['material_disposition']['decision']!='keep'
                    and not (validation.get('admission',{}).get('gate_policy')=='three-required-checks/1'
                             and validation['admission'].get('usable_unit_ids'))):
                advance('candidate_excluded',decision=candidate['profile']['material_disposition']);return state
            advance('candidate_validated')
            if stage=='extract':return state
            if stop is not None and stop.is_set():raise InterruptedError('已停止，候选保留')
            from preextract_publish import prepare_publication,deliver_publication,PUBLICATION_VERSION
            advance('redacting_and_packaging')
            prepared_path=work/'private'/'prepared-publication.json'
            prepared=None
            reuse_sealed=None
            if prepared_path.exists():
                cached=json.loads(prepared_path.read_text(encoding='utf-8'))
                if cached['candidate_sha256']!=file_sha(work/'private'/'candidate.json'):raise ValueError('候选发生变化，保留原封包待核对')
                import sys
                if str(paperflow_root) not in sys.path:sys.path.insert(0,str(paperflow_root))
                from paperflow.package import validate_package
                packet=validate_package(cached['package_path'],cached['task'],catalog,strict_seal=True)
                if packet['manifest_sha256']!=cached['validation']['manifest_sha256']:raise ValueError('已有封包发生变化')
                cached['catalog']=catalog
                from preextract_delivery_queue import publication_registered
                report=cached.get('validation',{})
                if (any((work/name).exists() for name in ('delivery-binding.json','delivery-receipt.json'))
                        or publication_registered(work,cached,roots=(DATA_DIR,))):
                    # A registered, in-flight or completed delivery owns this
                    # task and manifest through every packaging-policy upgrade.
                    prepared=cached
                elif (report.get('publication_version')!=PUBLICATION_VERSION
                        or report.get('admission',{}).get('policy')!='learning'
                        or report.get('redaction',{}).get('policy')!='best_effort'):
                    # Reuse sealed output when only organization changes; other
                    # policy upgrades still reuse the saved immutable reply.
                    from time import time_ns
                    history=work/'private'/'publication-history';history.mkdir(exist_ok=True)
                    (history/(str(time_ns())+'.json')).write_bytes(prepared_path.read_bytes())
                    if (report.get('admission',{}).get('policy')=='learning'
                            and report.get('redaction',{}).get('policy')=='best_effort'):
                        reuse_sealed=cached
                else:
                    prepared=cached
            if prepared is None:
                publication_path=work/'publication'
                if publication_path.exists():
                    from uuid import uuid4
                    publication_path=work/('publication-'+uuid4().hex)
                counter=offline_counter(runtime,paperflow_root)
                try:
                    if reuse_sealed is not None:
                        from preextract_reorganize import consolidate_publication
                        prepared=consolidate_publication(reuse_sealed,publication_path,counter=counter)
                    else:
                        prepared=prepare_publication(candidate,bundle,publication_path,paperflow_root=paperflow_root,catalog=catalog,counter=counter,
                                                     redaction_policy='best_effort',candidate_validation=validation,admission_policy='learning')
                finally:counter.close()
                from associated_delivery import write_plan
                write_plan(bundle,prepared,work)
                saved_prepared={k:v for k,v in prepared.items() if k!='packet'}
                saved_prepared['candidate_sha256']=file_sha(work/'private'/'candidate.json')
                write_json(prepared_path,saved_prepared)
            write_json(work/'package-report.json',prepared['validation'])
            advance('package_verified',package_path=prepared['package_path'],segmentation_warnings=prepared['validation'].get('segmentation_warnings',[]))
            if stage!='deliver':return state
            if stop is not None and stop.is_set():raise InterruptedError('已停止，封包保留')
            advance('delivering_and_reading_back')
            from associated_delivery import load_plan,deliver_associated
            if prepared.get('associated_upload_plan_sha256') and 'packet' not in prepared:
                from preextract_publish import _paperflow
                prepared['packet']=_paperflow(prepared['paperflow_root']).validate_package(
                    prepared['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
            attachment_plan=load_plan(prepared,work)
            active_client=None
            if attachment_plan and attachment_plan['files']:
                from paperflow.rag import native_client
                active_client=native_client(SimpleNamespace(**runtime))
            receipt=deliver_publication(prepared,config=SimpleNamespace(**runtime),receipt_path=work/'delivery-receipt.json',client=active_client)
            if attachment_plan and attachment_plan['files']:
                deliver_associated(prepared,receipt,work,active_client,stop=stop)
            advance('ingested_and_verified',completed=receipt.get('status')=='complete',receipt_path=str(work/'delivery-receipt.json'))
            return state
        except BaseException as exc:
            # No keys, prompt material or identity values in public status/error.
            failed_stage=state['stage'];message=str(exc)
            if isinstance(connection,dict) and connection.get('api_key'):message=message.replace(connection['api_key'],'<redacted>')
            from time import time_ns
            prior_error=work/'private'/'error.json'
            if prior_error.exists():
                history=work/'private'/'errors';history.mkdir(exist_ok=True)
                (history/(str(time_ns())+'.json')).write_bytes(prior_error.read_bytes())
            write_json(work/'private'/'error.json',dict(error_type=type(exc).__name__,message=message,failed_stage=failed_stage))
            advance('stopped',error_type=type(exc).__name__,failed_stage=failed_stage,details_private=True)
            raise


def main(argv=None):
    parser=argparse.ArgumentParser(description='完整MD一次提取：单篇隔离、无自动重试')
    parser.add_argument('source',type=Path)
    parser.add_argument('--stage',choices=['prepare','extract','package','deliver'],default='package')
    parser.add_argument('--work-dir',type=Path)
    parser.add_argument('--template',type=Path,default=DEFAULT_TEMPLATE)
    parser.add_argument('--runtime-dir',type=Path,default=DEFAULT_RUNTIME)
    parser.add_argument('--paperflow-root',type=Path,default=DEFAULT_PAPERFLOW)
    args=parser.parse_args(argv)
    try:
        result=run_single(args.source,work_dir=args.work_dir,template_path=args.template,runtime_dir=args.runtime_dir,
                          paperflow_root=args.paperflow_root,stage=args.stage,progress=lambda s:print(s,flush=True))
        print(json.dumps(result,ensure_ascii=False,indent=2));return 0 if result.get('stage')!='candidate_requires_attention' else 2
    except Exception as exc:
        print(json.dumps(dict(error_type=type(exc).__name__,status='stopped',note='原稿和已有结果保留，未自动重试'),ensure_ascii=False));return 2


if __name__=='__main__':raise SystemExit(main())
