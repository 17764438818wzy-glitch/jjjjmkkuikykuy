"""Adapt the complete MD stages to AutoProcessor; no queue, thread or UI."""
from __future__ import annotations

import copy
import json
import shutil
from contextlib import ExitStack
from pathlib import Path
from urllib.error import HTTPError
from filelock import Timeout as FileLockTimeout

from engine import digest, read_json
from processing_errors import LocalDocumentError, describe_failure
from preextract_material import file_sha
from preextract_pipeline import DEFAULT_TEMPLATE, default_work_dir, run_single

FORMAT='paper-preextract-prepared/1'
PROGRAM_STAGES={
    'waiting_for_local_lock':'本地资源占用，等待后自动继续',
    'preparing':'解析全文与图表', 'material_prepared':'材料准备完成',
    'waiting_for_model':'材料准备完成，等待 AI 名额',
    'adapting_images':'检查并适配图片尺寸与体积',
    'checking_request':'检查请求容量', 'planning_input':'整理输入并按容量规划分段',
    'assembling_parts':'装配分段学习内容和来源', 'validating_candidate':'校验提取结果',
    'response_saved':'完整回答已保存，等待程序后处理',
    'postprocessing_started':'程序后处理开始',
    'redacting_and_packaging':'执行脱敏、切片与封包', 'package_verified':'封包检查完成'}


def model_connection(processor):
    """Use the model/credential selected on the existing desktop form."""
    cfg=processor.config
    base,key=processor.agent.connection()
    model=str(cfg.get('text_model') or '').strip()
    limits=copy.deepcopy(cfg.get('preextract_limits') or {})
    if limits and (limits.pop('model',model)!=model or limits.pop('base_url',base).rstrip('/')!=base.rstrip('/')):
        limits={}
    if not limits:
        catalog_path=Path.home()/'.dsh'/'.workbuddy-ai-catalog.json'
        catalog=json.loads(catalog_path.read_text(encoding='utf8'))
        # The local relay exposes the same WorkBuddy model IDs with a prefix.
        # Keep the selected API ID verbatim; normalize only the catalog lookup.
        catalog_id=model.removeprefix('workbuddy-')
        def find(value):
            if isinstance(value,dict):
                if value.get('id')==catalog_id and 'maxTokens' in value:return value
                for child in value.values():
                    match=find(child)
                    if match:return match
            elif isinstance(value,list):
                for child in value:
                    match=find(child)
                    if match:return match
            return None
        entry=find(catalog)
        if not entry:raise LocalDocumentError('model_capacity_unknown','所选提取模型缺少容量记录；请在原模型设置中选择有容量记录且支持图像的模型。')
        limits=dict(context_window=max(entry.get('supportedContextWindows') or [entry['contextWindow']]),max_output_tokens=entry['maxTokens'],
                    supports_images=entry.get('supportsImages'),max_total_image_bytes=2*1024*1024,
                    capacity_source='local model catalog; image payload uses conservative 2 MiB client budget')
    if not limits.pop('supports_images',False):
        raise LocalDocumentError('model_image_input_unknown','所选提取模型未声明支持图像；请在原模型设置中选择支持图像的提取模型。')
    options=cfg.get('concurrency') or {}
    return {**limits,'base_url':base,'api_key':key,'model':model,
            'min_start_interval_seconds':float(options.get('min_start_interval_seconds',0)),
            'request_start_root':str(Path(getattr(processor,'_resume_root',processor.store.root))/'private'/'model-starts'),
            'dispatch_backoff_root':str(Path(getattr(processor,'_resume_root',processor.store.root))/'private'/'model-dispatch-pause-v1'),
            'timeout':cfg.get('timeout',90),'max_tokens':cfg.get('max_tokens',8192)}


def validate_prepared_material(record, root):
    from ai_resume import _comparison_path
    reference=record['prepared_checkpoint'];path=Path(reference['path']).resolve()
    root=Path(root).resolve()
    # Only a checkpoint belonging to this application's saved work is usable.
    if not _comparison_path(path).is_relative_to(_comparison_path(root)) or path.name!='material.json':
        raise ValueError('脚本中间结果不属于当前程序资料目录')
    from preextract_retirement import find_terminal,terminal_matches
    retired=find_terminal(root,work=path.parent,source_hash=digest(Path(record['path'])))
    if retired:
        return path.parent,{'retired_delivery':retired['id']}
    if digest(path)!=reference['sha256']:raise ValueError('脚本中间结果已改变')
    bundle=json.loads(path.read_text(encoding='utf8'))
    if bundle.get('version')!='preextract-material/1.0':raise ValueError('脚本中间结果版本不兼容')
    if digest(Path(record['path']))!=bundle['source']['source_sha256']:
        raise ValueError('原文件已修改，请在第一页重新生成对应文件的材料')
    for name,expected in bundle.get('file_hashes',{}).items():
        p=(path.parent/name).resolve()
        if digest(p)!=expected:
            raise ValueError('已保存材料缺失或发生变化')
    return path.parent,bundle


def _failure(exc):
    from preextract_request import RequestCapacityError,IncompleteModelResponse,ModelExplicitError
    if isinstance(exc,IncompleteModelResponse):
        return dict(code='model_output_length' if exc.finish_reason=='length' else 'model_response_incomplete',message=str(exc),retry_allowed=False)
    if isinstance(exc,ModelExplicitError):
        return dict(code='model_stream_aborted' if exc.error_code=='upstream_stream_error' else 'model_upstream_error',message=str(exc),retry_allowed=False)
    if isinstance(exc,InterruptedError):
        if getattr(exc,'stop_reason','body_user_stop')!='body_user_stop':
            return dict(code='processing_stopped',message='批次内部异常触发停止，已有请求证据和部分结果保留；未自动重发。',retry_allowed=False)
        return dict(code='model_user_stopped',message='用户已停止，已有请求证据和部分结果保留；未自动重发。',retry_allowed=False)
    if isinstance(exc,RequestCapacityError):return {'code':'request_capacity','message':str(exc)}
    if isinstance(exc,FileLockTimeout):
        return {'code':'local_lock_timeout','message':'本地资源锁仍被占用，保留待办；已有材料和结果未重跑。'}
    if isinstance(exc,HTTPError):
        return {'code':'model_http_'+str(exc.code),'message':f'模型请求返回 HTTP {exc.code}；请求记录及已有结果保留，未自动重发。'}
    if isinstance(exc,TimeoutError):
        return {'code':'model_timeout','message':'模型请求超时；已有结果保留，未自动重发。'}
    if isinstance(exc,(ValueError,RuntimeError)) and str(exc).startswith(
            ('分段结构或来源校验失败','模型回答格式校验未通过','全文统合')):
        return {'code':'preextract_response_validation','message':str(exc)}
    # These messages originate in deterministic local validation, never in a
    # model response. Other exception text stays in the private error journal.
    prefixes=('图像总容量超限','单图容量超限','输入输出容量不足','HTTP请求容量超限',
              '图片压缩后仍超过容量','图片容量配置无效','图片版本','图片请求','图片尚未完成',
              '已有适配图片版本','已有模型请求，必须复用',
              '该原稿已有失败或未决模型请求','已存在本版本模型请求记录','模型输出未完整结束',
              '材料缓存','保存的模型响应发生变化','已有结果对应不同','该原稿已有模型请求记录',
              '已有输入分段','输入分段模型','已登记分段','保存的分段','已完成分段',
              '已装配学习内容','分段结构或来源校验失败','全文统合')
    if isinstance(exc,(ValueError,RuntimeError)) and str(exc).startswith(prefixes):
        return {'code':'preextract_validation','message':str(exc)}
    return describe_failure(exc)



def invoke_once(processor,body,connection,model_dir,**kwargs):
    """Share the existing gate, billing and durable one-request transport."""
    from preextract_request import request_once
    ledger_task=kwargs.pop('ledger_task','paper_preextract')
    controlled=getattr(processor,'_prepared_request_control',None)
    from preextract_throttle import wait_for_dispatch,remaining_delay
    if getattr(processor,'_preextract_in_postprocess',False):
        raise RuntimeError('程序后处理阶段禁止发起模型请求')
    purpose=(ledger_task=='material_purpose_review')
    if purpose and getattr(processor,'_purpose_preclassified_only',False):
        raise RuntimeError('正文处理线程禁止发起用途模型请求')
    gate=None if controlled else getattr(processor.agent,'purpose_gate' if purpose else 'llm_gate',None)
    if purpose and gate is None and controlled is None:
        from auto_pipeline import _ObservedGate
        gate=_ObservedGate((getattr(processor,'config',{}).get('purpose_review') or {}).get('workers',10))
        processor.agent.purpose_gate=gate
    shared=(getattr(processor.agent,'llm_gate',None) if purpose and controlled is None
            and not processor.config.get('_independent_purpose_llm') else None)
    gates=[gate] if gate is not None else []
    if shared is not None and shared is not gate:gates.append(shared)
    acquired=[];receipt=None
    phase=ExitStack();phase_entered=False
    ledger=getattr(processor.agent,'ledger',None)
    def release_gates():
        while acquired:
            held,shared_purpose=acquired.pop()
            if shared_purpose:held.release(purpose=True)
            else:held.release()
    def ready():
        nonlocal phase_entered
        if purpose and not phase_entered:
            scope=getattr(processor,'_preextract_purpose',None)
            if scope:phase.enter_context(scope())
            phase_entered=True
        while True:
            wait_for_dispatch(connection,processor.stop,kwargs.get('progress'))
            for request_gate in gates:
                if any(held is request_gate for held,_ in acquired):continue
                shared_purpose=purpose and request_gate is shared
                while True:
                    if processor.stop.is_set():raise InterruptedError('请求前已停止')
                    admitted=(request_gate.acquire(timeout=0.1,purpose=True) if shared_purpose
                              else request_gate.acquire(timeout=0.1))
                    if admitted:
                        acquired.append((request_gate,shared_purpose))
                        break
            if remaining_delay(connection,processor.stop)<=0:break
            release_gates()
        if processor.stop.is_set():raise InterruptedError('请求前已停止')
    def started():
        nonlocal receipt
        if ledger:
            receipt=ledger.begin(connection['model'],'text',ledger_task,1,processor.config,
                                 processor.agent.billing_context)
    try:
        # Cached answers and both kinds of dispatch waits consume no model slot.
        kwargs['on_request_ready']=ready
        batch_control=processor._global_control() if processor.config.get('_native_task_batch') else None
        if controlled is not None:
            kwargs['dispatch']=lambda send,descriptor:controlled.dispatch(send,descriptor,purpose=purpose)
        elif batch_control is not None:
            hold=getattr(processor,'_task_binding_hold',None) or (getattr(processor,'_purpose_admission_hold',None) if purpose else None)
            if hold is not None:
                def guarded_dispatch(send,descriptor):
                    run_id=getattr(processor,'_global_batch_id',None) or processor._active_run_id
                    with hold.guard(batch_control.http_scope(run_id,descriptor['request_sha256'],purpose=purpose)):
                        return send()
                kwargs['dispatch']=guarded_dispatch
            else:
                kwargs['dispatch']=lambda send,descriptor:batch_control.dispatch(send,descriptor,
                    run_id=getattr(processor,'_global_batch_id',None) or processor._active_run_id,purpose=purpose)
        return request_once(body,connection,model_dir,opener=processor.agent.opener,
                            on_request_started=started,**kwargs)
    finally:
        release_gates()
        try:
            saved=Path(model_dir)/'request-state.json'
            state=json.loads(saved.read_text(encoding='utf8')) if saved.exists() else {}
            if ledger and receipt:
                complete=state.get('status')=='response_complete'
                ledger.finish(receipt,state.get('usage'),'response' if complete else 'error',ok=complete,
                              error_type=state.get('error_type'))
                if processor.agent.on_usage:processor.agent.on_usage()
        finally:phase.close()

def register_package_result(item, workspace, run, result):
    """The normal publication registration, shared with saved-answer recovery.

    Mutate only this item after verifying the native prepared package and copy.
    Its original run/item/source/checkpoint and delivery fields stay intact.
    The owning native caller still performs the manifest save and handoff.
    """
    if result.get('stage')!='package_verified':
        raise ValueError('封包尚未验证，不能授予入库资格')
    workspace,run=Path(workspace).resolve(),Path(run).resolve()
    if (Path(result['work_dir']).resolve()!=workspace
            or not item.get('source_hash') or result.get('source_sha256')!=item['source_hash']):
        raise ValueError('发布登记的原来源或工作目录身份不一致')
    prepared=json.loads((workspace/'private/prepared-publication.json').read_text(encoding='utf8'))
    if (prepared['task'].get('source_sha256')!=item['source_hash']
            or Path(result['package_path']).resolve()!=Path(prepared['package_path']).resolve()):
        raise ValueError('发布登记的封包来源或路径发生变化')
    from preextract_publish import _paperflow
    package=_paperflow(prepared['paperflow_root'])
    original=package.validate_package(result['package_path'],prepared['task'],prepared['catalog'],strict_seal=True)
    if original['manifest_sha256']!=prepared['validation']['manifest_sha256']:
        raise ValueError('原封包校验失败')
    ident=item.get('id')
    if (not isinstance(ident,str) or not ident or ident in {'.','..'}
            or Path(ident).name!=ident or any(c in ident for c in '/\\:')):
        raise ValueError('发布登记的原条目身份无效')
    final=run/'packages'/ident
    final.parent.mkdir(exist_ok=True)
    if not final.exists():
        # A failed copy stays separate from the final native identity. Never
        # merge into or replace a previous package, including a partial one.
        from uuid import uuid4
        pending=final.with_name('.'+ident+'.registering-'+uuid4().hex)
        shutil.copytree(result['package_path'],pending)
        copied=package.validate_package(pending,prepared['task'],prepared['catalog'],strict_seal=True)
        if copied['manifest_sha256']!=original['manifest_sha256']:
            raise ValueError('临时复制封包校验失败；原件及复制证据保留')
        if not final.exists():
            try:pending.rename(final)
            except FileExistsError:pass
    packet=package.validate_package(final,prepared['task'],prepared['catalog'],strict_seal=True)
    if packet['manifest_sha256']!=original['manifest_sha256']:
        raise ValueError('已有或复制封包校验失败；原封包保留，未覆盖')
    routes=packet['routes'];profile=packet['profile']
    redaction=prepared['validation'].get('redaction',{})
    warning_count=len(redaction.get('warnings',[]))+len(redaction.get('residual_findings',[]))
    item.update(title=profile['article']['title'],major='、'.join(r['name'] for r in routes),
        category='专业目录',status='pass',reason='MD 已校验，基础脱敏、切片与封包完成',reason_kind='',
        package_path=str(final),path=str(final/'article.redacted.md'),display_path=str(final/'article.redacted.md'),
        knowledge_manifest_path=str(final/'manifest.json'),paper_id=prepared['task']['source_id'],
        preextract_manifest_sha256=packet['manifest_sha256'],preextract_routes=routes,
        preextract_redaction=redaction,
        reviewed_hash=digest(final/'article.redacted.md'),reviewed_package_hash=packet['manifest_sha256'],
        automatic_approval=True,review_status='ai_passed',redaction_status='ai_passed',profile_status='ready')
    images=item.get('image_preprocessing',{})
    item['script_result']={'status':'completed','reason':'材料准备、结果校验、基础脱敏、切片和封包均已完成'
                          +(f"；图片检查 {images['image_count']} 张，适配 {images['adjusted_count']} 张" if images else '')
                          +(f'；脱敏有 {warning_count} 项提示，详见本篇记录' if warning_count else '')}
    return packet


def _local_registration_identity(item):
    return {key:copy.deepcopy(item.get(key)) for key in ('id','source_path','source_hash',
        'prepared_checkpoint','preextract_work_dir','package_path','preextract_manifest_sha256','automatic_approval')}


def _saved_answer_files(workspace):
    """Keep exact transport evidence immutable across local publication."""
    paths=[workspace/name for name in ('material.json','private/request-images.json',
        'revision.json','candidate-import.json')]
    paths.extend(path for path in (workspace/'model').rglob('*') if path.is_file())
    return {str(path.resolve()):file_sha(path) for path in paths if path.is_file()}


_LOCAL_PUBLICATION_FIELDS=(
    'title','major','category','status','reason','reason_kind','package_path','path','display_path',
    'knowledge_manifest_path','paper_id','preextract_manifest_sha256','preextract_routes',
    'preextract_redaction','reviewed_hash','reviewed_package_hash','automatic_approval',
    'review_status','redaction_status','profile_status','script_result','ai_result',
    'content_format','preextract_work_dir','local_publication_history')
_OLD_FAILURE_FIELDS=('business_decision','failure_code','failure_stage','failure_position')


def _assert_local_package_closure(workspace, record, result, template_path):
    prepared=read_json(workspace/'private/prepared-publication.json')
    candidate_path=workspace/'private/candidate.json'
    candidate=read_json(candidate_path)
    validation=read_json(workspace/'validation.json')
    context=read_json(workspace/'private/completed-source-context.json')
    if (result.get('stage')!='package_verified' or not validation.get('valid')
            or validation.get('candidate')!=candidate
            or prepared.get('candidate_sha256')!=file_sha(candidate_path)
            or prepared.get('task',{}).get('source_sha256')!=file_sha(Path(record['path']))
            or not prepared.get('validation',{}).get('embedding',{}).get('admission_verified')
            or context.get('schema')!='completed-source-context/1'
            or context.get('source_sha256')!=prepared['task']['source_sha256']
            or context.get('material_sha256')!=record['prepared_checkpoint']['sha256']
            or context.get('candidate_sha256')!=prepared['candidate_sha256']
            or Path(context.get('template_path','')).resolve()!=Path(template_path).resolve()
            or context.get('template_sha256')!=file_sha(Path(template_path))):
        raise ValueError('真实封包与原候选、校验、材料或模板证据未闭合，未登记')
    return prepared


def _assert_existing_delivery_identity(store, run_id, item, workspace, prepared, *, roots):
    """Read exact original references; never create, reset or enqueue a job."""
    import sqlite3
    from contextlib import closing
    from preextract_delivery_queue import queue_path, package_reference
    final=Path(item['package_path']).resolve()
    from preextract_publish import _paperflow
    package=_paperflow(prepared['paperflow_root'])
    manifest_sha=item['preextract_manifest_sha256']
    binding=None
    for name in ('delivery-binding.json','delivery-receipt.json'):
        path=workspace/name
        if not path.exists():continue
        saved=read_json(path)
        if (saved.get('task')!=prepared['task'] or saved.get('manifest_sha256')!=manifest_sha
                or (name=='delivery-binding.json' and saved.get('routes')!=item['preextract_routes'])):
            raise ValueError('已有交付意图或回执对应其他task或封包，未改变登记')
        if name=='delivery-binding.json':binding=saved
        elif binding is None:
            raise ValueError('已有交付回执缺少原服务绑定，未创建替代身份')
    paths={queue_path(root) for root in (store.root,*roots)}
    for path in paths:
        if not path.exists():continue
        try:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5)) as db:
                db.row_factory=sqlite3.Row
                references=db.execute('SELECT * FROM references_to_deliveries WHERE run_id=? AND item_id=?',
                                      (run_id,item['id'])).fetchall()
                jobs={row['id']:dict(row) for row in db.execute('''SELECT * FROM deliveries WHERE
                    id IN (SELECT delivery_id FROM references_to_deliveries WHERE run_id=? AND item_id=?)
                    OR json_extract(payload,'$.item.preextract_work_dir')=?
                    OR json_extract(payload,'$.item.package_path')=?
                    OR (json_extract(payload,'$.task.source_sha256')=?
                        AND json_extract(payload,'$.manifest_sha256')=?)''',
                    (run_id,item['id'],str(workspace),str(final),item['source_hash'],manifest_sha))}
                for reference in references:
                    if (reference['delivery_id'] not in jobs
                            or reference['source_hash']!=item['source_hash']
                            or Path(reference['package_path']).resolve()!=final):
                        raise ValueError('原run/item队列引用与来源或发布路径不一致')
                for job in jobs.values():
                    payload=json.loads(job['payload']);saved=payload['item']
                    expected=package_reference(store,run_id,item['id'],payload['service'],item=item)
                    if (job['status']=='uploading' or job['id']!=expected['id']
                            or payload.get('task')!=prepared['task']
                            or payload.get('manifest_sha256')!=manifest_sha
                            or saved.get('source_hash')!=item['source_hash']
                            or (binding is not None and binding.get('service')!=payload['service'])):
                        raise ValueError('已有交付仍在运行或对应其他原始身份，未接管或改写')
                    # A canonical payload may belong to an earlier shared run.
                    # Its original reference/work/receipt are separate from
                    # this run's reference. Never rewrite them to this work.
                    canonical_work=Path(saved['preextract_work_dir']).resolve()
                    canonical_root=Path(payload['store_root']).resolve()
                    canonical_path=Path(saved['package_path']).resolve()
                    reference=db.execute('SELECT * FROM references_to_deliveries WHERE run_id=? AND item_id=?',
                        (payload['run_id'],saved['id'])).fetchone()
                    if (queue_path(canonical_root)!=path
                            or not canonical_work.is_relative_to(canonical_root)
                            or not canonical_path.is_relative_to((canonical_root/'runs'/payload['run_id']/'packages').resolve())
                            or reference is None or reference['delivery_id']!=job['id']
                            or reference['source_hash']!=item['source_hash']
                            or Path(reference['package_path']).resolve()!=canonical_path):
                        raise ValueError('共享交付的原始run/item引用或路径不一致')
                    canonical_prepared=read_json(canonical_work/'private/prepared-publication.json')
                    if (canonical_prepared.get('task')!=prepared['task']
                            or canonical_prepared.get('validation',{}).get('manifest_sha256')!=manifest_sha):
                        raise ValueError('共享交付的原工作封包身份不一致')
                    for name in ('delivery-binding.json','delivery-receipt.json'):
                        saved_path=canonical_work/name
                        if not saved_path.exists():continue
                        intent=read_json(saved_path)
                        if (intent.get('task')!=prepared['task'] or intent.get('manifest_sha256')!=manifest_sha
                                or (name=='delivery-binding.json' and intent.get('service')!=payload['service'])
                                or (name=='delivery-receipt.json' and not (canonical_work/'delivery-binding.json').is_file())):
                            raise ValueError('共享交付的原服务意图或回执不一致')
                    canonical=package.validate_package(saved['package_path'],payload['task'],
                                                       prepared['catalog'],strict_seal=True)
                    if canonical['manifest_sha256']!=manifest_sha:
                        raise ValueError('已有交付的原封包strict seal不一致，未改写原引用')
        except (sqlite3.Error,OSError,KeyError,TypeError,ValueError) as exc:
            raise ValueError('原交付队列或回执身份无法精确核对，保留封包待接续') from exc


def recover_local_publication(store, run_id, item_id, *, exclusive_manifest_guard,
                              expected_manifest_sha256, data_root=None,
                              template_path=DEFAULT_TEMPLATE, runtime_dir=None,
                              paperflow_root=None, stop=None, progress=lambda name:None):
    """Reuse a complete unmarked answer and register its real original item.

    No AutoProcessor, purpose review, prepared enrollment, model or upload entry.
    The sole closeout caller must supply a context manager holding its ORIGINAL
    exclusion path(s) and proving all original manifest producers/writers exited
    or are excluded. It yields a recheck callback: recheck() must raise if any
    exclusion/producer identity ceases to hold. This is a trusted caller contract,
    not proof derived from a status label, a private new lock or Store.lock.
    expected_manifest_sha256 binds the original raw manifest version. No guard
    is acquired or global/task admission resumed implicitly by this function.
    """
    from filelock import FileLock
    from archive_identity import member_fields
    from global_task_control import GlobalTaskControl,GlobalPaused,StageSequence
    from preextract_scope_guard import require_managed
    root=Path(data_root or store.root).resolve()
    run=Path(store.manifest_path(run_id)).parent.resolve()
    if not Path(store.root).resolve().is_relative_to(root) or not run.is_relative_to(root/'runs'):
        # Child native stores are also below the data root, outside root/runs.
        if not (Path(store.root).resolve().is_relative_to(root) and run.is_relative_to(Path(store.root).resolve()/'runs')):
            raise ValueError('发布登记不属于指定原资料目录')
    control=GlobalTaskControl(root)
    if not control.path.is_file() or not control.snapshot()['admission_open']:
        raise GlobalPaused('原生全局控制未准入；未恢复或登记')
    def forbidden(*args,**kwargs):
        raise RuntimeError('local publication forbids any model or purpose request')
    if (not isinstance(expected_manifest_sha256,str) or len(expected_manifest_sha256)!=64
            or any(c not in '0123456789abcdef' for c in expected_manifest_sha256)
            or not hasattr(exclusive_manifest_guard,'__enter__')):
        raise ValueError('必须提供原manifest版本及原写入者的真实排他上下文')
    # The context belongs to the single existing closeout executor, not to a
    # second writer protocol which historical native writers never observe.
    with exclusive_manifest_guard as recheck_exclusive:
        if not callable(recheck_exclusive):
            raise ValueError('原写入者排他上下文缺少持续身份复核')
        recheck_exclusive()
        with store.lock:
            if file_sha(Path(store.manifest_path(run_id)))!=expected_manifest_sha256:
                raise ValueError('原manifest版本已变化，未接管新记录')
            manifest=read_json(Path(store.manifest_path(run_id)))
            if manifest.get('run_id')!=run_id or manifest.get('status')=='running':
                raise ValueError('原批次仍在处理或身份不一致，未接管运行中任务')
            matches=[row for row in manifest.get('items',[]) if row.get('id')==item_id]
            if len(matches)!=1:raise ValueError('原批次条目不存在或重复')
            item=copy.deepcopy(matches[0]);identity=_local_registration_identity(item)
        source=Path(item['source_path'])
        if file_sha(source)!=item.get('source_hash'):
            raise ValueError('原批次源文件版本发生变化')
        record=dict(path=str(source),name=source.name,**member_fields(item),
                    prepared_checkpoint=copy.deepcopy(item.get('prepared_checkpoint')))
        if not record['prepared_checkpoint'] or record['prepared_checkpoint'].get('format')!=FORMAT:
            raise ValueError('原条目缺少正式材料checkpoint，未创建替代checkpoint')
        workspace,bundle=validate_prepared_material(record,root)
        if bundle.get('retired_delivery'):
            raise ValueError('原条目已有核验归档事实，请沿原交付身份核对')
        if item.get('preextract_work_dir') and Path(item['preextract_work_dir']).resolve()!=workspace.resolve():
            raise ValueError('原条目工作目录与checkpoint不一致')
        def assert_unmarked():
            marker=workspace/'prepared-scope.json'
            if marker.exists() or marker.is_symlink():
                raise ValueError('此入口只接无marker回答；空或损坏marker也必须沿原scope核对')
            if any((workspace/name).exists() for name in ('revision.json','candidate-import.json')):
                raise ValueError('派生修订或导入候选须沿原版本恢复；此入口只登记原生保存回答')
            if stop is not None and stop.is_set():
                raise InterruptedError('已停止；原回答及真实封包保留，未继续登记')
            if file_sha(source)!=item['source_hash']:
                raise ValueError('原来源发生变化，未登记')
            validate_prepared_material(record,root)
            recheck_exclusive()
        with FileLock(str(workspace/'operation.lock'),timeout=0):
            assert_unmarked()
            protected=_saved_answer_files(workspace)
        phases=StageSequence(control,batch_id=run_id,paper_id=item_id,name=source.name)
        options={key:value for key,value in dict(runtime_dir=runtime_dir,paperflow_root=paperflow_root).items() if value is not None}
        failure=None;committed=False;registration=None
        try:
            result=run_single(source,work_dir=workspace,template_path=Path(template_path),
                stage='package',local_only=True,data_root=root,associated_document=record,
                stop=stop,progress=progress,connection=forbidden,invoke=forbidden,
                before_model=forbidden,before_postprocess=assert_unmarked,global_phases=phases,**options)
            if result.get('stage')!='package_verified':
                return dict(run_id=run_id,item_id=item_id,registered=False,stage=result.get('stage'),work_dir=str(workspace))
            phases.start('postprocess')
            with FileLock(str(workspace/'operation.lock'),timeout=0):
                assert_unmarked()
                require_managed(workspace,None)
                if _saved_answer_files(workspace)!=protected or file_sha(source)!=item['source_hash']:
                    raise ValueError('本地封包期间原来源或模型证据发生变化，未登记')
                prepared=_assert_local_package_closure(workspace,record,result,template_path)
                register_package_result(item,workspace,run,result)
                item['content_format']='paper_preextract';item['preextract_work_dir']=str(workspace)
                _assert_existing_delivery_identity(store,run_id,item,workspace,prepared,roots=(root,))
                item['ai_result']=dict(status='passed',reason='复用原完整回答并完成原生校验；未重新请求模型',stage='完整 MD 提取')
                from datetime import datetime,timezone
                history=copy.deepcopy(item.get('local_publication_history') or [])
                history.append(dict(at_utc=datetime.now(timezone.utc).isoformat(),
                    original_manifest_sha256=expected_manifest_sha256,
                    previous={key:copy.deepcopy(matches[0][key]) for key in
                        ('status','reason','reason_kind','ai_result','script_result',*_OLD_FAILURE_FIELDS)
                        if key in matches[0]}))
                item['local_publication_history']=history
                with store.lock:
                    assert_unmarked()
                    recheck_exclusive()
                    if file_sha(Path(store.manifest_path(run_id)))!=expected_manifest_sha256:
                        raise ValueError('原manifest在恢复期间变化，已保留封包，未覆盖任何记录')
                    current=read_json(Path(store.manifest_path(run_id)))
                    rows=[row for row in current.get('items',[]) if row.get('id')==item_id]
                    if (current.get('status')=='running' or len(rows)!=1
                            or _local_registration_identity(rows[0])!=identity):
                        raise ValueError('原条目在登记期间变化，已保留封包，未覆盖新记录')
                    # Only native publication fields; never persist a projected
                    # delivery snapshot, other items, batch counts or billing.
                    rows[0].update({key:copy.deepcopy(item[key]) for key in _LOCAL_PUBLICATION_FIELDS if key in item})
                    for key in _OLD_FAILURE_FIELDS:rows[0].pop(key,None)
                    store.save(current)
                    committed=True
                registration=dict(run_id=run_id,item_id=item_id,registered=True,stage='package_registered',
                    work_dir=str(workspace),package_path=item['package_path'],
                    manifest_sha256=item['preextract_manifest_sha256'],model_requests=0,upload_performed=False)
                return registration
        except BaseException as exc:
            failure=exc
            raise
        finally:
            try:phases.finish('failed' if failure else 'complete',type(failure).__name__ if failure else '')
            except Exception as secondary:
                if failure is None and not committed:raise
                if committed and registration is not None:
                    registration.update(stage_ledger_finalization='unknown',stage_ledger_error=type(secondary).__name__)
                # A cleanup/ledger error cannot erase the original business
                # error or misreport a manifest already durably committed.
                try:
                    from processing_stop import record_failure
                    record_failure(root,secondary,stage='local_publication_stage_finish',run_id=run_id)
                except Exception:pass


def process_item(processor,source,record,work,run,item,tracker,stage):
    """One worker body; AutoProcessor retains all scheduling and final saves."""
    item['content_format']='paper_preextract'
    item['previous_ai_reason']=record.get('previous_ai_reason','')
    # Upload history survives removal of local checkpoints and model answers.
    from preextract_retirement import find_terminal,terminal_matches
    from upload_tracking import service_key
    client=getattr(processor,'rag_client',None)
    history_root=getattr(processor,'_resume_root',processor.store.root)
    template=Path(processor.config.get('preextract_template') or DEFAULT_TEMPLATE)
    from preextract_regeneration import active_workspace
    regenerated=active_workspace(source,template,root=history_root,campaign=record.get('regeneration_campaign'))
    reference=record.get('prepared_checkpoint') or {}
    selected=(regenerated if regenerated is not None else
              Path(reference['path']).parent if reference.get('format')==FORMAT and reference.get('path') else None)
    filters=dict(source_hash=file_sha(source),service=service_key(client.base_url) if client else None)
    retired=find_terminal(history_root,work=selected,**filters) if selected else None
    active=selected is not None and (regenerated is not None or
        any((selected/name).is_file() for name in ('material.json','model/request-state.json','revision.json')))
    if not retired and not active:
        history=terminal_matches(history_root,**filters)
        if len(history)>1:
            raise ValueError('该原稿有多条已核验上传记录，请选定原任务；未重新请求模型')
        retired=history[0] if history else None
    if retired:
        old=retired['payload']['item']
        item.update({k:copy.deepcopy(v) for k,v in old.items() if k not in {'id','uploads'}})
        item.update(copy.deepcopy(retired['projection']))
        item.update(path=str(source),display_path=str(source),reason='已入库；本地产物按上传记录清理',
            preextract_delivery_queue=dict(mode='disk_queue_v1',id=retired['id'],status='complete',service=retired['payload']['service']))
        item['script_result']=dict(status='completed',reason='复用已核验上传记录')
        item['ai_result']=dict(status='completed',reason='已入库，未重复请求模型',stage='完整 MD 提取')
        tracker.notify(item)
        return
    reference=record.get('prepared_checkpoint') or {}
    if regenerated is not None:
        workspace=regenerated
        item['regeneration_campaign']=json.loads((workspace/'regeneration.json').read_text(encoding='utf8'))['campaign']
    elif reference.get('format')==FORMAT:
        workspace,_=validate_prepared_material(record,getattr(processor,'_resume_root',processor.store.root))
    else:
        existing=default_work_dir(source,template,data_root=history_root)
        workspace=existing if (existing/'material.json').exists() or (existing/'model'/'request-state.json').exists() else Path(work)/'preextract'
    item['preextract_work_dir']=str(workspace)
    from preextract_scope_guard import require_managed
    control=getattr(processor,'_prepared_request_control',None)
    actor=(dict(schema='prepared-scope-binding/1',database=str(control.registry.store.path),scope_id=control.scope_id,
                generation=control.generation,task_id=control.task_id) if control else None)
    require_managed(workspace,actor)
    item['associated_inventory_binding']='historical' if (workspace/'material.json').exists() else 'current'
    last=['preparing'];entered=[False];accepted=[False];reused=[False]
    post_scope=ExitStack()
    from global_task_control import GlobalTaskControl, GlobalPaused, StageSequence
    phases=StageSequence(None if control else processor._global_control(),
        batch_id=getattr(processor,'_global_batch_id',Path(run).name),paper_id=item['id'],name=source.name)
    processor._global_stage_sequence=phases

    def begin_model():
        phases.finish()
        notify('waiting_for_model')
        slot=getattr(processor,'_preextract_enter_model',None)
        if slot:slot()
        phases.start('model')

    def begin_postprocess():
        phases.finish()
        slot=getattr(processor,'_preextract_postprocess',None)
        if slot:post_scope.enter_context(slot())
        phases.start('postprocess')
        processor._preextract_in_postprocess=True
        notify('postprocessing_started')

    def notify(name):
        if name=='stopped':return
        if name not in PROGRAM_STAGES and name not in {'extracting_once','extracting_parts','reconciling_parts','reusing_saved_response','candidate_imported','saved_model_request_incomplete',
                'candidate_received','candidate_validated','candidate_requires_attention','candidate_excluded'}:
            tracker.notify(item)
            return
        last[0]=name
        if name in {'material_prepared','checking_request'}:
            image_path=workspace/'private/request-images.json'
            if image_path.exists():
                report=json.loads(image_path.read_text(encoding='utf8'))
                item['image_preprocessing']={key:report[key] for key in ('image_count','adjusted_count','max_side','max_bytes')}
        if name=='material_prepared':
            path=workspace/'material.json'
            item['prepared_checkpoint']={'format':FORMAT,'path':str(path),'sha256':digest(path)}
            images=item.get('image_preprocessing',{})
            detail=(f"；检查 {images['image_count']} 张图片，调整 {images['adjusted_count']} 张超限或方向异常图片" if images else '')
            item['script_result']={'status':'completed','reason':'全文与图表材料已保存'+detail+'；AI 结论见“AI 处理结果”'}
        if name=='waiting_for_model':
            item['script_result']={'status':'completed','reason':'全文与图表材料已保存，等待 AI 名额'}
            if not entered[0]:
                item['ai_result']={'status':'not_started','reason':'材料已就绪，等待 AI 名额','stage':''}
        if name in {'extracting_once','extracting_parts','reconciling_parts','reusing_saved_response','candidate_imported','saved_model_request_incomplete'}:
            entered[0]=True
            reused[0]=name not in {'extracting_once','extracting_parts','reconciling_parts'}
            tracker.observe('complete-md','paper_preextract','running')
            detail={'extracting_once':'一次请求读取完整文本和实际图像',
                    'extracting_parts':'读取程序划定的分段，完整保存本段学习内容',
                    'reconciling_parts':'统一全文画像和跨段关系，保留已提取详细内容',
                    'reusing_saved_response':'复用已保存的模型结果',
                    'candidate_imported':'收取有来源记录的格式修复或定向补读结果',
                    'saved_model_request_incomplete':'已保存模型请求未完整完成；保留结果，不自动重发'}
            stage('AI 完整 MD 提取',detail[name])
        elif name=='response_saved':
            item['ai_result']={'status':'response_saved','reason':'完整模型输出已保存，等待程序校验；尚未授予入库资格',
                               'stage':'完整 MD 提取'}
            item['script_result']={'status':'running','reason':PROGRAM_STAGES[name]}
            stage(PROGRAM_STAGES[name])
        elif name=='candidate_validated':
            accepted[0]=True
            tracker.observe('complete-md','paper_preextract','pass',cache_hit=reused[0])
            tracker.ai_complete()
            validation=json.loads((workspace/'validation.json').read_text(encoding='utf8'))
            item['preextract_model_record_kind']=validation['candidate']['record_kind']
            if item['preextract_model_record_kind']=='partial_candidate':
                item['ai_result']['reason']='模型标记为部分提取，缺项及原始判断已保留在候选 MD'
        elif name in PROGRAM_STAGES:
            if name in {'adapting_images','checking_request','planning_input','assembling_parts','postprocessing_started','validating_candidate','redacting_and_packaging'}:
                item['script_result']={'status':'running','reason':PROGRAM_STAGES[name]}
            stage(PROGRAM_STAGES[name])
        tracker.notify(item)
        if name in {'material_prepared','response_saved'}:phases.finish()

    def invoke(body,connection,model_dir,**kwargs):
        return invoke_once(processor,body,connection,model_dir,**kwargs)

    try:
        enter_prepare=getattr(processor,'_preextract_enter_prepare',None)
        if enter_prepare:enter_prepare()
        from material_conversion import preparation_context
        from contextlib import nullcontext
        from contextlib import contextmanager
        from purpose_review import scope as purpose_scope
        @contextmanager
        def material_scope():
            with preparation_context(getattr(processor,'_resume_root',processor.store.root),
                    enabled=(processor.config.get('preextract_preparation') or {}).get('office_enabled',True),
                    stop=processor.stop,wait_scope=getattr(processor,'_preextract_conversion',nullcontext)),purpose_scope(processor,work):
                yield
        result=run_single(source,work_dir=workspace,template_path=template,
            **{k:processor.config['preextract_'+k] for k in ('runtime_dir','paperflow_root') if 'preextract_'+k in processor.config},
            stage='package',stop=processor.stop,progress=notify,connection=lambda:model_connection(processor),
            invoke=invoke,max_pages=int(processor.config.get('max_pages',80)),
            associated_document=record,associated_records=getattr(processor,'_material_context',None) or getattr(processor,'_material_inputs',[]),
            before_postprocess=begin_postprocess,before_model=begin_model,preparation_context=material_scope,
            data_root=history_root,global_phases=phases)
        state=result['stage']
        if state in {'candidate_requires_attention','candidate_excluded'}:
            diagnostics=candidate_diagnostics(item,getattr(processor,'_resume_root',processor.store.root)) if state=='candidate_requires_attention' else []
            reason=('候选已保留；'+ '；'.join(diagnostics[:3])) if diagnostics else ('候选已保留，需核对实际校验记录' if state=='candidate_requires_attention' else '模型判断此材料不予保留；完整依据保存在候选 MD')
            if entered[0]:tracker.observe('complete-md','paper_preextract','reject',reason)
            item['ai_result']={'status':'incomplete' if state=='candidate_requires_attention' else 'rejected',
                               'reason':reason,'stage':'完整 MD 提取'}
            item['script_result']={'status':'completed','reason':'材料准备和字段校验已完成；AI 结论见“AI 处理结果”'}
            item.update(status='reject',reason=reason,reason_kind='evidence',business_decision='reject')
            dest=Path(run)/'rejected'/(item['id']+'.md');dest.parent.mkdir(exist_ok=True)
            dest.write_text('# 提取结果待处理\n\n'+reason,encoding='utf8')
            item.update(path=str(dest),display_path=str(dest));return
        register_package_result(item,workspace,run,result)
        stage('程序处理完成','封包已保存，按现有任务设置交接入库')
        tracker.notify(item)
    except GlobalPaused as exc:
        batch_paused=getattr(exc,'reason_kind',None)=='batch_paused'
        reason='本阶段结果已保存；本任务暂停，等待继续' if batch_paused else '本阶段结果已保存；全局暂停，等待继续'
        item.update(status='pending',reason=reason,
            reason_kind='batch_paused' if batch_paused else 'global_paused',business_decision='pending',automatic_approval=False)
        pending=Path(run)/'pending'/(item['id']+'.md');pending.parent.mkdir(exist_ok=True)
        pending.write_text('# 等待继续\n\n'+reason,encoding='utf8')
        item.update(path=str(pending),display_path=str(pending))
        if not entered[0]:
            item['ai_result']=dict(status='not_started',reason='材料已保留，等待继续',stage='')
        tracker.notify(item)
        return
    except Exception as exc:
        if (last[0]=='preparing' and not entered[0] and not processor.stop.is_set()
                and not (workspace/'material.json').exists()
                and (workspace/'private'/('original'+source.suffix.lower())).is_file()):
            # A failed native reader is a program state, never a quality verdict.
            from purpose_review import enabled as purpose_enabled
            if purpose_enabled(processor):
                from processing_errors import describe_failure
                reading_failure=describe_failure(exc)
                item.update(status='pending',title='正文待解析',content_format='paper_preextract',
                    reason='正文尚未读完，保留原稿待续办；'+reading_failure['message'],
                    failure_code=reading_failure['code'],failure_stage='解析全文与图表',
                    reason_kind='technical',business_decision='pending',automatic_approval=False,
                    script_result=dict(status='failed',reason='正文待解析，未转为附件'),
                    ai_result=dict(status='not_started',reason='正文尚未准备完成',stage=''))
                stage('正文待解析',item['reason'])
                return
            from associated_materials import process_associated
            process_associated(processor,source,record,work,item,stage)
            return
        if isinstance(exc,InterruptedError):exc.stop_reason=getattr(processor.stop,'reason','body_user_stop')
        failure=_failure(exc);reason=failure['message']
        waiting_response=item.get('ai_result',{}).get('status')=='response_saved' and last[0] in {'response_saved','postprocessing_started'}
        if entered[0] and not accepted[0] and not waiting_response:
            response_invalid=failure['code']=='preextract_response_validation'
            tracker.observe('complete-md','paper_preextract','validation_failed' if response_invalid else 'error',reason)
            item['ai_result']={'status':'incomplete' if processor.stop.is_set() else 'validation_failed' if response_invalid else
                               'incomplete' if last[0] in {'candidate_received','validating_candidate','saved_model_request_incomplete'} else 'call_failed',
                               'reason':reason,'stage':'完整 MD 提取'}
        ai_failure=entered[0] and not accepted[0] and last[0] in {'extracting_once','extracting_parts','reconciling_parts','reusing_saved_response','candidate_received','validating_candidate','saved_model_request_incomplete'}
        item['script_result']={'status':'completed' if ai_failure else 'stopped' if processor.stop.is_set() else 'failed',
            'reason':'全文与图表材料已保存；请求或候选问题见“AI 处理结果”' if ai_failure else reason}
        if not entered[0] and item.get('prepared_checkpoint') and last[0] in {'material_prepared','waiting_for_model'}:
            item['script_result']={'status':'completed','reason':'全文与图表材料已保存；尚未调用 AI，可继续处理'}
            item['ai_result']={'status':'not_started','reason':reason,'stage':''}
        raise LocalDocumentError(failure['code'],reason) from exc
    finally:
        processor._preextract_in_postprocess=False
        post_scope.close()


def candidate_diagnostics(item,root):
    """Read actual persisted validation; never infer a transport timeout."""
    if not item.get('preextract_work_dir'):return []
    work=Path(item['preextract_work_dir']).resolve()
    if not work.is_relative_to(Path(root).resolve()):return []
    try:
        validation=json.loads((work/'validation.json').read_text(encoding='utf8'))
    except (OSError,ValueError):return []
    labels={'source_reference':'材料引用未能定位','source_mismatch':'来源版本或校验值不一致',
            'source_identity':'来源身份无效','asset_identity':'图表编号不属于实际材料',
            'image_version':'图像版本或尺寸不一致','image_evidence':'未取得对应图像证据',
            'learning_unusable':'未取得可追溯的问题、材料、步骤和产出',
            'routing_code':'投递专业不在实际目录中','routing_missing':'缺少实际投递目标',
            'unknown_field':'有模板之外的字段','candidate_incomplete':'原候选仍有待处理项'}
    return [labels.get(row.get('code'),row.get('message') or '校验问题')+'（'+str(row.get('path') or '/')+'）'
            for row in validation.get('errors',[]) if isinstance(row,dict)]


def candidate_text(item,root):
    work=Path(item.get('preextract_work_dir','')).resolve()
    if not work.is_relative_to(Path(root).resolve()):raise ValueError('候选不属于当前程序资料目录')
    candidate=work/'private'/'论文预提取.md'
    text=candidate.read_text(encoding='utf8') if candidate.exists() else '尚未取得完整候选 MD。'
    validation=work/'validation.json'
    if validation.exists():
        result=json.loads(validation.read_text(encoding='utf8'))
        text+='\n\n程序校验记录：\n'+json.dumps({k:result.get(k) for k in ('valid','errors','warnings')},ensure_ascii=False,indent=2)
    return text
