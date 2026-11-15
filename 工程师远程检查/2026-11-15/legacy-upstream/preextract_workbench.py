"""Adapt the complete MD stages to AutoProcessor; no queue, thread or UI."""
from __future__ import annotations

import copy
import json
import shutil
from contextlib import ExitStack
from pathlib import Path
from urllib.error import HTTPError

from engine import digest
from processing_errors import LocalDocumentError, describe_failure
from preextract_material import file_sha
from preextract_pipeline import DEFAULT_TEMPLATE, default_work_dir, run_single

FORMAT='paper-preextract-prepared/1'
PROGRAM_STAGES={
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
            'timeout':cfg.get('timeout',90),'max_tokens':cfg.get('max_tokens',8192)}


def validate_prepared_material(record, root):
    from ai_resume import _comparison_path
    reference=record['prepared_checkpoint'];path=Path(reference['path']).resolve()
    root=Path(root).resolve()
    # Only a checkpoint belonging to this application's saved work is usable.
    if not _comparison_path(path).is_relative_to(_comparison_path(root)) or path.name!='material.json':
        raise ValueError('脚本中间结果不属于当前程序资料目录')
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
    from preextract_request import RequestCapacityError
    if isinstance(exc,RequestCapacityError):return {'code':'request_capacity','message':str(exc)}
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
    if getattr(processor,'_preextract_in_postprocess',False):
        raise RuntimeError('程序后处理阶段禁止发起模型请求')
    gate=getattr(processor.agent,'llm_gate',None);acquired=False;receipt=None
    ledger=getattr(processor.agent,'ledger',None)
    def ready():
        nonlocal acquired
        if gate:
            while not acquired:
                if processor.stop.is_set():raise InterruptedError('请求前已停止')
                acquired=gate.acquire(timeout=0.1)
        if processor.stop.is_set():raise InterruptedError('请求前已停止')
    def started():
        nonlocal receipt
        if ledger:
            receipt=ledger.begin(connection['model'],'text','paper_preextract',1,processor.config,
                                 processor.agent.billing_context)
    try:
        if float(connection.get('min_start_interval_seconds',0))>0:
            # Take a real in-flight permit only when the shared start budget
            # is ready. Pacing waits must not inflate the measured LLM peak.
            kwargs['on_request_ready']=ready
        else:ready()
        return request_once(body,connection,model_dir,opener=processor.agent.opener,
                            on_request_started=started,**kwargs)
    finally:
        if acquired:gate.release()
        saved=Path(model_dir)/'request-state.json'
        state=json.loads(saved.read_text(encoding='utf8')) if saved.exists() else {}
        if ledger and receipt:
            complete=state.get('status')=='response_complete'
            ledger.finish(receipt,state.get('usage'),'response' if complete else 'error',ok=complete,
                          error_type=state.get('error_type'))
            if processor.agent.on_usage:processor.agent.on_usage()

def process_item(processor,source,record,work,run,item,tracker,stage):
    """One worker body; AutoProcessor retains all scheduling and final saves."""
    item['content_format']='paper_preextract'
    item['previous_ai_reason']=record.get('previous_ai_reason','')
    template=Path(processor.config.get('preextract_template') or DEFAULT_TEMPLATE)
    reference=record.get('prepared_checkpoint') or {}
    if reference.get('format')==FORMAT:
        workspace,_=validate_prepared_material(record,getattr(processor,'_resume_root',processor.store.root))
    else:
        existing=default_work_dir(source,template)
        workspace=existing if (existing/'material.json').exists() or (existing/'model'/'request-state.json').exists() else Path(work)/'preextract'
    item['preextract_work_dir']=str(workspace)
    item['associated_inventory_binding']='historical' if (workspace/'material.json').exists() else 'current'
    last=['preparing'];entered=[False];accepted=[False];reused=[False]
    post_scope=ExitStack()

    def begin_model():
        notify('waiting_for_model')
        slot=getattr(processor,'_preextract_enter_model',None)
        if slot:slot()

    def begin_postprocess():
        slot=getattr(processor,'_preextract_postprocess',None)
        if slot:post_scope.enter_context(slot())
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

    def invoke(body,connection,model_dir,**kwargs):
        return invoke_once(processor,body,connection,model_dir,**kwargs)

    try:
        enter_prepare=getattr(processor,'_preextract_enter_prepare',None)
        if enter_prepare:enter_prepare()
        from material_conversion import preparation_context
        from contextlib import nullcontext
        material_scope=lambda:preparation_context(getattr(processor,'_resume_root',processor.store.root),
            enabled=(processor.config.get('preextract_preparation') or {}).get('office_enabled',True),
            stop=processor.stop,wait_scope=getattr(processor,'_preextract_conversion',nullcontext))
        result=run_single(source,work_dir=workspace,template_path=template,
            **{k:processor.config['preextract_'+k] for k in ('runtime_dir','paperflow_root') if 'preextract_'+k in processor.config},
            stage='package',stop=processor.stop,progress=notify,connection=lambda:model_connection(processor),
            invoke=invoke,max_pages=int(processor.config.get('max_pages',80)),
            associated_document=record,associated_records=getattr(processor,'_material_context',None) or getattr(processor,'_material_inputs',[]),
            before_postprocess=begin_postprocess,before_model=begin_model,preparation_context=material_scope)
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
        if state!='package_verified':raise ValueError('封包尚未验证，不能授予入库资格')
        prepared=json.loads((workspace/'private'/'prepared-publication.json').read_text(encoding='utf8'))
        final=Path(run)/'packages'/item['id'];final.parent.mkdir(exist_ok=True)
        shutil.copytree(result['package_path'],final)
        from preextract_publish import _paperflow
        packet=_paperflow(prepared['paperflow_root']).validate_package(final,prepared['task'],prepared['catalog'],strict_seal=True)
        if packet['manifest_sha256']!=prepared['validation']['manifest_sha256']:raise ValueError('复制封包校验失败')
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
        stage('程序处理完成','封包已保存，按现有任务设置交接入库')
    except Exception as exc:
        if (last[0]=='preparing' and not entered[0] and not processor.stop.is_set()
                and not (workspace/'material.json').exists()
                and (workspace/'private'/('original'+source.suffix.lower())).is_file()):
            # A failed native reader is a program state, never a quality verdict.
            from associated_materials import process_associated
            process_associated(processor,source,record,work,item,stage)
            return
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
