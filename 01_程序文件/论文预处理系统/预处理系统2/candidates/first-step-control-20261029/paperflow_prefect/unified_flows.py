"""The deployed five-stage entry. Native Prefect owns scheduling and quotas."""
import asyncio
from pathlib import Path
import os
import time
import uuid
from functools import partial

from filelock import FileLock
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.artifacts import create_markdown_artifact
from prefect.runtime import flow_run
from . import flows, legacy
from .records import identity, read_json, sha_file, write_json
from .resources import drain_thread
from .settings import PROJECT, Settings
from .stage3_handoff import read_receipt
from .unified import CONTRACT, UnifiedBackend, execute_unified
from .unified_inputs import receive_inputs


def source_files():
    files={str(p.relative_to(PROJECT)):sha_file(p) for p in sorted((PROJECT/'paperflow_prefect').rglob('*.py'))}
    for name in ('manage.py','config.json','requirements.txt','title_llm_supplement.py'):
        files[name]=sha_file(PROJECT/name)
    return files


LOADED_FILES = source_files()
LOADED_AT = time.time()


class UnifiedIncomplete(flows.PaperIncomplete):
    def __init__(self,result):
        self.business_result=result
        super().__init__('业务未完成：'+','.join(result.get('failures',{})))


def runtime_identity(settings):
    files = source_files()
    if files != LOADED_FILES: raise RuntimeError('运行中代码或配置发生改变，必须重新加载执行器')
    return dict(schema='paperflow-runtime-identity/1',contract=CONTRACT,pid=os.getpid(),
                observed_at=time.time(),loaded_at=LOADED_AT,source_root=str(PROJECT),files=LOADED_FILES,
                source_sha256=identity(LOADED_FILES),disk_matches_loaded=True,
                limits=settings['limits'],catalog_sha256=sha_file(settings.path('catalog_path')))


@task(name='① 分流与逐项接收',persist_result=False,cache_policy=NO_CACHE,retries=0)
async def input_stage(parameters):
    settings=Settings.load()
    async with flows.async_concurrency(flows.LIMIT_PREFIX+'local',occupy=1,strict=True):
        return await drain_thread(partial(receive_inputs,settings,**parameters))


@flow(name='单篇统一五步处理',persist_result=False,retries=0)
async def unified_paper(batch_id, receipt_path, mode='process'):
    batch=flows._batches[batch_id];backend=batch['backend']
    receipt,paper=read_receipt(receipt_path)
    if paper is None:
        return dict(status='incomplete',scope='receipt_only',receipt_id=receipt['receipt_id'],paper_id=None,
                    failures={'intake':'no_accepted_material'})
    # The same lock name and directory as the existing single-paper entry.
    lock=backend.root/'locks'/(identity(str(Path(paper['person_dir']).resolve()).casefold())+'.lock')
    lock.parent.mkdir(parents=True,exist_ok=True)
    identity_lock=backend.root/'locks'/('paper-'+identity(paper['paper_id'])+'.lock')
    with FileLock(lock,timeout=0,thread_local=False),FileLock(identity_lock,timeout=0,thread_local=False):
        paper=await drain_thread(backend.receive,receipt_path)
        batch['papers'][paper['paper_id']]=paper
        async def call(stage,current,row=None):
            return await flows.business_stage.with_options(name=flows.LABELS[stage],
                task_run_name=paper['paper_id'][-10:]+'-'+stage)(batch_id,paper['paper_id'],stage,row['file_id'] if row else '')
        from .records import Records
        stage5=Records(backend.paper_root(paper)).cached('stage5-binding')
        if mode=='resume-stage5' and not stage5: raise ValueError('没有可恢复的第五步绑定')
        result=await execute_unified(paper,receipt,call,resume_rag=mode=='resume-rag',
                                    resume_stage5=bool(stage5))
        result.update(receipt_id=receipt['receipt_id'],contract_version=CONTRACT)
        write_json(backend.paper_root(paper)/'last-run.json',result)
        if result['status']!='complete': raise UnifiedIncomplete(result)
        return result


@flow(name='论文五步流程（已接线）',description='统一H2.1分流接收、正文画像、附件MD、RAG20及NAS2；整篇核验后清理本地。默认检查，正式处理需明确输入。',
      persist_result=False,retries=0,log_prints=False)
async def paperflow_batch(run_mode: str='check',person_folders:list[str]|None=None,max_papers:int=30000,
                         receipt_paths:list[str]|None=None,handoff_paths:list[str]|None=None,
                         verified_new_groups:list[str]|None=None,migrations:dict|None=None,
                         handoff_evidence:dict[str,list[str]]|None=None):
    settings=Settings.load()
    if run_mode=='check':
        result=await asyncio.to_thread(legacy.preflight,settings)
        await asyncio.to_thread(flows.verify_limits,settings)
        if not result['template_exists'] or not result['catalog_exists'] or not all(result['counter_paths'].values()):
            raise ValueError('统一运行依赖尚未就绪')
        result.update(status='connection_check_passed',prefect_limits_checked=True,
                      local_retention='until_whole_paper_rag_and_nas_verified')
        result.update(contract_version=CONTRACT,runtime=runtime_identity(settings),
                      local_cleanup_enabled=settings.values.get('local_cleanup_enabled') is True,
                      supported_modes=['check','process','resume-rag','resume-stage5'])
        write_json(PROJECT/'runtime/unified-check.json',result)
        await create_markdown_artifact(key='paperflow-unified-runtime',markdown=__import__('json').dumps(result,ensure_ascii=False,indent=2),
                                      description='实际执行进程及代码身份；不代表真实整篇业务验收')
        return result
    settings.require_execution()
    runtime=runtime_identity(settings)
    if run_mode not in {'process','resume-rag','resume-stage5'}: raise ValueError('不支持的运行模式')
    if not 1<=max_papers<=30000: raise ValueError('批次上限应在1至30000之间')
    counts=sum(len(x or []) for x in (person_folders,receipt_paths,handoff_paths))
    if not counts or counts>max_papers: raise ValueError('输入不能为空或超过本次max_papers；未扫描历史资料')
    if run_mode!='process' and (person_folders or handoff_paths): raise ValueError('恢复必须提供原接收回执，不能重新分流')
    await asyncio.to_thread(flows.verify_limits,settings)
    inputs=await input_stage(dict(receipt_paths=receipt_paths or [],handoff_paths=handoff_paths or [],
        person_folders=person_folders or [],verified_new_groups=verified_new_groups or [],migrations=migrations,
        handoff_evidence=handoff_evidence,first_step_task_id=flow_run.id))
    backend=UnifiedBackend(settings,invoke=flows.invoke_model,
                           local_scope=lambda:flows.concurrency(flows.LIMIT_PREFIX+'local',occupy=1,strict=True),
                           title_local_scope=lambda:flows.concurrency(flows.LIMIT_PREFIX+'local',occupy=1,strict=True,timeout_seconds=10))
    batch_id=uuid.uuid4().hex
    batch={'backend':backend,'papers':{},'gates':flows.make_gates(settings)}
    flows._batches[batch_id]=batch
    gate=asyncio.Semaphore(settings['max_live_papers']);outcomes=[]
    async def worker(item):
        if not item.get('receipt_path'): outcomes.append(item);return
        async with gate:
            try: result=await unified_paper(batch_id,item['receipt_path'],run_mode)
            except Exception as exc:
                result=dict(status='incomplete',receipt_id=item['receipt_id'],receipt_path=item['receipt_path'],error_type=type(exc).__name__)
                current=getattr(exc,'business_result',None)
                if isinstance(current,dict) and current.get('status')=='incomplete': result.update(current)
                try:
                    _, paper = read_receipt(item['receipt_path'])
                    path = backend.paper_root(paper)/'last-run.json' if paper else None
                    if path and path.is_file() and not current: result['previous_result']=read_json(path)
                except (OSError, ValueError, KeyError, TypeError): pass
            result['receipt_path']=item['receipt_path']
            outcomes.append(result)
    try:
        await asyncio.gather(*(worker(item) for item in inputs))
        result=dict(schema='paperflow-unified-batch/1',batch_id=batch_id,contract_version=CONTRACT,
                    total=len(outcomes),complete=sum(r['status']=='complete' for r in outcomes),papers=outcomes,runtime=runtime)
        path=backend.root/'batches'/(batch_id+'.json');write_json(path,result)
        await create_markdown_artifact(key='paperflow-unified-batch',markdown=__import__('json').dumps(result,ensure_ascii=False,indent=2),
                                      description='统一五步业务结果；按各论文回执判断完成')
        if result['complete']!=result['total']: raise flows.BatchIncomplete(f"{result['total']-result['complete']}项未完成，原结果已保存")
        return result
    finally:flows._batches.pop(batch_id,None)
