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
    for name in ('paperflow_prefect/body_segment_vendor/deepseek-v41-tokenizer.json',
                 'paperflow_prefect/body_segment_vendor/python/tokenizers/tokenizers.pyd'):
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
    registered = None
    if Path(receipt['library_root']).resolve() != backend.settings.path('library_root'):
        from .registered_input import verify_registered_receipt
        from .attachment_queue import settings_for_verified_receipt
        registered = await drain_thread(verify_registered_receipt, receipt_path, receipt['receipt_id'])
        if registered['paper'] != paper:
            raise ValueError('回执论文内容与等待池复核不一致')
        scoped = settings_for_verified_receipt(backend.settings, registered)
        backend = await drain_thread(partial(UnifiedBackend, scoped, invoke=flows.invoke_model,
            local_scope=lambda:flows.concurrency(flows.LIMIT_PREFIX+'local',occupy=1,strict=True),
            title_local_scope=flows.title_local_concurrency))
        batch.setdefault('paper_backends', {})[paper['paper_id']] = backend
    # The same lock name and directory as the existing single-paper entry.
    lock=backend.root/'locks'/(identity(str(Path(paper['person_dir']).resolve()).casefold())+'.lock')
    lock.parent.mkdir(parents=True,exist_ok=True)
    identity_lock=backend.root/'locks'/('paper-'+identity(paper['paper_id'])+'.lock')
    with FileLock(lock,timeout=0,thread_local=False),FileLock(identity_lock,timeout=0,thread_local=False):
        if registered is not None:
            current = await drain_thread(verify_registered_receipt, receipt_path, receipt['receipt_id'])
            if current['paper'] != paper or current['receipt_sha256'] != registered['receipt_sha256']:
                raise ValueError('锁定期间登记内容发生变化')
        paper=await drain_thread(backend.receive,receipt_path)
        if registered is not None and paper != current['paper']:
            raise ValueError('锁定期间接收内容发生变化')
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


@flow(name='论文五步流程（已接线）',description='从等待前处理队列领取并完成本地准备，进入等待 LLM 队列后停止。',
      persist_result=False,retries=0,log_prints=False)
async def paperflow_batch(run_mode: str='check',person_folders:list[str]|None=None,max_papers:int=100,
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
                      supported_modes=['check','process'])
        write_json(PROJECT/'runtime/unified-check.json',result)
        await create_markdown_artifact(key='paperflow-unified-runtime',markdown=__import__('json').dumps(result,ensure_ascii=False,indent=2),
                                      description='实际执行进程及代码身份；不代表真实整篇业务验收')
        return result
    if run_mode != 'process':
        raise ValueError('主流程当前在等待 LLM 队列停止，不执行后续步骤')
    if any((person_folders, receipt_paths, handoff_paths, verified_new_groups, migrations, handoff_evidence)):
        raise ValueError('请先把资料登记到等待前处理队列，再由主流程领取')
    # The manual entry submits the same folder-specific root order as the
    # dispatcher. The order itself stops at the LLM waiting pool.
    from .local_preprocessing import submit_one_from_queue
    return await submit_one_from_queue()
