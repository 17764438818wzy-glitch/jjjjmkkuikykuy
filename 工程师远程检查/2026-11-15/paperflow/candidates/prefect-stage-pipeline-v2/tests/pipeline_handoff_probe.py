"""Fresh-process native TaskWorker probe with explicit loopback-only side effects."""
import asyncio
import os
from pathlib import Path
import socket
import sys
import time
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
from paperflow_prefect.records import read_json,write_json,sha_file


def main():
    mode,config,*args=sys.argv[1:]
    config=Path(config); settings=read_json(config); pipeline=settings['pipeline']
    os.environ.update(PAPERFLOW_PIPELINE_CONFIG=str(config),PAPERFLOW_PIPELINE_CONFIG_SHA256=sha_file(config))
    # The existing lab guard only permits API+fixture ports. Admit this process's
    # explicitly configured native status server too, while preserving host denial.
    from pipeline_lab import _deny_external_network,_http
    _deny_external_network(extra_loopback_ports=tuple(pipeline['worker_status_ports'].values()))
    import psutil
    from paperflow_prefect.pipeline_contracts import StageEnvelope,StageResult
    from paperflow_prefect.pipeline_dispatch import configured_coordinator,NativeGateway
    original_submit=NativeGateway.submit
    def diagnostic_submit(self,env):
        try: return original_submit(self,env)
        except Exception:
            import traceback
            traceback.print_exc()
            raise
    NativeGateway.submit=diagnostic_submit
    from paperflow_prefect.pipeline_runtime import artifact
    from paperflow_prefect.pipeline_ledger import StageLedger
    root=Path(pipeline['ledger_root']).parent
    if mode=='worker':
        from prefect.task_worker import aserve
        from prefect.filesystems import LocalFileSystem
        from prefect.settings import temporary_settings,PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK
        import paperflow_prefect.pipeline_tasks as module
        async def synthetic(envelope):
            if isinstance(envelope,dict): envelope=StageEnvelope.from_dict(envelope)
            ledger=StageLedger(Path(pipeline['ledger_root']))
            from prefect.runtime import task_run
            owner=dict(instance_id=pipeline['instance_id'],pid=os.getpid(),
                process_create_time=psutil.Process().create_time(),task_run_id=task_run.id)
            with ledger.claim(envelope,owner) as claim:
                if not claim['execute']: return StageResult.from_dict(claim['result'])
                block=root/(envelope.batch_id+'-block')
                if envelope.stage.value=='intake' and block.exists():
                    write_json(root/(envelope.batch_id+'-claimed.json'),owner)
                    while block.exists(): await asyncio.sleep(.1)
                skip=envelope.stage.value in {'attachment','rag_attachment'} and envelope.item_id is None
                if not skip:
                    # One explicit fake endpoint; no native model/RAG/NAS adapter imported.
                    await asyncio.to_thread(_http,os.environ['PIPELINE_FAKE_URL'], 'POST',
                        dict(reference=envelope.operation_key,stage=envelope.stage.value,batch=envelope.batch_id))
                evidence=root/'evidence'/envelope.operation_key/('material.json' if envelope.stage.value=='prepare' else 'receipt.json')
                write_json(evidence,dict(operation_key=envelope.operation_key,pid=os.getpid(),
                    reason='empty_inventory' if skip else 'loopback-fixture'))
                state=('skipped' if skip else 'passed' if envelope.stage.value in {'profile','attachment'}
                    else 'complete' if envelope.stage.value in {'intake','prepare','package'} else 'verified')
                result=StageResult(envelope.operation_key,state,(artifact(evidence),),())
                ledger.commit_result(envelope,result)
                # Real process-death breakpoint AFTER durable predecessor result.
                crash=root/(envelope.batch_id+'-crash-after-result')
                if envelope.stage.value=='prepare' and crash.exists():
                    # Isolate result->successor crash from a SECOND concurrent
                    # accepted/unsent-uncertain attachment handoff. The prepare
                    # operation claim stays held, so no profile can start yet.
                    coordinator=configured_coordinator(); deadline=time.monotonic()+30
                    while time.monotonic()<deadline:
                        siblings=[e for e in coordinator.envelopes(envelope.batch_id)
                            if e.stage.value in {'intake','attachment'}]
                        terminal=[]
                        for sibling in siblings:
                            intent=coordinator.intent(sibling)
                            if not intent or not intent['task_run_ids']: break
                            run=await asyncio.to_thread(_http,pipeline['prefect_api_url']+'/task_runs/'+intent['task_run_ids'][0])
                            terminal.append(run['state_type']=='COMPLETED')
                        if len(terminal)==2 and all(terminal): os._exit(29)
                        await asyncio.sleep(.1)
                    raise TimeoutError('fixture siblings did not finish before isolated result crash')
                return result
        module.execute_stage=synthetic
        async def serve():
            block=LocalFileSystem(basepath=pipeline['storage_root'])
            await block.save(pipeline['parameter_storage_block'],overwrite=True)
            tasks=[task.with_options(result_storage=block,cache_key_fn=module.no_cache,
                persist_result=True,cache_result_in_memory=False) for key,task in module.TASKS.items() if key!='model_recovery']
            who=dict(schema='pipeline-worker/1',role='fixture',instance_id=pipeline['instance_id'],
                pid=os.getpid(),process_create_time=psutil.Process().create_time(),
                client_id=f'{socket.gethostname()}-{os.getpid()}',config_sha256=sha_file(config),
                status_url=f"http://127.0.0.1:{pipeline['worker_status_ports']['fixture']}/status",
                task_keys=[t.task_key for t in tasks],limit=4)
            write_json(Path(pipeline['ledger_root'])/'workers'/(str(os.getpid())+'.json'),who)
            write_json(root/('worker-'+args[0]+'.json'),who)
            with temporary_settings({PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK:'local-file-system/'+pipeline['parameter_storage_block']}):
                await aserve(*tasks,limit=4,status_server_port=pipeline['worker_status_ports']['fixture'])
        asyncio.run(serve())
    elif mode=='flow-submit':
        from paperflow_prefect.pipeline_flows import submit_batch
        result=submit_batch(args[0],args[1]); write_json(args[2],dict(result,submitter_pid=os.getpid()))
    elif mode in {'submit','submit-crash'}:
        coordinator=configured_coordinator()
        if mode=='submit-crash':
            original=coordinator.gateway.submit
            def accepted_crash(env):
                original(env); os._exit(27)
            coordinator.gateway.submit=accepted_crash
        coordinator.register(args[0],args[1]); result=coordinator.reconcile_now(read_json(args[0])['batch_id'])
        write_json(args[2],dict(result,submitter_pid=os.getpid()))
    elif mode=='reconcile':
        coordinator=configured_coordinator(); result=coordinator.reconcile_now(args[0])
        write_json(args[1],dict(result,reconciler_pid=os.getpid()))
    elif mode=='duplicate':
        coordinator=configured_coordinator(); env=StageEnvelope.from_dict(read_json(args[0]))
        write_json(args[1],dict(task_run_id=coordinator.gateway.submit(env)))
    else: raise ValueError(mode)


if __name__=='__main__': main()
