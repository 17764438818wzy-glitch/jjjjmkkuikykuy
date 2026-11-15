"""Explicit stage Worker entry; importing this file never changes Prefect settings."""
import argparse
import asyncio
import os
import socket
from pathlib import Path


def configure(config_path,role):
    from paperflow_prefect.records import read_json,sha_file
    path=Path(config_path).resolve(); settings=read_json(path); pipeline=settings['pipeline']
    url=pipeline['prefect_api_url']; storage=Path(pipeline['storage_root']).resolve()
    if not url.startswith(('http://','https://')): raise ValueError('explicit Prefect API URL required')
    os.environ.update(PREFECT_API_URL=url,PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false',
        PREFECT_RESULTS_PERSIST_BY_DEFAULT='true',PREFECT_LOGGING_LOG_PRINTS='false',
        PREFECT_LOCAL_STORAGE_PATH=str(storage),PAPERFLOW_PIPELINE_CONFIG=str(path),
        PAPERFLOW_PIPELINE_CONFIG_SHA256=sha_file(path))
    ports=pipeline['worker_status_ports']
    if len(set(ports.values()))!=len(ports) or any(not 1024<=p<=65535 for p in ports.values()):
        raise ValueError('each worker role requires a distinct status port')
    return settings,storage,ports[role]


async def serve(config_path,role):
    settings,storage,port=configure(config_path,role)
    import prefect
    if prefect.__version__!='3.9.0': raise RuntimeError('Prefect 3.9.0 required')
    from prefect.filesystems import LocalFileSystem
    from prefect.task_worker import aserve
    from prefect.settings import temporary_settings,PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK
    from paperflow_prefect.pipeline_tasks import TASKS,WORKERS,no_cache
    from paperflow_prefect.records import write_json,sha_file
    import psutil
    block=LocalFileSystem(basepath=str(storage))
    block_name=settings['pipeline']['parameter_storage_block']
    # Named native parameter storage is shared with submitters; no custom queue.
    await block.save(block_name,overwrite=True)
    tasks=[TASKS[n].with_options(result_storage=block,cache_key_fn=no_cache,persist_result=True,
            cache_result_in_memory=False) for n in WORKERS[role]['tasks']]
    identity=dict(schema='pipeline-worker/1',role=role,instance_id=settings['pipeline']['instance_id'],
        pid=os.getpid(),process_create_time=psutil.Process().create_time(),status_url=f'http://127.0.0.1:{port}/status',
        client_id=f'{socket.gethostname()}-{os.getpid()}',config_sha256=sha_file(config_path),
        task_keys=[t.task_key for t in tasks],limit=WORKERS[role]['limit'])
    # Preserve prior live registrations; one replacement must not hide an old owner.
    write_json(Path(settings['pipeline']['ledger_root'])/'workers'/(role+'-'+str(os.getpid())+'.json'),identity)
    with temporary_settings({PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK:'local-file-system/'+block_name}):
        await aserve(*tasks,limit=WORKERS[role]['limit'],status_server_port=port)


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--config',required=True); parser.add_argument('--role',required=True)
    args=parser.parse_args(); asyncio.run(serve(args.config,args.role))


if __name__=='__main__': main()
