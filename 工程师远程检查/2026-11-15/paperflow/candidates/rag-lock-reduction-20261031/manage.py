"""Manual deployment management. Never enable real processing from this CLI."""
import argparse
import asyncio
import logging
import os
from pathlib import Path
import sys
import time
from paperflow_prefect.flows import LIMIT_PREFIX
from paperflow_prefect.unified_flows import paperflow_batch
from paperflow_prefect.local_preprocessing import (
    DISPATCH_DEPLOYMENT_NAME, PREPROCESSING_CONCURRENCY, PREPROCESSING_LIMIT, PREPROCESSING_POOL,
    WORKER_DEPLOYMENT_NAME,
    auto_preprocessing_dispatch, local_preprocessing_handoff,
)
from paperflow_prefect.settings import Settings, PROJECT
from paperflow_prefect.records import write_json

FLOW_NAME = '论文五步流程（已接线）'
DEPLOYMENT_NAME = '五步接入-手动运行'


def ensure_limits():
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.exceptions import ObjectNotFound
    settings = Settings.load()
    with get_client(sync_client=True) as api:
        limits = {LIMIT_PREFIX + name: limit for name, limit in settings['limits'].items()}
        limits[PREPROCESSING_LIMIT] = PREPROCESSING_CONCURRENCY
        for full, limit in limits.items():
            try: existing = api.read_global_concurrency_limit_by_name(full)
            except ObjectNotFound:
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=full, limit=limit, active=True))
            else:
                if existing.limit != limit or not existing.active or existing.slot_decay_per_second:
                    raise ValueError('已有并发配置不同，请先核对：' + full)


def deployment():
    from prefect.types.entrypoint import EntrypointType
    return paperflow_batch.to_deployment(name=DEPLOYMENT_NAME,
        parameters={'run_mode':'check', 'person_folders':None, 'max_papers':100},
        concurrency_limit=PREPROCESSING_CONCURRENCY, tags=['connected', 'manual-only', 'unified-five-v1'],
        entrypoint_type=EntrypointType.MODULE_PATH)


def preprocessing_deployment():
    from prefect.types.entrypoint import EntrypointType
    return local_preprocessing_handoff.to_deployment(name=WORKER_DEPLOYMENT_NAME, paused=False,
        parameters={'resume_attempt': None, 'task_id': None, 'generation': None,
                    'group_id': None, 'receipt_id': None, 'attempt_id': None, 'folder_label': None},
        concurrency_limit=PREPROCESSING_CONCURRENCY,
        tags=['local-preprocessing', 'task-controlled'],
        work_pool_name=PREPROCESSING_POOL, job_variables={'working_dir': str(PROJECT)},
        entrypoint_type=EntrypointType.MODULE_PATH)


def dispatch_deployment():
    from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
    from prefect.types.entrypoint import EntrypointType
    return auto_preprocessing_dispatch.to_deployment(name=DISPATCH_DEPLOYMENT_NAME,
        interval=60, paused=False,
        concurrency_limit=ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW),
        tags=['local-preprocessing', 'automatic'], entrypoint_type=EntrypointType.MODULE_PATH)


def register():
    ensure_limits()
    dep = deployment()
    deployment_id = dep.apply()
    from prefect.client.orchestration import get_client
    with get_client(sync_client=True) as api:
        actual = api.read_deployment(deployment_id)
        result = {'deployment_id':str(actual.id), 'flow_id':str(actual.flow_id), 'name':actual.name,
            'parameters':actual.parameters, 'entrypoint':actual.entrypoint,
            'schedules':[s.model_dump(mode='json') for s in actual.schedules],
            'tags':actual.tags, 'execution_enabled':Settings.load()['execution_enabled'],
            'prefect_api_url':Settings.load()['prefect_api_url'], 'registered_at':time.time()}
    write_json(PROJECT / 'runtime/deployment-receipt.json', result)
    print(str(deployment_id))


def ensure_folder_pool():
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
    from prefect.exceptions import ObjectNotFound
    from prefect.workers.process import ProcessWorker
    with get_client(sync_client=True) as api:
        try:
            pool = api.read_work_pool(PREPROCESSING_POOL)
        except ObjectNotFound:
            pool = api.create_work_pool(WorkPoolCreate(name=PREPROCESSING_POOL, type='process',
                is_paused=True, concurrency_limit=PREPROCESSING_CONCURRENCY,
                base_job_template=ProcessWorker.get_default_base_job_template()))
        if pool.type != 'process' or pool.concurrency_limit != PREPROCESSING_CONCURRENCY:
            raise ValueError('前处理 Prefect 工作池配置不一致，请先核对')
        if not pool.is_paused:
            api.update_work_pool(PREPROCESSING_POOL, WorkPoolUpdate(is_paused=True))


async def supervise_folder_worker():
    from prefect.workers.process import ProcessWorker

    class WorkerCleanupError(RuntimeError):
        pass

    class SupervisedProcessWorker(ProcessWorker):
        @classmethod
        def __dispatch_key__(cls):
            return 'paperflow-supervised-process'

        async def setup(self):
            try:
                await super().setup()
            except BaseException:
                # Prefect may have entered some async contexts before setup fails.
                try:
                    await self.teardown(None, None, None)
                except BaseException as cleanup_error:
                    raise WorkerCleanupError('前处理 worker 初始化后的清理未完成') from cleanup_error
                raise

    while True:
        worker = SupervisedProcessWorker(work_pool_name=PREPROCESSING_POOL,
                                         name='paperflow-folder-' + str(os.getpid()), limit=PREPROCESSING_CONCURRENCY,
                                         create_pool_if_not_found=False)
        try:
            await worker.start()
            logging.warning('前处理 Prefect worker 已退出，将继续重试')
        except asyncio.CancelledError:
            raise
        except WorkerCleanupError:
            raise
        except Exception:
            logging.exception('前处理 Prefect worker 未运行，将继续重试')
        await asyncio.sleep(30)


async def serve_with_folder_worker(main_deployment, dispatcher_deployment):
    from prefect.runner import Runner
    runner = Runner(limit=PREPROCESSING_CONCURRENCY + 1, name='paperflow-windows-manual',
                    pause_on_shutdown=True)
    await runner.aadd_deployment(main_deployment)
    await runner.aadd_deployment(dispatcher_deployment)
    runner_task = asyncio.create_task(runner.start())
    worker_task = asyncio.create_task(supervise_folder_worker())
    try:
        done, _ = await asyncio.wait((runner_task, worker_task), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            await task
    finally:
        runner_task.cancel()
        worker_task.cancel()
        await asyncio.gather(runner_task, worker_task, return_exceptions=True)


def serve():
    from filelock import FileLock
    import psutil
    runtime = PROJECT / 'runtime'; runtime.mkdir(exist_ok=True)
    with FileLock(runtime / 'runner.lock', timeout=0):
        ensure_limits()
        ensure_folder_pool()
        # The process worker, rather than this serve runner, owns the folder deployment.
        preprocessing_deployment().apply(work_pool_name=PREPROCESSING_POOL)
        write_json(runtime / 'runner-process.json', {'pid':os.getpid(),
            'create_time':psutil.Process().create_time(), 'entrypoint':str(Path(__file__).resolve()),
            'executable':sys.executable})
        main_deployment = deployment()
        dispatcher_deployment = dispatch_deployment()
        asyncio.run(serve_with_folder_worker(main_deployment, dispatcher_deployment))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['register','serve','preflight','check'])
    command = parser.parse_args().command
    if command == 'register': register()
    elif command == 'serve': serve()
    elif command == 'preflight':
        from paperflow_prefect.legacy import preflight
        result = preflight(Settings.load())
        write_json(PROJECT / 'runtime/preflight.json', result)
        print('模块/配置检查完成；未处理真实论文')
    elif command == 'check':
        import asyncio
        asyncio.run(paperflow_batch(run_mode='check'))


if __name__ == '__main__': main()
