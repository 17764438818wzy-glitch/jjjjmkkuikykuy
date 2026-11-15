"""Run the local-preprocessing worker on the new Prefect."""
import asyncio
import json
import os
from pathlib import Path
import sys
import traceback


MARKER = 'paperflow-order-v2-live-20261031'
# This pool only owns the computer-local preprocessing position. Model
# requests use independent task workers and the existing 50/20 Prefect leases.
FOLDER_CONCURRENCY = 8


def main():
    root = Path(__file__).resolve().parent
    config_path = root / 'config.json'
    config = json.loads(config_path.read_text(encoding='utf-8-sig'))
    api_url = str(config.get('prefect_api_url', '')).rstrip('/')
    if not api_url or '127.0.0.1:4291' in api_url or 'localhost:4291' in api_url:
        raise RuntimeError('统一 Prefect 地址未配置')
    os.environ.update(PAPERFLOW_CONFIG=str(config_path), PREFECT_API_URL=api_url, PYTHONUTF8='1',
                      PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS='false')
    log_path = root / 'runtime' / 'unified-folder-worker.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf8', buffering=1) as log:
        sys.stdout = sys.stderr = log
        try:
            from prefect.client.orchestration import get_client
            from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate, WorkPoolCreate
            from prefect.exceptions import ObjectNotFound
            from prefect.types.entrypoint import EntrypointType
            from prefect.workers.process import ProcessWorker
            from paperflow_prefect.local_preprocessing import (
                PREPROCESSING_CONCURRENCY, PREPROCESSING_LIMIT, PREPROCESSING_POOL,
                WORKER_DEPLOYMENT_NAME, WORKER_FLOW_NAME, local_preprocessing_handoff,
            )

            with get_client(sync_client=True) as client:
                try:
                    limit = client.read_global_concurrency_limit_by_name(PREPROCESSING_LIMIT)
                except ObjectNotFound:
                    client.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(
                        name=PREPROCESSING_LIMIT, limit=PREPROCESSING_CONCURRENCY, active=True))
                    limit = client.read_global_concurrency_limit_by_name(PREPROCESSING_LIMIT)
                if limit.limit != PREPROCESSING_CONCURRENCY or not limit.active or limit.slot_decay_per_second:
                    raise RuntimeError('已有前处理并发名额与原配置不一致')
                try:
                    pool = client.read_work_pool(PREPROCESSING_POOL)
                except ObjectNotFound:
                    pool = client.create_work_pool(WorkPoolCreate(
                        name=PREPROCESSING_POOL, type='process', is_paused=False,
                        description=MARKER, concurrency_limit=FOLDER_CONCURRENCY,
                        base_job_template=ProcessWorker.get_default_base_job_template()))
                if (pool.type != 'process' or pool.description != MARKER or pool.is_paused
                        or pool.concurrency_limit != FOLDER_CONCURRENCY):
                    raise RuntimeError('统一前处理工作池配置不一致')
                try:
                    existing = client.read_deployment_by_name(WORKER_FLOW_NAME + '/' + WORKER_DEPLOYMENT_NAME)
                except ObjectNotFound:
                    existing = None
                if existing is not None and (MARKER not in existing.tags or
                                              existing.work_pool_name != PREPROCESSING_POOL):
                    raise RuntimeError('同名工单部署不属于统一流程')

            deployment = local_preprocessing_handoff.to_deployment(
                name=WORKER_DEPLOYMENT_NAME,
                parameters={'resume_attempt': None, 'task_id': None, 'generation': None,
                            'group_id': None, 'receipt_id': None, 'attempt_id': None, 'folder_label': None},
                concurrency_limit=FOLDER_CONCURRENCY,
                tags=['local-preprocessing', 'task-controlled', MARKER],
                work_pool_name=PREPROCESSING_POOL, job_variables={'working_dir': str(root)},
                entrypoint_type=EntrypointType.MODULE_PATH)
            deployment.apply(work_pool_name=PREPROCESSING_POOL)
            worker = ProcessWorker(work_pool_name=PREPROCESSING_POOL, name='paperflow-unified-folder',
                                   limit=FOLDER_CONCURRENCY, create_pool_if_not_found=False)
            asyncio.run(worker.start())
        except BaseException:
            traceback.print_exc(file=log)
            raise


if __name__ == '__main__':
    main()
