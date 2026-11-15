"""Explicitly register the held waiting deployment; never start a worker."""
import argparse
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefect-api', required=True)
    args = parser.parse_args()
    os.environ['PREFECT_API_URL'] = args.prefect_api.rstrip('/')
    from prefect.client.orchestration import SyncPrefectClient
    from prefect.client.schemas.actions import WorkPoolCreate
    from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
    from prefect.types.entrypoint import EntrypointType
    from folder_order_flow import waiting_folder_order
    from prefect_orders import DEPLOYMENT, POOL

    marker = 'paperflow-order-v2-candidate-20261031'

    with SyncPrefectClient(args.prefect_api, httpx_settings={'timeout': 10, 'trust_env': False}) as client:
        try:
            first = client.read_deployment_by_name('论文第一步-任务工单候选/手动运行')
        except ObjectNotFound:
            first = None
        if first is not None and marker not in first.tags:
            raise RuntimeError('同名第一步部署不属于候选，未覆盖')
        try:
            pool = client.read_work_pool(POOL)
        except ObjectNotFound:
            try:
                pool = client.create_work_pool(WorkPoolCreate(name=POOL, type='process', is_paused=True,
                                                               description=marker))
            except ObjectAlreadyExists:
                pool = client.read_work_pool(POOL)
        if not pool.is_paused or pool.type != 'process' or pool.description != marker:
            raise RuntimeError('候选等待工作池不处于预期的暂停状态，未修改该工作池')
        try:
            existing = client.read_deployment_by_name(DEPLOYMENT)
        except ObjectNotFound:
            existing = None
        if existing is not None:
            if existing.work_pool_name != POOL or marker not in existing.tags:
                raise RuntimeError('同名文件夹部署不属于候选，未覆盖')
            print('候选等待部署已存在；工作池保持暂停。')
            return
    os.chdir(Path(__file__).resolve().parent)
    waiting_folder_order.deploy(name='等待前处理候选', work_pool_name=POOL, tags=[marker],
                                build=False, push=False, print_next_steps=False,
                                entrypoint_type=EntrypointType.MODULE_PATH)
    print('已登记候选等待部署；工作池保持暂停，没有启动前处理。')


if __name__ == '__main__':
    main()
