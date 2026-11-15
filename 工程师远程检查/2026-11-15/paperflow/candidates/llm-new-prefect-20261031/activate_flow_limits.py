"""Switch only the new folder pool/deployment limits; never dispatch work."""
import json
import os
from pathlib import Path
import sqlite3
import sys

from prefect.client.orchestration import SyncPrefectClient
from prefect.client.schemas.actions import GlobalConcurrencyLimitUpdate, WorkPoolUpdate
from prefect.client.schemas.filters import (FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType,
                                            WorkPoolFilter, WorkPoolFilterName)
from prefect.client.schemas.objects import StateType


ROOT = Path(__file__).resolve().parents[2]
POOL = 'paperflow-unified-preprocessing'
DEPLOYMENT = '资料文件夹工单/前处理'
MARKER = 'paperflow-order-v2-live-20261031'
CONTROL = ROOT / 'candidates/first-step-control-20261029/runtime/workspace/output/group-task-summaries/summary-index.sqlite3'


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def no_tasks():
    with sqlite3.connect(CONTROL.resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        if db.execute('SELECT COUNT(*) FROM tasks').fetchone()[0] != 0:
            raise ValueError('当前存在任务，未变更执行位置')


def snapshot(client):
    pool = client.read_work_pool(POOL)
    deployment = client.read_deployment_by_name(DEPLOYMENT)
    global_limit = deployment.global_concurrency_limit
    pre = client.read_global_concurrency_limit_by_name('paperflow-connected-preprocessing')
    limits = {name: client.read_global_concurrency_limit_by_name(name) for name in
              ('paperflow-connected-total_llm','paperflow-connected-body_llm','paperflow-connected-attachment_llm')}
    expected = {'paperflow-connected-total_llm':50,'paperflow-connected-body_llm':50,
                'paperflow-connected-attachment_llm':20}
    if (pool.type != 'process' or pool.description != MARKER or deployment.work_pool_name != POOL
            or MARKER not in deployment.tags or not global_limit
            or not global_limit.active or global_limit.slot_decay_per_second or global_limit.active_slots
            or not pre.active or pre.limit != 8 or pre.slot_decay_per_second or pre.active_slots
            or any(not limits[k].active or limits[k].limit != n or limits[k].slot_decay_per_second
                   for k,n in expected.items())):
        raise ValueError('新 Prefect 工单或共享并发状态不可核验')
    pool_filter = WorkPoolFilter(name=WorkPoolFilterName(any_=[POOL]))
    active_filter = FlowRunFilter(state=FlowRunFilterState(type=FlowRunFilterStateType(
        any_=[StateType.SCHEDULED,StateType.PENDING,StateType.RUNNING])))
    if client.count_flow_runs(work_pool_filter=pool_filter, flow_run_filter=active_filter):
        raise ValueError('文件夹工作池仍有未停止的工单')
    return {'pool_id':str(pool.id), 'pool_limit':pool.concurrency_limit, 'pool_paused':pool.is_paused,
            'deployment_id':str(deployment.id), 'deployment_limit_id':str(global_limit.id),
            'deployment_limit_name':global_limit.name, 'deployment_limit':global_limit.limit,
            'preprocessing_limit':pre.limit,
            'shared_limits':{name: row.limit for name,row in limits.items()}}


def update_back(client, saved):
    current = snapshot(client)
    if any(current[key] != saved[key] for key in ('pool_id','deployment_id','deployment_limit_id',
                                                   'deployment_limit_name')):
        raise ValueError('工作池或部署身份已变化，无法自动回退')
    client.update_work_pool(POOL, WorkPoolUpdate(is_paused=True))
    client.update_global_concurrency_limit(saved['deployment_limit_name'],
                                           GlobalConcurrencyLimitUpdate(limit=saved['deployment_limit']))
    client.update_work_pool(POOL, WorkPoolUpdate(concurrency_limit=saved['pool_limit'],
                                                is_paused=saved['pool_paused']))


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in {'prepare','release','rollback','readback'}:
        raise ValueError('invalid operation')
    operation, receipt_path = sys.argv[1], Path(sys.argv[2])
    first = read_json(ROOT / 'candidates/first-step-control-20261029/runtime/workspace-config.json')
    runtime = read_json(ROOT / 'prefect-paperflow/config.json')
    url = str(runtime.get('prefect_api_url','')).rstrip('/')
    if (not url or first.get('first_step_order_model') != 'v2'
            or str(first.get('prefect_api_url','')).rstrip('/') != url
            or '127.0.0.1:4291' in url or 'localhost:4291' in url):
        raise ValueError('指定的新 Prefect 地址不一致')
    no_tasks()
    with SyncPrefectClient(url, httpx_settings={'timeout':5,'trust_env':False}) as client:
        if operation == 'prepare':
            saved = snapshot(client)
            if saved['pool_limit'] != 8 or saved['pool_paused'] or saved['deployment_limit'] != 8:
                raise ValueError('工作池不是预期的原有 8 路状态，未修改')
            with receipt_path.open('x', encoding='utf8') as stream:
                json.dump(saved, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                client.update_work_pool(POOL, WorkPoolUpdate(is_paused=True))
                client.update_global_concurrency_limit(saved['deployment_limit_name'],
                                                       GlobalConcurrencyLimitUpdate(limit=50))
                client.update_work_pool(POOL, WorkPoolUpdate(concurrency_limit=50))
            except BaseException:
                update_back(client, saved)
                raise
            print('folder pool paused; folder/deployment limits updated to 50')
        elif operation == 'release':
            saved, current = read_json(receipt_path), snapshot(client)
            if (any(current[k] != saved[k] for k in ('pool_id','deployment_id','deployment_limit_id'))
                    or not current['pool_paused'] or current['pool_limit'] != 50 or current['deployment_limit'] != 50):
                raise ValueError('工作池切换身份或并发未确认')
            client.update_work_pool(POOL, WorkPoolUpdate(is_paused=False))
            print('folder pool restored to its previous unpaused state')
        elif operation == 'rollback':
            update_back(client, read_json(receipt_path))
            print('previous folder pool/deployment limits restored')
        else:
            print(json.dumps(snapshot(client), ensure_ascii=False))


if __name__ == '__main__':
    main()
