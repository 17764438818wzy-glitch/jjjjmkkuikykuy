"""Manual deployment management. Never enable real processing from this CLI."""
import argparse
import os
from pathlib import Path
import sys
import time
from paperflow_prefect.flows import paperflow_batch, LIMIT_PREFIX
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
        for name, limit in settings['limits'].items():
            full = LIMIT_PREFIX + name
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
        concurrency_limit=1, tags=['connected', 'manual-only', 'real-execution-disabled'],
        entrypoint_type=EntrypointType.MODULE_PATH)


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


def serve():
    from filelock import FileLock
    import psutil
    from prefect import serve as prefect_serve
    runtime = PROJECT / 'runtime'; runtime.mkdir(exist_ok=True)
    with FileLock(runtime / 'runner.lock', timeout=0):
        ensure_limits()
        write_json(runtime / 'runner-process.json', {'pid':os.getpid(),
            'create_time':psutil.Process().create_time(), 'entrypoint':str(Path(__file__).resolve()),
            'executable':sys.executable})
        prefect_serve(deployment(), limit=1, name='paperflow-windows-manual', pause_on_shutdown=True)


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
