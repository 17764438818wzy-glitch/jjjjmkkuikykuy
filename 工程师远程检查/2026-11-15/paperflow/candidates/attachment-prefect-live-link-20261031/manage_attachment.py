"""Register or serve the independent attachment flows on the configured Prefect API."""
import argparse
import os
from pathlib import Path

os.environ.setdefault('PAPERFLOW_CONFIG', str(Path(__file__).resolve().parents[2]/'prefect-paperflow'/'config.json'))
(Path(__file__).resolve().parent/'runtime').mkdir(parents=True, exist_ok=True)

from paperflow_prefect.attachment_prefect_flows import (attachment_job, attachment_scan,
    JOB_DEPLOYMENT_NAME, SCAN_DEPLOYMENT_NAME, runtime_identity)
from paperflow_prefect.records import write_json
from prefect import serve
from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.types.entrypoint import EntrypointType

PROJECT = Path(__file__).resolve().parent


def deployments(*, automatic=False):
    job = attachment_job.to_deployment(name=JOB_DEPLOYMENT_NAME, parameters={'run_mode': 'check', 'receipt_id': ''},
        concurrency_limit=20, tags=['attachment', 'upstream-only'], entrypoint_type=EntrypointType.MODULE_PATH)
    scan = attachment_scan.to_deployment(name=SCAN_DEPLOYMENT_NAME, interval=30, paused=not automatic,
        parameters={'run_mode': 'process'},
        concurrency_limit=ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW),
        tags=['attachment', 'upstream-only'],
        entrypoint_type=EntrypointType.MODULE_PATH)
    return job, scan


def run_runner(job, scan):
    from filelock import FileLock
    import psutil
    runtime = PROJECT/'runtime'
    runtime.mkdir(parents=True, exist_ok=True)
    with FileLock(runtime/'runner.lock', timeout=0):
        write_json(runtime/'runner-process.json', {'pid': os.getpid(),
            'create_time': psutil.Process().create_time(), 'runtime': runtime_identity()})
        serve(job, scan, limit=21, name='paperflow-attachment-upstream-only', pause_on_shutdown=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('command', choices=('register', 'serve'))
    parser.add_argument('--automatic', action='store_true')
    args=parser.parse_args()
    job, scan=deployments(automatic=args.automatic)
    if args.command == 'register':
        print('attachment_job_deployment', job.apply())
        print('attachment_scan_deployment', scan.apply())
    else:
        run_runner(job, scan)


if __name__ == '__main__':
    main()
