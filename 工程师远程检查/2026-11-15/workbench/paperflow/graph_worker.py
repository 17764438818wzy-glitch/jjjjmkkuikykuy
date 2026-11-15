"""Explicit one-run worker; must be launched inside Manager's Job and IPC epoch."""
import argparse
import json
from pathlib import Path
import sys
import uuid

from .config import Config
from .control_ipc import WorkerClient
from .processing_control import GraphActions, ProcessingCommits, ProcessingStopped
from .processing_graph import ProcessingWorkflow
from .store import Store
from .worker_retry import short_attempts,report_failure


def launch_graph(manager,config,task_id,*,total_timeout=14400,schedule_attempt=None):
    task=manager.store.get(task_id)
    if not task or task.get('delivery_task_id'):raise ValueError('registered initial source required')
    if task.get('kind')=='historical':raise ValueError('historical graph must never resume')
    if manager.processing is None:ProcessingCommits(manager,config)
    elif manager.processing.config!=config:raise ValueError('controller graph configuration mismatch')
    path=manager.root/'graph-configs'/(uuid.uuid4().hex+'.json')
    path.parent.mkdir(parents=True,exist_ok=True);config.save(path)
    return manager.launch('graph',task_id,[sys.executable,'-X','utf8','-B','-m','paperflow.graph_worker',
        '--config',str(path),'--ledger',str(manager.store.path),'--task',task_id],
        cwd=Path(__file__).parent.parent,total_timeout=total_timeout,schedule_attempt=schedule_attempt)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True)
    parser.add_argument('--ledger',required=True)
    parser.add_argument('--task',required=True)
    parser.add_argument('--repair')
    args=parser.parse_args()
    client=WorkerClient.from_environment()
    flow=None
    try:
        config=Config.load(args.config);store=Store(args.ledger)
        actions=GraphActions(client,config)
        flow=ProcessingWorkflow(config,store,actions)
        task=store.get(args.task)
        if args.repair:
            from .repair_store import RepairLedger
            row=RepairLedger(store).get(args.repair)
            if row['task_id']!=args.task or not row['run_data']:raise ValueError('repair source binding mismatch')
            task=json.loads(row['run_data'])
        result=short_attempts(client,lambda:flow.run(task),interval=config.short_retry_interval)
        client.report_execution('complete' if result['stage'] in ('handed_off','attachment_ready') else 'held',dict(stage=result['stage']))
    except ProcessingStopped as exc:
        report_failure(client,exc)
        return 2
    except Exception as exc:
        # Full artifacts remain in private attempts; do not place manuscripts/secrets in UI errors.
        if flow:flow.store.event(args.task,'processing_failed',dict(type=type(exc).__name__))
        report_failure(client,exc)
        return 1
    finally:
        if flow:flow.close()
        client.close()
    return 0


if __name__=='__main__':raise SystemExit(main())
