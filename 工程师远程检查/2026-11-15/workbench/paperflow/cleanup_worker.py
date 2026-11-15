"""Explicit isolated one-item cleanup launcher; no production scanning or resume."""
import argparse
from pathlib import Path
import sys
import uuid

from .cleanup import CleanupWorkflow
from .cleanup_control import CleanupActions,CleanupClient,CleanupCommits
from .cleanup_store import CleanupLedger
from .config import Config
from .control_ipc import WorkerClient
from .package import load,save
from .rag import native_client
from .store import Store
from .worker_retry import report_failure


def launch_cleanup(manager,config,item_id,*,isolation_root=None,total_timeout=300,schedule_attempt=None):
    if manager.cleanup is None:
        if isolation_root is None:raise ValueError('explicit cleanup isolation root required')
        CleanupCommits(manager,config,isolation_root=isolation_root)
    if manager.cleanup.config!=config:raise ValueError('cleanup configuration mismatch')
    item=manager.cleanup.ledger.item(item_id)
    if item['kind']=='remote':
        if not config.delivery_acceptance_report:raise ValueError('deployment acceptance required')
        report=load(config.delivery_acceptance_report)
        if report.get('passed') is not True or report.get('endpoint')!=config.rag_url:raise ValueError('cleanup deployment mismatch')
    key=manager.cleanup.prepare(item_id)
    path=manager.root/'cleanup-configs'/(uuid.uuid4().hex+'.json');path.parent.mkdir(parents=True,exist_ok=True);config.save(path)
    return manager.launch('cleanup',key,[sys.executable,'-X','utf8','-B','-m','paperflow.cleanup_worker',
        '--config',str(path),'--ledger',str(manager.store.path),'--item',item_id],
        cwd=Path(__file__).parent.parent,total_timeout=total_timeout,schedule_attempt=schedule_attempt,expected_generation=0)


def main():
    parser=argparse.ArgumentParser()
    for name in ('config','ledger','item'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args();client=WorkerClient.from_environment()
    try:
        config=Config.load(args.config);ledger=CleanupLedger(Store(args.ledger),config)
        item=ledger.item(args.item);actions=CleanupActions(client,config,args.item)
        native=CleanupClient(native_client(config),actions) if item['kind']=='remote' else None
        result=CleanupWorkflow(config,ledger,actions,native,args.item).run()
        client.report_execution('complete' if result['state']=='deleted' else 'blocked',dict(state=result['state']))
        return 0 if result['state']=='deleted' else 2
    except Exception as exc:report_failure(client,exc);return 1
    finally:client.close()


if __name__=='__main__':raise SystemExit(main())
