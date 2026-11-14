"""One attachment delivery, using the existing manager Job and action protocol."""
import argparse
from pathlib import Path
import sys
import uuid

from .attachment_delivery import AttachmentClient,AttachmentWorkflow,storage_acceptance
from .config import Config
from .control_ipc import WorkerClient
from .delivery_control import DeliveryActions
from .delivery_worker import _controller
from .package import save
from .rag import native_client
from .worker_retry import short_attempts,report_failure


def launch_attachment(manager,config,attachment_id,*,total_timeout=900,schedule_attempt=None):
    _controller(manager,config)
    row=manager.delivery.attachments.ledger.item(attachment_id)
    storage_acceptance(config,row['category'])
    path=manager.root/'delivery-configs'/(uuid.uuid4().hex+'.json')
    path.parent.mkdir(parents=True,exist_ok=True);config.save(path)
    return manager.launch('delivery','attachment:'+attachment_id,
        [sys.executable,'-X','utf8','-B','-m','paperflow.attachment_worker',
         '--config',str(path),'--attachment',attachment_id],
        cwd=Path(__file__).parent.parent,total_timeout=total_timeout,
        schedule_attempt=schedule_attempt,expected_generation=0)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True);parser.add_argument('--attachment',required=True)
    args=parser.parse_args();client=WorkerClient.from_environment()
    try:
        config=Config.load(args.config)
        identity=dict(attachment_id=args.attachment,generation=0)
        actions=DeliveryActions(client,config,identity)
        flow=AttachmentWorkflow(config,actions,AttachmentClient(native_client(config),actions))
        result=short_attempts(client,lambda:flow.run(args.attachment),interval=config.short_retry_interval)
        result_path=config.root/'delivery-results'/(client.execution_id+'.json')
        result_path.parent.mkdir(parents=True,exist_ok=True);save(result_path,result)
        complete=result['row']['state']=='complete'
        client.report_execution('complete' if complete else 'held',dict(state=result['row']['state']))
        return 0 if complete else 2
    except Exception as exc:
        report_failure(client,exc)
        return 1
    finally:client.close()


if __name__=='__main__':raise SystemExit(main())
