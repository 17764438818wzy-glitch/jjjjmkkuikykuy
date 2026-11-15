"""One owned cleanup item, with intent, effect and independent readback."""
import json
from pathlib import Path
from urllib.parse import quote

from .cleanup_paths import delete_inventory
from .delivery import DeliveryPublisher
from .maintenance import verify_retired_object
from .package import load
from .rag import RemoteConflict


class CleanupWorkflow:
    def __init__(self,config,ledger,actions,client,item_id):
        self.config,self.ledger,self.actions,self.client,self.item_id=config,ledger,actions,client,item_id
        self.publisher=DeliveryPublisher(client) if client else None

    def check_deployment(self):
        report=load(self.config.delivery_acceptance_report)
        if report.get('passed') is not True or report.get('endpoint')!=self.config.rag_url:
            raise ValueError('cleanup deployment acceptance required')
        if self.client.deployment_identity()!=report.get('identity'):raise ValueError('cleanup deployment identity changed')

    def result(self,outcome,**evidence):return self.actions.commit('result',dict(outcome=outcome,evidence=evidence))

    def run(self):
        item=self.actions.run('local',dict(operation='load'),lambda cancel:self.ledger.item(self.item_id))
        if item['state']=='deleted':return item
        try:
            if item['kind']=='local':
                item=self.actions.commit('begin',{})
                result=self.actions.run('local',dict(operation='delete_local'),lambda cancel:delete_inventory(
                    json.loads(item['resource']),self.ledger.roots(),self.ledger.originals(),cancel))
                return self.result('deleted',**result)
            self.check_deployment()
            resource=json.loads(item['resource']);obj=resource['object'];identity=resource['identity']
            ds,did=obj['dataset_id'],obj['document_id']
            docs=[d for d in self.publisher.documents(ds) if d['id']==did]
            unresolved=self.ledger.unresolved(item['source_id'],item['specialty_code'])
            if unresolved or item['state']=='pending_reconcile':
                # One missing-object read never proves a lost request has ended.
                return self.result('unknown',absent=not docs,error='remote request needs bounded reconciliation')
            if not docs:
                if item['state']!='deleting':raise RemoteConflict('owned document missing before delete intent')
                return self.result('deleted',absent=True,readback='complete dataset inventory')
            revision=obj['identity']['paperflow_revision']
            verify_retired_object(self.publisher,identity,revision,item['specialty_code'],resource['role'],obj,docs,strict_available=True)
            # A previous sent action whose effect has not converged is never blindly reissued.
            sent=self.ledger.rows("SELECT action_id FROM control_actions WHERE kind='cleanup_write' AND state IN ('possible','complete','unresolved','reconciled') "
                "AND json_extract(request,'$.resource.item_id')=?",(self.item_id,))
            if sent:return self.result('unknown',absent=False,error='previous delete has not converged')
            item=self.actions.commit('begin',{})
            self.client.request('DELETE',f'/datasets/{quote(ds,safe="")}/documents',{'ids':[did]})
            if any(d['id']==did for d in self.publisher.documents(ds)):
                return self.result('unknown',absent=False,error='delete readback pending')
            return self.result('deleted',absent=True,readback='complete dataset inventory')
        except Exception as exc:
            try:
                unknown=item['kind']=='remote' and bool(self.ledger.unresolved(item['source_id'],item['specialty_code']))
                self.result('unknown' if unknown else 'error',error=type(exc).__name__)
            except Exception:pass  # A paused/lost manager leaves the saved intent and action facts.
            raise
