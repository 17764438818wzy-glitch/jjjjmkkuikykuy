"""Remote exits qualify the existing Manager ownership, never a second scheduler."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import threading
import time

from .handoff_store import canonical
from .remote_harness import namespace
from .remote_transport import request,ssh_args,endpoint_identity
from .remote_diagnostics import describe,ExitLog
from .scheduling_policy import ActionDeferred


class RemoteExecutionGuard:
    def __init__(self,manager,settings):
        ssh_args(settings)
        self.manager,self.settings=manager,settings;self.namespace=namespace(manager.store.path)
        self.endpoint=endpoint_identity(settings)
        self.pool=ThreadPoolExecutor(2,thread_name_prefix='paperflow-remote-exit')
        self.closing=threading.Event()
        self.lock=threading.Lock();self.pending={};self.proofs={};self.retry={};self.error=None
        self.last_error=None;self.diagnostics=ExitLog(manager.root/'remote-exit-errors.jsonl')
        manager.ledger._write(lambda c:c.execute('''CREATE TABLE IF NOT EXISTS remote_harness_executions (
            execution_id TEXT PRIMARY KEY REFERENCES control_executions(execution_id),epoch TEXT NOT NULL,
            namespace TEXT NOT NULL,image TEXT NOT NULL,state TEXT NOT NULL,proof TEXT,endpoint TEXT NOT NULL)'''))
        self.observed={}
        from .remote_groups import recover_groups
        recover_groups(self)
    def register_sql(self,c,execution_id,epoch):
        c.execute('INSERT INTO remote_harness_executions VALUES(?,?,?,?,?,NULL,?)',
            (execution_id,epoch,self.namespace,self.settings['image'],'owned',canonical(self.endpoint)))
    def reap_sync(self,execution_id,epoch):
        started=time.monotonic();phase='ledger_read'
        try:
            rows=self.manager.ledger.rows('SELECT * FROM remote_harness_executions WHERE execution_id=?',(execution_id,))
            if not rows:return dict(all_exited=True,execution_closed=True,not_remote=True)
            row=rows[0];phase='owner_validate'
            if row['epoch']!=epoch or row['namespace']!=self.namespace:raise ValueError('remote execution identity mismatch')
            if row['image']!=self.settings['image'] or row['endpoint']!=canonical(self.endpoint):
                raise ValueError('remote endpoint or image changed; original owner must be reconciled')
            if row['state']=='exited':return json.loads(row['proof'])
            phase='remote_request'
            proof=getattr(self,'observed',{}).get((execution_id,epoch))
            groups=getattr(self.manager,'native_groups',None)
            if proof is None and groups:
                proof=groups.exit_proof(execution_id,epoch)
                if proof is False:raise ActionDeferred('remote_child_exit_pending')
            if proof is None:
                actor_rows=self.manager.ledger.rows('SELECT * FROM remote_sdk_actors WHERE execution_id=? AND epoch=?',(execution_id,epoch))
                if actor_rows:
                    if any(v['state']!='closed' for v in actor_rows):raise ValueError('native actor owner not reconciled')
                    proof=dict(execution_id=execution_id,epoch=epoch,all_exited=True,execution_closed=True,
                        at=time.time(),native_children=[dict(actor_id=v['actor_id'],proof=json.loads(v['proof'])) for v in actor_rows])
            if proof is None:
                proof=request(self.settings,dict(op='reap_execution',namespace=self.namespace,execution_id=execution_id,
                    epoch=epoch,job_id='0'*32,image=self.settings['image'],deployment_id=self.settings['deployment_id']),
                    timeout=12,queue_timeout=60,cancelled=getattr(self,'closing',threading.Event()).is_set)
            phase='proof_validate'
            if (proof.get('execution_id')!=execution_id or proof.get('epoch')!=epoch
                    or proof.get('all_exited') is not True or proof.get('execution_closed') is not True):
                raise ValueError('remote owner exit not confirmed')
            phase='proof_persist'
            if hasattr(self,'observed'):self.observed[(execution_id,epoch)]=proof
            self.manager.ledger._write(lambda c:c.execute("UPDATE remote_harness_executions SET state='exited',proof=? WHERE execution_id=?",
                (canonical(proof),execution_id)))
            if hasattr(self,'observed'):self.observed.pop((execution_id,epoch),None)
            return proof
        except ActionDeferred:raise
        except sqlite3.OperationalError as exc:
            if getattr(exc,'sqlite_errorcode',0)&255==sqlite3.SQLITE_BUSY:
                raise ActionDeferred('remote_proof_persistence_busy') from None
            raise
        except Exception as exc:
            self.last_error=dict(at=time.time(),execution_id=execution_id,epoch=epoch,phase=phase,
                elapsed_seconds=time.monotonic()-started,error=describe(exc))
            self.error='remote exit unknown'
            # Closing admission never waits on a database or diagnostic file.
            stopped=getattr(self.manager,'stopped',None)
            if stopped is not None and not stopped.is_set():self.manager.pause('remote Harness exit unconfirmed')
            diagnostics=getattr(self,'diagnostics',None)
            if diagnostics:diagnostics.record(self.last_error)
            raise
    def exit_proof(self,execution_id,epoch):
        with self.lock:
            if self.closing.is_set():return None
            if execution_id in self.proofs:return self.proofs[execution_id]
            pending=self.pending.get(execution_id)
            if pending and pending.done():
                self.pending.pop(execution_id)
                try:
                    proof=pending.result();self.proofs[execution_id]=proof;self.error=None;self.retry.pop(execution_id,None)
                    # The Manager retains the actual evidence; the guard cache is bounded.
                    if len(self.proofs)>self.manager.history_limit:self.proofs.pop(next(iter(self.proofs)))
                    return proof
                except ActionDeferred:
                    self.retry[execution_id]=time.monotonic()+.25
                except Exception:
                    self.error='remote exit unknown';self.retry[execution_id]=time.monotonic()+2
            if execution_id not in self.pending and time.monotonic()>=self.retry.get(execution_id,0):
                self.pending[execution_id]=self.pool.submit(self.reap_sync,execution_id,epoch)
        if self.error and not self.manager.stopped.is_set():self.manager.pause('remote Harness exit unconfirmed')
        return None
    def status(self):
        with self.lock:
            return dict(location='server',host=self.settings['host'],version=self.settings.get('version'),
                pending_exits=len(self.pending),state='unknown' if self.error else 'checking' if self.pending else 'configured',
                last_error=self.last_error,diagnostic_error=self.diagnostics.error,
                native_groups=self.manager.native_groups.status() if getattr(self.manager,'native_groups',None) else None)
    def close(self):
        self.closing.set()
        self.pool.shutdown(wait=False,cancel_futures=True);self.diagnostics.close()
