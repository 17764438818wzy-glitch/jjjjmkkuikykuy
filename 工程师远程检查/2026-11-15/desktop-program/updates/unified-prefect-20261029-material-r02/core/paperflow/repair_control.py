"""Repair-run binding and budget commits through the existing graph controller."""
import json
from pathlib import Path
import sys
import uuid

from .package import load,sha
from .phase_cache import CACHE_STAGES
from .repair_store import RepairLedger
from .producer import SyntaxRepairExhausted


def legacy_count(config,store,identity):
    # Existing mirrors refer to the same events: take a maximum, never a sum.
    used=0
    with store.db(timeout=.1) as c:
        tasks=[json.loads(row['data']) for row in c.execute('SELECT data FROM tasks WHERE source_sha256=?',(identity['source_sha256'],))]
    for task in tasks:
        if task.get('source_id')!=identity['source_id']:continue
        path=config.root/'private/delivery-corrections'/(task['task_id']+'.json')
        if path.exists():
            value=load(path)
            if not isinstance(value,dict):raise ValueError('invalid historical correction record')
            if value.get('source_sha256')==identity['source_sha256']:
                count=value.get('count',0)
                if type(count) is not int or count<0:raise ValueError('invalid historical correction count')
                used=max(used,count)
    return used


class RepairControl:
    def __init__(self,processing):
        self.processing=processing;self.manager=processing.manager;self.config=processing.config
        self.ledger=RepairLedger(self.manager.store)

    def task(self,execution_id):
        ex=self.manager.executions[execution_id]
        task=self.manager.store.get(ex.key)
        run_id=getattr(ex,'run_id',None)
        if run_id:
            row=self.ledger.get(run_id)
            if row['task_id']!=ex.key or not row['run_data'] or row['state'] not in ('running','complete'):
                raise ValueError('repair execution binding changed')
            task=json.loads(row['run_data'])
        if not task:raise ValueError('registered graph source required')
        return task

    def budget(self,execution_id,operation,descriptor):
        p=self.processing;m=self.manager;ex,action,ticket=p.guard(execution_id,operation)
        task=self.task(execution_id);legacy=legacy_count(self.config,m.store,task)
        if sha(task['source_path'])!=task['source_sha256']:raise ValueError('original source changed')
        if operation=='correction_reserve':
            if (set(descriptor)!={'stage','fingerprint','ordinal'} or descriptor['stage'] not in (*CACHE_STAGES,'repair')
                or type(descriptor['ordinal']) is not int or not 1<=descriptor['ordinal']<=2
                or not isinstance(descriptor['fingerprint'],str) or len(descriptor['fingerprint'])!=64):
                raise ValueError('invalid correction attempt')
            attempt='syntax:'+json.dumps([task.get('thread_id',task['task_id']),descriptor],sort_keys=True)
        elif descriptor:raise ValueError('invalid correction count request')
        def write(c):
            p._check(c,execution_id,ticket)
            if not c.execute('SELECT 1 FROM sources WHERE source_id=? AND task_id=?',(task['source_id'],task['task_id'])).fetchone():
                raise ValueError('source superseded')
            if operation=='repair_hold':
                if not task.get('repair_request_id'):raise ValueError('repair run required')
                if self.ledger.seed_sql(c,task,legacy)<2:raise ValueError('budget not exhausted')
                c.execute("UPDATE repair_requests SET state='held_budget' WHERE repair_request_id=? AND state='running'",(task['repair_request_id'],))
                return dict(state='held_budget')
            try:
                used=(self.ledger.seed_sql(c,task,legacy) if operation=='correction_count' else
                    self.ledger.reserve_sql(c,task,attempt,'syntax',dict(descriptor,repair_request_id=task.get('repair_request_id')),legacy_count=legacy))
            except SyntaxRepairExhausted:
                # Structured outcome survives RPC exception flattening.
                return dict(exhausted=True)
            return dict(used=used)
        return m.ledger._write(write,still_valid=lambda:m._valid(ticket['snapshot'],execution_id),
            before_commit=lambda:p.qualify(execution_id,action,ticket))


def launch_repair(manager,config,request_id,*,total_timeout=14400,worker_argv=None,schedule_attempt=None):
    from .processing_control import ProcessingCommits
    if manager.processing is None:ProcessingCommits(manager,config)
    if manager.processing.config!=config:raise ValueError('controller graph configuration mismatch')
    control=manager.processing.repairs
    with manager.gate_lock:
        generation=manager.generation
        def valid():return not manager.stopped.is_set() and not manager.closing and manager.generation==generation
        if not valid():raise ValueError('paused')
    def qualify():
        with manager.gate_lock:
            if not valid():raise ValueError('repair launch admission invalidated')
    run=control.ledger._write(lambda c:control.ledger.start_sql(c,request_id),still_valid=valid,before_commit=qualify)
    # Manager still serializes ownership by article task_id, while checkpoints use repair thread_id.
    path=manager.root/'graph-configs'/(uuid.uuid4().hex+'.json');path.parent.mkdir(parents=True,exist_ok=True);config.save(path)
    argv=worker_argv or [sys.executable,'-X','utf8','-B','-m','paperflow.graph_worker','--config',str(path),
        '--ledger',str(manager.store.path),'--task',run['task_id'],'--repair',request_id]
    return manager.launch('graph',run['task_id'],argv,cwd=Path(__file__).parent.parent,
        total_timeout=total_timeout,run_id=request_id,schedule_attempt=schedule_attempt)
