"""One-off operator recovery of an existing batch. Never starts a manager or work.

Run only after stopping the paused backend. Hold its existing controller lock,
keep the same tasks/jobs, and retain all artifacts, attempts and retry budgets.
"""
import argparse
import copy
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

from paperflow.config import Config
from paperflow.instance_lock import InstanceLock
from paperflow.package import load,save,sha,verify_delivery_identity,validate_package
from paperflow.phase_cache import CACHE_STAGES,PREVIOUS,cache_path,file_hashes,input_fingerprint,read_cache,read_prepared_cache
from paperflow.processing_graph import ProcessingWorkflow
from paperflow.store import Store


class NoBusinessWork:
    def run(self,*args,**kwargs):
        raise AssertionError('paused recovery must not execute business work')

    def run_stage(self,*args,**kwargs):
        raise AssertionError('paused recovery must not call a model')


def table_hash(db,table):
    rows=sorted(json.dumps(list(r),ensure_ascii=False,sort_keys=True,default=lambda b:b.hex())
                for r in db.execute('SELECT * FROM '+table))
    return hashlib.sha256('\n'.join(rows).encode()).hexdigest()


PROTECTED_TABLES=('sources','source_versions','receipts','web_batches','web_intakes','web_intake_items',
    'correction_budgets','correction_attempts','defective_cache','scheduler_attempts','scheduler_rounds',
    'delivery_packages','publication_intents','specialty_tasks','specialty_active','target_versions',
    'attachment_tasks','repair_requests','repair_reports','repair_targets')


def prepare(config,batch_id,protected_ids,evidence):
    db_path=config.root/'state.sqlite3'
    evidence=Path(evidence);evidence.mkdir(parents=True,exist_ok=False)
    with InstanceLock(db_path.with_suffix(db_path.suffix+'.controller.lock')):
        with sqlite3.connect(db_path) as db:
            db.row_factory=sqlite3.Row
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            state=dict(db.execute('SELECT * FROM controller_state WHERE id=1').fetchone())
            if not state['paused']:raise ValueError('manager must already be paused')
            if db.execute("SELECT 1 FROM control_executions WHERE state!='exited'").fetchone():
                raise ValueError('owned process exits must be reconciled')
            if config.remote_harness and 'remote_harness_executions' not in tables:
                raise ValueError('remote execution ownership table missing')
            if ('remote_harness_executions' in tables
                    and db.execute("SELECT 1 FROM remote_harness_executions WHERE state!='exited'").fetchone()):
                raise ValueError('remote process exits must be reconciled')
            if db.execute("SELECT 1 FROM control_actions WHERE kind IN ('rag_write','cleanup_write') "
                          "AND state IN ('possible','unknown','unresolved')").fetchone():
                raise ValueError('unresolved writes must keep their barriers')
            batch=db.execute('SELECT * FROM web_batches ORDER BY created DESC LIMIT 1').fetchone()
            if not batch or batch['id']!=batch_id:raise ValueError('current batch identity changed')
            task_ids=set(json.loads(batch['task_ids']));protected_ids=set(protected_ids)
            if not protected_ids.issubset(task_ids):raise ValueError('protected tasks outside batch')
            protected_tasks={key:tuple(db.execute('SELECT * FROM tasks WHERE task_id=?',(key,)).fetchone())
                             for key in protected_ids}
            if any(db.execute('SELECT stage FROM tasks WHERE task_id=?',(key,)).fetchone()[0]!='handed_off'
                   for key in protected_ids):raise ValueError('protected completed task changed')
            unchanged={table:table_hash(db,table) for table in PROTECTED_TABLES if table in tables}
            jobs=[];classification=[]
            for key in sorted(task_ids-protected_ids):
                row=db.execute('SELECT * FROM tasks WHERE task_id=?',(key,)).fetchone()
                if not row:raise ValueError('missing selected task')
                task=json.loads(row['data'])
                if not db.execute('SELECT 1 FROM sources WHERE task_id=?',(key,)).fetchone():
                    raise ValueError('selected task is no longer current')
                if row['stage']=='held' and task.get('hold_reason')=='classification_pending':classification.append(key)
                elif row['stage'] not in CACHE_STAGES:raise ValueError('unsupported unfinished stage: '+row['stage'])
                own=db.execute("SELECT * FROM scheduler_jobs WHERE kind='graph' AND unit_key=?",(key,)).fetchall()
                if len(own)!=1 or own[0]['state'] not in ('held','retry','queued','blocked_service'):
                    raise ValueError('only existing unfinished graph jobs can be retried')
                job=dict(own[0])
                if db.execute("SELECT 1 FROM scheduler_attempts WHERE job_id=? AND state!='finished'",
                              (job['job_id'],)).fetchone():raise ValueError('unfinished scheduler attempt')
                if db.execute('SELECT 1 FROM delivery_packages WHERE task_id=?',(key,)).fetchone():
                    raise ValueError('cannot reprocess a sealed source')
                jobs.append(job)
            target=sqlite3.connect(evidence/'state-before.sqlite3');db.backup(target);target.close()
            with sqlite3.connect(config.root/'checkpoints'/'processing.sqlite3') as source:
                target=sqlite3.connect(evidence/'processing-before.sqlite3');source.backup(target);target.close()
            save(evidence/'plan.json',dict(batch_id=batch_id,protected=sorted(protected_ids),
                retry_tasks=[j['unit_key'] for j in jobs],classification=classification,controller=state,
                unchanged_tables=unchanged))

        # A classification hold is a native terminal checkpoint. Use the same
        # verified-prefix/native-control mechanism as normal resume; changing a
        # tasks.stage mirror alone would immediately hold again.
        store=Store(db_path,config.contract_version)
        flow=None
        try:
            if classification:flow=ProcessingWorkflow(config,store,NoBusinessWork(),producer=NoBusinessWork())
            for key in classification:
                registered=store.get(key);graph_config=flow.graph_config(registered)
                snapshot=flow.graph.get_state(graph_config)
                task=snapshot.values.get('task',{})
                if (snapshot.next or task.get('stage')!='held' or task.get('hold_reason')!='classification_pending'
                        or task.get('task_id')!=key):raise ValueError('classification checkpoint changed')
                failed=task.get('stage_outputs',{}).get('round2')
                if not failed or not failed.get('input_sha256'):raise ValueError('classification cache identity missing')
                marker=cache_path(config,registered,'round2',failed['input_sha256'])
                prior=load(marker)
                save(evidence/('round2-marker-'+key+'.json'),prior)
                # Retain the old bytes but stop reusing this empty classification.
                # A normal successful publish can replace this marker at the same
                # input key; no permanent defective_cache entry blocks new output.
                save(marker,dict(prior,complete=False,operator_retry_reason='classification_pending'))
                prefix=flow.reusable_prefix(registered,task)
                if not prefix or prefix.get('stage')!='round2' or 'round1' not in prefix.get('stage_outputs',{}):
                    raise ValueError('verified first round required for classification retry')
                flow.graph.update_state(graph_config,dict(task=prefix),as_node='control')
                resumed=flow.graph.get_state(graph_config)
                if resumed.next!=('round2',):raise ValueError('classification retry checkpoint not prepared')
                store.update(key,stage='round2',hold_reason=None)
        finally:
            if flow:flow.close()

        with store.db() as db:
            db.execute('BEGIN IMMEDIATE')
            current=dict(db.execute('SELECT * FROM controller_state WHERE id=1').fetchone())
            if current!=state:raise ValueError('control state changed during offline recovery')
            for table,digest in unchanged.items():
                if table_hash(db,table)!=digest:raise ValueError('protected table changed: '+table)
            for key,prior in protected_tasks.items():
                if tuple(db.execute('SELECT * FROM tasks WHERE task_id=?',(key,)).fetchone())!=prior:
                    raise ValueError('completed task changed')
            for job in jobs:
                changed=db.execute("UPDATE scheduler_jobs SET state='retry',due=0 WHERE job_id=? AND state=?",
                                   (job['job_id'],job['state'])).rowcount
                if changed!=1:raise ValueError('job changed during recovery')
                db.execute('INSERT INTO events(at,task_id,kind,data) VALUES(?,?,?,?)',
                           (time.time(),job['unit_key'],'operator_graph_retry',json.dumps(
                               dict(batch_id=batch_id,previous_state=job['state'],preserve_outputs=True,
                                    classification_retry=job['unit_key'] in classification))))
        result=dict(batch_id=batch_id,protected_completed=len(protected_ids),retry_jobs=len(jobs),
                    classification_retry=len(classification),paused=True,business_work_started=False,
                    unchanged_tables=list(unchanged))
        save(evidence/'result.json',result)
        return result


def _selected_state(db,config,batch_id,task_ids):
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    state=db.execute('SELECT * FROM controller_state WHERE id=1').fetchone()
    if not state or state['paused']!=1:raise ValueError('manager must already be paused')
    if db.execute("SELECT 1 FROM control_executions WHERE state!='exited'").fetchone():
        raise ValueError('owned process exits must be reconciled')
    if config.remote_harness and 'remote_harness_executions' not in tables:
        raise ValueError('remote execution ownership table missing')
    if ('remote_harness_executions' in tables
            and db.execute("SELECT 1 FROM remote_harness_executions WHERE state!='exited'").fetchone()):
        raise ValueError('remote process exits must be reconciled')
    for table,terminal in (('remote_sdk_groups','exited'),('remote_sdk_actors','closed')):
        if config.remote_harness_groups and table not in tables:
            raise ValueError('SDK ownership table missing')
        if table in tables and db.execute('SELECT 1 FROM '+table+' WHERE state!=?',(terminal,)).fetchone():
            raise ValueError('SDK owner exits must be reconciled')
    batch=db.execute('SELECT * FROM web_batches ORDER BY created DESC LIMIT 1').fetchone()
    if not batch or batch['id']!=batch_id:raise ValueError('current batch identity changed')
    if not task_ids or not task_ids.issubset(set(json.loads(batch['task_ids']))):
        raise ValueError('selected tasks must belong to the current batch')
    tasks=[];jobs=[]
    for key in sorted(task_ids):
        row=db.execute('SELECT * FROM tasks WHERE task_id=?',(key,)).fetchone()
        if row is None:raise ValueError('missing selected task')
        task=Store.decode(row)
        if (task['stage'] not in (*CACHE_STAGES,'repair','handoff')
                or task.get('hold_reason')=='classification_pending'):
            raise ValueError('unsupported selected stage')
        if not db.execute('SELECT 1 FROM sources s JOIN source_versions v '
                          'ON v.path=s.path AND v.task_id=s.task_id WHERE s.task_id=? AND s.source_id=?',
                          (key,task['source_id'])).fetchone():
            raise ValueError('selected source is no longer current')
        if type(task.get('revision')) is not int or task['revision']<1:
            raise ValueError('selected revision missing')
        if row['source_sha256']!=task['source_sha256'] or sha(task['source_path'])!=task['source_sha256']:
            raise ValueError('original source changed')
        own=db.execute("SELECT * FROM scheduler_jobs WHERE kind='graph' AND unit_key=?",(key,)).fetchall()
        if len(own)!=1 or own[0]['state']!='held':
            raise ValueError('only existing held graph jobs can be retried')
        job=dict(own[0])
        if job['article']!=task['source_id'] or json.loads(job['data']).get('task_id')!=key:
            raise ValueError('selected job identity mismatch')
        if db.execute("SELECT 1 FROM scheduler_attempts WHERE job_id=? AND state!='finished'",
                      (job['job_id'],)).fetchone():raise ValueError('unfinished scheduler attempt')
        if db.execute('SELECT 1 FROM delivery_packages WHERE task_id=?',(key,)).fetchone():
            raise ValueError('cannot reprocess a registered delivery')
        tasks.append(task);jobs.append(job)
    barriers=[dict(r) for r in db.execute("SELECT * FROM control_actions WHERE kind IN ('rag_write','cleanup_write') "
                                         "AND state IN ('possible','unknown','unresolved')")]
    for row in barriers:
        try:identity=json.loads(row['request'])['resource']['identity']
        except (ValueError,KeyError,TypeError):raise ValueError('unresolved write identity unavailable')
        keys=('task_id','source_id','source_sha256')
        if not isinstance(identity,dict) or any(not isinstance(identity.get(k),str) or not identity[k] for k in keys):
            raise ValueError('unresolved write identity unavailable')
        if any(any(identity[k]==task[k] for k in keys) for task in tasks):
            raise ValueError('selected source has an unresolved write barrier')
    return tasks,jobs,barriers,tables


def _completed_candidates(config,task,stage):
    """Read bounded actor history; latest failure does not erase older results."""
    with sqlite3.connect((config.root/'state.sqlite3').as_uri()+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA temp_store=MEMORY')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='remote_sdk_actors'").fetchone():
            raise ValueError('no verified current phase result')
        rows=db.execute('SELECT a.* FROM remote_sdk_actors a JOIN control_executions e '
                        'ON e.execution_id=a.execution_id WHERE e.unit_kind=? AND e.unit_key=? '
                        'ORDER BY a.rowid DESC',('graph',task['task_id'])).fetchall()
        candidates=[]
        for row in rows:
            actor=dict(row);retained=json.loads(actor['proof'] or '{}').get('retained_result') or {}
            if Path(retained.get('output','')).name!=stage:continue
            action=db.execute('SELECT * FROM control_actions WHERE action_id=?',(retained.get('action_id'),)).fetchone()
            execution=db.execute('SELECT * FROM control_executions WHERE execution_id=?',(actor['execution_id'],)).fetchone()
            if action is None or execution is None:raise ValueError('retained action owner missing')
            candidates.append((actor,dict(action),dict(execution)))
            if len(candidates)>64:raise ValueError('retained result history lookup limit exceeded')
    if not candidates:raise ValueError('no verified current phase result')
    return candidates


def _recover_completed_result(config,task,stage,*,admission=None,pending_preparations=None):
    """Use an exact owned result; never replay a model to clear an old hold."""
    import uuid
    from paperflow.held_results import validate_completed_output
    from paperflow.phase_cache import cache_paths_retained,prepare_cache
    rejected=[]
    for actor,action,execution in _completed_candidates(config,task,stage):
        retained=json.loads(actor['proof'])['retained_result']
        # A deleting path is an active protection barrier, not a bad model
        # result to skip in favour of another candidate.
        cache_paths_retained(config,dict(result=dict(draft_dir=retained['output'],reading_dir=task.get('reading_dir'))))
        destination=config.root/'work'/task['task_id']/'attempts'/uuid.uuid4().hex/stage
        try:
            result,proof=validate_completed_output(config,task,stage,actor,action,execution,destination,
                                                  admission=admission)
        except (ValueError,OSError) as exc:
            rejected.append(dict(action_id=action['action_id'],error=type(exc).__name__,reason=str(exc)))
            continue
        save(destination.parent/'recovery-evidence.json',dict(proof,earlier_rejections=rejected))
        # This is a prepared, hash-bound result only. Normal resume still
        # performs the existing manager-qualified commit and following phases.
        fingerprint=input_fingerprint(config,task,stage)
        if pending_preparations is not None:
            pending_preparations.append((task,stage,fingerprint,result,file_hashes(result['draft_dir'])))
            return result
        prepare_cache(config,task,stage,fingerprint,result)
        return read_prepared_cache(config,task,stage,fingerprint)
    raise ValueError('no verified current phase result: '+rejected[-1]['reason'])


def _selected_result(config,registered,snapshot,*,recover_completed=False,admission=None,pending_preparations=None):
    task=snapshot.values.get('task',{})
    stage=registered['stage']
    if tuple(snapshot.next)!=(stage,) or task.get('stage')!=stage:
        raise ValueError('current native checkpoint stage mismatch')
    if any(task.get(k)!=registered.get(k) for k in
           ('task_id','source_id','source_sha256','source_path','revision','contract_version')):
        raise ValueError('native checkpoint identity mismatch')
    phases=ProcessingWorkflow.phases(task)
    if stage not in (*phases,'handoff'):raise ValueError('selected phase is not in current lineage')
    base=dict(registered) if registered.get('repair_request_id') else {k:registered[k] for k in
        ('task_id','source_id','source_sha256','source_path','revision','contract_version',
         'display_name','article_title','original_display_name') if k in registered}
    base.update(stage=phases[0],stage_outputs={})
    predecessors=phases if stage=='handoff' else phases[:phases.index(stage)]
    for prior in predecessors:
        output=task.get('stage_outputs',{}).get(prior,{})
        fingerprint=input_fingerprint(config,base,prior)
        cached=read_cache(config,base,prior,fingerprint)
        if (cached is None or fingerprint!=output.get('input_sha256')
                or cached['draft_dir']!=output.get('directory')):
            raise ValueError('checkpoint predecessor cache lineage invalid')
        base.update(cached);base['stage_outputs'][prior]=output
    if stage=='handoff':
        for key in ('package_dir','manifest_sha256'):
            if not task.get(key) or task[key]!=registered.get(key):
                raise ValueError('handoff checkpoint package identity mismatch')
        manifest,digest,_,_=verify_delivery_identity(task['package_dir'],task,strict_seal=True)
        if (digest!=task['manifest_sha256'] or manifest.get('source_id')!=task['source_id']
                or manifest.get('revision')!='r'+str(task['revision']).zfill(3)):
            raise ValueError('handoff seal differs from current source revision')
        packet=validate_package(task['package_dir'],task,config.catalog,strict_seal=True)
        if not packet['routes'] or packet['pending_routes']:
            raise ValueError('handoff routes remain unresolved')
        return dict(stage=stage,result_kind='sealed_handoff',package_dir=task['package_dir'],
                    manifest_sha256=digest,checkpoint_id=snapshot.config['configurable'].get('checkpoint_id'))
    previous=task.get('stage_outputs',{}).get(PREVIOUS.get(stage),{}).get('directory')
    current=dict(task,draft_dir=previous) if previous and task.get('repaired_stage')!=stage else task
    fingerprint=input_fingerprint(config,current,stage)
    if input_fingerprint(config,base,stage)!=fingerprint:
        raise ValueError('checkpoint input differs from verified predecessor lineage')
    cached=read_cache(config,current,stage,fingerprint)
    kind='published'
    if cached is None:
        cached=read_prepared_cache(config,current,stage,fingerprint);kind='prepared'
    if cached is None and recover_completed:
        cached=_recover_completed_result(config,current,stage,admission=admission,
                                         pending_preparations=pending_preparations);kind='prepared'
    if cached is None:raise ValueError('no verified current phase result')
    return dict(stage=stage,fingerprint=fingerprint,result_kind=kind,
                draft_dir=cached['draft_dir'],checkpoint_id=snapshot.config['configurable'].get('checkpoint_id'))


def prepare_selected(config,batch_id,task_ids,evidence):
    """Requeue exact held jobs with existing verified results; never start work."""
    task_ids=set(task_ids)
    if any(not isinstance(key,str) or not key for key in task_ids):
        raise ValueError('invalid selected task identity')
    db_path=config.root/'state.sqlite3';evidence=Path(evidence)
    evidence.mkdir(parents=True,exist_ok=False)
    with InstanceLock(db_path.with_suffix(db_path.suffix+'.controller.lock')):
        with sqlite3.connect(db_path) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA temp_store=MEMORY')
            tasks,jobs,barriers,tables=_selected_state(db,config,batch_id,task_ids)
            unchanged={table:table_hash(db,table) for table in tables if table!='scheduler_jobs'}
            old_jobs={r['job_id']:dict(r) for r in db.execute('SELECT * FROM scheduler_jobs')}
            snapshot_root=evidence/'snapshot';snapshot_root.mkdir()
            with sqlite3.connect(snapshot_root/'state.sqlite3') as target:db.backup(target)
        checkpoint=config.root/'checkpoints/processing.sqlite3'
        with sqlite3.connect(checkpoint.as_uri()+'?mode=ro',uri=True) as source:
            source.execute('PRAGMA temp_store=MEMORY')
            checkpoint_tables=[r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            checkpoint_before={name:table_hash(source,name) for name in checkpoint_tables}
            (snapshot_root/'checkpoints').mkdir()
            with sqlite3.connect(snapshot_root/'checkpoints/processing.sqlite3') as target:source.backup(target)
        # Constructors may initialize tables/paths, so use disposable DB copies
        # for native checkpoint reading. Cache validation only reads live bytes.
        copied=copy.deepcopy(config);copied.home=str(snapshot_root);copied.handoff_root=str(evidence/'handoff')
        flow=ProcessingWorkflow(copied,Store(snapshot_root/'state.sqlite3',config.contract_version),
                                NoBusinessWork(),producer=NoBusinessWork())
        checked=copy.deepcopy(config);checked.processing_ledger_path=str(db_path)
        snapshots={};proof={};pending=[]
        try:
            with ExitStack() as owners:
                admission=None
                if any(task['stage']=='round3_chunk' for task in tasks):
                    from paperflow.embedding_admission import admission_from_config
                    from paperflow.embedding_service import CountBroker
                    admission=admission_from_config(checked)
                    # Reuse the exact vocabulary service under the paused
                    # exclusive lock. Close its owner before any retry commit.
                    admission.counter=owners.enter_context(CountBroker(admission.policy,admission.settings))
                for task in tasks:
                    snapshots[task['task_id']]=flow.graph.get_state(flow.graph_config(task))
                    proof[task['task_id']]=_selected_result(checked,task,snapshots[task['task_id']],
                        recover_completed=True,admission=admission,pending_preparations=pending)
            # Failed owner shutdown must not leave a reusable prepared marker
            # that a later recovery could accept without checking the counter.
            from paperflow.phase_cache import prepare_cache
            for task,stage,fingerprint,result,verified_files in pending:
                if file_hashes(result['draft_dir'])!=verified_files:
                    raise ValueError('validated recovery output changed before preparation')
                prepare_cache(checked,task,stage,fingerprint,result)
        finally:flow.close()
        save(evidence/'plan.json',dict(batch_id=batch_id,task_ids=sorted(task_ids),proof=proof,
            preserved_tables=unchanged,unresolved_preserved=len(barriers)))
        with sqlite3.connect(db_path) as db:
            db.row_factory=sqlite3.Row;db.execute('PRAGMA temp_store=MEMORY');db.execute('BEGIN IMMEDIATE')
            current_tasks,current_jobs,_,_=_selected_state(db,config,batch_id,task_ids)
            if current_jobs!=jobs:raise ValueError('selected jobs changed during recovery')
            for table,digest in unchanged.items():
                if table_hash(db,table)!=digest:raise ValueError('protected table changed: '+table)
            with sqlite3.connect(checkpoint.as_uri()+'?mode=ro',uri=True) as source:
                source.execute('PRAGMA temp_store=MEMORY')
                if any(table_hash(source,name)!=digest for name,digest in checkpoint_before.items()):
                    raise ValueError('native checkpoint changed during recovery')
            for task in current_tasks:
                if _selected_result(checked,task,snapshots[task['task_id']])!=proof[task['task_id']]:
                    raise ValueError('selected result changed during recovery')
            for job in jobs:
                if db.execute("UPDATE scheduler_jobs SET state='retry',due=0 WHERE job_id=? AND state='held'",
                              (job['job_id'],)).rowcount!=1:raise ValueError('selected job changed')
                old_jobs[job['job_id']].update(state='retry',due=0)
            if {r['job_id']:dict(r) for r in db.execute('SELECT * FROM scheduler_jobs')}!=old_jobs:
                raise ValueError('unselected job or retained job fields changed')
        result=dict(batch_id=batch_id,retry_jobs=len(jobs),paused=True,business_work_started=False,
                    unresolved_preserved=len(barriers),proof=proof,preserved_tables=sorted(unchanged))
        save(evidence/'result.json',result)
        return result


def _require_selected_base_python():
    """WSL needs a direct base-Python launch, as used by the Windows services."""
    if sys.platform!='win32':return None
    base=Path(getattr(sys,'_base_executable',sys.executable)).resolve()
    if base==Path(sys.executable).resolve():return None
    # Re-executing as a child of the venv launcher inherits its WSL denial.
    # Reject before recovery; operators must launch the base interpreter directly.
    raise RuntimeError('Launch paused counter recovery directly with the base Python executable: '+str(base))


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',required=True);parser.add_argument('--batch-id',required=True)
    selection=parser.add_mutually_exclusive_group(required=True)
    selection.add_argument('--protected');selection.add_argument('--selected')
    parser.add_argument('--evidence',required=True)
    args=parser.parse_args()
    if args.selected:
        _require_selected_base_python()
    selected=json.loads(Path(args.selected or args.protected).read_text(encoding='utf-8'))
    if selected['batch_id']!=args.batch_id:raise ValueError('approved completion snapshot mismatch')
    operation=prepare_selected if args.selected else prepare
    print(json.dumps(operation(Config.load(args.config),args.batch_id,selected['task_ids'],args.evidence),ensure_ascii=False))
