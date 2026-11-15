"""Explicit bounded operator entry for the existing managed workflow.

Directory intake reuses the existing stable-file scanner and controlled scheduler.
"""
import hashlib
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import threading
import time
from urllib.parse import urlparse

import psutil

from .control import Manager
from .legacy_readback import article_status
from .package import load, save, sha
from .scheduler import Scheduler
from .store import Store
from .transition import TransitionGuard
from .winjob import process_identity


def legacy_guard(root):
    root=Path(root).resolve();runtime=root/'langgraph_workflow/runtime'
    trial=runtime/'batch10-20261016-01';dispatcher=root/'working/codex-dispatcher'
    identities=[]
    for process in psutil.process_iter(['pid','name','cmdline']):
        if process.pid==os.getpid() or (process.info['name'] or '').lower() not in ('python.exe','pythonw.exe'):continue
        command=' '.join(process.info['cmdline'] or []).lower()
        if any(name in command for name in ('run_dispatcher.py','run_trial.py','resume_trial.py')):
            identities.append(process_identity(process.pid))
    return TransitionGuard([runtime/'controller.lock',trial/'controller.lock',trial/'trial-supervisor.lock',dispatcher/'worker.lock'],
                           [trial/'STOP_TRIAL',dispatcher/'STOP'],identities=identities)


def validate_scope(config,root,sources,legacy_root,*,watch=False,selection_limit=10):
    root=Path(root).resolve();legacy_root=Path(legacy_root).resolve()
    if not watch and (not sources or (selection_limit is not None and len(sources)>selection_limit)):
        raise ValueError('explicit source files exceed the authorized selection limit')
    protected=[legacy_root/'langgraph_workflow/runtime',legacy_root/'working/codex-dispatcher']
    if root==legacy_root or any(root.is_relative_to(p) or p.is_relative_to(root) for p in protected):
        raise ValueError('isolated root overlaps formal runtime')
    for path in (config.root,Path(config.handoff_root),Path(config.catalog_path),Path(config.delivery_acceptance_report or '')):
        if not path.resolve().is_relative_to(root):raise ValueError('configuration outside isolated root')
    if watch:
        if sources or not config.source_roots:raise ValueError('watch requires configured source_roots and no explicit files')
        if config.scan_seconds<=0 or config.stable_seconds<0:raise ValueError('invalid intake intervals')
        owned=[config.root,Path(config.handoff_root),root/'manager',root/'inputs']
        for directory in config.source_roots:
            directory=Path(directory).resolve()
            if not directory.is_dir():raise ValueError('watch source directory unavailable')
            if any(directory.is_relative_to(p.resolve()) or p.resolve().is_relative_to(directory) for p in owned):
                raise ValueError('watch directory overlaps generated workflow files')
    elif config.source_roots:raise ValueError('bounded entry requires empty source_roots; select files explicitly')
    paths=[Path(p).resolve() for p in sources]
    if config.production_pilot:
        scope_path=Path(config.production_pilot).resolve()
        if not scope_path.is_relative_to(root):raise ValueError('pilot scope outside run root')
        scope=load(scope_path)
        if (scope.get('mode')!=('production-watch' if watch else 'production-pilot') or scope.get('endpoint')!=config.rag_url or
            Path(scope.get('root','')).resolve()!=root):raise ValueError('production pilot scope mismatch')
        if scope.get('expires_at',0)<=time.time():raise ValueError('production pilot selection expired')
        if sha(Path(config.catalog_path))!=scope.get('catalog_sha256'):raise ValueError('pilot catalog changed')
        if watch:
            if {str(Path(p).resolve()) for p in config.source_roots}!={str(Path(p).resolve()) for p in scope.get('source_roots',[])}:
                raise ValueError('production watch roots changed')
        else:
            selected={str(p):sha(p) for p in paths}
            approved={str(Path(row['path']).resolve()):row['sha256'] for row in scope['sources']}
            if selected!=approved:raise ValueError('production pilot selection changed')
    if urlparse(config.rag_url).hostname not in ('127.0.0.1','localhost','::1'):
        if not config.production_pilot:raise ValueError('isolated accepted loopback endpoint or explicit production pilot required')
    report=load(config.delivery_acceptance_report)
    if report.get('passed') is not True or report.get('endpoint')!=config.rag_url:
        raise ValueError('accepted endpoint evidence required')
    accepted=set(report.get('identity',{}).get('dataset_ids',[]))
    targets={row[key] for row in config.catalog['majors'] for key in ('paper_dataset_id','image_dataset_id') if row.get(key)}
    if not targets or not targets.issubset(accepted):raise ValueError('catalog outside accepted owned datasets')
    for kind,target in config.attachment_targets.items():
        proof=report.get('attachment_storage',{})
        if (kind not in ('drawing','software') or proof.get('passed') is not True
            or proof.get('targets',{}).get(kind)!=target or not target.get('parent_id') or target.get('dataset_id') not in accepted):
            raise ValueError('attachment target outside accepted owned storage')
    if len(set(paths))!=len(paths) or any(not p.is_file() for p in paths):raise ValueError('distinct existing sources required')
    from .intake import EXTENSIONS
    if any(p.suffix.lower() not in EXTENSIONS for p in paths):raise ValueError('unsupported source format')
    return root,paths


def run_managed(config,root,sources,*,legacy_root,execute=False,seconds=3600,stop=None,watch=False,retry_tasks=()):
    """Prepare a paused run; --execute explicitly admits only its selected articles."""
    started=time.time();admitted=None
    if retry_tasks and (not execute or len(retry_tasks)>10 or len(set(retry_tasks))!=len(retry_tasks)):
        raise ValueError('retry requires --execute and 1..10 distinct task IDs')
    root,sources=validate_scope(config,root,sources,legacy_root,watch=watch)
    if not 0<seconds<=86400:raise ValueError('run duration must be 1 second to 24 hours')
    if config.production_pilot:
        scope=load(config.production_pilot)
        if seconds>scope['maximum_seconds'] or time.time()+seconds>scope['expires_at']:
            raise ValueError('run exceeds approved pilot time window')
    stop=stop or threading.Event();guard=legacy_guard(legacy_root)
    # Acquire old locks before even registering a new source. Failed recovery must
    # retain them: TransitionSession owns that existing contract.
    guard.__enter__()
    try:
        config.prepare();store=Store(config.root/'state.sqlite3',config.contract_version)
        inputs=root/'inputs';inputs.mkdir(exist_ok=True)
        selection=[]
        for source in sources:
            digest=sha(source)
            name=hashlib.sha256(str(source).casefold().encode()).hexdigest()[:20]+source.suffix.lower()
            target=inputs/name
            if target.exists() and sha(target)!=digest:raise ValueError('selected source changed; use a new isolated run root')
            if not target.exists():shutil.copy2(source,target)
            if sha(source)!=digest or sha(target)!=digest:raise ValueError('source changed during snapshot')
            selection.append(dict(original=str(source),path=str(target),sha256=digest))
        expected={str(Path(row['path']).resolve()) for row in selection}
        with store.db() as db:
            if not watch and any(row['path'] not in expected for row in db.execute('SELECT path FROM sources')):
                raise ValueError('run contains unselected sources')
        if watch:
            selected=load(root/'selection.json') if (root/'selection.json').is_file() else []
            scope=load(config.production_pilot) if config.production_pilot else {}
            approved={str(Path(r['path']).resolve()):r['sha256'] for r in scope.get('sources',[])}
            verified=[]
            for task in store.all_tasks():
                if not store.is_current(task['task_id']):continue
                origin=task.get('original_source_path')
                if not origin:
                    choices=[r['original'] for r in selected if r.get('sha256')==task['source_sha256']
                        and 'src_'+hashlib.sha256(str(Path(r['original']).resolve()).casefold().encode()).hexdigest()[:24]==task['source_id']]
                    if len(set(choices))==1:origin=choices[0]
                if not origin or not any(Path(origin).resolve().is_relative_to(Path(p).resolve()) for p in config.source_roots):
                    raise ValueError('existing task outside approved watch roots')
                if approved and approved.get(str(Path(origin).resolve()))!=task['source_sha256']:
                    raise ValueError('existing task outside selected trial')
                if not Path(task['source_path']).resolve().is_relative_to(root):raise ValueError('existing input outside run root')
                verified.append((task,origin))
            for task,origin in verified:
                if not task.get('original_source_path'):store.update(task['task_id'],original_source_path=str(Path(origin).resolve()))
        tasks=[store.register_source(row['path'],row['sha256'],
            identity_path=row['original'] if config.production_pilot else None) for row in selection]
        if config.production_pilot and not watch:
            from .repair_store import RepairLedger
            repairs=RepairLedger(store)
            evidence={str(Path(r['path']).resolve()):r for r in load(config.production_pilot)['sources']}
            for row,task in zip(selection,tasks):
                prior=evidence[row['original']]
                unknown=prior.get('legacy_budget_unknown',False)
                count=2 if unknown else prior.get('legacy_correction_count',0)
                if type(count) is not int or count<0:raise ValueError('invalid pilot legacy budget')
                task=store.update(task['task_id'],original_source_path=row['original'],legacy_budget_unknown=unknown,
                    legacy_ids=prior.get('legacy_ids',[]),correction_count=max(task.get('correction_count',0),count))
                # Reserve conservatively; never fabricate historical correction attempts.
                with store.db() as db:repairs.seed_sql(db,task,legacy_count=count)
        if not watch:save(root/'selection.json',selection)
    except BaseException:
        guard.__exit__(None,None,None)
        raise
    manager=Manager(store,root/'manager',remote_harness=config.remote_harness)
    # Guard is already held while taking immutable input copies.
    manager.transition_guard=guard
    manager.start()
    scheduler=None;watcher=None;reason=['paused'];watch_errors=[];previous_handlers={}
    try:
        scheduler=Scheduler(manager,config,isolation_root=root)
        if retry_tasks:
            if (root/'STOP').exists():raise ValueError('retry requires clearing this run STOP explicitly')
            # The scope and existing source identities were verified above. Keep
            # all attempts, retry budgets, caches and graph checkpoints intact.
            with store.db() as db:
                db.execute('BEGIN IMMEDIATE')
                for task_id in retry_tasks:
                    task=store.get(task_id)
                    job=db.execute("SELECT job_id,state FROM scheduler_jobs WHERE kind='graph' AND unit_key=?",(task_id,)).fetchone()
                    if (not task or not store.is_current(task_id) or task['stage'] in
                        ('held','parked','superseded','handed_off','attachment_ready','complete','code_error')
                        or not job or job['state']!='held'):
                        raise ValueError('only a current held graph with unfinished processing can be retried')
                    if db.execute("SELECT 1 FROM scheduler_attempts WHERE job_id=? AND state!='finished'",(job['job_id'],)).fetchone():
                        raise ValueError('retry execution exit not reconciled')
                    db.execute("UPDATE scheduler_jobs SET state='queued',due=0 WHERE job_id=?",(job['job_id'],))
                    db.execute('INSERT INTO events(at,task_id,kind,data) VALUES(?,?,?,?)',
                        (time.time(),task_id,'operator_graph_retry','{}'))
        from .intake import Intake
        def register(source,digest):
            source=Path(source).resolve()
            if not any(source.is_relative_to(Path(p).resolve()) for p in config.source_roots):
                raise ValueError('source escaped configured directory')
            if config.production_pilot:
                current_scope=load(config.production_pilot)
                if current_scope.get('expires_at',0)<=time.time():raise ValueError('production watch expired')
                approved={str(Path(r['path']).resolve()):r['sha256'] for r in current_scope.get('sources',[])}
                if approved and approved.get(str(source))!=digest:raise ValueError('source outside selected trial')
            name=hashlib.sha256(str(source).casefold().encode()).hexdigest()[:20]
            target=root/'inputs'/name/(digest+source.suffix.lower())
            target.parent.mkdir(parents=True,exist_ok=True)
            if not target.exists():
                scratch=target.with_suffix(target.suffix+'.copying')
                shutil.copy2(source,scratch)
                if sha(source)!=digest or sha(scratch)!=digest:raise ValueError('source changed during snapshot')
                scratch.replace(target)
            if sha(source)!=digest or sha(target)!=digest:raise ValueError('snapshot identity mismatch')
            return store.register_source(target,digest,identity_path=source,supersede_identity=True,
                                         original_source_path=str(source))
        intake=Intake(config,store,registrar=register) if watch else None
        last_scan=[0.0]
        def cancel(label):
            manager.pause(label);reason[0]=label;stop.set()
        if threading.current_thread() is threading.main_thread():
            for kind in (signal.SIGINT,signal.SIGTERM):
                previous_handlers[kind]=signal.signal(kind,lambda *_:cancel('operator stop'))
        if execute and not stop.is_set() and not (root/'STOP').exists():
            deadline=time.monotonic()+seconds
            def monitor():
                while not stop.wait(.1):
                    try:
                        guard.check()
                        if (root/'STOP').exists():cancel('operator stop');return
                        if time.monotonic()>=deadline:cancel('time limit');return
                    except sqlite3.OperationalError as exc:
                        if getattr(exc,'sqlite_errorcode',0)&255==sqlite3.SQLITE_BUSY:continue
                        watch_errors.append(type(exc).__name__);cancel('monitor failed');return
                    except Exception as exc:
                        watch_errors.append(type(exc).__name__);cancel('control evidence changed');return
            admitted=time.time();manager.resume()
            watcher=threading.Thread(target=monitor,name='managed-operator-stop',daemon=True);watcher.start()
            original_tick=scheduler.tick
            def guarded_tick():
                guard.check()
                if (root/'STOP').exists() or stop.is_set():cancel('operator stop')
                if watch and not manager.stopped.is_set() and time.monotonic()-last_scan[0]>=config.scan_seconds:
                    stats=intake.scan();last_scan[0]=time.monotonic()
                    save(root/'intake-status.json',dict(updated_at=time.time(),**stats))
                result=original_tick()
                jobs=scheduler.ledger.rows('SELECT * FROM scheduler_jobs')
                active=any(j['state'] in ('queued','retry','running','blocked_service') for j in jobs)
                if not watch and jobs and not active and manager.status()['live_units']==0 and not stop.is_set():cancel('settled')
                return result
            scheduler.tick=guarded_tick
            scheduler.run(stop,interval=min(.5,config.scan_seconds) if watch else .5)
        else:
            scheduler.tick()  # discovery only: Manager starts paused
    finally:
        stop.set()
        if watcher:watcher.join(3)
        manager.shutdown(timeout=35)
        from .office import cleanup_office_temps
        save(root/'office-temp-cleanup.json',cleanup_office_temps(config))
        if scheduler:
            from .run_timing import run_timing
            save(root/'run-timing.json',run_timing(scheduler,started,admitted,time.time()))
        # A failed shutdown must retain the old locks. Only release after actual exit.
        guard.check();guard.__exit__(None,None,None)
        for kind,handler in previous_handlers.items():signal.signal(kind,handler)
    # Interrupted attempts are reconciled by the next started manager. Do not
    # mutate service admission after shutdown (a half-open probe may remain).
    status=manager.status()
    if watch:tasks=store.all_tasks()
    task_rows=[dict(task_id=t['task_id'],stage=store.get(t['task_id'])['stage']) for t in tasks]
    from .attachments import attachment_status
    articles=[attachment_status(store,t['task_id']) or article_status(store,t['task_id'],scheduler.manager.cleanup.ledger.status(t['task_id'])) for t in tasks]
    result=dict(state='paused',reason=reason[0],manager=status,tasks=task_rows,articles=articles,
        complete=all(a['complete'] for a in articles),watch_errors=watch_errors,
        jobs=[{k:r[k] for k in ('job_id','kind','state')} for r in scheduler.ledger.rows('SELECT * FROM scheduler_jobs')])
    save(root/'managed-result.json',result)
    if watch_errors:raise RuntimeError('managed monitor stopped: '+','.join(watch_errors))
    return result
