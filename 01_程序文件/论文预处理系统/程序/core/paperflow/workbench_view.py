"""Read-only projections of existing workflow, routing, retention and execution facts."""
import json
from pathlib import Path
import re
import time
import psutil

from .attachments import attachment_status
from .intake import EXTENSIONS
from .legacy_readback import article_status
from .package import load
from .run_timing import job_belongs


def remote_processes(config,task_ids,store=None):
    if not config.remote_harness:return []
    rows=[];values=[];now=time.time()
    for task_id in task_ids:
        path=config.root/'private/remote-current'/(task_id+'.json')
        if not path.is_file():continue
        try:
            pointer=load(path);job=pointer.get('job_id','')
            if not re.fullmatch('[A-Za-z0-9_-]{1,128}',job):continue
            value=load(config.root/'private/remote-executions'/job/'status.json')
        except (ValueError,OSError):continue
        if value.get('task_id')!=task_id or value.get('job_id')!=job:continue
        values.append(value)
    if store and values:
        with store.db() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='remote_harness_executions'").fetchone():
                for value in values:
                    row=db.execute("SELECT proof FROM remote_harness_executions WHERE execution_id=? AND epoch=? AND state='exited'",
                        (value.get('execution_id'),value.get('epoch'))).fetchone()
                    if row:
                        proof=json.loads(row['proof'])
                        if proof.get('all_exited') is True and proof.get('execution_closed') is True:
                            value.update(state='exited',exit_confirmed=True,ended_at=value.get('ended_at') or proof['at'])
    for value in values:
        age=max(0,now-value['last_remote_heartbeat']) if value.get('last_remote_heartbeat') else None
        state=value.get('state','unknown')
        heartbeat_window=5 if value.get('execution_kind')=='native_child' else 3
        if not value.get('exit_confirmed') and (age is None or age>heartbeat_window):state='unknown'
        rows.append(dict(task_id=value['task_id'],location='server',host=value.get('host'),pid=value.get('remote_pid'),
            container=value.get('container'),job_id=value.get('job_id'),state=state,
            heartbeat_age_seconds=age,elapsed_seconds=max(0,(value.get('ended_at') or now)-value.get('started_at',now)),
            exit_confirmed=value.get('exit_confirmed') is True))
        rows[-1].update(execution_kind=value.get('execution_kind'),group_id=value.get('group_id'),child_id=value.get('child_id'))
    return rows


def _artifact_files(task, root, complete):
    """Expose existence/counts, never manuscript text or private file paths."""
    paths = [Path(task[k]).resolve() for k in ('package_dir','draft_dir') if task.get(k)]
    folder = next((p for p in paths if p.is_relative_to(root) and p.is_dir()), None)
    result = {key:'pending' for key in ('profile','tree','chunks','images')}
    result.update(selected_images=0,chunk_count=0,tree_nodes=0)
    if not folder:
        return result
    for key, filename in (('profile','profile.json'),('tree','tree.json'),('chunks','chunks.json'),('images','assets.json')):
        path = folder/filename
        if not path.is_file():
            continue
        result[key] = 'available' if complete else 'packaged' if task.get('package_dir') else 'generated'
        try:
            value = load(path)
            if not isinstance(value,dict):raise ValueError('artifact is not an object')
            if key == 'tree':result['tree_nodes']=len(value.get('nodes',[]))
            if key == 'chunks':result['chunk_count']=len(value.get('chunks',value.get('items',[])))
            if key == 'images':
                items=value.get('items',[])
                if not isinstance(items,list) or any(not isinstance(i,dict) for i in items):raise ValueError('unreadable asset items')
                result['selected_images']=sum(i.get('storage')=='image' for i in items)
        except (ValueError,TypeError,OSError):
            result[key]='unreadable'
    return result


def artifacts(task,root,complete,*,versions=None):
    if not versions:
        return dict(_artifact_files(task,root,complete),versions=[])
    rows=[]
    for version in versions:
        # The first graph's task fields intentionally remain immutable after repair.
        view=_artifact_files({'package_dir':version['path']} if version.get('path') else {},root,version['complete'])
        rows.append(dict(version,artifacts=view))
    result={key:sum(v['artifacts'][key] for v in rows) for key in ('selected_images','chunk_count','tree_nodes')}
    for key in ('profile','tree','chunks','images'):
        values=[v['artifacts'][key] for v in rows]
        result[key]='available' if all(v=='available' for v in values) else 'unreadable' if 'unreadable' in values else 'packaged' if 'packaged' in values else 'pending'
    result['versions']=[{k:v for k,v in item.items() if k!='path'} for item in rows]
    return result


def _jobs_by_task(rows, tasks, packages):
    """Index immutable execution ownership once, retaining the existing check."""
    attachments={task.get('attachment_id'):key for key,task in tasks.items() if task.get('attachment_id')}
    grouped={key:[] for key in tasks}
    for row in rows:
        if row['kind'] in ('graph','repair'):
            key=row['unit_key']
        elif row['kind']=='delivery':
            payload=json.loads(row.get('payload',row.get('data','{}')))
            key=attachments.get(payload['attachment_id']) if payload.get('attachment_id') else packages.get(payload.get('manifest_sha256'))
        else:
            continue
        if key in tasks and job_belongs(row,tasks[key],packages):
            grouped[key].append(row)
    return grouped


def status(workbench, batch_id=None, *, task_ids=None, articles_only=False):
    w=workbench;state=w.control_state();now=time.time()
    with w.store.db(timeout=.1) as db:
        batches=[dict(r) for r in db.execute('SELECT * FROM web_batches ORDER BY created DESC LIMIT 100')]
        chosen=next((b for b in batches if b['id']==batch_id),None) if batch_id else next(iter(batches),None)
        ids=list(task_ids) if task_ids is not None else json.loads(chosen['task_ids']) if chosen else []
        tasks={key:w.store.decode(db.execute('SELECT * FROM tasks WHERE task_id=?',(key,)).fetchone()) for key in ids}
        tasks={key:value for key,value in tasks.items() if value}
        from .scheduling_store import selection_filter
        selected,parameters=selection_filter(ids)
        jobs=[dict(r) for r in db.execute('SELECT j.* FROM scheduler_jobs j WHERE 1'+selected,parameters)]
        attempts=[dict(r) for r in db.execute('''SELECT a.*,j.article,j.kind,j.unit_key,j.data AS payload
            FROM scheduler_attempts a JOIN scheduler_jobs j ON j.job_id=a.job_id WHERE 1'''+selected+' ORDER BY a.started DESC',parameters)]
        package_rows={r['manifest_sha256']:dict(r) for r in db.execute('SELECT manifest_sha256,task_id,package_dir FROM delivery_packages')}
        packages={key:r['task_id'] for key,r in package_rows.items()}
        mappings={(r['manifest_sha256'],r['specialty_code']):json.loads(r['mapping_json'])
                  for r in db.execute('SELECT manifest_sha256,specialty_code,mapping_json FROM specialty_tasks')}
        attachments={r['task_id']:dict(r) for r in db.execute('SELECT * FROM attachment_tasks WHERE task_id IN (SELECT value FROM json_each(?))',(json.dumps(ids),))}
        for key,row in attachments.items():tasks[key]=dict(tasks[key],attachment_id=row['attachment_id'])
        budgets={(r['source_id'],r['source_sha256']):r['used'] for r in db.execute('SELECT * FROM correction_budgets')}
        executions=[dict(r) for r in db.execute('SELECT * FROM control_executions ORDER BY rowid DESC LIMIT 200')]
        services=[dict(kind=r['kind'],state=r['state'],next_probe=r['next_probe'],failures=r['failures'])
                  for r in db.execute('SELECT * FROM scheduler_services')]
    attempts_by_task=_jobs_by_task(attempts,tasks,packages)
    jobs_by_task=_jobs_by_task(jobs,tasks,packages)
    attempts_by_execution={}
    owners_by_execution={}
    for key,rows in attempts_by_task.items():
        for row in rows:
            owners_by_execution.setdefault(row['execution_id'],key)
    for row in attempts:
        attempts_by_execution.setdefault(row['execution_id'],row)
    processes=[]
    with w.manager.gate_lock:
        live={key:ex for key,ex in w.manager.executions.items() if not ex.exit_saved}
        for row in executions:
            attempt=attempts_by_execution.get(row['execution_id'])
            task_id=owners_by_execution.get(row['execution_id'])
            if not task_id:
                # Cleanup is separate from processing/delivery time attribution.
                if not attempt or attempt['kind']!='cleanup':continue
                task_id=next((key for key,t in tasks.items() if t['source_id']==attempt['article']),None)
            if not task_id:continue
            ex=live.get(row['execution_id']);action=ex.active_action if ex else None
            resource=w.manager.tickets.get(action['action_id'],{}).get('request',{}).get('resource',{}) if action else {}
            identity=json.loads(row['worker_identity'] or row['root_identity'] or '{}')
            processes.append(dict(execution_id=row['execution_id'],task_id=task_id,kind=row['unit_kind'],
                pid=identity.get('pid'),root_pid=json.loads(row['root_identity'] or '{}').get('pid'),
                state='running' if ex else row['state'],
                members=[dict(item,state='exited' if key in ex.exit_times else 'running') for key,item in ex.identities.items()] if ex else
                        [dict(item,state='exited') for item in json.loads(row['members'])],
                heartbeat_age_seconds=max(0,time.monotonic()-ex.last_heartbeat) if ex else max(0,now-row['last_heartbeat']),
                action=resource.get('stage') or resource.get('operation') or (action or {}).get('kind'),
                elapsed_seconds=max(0,(attempt['finished'] or now)-attempt['started']),exit_confirmed=bool(row['exit_evidence'])))
    catalog={r['code']:r for r in w.config.catalog['majors']}
    articles=[]
    for key,task in tasks.items():
        fact=attachment_status(w.store,key)
        if fact is None:fact=article_status(w.store,key,w.manager.cleanup.ledger.status(key))
        own=attempts_by_task[key]
        own_jobs=jobs_by_task[key]
        active=next((p for p in processes if p['task_id']==key and p['state']=='running'),None)
        progressing=active or any(j['state'] in ('queued','retry','running') for j in own_jobs)
        blocked=None if progressing else next((j for j in reversed(own_jobs) if j['state'] in ('held','held_budget','blocked','blocked_service')),None)
        problem=next((json.loads(a['data'] or '{}') for a in own if blocked and a['job_id']==blocked['job_id'] and a['outcome'] in ('held','failed','blocked')), {})
        fault=problem.get('fault') or {}
        issue=problem.get('reason') or problem.get('error') or fault.get('reason') or fault.get('message') or (blocked['state'] if blocked else None)
        current=('complete' if fact['complete'] else 'excluded' if fact.get('excluded') else
            'pending_reconcile' if fact.get('pending_reconcile') else 'processing' if active else
            'configuration_pending' if fact.get('state')=='configuration_pending' else
            'held' if issue or (not progressing and task['stage'] in ('held','parked','code_error')) else
            'pending_delivery' if task['stage'] in ('handed_off','attachment_ready') else 'queued')
        routes=[]
        for target in fact.get('targets',[]):
            entry=catalog.get(target['code'],{})
            registered=mappings.get((target['expected_manifest'],target['code']),{})
            routes.append(dict(target, name=registered.get('name') or entry.get('name',target['code']),
                paper_dataset_id=registered.get('paper_dataset_id'),materials_dataset_id=registered.get('image_dataset_id')))
        attachment=None
        if key in attachments:
            data=json.loads(attachments[key]['data'])
            attachment=dict(kind=attachments[key]['category'],state=attachments[key]['state'],
                target=data['target'],note=data['note'],association=data['association'],
                verification=fact['verification'],retrieval=fact['retrieval'],remote=fact['remote'])
        expected={}
        for target in fact.get('targets',[]):
            digest=target['expected_manifest']
            version=expected.setdefault(digest,dict(manifest=digest,path=package_rows.get(digest,{}).get('package_dir'),codes=[],complete=True))
            version['codes'].append(target['code'])
            version['complete']=version['complete'] and target['expected_complete']
        intervals=[(a['started'],a['finished'] or now) for a in own]
        articles.append(dict(task_id=key,name=task.get('display_name',key),stage=task['stage'],state=current,
            complete=fact['complete'],detail=fact,reason=(task.get('hold_reason') or issue) if current=='held' else None,
            routes=routes,attachment=attachment,artifacts=artifacts(task,w.root,fact['complete'],versions=list(expected.values())),
            correction_budget=dict(used=budgets.get((task['source_id'],task['source_sha256']),0),limit=2),
            recent_attempts=[dict(kind=a['kind'],outcome=a['outcome'],started=a['started'],finished=a['finished'],result=json.loads(a['data'] or '{}')) for a in own[:10]],
            process_id=active['pid'] if active else None,current_action=active['action'] if active else None,
            elapsed_seconds=max(b for a,b in intervals)-min(a for a,b in intervals) if intervals else 0,
            graph_seconds=sum(max(0,(a['finished'] or now)-a['started']) for a in own if a['kind'] in ('graph','repair')),
            delivery_seconds=sum(max(0,(a['finished'] or now)-a['started']) for a in own if a['kind']=='delivery'),attempts=len(own)))
    # Completed-record pagination reuses the authoritative article projection.
    # Batch intake capacity and machine pressure belong to the live dashboard;
    # computing them for every scan page makes a small historical read unbounded.
    if articles_only:
        return dict(articles=articles)
    by_dataset={r.get('image_dataset_id'):r for r in catalog.values()}
    resources=w.scheduler.pressure()
    memory=psutil.virtual_memory()
    resources.update(memory_total_bytes=memory.total,memory_used_bytes=memory.total-memory.available)
    return dict(mode='operator' if w.deployment else 'isolated',observed_at=now,manager=state,
        batch_id=chosen['id'] if chosen else None,
        latest_batch_id=batches[0]['id'] if batches else None,
        batches=[dict(id=b['id'],created=b['created'],count=len(json.loads(b['task_ids']))) for b in batches],
        intake=w.intake.summary(chosen['id']) if chosen else None,
        articles=articles,processes=processes,remote_processes=remote_processes(w.config,ids,w.store),jobs=[{k:j[k] for k in ('job_id','kind','state','due')} for j in jobs],
        services=services,resources=resources,extra_audit='not_checked',
        settings=dict(model=w.config.model,llm_workers=w.config.llm_workers,rag_workers=w.config.rag_workers,
            harness=state.get('harness',dict(location='local',state='ready')),
            max_memory_percent=w.config.max_memory_percent,min_free_gib=w.config.min_free_gib,
            routing='automatic',specialty_count=len(catalog),selected_limit=w.selected_limit,article_workers=w.config.article_workers,
            supported_formats=sorted(EXTENSIONS),
            attachment_targets=[dict(kind=k,dataset_id=v.get('dataset_id'),
                name=by_dataset.get(v.get('dataset_id'),{}).get('name','现有附件归档库')) for k,v in w.config.attachment_targets.items()]))
