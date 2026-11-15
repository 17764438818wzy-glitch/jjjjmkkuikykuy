"""Report program preparation and runtime separately from external test/audit work."""
import json


def job_belongs(row, task, packages):
    """Versions share source_id; execution attribution must use immutable task identity."""
    if row['kind'] in ('graph','repair'):
        return row['unit_key']==task['task_id']
    if row['kind']!='delivery':
        return False
    payload=json.loads(row.get('payload',row.get('data','{}')))
    if payload.get('attachment_id'):
        return payload['attachment_id']==task.get('attachment_id')
    return packages.get(payload.get('manifest_sha256'))==task['task_id']


def run_timing(scheduler, started, admitted, finished):
    attempts=scheduler.ledger.rows('SELECT a.*,j.kind,j.unit_key,j.article,j.data AS payload FROM scheduler_attempts a '
        'JOIN scheduler_jobs j ON j.job_id=a.job_id WHERE a.started>=?',(admitted or finished,))
    packages={r['manifest_sha256']:r['task_id'] for r in scheduler.ledger.rows('SELECT manifest_sha256,task_id FROM delivery_packages')}
    articles=[]
    for task in scheduler.manager.store.all_tasks():
        rows=[r for r in attempts if job_belongs(r,task,packages)]
        if not rows:continue
        intervals=[(r['started'],r['finished'] or finished) for r in rows]
        work={kind:sum(max(0,(r['finished'] or finished)-r['started']) for r in rows if r['kind']==kind)
              for kind in ('graph','repair','delivery')}
        articles.append(dict(task_id=task['task_id'],stage=task['stage'],
            started_at=min(a for a,b in intervals),finished_at=max(b for a,b in intervals),
            elapsed_seconds=max(b for a,b in intervals)-min(a for a,b in intervals),
            processing_seconds=work['graph']+work['repair'],delivery_seconds=work['delivery']))
    return dict(started_at=started,runtime_started_at=admitted,finished_at=finished,
        preparation_seconds=(admitted or finished)-started,runtime_seconds=finished-admitted if admitted else 0,
        extra_audit_seconds=None,articles=articles,
        note='Runtime includes directory idle/stability/queue waits; per-article work uses actual attempts. '
             'Developer/test preparation and additional audits are not included.')
