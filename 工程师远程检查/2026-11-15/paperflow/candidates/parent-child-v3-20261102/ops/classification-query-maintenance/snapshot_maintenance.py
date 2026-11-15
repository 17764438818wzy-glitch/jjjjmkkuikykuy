"""Compact superseded derived snapshots only; test and live use the same predicate."""
import hashlib,json,subprocess,time
from datetime import datetime,timezone
from uuid import UUID

if 'PHASE' not in globals():
    import argparse
    from pathlib import Path
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=['test','test-extra','test-orphans','test-bounded','preview','apply'])
    parser.add_argument('--options',type=Path)
    args=parser.parse_args()
    PHASE=args.phase
    OPTIONS=json.loads(args.options.read_text(encoding='utf-8-sig')) if args.options else {}

TEST='paperflow-classification-index-test-20261103'
LIVE='paperflow-prefect-preview-postgres-1'
TYPES="'paperflow-v3-stage-snapshot','paperflow-v3-task-summary','paperflow-v3-history-checked','paperflow-v3-snapshot-gap','paperflow-v3-snapshot-resolved'"
fixture=PHASE in {'test','test-extra','test-orphans','test-bounded'}
container=TEST if fixture else LIVE

def sql(query,timeout=30,statement_seconds=15):
    assert statement_seconds in {15,45}
    if fixture:
        x=json.loads(subprocess.check_output(['docker','inspect',TEST]))[0]
        assert x['Config']['Labels'].get('paperflow.classification-index-test')=='20261103'
        assert x['HostConfig']['NetworkMode']=='none'
        args=['docker','exec','-i',TEST,'psql','-X','-q','-v','ON_ERROR_STOP=1','-U','bench_owner','-d','classification_index_bench','-A','-t']
    else:
        x=json.loads(subprocess.check_output(['docker','inspect',LIVE]))[0]
        assert x['Config']['Image']=='postgres:18.6-alpine'
        script='PGPASSWORD="<REDACTED_SECRET>"'
        args=['docker','exec','-i',LIVE,'sh','-c',script]
    p=subprocess.run(args,input="SET statement_timeout='"+str(statement_seconds)+"s'; SET lock_timeout='1s'; SET max_parallel_workers_per_gather=0;\n"+query,
                     capture_output=True,text=True,encoding='utf-8',timeout=timeout)
    if p.returncode:raise RuntimeError(p.stderr)
    return p.stdout.strip()

def emit(name,result):
    print(json.dumps({'name':name,'result':result}),flush=True)

def scope(ids):
    values=','.join("('"+str(UUID(i))+"'::uuid)" for i in ids)
    return """WITH selected_tasks(task_id) AS (VALUES %s), scoped_runs AS (
 SELECT task_id,task_id AS run_id FROM selected_tasks
 UNION SELECT s.task_id,r.id FROM selected_tasks s JOIN task_run t ON t.flow_run_id=s.task_id
 JOIN flow_run r ON r.parent_task_run_id=t.id)
""" % values

def predicate(cutoff):
    stamp=datetime.fromisoformat(cutoff).astimezone(timezone.utc).isoformat()
    return """a.type IN (%s) AND a.key LIKE 'paperflow-v3-snapshot-%%'
 AND a.id<>c.latest_id AND a.created<'%s'::timestamptz
 AND a.data->>'task_id'=s.task_id::text
 AND NOT EXISTS(SELECT 1 FROM flow_run_state z WHERE z.result_artifact_id=a.id)
 AND NOT EXISTS(SELECT 1 FROM task_run_state z WHERE z.result_artifact_id=a.id)""" % (TYPES,stamp)

def candidates(ids,cutoff,locking=False,artifact_ids=None):
    selected=''
    if artifact_ids is not None:
        assert 1<=len(artifact_ids)<=250
        selected=" AND a.id IN ("+','.join("'"+str(UUID(i))+"'::uuid" for i in artifact_ids)+')'
    return """SELECT a.id,a.type FROM artifact a
 JOIN scoped_runs s ON s.run_id=a.flow_run_id
 JOIN artifact_collection c ON c.key=a.key
 JOIN artifact h ON h.id=c.latest_id AND h.key=c.key
 WHERE """+predicate(cutoff)+selected+(" ORDER BY a.created,a.id LIMIT 250 FOR UPDATE OF c SKIP LOCKED" if locking else '')

def inventory(ids,cutoff):
    return json.loads(sql(scope(ids)+"SELECT coalesce(json_agg(t),'[]') FROM (SELECT type,count(*) AS count FROM ("+candidates(ids,cutoff)+")x GROUP BY type)t",timeout=60,statement_seconds=45))

def candidate_ids(ids,cutoff):
    result=json.loads(sql(scope(ids)+"SELECT coalesce(json_agg(id),'[]') FROM ("+candidates(ids,cutoff)+" LIMIT 50001)x",timeout=60,statement_seconds=45))
    assert len(result)<=50000,'Candidate set exceeds the bounded maintenance size'
    return result

def prune_batch(ids,cutoff,artifact_ids=None):
    if artifact_ids is None:artifact_ids=candidate_ids(ids,cutoff)[:250]
    if not artifact_ids:return []
    query="BEGIN;\n"+scope(ids)+", doomed AS ("+candidates(ids,cutoff,True,artifact_ids)+"), removed AS (DELETE FROM artifact a USING doomed d WHERE a.id=d.id RETURNING a.id,a.type) SELECT coalesce(json_agg(t),'[]') FROM (SELECT type,count(*) AS count FROM removed GROUP BY type)t; COMMIT;"
    return json.loads(sql(query))

def check_scope(ids):
    values=','.join("'"+str(UUID(i))+"'" for i in ids)
    if not fixture:
        valid=json.loads(sql("SELECT coalesce(json_agg(r.id),'[]') FROM flow_run r JOIN flow f ON f.id=r.flow_id WHERE r.id IN ("+values+") AND f.name='paperflow-v3-parent'"))
        assert set(valid)==set(ids), 'Task scope does not belong to the expected project flow'

def integrity(ids):
    return json.loads(sql(scope(ids)+"""SELECT json_build_object(
 'original_receipts',(SELECT count(*) FROM artifact a JOIN scoped_runs s ON s.run_id=a.flow_run_id WHERE a.type IN ('paperflow-v3-receipt','paperflow-v3-folder-completion')),
 'dangling_latest',(SELECT count(*) FROM artifact_collection c JOIN scoped_runs s ON s.run_id=c.flow_run_id LEFT JOIN artifact a ON a.id=c.latest_id AND a.key=c.key WHERE a.id IS NULL),
 'dangling_latest_ids',(SELECT coalesce(json_agg(c.id::text||':'||c.latest_id::text ORDER BY c.id),'[]') FROM artifact_collection c JOIN scoped_runs s ON s.run_id=c.flow_run_id LEFT JOIN artifact a ON a.id=c.latest_id AND a.key=c.key WHERE a.id IS NULL))"""))

def assert_no_new_missing_heads(before,after):
    assert set(after['dangling_latest_ids']).issubset(before['dangling_latest_ids']), 'New missing collection head detected'

if fixture:
    base={'test':100,'test-extra':200,'test-orphans':300,'test-bounded':400}[PHASE]
    task,child,other,taskrun=[str(UUID(int=base+n)) for n in range(1,5)]
    sql("CREATE TABLE IF NOT EXISTS task_run(id uuid PRIMARY KEY,flow_run_id uuid); CREATE TABLE IF NOT EXISTS flow_run(id uuid PRIMARY KEY,parent_task_run_id uuid); CREATE TABLE IF NOT EXISTS flow_run_state(result_artifact_id uuid REFERENCES artifact(id) ON DELETE SET NULL); CREATE TABLE IF NOT EXISTS task_run_state(result_artifact_id uuid REFERENCES artifact(id) ON DELETE SET NULL);")
    sql("INSERT INTO flow_run VALUES ('"+task+"',NULL),('"+child+"','"+taskrun+"'),('"+other+"',NULL); INSERT INTO task_run VALUES ('"+taskrun+"','"+task+"');")
    rows=[]
    def add(label,key,typ,body,*,owner=task,run=child,head=True):
        aid=str(UUID(hashlib.md5((PHASE+label).encode()).hexdigest()))
        data=json.dumps({'task_id':owner,**body}).replace("'","''")
        q="INSERT INTO artifact(id,created,updated,key,type,data,flow_run_id) VALUES('%s',now()-interval '1 hour',now(),'%s','%s','%s','%s');"%(aid,key,typ,data,run)
        if head:
            q+="INSERT INTO artifact_collection SELECT a.*,a.id AS latest_id FROM artifact a WHERE a.id='%s' ON CONFLICT(key) DO UPDATE SET latest_id=excluded.latest_id,type=excluded.type,data=excluded.data;"%aid
        sql(q)
        rows.append(aid)
        return aid
    pref='paperflow-v3-snapshot-fixture-'+PHASE+'-'
    add('progress-old',pref+'classification','paperflow-v3-stage-snapshot',{'result':None})
    add('progress-new',pref+'classification','paperflow-v3-stage-snapshot',{'result':'passed'})
    add('request-old',pref+'request','paperflow-v3-stage-snapshot',{'pending_request_count':1})
    add('request-resolved',pref+'request','paperflow-v3-snapshot-resolved',{'pending_request_count':0})
    add('gap-old',pref+'gap','paperflow-v3-snapshot-gap',{'source_artifact_id':'original'})
    add('gap-resolved',pref+'gap','paperflow-v3-snapshot-resolved',{'source_artifact_id':'original'})
    add('final',pref+'final','paperflow-v3-stage-snapshot',{'final':True,'result':'passed'})
    unknown=add('unknown',pref+'new-request','paperflow-v3-stage-snapshot',{'pending_request_count':1,'state':'unknown'})
    original=add('raw-unknown','paperflow-v3-fixture-pfv3snapshotpending-'+PHASE,'paperflow-v3-receipt',{'state':'unknown'})
    add('indexed','paperflow-v3-fixture-pfv3snapshotpending-'+PHASE,'paperflow-v3-receipt-indexed',{'source_artifact_id':original})
    for stage in ('rag','nas','cleanup'):
        add(stage+'-complete','paperflow-v3-folder-fixture-'+PHASE+stage,'paperflow-v3-folder-completion',{'status':'complete','stage':stage})
    protected=add('native-result-old',pref+'protected','paperflow-v3-stage-snapshot',{'result':'passed'})
    add('native-result-new',pref+'protected','paperflow-v3-stage-snapshot',{'result':'passed'})
    sql("INSERT INTO flow_run_state VALUES ('"+protected+"')")
    add('other-old',pref+'other','paperflow-v3-stage-snapshot',{},owner=other,run=other)
    add('other-new',pref+'other','paperflow-v3-stage-snapshot',{},owner=other,run=other)
    if PHASE=='test-orphans':
        orphan=add('orphan-old',pref+'orphan','paperflow-v3-stage-snapshot',{})
        sql("UPDATE artifact_collection SET latest_id='00000000-0000-0000-0000-000000000999' WHERE key='"+pref+"orphan'")
    def state_digest():
        return sql("SELECT md5(string_agg(row_to_json(c)::text,'' ORDER BY c.id)) FROM artifact_collection c WHERE key LIKE '%fixture%'")
    before=state_digest(); originals=integrity([task]); cutoff=datetime.now(timezone.utc).isoformat()
    selected=inventory([task],cutoff)
    assert sum(x['count'] for x in selected)==3,selected
    removed=prune_batch([task],cutoff)
    assert sum(x['count'] for x in removed)==3
    assert state_digest()==before
    assert integrity([task])==originals
    assert_no_new_missing_heads(originals,integrity([task]))
    assert int(sql("SELECT count(*) FROM artifact WHERE id IN ('"+unknown+"','"+protected+"','"+original+"')"))==3
    assert prune_batch([task],cutoff)==[]
    emit('cleanup_fixture',{'passed':True,'removed':3,'latest_digest_unchanged':True,'originals_unchanged':True,'unknown_and_native_reference_retained':True,'repeat_removed':0})
    if PHASE=='test-orphans':
        assert originals['dangling_latest']==1
        assert int(sql("SELECT count(*) FROM artifact WHERE id='"+orphan+"'"))==1
        try:
            assert_no_new_missing_heads(originals,{'dangling_latest_ids':originals['dangling_latest_ids']+['new-missing-head']})
        except AssertionError:pass
        else:raise AssertionError('New missing head was not rejected')
        emit('cleanup_preexisting_orphan_fixture',{'passed':True,'existing_orphan_unchanged':True,'orphan_old_history_excluded':True,'new_missing_head_rejected':True})

    if PHASE in {'test-extra','test-bounded'}:
        protected_ids=[]
        for label,owner,run in [('wrong-owner',other,child),('wrong-run',task,other),('after-cutoff',task,child),('task-state',task,child)]:
            old=add(label+'-old',pref+label,'paperflow-v3-stage-snapshot',{},owner=owner,run=run)
            add(label+'-new',pref+label,'paperflow-v3-stage-snapshot',{},owner=owner,run=run)
            protected_ids.append(old)
            if label=='after-cutoff':sql("UPDATE artifact SET created=now()+interval '1 hour' WHERE id='"+old+"'")
            if label=='task-state':sql("INSERT INTO task_run_state VALUES ('"+old+"')")
        assert inventory([task],cutoff)==[]
        assert prune_batch([task],cutoff)==[]
        assert int(sql("SELECT count(*) FROM artifact WHERE id IN ("+','.join("'"+i+"'" for i in protected_ids)+")"))==4
        lockkey=pref+'concurrent'
        add('lock-old',lockkey,'paperflow-v3-stage-snapshot',{})
        add('lock-current',lockkey,'paperflow-v3-stage-snapshot',{})
        new=add('lock-new',lockkey,'paperflow-v3-stage-snapshot',{'state':'unknown'},head=False)
        sql("UPDATE artifact SET created=now()+interval '1 hour' WHERE id='"+new+"'")
        args=['docker','exec','-i',TEST,'psql','-X','-q','-v','ON_ERROR_STOP=1','-U','bench_owner','-d','classification_index_bench','-A','-t']
        writer=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        writer.stdin.write("BEGIN; SELECT 'locked' FROM artifact_collection WHERE key='"+lockkey+"' FOR UPDATE; SELECT pg_sleep(3); UPDATE artifact_collection SET latest_id='"+new+"',data='{\"state\":\"unknown\"}' WHERE key='"+lockkey+"'; COMMIT;\n")
        writer.stdin.close()
        assert writer.stdout.readline().strip()=='locked'
        assert prune_batch([task],cutoff)==[]
        remaining=inventory([task],cutoff)
        assert sum(x['count'] for x in remaining)==1
        assert writer.wait(timeout=10)==0,writer.stderr.read()
        removed=prune_batch([task],cutoff)
        assert sum(x['count'] for x in removed)==2
        assert inventory([task],cutoff)==[]
        assert sql("SELECT latest_id FROM artifact_collection WHERE key='"+lockkey+"'")==new
        assert integrity([task])['dangling_latest']==0
        emit('cleanup_extra_fixture',{'passed':True,'cutoff_and_scope_mismatches_retained':True,'task_state_reference_retained':True,'locked_candidates_reported_remaining':1,'removed_after_unlock':2,'concurrent_new_unknown_retained':True,'remaining':0})
    if PHASE=='test-bounded':
        stale=add('selected-old',pref+'selected','paperflow-v3-stage-snapshot',{'state':'unknown'})
        later=add('selected-later',pref+'selected','paperflow-v3-stage-snapshot',{})
        selected=candidate_ids([task],cutoff)
        assert selected==[stale]
        sql("UPDATE artifact_collection SET latest_id='"+stale+"' WHERE key='"+pref+"selected'")
        assert prune_batch([task],cutoff,selected+protected_ids+[unknown])==[]
        assert int(sql("SELECT count(*) FROM artifact WHERE id='"+stale+"'"))==1
        assert candidate_ids([task],cutoff)==[later]
        assert sum(x['count'] for x in prune_batch([task],cutoff,[later]))==1
        assert candidate_ids([task],cutoff)==[]
        emit('cleanup_bounded_fixture',{'passed':True,'stale_candidate_latest_rechecked':True,'explicit_protected_ids_retained':True,'second_pass_finds_newly_superseded':True})

elif PHASE in {'preview','apply'}:
    ids=[str(UUID(i)) for i in OPTIONS['task_ids']]
    assert 1<=len(ids)<=10
    check_scope(ids)
    cutoff=OPTIONS.get('cutoff') or datetime.now(timezone.utc).isoformat()
    before=integrity(ids)
    selected=inventory(ids,cutoff)
    emit('cleanup_preview',{'task_ids':ids,'cutoff':cutoff,'candidates':selected,'integrity':before})
    if PHASE=='apply':
        assert OPTIONS.get('approved') is True
        total={}; batches=0
        try:
            todo=candidate_ids(ids,cutoff)
            while todo:
                pass_deleted=0
                for start in range(0,len(todo),250):
                    if batches>=200:raise RuntimeError('Bounded cleanup exceeded 200 batches')
                    batch=prune_batch(ids,cutoff,todo[start:start+250])
                    count=sum(row['count'] for row in batch)
                    for row in batch:total[row['type']]=total.get(row['type'],0)+row['count']
                    pass_deleted+=count; batches+=1
                    emit('cleanup_progress',{'batches':batches,'batch_deleted':count,'deleted':sum(total.values()),'deleted_by_type':total})
                    time.sleep(.15)
                if pass_deleted==0:break
                todo=candidate_ids(ids,cutoff)
        except Exception:
            emit('cleanup_interrupted',{'complete':False,'committed_deleted_by_type':total,'committed_deleted':sum(total.values()),'completed_batches':batches,'cutoff':cutoff})
            raise
        after=integrity(ids)
        assert_no_new_missing_heads(before,after)
        assert after['original_receipts']>=before['original_receipts']
        remaining=inventory(ids,cutoff)
        complete=not remaining
        emit('cleanup_done' if complete else 'cleanup_partial',{'complete':complete,'deleted_by_type':total,'batches':batches,'integrity_after':after,'remaining':remaining})
        if not complete:raise RuntimeError('Cleanup is incomplete: locked or concurrent candidates remain; preserve this cutoff for a later bounded run')
else:raise ValueError(PHASE)
