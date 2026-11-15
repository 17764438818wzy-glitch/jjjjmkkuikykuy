"""Online indexes and bounded read measurements for the existing Prefect database."""
import hashlib,json,re,shutil,statistics,subprocess,time,urllib.request
from uuid import UUID

if 'PHASE' not in globals():
    import argparse
    from pathlib import Path
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase',choices=['preflight','measure','apply'])
    p.add_argument('--options',type=Path)
    a=p.parse_args(); PHASE=a.phase
    OPTIONS=json.loads(a.options.read_text(encoding='utf-8-sig')) if a.options else {}

LIVE='paperflow-prefect-preview-postgres-1'
API='http://192.168.1.120:4200/api'
ROOT='2b06e4c1-9ef8-46a3-810e-6e9cc69644e3'
INDEXES={
 'ix_pfv3_artifact_key_trgm':'ON public.artifact USING gin (key gin_trgm_ops)',
 'ix_pfv3_collection_flow_type':'ON public.artifact_collection USING btree (flow_run_id, type)',
}

def emit(name,value):print(json.dumps({'name':name,'result':value}),flush=True)

def sql(query,*,ddl=False):
    x=json.loads(subprocess.check_output(['docker','inspect',LIVE]))[0]
    assert x['Config']['Image']=='postgres:18.6-alpine'
    script='PGPASSWORD="<REDACTED_SECRET>"'+('150000' if ddl else '15000')+' -c lock_timeout=1500 -c application_name=paperflow_index_maintenance" psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -A -t'
    r=subprocess.run(['docker','exec','-i',LIVE,'sh','-c',script],input=query,capture_output=True,text=True,timeout=170 if ddl else 25)
    if r.returncode:raise RuntimeError(r.stderr)
    return r.stdout.strip()

def indexes():
    names=','.join("'"+n+"'" for n in INDEXES)
    return json.loads(sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT c.relname AS name,pg_get_indexdef(i.indexrelid) AS definition,i.indisvalid,i.indisready,pg_relation_size(i.indexrelid) AS bytes FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname IN ("+names+"))t"))

def health():
    return json.loads(sql("""SELECT json_build_object('database',current_database(),'server_version',current_setting('server_version'),
 'gin_fuzzy_search_limit',current_setting('gin_fuzzy_search_limit'),'trgm_installed',EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_trgm'),
 'activity',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT state,count(*) AS count,round(max(extract(epoch from clock_timestamp()-xact_start))) AS oldest_xact_seconds FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY state)t),
 'index_builds',(SELECT count(*) FROM pg_stat_progress_create_index),
 'roots',(SELECT json_agg(t) FROM (SELECT id,state_type FROM flow_run WHERE id IN ('2b06e4c1-9ef8-46a3-810e-6e9cc69644e3','de360c0e-d4fc-4261-8c10-3925bb6f2790'))t))"""))

def context():
    prefix=sql("SELECT left(key,length('paperflow-v3-firststep')+64) FROM artifact WHERE flow_run_id='"+ROOT+"' AND type='paperflow-v3-receipt' AND key LIKE 'paperflow-v3-firststep%' ORDER BY created LIMIT 1")
    assert re.fullmatch('paperflow-v3-firststep[a-f0-9]{64}',prefix),prefix
    runs=json.loads(sql("SELECT json_agg(id) FROM (SELECT r.id FROM flow_run r JOIN task_run t ON r.parent_task_run_id=t.id WHERE t.flow_run_id='"+ROOT+"' ORDER BY r.id LIMIT 200)t"))
    assert runs and len(runs)<=200
    return {'source_prefix':prefix,'run_ids':runs}

def plan(query):
    result=json.loads(sql('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+query))[0]
    def nodes(n):
        return [{'node':n['Node Type'],'index':n.get('Index Name'),'rows':n.get('Actual Rows'),'removed':n.get('Rows Removed by Filter'),'shared_read':n.get('Shared Read Blocks')}]+[v for c in n.get('Plans',[]) for v in nodes(c)]
    return {'execution_ms':result['Execution Time'],'nodes':nodes(result['Plan'])}

def read_api(path,body):
    request=urllib.request.Request(API+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    start=time.monotonic()
    with urllib.request.urlopen(request,timeout=35) as response:
        assert response.status==200
        rows=json.load(response)
    digest=hashlib.sha256(json.dumps(rows,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    return {'ms':round((time.monotonic()-start)*1000,3),'count':len(rows),'full_response_sha256':digest}

if PHASE=='preflight':
    emit('health',health()); emit('existing_indexes',indexes())
    emit('free_disk_gib',round(shutil.disk_usage('/').free/1024**3,2))
    emit('measurement_context',context())
elif PHASE=='measure':
    prefix=OPTIONS['source_prefix']; assert re.fullmatch('paperflow-v3-firststep[a-f0-9]{64}',prefix)
    runs=[str(UUID(x)) for x in OPTIONS['run_ids']]; assert 1<=len(runs)<=200
    typ=['paperflow-v3-stage-snapshot','paperflow-v3-snapshot-gap','paperflow-v3-history-checked','paperflow-v3-folder-completion']
    source_filter={'type':{'any_':['paperflow-v3-receipt','paperflow-v3-folder-completion']},'key':{'like_':prefix+'%'}}
    cases={'source':('/artifacts/filter',{'artifacts':source_filter,'sort':'CREATED_DESC','limit':200,'offset':0},"SELECT * FROM artifact WHERE type IN ('paperflow-v3-receipt','paperflow-v3-folder-completion') AND key ILIKE '%"+prefix+"%%' ORDER BY created DESC LIMIT 200")}
    for n in (1,len(runs)):
        ids=','.join("'"+x+"'::uuid" for x in runs[:n])
        filt={'flow_run_id':{'any_':runs[:n]},'type':{'any_':typ}}
        query='SELECT * FROM artifact_collection WHERE flow_run_id IN ('+ids+") AND type IN ("+','.join("'"+t+"'" for t in typ)+') ORDER BY id DESC LIMIT 200'
        cases['latest_'+str(n)]=('/artifacts/latest/filter',{'artifacts':filt,'limit':200,'offset':0},query)
    ids=','.join("'"+x+"'::uuid" for x in runs)
    cases['pending']=('/artifacts/latest/filter',{'artifacts':{'flow_run_id':{'any_':runs},'type':{'any_':['paperflow-v3-receipt']},'key':{'like_':'pfv3snapshotpending-'}},'limit':200,'offset':0},"SELECT * FROM artifact_collection WHERE flow_run_id IN ("+ids+") AND type='paperflow-v3-receipt' AND key ILIKE '%pfv3snapshotpending-%' ORDER BY id DESC LIMIT 200")
    for name,(path,body,query) in cases.items():
        emit(name+'_sql_plan',plan(query))
        samples=[read_api(path,body) for _ in range(3)]
        emit(name+'_api',{'samples':samples,'median_ms':round(statistics.median(x['ms'] for x in samples),3)})
    emit('health',health()); emit('indexes',indexes())
elif PHASE=='apply':
    assert OPTIONS.get('approved') is True
    assert shutil.disk_usage('/') .free>2*1024**3
    h=health(); emit('pre_build_health',h)
    assert h['trgm_installed'] and h['gin_fuzzy_search_limit']=='0'
    assert h['index_builds']==0, 'Another online index build is already running'
    assert all((x.get('oldest_xact_seconds') or 0)<120 for x in h['activity']), 'Long open transaction; do not build behind it'
    for name,tail in INDEXES.items():
        existing=next((x for x in indexes() if x['name']==name),None)
        if existing:
            assert existing['indisvalid'] and existing['indisready'] and existing['definition']=='CREATE INDEX '+name+' '+tail
            emit('index_already_valid',existing); continue
        start=time.monotonic()
        try:
            sql('CREATE INDEX CONCURRENTLY '+name+' '+tail+';',ddl=True)
        except Exception:
            partial=next((x for x in indexes() if x['name']==name),None)
            if partial and not partial['indisvalid']:
                sql('DROP INDEX CONCURRENTLY public.'+name+';',ddl=True)
                emit('invalid_index_removed',name)
            raise
        actual=next(x for x in indexes() if x['name']==name)
        assert actual['indisvalid'] and actual['indisready'] and actual['definition']=='CREATE INDEX '+name+' '+tail
        emit('online_index_built',{'seconds':round(time.monotonic()-start,3),**actual})
    emit('post_build_health',health())
else:raise ValueError(PHASE)
