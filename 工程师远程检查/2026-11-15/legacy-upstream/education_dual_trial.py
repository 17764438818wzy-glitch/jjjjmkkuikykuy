"""Representative education trials on confirmed existing document IDs."""
from __future__ import annotations
import argparse
import json
import statistics
import time
from dataclasses import replace
from pathlib import Path
from auto_config import load_key
from education_stream_ingest import write_json
from ingest_contracts import ArtifactSpec
from ingest_education import EducationArtifacts, classify_education
from ingest_queue import QueueStore
from ingest_dual_router import DeepSeekRouter
from ingest_model_routes import load_routes
from ingest_gateway import RagflowGateway
from ingest_scheduler import WorkPool
from ingest_remote_activity import RagflowInFlightCounts
from ragflow_client import RagflowClient


def select_samples(items: list[dict],size: int,previous_results: dict) -> list[dict]:
    candidates=sorted([row for row in items if row.get('status')=='pass' and previous_results.get(row['paper_id'],{}).get('outcome')=='accepted'
                      and previous_results[row['paper_id']].get('remote_id')],key=lambda row:(row.get('length',0),row['paper_id']))
    if size<1 or len(candidates)<size: raise ValueError('Not enough verified representative sources')
    selected=[]; methods=set()
    # Cover length strata, preferring a method not already represented in each stratum.
    for index in range(size):
        lo=index*len(candidates)//size; hi=(index+1)*len(candidates)//size
        group=candidates[lo:hi]
        chosen=next((row for row in group if json.dumps(row.get('method'),sort_keys=True,ensure_ascii=False) not in methods),group[len(group)//2])
        selected.append(chosen); methods.add(json.dumps(chosen.get('method'),sort_keys=True,ensure_ascii=False))
    return selected


def summarize(queue: QueueStore,observations: list[dict]) -> dict:
    valid={'accepted','quality_hold','quality_reject'}
    count=len(observations); outcomes=[row.get('outcome') for row in observations]
    durations=[row['duration'] for row in observations if isinstance(row.get('duration'),(float,int)) and row['duration']>=0]
    missing=0
    for obs in observations:
        task=queue.get(obs['task_id']) if obs.get('task_id') else None
        if (not task or not task.get('remote_id') or not task.get('execution_attempt')
                or task['status'] not in valid or obs.get('outcome')!=task['status']
                or (task['status']=='accepted' and (task.get('post_status')!='done' or not obs.get('artifacts')))
                or not obs.get('finished_at') or not obs.get('started_at')): missing+=1
    return {'sample_count':count,'valid_decisions':sum(v in valid for v in outcomes),'accepted':outcomes.count('accepted'),
            'quality_hold':outcomes.count('quality_hold'),'quality_reject':outcomes.count('quality_reject'),
            'technical_failures':sum(v not in valid for v in outcomes),'missing_evidence':missing,
            'median_duration_seconds':statistics.median(durations) if durations else None,
            'passed':count>0 and missing==0 and all(v in valid for v in outcomes)}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--data-root',type=Path,default=Path(__file__).parent/'data')
    parser.add_argument('--stage',required=True)
    parser.add_argument('--channel',choices=['school','official','dual'],required=True)
    parser.add_argument('--size',type=int,default=8)
    parser.add_argument('--school-limit',type=int,default=6)
    args=parser.parse_args()
    data=args.data_root; private=data/'private/education_dual_channel'
    routes=load_routes(private/'trial_routes.json',json.loads((data/'settings.json').read_text(encoding='utf-8'))['ragflow_url'])
    routes['school']['initial_limit']=args.school_limit; routes['school']['max_limit']=args.school_limit
    routes['total_limit']=args.school_limit+routes['official']['initial_limit']
    output=private/args.stage; output.mkdir(parents=True,exist_ok=True)
    q=QueueStore(output/'queue.sqlite3')
    key=load_key(data,'ragflow'); factory=lambda:RagflowClient(routes['service_url'],key)
    audit=json.loads((private/'adoption_audit.json').read_text(encoding='utf-8'))
    entries=[row for row in audit['entries'] if row['spec']['role']=='paper_primary' and row['initial']['status']=='accepted']
    previous={row['spec']['source_id']:{'outcome':'accepted','remote_id':row['initial']['remote_id']} for row in entries}
    inventory=json.loads((private/'md_inventory_before.json').read_text(encoding='utf-8')); docs={d['id']:d for d in inventory}
    candidates=[{'paper_id':row['spec']['source_id'],'length':Path(row['spec']['path']).stat().st_size,
                 'method':(docs.get(row['initial']['remote_id'],{}).get('meta_fields') or {}).get('education_research_methods','unknown'),
                 'status':'pass','entry':row} for row in entries]
    selected=select_samples(candidates,args.size,previous)
    if not q.summary():
        for row in selected:
            entry=row['entry']; raw=dict(entry['spec']); raw['path']=Path(raw['path']); raw['package_path']=Path(raw['package_path'])
            spec=ArtifactSpec(**raw); rid=entry['initial']['remote_id']
            remote=factory().documents(spec.dataset_id,document_id=rid)
            if len(remote)!=1 or remote[0].get('name')!=spec.remote_name or remote[0].get('run') not in ('DONE','FAIL'):
                raise ValueError('Trial source is not a confirmed terminal document')
            q.enqueue(spec,initial_state={'status':'parse_waiting','remote_id':rid,'remote_run':remote[0]['run'],
                      'execution_previous_pipeline_id':remote[0].get('pipeline_id')})
        write_json(output/'selection.json',{'channel':args.channel,'size':args.size,'paper_ids':[r['paper_id'] for r in selected],
                  'purpose':'explicit representative re-evaluation on original IDs; no new uploads'})
    if args.channel!='dual':
        other='official' if args.channel=='school' else 'school'
        q.save_route_state(routes[other]['channel_key'],{'halted':True})
    router=DeepSeekRouter(q,routes); artifacts=EducationArtifacts(factory,output/'artifacts')
    class Gateway(RagflowGateway):
        def upload(self,task): raise ValueError('Trials cannot create remote documents')
        def parse(self,task):
            artifacts.invalidate(task); super().parse(task)
    activity=RagflowInFlightCounts(factory,q,routes)
    pool=WorkPool(q,lambda:Gateway(factory),classify_education,on_accepted_for=artifacts,
                  parse_limit=routes['total_limit'],router=router,external_counts_reader=activity)
    snapshots=[]; started=time.time(); last_scan=0; observed=[]
    try:
        while True:
            progress=pool.tick(); print(json.dumps({'stage':args.stage,'progress':progress},ensure_ascii=False),flush=True)
            tasks=q.list_statuses(('parse_waiting','parse_intent','submitted','running','parse_unknown','accepted','quality_hold','quality_reject','technical_review','outcome_unresolved'),100000)
            if time.monotonic()-last_scan>=15:
                scan_start=time.time()
                from education_dual_adopt import inventory as read_inventory
                docs=read_inventory(factory(),routes['dataset_id'])
                cohort={r['remote_id'] for r in tasks}
                complete=len(docs)>=len(inventory)
                snapshots.append({'at':time.time(),'request_started_at':scan_start,'complete':complete,
                                  'running':[r['id'] for r in docs if r.get('run')=='RUNNING' and r['id'] in cohort],
                                  'pipelines':{r['id']:r.get('pipeline_id') for r in docs if r.get('run')=='RUNNING' and r['id'] in cohort}})
                last_scan=time.monotonic()
            write_json(output/'progress.json',{'started_at':started,'snapshot':progress,'routes':router.snapshot(),'elapsed_seconds':time.time()-started})
            terminal=all(r['status'] in ('accepted','quality_hold','quality_reject','technical_review','outcome_unresolved') and
                         (r['status']!='accepted' or r.get('post_status') in ('done','needs_review')) for r in tasks)
            if terminal and len(tasks)==args.size: break
            if time.time()-started>3600:
                raise TimeoutError('Trial remains unfinished; queue preserved for reconciliation')
            time.sleep(3)
    finally: pool.close()
    observations=[]
    for task in tasks:
        attempts=q.parse_attempts(task['id']); last=attempts[-1] if attempts else {}
        observations.append({'task_id':task['id'],'paper_id':task['source_id'],'remote_id':task['remote_id'],'outcome':task['status'],
                             'channel':task['channel_key'],'started_at':last.get('started_at'),'finished_at':last.get('finished_at'),
                             'duration':max(0,(last.get('finished_at') or time.time())-(last.get('started_at') or time.time())),
                             'artifacts':task.get('post_status')=='done','attempts':attempts})
    report=summarize(q,observations); report.update(elapsed_seconds=time.time()-started,observations=observations,
        maximum_observed_running=max((len(r['running']) for r in snapshots if r['complete']),default=0),running_snapshots=snapshots)
    write_json(output/'results.json',report);print(json.dumps({k:v for k,v in report.items() if k not in ('observations','running_snapshots')},ensure_ascii=False),flush=True)

if __name__=='__main__': main()
