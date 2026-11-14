"""Native read surface with task-owned display-cache invalidation."""
import argparse,asyncio,json,os,sys
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI,HTTPException,Query
from fastapi.middleware.cors import CORSMiddleware

from .dashboard_reader import DashboardReader
from .dashboard_source import NativeDashboardSource,native_context
from prefect.client.schemas.filters import ArtifactFilter


def create_dashboard_app(api_url,cache_dir,*,source=None):
    reader=DashboardReader(source or NativeDashboardSource(api_url),cache_dir)
    @asynccontextmanager
    async def lifespan(app):
        yield
        await reader.close()
    app=FastAPI(lifespan=lifespan,docs_url=None,redoc_url=None,openapi_url=None)
    app.state.reader=reader
    app.add_middleware(CORSMiddleware,allow_origins=['http://127.0.0.1:8796','http://localhost:8796'],allow_methods=['GET'],allow_headers=['*'])

    @app.get('/health')
    async def health():return {'service':'paperflow-dashboard-query','read_only':True,'native_api':api_url}

    @app.get('/tasks')
    async def tasks():return await reader.source.tasks()

    @app.delete('/cache/tasks/{task_id}')
    async def forget_task(task_id:UUID,deletion_plan_id:UUID):
        from .deletion import PLAN_TYPE,plan_key
        async with native_context(api_url) as client:
            plans=await client.read_artifacts(
                artifact_filter=ArtifactFilter(id={'any_':[deletion_plan_id]}),limit=1)
        if not plans:raise HTTPException(409,'Deletion plan is unavailable')
        plan=plans[0]
        data=plan.data
        if (plan.id!=deletion_plan_id or str(plan.flow_run_id)!=str(task_id)
                or plan.key!=plan_key(task_id) or plan.type!=PLAN_TYPE
                or not isinstance(data,dict) or data.get('task_id')!=str(task_id)):
            raise HTTPException(409,'Deletion plan does not match this task')
        sources=data.get('source_paths')
        archive=data.get('nas_destination')
        if (not isinstance(sources,list) or not sources
                or not all(isinstance(path,str) and path for path in sources)
                or not isinstance(archive,str) or not archive):
            raise HTTPException(409,'Deletion plan must identify originals and archive')
        try:await reader.forget_task(task_id,protected_paths=[*sources,archive])
        except (ValueError,TimeoutError) as exc:
            raise HTTPException(409,str(exc)) from exc
        return {'task_id':str(task_id),'deletion_plan_id':str(deletion_plan_id),'forgotten':True}

    @app.get('/summary')
    async def summary(task_id:UUID):return await reader.summary(task_id)

    @app.get('/folder-tickets')
    async def details(task_id:UUID,folder_id:str|None=None,section:str|None=None,result:str|None=None,failed:bool=False,
                      offset:int=Query(0,ge=0),limit:int=Query(20,ge=1,le=100)):
        if folder_id is None and section is None and not failed:raise HTTPException(422,'请选择要查看的工单')
        parent=await reader.summary(task_id)
        saved=reader.load(str(task_id))
        selected=[]
        if section is not None and section not in ('preprocessing','llm','postprocessing','rag','nas','cleanup'):
            raise HTTPException(422,'Unknown stage')
        if result is not None and result not in ('passed','failed','unrecognized'):
            raise HTTPException(422,'Unknown result')
        for key,row in saved['rows'].items():
            if folder_id is not None and row['folder_id']!=folder_id:continue
            if failed and row['stage']!='failed':continue
            if section is not None and row[section+'_result']!=result:continue
            selected.append(key)
        total=len(selected)
        if section is not None:selected=selected[offset:offset+limit]
        projection=await reader.details(task_id,selected)
        return {**parent,'summary':None,'folders':projection['folders'],
                'page':{'offset':offset,'limit':limit if section is not None else total,'total':total}}

    @app.get('/tasks/{task_id}/classifications')
    async def classifications(task_id:UUID):return await reader.classifications(task_id)
    return app


async def prime(args):
    reader=DashboardReader(NativeDashboardSource(args.prefect_api_url),args.cache_dir)
    try:
        result=await reader.prime(args.prime)
        print(json.dumps({'task_id':result['task_id'],'summary':result['summary'],
                          'classification':result['classification'],'compatibility':result['compatibility']},ensure_ascii=False))
    finally:await reader.close()


if __name__=='__main__':
    import uvicorn
    parser=argparse.ArgumentParser()
    parser.add_argument('--prefect-api-url',required=True)
    parser.add_argument('--cache-dir',required=True)
    parser.add_argument('--project-config',required=True)
    parser.add_argument('--core-root',required=True)
    parser.add_argument('--port',type=int,default=8826)
    parser.add_argument('--prime')
    args=parser.parse_args()
    os.environ['PAPERFLOW_V3_PROJECT_CONFIG']=args.project_config
    sys.path.insert(0,args.core_root)
    if args.prime:asyncio.run(prime(args))
    else:uvicorn.run(create_dashboard_app(args.prefect_api_url,args.cache_dir),host='127.0.0.1',port=args.port)
