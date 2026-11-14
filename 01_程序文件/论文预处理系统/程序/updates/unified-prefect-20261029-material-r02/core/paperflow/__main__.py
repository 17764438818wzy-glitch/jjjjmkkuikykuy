from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import signal
import threading

from .config import Config


def main():
    parser=argparse.ArgumentParser(description='论文持续处理工作流')
    parser.add_argument('--config',type=Path)
    sub=parser.add_subparsers(dest='command',required=True)
    doctor=sub.add_parser('doctor',help='检查配置和资源，不请求模型')
    doctor.add_argument('--network',action='store_true')
    sub.add_parser('status')
    sub.add_parser('scan',help='登记原稿及现有成果，不启动模型')
    run=sub.add_parser('run',help='受控持续收稿；复用 configured source_roots')
    run.add_argument('--root',type=Path,required=True)
    run.add_argument('--legacy-root',type=Path,default=Path(r'E:\rag mid'))
    run.add_argument('--execute',action='store_true')
    run.add_argument('--seconds',type=float,default=86400)
    run.add_argument('--retry-task',action='append',default=[],help='显式重试本次范围内处理未完成的暂存任务，保留全部历史')
    process=sub.add_parser('process',help='只处理指定原稿，用于有限验收')
    process.add_argument('source',type=Path)
    process.add_argument('--steps',type=int,default=12)
    ui=sub.add_parser('ui'); ui.add_argument('--smoke-test',action='store_true')
    sub.add_parser('graph',help='输出阶段图Mermaid')
    managed=sub.add_parser('managed',help='新链路受控小批入口；默认暂停')
    managed.add_argument('--root',type=Path,required=True)
    managed.add_argument('--source',type=Path,action='append',default=[])
    managed.add_argument('--watch',action='store_true',help='持续监听配置的原稿目录；空队列继续等待')
    managed.add_argument('--legacy-root',type=Path,default=Path(r'E:\rag mid'))
    managed.add_argument('--execute',action='store_true',help='显式开始本次选定的隔离小批')
    managed.add_argument('--seconds',type=float,default=3600)
    managed.add_argument('--retry-task',action='append',default=[],help='显式重试本次范围内处理未完成的暂存任务，保留全部历史')
    args=parser.parse_args()
    if args.command in ('managed','run'):
        if not args.config:parser.error('managed requires an explicit isolated --config')
        from .managed import run_managed
        result=run_managed(Config.load(args.config),args.root,getattr(args,'source',[]),legacy_root=args.legacy_root,
                           execute=args.execute,seconds=args.seconds,watch=args.command=='run' or args.watch,retry_tasks=args.retry_task)
        print(json.dumps(result,ensure_ascii=False,indent=2));return
    if args.command in ('run','process','scan','ui'):
        parser.error('旧执行入口已封存；请使用 managed --root ... --source ...。默认暂停，--execute 仅启动选定隔离小批。')
    config=Config.load(args.config); config.prepare()
    if args.command=='ui':
        from .ui import main as ui_main
        result=ui_main(config,args.smoke_test)
        if result: print(json.dumps(result,ensure_ascii=False))
        return
    from .store import Store,TERMINAL
    store=Store(config.root/'state.sqlite3',config.contract_version)
    if args.command=='doctor':
        import subprocess,sys
        reader=config.reader_python or sys.executable
        checked=subprocess.run([reader,'-X','utf8','-B','-c',
            'import paperflow.reading,fitz,PIL; print("ok")'],cwd=Path(__file__).parent.parent,
            capture_output=True,timeout=30,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        result=dict(home=str(config.root),free_gib=round(shutil.disk_usage(config.root).free/2**30,2),
                    source_roots_available=[Path(p).is_dir() for p in config.source_roots],
                    specialties=len(config.catalog['majors']),provider=config.provider,model=config.model,
                    original_prompts_present=(Path(__file__).parent/'prompts/round1.original.txt').is_file(),
                    reader_python=reader,reader_dependencies='ok' if checked.returncode==0 else 'unavailable')
        if args.network:
            from .rag import native_client
            client=native_client(config)
            ds=config.catalog['majors'][0]['paper_dataset_id']
            client.request('GET',f'/datasets/{ds}/documents?page=1&page_size=1')
            result['rag_read']='ok'
        print(json.dumps(result,ensure_ascii=False,indent=2)); return
    if args.command=='status':
        from .readiness import readiness
        print(json.dumps(dict(tasks=store.counts(),metrics=store.metrics(),acceptance=readiness(config)),ensure_ascii=False,indent=2)); return
    if args.command=='scan':
        from .intake import Intake
        intake=Intake(config,store)
        summary=dict(handoffs=intake.import_handoffs(),originals=intake.scan())
        from .graph import Workflow
        flow=Workflow(config,store)
        try:flow.apply_pending_requests()
        finally:flow.close()
        print(json.dumps(summary,ensure_ascii=False));return
    from .graph import Workflow
    if args.command=='graph':
        flow=Workflow(config,store)
        try: print(flow.graph.get_graph().draw_mermaid())
        finally: flow.close()
        return
    if args.command=='process':
        from .package import sha
        from .service import InstanceLock
        with InstanceLock(config.root/'controller.lock'):
            task=store.register_source(args.source,sha(args.source))
            flow=Workflow(config,store)
            try:
                for _ in range(args.steps):
                    if task['stage'] in TERMINAL:break
                    task=flow.step(task)
                    print(json.dumps({k:task.get(k) for k in ('task_id','stage','revision','error_kind')},ensure_ascii=False),flush=True)
                    if task.get('error_kind')=='infrastructure': break
            finally: flow.close()
        return
    from .service import Service
    stop=threading.Event()
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    service=Service(config)
    try: service.run(stop)
    finally: service.close()


if __name__=='__main__': main()
