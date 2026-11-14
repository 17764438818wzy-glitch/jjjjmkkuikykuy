"""Future target-PC service control. Packaging never invokes these actions."""
from __future__ import annotations
import argparse, json, os, socket, subprocess, sys, time, urllib.request
from pathlib import Path
import bundle_tools as b

REG=b.RUN/'services.json'
def records():return b.read(REG) if REG.exists() else {}
def process(row):
    import psutil
    try:
        p=psutil.Process(row['pid'])
        if abs(p.create_time()-row['created'])>.1:return None
        if not Path(p.exe()).resolve().is_relative_to(b.ROOT):return None
        if str(b.ROOT).lower() not in ' '.join(p.cmdline()).lower():return None
        return p
    except psutil.NoSuchProcess:return None
def ensure_ports(ports):
    for port in ports:
        with socket.socket() as sock:
            if hasattr(socket,'SO_EXCLUSIVEADDRUSE'):sock.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
            try:sock.bind(('127.0.0.1',port))
            except OSError:raise RuntimeError(f'端口{port}已占用；先查明已有服务，不接管或停止其它服务') from None
def http(url):
    with urllib.request.urlopen(url,timeout=3) as response:return json.load(response)
def start(name,args,cwd,ports,ready=None):
    rows=records();old=rows.get(name)
    if old and process(old):
        print(name+'已在本包进程记录中运行');return
    ensure_ports(ports)
    logs=b.RUN/'logs';logs.mkdir(parents=True,exist_ok=True)
    stamp=time.strftime('%Y%m%d-%H%M%S')
    stdout=logs/(name+'-'+stamp+'.stdout.log');stderr=logs/(name+'-'+stamp+'.stderr.log')
    env=dict(os.environ)
    # Use an absolute script entry as ownership evidence in the process command.
    command=[str(b.PROJECT/'.venv/Scripts/python.exe'),'-X','utf8','-B',str(Path(__file__).resolve()),'entry',name,*args]
    with stdout.open('ab') as out,stderr.open('ab') as err:
        child=subprocess.Popen(command,cwd=cwd,env=env,stdout=out,stderr=err,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    import psutil
    rows[name]={'pid':child.pid,'created':psutil.Process(child.pid).create_time(),
      'command':command,'stdout':str(stdout),'stderr':str(stderr),'ports':ports}
    b.write(REG,rows)
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        if child.poll() is not None:raise RuntimeError(name+'已退出，请读日志 '+str(stderr))
        if ready:
            try:
                http(ready);print(name+'已就绪');return
            except Exception:pass
        else:
            if time.monotonic()>deadline-87:print(name+'进程已启动；状态见日志');return
        time.sleep(.5)
    raise RuntimeError(name+'就绪等待超时；保留进程与日志，请先检查，不要重复启动')
def start_prefect():
    b.apply_environment()
    start('prefect',['server','start','--host','127.0.0.1','--port','14200'],b.ROOT,[14200],
          'http://127.0.0.1:14200/api/health')
def start_client(monitors):
    b.apply_environment();cfg=b.read(b.PROJECT/'config.json')
    if not cfg.get('execution_enabled'):raise RuntimeError('请先完成配置、预检和05_允许测试运行.ps1')
    rows=records()
    if 'prefect' not in rows or not process(rows['prefect']):raise RuntimeError('请先用04脚本启动本包独立Prefect')
    http(b.settings()['prefect_api_url']+'/health')
    import asyncio
    asyncio.run(b.bootstrap_limits())
    start('client',[],b.MAIN,[8796],'http://127.0.0.1:8796/api-config.json')
    query=['--prefect-api-url',b.settings()['prefect_api_url'],'--cache-dir',str(b.RUN/'dashboard-query'),
       '--project-config',str(b.PROJECT/'config.json'),'--core-root',str(b.PROJECT),'--port','8826']
    start('query',query,b.MAIN,[8826],'http://127.0.0.1:8826/health')
    if monitors:start('monitors',[],b.MAIN,[8797,8798,8799])
    print('页面 http://127.0.0.1:8796/；尚未创建或提交论文任务。')
def stop(paused):
    if not paused:raise RuntimeError('先在页面暂停任务并核对未决请求，再用 -TasksPaused 运行停止脚本')
    import psutil
    rows=records()
    for name in ['monitors','query','client','prefect']:
        row=rows.get(name)
        if not row:continue
        owner=process(row)
        if not owner:
            print(name+'记录已过期，不按PID猜测停止其它进程');continue
        children=owner.children(recursive=True)
        owned=[p for p in children if Path(p.exe()).resolve().is_relative_to(b.ROOT)]
        targets=list(reversed(owned))+[owner]
        for p in targets:
            try:p.terminate()
            except psutil.NoSuchProcess:pass
        _,alive=psutil.wait_procs(targets,timeout=12)
        for p in alive:
            try:p.kill()
            except psutil.NoSuchProcess:pass
        row['stopped_at']=time.strftime('%Y-%m-%d %H:%M:%S')
    b.write(REG,rows)
    print('已按本包进程身份停止服务；未删除任务、回执、输入或知识库。Nimble请用其专用停止脚本。')
def entry(name,args):
    b.apply_environment()
    import runpy
    if name=='prefect':sys.argv=['prefect',*args];runpy.run_module('prefect',run_name='__main__')
    elif name=='client':sys.argv=['launch'];runpy.run_module('paperflow_v3.control.launch',run_name='__main__')
    elif name=='query':sys.argv=['dashboard_api',*args];runpy.run_module('paperflow_v3.control.dashboard_api',run_name='__main__')
    elif name=='monitors':sys.argv=['panel_service','--serve'];runpy.run_path(str(b.MAIN/'runtime/panel-monitor/panel_service.py'),run_name='__main__')
    else:raise ValueError(name)
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['start-prefect','start-client','stop','entry'])
    p.add_argument('--with-monitors',action='store_true');p.add_argument('--tasks-paused',action='store_true')
    args,rest=p.parse_known_args()
    if args.action=='entry':entry(rest[0],rest[1:])
    elif rest:p.error('未知参数')
    elif args.action=='start-prefect':start_prefect()
    elif args.action=='start-client':start_client(args.with_monitors)
    elif args.action=='stop':stop(args.tasks_paused)
