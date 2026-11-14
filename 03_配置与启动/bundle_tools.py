"""Relocate/configure the client copy; no business requests are made by setup/check."""
from __future__ import annotations
import argparse, asyncio, getpass, hashlib, importlib, json, os, re, shutil, socket
import subprocess, sys, time, urllib.request, uuid, venv
from pathlib import Path
from urllib.parse import urlsplit

ROOT=Path(__file__).resolve().parent.parent
APP=ROOT/'01_程序文件/论文预处理系统'
WORKSPACE=APP/'预处理系统2'
PROJECT=WORKSPACE/'prefect-paperflow'
MAIN=WORKSPACE/'candidates/parent-child-v3-20261102'
FIRST=WORKSPACE/'candidates/first-step-control-20261029'
ENV=ROOT/'02_运行环境'
RUN=ROOT/'运行数据'
META=ROOT/'05_清单与校验'
CONFIG_FILE=ROOT/'03_配置与启动/新电脑配置.json'
ORIGINAL_APP=r'F:\论文预处理系统'
ORIGINAL_EMBEDDING=r'E:\rag mid\deployments\workbench-8089-20261018\runtime\embedding'

def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def write(path,obj):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
def sha(path):
    with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def local_url(url):
    v=urlsplit(url)
    return v.scheme=='http' and v.hostname in {'localhost','127.0.0.1'} and not v.username and not v.password
def settings():
    s=read(CONFIG_FILE)
    if not local_url(s['prefect_api_url']) or urlsplit(s['prefect_api_url']).port!=14200:
        raise ValueError('测试Prefect必须是本机 http://127.0.0.1:14200/api，不接受正式服务')
    q=urlsplit(s['qdrant_gateway_url'])
    if q.scheme not in {'http','https'} or not q.hostname or q.username or q.password:
        raise ValueError('测试知识库URL无效')
    if (q.hostname in {'192.168.1.120','localhost','127.0.0.1'} and q.port in {8097,8098,8088}):
        raise ValueError('这里是正式知识库端口；请使用独立测试网关，例如18097')
    if s['model_base_url'].rstrip('/')!='http://192.168.1.120:8081/v1':
        raise ValueError('原模型线路核对绑定192.168.1.120:8081/v1；更换服务器需另审适配')
    return s
def wsl_path(path):
    p=Path(path).resolve();drive=p.drive
    if not re.fullmatch(r'[A-Za-z]:',drive):raise ValueError('计数模型必须位于本地盘')
    return '/mnt/'+drive[0].lower()+'/'+p.as_posix()[3:]

def replacement_pairs(previous=None):
    pairs=[(ORIGINAL_APP,str(APP)),(r'F:\nimble',str(ENV/'nimble')),
           (ORIGINAL_EMBEDDING,str(ENV/'embedding')),
           (r'C:\Program Files\NVIDIA Corporation\NVIDIA App\7z.exe',str(ENV/'7zip/7z.exe'))]
    if previous:
        for old,new in [(previous.get('app'),str(APP)),(previous.get('environment'),str(ENV))]:
            if old and old!=new:pairs.append((old,new))
    return pairs

def patch_paths(previous=None):
    changed=[]
    for base in [APP,ENV/'project-site-packages',ENV/'user-site-packages']:
        for p in base.rglob('*'):
            if not p.is_file() or p.suffix not in {'.py','.pth'} or '__pycache__' in p.parts:continue
            original=p.read_text(encoding='utf-8-sig',errors='strict')
            content=original
            for old,new in replacement_pairs(previous):
                content=content.replace(old.replace('\\','\\\\'),new.replace('\\','\\\\'))
                content=content.replace(old.replace('\\','/'),new.replace('\\','/'))
                content=content.replace(old,new)
            if content!=original:
                p.write_text(content,encoding='utf-8')
                changed.append(str(p.relative_to(ROOT)))
    monitor=WORKSPACE/'candidates/first-step-preview-8795/workbuddy_monitor.py'
    text=monitor.read_text(encoding='utf-8')
    value=settings()['prefect_api_url'].rstrip('/')+'/v2/concurrency_limits/paperflow-connected-total_llm'
    text,n=re.subn(r'^PREFECT_LIMIT\s*=.*$', 'PREFECT_LIMIT = '+repr(value),text,flags=re.M)
    if n!=1:raise RuntimeError('监控Prefect地址绑定位置已变化，停止配置')
    monitor.write_text(text,encoding='utf-8')
    changed.append(str(monitor.relative_to(ROOT)))
    return sorted(set(changed))

def configure():
    s=settings()
    previous=read(RUN/'安装绑定.json') if (RUN/'安装绑定.json').exists() else None
    changed=sorted(set(patch_paths(previous)+(previous or {}).get('patched_files',[])))
    cfg=read(ROOT/'03_配置与启动/项目配置模板.json')
    release=APP/'程序/updates/unified-prefect-20261029-material-r02'
    folders={'data_root':RUN/'data','receipt_root':RUN/'receipts',
       'library_root':RUN/'测试输入','classification_root':RUN/'classification',
       'settings_root':RUN/'settings','nas_destination':Path(s['archive_destination']) if s.get('archive_destination') else RUN/'测试归档'}
    for p in folders.values():p.mkdir(parents=True,exist_ok=True)
    for key,value in folders.items():cfg[key]=str(value.resolve())
    cfg.update(program_root=str(APP/'程序'),release_root=str(release),
       template_path=str(APP/'程序/resources/论文预提取_空白模板_v1.1_研究类型.md'),
       catalog_path=str(PROJECT/'catalog.json'),runtime_config=str(PROJECT/'runtime-config.json'),
       model_config=str(PROJECT/'model-config.json'),source_manifest=str(PROJECT/'source-manifest.json'),
       gateway_admission_source=str(MAIN/'paperflow_v3/stages/gateway_admission.py'),
       execution_enabled=False,prefect_api_url=s['prefect_api_url'],qdrant_gateway_url=s['qdrant_gateway_url'],
       rag_url=s['qdrant_gateway_url'],history_roots=[],rag_history_roots=[],attachment_receipt_roots=[],
       max_live_papers=1,workbuddy_credit_pool=False,local_cleanup_enabled=True)
    cfg['limits']={k:1 for k in ['body_llm','attachment_llm','total_llm','local','package','rag','nas','cleanup']}
    model=read(PROJECT/'model-config.json');model['base_url']=s['model_base_url'];model['text_model']=s['text_model']
    write(PROJECT/'model-config.json',model)
    runtime=read(PROJECT/'runtime-config.json');counter=runtime['embedding_admission']['counter'];emb=ENV/'embedding'
    for k in ['model_path','library_path','cache_dir','python_path','runtime_manifest']:
        value=counter[k]
        for old,new in [(ORIGINAL_EMBEDDING,str(emb)),((previous or {}).get('embedding',''),str(emb))]:
            if old:value=value.replace(old,new)
        counter[k]=value
    counter['command']=['wsl.exe','--distribution',s['wsl_distribution'],'--exec','env',
      'LD_LIBRARY_PATH='+wsl_path(emb/'model'),'python3','-B']
    write(PROJECT/'runtime-config.json',runtime)
    paths={'schema':'paper-source-layout/1','python':str(PROJECT/'.venv/Scripts/python.exe'),
      'production_app':str(APP/'程序/desktop'),'data_dir':str(RUN/'legacy-data'),
      'runtime_dir':str(RUN/'native-runtime'),
      'original_core':str(APP/'预处理核心与工作台/langgraph_workflow')}
    write(APP/'程序/config/paths.json',paths)
    for name in ['ai_settings.json','settings.json']:
        source=ROOT/'03_配置与启动/设置模板'/name;target=folders['settings_root']/name
        if source.exists() and not target.exists():write(target,read(source))
    # Use test IDs as well as a physically separate test gateway. Raw-retention IDs
    # are fixed in the business code, so a separate collection remains mandatory.
    catalog=read(PROJECT/'catalog.json')
    for major in catalog.get('majors',[]):
        for key,kind in [('paper_dataset_id','body'),('image_dataset_id','images')]:
            major[key]=str(uuid.uuid5(uuid.NAMESPACE_URL,'paperflow-migration-test/v1/'+str(major.get('code'))+'/'+kind))
    write(PROJECT/'catalog.json',catalog)
    original=read(META/'原始源码清单.json');files={}
    for p in original['files']:
        relative=Path(p).relative_to(Path(ORIGINAL_APP));target=APP/relative
        if not target.is_file():raise FileNotFoundError(target)
        files[str(target)]=sha(target)
    write(PROJECT/'source-manifest.json',{'release':str(release),'files':files})
    write(PROJECT/'config.json',cfg)
    write(FIRST/'config.json',cfg)
    write(FIRST/'model-config.json',model)
    write(FIRST/'runtime-config.json',runtime)
    # Keep the current override layer, base packages, and user packages together.
    venv_path=PROJECT/'.venv'
    venv.EnvBuilder(system_site_packages=True,with_pip=False).create(venv_path)
    target_site=venv_path/'Lib/site-packages'
    shutil.copytree(ENV/'project-site-packages',target_site,dirs_exist_ok=True)
    (target_site/'migration-user-packages.pth').write_text(str(ENV/'user-site-packages')+'\n',encoding='utf-8')
    write(RUN/'安装绑定.json',{'root':str(ROOT),'app':str(APP),'environment':str(ENV),
      'embedding':str(emb),'configured_at':time.strftime('%Y-%m-%d %H:%M:%S'),
      'binding_id':str(uuid.uuid4()),
      'source_files':len(files),'patched_files':changed,'business_execution_enabled':False})
    write(META/'迁移配置变更.json',{'files':changed,'business_algorithms_changed':False,
       'test_configuration':{'credit_pool':False,'total_llm':1,'max_live_papers':1,'cleanup_uses_input_copies':True}})
    print(json.dumps({'configured':True,'root':str(ROOT),'source_files':len(files),
      'execution_enabled':False,'prefect_api_url':cfg['prefect_api_url']},ensure_ascii=False))

def apply_environment():
    os.environ.update(PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1',PYTHONNOUSERSITE='1',
      PREFECT_API_URL=settings()['prefect_api_url'],PREFECT_HOME=str(RUN/'prefect-home'),
      PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false',PREFECT_SERVER_ANALYTICS_ENABLED='false',
      HAYSTACK_TELEMETRY_ENABLED='false',HF_HUB_OFFLINE='1',
      PAPERFLOW_V3_PROJECT_ROOT=str(PROJECT),PAPERFLOW_V3_PROJECT_CONFIG=str(PROJECT/'config.json'),
      PAPERFLOW_CONFIG=str(PROJECT/'config.json'),PAPERFLOW_V3_FIRST_STEP_ROOT=str(FIRST),
      PAPERFLOW_V3_SITE_ROOT=str(WORKSPACE/'candidates/prefect-folder-frontend-20261101'),
      PAPERFLOW_V3_POOL='paperflow-migration-validation',
      PAPERFLOW_OFFICE_TEMP_ROOT=str(RUN/'lo-temp'),PAPERFLOW_LEGACY_APP=str(APP/'程序/desktop'),
      LIBREOFFICE_PATH=str(ENV/'LibreOffice/program/soffice.exe'),
      OLLAMA_MODELS=str(ENV/'nimble/models'))
    os.environ['PATH']=str(ENV/'LibreOffice/program')+os.pathsep+os.environ.get('PATH','')
    os.environ['PYTHONPATH']=os.pathsep.join([str(MAIN),str(PROJECT)])
    os.environ['NO_PROXY']='127.0.0.1,localhost,192.168.1.120,::1'
    os.environ['OLLAMA_HOST']='127.0.0.1:11434'
    for p in [str(PROJECT),str(MAIN)]:
        if p not in sys.path:sys.path.insert(0,p)

def offline_check():
    apply_environment()
    def audit(event,args):
        if event in {'socket.connect','socket.getaddrinfo','subprocess.Popen'}:
            raise RuntimeError('离线检查禁止网络连接和启动外部程序')
    sys.addaudithook(audit)
    results=[]
    def check(name,action):
        try:
            value=action();results.append({'name':name,'ok':bool(value),'detail':value})
        except Exception as exc:results.append({'name':name,'ok':False,'error':type(exc).__name__+': '+str(exc)[:200]})
    check('python_version',lambda:sys.version_info[:3]==(3,14,0))
    check('interpreter_in_bundle',lambda:Path(sys.base_prefix).resolve().is_relative_to(ROOT))
    for name in ['prefect','fastapi','uvicorn','paramiko','win32crypt','fitz','docx','pypdf','PIL','psutil','async_lru','paperflow_qdrant.delivery']:
        def load(n=name):
            module=importlib.import_module(n);where=Path(module.__file__).resolve()
            if not where.is_relative_to(ROOT):raise RuntimeError('模块来自迁移包外: '+str(where))
            return str(where)
        check('import_'+name,load)
    check('project_settings',lambda:local_url(read(PROJECT/'config.json')['prefect_api_url']))
    check('prefect_limit_schemas',lambda: all(hasattr(importlib.import_module('prefect.client.schemas.actions'),n) for n in ['GlobalConcurrencyLimitCreate','GlobalConcurrencyLimitUpdate']))
    from paperflow_prefect import legacy
    from paperflow_prefect.settings import Settings
    check('native_modules_and_paths',lambda:legacy.preflight(Settings.load(PROJECT/'config.json')))
    for name in ['paperflow_v3.control.flows','paperflow_v3.control.server','paperflow_v3.stages.pipeline','paperflow_v3.delivery.runtime']:
        check(name,lambda n=name: str(Path(importlib.import_module(n).__file__).resolve()))
    check('7zip_binary',lambda:(ENV/'7zip/7z.exe').is_file() and (ENV/'7zip/7z.dll').is_file())
    check('libreoffice_binary',lambda:(ENV/'LibreOffice/program/soffice.exe').is_file())
    check('nimble_binary_and_model',lambda:(ENV/'nimble/runtime/ollama.exe').is_file() and any((ENV/'nimble/models/manifests').rglob('9b-q4_K_M')))
    # A dictionary result also has meaningful internal success requirements.
    for result in results:
        if result['name']=='native_modules_and_paths' and isinstance(result.get('detail'),dict):
            d=result['detail'];result['ok']=d['template_exists'] and d['catalog_exists'] and all(d['counter_paths'].values())
    report={'mode':'offline','passed':all(r['ok'] for r in results),'checks':results,
      'binding_id':read(RUN/'安装绑定.json')['binding_id'],
      'network_allowed':False,'child_processes_allowed':False,'real_papers_processed':0}
    write(RUN/'检查报告/离线检查.json',report)
    print(json.dumps({'passed':report['passed'],'checks':len(results),'failures':[r for r in results if not r['ok']]},ensure_ascii=False))
    return 0 if report['passed'] else 1

def save_credentials():
    apply_environment()
    import win32crypt
    cfg=read(PROJECT/'config.json');destination=Path(cfg['settings_root']);destination.mkdir(parents=True,exist_ok=True)
    prompts=[('ai','专用测试模型业务API Key'),('qdrant','独立测试知识库API Key'),
      ('ai_vision_primary','视觉主通道API Key（不用可留空）'),('ai_vision_fallback','视觉备用通道API Key（不用可留空）')]
    for name,label in prompts:
        value=getpass.getpass(label+': ')
        if not value:continue
        encrypted=win32crypt.CryptProtectData(value.encode(),None,None,None,None,1)
        (destination/(name+'_key.dpapi')).write_bytes(encrypted)
        if name=='ai_vision_primary':(destination/'ai_vision_key.dpapi').write_bytes(encrypted)
    for name,label in [('server-monitor-8796.dpapi','服务器wzy用户SSH密码（主流程路由核对必需）'),('workbuddy-monitor-8795.dpapi','WorkBuddy网关管理密码（主流程账号探测必需）')]:
        value=getpass.getpass(label+'（已有可留空）: ')
        if value:
            p=Path(os.environ['LOCALAPPDATA'])/'Paperflow'/name;p.parent.mkdir(parents=True,exist_ok=True)
            p.write_bytes(win32crypt.CryptProtectData(value.encode(),None,None,None,None,1))
    print('凭据已按当前Windows用户加密保存；未打印或写入明文。')

def trusted_host():
    source=ROOT/'03_配置与启动/服务器公钥_known_hosts'
    lines=[s for s in source.read_text(encoding='utf-8').splitlines() if s.strip()]
    if not lines:raise RuntimeError('包中没有原电脑已信任的服务器公钥，请按交接文档确认指纹')
    target=Path.home()/'.ssh/known_hosts';target.parent.mkdir(parents=True,exist_ok=True)
    current=target.read_text(encoding='utf-8') if target.exists() else ''
    import paramiko
    incoming=paramiko.HostKeys(str(source)).lookup('192.168.1.120')
    existing=paramiko.HostKeys(str(target)).lookup('192.168.1.120') if target.exists() else None
    if existing and (not incoming or any(kind not in incoming or incoming[kind]!=key for kind,key in existing.items())):
        raise RuntimeError('新机已存服务器公钥与迁移包不同，保留原记录，请人工核对')
    additions=[s for s in lines if s not in current.splitlines()]
    if additions:
        with target.open('a',encoding='utf-8') as f:f.write(('\n' if current and not current.endswith('\n') else '')+'\n'.join(additions)+'\n')
    print('已合并原电脑已信任的目标服务器公钥；未连接服务器。')

def enable_execution():
    s=settings()
    if not s.get('test_route_prepared') or not s.get('test_gateway_storage_isolated'):
        raise RuntimeError('先在新电脑配置.json确认专用测试线路和独立测试知识库存储；不能使用正式论文Key')
    cfg=read(PROJECT/'config.json')
    for key in ['ai','qdrant']:
        if not (Path(cfg['settings_root'])/(key+'_key.dpapi')).is_file():raise RuntimeError('尚未保存'+key+'凭据')
    import paramiko
    for name in ['server-monitor-8796.dpapi','workbuddy-monitor-8795.dpapi']:
        if not (Path(os.environ['LOCALAPPDATA'])/'Paperflow'/name).is_file():raise RuntimeError('主流程路由核对尚缺凭据: '+name)
    hosts=Path.home()/'.ssh/known_hosts'
    if not hosts.is_file() or not paramiko.HostKeys(str(hosts)).lookup('192.168.1.120'):
        raise RuntimeError('请先合并并核对服务器公钥')
    if not s.get('shared_gateway_probe_acknowledged'):
        raise RuntimeError('现有开始操作会探测共享网关国内账号；请先确认该影响并填写配置确认项')
    report=read(RUN/'检查报告/离线检查.json')
    binding=read(RUN/'安装绑定.json')['binding_id']
    if not report['passed'] or report.get('binding_id')!=binding:raise RuntimeError('请在本次初始化后重新通过离线检查')
    counted=read(RUN/'检查报告/计数器检查.json')
    if not counted.get('passed') or counted.get('binding_id')!=binding:raise RuntimeError('请在本次初始化后重新通过计数器检查')
    cfg['execution_enabled']=True;write(PROJECT/'config.json',cfg);write(FIRST/'config.json',cfg)
    print('已启用本迁移副本的执行配置；未启动服务或提交论文。')

async def bootstrap_limits():
    apply_environment()
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate,GlobalConcurrencyLimitUpdate
    from prefect.exceptions import ObjectNotFound
    cfg=read(PROJECT/'config.json')
    async with get_client() as client:
        for key,limit in cfg['limits'].items():
            name='paperflow-connected-'+key
            try:
                row=await client.read_global_concurrency_limit_by_name(name)
                await client.update_global_concurrency_limit(row.id,GlobalConcurrencyLimitUpdate(limit=limit,active=True))
            except ObjectNotFound:
                await client.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name,limit=limit,active=True))
    print('已初始化本机测试Prefect并发额度。')

def counter_check():
    apply_environment()
    cfg=read(PROJECT/'config.json');from paperflow_prefect import legacy
    from paperflow_prefect.settings import Settings
    legacy.bind(Settings(cfg))
    from preextract_pipeline import offline_counter
    with offline_counter(read(PROJECT/'runtime-config.json'),Path(cfg['release_root'])/'core') as counter:
        value=counter.count_many(['迁移环境计数检查。'])
    write(RUN/'检查报告/计数器检查.json',{'passed':True,'result':value,'input':'synthetic text only',
      'binding_id':read(RUN/'安装绑定.json')['binding_id']})
    print(json.dumps({'counter_check':True,'result':value},ensure_ascii=False))

def verify_payload():
    rows=read(META/'交付文件SHA256.json');failures=[]
    for row in rows:
        path=(ROOT/row['path']).resolve()
        if not path.is_relative_to(ROOT) or not path.is_file() or path.stat().st_size!=row['bytes'] or sha(path)!=row['sha256']:
            failures.append(row['path'])
    print(json.dumps({'payload_integrity':not failures,'files':len(rows),'failures':failures},ensure_ascii=False))
    return 1 if failures else 0

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['configure','offline-check','save-credentials','trust-host','enable-execution','bootstrap-limits','counter-check','verify-payload'])
    args=parser.parse_args()
    if args.action=='configure':configure()
    elif args.action=='offline-check':raise SystemExit(offline_check())
    elif args.action=='save-credentials':save_credentials()
    elif args.action=='trust-host':trusted_host()
    elif args.action=='enable-execution':enable_execution()
    elif args.action=='bootstrap-limits':asyncio.run(bootstrap_limits())
    elif args.action=='counter-check':counter_check()
    elif args.action=='verify-payload':raise SystemExit(verify_payload())
