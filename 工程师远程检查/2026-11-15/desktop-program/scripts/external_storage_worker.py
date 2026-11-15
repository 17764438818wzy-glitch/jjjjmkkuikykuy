"""Thin normal-start hook for an explicitly installed finite storage tail."""
import hashlib,json,os,subprocess
from pathlib import Path


def read(path):return json.loads(Path(path).read_text(encoding='utf8'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ensure_started(app_root,root):
    app_root,root=Path(app_root).resolve(),Path(root).resolve()
    folder=root/'private/original_archive/external-tail';reference=folder/'installation.json'
    if not reference.is_file():return dict(state='disabled')
    ref=read(reference);config_path=Path(ref['config_path']).resolve()
    if not config_path.is_relative_to(folder) or sha(config_path)!=ref['config_sha256']:raise ValueError('storage installation changed')
    config=read(config_path)
    if Path(config['app_root']).resolve()!=app_root:raise ValueError('storage installation belongs to another application')
    authority=read(config['authority_path'])
    if Path(authority['root']).resolve()!=root:raise ValueError('storage installation belongs to another workspace')
    worker=Path(config['worker_path']).resolve()
    if not worker.is_relative_to(app_root/'updates'):raise ValueError('storage worker is outside installed versions')
    for path,digest in config['module_hashes'].items():
        if sha(path)!=digest:raise ValueError('installed storage code changed')
    import psutil
    status=folder/'worker-status.json'
    if status.is_file():
        owner=read(status)
        try:
            process=psutil.Process(owner['pid'])
            if abs(process.create_time()-owner['create_time'])<.01 and process.is_running():
                cmd=process.cmdline()
                if owner.get('config_sha256')==ref['config_sha256'] and str(worker) in cmd:
                    return dict(state='running',pid=owner['pid'])
                return dict(state='held',reason='existing_storage_owner_differs')
        except psutil.NoSuchProcess:pass
    python=Path(config['python']);hidden=python.with_name('pythonw.exe')
    if hidden.is_file():python=hidden
    with (folder/'worker.log').open('ab') as log:
        child=subprocess.Popen([str(python),'-B','-X','utf8',str(worker),'--config',str(config_path),
            '--config-sha',ref['config_sha256'],'--watch'],cwd=app_root,stdin=subprocess.DEVNULL,
            stdout=log,stderr=subprocess.STDOUT,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    return dict(state='starting',pid=child.pid)
