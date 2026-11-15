"""Storage-only worker around the native archive copier and guarded releaser."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def atomic(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+f'.{os.getpid()}.{threading.get_ident()}.tmp')
    try:
        temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
        os.replace(temporary,path)
    finally:
        temporary.unlink(missing_ok=True)


def policy_path(root):
    return Path(root)/'private/preextract_delivery/nas-policy.json'


class SelectionChanged(RuntimeError):pass


def activate_selected(base, activate):
    path=base/'config/source-update.json'
    before=path.read_bytes() if path.is_file() else b''
    selected=activate(base)
    if (path.read_bytes() if path.is_file() else b'')!=before:raise SelectionChanged()
    return selected,before


class ArchiveService:
    def __init__(self, root, backend, *, stop=None, poll_seconds=3, release_seconds=20,
                 version_current=lambda:True, version='',task_selection=None,selection_sha=''):
        self.root=Path(root);self.backend=backend
        self.stop=stop if stop is not None else threading.Event()
        self.poll_seconds=poll_seconds;self.release_seconds=release_seconds
        self.version_current=version_current;self.reconfigure=False
        self.policy=None;self.life=None
        self.task_selection=task_selection
        self.lock=threading.Lock()
        import psutil
        self.state=dict(schema='original-archive-worker/1',pid=os.getpid(),
            create_time=psutil.Process().create_time(),version=version,state='starting',
            task_scope_version=2,selection_sha256=selection_sha,
            selected_items=len(task_selection) if task_selection is not None else 0)
        self.status=self.root/'private/original_archive/worker-status.json'

    def publish(self, **fields):
        with self.lock:
            self.state.update(fields,observed_at=time.time())
            atomic(self.status,self.state)

    def enabled(self):
        while not self.stop.is_set():
            try:
                config=self.backend.policy(self.root)
                same_version=self.version_current()
                break
            except (OSError,ValueError):
                self.publish(policy_state='waiting_for_readable_policy')
                self.stop.wait(self.poll_seconds)
        else:return False
        if not config.get('enabled'):return False
        if self.policy is None:self.policy=json.loads(json.dumps(config))
        if config!=self.policy or not same_version:
            self.reconfigure=True
            return False
        return True

    def release_loop(self):
        while self.enabled():
            if not self.policy.get('release_enabled',True):
                self.publish(release_state='disabled',release_error='')
                if self.stop.wait(self.release_seconds):break
                continue
            try:
                selections=(self.backend.submitted_task_selections(self.root) if self.task_selection is None
                            else [self.task_selection])
                if not selections:
                    self.publish(release_state='held',release_error='explicit_selected_task_items_required')
                    if self.stop.wait(self.release_seconds):break
                    continue
                results=[];failures={}
                for selection in selections:
                    if self.stop.is_set():break
                    try:
                        items,held=self.backend.restore_task_items(self.root,selection,stop=self.stop)
                        if not items:
                            failures.update(held);continue
                        self.publish(release_state='checking')
                        self.backend.cleanup_once(self.root,task_items=items,stop=self.stop)
                        results.extend(self.backend.release_once(self.root,limit=2,stop=self.stop,task_items=items))
                    except Exception as exc:
                        failures[str(len(failures))]=type(exc).__name__
                self.publish(release_state='held' if failures else 'idle',release_checked_at=time.time(),
                    release_error='',release_held=dict(list(failures.items())[:64]),
                    last_release=[dict(status=r.get('status'),released_files=r.get('released_files',0),
                                      released_bytes=r.get('released_bytes',0),isolated_files=r.get('isolated_files',0),
                                      isolated_bytes=r.get('isolated_bytes',0),local_mode=r.get('local_mode'),
                                      reason=r.get('reason','')) for r in results][-64:])
            except Exception as exc:
                self.publish(release_state='retry',release_error=type(exc).__name__)
            if self.stop.wait(self.release_seconds):break

    def run(self):
        release=None
        try:
            if not self.enabled():return
            self.life=self.backend.lifecycle(self.root)
            if self.life is None:return
            release=threading.Thread(target=self.release_loop,name='nas-safe-release')
            release.start()
            while self.enabled():
                if not self.policy.get('copy_enabled',True):
                    self.publish(state='running',copy_state='disabled',copy_error='')
                    if self.stop.wait(self.poll_seconds):break
                    continue
                try:
                    self.publish(state='running',copy_state='checking')
                    # Native writer owns the per-job copy mutex, immutable NAS
                    # objects, read-back hash verification and durable backoff.
                    items,held=(self.backend.background_task_items(self.root,stop=self.stop)
                        if self.task_selection is None else
                        self.backend.restore_task_items(self.root,self.task_selection,stop=self.stop))
                    result=self.backend.copy_once(self.root,limit=1,stop=self.stop,task_items=items)
                    if items:self.backend.cleanup_once(self.root,task_items=items,stop=self.stop)
                    result.setdefault('held',{}).update(held)
                    self.publish(copy_state=result['state'],copy_error='',copy_checked_at=time.time(),
                        queue=dict(selected=result.get('selected',0),held_count=len(result.get('held',{}))),
                        held=dict(list(result.get('held',{}).items())[:64]))
                except Exception as exc:
                    self.publish(copy_state='retry',copy_error=type(exc).__name__)
                if self.stop.wait(self.poll_seconds):break
        finally:
            self.stop.set()
            # No release is abandoned while it holds file/admission handles.
            if release is not None:release.join()
            self.publish(state='stopped',copy_state='stopped',release_state='stopped')


def ensure_started(app_root, data_root=None, *,task_selection=None):
    """Called by the normal desktop entry; never starts a paper or uploader."""
    app_root=Path(app_root);paths=read(app_root/'config/paths.json')
    root=Path(data_root) if data_root is not None else Path(os.environ.get('PAPER_UPSTREAM_DATA_DIR',paths['data_dir']))
    if not policy_path(root).is_file():return dict(state='disabled')
    try:
        if not read(policy_path(root)).get('enabled'):return dict(state='disabled')
    except (OSError,ValueError):pass  # The service waits for a readable policy.
    # Optional, explicitly installed storage-only scope. It cannot mint another
    # admission or resume the manuscript task. Existing archive owners continue.
    launcher=app_root/'scripts/external_storage_worker.py'
    if launcher.is_file() and (root/'private/original_archive/external-tail/installation.json').is_file():
        try:
            import importlib.util
            spec=importlib.util.spec_from_file_location('_paperflow_external_storage_launcher',launcher)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            module.ensure_started(app_root,root)
        except (OSError,ValueError,KeyError,TypeError,RuntimeError):
            # An unavailable independent tail never changes native admissions.
            pass
    selection_sha='';selection_args=[]
    if task_selection is not None:
        if not isinstance(task_selection,list) or not task_selection:
            return dict(state='held',reason='explicit_selected_task_items_missing')
        # Internal worker-parameter transport, not another permission ledger.
        # A file avoids Windows' command-line limit for a 500-item selection.
        raw=json.dumps(dict(schema='nas-worker-selection/1',root=str(root.resolve()),items=task_selection),
                       ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf8')
        selection_sha=hashlib.sha256(raw).hexdigest()
        target=root/'private/original_archive'/('worker-selection-'+selection_sha+'.json')
        target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists():
            if target.read_bytes()!=raw:raise ValueError('NAS worker selection transport changed')
        else:
            temporary=target.with_suffix('.'+str(os.getpid())+'.tmp')
            try:
                with temporary.open('xb') as stream:
                    stream.write(raw);stream.flush();os.fsync(stream.fileno())
                os.replace(temporary,target)
            finally:temporary.unlink(missing_ok=True)
        selection_args=['--task-selection',str(target),'--task-selection-sha',selection_sha]
    status=root/'private/original_archive/worker-status.json'
    try:
        import psutil
        owner=read(status)
        if (owner.get('state') in {'starting','running'} and
            abs(psutil.Process(owner['pid']).create_time()-owner['create_time'])<.01):
            if owner.get('task_scope_version')!=2:
                return dict(state='held',reason='existing_nas_worker_scope_capability_unknown',pid=owner['pid'])
            if owner.get('selection_sha256','')!=selection_sha:
                return dict(state='held',reason='existing_nas_worker_owns_different_selection',pid=owner['pid'])
            return dict(state='running',pid=owner['pid'])
    except (OSError,ValueError,KeyError,psutil.Error):pass
    python=Path(paths['python']).with_name('pythonw.exe')
    if not python.is_file():python=Path(paths['python'])
    status.parent.mkdir(parents=True,exist_ok=True)
    with (status.parent/'worker.log').open('ab') as log:
        child=subprocess.Popen([str(python),'-B','-X','utf8',str(app_root/'scripts/original_archive_worker.py'),
            '--app-root',str(app_root),'--data-root',str(root),*selection_args],cwd=str(app_root),
            stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    return dict(state='starting',pid=child.pid)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--app-root',type=Path,default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--data-root',type=Path)
    parser.add_argument('--task-selection',type=Path)
    parser.add_argument('--task-selection-sha')
    args=parser.parse_args();base=args.app_root.resolve()
    paths=read(base/'config/paths.json')
    root=args.data_root or Path(os.environ.get('PAPER_UPSTREAM_DATA_DIR',paths['data_dir']))
    root=root.resolve()
    task_selection=None;selection_sha=''
    if args.task_selection is not None:
        context=args.task_selection.resolve()
        if not context.is_relative_to(root/'private/original_archive'):
            raise ValueError('NAS worker task selection belongs to another workspace')
        raw=context.read_bytes();selection_sha=hashlib.sha256(raw).hexdigest()
        if selection_sha!=args.task_selection_sha:
            raise ValueError('NAS worker task selection hash changed')
        value=json.loads(raw)
        if (value.get('schema')!='nas-worker-selection/1' or value.get('root')!=str(root)
                or not isinstance(value.get('items'),list) or not value['items']):
            raise ValueError('NAS worker task selection unavailable')
        task_selection=value['items']
    if not policy_path(root).is_file():return
    from filelock import FileLock,Timeout
    mutex=root/'private/original_archive/service.lock';mutex.parent.mkdir(parents=True,exist_ok=True)
    restart=False
    try:
        with FileLock(str(mutex),timeout=0):
            # Keep the whole desktop/core pair from the selected release.
            # The overlay contains only replaceable desktop modules and the
            # paperflow package. Native support modules remain in base/core.
            sys.path[:0]=[str(base/'scripts'),str(base/'desktop'),str(base/'core')]
            from source_update import activate_source_update
            selected,stamp=activate_selected(base,activate_source_update)
            if selected is None:
                sys.path[:0]=[str(base/'desktop'),str(base/'core')]
            selection=base/'config/source-update.json'
            import preextract_nas
            service=ArchiveService(root,preextract_nas,
                version_current=lambda:(selection.read_bytes() if selection.is_file() else b'')==stamp,
                version=selected.parent.name if selected else 'base',
                task_selection=task_selection,selection_sha=selection_sha)
            for name in ('SIGINT','SIGTERM','SIGBREAK'):
                if hasattr(signal,name):signal.signal(getattr(signal,name),lambda *_:service.stop.set())
            service.run();restart=service.reconfigure
    except Timeout:return
    except SelectionChanged:restart=True
    if restart:ensure_started(base,data_root=root,task_selection=task_selection)


if __name__=='__main__':main()
