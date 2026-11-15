"""Small operator adapter over the existing Manager and Scheduler.

The web tables group operator selections. Scheduling, retries and admission
remain owned by the existing workflow. Starting the web service stays paused.
"""
from pathlib import Path
import hashlib
import json
import sqlite3
import threading
import time
from urllib.parse import urlparse
import uuid

from .intake import EXTENSIONS
from .control import Manager
from .legacy_readback import article_status
from .managed import validate_scope, legacy_guard
from .package import sha, load, save
from .scheduler import Scheduler
from .store import Store


FORMATS = EXTENSIONS
MAX_UPLOAD = 2 * 1024 ** 3


class ControlConflict(ValueError):
    pass


class Workbench:
    def __init__(self, config, root, *, legacy_root, deployment=None):
        self.config, self.root = config, Path(root).resolve()
        self.legacy_root = Path(legacy_root).resolve()
        self.deployment = Path(deployment).resolve() if deployment else None
        self.active_deadline = None
        self.guard = None
        if self.root == self.legacy_root or config.source_roots or (config.production_pilot and not deployment):
            raise ValueError('web workbench requires an explicit operator selection')
        for value in (config.root, config.handoff_root, config.catalog_path,
                      config.delivery_acceptance_report or self.root.parent):
            if not Path(value).resolve().is_relative_to(self.root):
                raise ValueError('web configuration outside isolation root')
        protected = (self.legacy_root / 'langgraph_workflow' / 'runtime',
                     self.legacy_root / 'working' / 'codex-dispatcher')
        if any(self.root.is_relative_to(p) or p.is_relative_to(self.root) for p in protected):
            raise ValueError('web isolation overlaps formal runtime')
        if not deployment and urlparse(config.rag_url).hostname not in ('127.0.0.1', 'localhost', '::1'):
            raise ValueError('web acceptance release requires a loopback RAG endpoint')
        if deployment:
            self._check_deployment()
        self.command_lock = threading.Lock()
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.thread = None
        self.stop_watcher = None
        self.error = None
        self.failure = None
        self.manager = self.scheduler = self.store = None

    def _check_deployment(self):
        if not self.deployment:
            return None
        if not self.deployment.is_relative_to(self.root):
            raise ValueError('operator deployment outside owned root')
        item = load(self.deployment)
        if (item.get('mode') != 'operator-web' or item.get('endpoint') != self.config.rag_url
                or Path(item.get('root','')).resolve() != self.root
                or item.get('catalog_sha256') != sha(Path(self.config.catalog_path))
                or item.get('acceptance_sha256') != sha(Path(self.config.delivery_acceptance_report))
                or item.get('native_pipeline_id') != self.config.native_pipeline_id
                or item.get('maximum_selected') != 10
                or not 0 < item.get('maximum_seconds',0) <= 86400
                or not self.config.production_pilot
                or not Path(self.config.production_pilot).resolve().is_relative_to(self.root)):
            raise ValueError('operator deployment configuration changed or is invalid')
        return item

    def _issue_scope(self, rows):
        item = self._check_deployment()
        if item:
            # A logged-in explicit click grants this exact selection, never a directory scan.
            deadline = time.time() + item['maximum_seconds']
            save(self.config.production_pilot, dict(mode='production-pilot', root=str(self.root),
                endpoint=self.config.rag_url, catalog_sha256=item['catalog_sha256'],
                expires_at=deadline, maximum_seconds=item['maximum_seconds'],
                sources=[dict(path=r['path'], sha256=r['sha256']) for r in rows]))
            self.active_deadline = deadline
        validate_scope(self.config, self.root, [r['path'] for r in rows], self.legacy_root)

    def open(self):
        if self.thread:
            raise RuntimeError('workbench already opened')
        self.thread = threading.Thread(target=self._run, name='paperflow-web-session', daemon=True)
        self.thread.start()
        if not self.ready.wait(15) or self.error:
            raise RuntimeError('工作台管理进程初始化失败，请检查配置与旧执行器状态') from self.failure

    def _run(self):
        try:
            self.guard = legacy_guard(self.legacy_root)
            self.guard.__enter__()
            self.config.prepare()
            self.store = Store(self.config.root / 'state.sqlite3', self.config.contract_version)
            with self.store.db() as db:
                db.executescript('''
                    CREATE TABLE IF NOT EXISTS web_inputs (
                        id TEXT PRIMARY KEY,name TEXT NOT NULL,path TEXT NOT NULL,
                        sha256 TEXT NOT NULL,bytes INTEGER NOT NULL,created REAL NOT NULL);
                    CREATE TABLE IF NOT EXISTS web_batches (
                        id TEXT PRIMARY KEY,request_id TEXT UNIQUE NOT NULL,selection TEXT NOT NULL,
                        task_ids TEXT NOT NULL,created REAL NOT NULL);
                ''')
            self.manager = Manager(self.store, self.root / 'manager',remote_harness=self.config.remote_harness)
            self.manager.transition_guard = self.guard
            self.manager.start()
            self.scheduler = Scheduler(self.manager, self.config, isolation_root=self.root)
            if self.deployment:
                self.scheduler.service.select_tasks([],None)
            self.stop_watcher = threading.Thread(target=self._watch_stop,name='paperflow-web-stop',daemon=True)
            self.stop_watcher.start()
            tick = self.scheduler.tick
            def observed_tick():
                self.guard.check()
                if (self.root / 'STOP').exists() and not self.manager.stopped.is_set():
                    self.manager.pause('operator STOP file')
                result = tick()
                # Do this only after discovery/reaping in the same complete tick.
                # A handed-off graph may have just created a delivery job.
                from .scheduling_store import selection_filter
                selected,parameters=selection_filter(self.scheduler.service.selected_tasks)
                jobs = self.scheduler.ledger.rows('SELECT j.state FROM scheduler_jobs j WHERE 1'+selected,parameters)
                if (jobs and not any(j['state'] in ('queued','retry','blocked_service','running') for j in jobs)
                        and not self.manager.stopped.is_set() and not self.manager.status()['live_units']):
                    self.manager.pause('selected work settled')
                return result
            self.scheduler.tick = observed_tick
            self.ready.set()
            self.scheduler.run(self.stop, interval=.15)
        except BaseException as exc:
            self.failure = exc
            self.error = type(exc).__name__
            if self.manager:
                self.manager.pause('web session failed')
            self.ready.set()
        finally:
            self.stop.set()
            if self.stop_watcher:
                self.stop_watcher.join(1)
            if self.manager:
                try:
                    self.manager.shutdown(timeout=35)
                    from .office import cleanup_office_temps
                    save(self.root/'office-temp-cleanup.json',cleanup_office_temps(self.config))
                    self.guard.check()
                    self.guard.__exit__(None,None,None)
                except Exception as exc:
                    self.error = type(exc).__name__
            elif self.guard and self.guard.stack:
                self.guard.__exit__(None,None,None)

    def _watch_stop(self):
        previously_present = False
        expired_deadline = None
        while not self.stop.wait(.05):
            try:
                present = (self.root / 'STOP').exists()
                if self.active_deadline and time.time() >= self.active_deadline and self.active_deadline != expired_deadline:
                    self.manager.pause('operator selection time limit')
                    expired_deadline = self.active_deadline
            except OSError:
                self.manager.pause('STOP state unknown')
                return
            if present and not previously_present:
                # Advance the generation even when already paused: an older
                # /start request may currently be waiting on a database writer.
                self.manager.pause('operator STOP file')
            previously_present = present

    def close(self):
        if self.manager:
            self.manager.pause('workbench shutdown')
        self.stop.set()
        if self.thread:
            self.thread.join(40)
            if self.thread.is_alive() or self.error:
                raise RuntimeError('工作台退出尚未确认；请保留台账并核对进程树')

    def control_state(self):
        if not self.manager or self.error or not self.thread or not self.thread.is_alive():
            raise RuntimeError('管理进程离线，状态未知；拒绝新动作')
        return self.manager.status()

    def _expected(self, epoch, generation=None):
        state = self.control_state()
        if epoch != state['epoch'] or (generation is not None and generation != state['generation']):
            raise ControlConflict('控制状态已变化，请刷新后明确操作')
        return state

    def pause(self, epoch):
        self._expected(epoch)
        # Deliberately no command_lock and no workflow SQLite on the stop path.
        return {'manager': self.manager.pause('web operator pause')}

    def resume(self, epoch, generation, batch_id):
        if type(generation) is not int:
            raise ControlConflict('缺少控制代次，请刷新页面')
        with self.command_lock:
            self._expected(epoch, generation)
            if (self.root / 'STOP').exists():
                raise ControlConflict('STOP 文件仍在，不能继续')
            with self.store.db(timeout=.1) as db:
                batch = db.execute('SELECT * FROM web_batches ORDER BY created DESC LIMIT 1').fetchone()
                if not batch or batch['id'] != batch_id:
                    raise ControlConflict('历史批次仅供查看，请返回当前批次后继续')
                rows = [dict(db.execute('SELECT * FROM web_inputs WHERE id=?',(key,)).fetchone())
                        for key in json.loads(batch['selection'])] if batch else []
            if not rows:
                raise ValueError('请先选择原稿并开始一个批次')
            for row in rows:
                if sha(Path(row['path'])) != row['sha256']:
                    raise ValueError('上传副本已改变，请核对后重新选择')
            self._issue_scope(rows)
            try:
                if self.deployment:
                    self.scheduler.service.select_tasks(json.loads(batch['task_ids']),self.active_deadline)
                if (self.root / 'STOP').exists():
                    self.manager.pause('operator STOP file')
                    raise ValueError('operator STOP file')
                self.manager.resume(expected_generation=generation)
            except ValueError as exc:
                raise ControlConflict('任务仍在收束或控制状态已变化，请刷新后继续') from exc
            return {'manager': self.manager.status()}

    @staticmethod
    def check_name(name):
        if (not isinstance(name, str) or not name or len(name) > 240
                or Path(name).name != name or any(c in name for c in ('/', '\\', ':'))
                or any(ord(c) < 32 for c in name) or Path(name).suffix.lower() not in FORMATS):
            raise ValueError('文件名或格式不支持；请选择现有链路支持的正文或附件格式')
        return name

    def record_upload(self, name, path):
        self.check_name(name)
        path = Path(path).resolve()
        if not path.is_relative_to(self.root / 'uploads') or not path.is_file():
            raise ValueError('upload outside owned input root')
        digest = sha(path)
        identity = digest[:32]
        with self.store.db(timeout=.1) as db:
            prior = db.execute('SELECT * FROM web_inputs WHERE sha256=?', (digest,)).fetchone()
            if prior:
                path.unlink()
                if path.parent != self.root / 'uploads':
                    path.parent.rmdir()
                return dict(prior)
            db.execute('INSERT INTO web_inputs VALUES(?,?,?,?,?,?)',
                       (identity, name, str(path), digest, path.stat().st_size, time.time()))
            return dict(db.execute('SELECT * FROM web_inputs WHERE id=?', (identity,)).fetchone())

    def inputs(self):
        with self.store.db(timeout=.1) as db:
            return [dict(row) for row in db.execute('SELECT id,name,bytes,created FROM web_inputs ORDER BY created DESC LIMIT 256')]

    def start(self, selections, request_id, epoch, generation):
        if (not isinstance(selections, list) or not 1 <= len(selections) <= 10
                or any(not isinstance(x,str) for x in selections) or len(set(selections)) != len(selections)
                or not isinstance(request_id,str) or not 1 <= len(request_id) <= 100):
            raise ValueError('请选择 1–10 个不同文件，并提供有效提交编号')
        if type(generation) is not int:
            raise ControlConflict('缺少控制代次，请刷新页面')
        with self.command_lock:
            state = self._expected(epoch, generation)
            with self.store.db(timeout=.1) as db:
                prior = db.execute('SELECT * FROM web_batches WHERE request_id=?', (request_id,)).fetchone()
                if prior:
                    if json.loads(prior['selection']) != selections:
                        raise ValueError('相同提交编号不能更换文件')
                    # A replay is a receipt lookup, never a second resume.
                    return {'batch_id': prior['id'], 'manager': self.manager.status()}
                rows = [db.execute('SELECT * FROM web_inputs WHERE id=?', (key,)).fetchone() for key in selections]
                latest = db.execute('SELECT task_ids FROM web_batches ORDER BY created DESC LIMIT 1').fetchone()
            if any(row is None for row in rows):
                raise ValueError('所选文件不存在，请刷新页面')
            from .scheduling_store import selection_filter
            selected,parameters=selection_filter(json.loads(latest['task_ids']) if latest else [])
            if state['state'] != 'paused' or self.scheduler.ledger.rows(
                    "SELECT 1 FROM scheduler_jobs j WHERE state IN ('queued','retry','running','blocked_service')"+selected+' LIMIT 1',parameters):
                raise ControlConflict('请先完成或继续当前批次，再建立新批次')
            if (self.root / 'STOP').exists():
                raise ControlConflict('STOP 文件仍在，不能开始')
            for row in rows:
                if sha(Path(row['path'])) != row['sha256']:
                    raise ValueError('上传副本已改变，请重新选择原稿')
            self._issue_scope(rows)
            batch_id = uuid.uuid4().hex
            with self.store.db() as db:
                tasks = [self.store.register_source_sql(db,r['path'],r['sha256'],display_name=r['name'])['task_id'] for r in rows]
                db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',
                           (batch_id, request_id, json.dumps(selections), json.dumps(tasks), time.time()))
            # A pause while registration was waiting invalidates this request.
            # Manager checks the generation inside its own final memory gate.
            try:
                if self.deployment:
                    self.scheduler.service.select_tasks(tasks,self.active_deadline)
                if (self.root / 'STOP').exists():
                    self.manager.pause('operator STOP file')
                    raise ValueError('operator STOP file')
                self.manager.resume(expected_generation=generation)
            except ValueError as exc:
                raise ControlConflict('已保存所选批次，暂停优先生效；请刷新后明确继续') from exc
            return {'batch_id': batch_id, 'manager': self.manager.status()}

    def status(self, batch_id=None):
        from .workbench_view import status
        return status(self, batch_id)
