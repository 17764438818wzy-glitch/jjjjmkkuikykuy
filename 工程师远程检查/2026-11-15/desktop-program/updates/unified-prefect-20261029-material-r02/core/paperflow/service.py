"""持续调度：独占实例、队列分流、背压和正常关闭。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import shutil
import threading
import time
import uuid

from .graph import LLM_STAGES, LOCAL_STAGES, RAG_STAGES, Workflow
from .intake import Intake
from .package import save
from .store import Store
from .errors import controller_fields


from .instance_lock import InstanceLock


class Service:
    def __init__(self,config,workflow_factory=None):
        self.config=config
        config.prepare()
        self.store=Store(config.root/'state.sqlite3',config.contract_version)
        self.intake=Intake(config,self.store)
        self.workflow=(workflow_factory or Workflow)(config,self.store)
        self.running=False
        self.paused=False
        self.last_scan=0
        self.futures={}
        self.owner=str(os.getpid())+'-'+uuid.uuid4().hex[:8]
        self.pools={}
        self.last_error=None
        self.maintenance_future=None
        self.last_maintenance=time.time()

    def backpressure(self):
        counts=self.store.counts()
        if sum(counts.get(s,0) for s in RAG_STAGES)>=self.config.max_ingest_backlog:
            return True
        if shutil.disk_usage(self.config.root).free/2**30<self.config.min_free_gib:
            return True
        try:
            import psutil
            if psutil.virtual_memory().percent>=self.config.max_memory_percent:
                return True
        except ImportError:
            pass
        return False

    def allowed_stages(self,lane):
        if self.paused:
            return []
        if lane=='llm':
            return [] if self.backpressure() else list(LLM_STAGES)
        return list(RAG_STAGES if lane=='rag' else LOCAL_STAGES)

    def status(self):
        from .readiness import readiness
        metrics=self.store.metrics()
        return dict(running=self.running,paused=self.paused,tasks=self.store.counts(),
                    display_counts=metrics['display_counts'],
                    active=len(self.futures),backpressure=self.backpressure(),last_error=self.last_error,
                    last_scan=self.last_scan,updated_at=time.time(),metrics=metrics,alerts=metrics['alerts'],
                    acceptance=readiness(self.config))

    def tick(self):
        if hasattr(self.workflow,'apply_pending_requests'):self.workflow.apply_pending_requests()
        active_ids=[task['task_id'] for _,task in self.futures.values()]
        self.store.heartbeat(active_ids,self.owner,lease_seconds=self.config.lease_seconds)
        for future,(lane,task) in list(self.futures.items()):
            if future.done():
                try:
                    future.result()
                except Exception as exc:
                    self.last_error=type(exc).__name__
                    self.store.update(task['task_id'],**controller_fields(self.config,task,task['stage'],exc))
                del self.futures[future]
        capacities=dict(llm=self.config.llm_workers,rag=self.config.rag_workers,local=self.config.local_workers)
        for lane,capacity in capacities.items():
            if lane not in self.pools:
                self.pools[lane]=ThreadPoolExecutor(max_workers=capacity,thread_name_prefix='paperflow-'+lane)
            used=sum(1 for group,_ in self.futures.values() if group==lane)
            if lane=='rag' and self.maintenance_future is not None:used+=1
            for slot in range(capacity-used):
                repairs=sum(1 for group,t in self.futures.values() if group=='llm' and t['kind']=='repair')
                reserve=lane=='llm' and capacity>1 and repairs==0 and slot==capacity-used-1
                task=self.store.claim(self.allowed_stages(lane),self.owner,lease_seconds=self.config.lease_seconds,
                                      exclude_ids=[t['task_id'] for _,t in self.futures.values()],
                                      prefer_kind='repair' if reserve else None)
                if task is None:
                    break
                future=self.pools[lane].submit(self.workflow.step,task)
                self.futures[future]=(lane,task)
        if self.maintenance_future and self.maintenance_future.done():
            try:self.maintenance_future.result()
            except Exception as exc:
                self.last_error='maintenance:'+type(exc).__name__
                self.store.event(None,'maintenance_error',dict(type=type(exc).__name__))
            self.maintenance_future=None
        rag_used=sum(1 for lane,_ in self.futures.values() if lane=='rag')
        if (self.running and not self.paused and self.maintenance_future is None and rag_used==0 and
                time.time()-self.last_maintenance>=self.config.maintenance_seconds):
            from .maintenance import maintain
            self.last_maintenance=time.time()
            def maintenance():
                # 与 activate 同序：先取 RAG 名额，再在 maintain 中取激活锁。
                with self.workflow.rag_slots:
                    return maintain(self.config,self.store,self.workflow._publisher())
            self.maintenance_future=self.pools['rag'].submit(maintenance)
        save(self.config.root/'status.json',self.status())
        return self.status()

    def run(self,stop_event):
        from .readiness import require_ready
        require_ready(self.config)
        with InstanceLock(self.config.root/'controller.lock'):
            from .producer import cleanup_worker_homes
            cleanup_worker_homes(self.config)
            self.store.recover_leases()
            self.running=True
            try:
                while not stop_event.is_set():
                    if not self.paused and time.time()-self.last_scan>=self.config.scan_seconds:
                        # 先接管已交付成果，再发现原目录，避免给已有成果重复启动第一轮。
                        try:
                            self.intake.import_handoffs()
                            self.intake.scan()
                            self.last_scan=time.time()
                        except Exception as exc:
                            self.last_error=type(exc).__name__
                    self.tick()
                    stop_event.wait(1)
            finally:
                self.paused=True
                for pool in self.pools.values():
                    pool.shutdown(wait=True)
                self.pools.clear()
                # 完成的future已由工作流落台账，退出前刷新显示。
                self.futures.clear()
                self.running=False
                save(self.config.root/'status.json',self.status())

    def close(self):
        if self.running:
            raise RuntimeError('stop controller before close')
        for pool in self.pools.values():
            pool.shutdown(wait=True)
        self.workflow.close()
