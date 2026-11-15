"""Coalesced reads over one current display index; never repairs business history."""
import asyncio
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime,timezone
import json
from pathlib import Path
from uuid import UUID

from async_lru import alru_cache
from fastapi import HTTPException
from prefect.exceptions import ObjectNotFound

from .dashboard_cache import aggregate,compact_folder,merge_folder


class DashboardReader:
    def __init__(self,source,cache_dir,*,ttl=2):
        self.source,self.cache_dir=source,Path(cache_dir)
        self.index={}
        self.refreshes={}
        self.failed=set()
        self.active_reads=set()
        self.task_reads={}
        self.forgotten=set()
        self.detail_keys=OrderedDict()
        self.closed=False
        self.reads=alru_cache(maxsize=128,ttl=ttl)(self._read)
        self.detail_reads=alru_cache(maxsize=128,ttl=ttl)(self._details)

    def path(self,task_id):return self.cache_dir/(str(UUID(str(task_id)))+'.json')

    def load(self,task_id):
        self.check_task(task_id)
        if task_id not in self.index:
            try:
                saved=json.loads(self.path(task_id).read_text(encoding='utf8'))
                if saved.get('version')!=1 or saved.get('task_id')!=task_id:raise ValueError('Display cache version mismatch')
                self.index[task_id]=saved
            except FileNotFoundError:return None
        return self.index[task_id]

    def save(self,task_id,value):
        self.check_task(task_id)
        self.cache_dir.mkdir(parents=True,exist_ok=True)
        temporary=self.path(task_id).with_suffix('.tmp')
        temporary.write_text(json.dumps(value,ensure_ascii=False,separators=(',',':')),encoding='utf8')
        temporary.replace(self.path(task_id))
        self.index[task_id]=value

    def view(self,saved,*,stale=False):
        return {**saved['header'],'task_id':saved['task_id'],'folders':[], 'summary':saved['summary'],
                'classification':saved['classification'],'as_of':saved['as_of'],'complete':saved['complete'],
                'has_pending':None if stale else saved['header'].get('desired_state')=='running' or
                    (saved['header'].get('pause_progress') or {}).get('state')=='pausing',
                'query_status':'stale' if stale else 'updated','compatibility':saved.get('compatibility',{})}

    async def prime(self,task_id):
        task_id=str(UUID(str(task_id)))
        async with self.track_read(task_id):
            fresh=await self.source.observe(task_id,None,prime=True)
            self.check_task(task_id)
        saved=self.apply(task_id,None,fresh)
        self.save(task_id,saved);self.reads.cache_invalidate(task_id)
        return self.view(saved)

    def apply(self,task_id,saved,fresh):
        rows=dict(saved['rows']) if saved else {}
        for folder in fresh['folders']:
            if folder.get('task_id')!=task_id:raise ValueError('Folder belongs to another task')
            row=compact_folder(folder);before=rows.get(row['ticket_id'])
            rows[row['ticket_id']]=merge_folder(before,row) if before else row
        if len(rows)!=fresh['total']:raise ValueError('Folder identities changed; automatic history rebuild is disabled')
        if len({row['folder_id'] for row in rows.values()})!=len(rows):raise ValueError('Duplicate source folder identity')
        value={'version':1,'task_id':task_id,'rows':rows,'summary':aggregate(rows.values()),
               'header':fresh['header'],'classification':fresh['classification'],'as_of':fresh['as_of'],
               'complete':fresh['complete'],'active_ids':fresh['active_ids'],'end_cursor':fresh['end_cursor'],
               'compatibility':fresh.get('compatibility',{}), 'source_state':fresh.get('source_state',{})}
        if saved and all(value[key]==saved.get(key) for key in value if key!='as_of'):
            value['as_of']=saved['as_of']
        return value

    async def summary(self,task_id):
        if self.closed:raise RuntimeError('Dashboard reader is closed')
        task_id=str(UUID(str(task_id)))
        saved=self.load(task_id)
        if saved is None:return await self.reads(task_id)
        pending=self.refreshes.get(task_id)
        if pending is None or pending.done() and not self.reads.cache_contains(task_id):
            pending=asyncio.create_task(self.reads(task_id))
            self.refreshes[task_id]=pending
            # Observe errors even when the initiating HTTP request has ended.
            pending.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
        value=self.view(saved,stale=task_id in self.failed)
        if value['query_status']!='stale':value['query_status']='reused'
        return value

    def check_task(self,task_id):
        if self.closed:raise RuntimeError('Dashboard reader is closed')
        if task_id in self.forgotten:raise HTTPException(404,'Task display cache was deleted')

    @asynccontextmanager
    async def track_read(self,task_id):
        self.check_task(task_id)
        task=asyncio.current_task()
        self.active_reads.add(task)
        self.task_reads.setdefault(task_id,set()).add(task)
        try:yield
        finally:
            self.active_reads.discard(task)
            own=self.task_reads.get(task_id,set())
            own.discard(task)
            if not own:self.task_reads.pop(task_id,None)

    async def _read(self,task_id):
        async with self.track_read(task_id):
            return await self._observe(task_id)

    async def details(self,task_id,ids):
        task_id=str(UUID(str(task_id)))
        self.check_task(task_id)
        key=(task_id,tuple(sorted(set(ids))))
        # Failed/expired alru entries must not evict still-cached keys from this index.
        for cached in tuple(self.detail_keys):
            if not self.detail_reads.cache_contains(*cached):self.detail_keys.pop(cached,None)
        self.detail_keys[key]=None
        self.detail_keys.move_to_end(key)
        if len(self.detail_keys)>128:self.detail_keys.popitem(last=False)
        value=await self.detail_reads(*key)
        self.check_task(task_id)
        return value

    async def _details(self,task_id,ids):
        async with self.track_read(task_id):
            value=await self.source.details(task_id,list(ids))
            self.check_task(task_id)
            return value

    async def classifications(self,task_id):
        task_id=str(UUID(str(task_id)))
        async with self.track_read(task_id):
            value=await self.source.classifications(task_id)
            self.check_task(task_id)
            return value

    def cache_files(self,task_id,protected_paths):
        root=self.cache_dir.resolve()
        if self.cache_dir.is_symlink() or self.cache_dir.is_junction():
            raise ValueError('Display cache directory is linked')
        protected=[Path(path).resolve() for path in protected_paths]
        files=[self.path(task_id),self.path(task_id).with_suffix('.tmp')]
        for path in files:
            resolved=path.resolve()
            if path.is_symlink() or path.is_junction() or resolved.parent!=root:
                raise ValueError('Display cache file is outside its owner')
            if any(resolved==item or resolved.is_relative_to(item) for item in protected):
                raise ValueError('Display cache overlaps source originals or archive')
        return files

    async def forget_task(self,task_id,*,protected_paths=()):
        task_id=str(UUID(str(task_id)))
        self.cache_files(task_id,protected_paths)
        self.forgotten.add(task_id)
        reads=set(self.task_reads.get(task_id,()))
        reads.discard(asyncio.current_task())
        for task in reads:
            if not task.done() and not task.cancelling():task.cancel()
        if reads:
            await asyncio.wait_for(asyncio.shield(asyncio.gather(*reads,return_exceptions=True)),120)
        wrapper=self.refreshes.pop(task_id,None)
        if wrapper is not None:
            if not wrapper.done():wrapper.cancel()
            await asyncio.gather(wrapper,return_exceptions=True)
        self.reads.cache_invalidate(task_id)
        for key in list(self.detail_keys):
            if key[0]==task_id:
                self.detail_reads.cache_invalidate(*key)
                self.detail_keys.pop(key,None)
        self.failed.discard(task_id)
        self.index.pop(task_id,None)
        for path in self.cache_files(task_id,protected_paths):path.unlink(missing_ok=True)

    async def _observe(self,task_id):
        saved=self.load(task_id)
        try:
            fresh=await self.source.observe(task_id,saved)
            self.check_task(task_id)
            if saved is None and fresh['total']:
                raise HTTPException(409,{'code':'index_required','task_id':task_id,'total':fresh['total'],'delete_allowed':True})
            value=self.apply(task_id,saved,fresh)
            if value!=saved:self.save(task_id,value)
            self.failed.discard(task_id)
            return self.view(value)
        except ObjectNotFound:
            self.failed.discard(task_id)
            self.index.pop(task_id,None);self.path(task_id).unlink(missing_ok=True)
            raise HTTPException(404,'任务不存在')
        except HTTPException as exc:
            if exc.status_code==404:
                self.failed.discard(task_id)
                self.index.pop(task_id,None);self.path(task_id).unlink(missing_ok=True)
                raise
            if saved:
                self.failed.add(task_id)
                return self.view(saved,stale=True)
            raise
        except Exception:
            self.failed.add(task_id)
            if saved:return self.view(saved,stale=True)
            raise HTTPException(503,'进度暂不可用，尚无已保存数字')

    async def close(self):
        self.closed=True
        # Invalidated/evicted alru entries can still own a native client. Wait
        # for those actual reads' finally blocks before cancelling wrappers.
        reads=tuple(self.active_reads)
        for task in reads:
            if not task.done() and not task.cancelling():task.cancel()
        await asyncio.gather(*reads,return_exceptions=True)
        await self.reads.cache_close()
        await self.detail_reads.cache_close()
        for task in self.refreshes.values():
            if not task.done():task.cancel()
        await asyncio.gather(*self.refreshes.values(),return_exceptions=True)
