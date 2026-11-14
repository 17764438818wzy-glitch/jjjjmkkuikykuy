"""Bounded, independent purpose classification before formal paper admission."""
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from queue import Empty, Full, Queue
import threading


class StagedAdmission:
    def __init__(self,sources,classify,*,workers=10,startup=50,ready_limit=100,
                 stop=None,on_change=None,global_control=None,batch_id='',diagnostic_root=None,can_dispatch=None):
        self.sources=sources;self.classify=classify
        self.workers=max(1,int(workers));self.startup=max(0,int(startup))
        self.ready_limit=max(self.startup,int(ready_limit))
        self.stop=stop if stop is not None else threading.Event()
        self.on_change=on_change
        self.can_dispatch=can_dispatch
        self.global_control,self.batch_id=global_control,batch_id
        self.diagnostic_root=diagnostic_root or getattr(global_control,'root',None)
        self.changed=threading.Condition(threading.RLock())
        self.bodies=deque();self.others=deque();self.deferred=deque();self.seen=set()
        self.groups={};self.groups_ready=deque()
        self.classified=0;self.active=0;self.associated=0;self.pending=0
        self.formal_started=self.startup==0;self.done=False;self.error='';self.thread=None
        self.failure={};self.preserved_completed=0
        self.source_exhausted=False;self.admission_interrupted=False
        self._producer_close=threading.Event()

    def admission_open(self):
        if self.admission_interrupted:return False
        poll=getattr(self.global_control,'poll_admission',None)
        allowed=(self.can_dispatch is None or self.can_dispatch()) and (
            self.global_control is None or (poll() if callable(poll) else self.global_control.snapshot()['admission_open']))
        if not allowed:self.admission_interrupted=True
        return allowed

    def interrupt(self):
        # A caller may be the only observer of a transient pause. Wake both
        # sides before waiting for them, without throwing away accepted rows.
        with self.changed:
            self.admission_interrupted=True
            self._producer_close.set()
            self.changed.notify_all()

    def snapshot(self):
        with self.changed:
            return dict(classified_files=self.classified,confirmed_bodies=len(self.seen),
                body_candidates=len(self.bodies),classification_active=self.active,
                classification_workers=self.workers,classification_done=self.done,
                formal_started=self.formal_started,associated_files=self.associated,
                purpose_pending=self.pending,auxiliary_candidates=len(self.others)+len(self.deferred),
                project_pending=sum(len(g['rows']) for g in self.groups.values()),
                startup_target=self.startup,ready_limit=self.ready_limit,
                admission_error=self.error,admission_failure=dict(self.failure),
                preserved_completed_results=self.preserved_completed,
                source_exhausted=self.source_exhausted,admission_interrupted=self.admission_interrupted)

    def _signal(self):
        self.changed.notify_all()
        if self.on_change:
            self.on_change(self.snapshot())

    def _append(self,row,*,preserve=False):
        lane=row.get('admission_lane','pending')
        if lane=='body':
            identity=row.get('source_hash') or row.get('path') or id(row)
            if identity in self.seen:
                self.deferred.append(dict(row,admission_lane='duplicate'))
                self._signal();return
            while not preserve and len(self.bodies)>=self.ready_limit and not self.stop.is_set() and self.admission_open():
                self.changed.wait(.1)
            if self.stop.is_set() and not preserve:return
            self.bodies.append(row)
            self.seen.add(identity)
            if len(self.seen)>=self.startup:self.formal_started=True
        else:
            self.others.append(row)
        self._signal()

    def _accept(self,row,*,preserve=False):
        if row is None:return
        self.classified+=1
        self.associated+=row.get('admission_lane')=='associated'
        self.pending+=row.get('admission_lane','pending')=='pending'
        group=row.get('_admission_group')
        if group:
            state=self.groups.setdefault(group,dict(rows=[],expected=int(row['_admission_group_size'])))
            state['rows'].append(row)
            if len(state['rows'])<state['expected']:
                self._signal();return
            rows=self.groups.pop(group)['rows']
        else:rows=[row]
        # Wait before publishing any of the group. A single group may exceed
        # the nominal body limit; splitting it would expose a body before its
        # attachment, and waiting for an oversized group to fit would deadlock.
        if group and any(member.get('admission_lane')=='body' for member in rows):
            while not preserve and len(self.bodies)>=self.ready_limit and not self.stop.is_set() and self.admission_open():
                self.changed.wait(.1)
        # Hold the condition for the whole group and never wait mid-publish.
        # Completed decisions survive a concurrent pause and remain unavailable
        # to new workers until the native admission gate permits them.
        for member in rows:self._append(member,preserve=preserve or bool(group))

    def _run(self):
        # Source expansion and durable binding remain ordered on one producer.
        # The collector is independent: a slow next(source) must not hide a
        # classification that has already completed. No item is queued before
        # its source generator has performed its existing binding checks.
        futures = {}
        source = iter(self.sources)
        ready_sources = Queue(maxsize=self.workers)
        producer_done = threading.Event()
        producer_close = self._producer_close
        producer_outcome = [None]

        def produce():
            kind, error = 'cancelled', None
            try:
                while not producer_close.is_set() and not self.stop.is_set():
                    if not self.admission_open():
                        kind = 'paused'
                        break
                    from contextlib import nullcontext
                    with self.global_control.stage('preparation', batch_id=self.batch_id) if self.global_control else nullcontext():
                        record = next(source)
                    while not producer_close.is_set() and not self.stop.is_set():
                        try:
                            ready_sources.put(record, timeout=.05)
                            with self.changed:
                                self.changed.notify_all()
                            break
                        except Full:
                            continue
            except StopIteration:
                kind = 'eof'
            except __import__('global_task_control').GlobalPaused:
                kind = 'paused'
            except Exception as exc:
                kind, error = 'error', exc
            finally:
                # Outcome and event are published together, before the collector
                # can interpret producer_done as success.
                with self.changed:
                    self.source_exhausted = kind == 'eof'
                    self.admission_interrupted = self.admission_interrupted or kind != 'eof'
                    producer_outcome[0] = (kind, error)
                    producer_done.set()
                    self.changed.notify_all()

        producer = threading.Thread(target=produce, name='purpose-source', daemon=True)

        def collect(completed, *, preserve=False):
            for future in completed:
                original = futures.pop(future)
                if future.cancelled():
                    continue
                try:
                    result = future.result()
                except Exception as exc:
                    result = dict(original, admission_lane='pending', admission_error=type(exc).__name__)
                with self.changed:
                    self.active = len(futures)
                    self._accept(result, preserve=preserve)
                    if preserve:
                        self.preserved_completed += 1

        try:
            with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix='purpose-admission') as pool:
                producer.start()
                try:
                    while True:
                        completed = [future for future in futures if future.done()]
                        collect(completed, preserve=self.stop.is_set())
                        if producer_done.is_set():
                            with self.changed:
                                kind, error = producer_outcome[0]
                            if kind == 'error':
                                raise error
                            if kind != 'eof':
                                break
                        if self.stop.is_set() or not self.admission_open():
                            with self.changed:
                                self.admission_interrupted = True
                            break
                        with self.changed:
                            full = len(self.bodies) >= self.ready_limit
                        while len(futures) < self.workers and not full and not self.stop.is_set() and self.admission_open():
                            try:
                                record = ready_sources.get_nowait()
                            except Empty:
                                break
                            futures[pool.submit(self.classify, record)] = record
                            with self.changed:
                                self.active = len(futures)
                                self._signal()
                                full = len(self.bodies) >= self.ready_limit
                        if producer_done.is_set():
                            with self.changed:
                                kind, error = producer_outcome[0]
                            if kind == 'error':
                                raise error
                            if kind == 'eof' and ready_sources.empty() and not futures:
                                with self.changed:
                                    if self.groups:self.admission_interrupted=True
                            if kind != 'eof' or (ready_sources.empty() and not futures):
                                break
                        if futures:
                            wait(futures, timeout=.05, return_when=FIRST_COMPLETED)
                        else:
                            # A set producer_done event is not a wait primitive:
                            # the ready-body queue may still be full. Wake on take,
                            # source publication, or a short bounded timeout.
                            with self.changed:
                                self.changed.wait(.05)
                finally:
                    # Close source prefetch before waiting for admitted workers.
                    producer_close.set()
                    for future in futures:
                        future.cancel()
            # Executor shutdown has joined admitted classifications. Preserve
            # their completed decisions even if a pause arrived during drain.
            collect([future for future in futures if future.done()], preserve=True)
        except Exception as exc:
            from processing_stop import record_failure
            record = (record_failure(self.diagnostic_root, exc, stage='purpose_admission', run_id=self.batch_id)
                      if self.diagnostic_root else None)
            with self.changed:
                self.error = type(exc).__name__
                self.failure = dict(error_type=self.error, record=record,
                    lock_kind=getattr(exc, 'lock_kind', None),
                    wait_seconds=getattr(exc, 'lock_wait_seconds', None),
                    location=getattr(exc, 'lock_location', None))
            collect([future for future in futures if future.done()], preserve=True)
        finally:
            producer_close.set()
            if producer.ident is not None:
                producer.join()
            if producer_outcome[0] and producer_outcome[0][0] == 'error' and not self.error:
                exc = producer_outcome[0][1]
                from processing_stop import record_failure
                record = (record_failure(self.diagnostic_root, exc, stage='purpose_admission', run_id=self.batch_id)
                          if self.diagnostic_root else None)
                with self.changed:
                    self.error = type(exc).__name__
                    self.failure = dict(error_type=self.error, record=record,
                        lock_kind=getattr(exc, 'lock_kind', None),
                        wait_seconds=getattr(exc, 'lock_wait_seconds', None),
                        location=getattr(exc, 'lock_location', None))
            with self.changed:
                self.active = 0
                self.done = True
                if not self.stop.is_set() and not self.error:
                    self.formal_started = True
                self._signal()

    def start(self):
        if self.thread is None:
            self.thread=threading.Thread(target=self._run,name='purpose-admission',daemon=True)
            self.thread.start()
        return self

    def take(self):
        with self.changed:
            if self.stop.is_set() or self.admission_interrupted or (self.can_dispatch is not None and not self.can_dispatch()):return None
            if self.formal_started and self.bodies:row=self.bodies.popleft()
            elif self.formal_started and self.deferred:row=self.deferred.popleft()
            elif self.others:row=self.others.popleft()
            else:return None
            self._signal();return row

    def close(self):
        if self.thread:self.thread.join()

    def wait_formal(self):
        """Also guard unfinished historical partitions which can send a request."""
        with self.changed:
            while not self.formal_started:
                if not self.admission_open():
                    from global_task_control import GlobalPaused
                    raise GlobalPaused('前置分类结果保留，等待继续')
                if self.stop.is_set():raise InterruptedError('前置分类已停止')
                if self.error:raise RuntimeError('前置分类失败：'+self.error)
                self.changed.wait(.1)
            if self.stop.is_set():raise InterruptedError('正文请求前已停止')

    def drained(self):
        with self.changed:return self.done and (self.admission_interrupted or
            (self.can_dispatch is not None and not self.can_dispatch()) or
            (not self.bodies and not self.others and not self.deferred))


def model_history_index(root):
    """Read the native workspace registry without creating its writer lock."""
    import json
    from pathlib import Path
    try:records=json.loads((Path(root)/'single-request/source-index.json').read_text(encoding='utf8'))
    except FileNotFoundError:return set()
    except (OSError,ValueError) as exc:raise RuntimeError('已有模型工作索引不可读，不能按新正文计数') from exc
    if not isinstance(records,dict):raise RuntimeError('已有模型工作索引格式无效')
    return {key.split('|')[0] for key,path in records.items()
            if any((Path(path)/'model'/name).exists() for name in ('request-state.json','partition-state.json'))}


def project_sources(processor,sources,run,on_discovered,*,batch_size=1,can_batch=None):
    """Expand only selected inputs, reusing the native archive extractor."""
    import shutil
    import uuid
    from pathlib import Path
    from engine import digest
    from material_intake import extract_material_archive
    from purpose_review import project_key,confirmed_container,scope
    from archive_identity import record_key
    groups={}
    for record in sources:groups.setdefault(project_key(record),[]).append(record)
    batch_size=max(1,min(16,int(batch_size)))

    def expand(record,depth=0):
        pressure=getattr(processor,'_storage_pressure',None)
        if pressure:pressure.wait(processor.stop)
        source=Path(record['path'])
        if source.suffix.lower() not in {'.zip','.rar','.7z'} or confirmed_container(record,processor.config['purpose_review']):
            return [record]
        sha=digest(source)
        try:
            if depth>=8:raise ValueError('归档嵌套过深')
            work=Path(run)/'private/admission/archives'/uuid.uuid4().hex
            work.mkdir(parents=True)
            original=work/('source'+source.suffix.lower());shutil.copy2(source,original)
            if digest(original)!=sha:raise ValueError('复制时源文件变化')
            rows=extract_material_archive(original,work/'archive')
            if any(row['status'] not in {'success','archive_extracted'} for row in rows):raise ValueError('存在未解压成员')
            result=[]
            for row in rows:
                if row['status']!='success':continue
                path=Path(row['extracted_path']).resolve()
                if not path.is_relative_to((work/'archive').resolve()):raise ValueError('归档路径越界')
                member=dict(path=str(path),name=path.name,format=path.suffix,size=path.stat().st_size,
                    input_source_path=record.get('input_source_path',str(source)),
                    archive_source_hash=record.get('archive_source_hash',sha),
                    archive_member_path='/'.join(v for v in (record.get('archive_member_path',''),path.relative_to((work/'archive').resolve()).as_posix()) if v))
                result.extend(expand(member,depth+1))
            if not result:raise ValueError('归档没有可读成员')
            return result
        except InterruptedError:raise
        except Exception as exc:
            return [dict(record,source_hash=sha,_admission_role=dict(kind='unknown',action='pending',
                confirmed=False,evidence_sha256=sha,reason='前置解包未完成，原件保留：'+type(exc).__name__))]

    def publish(pending):
        originals=[record for group,_ in pending for record in group]
        leaves=[record for _,group in pending for record in group]
        if processor.stop.is_set():return
        # One native binding still validates every byte and every archive
        # proof. Nothing becomes visible until that whole check succeeds.
        on_discovered(originals,leaves)
        for _,group in pending:
            leaf_groups={}
            for record in group:leaf_groups.setdefault(project_key(record),[]).append(record)
            for key,records in leaf_groups.items():
                for record in records:
                    record.update(_admission_group=key,_admission_group_size=len(records))
                    yield record

    pending=[]
    for originals in groups.values():
        if processor.stop.is_set():return
        # Do not make already prepared ordinary papers wait for a large archive.
        if pending and any(Path(record['path']).suffix.lower() in {'.zip','.rar','.7z'}
                or any(record.get(key) for key in ('archive_member_path','archive_source_hash','_native_task_source_evidence','source_education'))
                or (record.get('input_source_path') and Path(record['input_source_path']).resolve()!=Path(record['path']).resolve())
                for record in originals):
            yield from publish(pending)
            pending=[]
        with scope(processor,allow_requests=False):
            leaves=[]
            for record in originals:leaves.extend(expand(record))
            context=processor._material_context
            old={record_key(record) for record in originals}
            # Copy-on-write project lists stay stable for concurrent readers.
            context.records=[r for r in context.records if record_key(r) not in old]+leaves
            projects={}
            for record in context.records:projects.setdefault(project_key(record),[]).append(record)
            context._purpose_projects=projects
            processor._material_inputs=context.records
        ordinary = (len(originals)==len(leaves) and all(
            str(Path(original['path']).resolve())==str(Path(leaf['path']).resolve())
            and not any(record.get(key) for record in (original,leaf)
                        for key in ('archive_member_path','archive_source_hash','_native_task_source_evidence','source_education'))
            and all(not record.get('input_source_path') or Path(record['input_source_path']).resolve()==Path(record['path']).resolve()
                    for record in (original,leaf))
            for original,leaf in zip(originals,leaves)))
        ordinary = ordinary and (can_batch is None or can_batch(originals,leaves))
        if not ordinary:
            if pending:
                yield from publish(pending)
                pending=[]
            yield from publish([(originals,leaves)])
            continue
        pending.append((originals,leaves))
        if len(pending)>=batch_size:
            yield from publish(pending)
            pending=[]
    if pending:yield from publish(pending)


def classify_source(processor,record,run,run_id,purpose_gate,ledger,history,*,global_gate=None,purpose_hold=None):
    """Independent purpose worker. Never enters a formal preparation ticket."""
    pressure=getattr(processor,'_storage_pressure',None)
    if pressure:pressure.wait(processor.stop)
    import copy
    import uuid
    from pathlib import Path
    from engine import digest,write_json
    from associated_materials import material_kind
    from material_roles import classify_document
    from purpose_review import scope,confirmed_container,DOCUMENTS,IMAGES
    from processing_stop import stop_scope
    worker=copy.copy(processor);worker.agent=copy.copy(processor.agent)
    worker._purpose_admission_hold=purpose_hold
    worker.agent.purpose_gate=purpose_gate;worker.agent.ledger=ledger
    if global_gate is not None:worker.agent.llm_gate=global_gate
    identity=uuid.uuid4().hex
    worker.agent.billing_context=dict(run_id=run_id,item_id='PURPOSE_'+identity)
    worker.agent.on_usage=None;worker.agent.on_ai_step=None
    work=Path(run)/'private/admission/decisions'/identity
    work.mkdir(parents=True)
    expected=(record.get('_native_task_source_evidence') or {}).get('source_sha256') or record.get('source_hash') or record.get('sha256')
    sha=expected
    try:
        with stop_scope(processor.stop),scope(worker,work,allow_requests=True):
            actual=digest(Path(record['path']))
            if expected and actual!=expected:
                raise ValueError('已登记来源的内容发生变化，保留原身份等待核对')
            sha=actual
            role=record.get('_admission_role') or confirmed_container(record,processor.config['purpose_review'])
            if role is not None:
                from purpose_review import frontmatter_role
                role=frontmatter_role(record,processor.config['purpose_review'],role)
            if role is None:
                if Path(record['path']).suffix.lower() in DOCUMENTS|IMAGES:
                    role=classify_document(record,processor=worker,work=work)
                else:
                    kind=material_kind(record)
                    role=dict(kind=kind,action='associated' if kind in {'drawing','source_code'} else 'pending',
                        confirmed=kind in {'drawing','source_code'},reason='程序确认的静态项目附件' if kind in {'drawing','source_code'} else '用途尚未确认，保留原件',evidence_sha256=sha)
    except InterruptedError:raise
    except Exception as exc:
        from purpose_review import purpose_failure
        role=dict(kind='unknown',action='pending',confirmed=False,evidence_sha256=sha,
            reason='用途读取或处理未完成，原件保留；'+type(exc).__name__,purpose_failure=purpose_failure(exc))
    with stop_scope(processor.stop):
        result=record
        result.update(source_hash=sha,_admission_role=role)
        if role.get('purpose_failure'):result['admission_failure']=role['purpose_failure']
        if role.get('confirmed') and role.get('kind')=='document' and role.get('action')=='review':
            reference=record.get('prepared_checkpoint') or {}
            location=Path(reference['path']).parent if reference.get('path') else None
            saved=sha in history or (location is not None and any((location/'model'/n).exists() for n in ('request-state.json','partition-state.json')))
            result['admission_lane']='reuse' if saved else 'body'
        elif role.get('confirmed') and role.get('action')=='associated':result['admission_lane']='associated'
        else:result['admission_lane']='pending'
        result['_admission_decision_path']=str((work/'decision.json').resolve())
        write_json(work/'decision.json',result)
        return result
