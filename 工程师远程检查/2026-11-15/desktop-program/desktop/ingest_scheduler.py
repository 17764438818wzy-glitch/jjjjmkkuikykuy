"""Bounded local scheduler for independent RAGFlow upload and parse work."""
from __future__ import annotations

import time
import uuid
import re
from concurrent.futures import Future, ThreadPoolExecutor

from ingest_contracts import Outcome
from ingest_gateway import SourceChanged, IdentityMismatch, ParsePreparationFailed, reconcile_write
from ingest_queue import QueueStore
from ingest_execution import (RemoteExecutionChanged, ExecutionUnconfirmed,
                              ArtifactEvidencePending, current_execution,
                              is_database_status_diagnostic)


_PARSE_ACTIVE = ('parse_intent', 'submitted', 'running')


class WorkPool:
    def __init__(self, queue: QueueStore, gateway_factory, classify_for,
                 on_accepted_for=None, upload_limit: int = 2,
                 parse_limit: int = 3, image_limit: int = 2,
                 post_limit: int = 2, poll_limit: int = 6,
                 service_limits: dict[str, int] | None = None,
                 dataset_limits: dict[str, int] | None = None,
                 pipeline_limits: dict[str, int] | None = None,
                 channel_limits: dict[str, int] | None = None,
                 router=None, diagnostics_reader=None, external_counts_reader=None,
                 on_invalidated_for=None, on_source_metadata=None):
        limits = (upload_limit, parse_limit, image_limit, post_limit, poll_limit)
        if any(not isinstance(value, int) or value < 1 for value in limits):
            raise ValueError('Pool limits must be positive integers')
        self.queue = queue
        self.gateway_factory = gateway_factory
        self.classify_for = classify_for
        self.on_accepted_for = on_accepted_for
        self.on_invalidated_for = on_invalidated_for
        self.on_source_metadata = on_source_metadata
        self.upload_limit = upload_limit
        self.parse_limit = parse_limit
        self.image_limit = image_limit
        self.post_limit = post_limit
        self.poll_limit = poll_limit
        self.service_limits = service_limits or {}
        self.dataset_limits = dataset_limits or {}
        self.pipeline_limits = pipeline_limits or {}
        self.channel_limits = channel_limits or {}
        self.router=router
        self.diagnostics_reader=diagnostics_reader
        self.external_counts_reader=external_counts_reader
        self._halted_channels: set[str] = set()
        self._paused_services: dict[str,float] = {}
        self.owner = uuid.uuid4().hex
        self._upload_pool = ThreadPoolExecutor(max_workers=upload_limit)
        self._image_pool = ThreadPoolExecutor(max_workers=image_limit)
        self._parse_pool = ThreadPoolExecutor(max_workers=parse_limit)
        self._poll_pool = ThreadPoolExecutor(max_workers=poll_limit)
        self._post_pool = ThreadPoolExecutor(max_workers=post_limit)
        self._futures: dict[Future, tuple[str, int]] = {}
        self._future_tasks: dict[Future, dict] = {}
        self._read_category = 0
        self._read_sequence = 0
        self._read_order: dict[int, int] = {}

    def _submit(self, pool, kind: str, task: dict, operation) -> None:
        future = pool.submit(operation, task)
        self._futures[future] = (kind, task['id'])
        self._future_tasks[future] = dict(task)

    def _gateway_upload(self, task):
        return self.gateway_factory().upload(task)

    def _gateway_parse(self, task):
        self.gateway_factory().parse(task)

    def _source_metadata(self, task):
        from ingest_source_metadata import SourceMetadataBlocked, verify_local, validate_receipt
        verify_local(task)
        if self.on_source_metadata is None:
            raise SourceMetadataBlocked('receiver_not_configured')
        docs = self.gateway_factory().lookup(task)
        if (len(docs) != 1 or docs[0].get('id') != task['remote_id']
                or docs[0].get('name') != task['remote_name']):
            raise SourceMetadataBlocked('remote_identity_mismatch')
        return validate_receipt(task, self.on_source_metadata(task, docs[0]))

    def _gateway_lookup(self, task):
        gateway=self.gateway_factory()
        docs=gateway.lookup(task)
        if len(docs)==1 and hasattr(gateway,'verify_execution'):
            gateway.verify_execution(task,docs[0])
        if not self.diagnostics_reader:return docs
        diagnostic,error=None,None
        if len(docs)==1 and docs[0].get('run')=='FAIL' and task.get('policy_key') in ('education','research_profile','research_material'):
            try:diagnostic=self.diagnostics_reader(task,docs[0])
            except Exception as exc:error=type(exc).__name__
        return docs,diagnostic,error

    def _gateway_reconcile(self, task):
        gateway = self.gateway_factory()
        operation = 'upload' if task['status'].startswith('upload_') else 'parse'
        outcome = reconcile_write(task, operation, gateway)
        docs = gateway.lookup(task) if outcome == 'confirmed' else []
        if operation=='parse' and (len(docs)!=1 or not current_execution(task,docs[0])):
            outcome,docs='uncertain',[]
        return outcome, docs

    def _postprocess(self, task):
        try:
            gateway = self.gateway_factory()
            docs = gateway.lookup(task)
            if len(docs) != 1:
                raise IdentityMismatch('Accepted source is no longer unique remotely')
            original = docs[0]
            if original.get('run') != 'DONE':
                raise RemoteExecutionChanged(original)
            if hasattr(gateway,'verify_execution'):
                gateway.verify_execution(task,original)
            try:
                self.on_accepted_for(task, original)
            finally:
                refreshed = gateway.lookup(task)
                if len(refreshed) != 1:
                    raise IdentityMismatch('Accepted source is no longer unique remotely')
                latest=refreshed[0]
                if any(latest.get(key)!=original.get(key) for key in
                       ('id','name','pipeline_id','process_begin_at','run')):
                    raise RemoteExecutionChanged(latest)
        except Exception as exc:
            if self.on_invalidated_for is not None:
                try:
                    self.on_invalidated_for(task)
                except Exception as invalidation_error:
                    # Preserve the execution failure so local admission is still
                    # withdrawn, and expose any failed cleanup without server text.
                    exc.artifact_invalidation_error = type(invalidation_error).__name__
            raise

    def _has_future(self, task_id: int, kinds: tuple[str, ...] | None = None) -> bool:
        return any(row_id == task_id and (kinds is None or kind in kinds)
                   for kind, row_id in self._futures.values())

    def _collect(self) -> None:
        for future, (kind, task_id) in list(self._futures.items()):
            if not future.done():
                continue
            del self._futures[future]
            captured=self._future_tasks.pop(future,None)
            task = self.queue.get(task_id)
            if task is None:
                continue
            if kind == 'source_metadata':
                from ingest_source_metadata import SourceMetadataBlocked
                try:
                    receipt = future.result()
                    status = {'confirmed':'confirmed', 'retryable_error':'pending', 'blocked':'blocked'}[receipt['status']]
                    reason = receipt['reason_code']
                except SourceMetadataBlocked as exc:
                    status, reason, receipt = 'blocked', str(exc), None
                except Exception:
                    status, reason, receipt = 'pending', 'receiver_unavailable', None
                self.queue.finish_source_metadata(captured, status=status, reason=reason, receipt=receipt)
                continue
            if captured:
                keys=('status','execution_attempt','execution_pipeline_id','remote_id')
                if kind in ('poll','post','reconcile'): keys+=('updated_at',)
                if any(task.get(key)!=captured.get(key) for key in keys):continue

            def transition(expected, target, **fields):
                # Baseline persistence may change the revision after dispatch.
                # Use the fresh row and commit only if it is still current.
                return self.queue.transition(task_id, expected, target,
                    expected_updated_at=task['updated_at'], **fields)

            def invalidation_note(exc):
                kind = getattr(exc, 'artifact_invalidation_error', None)
                return ('；产物撤回失败：' + kind) if kind else ''

            try:
                result = future.result()
            except RemoteExecutionChanged as exc:
                remote=exc.remote
                same=(not hasattr(self.gateway_factory(),'verify_execution')
                      or current_execution(task,remote))
                target=('outcome_unresolved' if remote.get('run')=='FAIL' else
                        'running' if remote.get('run')=='RUNNING' else 'technical_review') if same else 'technical_review'
                changed=transition(task['status'], target,post_status=None,outcome=None,
                    remote_run=remote.get('run'),
                    failure_kind='diagnostic_pending' if target=='outcome_unresolved' else 'remote_execution_changed',
                    error='产物处理时远端状态发生变化，已撤销本地成功计数并重新核查'+invalidation_note(exc))
                if changed:self.queue.append_attempt(task_id,kind,'remote_execution_changed',time.time())
                continue
            except ExecutionUnconfirmed as exc:
                # A previous terminal cannot release or finish an uncertain write.
                if kind=='post':
                    if not transition('accepted','technical_review',post_status=None,
                        failure_kind='execution_unconfirmed',error='当前解析轮次未核实，产物未完成'+invalidation_note(exc)):continue
                self.queue.append_attempt(task_id,kind,'execution_unconfirmed',time.time())
                continue
            except ParsePreparationFailed as exc:
                if exc.status_code==429:self._paused_services[task['service_key']]=time.time()+30
                if transition(task['status'],'technical_review',owner=None,lease_until=None,
                        failure_kind='parse_preparation_failed',error=str(exc)):
                    self.queue.append_attempt(task_id,kind,'not_submitted',time.time())
                continue
            except ArtifactEvidencePending as exc:
                retries=(task.get('post_retry_count') or 0)+1
                delays=(15,45,90)
                pending=retries<=len(delays) and not invalidation_note(exc)
                if transition('accepted','accepted',post_status='pending' if pending else 'needs_review',
                        post_retry_count=retries,
                        post_retry_at=time.time()+delays[retries-1] if pending else None,
                        failure_kind='artifact_evidence_pending' if pending else 'artifact_evidence_unavailable',
                        error=('本轮执行日志尚未就绪，等待只读补核' if pending else
                               '本轮执行日志补核未完成，产物待复核')+invalidation_note(exc)):
                    self.queue.append_attempt(task_id,'artifact_evidence_pending',
                        str(task.get('execution_attempt',0))+':'+str(retries),time.time())
                continue
            except (SourceChanged,IdentityMismatch) as exc:
                failure_fields = {'error': invalidation_note(exc).lstrip('；')} if invalidation_note(exc) else {}
                changed=transition(task['status'], 'technical_review',
                                      failure_kind='source_changed' if isinstance(exc,SourceChanged) else 'identity_mismatch', owner=None,
                                      lease_until=None, **failure_fields)
                if changed:self.queue.append_attempt(task_id, kind, type(exc).__name__, time.time())
                continue
            except Exception as exc:
                if getattr(exc,'status_code',None)==429:
                    self._paused_services[task['service_key']]=time.time()+30
                changed=True
                if kind in ('upload', 'image'):
                    changed=transition('upload_intent', 'upload_unknown',
                                          failure_kind=type(exc).__name__, owner=None,
                                          lease_until=None)
                elif kind == 'parse':
                    changed=transition('parse_intent', 'parse_unknown',
                                          failure_kind=type(exc).__name__, owner=None,
                                          lease_until=None)
                elif kind == 'reconcile':
                    target = ('upload_unknown' if task['status'].startswith('upload_')
                              else 'parse_unknown')
                    changed=transition(task['status'], target,
                                          failure_kind=type(exc).__name__, owner=None,
                                          lease_until=None)
                elif kind == 'post':
                    reason=re.sub(r'sk-[A-Za-z0-9_-]+|Bearer\s+\S+', '[credential]', str(exc))[:300]
                    if getattr(exc,'artifact_evidence_reason',None)=='proof_conflict':
                        reason+=' [proof_conflict]'
                    changed=transition('accepted', 'accepted', post_status='needs_review',
                        failure_kind='artifact_error',error=type(exc).__name__+': '+reason+invalidation_note(exc))
                if changed:self.queue.append_attempt(task_id, kind, type(exc).__name__, time.time())
                continue
            if kind in ('upload', 'image'):
                target = 'parse_waiting' if task['parse_required'] else 'uploaded'
                changed=transition('upload_intent', target,
                                      remote_id=result, owner=None, lease_until=None)
                if changed:self.queue.append_attempt(task_id, kind, 'confirmed', time.time())
            elif kind == 'parse':
                changed=transition('parse_intent', 'submitted',
                                      owner=None, lease_until=None)
                if changed:self.queue.append_attempt(task_id, kind, 'submitted', time.time())
            elif kind == 'reconcile':
                outcome, docs = result
                if outcome == 'confirmed' and len(docs) == 1:
                    if task['status'].startswith('upload_'):
                        target = 'parse_waiting' if task['parse_required'] else 'uploaded'
                        changed=transition(task['status'], target,
                                              remote_id=docs[0]['id'], owner=None,
                                              lease_until=None)
                    else:
                        changed=transition(task['status'], 'submitted',
                                              owner=None, lease_until=None,
                                              execution_begin_at=docs[0].get('process_begin_at'))
                else:
                    target = ('upload_unknown' if task['status'].startswith('upload_')
                              else 'parse_unknown')
                    changed=transition(task['status'], target,
                                          owner=None, lease_until=None)
                if changed:self.queue.append_attempt(task_id, kind, outcome, time.time())
            elif kind == 'poll':
                diagnostic,diagnostic_error=None,None
                if isinstance(result,tuple):
                    result,diagnostic,diagnostic_error=result
                if diagnostic and (diagnostic.get('attempt')!=task.get('execution_attempt',0)
                        or diagnostic.get('remote_id')!=task.get('remote_id')
                        or diagnostic.get('pipeline_id')!=(task.get('execution_pipeline_id') or task.get('pipeline_id'))):
                    continue
                if len(result) != 1:
                    transition(task['status'], 'technical_review',
                                          failure_kind='remote_identity_mismatch')
                    continue
                remote = result[0]
                run = remote.get('run')
                if run in ('DONE', 'FAIL', 'CANCEL'):
                    if self.router is None and run == 'FAIL' and 'Arrearage' in (remote.get('progress_msg') or ''):
                        self._halted_channels.add(task['channel_key'])
                    try:
                        outcome = self.classify_for(task, remote)
                    except Exception as exc:
                        outcome=Outcome.technical_review
                        self.queue.append_attempt(task_id,'classify',type(exc).__name__,time.time())
                    outcome = outcome.value if isinstance(outcome, Outcome) else outcome
                    if outcome not in {item.value for item in Outcome}:
                        outcome = 'technical_review'
                    if run != 'DONE' and outcome == 'accepted':
                        outcome = 'technical_review'
                    fields={'failure_kind':None,'error':None}
                    if remote.get('process_begin_at'):
                        fields['execution_begin_at']=remote['process_begin_at']
                    if diagnostic and (diagnostic.get('source')=='execution_node'
                                       or is_database_status_diagnostic(diagnostic)):
                        if diagnostic.get('outcome') in {'quality_hold','quality_reject','technical_review'}:
                            outcome=diagnostic['outcome']
                        fields.update(failure_kind=diagnostic.get('kind'),error=diagnostic.get('reason'))
                    elif run=='FAIL' and self.diagnostics_reader and task.get('policy_key') in ('education','research_profile','research_material'):
                        attempt_prefix=str(task.get('execution_attempt',0))+':'
                        misses=sum(row['operation']=='diagnostic_unavailable' and row['result'].startswith(attempt_prefix)
                                   for row in self.queue.attempts(task_id))+1
                        self.queue.append_attempt(task_id,'diagnostic_unavailable',
                            attempt_prefix+(diagnostic_error or 'record_not_ready'),time.time())
                        outcome='outcome_unresolved'
                        fields.update(failure_kind='diagnostic_pending' if misses<3 else 'diagnostic_unavailable',
                                      error='远端已失败，节点诊断暂未取得；已释放解析名额')
                    changed=transition(task['status'], outcome,
                                          outcome=outcome, remote_run=run,
                                          post_status='pending' if outcome == 'accepted' else None,**fields)
                    if not changed:continue
                    self.queue.append_attempt(task_id, 'poll', outcome, time.time())
                    if diagnostic and (diagnostic.get('log_id') or is_database_status_diagnostic(diagnostic)):
                        self.queue.record_diagnostic(task_id,diagnostic,outcome)
                    if (self.router and self.router.eligible(task)
                            and fields.get('failure_kind')!='diagnostic_pending'):
                        self.router.observe(task,remote,outcome,diagnostic,time.time())
                elif run == 'RUNNING' and task['status'] == 'submitted':
                    transition('submitted', 'running',
                                          remote_run='RUNNING',execution_begin_at=remote.get('process_begin_at'))
            elif kind == 'post':
                changed=transition('accepted', 'accepted', post_status='done',
                                      failure_kind=None,error=None,post_retry_at=None)
                if changed:self.queue.append_attempt(task_id, 'post', 'done', time.time())

    def _schedule_remote_reads(self) -> None:
        """Share the read budget fairly across active work and maintenance.

        A category gets the next free worker in turn; within a category, tasks
        not queried recently go first. Remember scheduling rather than successful
        completion so a repeatedly failing lookup cannot monopolize the pool.
        """
        available = self.poll_limit - sum(kind in ('poll', 'reconcile')
                                          for kind, _ in self._futures.values())
        if available <= 0:
            return
        now = time.time()
        diagnostics = []
        if self.diagnostics_reader:
            for task in self.queue.diagnostics_ready():
                misses = [row for row in self.queue.attempts(task['id'])
                          if row['operation'] == 'diagnostic_unavailable']
                if not misses or now - misses[-1]['at'] >= 5:
                    diagnostics.append(task)
        groups = [self.queue.list_statuses(('submitted', 'running')),
                  self.queue.reconciliation_ready(now, max(1000, self.poll_limit)),
                  diagnostics]
        pending_ids = {task['id'] for group in groups for task in group}
        pending_ids.update(task_id for _, task_id in self._futures.values())
        self._read_order = {task_id: order for task_id, order in self._read_order.items()
                            if task_id in pending_ids}
        groups = [sorted((task for task in group if not self._has_future(task['id'])),
                         key=lambda task: self._read_order.get(task['id'], -1))
                  for group in groups]
        while available > 0 and any(groups):
            category = self._read_category
            self._read_category = (category + 1) % len(groups)
            if not groups[category]:
                continue
            task = groups[category].pop(0)
            kind = 'reconcile' if category == 1 else 'poll'
            operation = self._gateway_reconcile if category == 1 else self._gateway_lookup
            self._submit(self._poll_pool, kind, task, operation)
            self._read_order[task['id']] = self._read_sequence
            self._read_sequence += 1
            available -= 1

    def _dispatch_uploads(self, stage: str, limit: int, pool) -> None:
        kind = 'image' if stage == 'image' else 'upload'
        active = sum(k == kind for k, _ in self._futures.values())
        if active >= limit:
            return
        tasks = self.queue.claim(stage, self.owner, limit-active,
                                 time.time(), lease_seconds=120,
                                 exclude_routes={'service_key':{
                                     service for service,until in self._paused_services.items()
                                     if until>time.time()}})
        for task in tasks:
            self._submit(pool, kind, task, self._gateway_upload)

    def _limit_reached(self, candidate: dict, active: list[dict]) -> bool:
        if candidate['channel_key'] in self._halted_channels:
            return True
        limits = ((self.service_limits, 'service_key'),
                  (self.dataset_limits, 'dataset_id'),
                  (self.pipeline_limits, 'pipeline_id'),
                  (self.channel_limits, 'channel_key'))
        for mapping, key in limits:
            value = candidate.get(key)
            if value in mapping and sum(row.get(key) == value for row in active) >= mapping[value]:
                return True
        return False

    def _dispatch_parses(self) -> None:
        states=_PARSE_ACTIVE+('parse_unknown',)
        active = self.queue.list_statuses(states, limit=100000)
        external=(self.external_counts_reader() if self.external_counts_reader else {'total':0})
        if (not isinstance(external,dict) or 'total' not in external
                or type(external['total']) is not int or external['total']<0):
            raise ValueError('Remote active task count is unverified')
        external_total=external['total']
        if self.router:
            total_limit=min(self.parse_limit,self.router.total_limit)
            if not any(until>time.time() for service,until in self._paused_services.items()
                       if service==self.router.routes['school']['service_key']):
                for _ in range(max(0,total_limit-len(active)-external_total)):
                    task=self.router.claim(self.owner,time.time(),external)
                    if not task:
                        break
                    active.append(task)
                    self._submit(self._parse_pool,'parse',task,self._gateway_parse)
        total_limit=min(self.parse_limit,self.router.total_limit) if self.router else self.parse_limit
        available = total_limit - len(active) - external_total
        for _ in range(max(available,0)):
            blocked={key:{value for value,cap in mapping.items()
                          if sum(row.get(key)==value for row in active)>=cap}
                     for mapping,key in ((self.service_limits,'service_key'),
                                         (self.dataset_limits,'dataset_id'),
                                         (self.pipeline_limits,'pipeline_id'),
                                         (self.channel_limits,'channel_key'))}
            blocked['channel_key'].update(self._halted_channels)
            blocked['service_key'].update(service for service,until in self._paused_services.items()
                                          if until>time.time())
            if self.router:
                blocked['pipeline_id'].add(self.router.routes['source_pipeline_id'])
            claimed = self.queue.claim('parse', self.owner, 1, time.time(),
                                       lease_seconds=120,exclude_routes=blocked)
            if not claimed:
                break
            task = claimed[0]
            active.append(task)
            available -= 1
            self._submit(self._parse_pool, 'parse', task, self._gateway_parse)

    def _schedule_post(self) -> None:
        if self.on_accepted_for is None:
            return
        active = sum(kind in ('post','source_metadata') for kind, _ in self._futures.values())
        for task in self.queue.post_ready(self.post_limit):
            if active >= self.post_limit:
                break
            if (task['post_status'] not in ('pending', 'running') or self._has_future(task['id'])
                    or task.get('source_metadata_status') not in (None,'confirmed')):
                continue
            if not self.queue.transition(task['id'], 'accepted', 'accepted',
                    expected_updated_at=task['updated_at'],post_status='running'):continue
            self._submit(self._post_pool, 'post', self.queue.get(task['id']), self._postprocess)
            active += 1

    def _schedule_source_metadata(self) -> None:
        # Share the existing bounded post executor, without consuming model slots.
        active = sum(kind in ('post','source_metadata') for kind, _ in self._futures.values())
        available = self.post_limit-active
        if available <= 0:
            return
        busy = {task_id for _, task_id in self._futures.values()}
        for task in self.queue.claim_source_metadata(time.time(), available, busy):
            self._submit(self._post_pool, 'source_metadata', task, self._source_metadata)

    def tick(self) -> dict:
        self._collect()
        self._schedule_source_metadata()
        self._schedule_remote_reads()
        self._dispatch_uploads('image', self.image_limit, self._image_pool)
        self._dispatch_uploads('upload', self.upload_limit, self._upload_pool)
        self._dispatch_parses()
        self._schedule_post()
        counts = self.queue.summary()
        return {'upload_waiting': counts.get('upload_waiting', 0),
                'parse_waiting': counts.get('parse_waiting', 0),
                'upload_active': sum(kind in ('upload', 'image')
                                     for kind, _ in self._futures.values()),
                'parse_active': sum(counts.get(key, 0) for key in _PARSE_ACTIVE+('parse_unknown',)),
                'artifacts': self.queue.artifact_summary(),
                'source_metadata': self.queue.source_metadata_summary(),
                'terminal_by_outcome': {key: counts.get(key, 0) for key in
                                        ('accepted', 'quality_hold', 'quality_reject',
                                         'technical_review', 'outcome_unresolved')}}

    def close(self) -> None:
        for executor in (self._upload_pool, self._image_pool, self._parse_pool,
                         self._poll_pool, self._post_pool):
            executor.shutdown(wait=True)
        self._collect()
