"""Ordered, unattended preprocessing. Business decisions are pass/reject only."""
import json
import os
import re
import shutil
import sys
import uuid
import time
import copy
import hashlib
import threading
import zipfile
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from engine import Processor,LEGACY_ROOT,OFFICE_FORMATS,convert_office,write_json,validate_zip,load_legacy_taxonomy
from processing_stop import digest
from agent_client import AgentClient,AgentError,AgentUnavailable
from auto_evidence import Rejected,ask,source_pages,finish_evidence
from content_rules import detect_title,remove_acknowledgements,valid_title
from evidence import build_package,export_package,package_fingerprint,redactor
from screening import nonpaper_heading,MIN_CHARS
from billing import Ledger,summarize
from archive_identity import member_fields, record_key
from classification_rules import RULES_VERSION as CLASSIFICATION_RULES_VERSION,resolve_identity,apply_identity,source_context
from material_intake import POLICY as INTAKE_POLICY,MaterialPending,intake_enabled,read_material,classify_material,material_pipeline
from associated_materials import MaterialContext, material_kind, process_associated, duplicate_project_reference, resolve_duplicate_projects, is_attachment_container, DOCUMENTS as MATERIAL_DOCUMENT_FORMATS
from material_intake import IMAGE_SUFFIXES

VERSION='automatic-v2.6.14-audit-recovery-integrity'
TASK_DELIVERY_BINDING_VERSION=1
TASK_ADMISSION_GUARD_VERSION=1


def _is_uploaded_record(record, source_hash):
    """Trust a scan's upload mark only when it still refers to this file."""
    if not isinstance(record, dict) or record.get('upload_mark') != '已上传':
        return False
    marked_hash=record.get('upload_source_hash')
    return not marked_hash or marked_hash == source_hash


def _validate_archive_member(record, checked):
    expected=record.get('archive_source_hash')
    parent=record.get('input_source_path')
    if not expected or not parent or str(parent)==str(record.get('path')):
        return
    archive=Path(parent)
    try:
        stat=archive.stat()
        key=(str(archive.resolve()),stat.st_size,stat.st_mtime_ns)
        if key not in checked:
            checked[key]=digest(archive)
    except OSError as exc:
        raise AgentError('原压缩包无法读取，请重新扫描后处理') from exc
    if checked[key]!=expected:
        raise AgentError('原压缩包已修改，请重新扫描；不能继续使用旧解压文件')


class _ObservedGate:
    """Semaphore wrapper that records the actual in-flight LLM peak."""
    def __init__(self, limit, *, disk=None, stop=None):
        self.limit=max(1,int(limit))
        self.disk=disk;self.stop=stop
        self._semaphore=threading.BoundedSemaphore(self.limit)
        self._lock=threading.Lock()
        self.current=0
        self.peak=0
        self.purpose_current=0
        self.purpose_peak=0

    def acquire(self, blocking=True, timeout=None, *, purpose=False):
        if not blocking and timeout is not None:raise ValueError('timeout with nonblocking acquisition')
        deadline=None if timeout is None else time.monotonic()+max(0,timeout)
        while True:
            remaining=None if deadline is None else max(0,deadline-time.monotonic())
            if self.disk and not self.disk.wait(self.stop,timeout=remaining,blocking=blocking):return False
            remaining=None if deadline is None else max(0,deadline-time.monotonic())
            acquired=self._semaphore.acquire(blocking,remaining)
            if not acquired:return False
            if self.disk and not self.disk.ready():
                self._semaphore.release()
                if deadline is not None and time.monotonic()>=deadline:return False
                continue
            with self._lock:
                self.current+=1
                self.peak=max(self.peak,self.current)
                if purpose:
                    self.purpose_current+=1
                    self.purpose_peak=max(self.purpose_peak,self.purpose_current)
            return True

    def release(self, *, purpose=False):
        with self._lock:
            self.current=max(0,self.current-1)
            if purpose:self.purpose_current=max(0,self.purpose_current-1)
        self._semaphore.release()

    def snapshot(self):
        with self._lock:
            return dict(llm_active=self.current,paper_llm_active=self.current-self.purpose_current,
                        purpose_llm_active=self.purpose_current,total_llm_active=self.current,
                        purpose_llm_peak=self.purpose_peak)


class _AdmissionHold:
    """Linearize a binding hold with classifier stage registration only."""
    def __init__(self):
        self._changed = threading.Condition(threading.Lock())
        self._registrations = 0
        self._writer = False
        self._writers_waiting = 0
        self._error = None

    def is_open(self):
        with self._changed:
            while self._writer or self._writers_waiting:
                self._changed.wait()
            return self._error is None

    def hold(self, error):
        with self._writing():
            if self._error is None:
                self._error = error

    @contextmanager
    def _writing(self):
        with self._changed:
            self._writers_waiting += 1
            try:
                while self._writer or self._registrations:
                    self._changed.wait()
                self._writer = True
            finally:
                self._writers_waiting -= 1
                self._changed.notify_all()
        try:
            yield
        finally:
            with self._changed:
                self._writer = False
                self._changed.notify_all()

    @contextmanager
    def binding(self):
        """Exclude all new registrations during a parent binding extension."""
        from native_batch_control import BatchPaused
        with self._writing():
            if self._error is not None:
                raise BatchPaused('本任务来源绑定待续，未准入新阶段')
            try:
                yield
            except BaseException as exc:
                # Publish failure before a waiting classifier can acquire the
                # registration lock. Model transport never holds this lock.
                if self._error is None:self._error=exc
                raise

    @contextmanager
    def guard(self, scope):
        from contextlib import ExitStack
        from native_batch_control import BatchPaused
        with ExitStack() as stack:
            with self._changed:
                # Independent checks can overlap. A pending writer gets
                # priority so source changes and holds cannot be starved.
                while self._writer or self._writers_waiting:
                    self._changed.wait()
                if self._error is not None:
                    raise BatchPaused('本任务来源绑定待续，未准入新阶段')
                self._registrations += 1
            try:
                stack.enter_context(scope)
            finally:
                with self._changed:
                    self._registrations -= 1
                    self._changed.notify_all()
            # A model send never holds a registration or the condition lock.
            yield

    @contextmanager
    def stage(self, control, stage, **identity):
        with self.guard(control.stage(stage, **identity)):
            yield


def _guard_frozen_plain(record, frozen_paths, allocated_paths):
    """Freeze an original's lexical path and exact WEB bytes, or fail closed."""
    from native_batch_control import BatchPaused
    path=Path(os.path.abspath(record['path']))
    if (any(record.get(key) for key in ('archive_member_path','archive_source_hash','_native_task_source_evidence','source_education'))
            or record.get('input_source_path') and Path(os.path.abspath(record['input_source_path']))!=path):
        return None
    key=str(path);expected=frozen_paths.get(key)
    if not expected or allocated_paths.get(key)!=expected or path.resolve()!=path:
        raise BatchPaused('普通原稿缺少原任务冻结路径与内容，或路径指向已变化')
    if any(record.get(name) and record[name]!=expected for name in ('sha256','source_hash')):
        raise BatchPaused('原稿声明与WEB冻结路径和内容不一致')
    record['source_hash']=expected
    return key,expected


def _configure_llm_lanes(config, options, md_mode, global_gate, purpose_gate):
    """Opt in to the existing shared transport gate without changing older runs."""
    shared = bool(md_mode and options.get('shared_purpose_llm') is True)
    if md_mode:
        config['_independent_purpose_llm'] = not shared
    return dict(global_llm=global_gate.limit + (purpose_gate.limit if md_mode and not shared else 0),
                purpose_llm_limit=purpose_gate.limit,
                request_lanes='independent-purpose/1' if md_mode and not shared else 'shared-total/1')


class _PreextractSlots:
    """Bound phases of the existing workers without replacing their run/item."""
    def __init__(self, model_limit, post_limit, max_waiting, stop, *, prepare_limit=2, max_prepared=8, purpose_limit=8, disk=None,admission_check=None):
        self.admission_check=admission_check
        self.disk=disk
        self.model_limit=max(1,int(model_limit))
        self.post_limit=max(1,int(post_limit))
        self.max_waiting=max(0,int(max_waiting))
        # Reserve room for the results of in-flight requests as well. Even if
        # all requests finish together, at most max_waiting results must wait.
        self.post_capacity=self.post_limit+self.max_waiting
        self.prepare_limit=max(1,int(prepare_limit))
        # Pending material reservations include preparation in flight. They do
        # not hold model/postprocessing capacity or built image request bodies.
        self.prepare_capacity=max(self.prepare_limit,int(max_prepared))
        self.purpose_limit=max(1,int(purpose_limit))
        self.purpose_capacity=2*self.purpose_limit+self.prepare_limit
        self.capacity=self.post_capacity+self.prepare_capacity+self.purpose_capacity
        self.stop=stop
        self.changed=threading.Event()
        self.condition=threading.Condition()
        self.phases={}
        self.post_peak=0
        self.prepare_peak=0
        self.outstanding_peak=0

    def _models(self):
        return sum(phase=='model' for phase in self.phases.values())

    def _preparing(self):
        return sum(phase=='prepare' for phase in self.phases.values())

    def _prepared(self):
        return sum(phase in {'prepare','model_wait','post_ready','conversion','conversion_return'} for phase in self.phases.values())

    def _post_reserved(self):
        return sum(phase in {'model','post_wait','post','finishing'} for phase in self.phases.values())

    def admit(self, ticket, *, auxiliary=False):
        with self.condition:
            if auxiliary:
                if (self.stop.is_set() or (self.disk and not self.disk.ready())
                        or len(self.phases)>=self.capacity
                        or sum(p=='attachment' for p in self.phases.values())>=2):return False
                self.phases[ticket]='attachment'
                self.outstanding_peak=max(self.outstanding_peak,len(self.phases))
                return True
            if (self.stop.is_set() or (self.disk and not self.disk.ready()) or len(self.phases)>=self.capacity or
                    sum(p=='purpose_wait' for p in self.phases.values())>=2*self.purpose_limit or
                    self._preparing()>=self.prepare_limit or self._prepared()>=self.prepare_capacity or
                    any(p in {'prepare_wait','conversion_return'} for p in self.phases.values())):
                return False
            self.phases[ticket]='prepare'
            self.prepare_peak=max(self.prepare_peak,self._preparing())
            self.outstanding_peak=max(self.outstanding_peak,len(self.phases))
            return True

    def _phase(self, ticket, phase):
        with self.condition:
            if ticket not in self.phases:return
            self.phases[ticket]=phase
            self.post_peak=max(self.post_peak,sum(p=='post' for p in self.phases.values()))
            self.condition.notify_all()
        self.changed.set()

    def _check_admission(self):
        if self.admission_check:self.admission_check()

    def _admission_permitted(self):
        try:self._check_admission();return True
        except __import__('global_task_control').GlobalPaused:return False

    def enter_prepare(self, ticket):
        # An archive's next member reuses this worker but must release the
        # previous member's downstream reservation and reclaim preparation.
        with self.condition:
            self._check_admission()
            if self.stop.is_set():raise InterruptedError('已停止，未准备下一篇')
            if ticket not in self.phases:raise RuntimeError('论文任务已经结束')
            if self.phases[ticket] in {'prepare','attachment'}:return
            self.phases[ticket]='prepare_wait'
            self.condition.notify_all()
            self.changed.set()
            while (self._preparing()>=self.prepare_limit or self._prepared()>=self.prepare_capacity
                   or (self.disk and not self.disk.ready())):
                if self.stop.is_set():raise InterruptedError('已停止，未准备下一篇')
                self._check_admission()
                self.condition.wait(timeout=0.1)
            if self.stop.is_set():raise InterruptedError('已停止，未准备下一篇')
            self.phases[ticket]='prepare'
            self.prepare_peak=max(self.prepare_peak,self._preparing())
        self.changed.set()

    def enter_model(self, ticket):
        with self.condition:
            self._check_admission()
            if self.stop.is_set():raise InterruptedError('已停止，准备好的材料已保留')
            if ticket not in self.phases:raise RuntimeError('论文任务已经结束')
            if self.phases[ticket]=='model':return
            self.phases[ticket]='model_wait'
            self.condition.notify_all()
            self.changed.set()
            while (self._models()>=self.model_limit or self._post_reserved()>=self.post_capacity
                   or (self.disk and not self.disk.ready())):
                if self.stop.is_set():raise InterruptedError('已停止，未派发下一篇')
                self._check_admission()
                self.condition.wait(timeout=0.1)
            if self.stop.is_set():raise InterruptedError('已停止，未派发下一篇')
            self.phases[ticket]='model'
            self.condition.notify_all()
        self.changed.set()

    @contextmanager
    def purpose(self,ticket):
        """Wait for the independent review transport without retaining a parser."""
        with self.condition:
            if self.stop.is_set():raise InterruptedError('用途复检已停止')
            independent=self.phases.get(ticket) in {'conversion','attachment'}
        if independent:
            # These callers already released the body parser. The native
            # purpose transport still enforces its separate ten-request cap.
            yield
            return
        self.enter_prepare(ticket)
        with self.condition:
            if self.stop.is_set():raise InterruptedError('用途复检已停止')
            self.phases[ticket]='purpose_wait'
            self.condition.notify_all()
        self.changed.set()
        try:
            yield
        finally:
            with self.condition:
                if ticket in self.phases:
                    self.phases[ticket]='conversion_return'
                    self.condition.notify_all();self.changed.set()
                    while self._preparing()>=self.prepare_limit and not self.stop.is_set() and self._admission_permitted():
                        self.condition.wait(timeout=.1)
                    self.phases[ticket]='cancelled' if self.stop.is_set() or not self._admission_permitted() else 'prepare'
                    self.prepare_peak=max(self.prepare_peak,self._preparing())
                    self.condition.notify_all()
            self.changed.set()

    @contextmanager
    def conversion(self,ticket):
        """Keep this paper's material reservation, releasing only its parser."""
        with self.condition:
            if self.stop.is_set():raise InterruptedError('已停止，原材料保留')
            independent=self.phases.get(ticket) in {'conversion','attachment'}
            if not independent:
                if self.phases.get(ticket)!='prepare':raise RuntimeError('材料转换阶段不正确')
                self.phases[ticket]='conversion';self.condition.notify_all()
        if independent:
            yield
            return
        self.changed.set()
        conversion_failed=False
        try:
            yield
        except BaseException:
            conversion_failed=True
            raise
        finally:
            with self.condition:
                if ticket in self.phases:
                    self.phases[ticket]='conversion_return';self.condition.notify_all();self.changed.set()
                    while self._preparing()>=self.prepare_limit and not self.stop.is_set() and self._admission_permitted():
                        self.condition.wait(timeout=.1)
                    if self.stop.is_set():
                        self.phases[ticket]='cancelled'
                        if not conversion_failed:
                            raise InterruptedError('已停止，已转换材料保留')
                    elif not self._admission_permitted():
                        self.phases[ticket]='cancelled'
                    else:
                        self.phases[ticket]='prepare'
                        self.prepare_peak=max(self.prepare_peak,self._preparing())
                    self.condition.notify_all()
            self.changed.set()

    @contextmanager
    def postprocess(self, ticket):
        reserved=False
        try:
            with self.condition:
                self._check_admission()
                if ticket not in self.phases:raise RuntimeError('论文任务已经结束')
                if self.phases[ticket]!='model':
                    # A saved response skips the model queue, but still needs
                    # the same bounded downstream reservation as a new reply.
                    self.phases[ticket]='post_ready'
                    self.condition.notify_all()
                    self.changed.set()
                    while self._post_reserved()>=self.post_capacity:
                        if self.stop.is_set():raise InterruptedError('已停止，完整模型回答已保留')
                        self._check_admission()
                        self.condition.wait(timeout=0.1)
                reserved=True
                self.phases[ticket]='post_wait'
                self.condition.notify_all()
                self.changed.set()
                while sum(p=='post' for p in self.phases.values())>=self.post_limit:
                    if self.stop.is_set():raise InterruptedError('已停止，完整模型回答已保留')
                    self._check_admission()
                    self.condition.wait(timeout=0.1)
                if self.stop.is_set():raise InterruptedError('已停止，完整模型回答已保留')
                # Publish waiting/active counts atomically: an immediately
                # available post slot is never counted as queued backlog.
                self.phases[ticket]='post'
                self.post_peak=max(self.post_peak,sum(p=='post' for p in self.phases.values()))
                self.condition.notify_all()
            self.changed.set()
            yield
        finally:
            self._phase(ticket,'finishing' if reserved else 'cancelled')

    def finish(self, ticket):
        with self.condition:
            self.phases.pop(ticket,None)
            self.condition.notify_all()
        self.changed.set()

    def snapshot(self):
        with self.condition:
            return dict(model_stage_active=self._models(),postprocess_workers=self.post_limit,
                preparation_workers=self.prepare_limit,preparation_capacity=self.prepare_capacity,
                preparation_active=self._preparing(),preparation_waiting=sum(p=='model_wait' for p in self.phases.values()),
                conversion_waiting=sum(p in {'conversion','conversion_return'} for p in self.phases.values()),
                attachment_workers=2,attachment_active=sum(p=='attachment' for p in self.phases.values()),
                preparation_reserved=self._prepared(),preparation_peak=self.prepare_peak,
                purpose_waiting=sum(p=='purpose_wait' for p in self.phases.values()),
                purpose_capacity=self.purpose_capacity,
                postprocess_max_waiting=self.max_waiting,
                postprocess_reserved=self._post_reserved(),
                postprocess_active=sum(p=='post' for p in self.phases.values()),
                postprocess_waiting=sum(p=='post_wait' for p in self.phases.values()),
                outstanding=len(self.phases),postprocess_peak=self.post_peak,
                outstanding_peak=self.outstanding_peak,**(self.disk.snapshot() if self.disk else {}))


def coarse_screen(raw,reliable,source_name=''):
    from screening import similarity_report
    body,_=remove_acknowledgements(raw)
    count=len(re.sub(r'\s+|\[(?:PAGE_BREAK|SECTION_BREAK)\]','',body))
    attachment=similarity_report(raw,source_name)
    if attachment:return {'decision':'reject','reason':attachment,'chars':count,'attachment':True}
    if not reliable: return {'decision':'agent','reason':'文字提取不完整，不能按字数筛除','chars':count}
    kind=nonpaper_heading(raw)
    if kind: return {'decision':'reject','reason':'明确非论文文种：'+kind,'chars':count}
    if count<MIN_CHARS: return {'decision':'reject','reason':f'有效字符不足 {MIN_CHARS}（当前 {count}）','chars':count}
    if not re.search(r'摘要|abstract',raw,re.I) or not re.search(r'参考文献|references',raw,re.I):
        return {'decision':'agent','reason':'论文结构不明确，由 AI 判断','chars':count}
    return {'decision':'pass','reason':'基本长度和论文结构满足规则','chars':count}


def lightweight(source):
    source=Path(source)
    if source.suffix.lower()=='.docx':
        from office_input import validate_docx_container
        validate_docx_container(source)
    if str(LEGACY_ROOT) not in sys.path: sys.path.insert(0,str(LEGACY_ROOT))
    from src.paper_cleaning.extract_text import extract_text_by_extension
    if source.suffix.lower() in {'.txt','.md'}:
        text=source.read_text(encoding='utf-8-sig')
        return text,text.count('\ufffd')/max(1,len(text))<.02
    if source.suffix.lower()=='.pdf':
        import fitz
        with fitz.open(source) as pdf:
            parts=[p.get_text() for p in pdf]
            reliable=bool(parts) and all(len(re.sub(r'\s+','',p))>=100 for p in parts)
        return '\n'.join(parts),reliable
    if source.suffix.lower()=='.docx':
        from office_input import normalized_docx_text
        text=normalized_docx_text(source);status='success'
    else:
        text,status,_=extract_text_by_extension(source)
    if status!='success': raise AgentError('轻量解析失败，未判定论文质量')
    # Image-heavy DOCX may be a scanned paper. Short text is not trustworthy there.
    import zipfile
    with zipfile.ZipFile(source) as archive:
        xml=archive.read('word/document.xml')
        visual=b'imagedata' in xml or b':blip' in xml or b':object' in xml or b':chart' in xml
    return text,not visual or len(re.sub(r'\s+','',text))>=MIN_CHARS


class AutoProcessor(Processor):
    @contextmanager
    def task_execution(self,selected):
        """Own one WEB execution across saved-package handoff and normal run."""
        binding=self.config.get('_native_task_batch')
        if not binding:
            raise ValueError('bind_task_batch must precede task_execution')
        from native_batch_control import NativeBatchControl
        control=NativeBatchControl(binding['root'])
        opened=getattr(self,'_native_task_execution',None)
        if opened is not None:
            with control.global_control._admission_lock():
                row=control._execution_row(binding['batch_id'],opened['epoch'])
                if row['execution_owner']['token']!=opened['owner_token']:
                    raise __import__('native_batch_control').BatchPaused('本任务执行上下文已变化')
            yield opened['epoch']
            return
        # This is the complete original selected set already supplied by the
        # normal WEB adapter, never a derived child's replacement source list.
        from preextract_material import file_sha
        frozen_paths=None
        if callable(getattr(self,'task_admission_guard',None)):
            frozen_paths={}
            for record in selected:
                if any(record.get(k) for k in ('archive_member_path','archive_source_hash',
                        '_native_task_source_evidence','source_education')):
                    continue
                path=Path(os.path.abspath(record['path']))
                actual=file_sha(path)
                if (path.resolve()!=path or any(record.get(k) and record[k]!=actual
                        for k in ('sha256','source_hash'))):
                    raise __import__('native_batch_control').BatchPaused('原任务路径或声明字节已变化')
                frozen_paths[str(path)]=actual
        frozen_previous=getattr(self,'_task_frozen_inputs',None)
        previous=self.config.get('_native_task_epoch')
        agent_previous=self.agent.config.get('_native_task_epoch')
        with control.execution(binding['batch_id'],selected,
                admission_guard=getattr(self,'task_admission_guard',None)) as epoch:
            with control.global_control._admission_lock():
                owner=control._execution_row(binding['batch_id'],epoch)['execution_owner']
            self._native_task_execution=dict(epoch=epoch,owner_token=owner['token'])
            self._task_frozen_inputs=frozen_paths
            self.config['_native_task_epoch']=epoch
            self.agent.config['_native_task_epoch']=epoch
            try:
                yield epoch
            finally:
                self._native_task_execution=None
                self._task_frozen_inputs=frozen_previous
                if previous is None:self.config.pop('_native_task_epoch',None)
                else:self.config['_native_task_epoch']=previous
                if agent_previous is None:self.agent.config.pop('_native_task_epoch',None)
                else:self.agent.config['_native_task_epoch']=agent_previous

    def refresh_task_run(self,run_id):
        """Refresh only this WEB's registered saved AUTO inside task_execution."""
        opened=getattr(self,'_native_task_execution',None)
        binding=self.config.get('_native_task_batch')
        if not opened or not binding:
            raise __import__('native_batch_control').BatchPaused('旧封包续办须在task_execution上下文内')
        from native_batch_control import NativeBatchControl
        return NativeBatchControl(binding['root']).bind_saved_run(binding['batch_id'],run_id,epoch=opened['epoch'])

    def _assert_task_admission(self):
        binding=self.config.get('_native_task_batch')
        if binding:
            from native_batch_control import NativeBatchControl
            NativeBatchControl(binding['root']).assert_running(binding['batch_id'],epoch=self.config.get('_native_task_epoch'))

    def bind_task_batch(self,batch_id):
        """Bind a previously registered WEB task before the normal native run."""
        from native_batch_control import NativeBatchControl
        if self.config.get('prepared_dispatch_enabled') or self.config.get('extraction_mode')!='paper-preextract/1.0':
            raise ValueError('WEB task control requires the normal native paper-preextract path')
        root=Path(getattr(self,'_resume_root',self.store.root)).resolve()
        control=NativeBatchControl(root)
        control._read(batch_id)
        self.config['_native_task_batch']=dict(batch_id=batch_id,root=str(root))
        self.agent.config['_native_task_batch']=copy.deepcopy(self.config['_native_task_batch'])
        return control.snapshot(batch_id)

    def _allocate_task_run(self,proposed_run_id,selected):
        binding=self.config.get('_native_task_batch')
        if not binding:return proposed_run_id
        from native_batch_control import NativeBatchControl
        control=NativeBatchControl(binding['root'])
        parent=getattr(self,'_global_batch_id',None) or self.config.get('retry_of')
        run_id,inputs=control.allocate_run(binding['batch_id'],proposed_run_id,store_root=self.store.root,
            parent_run_id=parent,selected=selected,epoch=self.config.get('_native_task_epoch'))
        self._active_run_id=run_id
        self._bind_task_run(run_id,selected,allocation_inputs=inputs)
        return run_id

    def _task_manifest_published(self,run_id):
        binding=self.config.get('_native_task_batch')
        if binding:
            from native_batch_control import NativeBatchControl
            NativeBatchControl(binding['root']).mark_manifest_published(binding['batch_id'],run_id,
                epoch=self.config.get('_native_task_epoch'))

    def _bind_task_run(self,run_id,selected,*,extend=False,allocation_inputs=None):
        binding=self.config.get('_native_task_batch')
        if binding:
            from native_batch_control import NativeBatchControl, BatchPaused
            from contextlib import nullcontext
            control=NativeBatchControl(binding['root'])
            parent=getattr(self,'_global_batch_id',None) or self.config.get('retry_of')
            try:
                with getattr(self,'_task_binding_manifest_lock',None) or nullcontext():
                    sources,evidence=control.sources_for_records(binding['batch_id'],selected,
                        parent_run_id=run_id if extend else parent)
                    if extend:
                        row=control._read(binding['batch_id'])
                        prior=next(value for value in row['run_bindings'] if value['run_id']==run_id)
                        sources=sorted(set(prior['source_hashes'])|set(sources))
                        proofs={proof['source_sha256']:proof for proof in prior.get('source_evidence',[])}
                        proofs.update({proof['source_sha256']:proof for proof in evidence})
                        evidence=control._refresh_evidence(proofs.values())
                    return control.bind_run(binding['batch_id'],run_id,
                        store_root=self.store.root,parent_run_id=parent,source_hashes=sources,source_evidence=evidence,
                        epoch=self.config.get('_native_task_epoch'),allow_extension=extend,allocation_inputs=allocation_inputs)
            except Exception as exc:
                if isinstance(exc,BatchPaused):raise
                raise BatchPaused('本次实际来源绑定无法重核；未启动新阶段，结果保留') from exc

    def _bind_discovered_task_sources(self,run_id,manifest,leaves,start,*,expected_sources=None,frozen_paths=None,allocated_paths=None):
        """Bind durable native archive rows before their first classifier/child."""
        binding=self.config.get('_native_task_batch')
        if not binding:return
        from native_batch_control import NativeBatchControl, BatchPaused
        try:
            control=NativeBatchControl(binding['root'])
            row=control.assert_running(binding['batch_id'],epoch=self.config.get('_native_task_epoch'))
            frozen=set(row['source_hashes'])
            path=(self.store.runs/run_id/'manifest.json').resolve()
            raw=path.read_bytes();saved=json.loads(raw)
            if saved.get('run_id')!=run_id:raise ValueError('archive producer identity changed')
            added=False
            for offset,record in enumerate(leaves):
                source=Path(record['path']).resolve();source_sha=digest(source)
                if expected_sources is not None and str(source) in expected_sources and expected_sources[str(source)]!=source_sha:
                    raise ValueError('普通原稿与冻结路径和字节不一致')
                if source_sha in frozen:
                    if frozen_paths is not None:
                        lexical=Path(os.path.abspath(record['path']));key=str(lexical)
                        if (lexical.resolve()!=lexical or frozen_paths.get(key)!=source_sha
                                or (allocated_paths or {}).get(key)!=source_sha
                                or any(record.get(name) for name in ('archive_member_path','archive_source_hash',
                                    '_native_task_source_evidence','source_education'))
                                or record.get('input_source_path') and Path(os.path.abspath(record['input_source_path']))!=lexical):
                            raise ValueError('冻结SHA与原稿路径不一致，不能借同内容放行')
                    continue
                if not source.is_relative_to(path.parent):
                    # Reused old members retain their original registered
                    # producer; never relabel an old path as a new extraction.
                    continue
                existing=saved['discovered_inputs'][start+offset]
                if existing.get('path')!=record.get('path'):raise ValueError('archive record identity changed')
                root_path=Path(existing['input_source_path']).resolve()
                root_sha=digest(root_path)
                if (root_sha not in frozen or existing.get('archive_source_hash')!=root_sha
                        or not existing.get('archive_member_path')):
                    raise ValueError('archive member has no frozen original relation')
                record['_native_task_source_evidence']=dict(source_sha256=source_sha,
                    input_source_sha256=root_sha,input_source_path=str(root_path),source_path=str(source),
                    producer_run_id=run_id,relation='archive_member',archive_member_path=existing['archive_member_path'],manifest_path=str(path),
                    manifest_sha256=hashlib.sha256(raw).hexdigest(),record_pointer=f'/discovered_inputs/{start+offset}')
                added=True
            if added:
                self._bind_task_run(run_id,leaves,extend=True)
            else:
                current=next(value for value in row['run_bindings'] if value['run_id']==run_id)
                actual={digest(Path(record['path'])) for record in leaves}
                if not actual<=set(current['source_hashes']):
                    self._bind_task_run(run_id,leaves,extend=True)
                else:
                    control.assert_delivery_ready(binding['batch_id'],run_id,epoch=self.config.get('_native_task_epoch'))
        except Exception as exc:
            if isinstance(exc,BatchPaused):raise
            raise BatchPaused('已保存归档成员，但原生派生证明未闭合；成员暂不准入') from exc

    def _task_delivery_guard(self,run_id,items=None):
        binding=self.config.get('_native_task_batch')
        if not binding:return
        from native_batch_control import NativeBatchControl, BatchPaused
        control=NativeBatchControl(binding['root'])
        epoch=self.config.get('_native_task_epoch')
        try:
            with control.global_control._admission_lock():
                row=control._execution_row(binding['batch_id'],epoch)
            found=[value for value in row['run_bindings'] if value.get('run_id')==run_id]
            if len(found)!=1:raise BatchPaused('排队前未取得本WEB真实AUTO来源绑定')
            current=found[0]
            if current.get('native_revision')!=epoch:
                # SavedHandoffs may revisit an explicitly registered old lineage.
                # Its original identity is refreshed, never this new run's sources.
                self.refresh_task_run(run_id)
            manifest=self.store.load(run_id)
            wanted={item['id'] for item in items} if items is not None else None
            rows=[item for item in manifest.get('items',[]) if
                (wanted is None or item.get('id') in wanted) and
                (item.get('content_format')=='associated_material' or item.get('status')=='pass' and item.get('automatic_approval'))]
            if wanted is not None and wanted!={item['id'] for item in rows}:
                raise BatchPaused('待排队成员与原manifest中实际可交付成员不一致')
            sources=[item.get('source_hash') for item in rows]
            control.assert_delivery_ready(binding['batch_id'],run_id,epoch=epoch,source_hashes=sources or None)
        except Exception as exc:
            if isinstance(exc,BatchPaused):raise
            raise BatchPaused('排队前原AUTO或成果来源无法核对；结果保留未发送') from exc

    def _task_handoff(self,run_id,items,send):
        if not self.config.get('_native_task_batch'):return send()
        try:
            self._task_delivery_guard(run_id,items)
        except __import__('native_batch_control').BatchPaused as exc:
            # A binding hold is not a collector failure and must never set the
            # shared stop token while sibling streams are saving responses.
            write_json(self.store.runs/run_id/'private/task-delivery-binding-hold.json',dict(
                state='pending',reason_kind='batch_paused',error_type=type(exc).__name__,
                reason=str(exc),run_id=run_id,batch_id=self.config['_native_task_batch']['batch_id']))
            return dict(state='pending',reason_kind='batch_paused')
        return send()

    def _global_control(self):
        binding=self.config.get('_native_task_batch')
        if binding:
            from native_batch_control import BatchGlobalTaskControl
            if Path(binding['root']).resolve()!=Path(getattr(self,'_resume_root',self.store.root)).resolve():
                raise ValueError('task control data root changed')
            return BatchGlobalTaskControl(binding['root'],binding['batch_id'],epoch=self.config.get('_native_task_epoch'))
        from global_task_control import GlobalTaskControl
        return GlobalTaskControl(getattr(self,'_resume_root',self.store.root))

    def _global_open(self):
        control=self._global_control()
        poll=getattr(control,'poll_admission',None)
        return poll() if callable(poll) else control.snapshot()['admission_open']

    def __init__(self,store,config=None,key='',vision_key='',fallback_key=''):
        super().__init__(store)
        from preextract_delivery_control import LinkedStop
        self._active_run_id=None
        self.stop=LinkedStop(store.root,lambda:self._active_run_id)
        self.config=config or {}
        resume=self.config.get('resume') if isinstance(self.config.get('resume'),dict) else {}
        state_file=Path(resume.get('state_file','processing_state.json'))
        circuit_root=state_file.parent if state_file.is_absolute() else Path(store.root)/state_file.parent
        self.agent=AgentClient(self.config,key,self.stop,vision_key,fallback_key,state_root=circuit_root)
        self.agent.ledger=Ledger(store.root)
        from stage_cache import StageCache
        self.stage_cache=StageCache(store.root)
        self.rag_client=None
        self._work_pool_datasets=None

    def _material_screen(self,original,source,work,item,stage,*,raw=None):
        stage('前置材料判断','只判断去留和用途；不生成画像、不查引用位置')
        from agent_client import contract_for
        from material_intake import validate_intake
        current={'policy':INTAKE_POLICY,'source_sha256':item['source_hash'],'model':self.config.get('text_model'),
                 'endpoint':self.config.get('base_url'),'vision':self.config.get('vision_model'),
                 'contract_hash':hashlib.sha256(contract_for('material_intake').encode()).hexdigest()}
        if item.get('material_intake_binding')==current:
            result=validate_intake(item['material_intake'])
            if result['intake']=='exclude':raise Rejected(result['reason'],'material_excluded')
            if result['intake']=='pending':raise MaterialPending(result['reason'])
            item['material_processing']=material_pipeline(item)
            return None,{'decision':'pass','reason':result['reason'],'policy':INTAKE_POLICY}
        material=read_material(original,work) if not raw else {'status':'readable','text':raw,'images':[],'format':original.suffix}
        if material['status']!='readable':
            item['material_intake']={'intake':'pending','kind':'unreadable','route':'pending','reason':material.get('reason','未取得可读内容，待解析')}
            raise MaterialPending(item['material_intake']['reason'])
        result=classify_material(self.agent,material['text'],source_sha=item['source_hash'],
            cache_root=self.store.root/'private',images=material['images'],input_format=material['format'])
        from evidence import redactor
        result['reason']=redactor()(result['reason'])
        item['material_intake']=result
        item['material_intake_binding']=current
        if result['intake']=='exclude':raise Rejected(result['reason'],'material_excluded')
        if result['intake']=='pending':raise MaterialPending(result['reason'])
        item['material_processing']=material_pipeline(item)
        return material,{'decision':'pass','reason':result['reason'],'policy':INTAKE_POLICY}

    def _enqueue_approved_after_save(self, run_id, items, progress):
        """Queue completed papers as soon as their manifest record is durable."""
        if self.stop.is_set():return
        disk_delivery=self.config.get('preextract_delivery_mode')=='disk_queue_v1'
        if not ((self.config.get('work_pool_enabled') or disk_delivery) and self.config.get('auto_upload', True)
                and self.rag_client):
            return
        ids=[item['id'] for item in items
             if (disk_delivery and item.get('content_format')=='associated_material') or
             (item.get('status')=='pass' and item.get('automatic_approval')
              and (self.config.get('work_pool_enabled') or item.get('content_format')=='paper_preextract'))]
        if not ids:
            return
        from auto_ragflow import handoff
        def send():
            cached=getattr(self,'_work_pool_datasets',None)
            if cached is None and any(item.get('content_format') not in {'paper_preextract','associated_material'} for item in items if item['id'] in ids):
                try:
                    cached=self.rag_client.datasets()
                    self._work_pool_datasets=cached
                except Exception:
                    # The batch-end sweep retries when RAGFlow inventory is available.
                    return
            return handoff(self.store,run_id,self.rag_client,self.config.get('dataset_map',{}),progress,
                self.config.get('auto_parse',True),
                self.config.get('legacy_dataset_map_fallback',True),
                self.config.get('concurrency'),target_datasets=self.config.get('task_datasets'),
                item_ids=ids,upload_config=dict(self.config,_work_pool_datasets=cached,
                                              manual_request=False,work_pool_strict=False,_stop_event=self.stop))
        return self._task_handoff(run_id,[item for item in items if item['id'] in ids],send)

    def _concurrency_options(self):
        raw=self.config.get('concurrency') if isinstance(self.config.get('concurrency'),dict) else {}
        return {**{'enabled':True,'papers':2,'images_per_paper':2,'global_llm':4,
                   'max_retries':4,'backoff_base_seconds':1.0},**raw}

    def _known_sources(self):
        # A historical fingerprint is a local-cache hint, not proof that its
        # RAG documents still exist. The new adapter validates/reuses that cache
        # and lets the existing handoff check each remote document again.
        if self.config.get('extraction_mode')=='paper-preextract/1.0':return set()
        known=set()
        for previous in self.store.history():
            broken={issue.get('id') for issue in previous.get('human_view',{}).get('issues',[])
                    if isinstance(issue,dict) and issue.get('reason')}
            known.update(item.get('source_hash') for item in previous.get('items',[])
                         if item.get('status')=='pass' and item.get('processing_version')==VERSION
                         and item.get('id') not in broken)
        return known

    def _run_parallel(self,selected,callback,operation='process'):
        """Run independent papers in private stores and merge every result.

        The legacy single-paper implementation remains the worker body.  This
        keeps the disabled path byte-for-byte compatible while giving each
        worker its own mutable Agent/cache context. Private source copies and
        child manifests remain available for diagnosis and failed-item retries.
        """
        from billing import Ledger,summarize
        from processing_state import ProcessingState
        options=self._concurrency_options();paper_workers=max(1,int(options['papers']))
        from preextract_disk_pressure import DiskPressure
        md_mode=(self.config.get('extraction_mode')=='paper-preextract/1.0' or
                 all((r.get('prepared_checkpoint') or {}).get('format')=='paper-preextract-prepared/1' for r in selected))
        self._storage_pressure=DiskPressure(self.store.root,
            admission_check=self._assert_task_admission if self.config.get('_native_task_batch') else None) if md_mode else None
        global_gate=_ObservedGate(options['global_llm'],disk=self._storage_pressure,stop=self.stop)
        purpose_options=self.config.get('purpose_review') or {}
        purpose_gate=_ObservedGate(purpose_options.get('workers',10),disk=self._storage_pressure,stop=self.stop)
        lanes=_configure_llm_lanes(self.config,options,md_mode,global_gate,purpose_gate)
        staged=bool(md_mode and purpose_options.get('enabled') and not getattr(self,'_preserve_stop',False))
        post_options=self.config.get('preextract_postprocess') or {}
        prepare_options=self.config.get('preextract_preparation') or {}
        post_workers=post_options.get('workers',2)
        # Requests reserve space for their replies. Keep room for a complete
        # second cohort so ten in-flight requests do not make a saved reply
        # wait for local processing before the next model can be admitted.
        post_waiting=post_options.get('max_waiting',max(8,2*paper_workers-int(post_workers)))
        slots=(_PreextractSlots(paper_workers,post_workers,post_waiting,self.stop,
                               prepare_limit=prepare_options.get('workers',4),max_prepared=prepare_options.get('max_pending',8),
                               purpose_limit=purpose_gate.limit,disk=self._storage_pressure,
                               admission_check=self._assert_task_admission if self.config.get('_native_task_batch') else None)
               if md_mode else None)
        rid=datetime.now().strftime('AUTO_%Y%m%d_%H%M%S_')+uuid.uuid4().hex[:6]
        rid=self._allocate_task_run(rid,selected)
        self._active_run_id=rid
        run=self.store.runs/rid;run.mkdir(parents=True,exist_ok=True)
        state_cfg=self.config.get('resume') if isinstance(self.config.get('resume'),dict) else {}
        state=ProcessingState(self.store.root,state_cfg.get('state_file','processing_state.json'),processing_version=VERSION) if state_cfg.get('enabled',True) else None
        manifest={'schema_version':2,'pipeline':'automatic','run_id':rid,'operation':operation,'status':'running','source_count':len(selected),
                  'processed_sources':0,'started_at':datetime.now().isoformat(timespec='seconds'),'items':[],'events':[],
                  'inputs':copy.deepcopy(selected),'message':'开始并发自动预处理','billing':summarize([]),'child_runs':[],
                  'retry_of':self.config.get('retry_of'), 'task_settings':copy.deepcopy(self.config.get('task_settings',{})),
                  'concurrency':{'enabled':True,'papers':paper_workers,
                                 'images_per_paper':int(options['images_per_paper']),
                                  **lanes,'llm_peak':0,
                                 'min_start_interval_seconds':float(options.get('min_start_interval_seconds',0))}}
        if staged:
            manifest.update(scan_source_count=len(selected),admission_schema='confirmed-body-admission/1')
            manifest['concurrency'].update(startup_target=0,classification_workers=purpose_gate.limit,
                formal_started=False,confirmed_bodies=0,body_candidates=0)
        self.store.save(manifest)
        self._task_manifest_published(rid)
        started_monotonic=time.monotonic()
        from model_activity import record_run_owner
        record_run_owner(self.store.root,rid)
        manifest_lock=threading.RLock()
        billing_run_ids=set()
        parent_ledger=Ledger(self.store.root)
        known=self._known_sources()
        claimed=set(known)
        claim_lock=threading.Lock()
        def claim_source(source_hash):
            with claim_lock:
                if source_hash in claimed:return False
                claimed.add(source_hash)
                return True

        candidates=[]
        input_hashes=set(known)
        for record in selected:
            if self.stop.is_set():break
            kind=material_kind(record)
            related_container=kind=='archive' and is_attachment_container(record,getattr(self,'_material_context',None))
            retain_relation=self.config.get('extraction_mode')=='paper-preextract/1.0' and (kind not in {'document','archive'} or related_container)
            try: source_hash=None if staged or record.get('archive_source_hash') or retain_relation else digest(Path(record['path']))
            except Exception: source_hash=None
            if source_hash and self.config.get('extraction_mode')!='paper-preextract/1.0' and _is_uploaded_record(record,source_hash):
                with manifest_lock:
                    manifest['events'].append({'status':'uploaded_skip','source':str(record['path']),
                                               'reason':'源文件已有已上传记录，本批跳过预处理，未调用 AI'})
                    manifest['processed_sources']+=1
                    self.store.save(manifest)
                continue
            if source_hash and source_hash in input_hashes:
                with manifest_lock:
                    manifest['events'].append({'status':'duplicate','source':str(record['path']),
                                               **({'project_reference':duplicate_project_reference(record,source_hash)} if md_mode and material_kind(record)=='document' else {}),
                                               'reason':'已有相同源文件在其他批次完成或正在处理，本次跳过'})
                    manifest['processed_sources']+=1
                    self.store.save(manifest)
                continue
            if source_hash:input_hashes.add(source_hash)
            candidates.append(record)

        def emit_parent(stage,message='',filename=''):
            with manifest_lock:
                self._emit(manifest,callback,stage,filename,message)

        from progress_snapshots import ProgressSnapshots
        progress_snapshots=ProgressSnapshots(self.store,rid)
        # Child business work never waits for parent reporting or its disk I/O.
        progress_lock=threading.Lock()
        pending_progress={}
        binding_hold=_AdmissionHold()
        def hold_binding(exc):
            binding_hold.hold(exc)
            with manifest_lock:
                manifest['task_binding_attention']=dict(reason_kind='batch_paused',error_type=type(exc).__name__,
                    reason=str(exc),state='pending',updated_at=datetime.now().isoformat(timespec='seconds'))
                self.store.save(manifest)
        def flush_progress():
            with progress_lock:
                updates=list(pending_progress.values());pending_progress.clear()
            for apply,payload in updates:apply(payload)

        def child(record,ticket=None):
            result=None
            worker_started=False
            worker_root=run/'private'/'workers'/uuid.uuid4().hex
            context={'input_source':str(record['path']),
                     'input_name':record.get('name') or Path(record['path']).name}
            try:
                if self.stop.is_set():
                    return {**context,'items':[],'run_id':'','events':[],'error':'','status':'stopped'}
                from engine import Store
                worker_config=copy.deepcopy(self.config)
                worker_config.pop('retry_of',None)
                worker_config['auto_upload']=False
                worker_config['concurrency']={**options,'enabled':True,'papers':1,
                                              '_llm_gate':global_gate}
                if state is not None:
                    worker_config['resume']={**state_cfg,'state_file':str(state.path)}
                worker_store=Store(worker_root)
                worker=AutoProcessor(worker_store,worker_config)
                worker._material_inputs=getattr(self,'_material_inputs',selected)
                worker._material_context=getattr(self,'_material_context',None)
                worker._claim_source=claim_source
                worker.stop=self.stop
                worker._preserve_stop=True
                worker._purpose_preclassified_only=staged
                template=self.agent
                if isinstance(template,AgentClient):
                    worker_state_file=Path(worker_config.get('resume',{}).get('state_file','processing_state.json'))
                    worker_circuit_root=worker_state_file.parent if worker_state_file.is_absolute() else Path(self.store.root)/worker_state_file.parent
                    worker.agent=AgentClient(worker_config,getattr(template,'key',''),worker.stop,
                                             getattr(template,'vision_key',''),getattr(template,'vision_fallback_key',''),
                                             state_root=worker_circuit_root)
                    worker.agent.opener=template.opener
                else:
                    worker.agent=copy.copy(template)
                worker.agent.stop=worker.stop
                worker.agent.llm_gate=global_gate
                worker.agent.purpose_gate=purpose_gate
                worker.agent.stage_cache=self.stage_cache
                worker.agent.ledger=Ledger(self.store.root)
                worker.agent.config=worker_config
                worker.agent.vision_key=getattr(template,'vision_key','')
                worker.agent.key=getattr(template,'key','')
                worker.agent.billing_context={'run_id':rid}
                worker.stage_cache=self.stage_cache
                worker._resume_root=getattr(self,'_resume_root',self.store.root)
                worker._global_batch_id=rid
                worker._task_binding_manifest_lock=manifest_lock
                worker._task_binding_hold=binding_hold
                if slots:
                    worker._preextract_enter_prepare=lambda:slots.enter_prepare(ticket)
                    def enter_model():
                        if admission:admission.wait_formal()
                        slots.enter_model(ticket)
                    worker._preextract_enter_model=enter_model
                    worker._preextract_postprocess=lambda:slots.postprocess(ticket)
                    worker._preextract_conversion=lambda:slots.conversion(ticket)
                    worker._preextract_purpose=lambda:slots.purpose(ticket)

                def apply_child_progress(payload):
                    # worker.stop is the parent's shared Event; notification
                    # must not re-register the same durable upload stop.
                    progress=payload.get('progress') or {}
                    progress={**progress,'paper_id':record.get('name') or Path(record['path']).name}
                    if progress.get('stage') == '任务结束':
                        progress.update(stage='单篇处理结束', message='单篇结果已保存，整批继续处理')
                    with manifest_lock:
                        if payload.get('run_id'):
                            billing_run_ids.add(payload['run_id'])
                            manifest['billing_run_ids']=sorted(billing_run_ids)
                        if payload.get('ai_item'):
                            active=copy.deepcopy(payload['ai_item'])
                            key=str(record['path'])
                            active.setdefault('input_source_path',key)
                            active['_worker_input_source']=key
                            manifest['active_items']=[i for i in manifest.get('active_items',[]) if i.get('_worker_input_source',i.get('input_source_path'))!=key]+[active]
                            progress_snapshots.progress(manifest)
                        progress.update(completed=manifest['processed_sources'],total=manifest['source_count'],
                                        source_path=str(record['path']))
                        paper_label=record.get('name') or Path(record['path']).name
                        update={'run_id':rid,'status':manifest['status'],'message':f'[{paper_label}] '+payload.get('message',''),
                                  'processed_sources':manifest['processed_sources'],'source_count':manifest['source_count'],
                                  'progress':progress, 'ai_item':payload.get('ai_item')}
                        if 'billing' in payload:
                            manifest['billing']=parent_ledger.summary(run_ids=billing_run_ids)
                            progress_snapshots.progress(manifest)
                            update['billing'] = copy.deepcopy(manifest['billing'])
                            update['billing_item'] = payload.get('billing_item')
                        callback(update)
                def child_progress(payload):
                    snapshot=copy.deepcopy({key:payload[key] for key in (
                        'run_id','ai_item','progress','message','billing','billing_item') if key in payload})
                    key=str(record['path'])
                    with progress_lock:
                        prior=pending_progress.get(key)
                        if prior:
                            merged=dict(prior[1]);merged.update(snapshot);snapshot=merged
                        pending_progress[key]=(apply_child_progress,snapshot)
                    if slots:slots.changed.set()
                worker_started=True
                result=(worker.run([record],child_progress,operation=operation)
                        if operation != 'process' else worker.run([record],child_progress))
                if result.get('status')=='completed' and Path(record['path']).suffix.lower()=='.zip':
                    from purpose_review import remember_container
                    remember_container(record,result.get('items',[]),worker.config.get('purpose_review'))
                items=[]
                for child_item in result.get('items',[]):
                    if child_item.get('status') not in {'pass','reject','pending','associated','deleted'}:continue
                    item=copy.deepcopy(child_item)
                    item.setdefault('input_source_path',context['input_source'])
                    if item.get('status')=='pass':
                        source_package=Path(item['package_path']);destination=run/'packages'/item['id']
                        destination.parent.mkdir(parents=True,exist_ok=True);shutil.copytree(source_package,destination)
                        item['package_path']=str(destination)
                        if item.get('content_format')=='paper_preextract':
                            item['path']=item['display_path']=str(destination/'article.redacted.md')
                        else:
                            item['path']=str(destination/'ingest.md');item['display_path']=str(destination/'document.md')
                        item['knowledge_manifest_path']=str(destination/'manifest.json')
                    elif item.get('status')=='deleted':
                        audit=Path(item['deletion_receipt']);destination=run/'deleted'/(item['id']+'.json')
                        destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(audit,destination)
                        item['deletion_receipt']=item['path']=item['display_path']=str(destination)
                    else:
                        source_path=Path(item['path']);destination=run/('pending' if item.get('status')=='pending' else 'associated' if item.get('status')=='associated' else 'rejected')/(item['id']+'.md')
                        destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source_path,destination)
                        item['path']=str(destination);item['display_path']=str(destination)
                    items.append(item)
                events=copy.deepcopy(result.get('events',[]))
                for event in events:
                    event.setdefault('input_source_path',context['input_source'])
                error='' if items or events or result.get('status')=='stopped' else 'worker 未返回终态结果或跳过记录'
                child_run_id=result.get('run_id','')
                return {**context,'items':items,'events':events,'run_id':child_run_id,
                        'manifest_path':str(worker_store.runs/child_run_id/'manifest.json'),
                         'status':result.get('status','completed'),'error':error,
                         'discovered_inputs':result.get('discovered_inputs',[])}
            except Exception as exc:
                if isinstance(exc,__import__('native_batch_control').BatchPaused):
                    if result is None and worker_started:
                        active_id=getattr(locals().get('worker'),'_active_run_id',None)
                        if active_id:
                            try:result=worker_store.load(active_id)
                            except (OSError,ValueError):pass
                    hold_binding(exc)
                    # This is a permission/mapping hold, never a paper quality
                    # rejection. Preserve any already durable child outcome.
                    saved_items=copy.deepcopy((result or {}).get('items',[]))
                    saved_events=copy.deepcopy((result or {}).get('events',[]))
                    for saved_item in saved_items:saved_item.setdefault('input_source_path',context['input_source'])
                    saved_events.append(dict(status='task_binding_pending',source=str(record['path']),
                        reason_kind='batch_paused',reason=str(exc),business_decision='pending'))
                    child_id=(result or {}).get('run_id','')
                    if not child_id:
                        # An allocated ID is not a published child manifest.
                        # Keep it as evidence, but do not add a dangling child
                        # provenance edge to a file that never existed.
                        allocated=getattr(locals().get('worker'),'_active_run_id','')
                        if allocated:saved_events[-1]['allocated_run_id']=allocated
                    return {**context,'items':saved_items,'events':saved_events,'run_id':child_id,
                        'manifest_path':str(worker_store.runs/child_id/'manifest.json') if child_id else '',
                        'error':'','status':'stopped','discovered_inputs':copy.deepcopy((result or {}).get('discovered_inputs',[]))}
                if result is None and worker_started:
                    saved_runs=worker_store.history()
                    result=saved_runs[0] if saved_runs else None
                if result and (result.get('items') or result.get('events') or result.get('discovered_inputs')):
                    # A failed view/report/copy must not erase durable member
                    # outcomes. Their saved private outputs remain readable.
                    saved_items=copy.deepcopy(result.get('items',[]))
                    for saved_item in saved_items:
                        saved_item.setdefault('input_source_path',context['input_source'])
                    saved_events=copy.deepcopy(result.get('events',[]))
                    for saved_event in saved_events:
                        saved_event.setdefault('input_source_path',context['input_source'])
                    child_run_id=result.get('run_id','')
                    return {**context,'items':saved_items,'events':saved_events,'run_id':child_run_id,
                            'manifest_path':str(worker_store.runs/child_run_id/'manifest.json'),
                            'error':type(exc).__name__,'status':'failed',
                            'discovered_inputs':copy.deepcopy(result.get('discovered_inputs',[]))}
                child_items=(result or {}).get('items',[])
                saved_child=child_items[0] if len(child_items)==1 else {}
                ai_result=(copy.deepcopy(child_items[0].get('ai_result',{})) if len(child_items)==1 else {})
                if not ai_result:
                    ai_result={'status':'unknown' if worker_started else 'not_started',
                               'reason':'并发执行异常，未取得完整 AI 记录' if worker_started else '并发任务未启动 AI'}
                return {**context,'items':[{'id':'AUTO_'+uuid.uuid4().hex[:20],'title':record.get('name','未通过文件'),
                                 'major':'未识别专业','category':'未识别','source_path':str(record.get('path','')),
                                 'source_hash':saved_child.get('source_hash',''),'status':'reject','reason':'并发 worker 异常：'+type(exc).__name__,
                                 'reason_kind':'technical','business_decision':'reject','uploads':{},
                                 'prepared_checkpoint':copy.deepcopy(saved_child.get('prepared_checkpoint')),
                                 'previous_ai_reason':saved_child.get('previous_ai_reason',''),
                                 'processing_version':saved_child.get('processing_version',VERSION),
                                 'path':'','display_path':'','automatic_approval':False,
                                 'script_result':{'status':'failed','reason':'并发执行或结果保存失败：'+type(exc).__name__},
                                 'ai_result':ai_result,
                                 'ai_steps':copy.deepcopy(child_items[0].get('ai_steps',[])) if len(child_items)==1 else []}],
                         'run_id':'','events':[],'error':type(exc).__name__,'status':'failed',
                         'discovered_inputs':(result or {}).get('discovered_inputs',[])}
            finally:
                if slots:slots.finish(ticket)

        admission=None
        storage_source_eof=[False]
        def begin_admission():
            from preextract_admission import StagedAdmission,project_sources,classify_source,model_history_index
            batch_binding=self.config.get('_native_task_batch')
            plain_sources=None
            frozen_paths=None
            allocated_paths={}
            if batch_binding:
                from native_batch_control import NativeBatchControl
                native=NativeBatchControl(batch_binding['root'])
                frozen_paths=getattr(self,'_task_frozen_inputs',None)
                with native.global_control._admission_lock():
                    row=native._execution_row(batch_binding['batch_id'],self.config.get('_native_task_epoch'))
                    matches=[value for value in row['run_bindings'] if value.get('run_id')==rid]
                    if len(matches)!=1:raise ValueError('当前原稿冻结绑定不唯一')
                    frozen=set(row['source_hashes'])
                    plain_sources={}
                    if frozen_paths is not None:
                        for value in (matches[0].get('allocation') or {}).get('selected_inputs',()):
                            path=str(Path(os.path.abspath(value['path'])));sha=value.get('source_hash')
                            allocated_paths[path]=sha
                            if path in frozen_paths:
                                if sha!=frozen_paths[path] or sha not in frozen or Path(path).resolve()!=Path(path):
                                    raise __import__('native_batch_control').BatchPaused('实际原稿与WEB冻结路径和内容不一致')
                                plain_sources[path]=sha
            def is_frozen_plain(originals,leaves):
                if plain_sources is None:return True
                if len(originals)!=len(leaves):return False
                for original,leaf in zip(originals,leaves):
                    path=str(Path(original['path']).resolve());expected=plain_sources.get(path)
                    if (not expected or str(Path(leaf['path']).resolve())!=path
                            or any(record.get(key) for record in (original,leaf)
                                for key in ('archive_member_path','archive_source_hash','_native_task_source_evidence','source_education'))
                            or any(record.get('input_source_path') and str(Path(record['input_source_path']).resolve())!=path for record in (original,leaf))
                            or any(record.get(key) and record[key]!=expected for record in (original,leaf) for key in ('sha256','source_hash'))):
                        return False
                return True
            def discovered(originals,leaves):
                with manifest_lock:
                    strict=None
                    if frozen_paths is not None:
                        strict={}
                        for record in originals:
                            _guard_frozen_plain(record,frozen_paths,allocated_paths)
                        for record in leaves:
                            identity=_guard_frozen_plain(record,frozen_paths,allocated_paths)
                            if identity:strict[identity[0]]=identity[1]
                    start=len(manifest.setdefault('discovered_inputs',[]))
                    manifest.setdefault('discovered_inputs',[]).extend(copy.deepcopy(leaves))
                    manifest['source_count']+=len(leaves)-len(originals)
                    manifest['formal_task_count']=manifest['source_count']
                    self.store.save(manifest)
                    from contextlib import nullcontext
                    guard=binding_hold.binding() if not is_frozen_plain(originals,leaves) else nullcontext()
                    with guard:
                        self._bind_discovered_task_sources(rid,manifest,leaves,start,expected_sources=strict,
                            frozen_paths=frozen_paths,allocated_paths=allocated_paths)
            def sources():
                try:
                    yield from project_sources(self,candidates,run,discovered,
                        batch_size=options.get('source_binding_batch_size',purpose_options.get('source_binding_batch_size',1)),
                        can_batch=is_frozen_plain)
                    if binding_hold.is_open() and not self.stop.is_set() and self._global_open():
                        storage_source_eof[0]=True
                except __import__('native_batch_control').BatchPaused as exc:
                    hold_binding(exc)
                    # The iterator closes without StagedAdmission.admission_error,
                    # so sibling requests drain without shared failure-stop.
                    return
            history=model_history_index(getattr(self,'_resume_root',self.store.root))
            def classify(record):
                with binding_hold.stage(self._global_control(),'model',batch_id=rid,paper_id=record['path'],name=Path(record['path']).name):
                    return classify_source(self,record,run,rid,purpose_gate,parent_ledger,history,global_gate=global_gate,purpose_hold=binding_hold)
            admission=StagedAdmission(sources(),
                classify,
                workers=purpose_gate.limit,startup=0,ready_limit=max(100,2*paper_workers),
                stop=self.stop,on_change=(lambda counts:slots.changed.set()) if slots else None,
                global_control=self._global_control(),batch_id=rid,diagnostic_root=self.store.root,
                can_dispatch=lambda:binding_hold.is_open())
            billing_run_ids.add(rid)
            admission.start()
            return admission
        child_run_ids=[]
        first_failure=None
        handoffs=None
        if md_mode and self.config.get('preextract_delivery_mode')=='disk_queue_v1':
            from saved_handoffs import SavedHandoffs
            handoffs=SavedHandoffs(self.store,self.stop,
                lambda target,rows:self._enqueue_approved_after_save(target,rows,lambda *args,**kwargs:None),
                retry_of=self.config.get('retry_of'))
        try:
            if staged:admission=begin_admission()
            with ThreadPoolExecutor(max_workers=slots.capacity if slots else paper_workers,thread_name_prefix='paper') as pool:
                try:
                    remaining=iter(candidates)
                    pending=set()
                    tickets={}
                    next_record=[None]
                    def refill():
                        if admission and admission.snapshot().get('admission_error'):
                            raise RuntimeError('前置分类异常，已保留来源与完成结果：'+admission.snapshot()['admission_error'])
                        while binding_hold.is_open() and not self.stop.is_set() and self._global_open() and len(pending)<(slots.capacity if slots else paper_workers):
                            if next_record[0] is None:
                                if admission:
                                    next_record[0]=admission.take()
                                    if next_record[0] is None:break
                                else:
                                    try:next_record[0]=next(remaining)
                                    except StopIteration:
                                        storage_source_eof[0]=True
                                        break
                            ticket=uuid.uuid4().hex
                            auxiliary=next_record[0].get('admission_lane') in {'associated','pending','duplicate'}
                            if slots and not slots.admit(ticket,auxiliary=auxiliary):
                                if auxiliary and admission:
                                    # Do not retain an attachment at the head
                                    # of the ready-body queue while its lane is full.
                                    with admission.changed:
                                        admission.others.append(next_record[0]);admission.changed.notify_all()
                                    next_record[0]=None
                                break
                            record=next_record[0];next_record[0]=None
                            try:future=pool.submit(child,record,ticket)
                            except BaseException:
                                if slots:slots.finish(ticket)
                                raise
                            pending.add(future);tickets[future]=ticket
                            if slots:future.add_done_callback(lambda _future:slots.changed.set())
                    refill()
                    while (pending or (binding_hold.is_open() and admission and not admission.drained() and not self.stop.is_set() and self._global_open())
                           or (binding_hold.is_open() and not admission and next_record[0] is not None and not self.stop.is_set() and self._global_open())):
                        flush_progress()
                        if slots:
                            # Preparing material and saving a response both wake
                            # refill while the original worker continues its stages.
                            slots.changed.clear()
                            finished={future for future in pending if future.done()}
                            pending-=finished
                        else:
                            finished,pending=wait(pending,return_when=FIRST_COMPLETED)
                        for future in finished:
                            result=future.result()
                            tickets.pop(future,None)
                            with manifest_lock:
                                items=result.get('items',[])
                                manifest['items'].extend(items)
                                manifest['active_items']=[i for i in manifest.get('active_items',[])
                                                          if i.get('_worker_input_source',i.get('input_source_path'))!=result.get('input_source')]
                                manifest.setdefault('discovered_inputs',[]).extend(result.get('discovered_inputs',[]))
                                manifest['events'].extend(result.get('events',[]))
                                if result.get('run_id'):
                                    child_run_ids.append(result['run_id'])
                                    billing_run_ids.add(result['run_id'])
                                    manifest['billing_run_ids']=sorted(billing_run_ids)
                                    manifest['child_runs'].append({key:result.get(key,'') for key in
                                                                  ('run_id','input_source','manifest_path','status')})
                                manifest['processed_sources']+=1
                                if result.get('error'):
                                    manifest['events'].append({'status':'worker_error','reason':result['error']})
                                manifest['billing']=parent_ledger.summary(run_ids=billing_run_ids)
                                elapsed=max(time.monotonic()-started_monotonic,1e-6)
                                rate=manifest['processed_sources']/elapsed
                                eta_seconds=round((manifest['source_count']-manifest['processed_sources'])/rate) if rate else None
                                manifest['eta_seconds']=eta_seconds
                                manifest['concurrency']['llm_peak']=global_gate.peak
                                self.store.save(manifest)
                                if handoffs:handoffs.submit(rid,items)
                                else:self._enqueue_approved_after_save(rid,items,emit_parent)
                                if self.config.get('work_pool_enabled'):
                                    manifest=self.store.load(rid)
                                eta_text='计算中' if eta_seconds is None else f'{eta_seconds//3600}h{(eta_seconds%3600)//60:02d}m'
                                paper_label=result.get('input_name','')
                                callback({'run_id':rid,'status':'running','message':f"[{paper_label}] 已完成 {manifest['processed_sources']}/{manifest['source_count']}，ETA {eta_text}",
                                          'processed_sources':manifest['processed_sources'],'source_count':manifest['source_count'],
                                          'billing':copy.deepcopy(manifest['billing']),
                                          'progress':{'stage':('不通过' if any(i.get('script_result',{}).get('status') in {'failed','rejected','stopped'} or
                                                                                           (not i.get('script_result') and i.get('status')=='reject') for i in items) else
                                                              '待解析' if any(i.get('status')=='pending' for i in items) else
                                                              '关联资料保存' if items and all(i.get('status')=='associated' for i in items) else
                                                              '通过' if items else '用户停止' if result.get('status')=='stopped' else '跳过'),
                                                      'file':paper_label,'source_path':result.get('input_source',''),
                                                      'completed':manifest['processed_sources'],'total':manifest['source_count']}})
                        refill()
                        if slots:
                            with manifest_lock:
                                counts=slots.snapshot()
                                counts.update(global_gate.snapshot())
                                if md_mode and lanes['request_lanes']=='independent-purpose/1':
                                    counts['paper_llm_active']=global_gate.current
                                    counts['purpose_llm_active']=purpose_gate.current
                                    counts['llm_active']=counts['total_llm_active']=global_gate.current+purpose_gate.current
                                    counts['purpose_llm_peak']=purpose_gate.peak
                                counts['purpose_llm_limit']=purpose_gate.limit
                                if admission:counts.update(admission.snapshot())
                                if any(manifest['concurrency'].get(k)!=v for k,v in counts.items()):
                                    manifest['concurrency'].update(counts)
                                    progress_snapshots.progress(manifest)
                                    callback({'run_id':rid,'status':'running','pipeline_concurrency':counts,
                                        'processed_sources':manifest['processed_sources'],'source_count':manifest['source_count'],
                                        'message':f"正文请求 {counts['paper_llm_active']}/{global_gate.limit} · 用途识别 {counts['purpose_llm_active']}/{purpose_gate.limit} · 正文准备 {counts['preparation_active']}/{slots.prepare_limit} · 正文就绪 {counts['preparation_waiting']} · 后处理 {counts['postprocess_active']}/{slots.post_limit}"+(' · 磁盘余量不足，暂停新任务，已发请求继续收尾' if counts.get('storage_paused') else '')})
                            if pending or admission or next_record[0] is not None:slots.changed.wait(timeout=0.1)
                    if admission and (not admission.drained() or next_record[0] is not None):
                        # This thread may be the only observer of a short pause.
                        # Stop its producer before joining a bounded ready queue.
                        admission.interrupt()
                except BaseException as exc:
                    from processing_stop import record_failure,stop_after_failure
                    record_failure(self.store.root,exc,stage='parallel_collector',run_id=rid)
                    stop_after_failure(self.store.root,self.stop,run_id=rid)
                    if slots:
                        with slots.condition:slots.condition.notify_all()
                        slots.changed.set()
                    raise
            if admission:
                # Finish the source producer before deciding whether the batch
                # reached EOF. A transient pause may have interrupted it.
                admission.close()
                if admission.snapshot().get('admission_error'):
                    raise RuntimeError('前置分类未完成：'+admission.snapshot()['admission_error'])
            source_complete = bool(not admission or (storage_source_eof[0] and
                admission.snapshot().get('source_exhausted') and not admission.snapshot().get('admission_interrupted')
                and admission.snapshot().get('project_pending')==0 and admission.drained()
                and next_record[0] is None))
            manifest=self.store.load(rid)
            if admission:manifest['concurrency'].update(admission.snapshot())
            failed_children=any(child.get('status')=='failed' for child in manifest.get('child_runs',[])) or any(
                event.get('status')=='worker_error' for event in manifest.get('events',[]))
            manifest['status']='stopped' if not binding_hold.is_open() or self.stop.is_set() or not self._global_open() or not source_complete else 'failed' if failed_children else 'completed'
            resolve_duplicate_projects(manifest)
            from preextract_task_storage import mark_production
            mark_production(manifest,storage_source_eof[0] and source_complete)
            manifest['message']=f"通过 {sum(i.get('status')=='pass' for i in manifest['items'])}，不通过 {sum(i.get('status')=='reject' for i in manifest['items'])}"
            for code,label in [('associated','关联资料'),('pending','待解析'),('deleted','已删除')]:
                count=sum(i.get('status')==code for i in manifest['items'])
                if count:manifest['message']+=f'，{label} {count}'
            if failed_children:manifest['message']+='；有子任务异常，已保存成员结果与待处理来源，可继续处理'
            if not binding_hold.is_open():manifest['message']+='；交付来源绑定待续，已保存结果与未完成来源'
            if admission and not source_complete:manifest['message']+='；前置来源尚未扫完，已保存结果与未完成来源'
            duplicate_count=sum(e.get('status')=='duplicate' for e in manifest['events'])
            if duplicate_count:manifest['message']+=f'，重复跳过 {duplicate_count}'
            uploaded_skip_count=sum(e.get('status')=='uploaded_skip' for e in manifest['events'])
            if uploaded_skip_count:manifest['message']+=f'，已上传跳过 {uploaded_skip_count}'
            manifest.setdefault('concurrency',{})['llm_peak']=global_gate.peak
            manifest['finished_at']=datetime.now().isoformat(timespec='seconds')
            self.store.save(manifest)
        except BaseException as exc:
            first_failure=exc
            if admission:manifest['concurrency'].update(admission.snapshot())
            manifest['status']='failed'
            manifest['message']='并发任务结束异常：'+type(exc).__name__+'；已保存结果与未完成来源'
            manifest['finished_at']=datetime.now().isoformat(timespec='seconds')
            try:self.store.save(manifest)
            except Exception:pass
            raise
        finally:
            if admission:
                if first_failure is not None:
                    from processing_stop import stop_after_failure
                    stop_after_failure(self.store.root,self.stop,run_id=rid)
                admission.close()
            if handoffs:handoffs.close()
            if manifest.get('status')=='completed' and self.rag_client and self.config.get('auto_upload',True) and not self.stop.is_set():
                from auto_ragflow import handoff
                self._task_handoff(rid,None,lambda:handoff(self.store,rid,self.rag_client,self.config.get('dataset_map',{}),emit_parent,
                        self.config.get('auto_parse',True),self.config.get('legacy_dataset_map_fallback',True),options,
                        target_datasets=self.config.get('task_datasets'),upload_config=dict(self.config,_stop_event=self.stop)))
                manifest=self.store.load(rid)
                if self.stop.is_set():
                    manifest.update(status='stopped',message='已停止交付，已有处理结果和回执保留')
                    self.store.save(manifest)
            try:
                self.store.refresh_view(rid)
                if first_failure is None:
                    manifest=self.store.load(rid);emit_parent('任务结束',manifest['message'])
            except Exception:
                if first_failure is None:raise
        return manifest

    def run(self,selected,callback=lambda event:None,operation='process'):
        binding=self.config.get('_native_task_batch')
        if binding and not getattr(self,'_preserve_stop',False):
            with self.task_execution(selected):
                return self._run_task_originals(selected,callback,operation)
        return self._run_task_originals(selected,callback,operation)

    def _run_task_originals(self,selected,callback,operation):
        from preextract_nas import use_inputs
        with use_inputs(self.store.root,selected,stop=self.stop) as save:
            result=self._run_with_originals(selected,callback,operation);save(result);return result

    def _run_with_originals(self,selected,callback=lambda event:None,operation='process'):
        if not selected:raise ValueError('请选择源文件')
        # The UI clears the event before dispatch. Never clear a user's stop
        # request inside a worker, including one still entering this method.
        from processing_stop import stop_scope
        if self.stop.is_set() or not self._global_open():
            return self._stopped_before_start(selected,callback,operation)
        if self.config.get('prepared_dispatch_enabled'):
            from preextract_coordinator import run_scope
            return run_scope(self,selected,callback,resume_scope_id=self.config.get('prepared_resume_scope_id'),
                             startup=bool(self.config.get('prepared_startup_recovery')))
        from purpose_review import scope as purpose_scope
        with stop_scope(self.stop),purpose_scope(self,allow_requests=False):
            result=self._run(selected,callback,operation)
        # The legacy automatic tail starts a prepared Coordinator, whose stop
        # lifecycle is global. WEB tasks keep checkpoints for explicit scoped
        # resume instead of silently crossing to that different controller.
        if self.config.get('_native_task_batch'):return result
        from preextract_recovery_entry import recover_failed_batch
        if not self._global_open():return result
        return recover_failed_batch(self,result,callback,operation)

    def _stopped_before_start(self,selected,callback,operation):
        rid=datetime.now().strftime('AUTO_%Y%m%d_%H%M%S_')+uuid.uuid4().hex[:6]
        rid=self._allocate_task_run(rid,selected)
        now=datetime.now().isoformat(timespec='seconds')
        manifest=dict(schema_version=2,pipeline='automatic',run_id=rid,operation=operation,
            status='stopped',source_count=len(selected),processed_sources=0,inputs=list(selected),
            items=[],events=[],started_at=now,finished_at=now,
            retry_of=self.config.get('retry_of'),task_settings=copy.deepcopy(self.config.get('task_settings',{})),
            message='已停止；尚未进入处理，全部来源保留待继续',billing=summarize([]))
        self.store.save(manifest)
        self._task_manifest_published(rid)
        self.store.refresh_view(rid)
        callback(manifest.copy())
        return manifest

    def _run(self,selected,callback=lambda event:None,operation='process'):
        if not selected: raise ValueError('请选择源文件')
        # Batch context is selected inputs only. Workers inherit it; never scan
        # neighboring unselected directories to guess project relationships.
        if not getattr(self,'_preserve_stop',False):
            from purpose_review import wait_for_delivery_generation
            wait_for_delivery_generation(self,callback)
            if self.stop.is_set():return self._stopped_before_start(selected,callback,operation)
            self._material_inputs=list(selected)
            if self.config.get('extraction_mode')=='paper-preextract/1.0':
                intake_last=[0.0,'']
                def intake_progress(phase,completed,total,record):
                    self._assert_task_admission()
                    now=time.monotonic()
                    if phase==intake_last[1] and completed!=total and now-intake_last[0]<.2:return
                    intake_last[:]=[now,phase]
                    callback(dict(run_id=None,status='preparing',processed_sources=0,source_count=len(selected),
                        intake_progress=dict(phase=phase,completed=completed,total=total),
                        progress=dict(stage='前置用途识别',file=Path(record.get('path','')).name,
                            message=f'{phase} {completed}/{total}；尚未进入 AI')))
                try:
                    self._material_context=MaterialContext(selected,progress=intake_progress,stop=self.stop)
                except InterruptedError:
                    if not self.stop.is_set():raise
                    return self._stopped_before_start(selected,callback,operation)
        if operation=='retry_ai':
            from ai_resume import validate_prepared
            for record in selected:
                if self.stop.is_set():break
                md_mode=(self.config.get('extraction_mode')=='paper-preextract/1.0' or
                    (record.get('prepared_checkpoint') or {}).get('format')=='paper-preextract-prepared/1')
                if md_mode and material_kind(record)!='document':continue
                try:validate_prepared(record,getattr(self,'_resume_root',self.store.root),VERSION)
                except ValueError as exc:raise ValueError(Path(record.get('path','')).name+'：'+str(exc)) from exc
        options=self._concurrency_options()
        if options.get('enabled') and int(options.get('papers',1))>1 and (len(selected)>1 or
                (self.config.get('extraction_mode')=='paper-preextract/1.0' and self.config.get('purpose_review',{}).get('enabled')
                 and not getattr(self,'_preserve_stop',False))):
            return self._run_parallel(selected,callback,operation)
        from processing_state import ProcessingState
        resume_cfg=self.config.get('resume') if isinstance(self.config.get('resume'),dict) else {}
        checkpoint=ProcessingState(self.store.root,resume_cfg.get('state_file','processing_state.json'),processing_version=VERSION) if resume_cfg.get('enabled',True) else None
        if options.get('enabled'):
            gate=options.get('_llm_gate') or _ObservedGate(options.get('global_llm',1))
            options={**options,'_llm_gate':gate}
            self.agent.llm_gate=options['_llm_gate']
        rid=datetime.now().strftime('AUTO_%Y%m%d_%H%M%S_')+uuid.uuid4().hex[:6]
        rid=self._allocate_task_run(rid,selected)
        self._active_run_id=rid
        run=self.store.runs/rid
        manifest={'schema_version':2,'pipeline':'automatic','run_id':rid,'operation':operation,'status':'running','source_count':len(selected),'processed_sources':0,
                  'started_at':datetime.now().isoformat(timespec='seconds'),'items':[],'events':[],'inputs':selected,'message':'开始自动预处理',
                  'retry_of':self.config.get('retry_of'), 'task_settings':copy.deepcopy(self.config.get('task_settings',{})),
                  'concurrency':{'enabled':bool(options.get('enabled')),'papers':int(options.get('papers',1)),
                                 'images_per_paper':int(options.get('images_per_paper',1)),
                                 'global_llm':int(options.get('global_llm',1))}}
        self.store.save(manifest)
        self._task_manifest_published(rid)
        manifest['billing']=summarize([])
        from model_activity import record_run_owner
        record_run_owner(self.store.root,rid)
        item=None
        def update_billing():
            manifest['billing']=self.agent.ledger.summary(run_id=rid)
            if item is not None: item['billing']=self.agent.ledger.summary(run_id=rid,item_id=item['id'])
            self.store.save(manifest)
            callback({**manifest,'billing_item':item.get('billing') if item else None})
        if isinstance(self.agent,AgentClient):
            self.agent.billing_context={'run_id':rid}
            self.agent.on_usage=update_billing
        known=self._known_sources()
        def emit(stage,message='',filename=''):
            self._emit(manifest,callback,stage,filename,message)
        emit('源文件','创建只读源文件的处理副本')
        queue=list(selected);index=0;checked_archives={};binding_paused=False
        try:
            while index<len(queue):
                if binding_paused or self.stop.is_set() or not self._global_open(): break
                record=queue[index];index+=1;source=Path(record['path'])
                item={'id':'AUTO_'+uuid.uuid4().hex[:20],'title':'处理未完成','major':'未识别专业','category':'未识别',
                      'source_path':str(source),'source_hash':'','status':'pending','reason':'','reviewed_hash':'','reviewed_package_hash':'','uploads':{},
                      'path':'','display_path':'','processing_version':VERSION,'academic_rating':None,'automatic_approval':False,'stages':[]}
                if record.get('input_source_path'):
                    item['input_source_path']=record['input_source_path']
                item.update(member_fields(record))
                item['billing']=summarize([])
                from processing_outcomes import PhaseTracker
                manifest['active_items']=[item]
                def notify_ai(active):
                    self.store.save(manifest)
                    callback({'run_id':rid,'status':manifest['status'],'message':'AI 处理状态更新',
                              'processed_sources':manifest['processed_sources'],'source_count':manifest['source_count'],
                              'ai_item':copy.deepcopy(active),
                              'progress':{'stage':'AI 处理','file':source.name,'source_path':str(source),
                                          'completed':manifest['processed_sources'],'total':manifest['source_count']}})
                tracker=PhaseTracker(item,notify_ai,self.store.lock)
                self.agent.on_ai_step=tracker.observe
                item_started=time.monotonic();self.agent.processing_metrics=[]
                if isinstance(self.agent,AgentClient): self.agent.billing_context={'run_id':rid,'item_id':item['id']}
                def stage(name,message=''):
                    item['stages'].append({'stage':name,'time':datetime.now().isoformat(timespec='seconds')})
                    emit(name,message,source.name)
                if isinstance(self.agent,AgentClient):
                    labels={'privacy_image':'图像识别与脱敏','privacy_text':'文本脱敏','repair':'局部内容恢复',
                            'extract_page':'页面识别','classify':'专业分类','screen':'文种判断'}
                    self.agent.on_cache_hit=lambda task:stage('续跑复用','已复用校验通过的'+labels.get(task,'模型')+'结果，未重复调用模型；继续核对原文证据')
                work=run/'private'/item['id'];work.mkdir(parents=True,exist_ok=True)
                try:
                    _validate_archive_member(record,checked_archives)
                    from preextract_contract import is_preextract_artifact
                    if is_preextract_artifact(source):
                        reason='已识别为预提取中间产物，跳过重复处理；原文件和已有 AI 回答保留'
                        item.update(status='reject',reason_kind='intermediate',business_decision='skip',reason=reason,
                            ai_result=dict(status='not_started',reason=reason,stage='输入识别'),
                            script_result=dict(status='completed',reason=reason))
                        stage('跳过中间产物',reason)
                        continue
                    item['source_hash']=digest(source)
                    md_mode=(self.config.get('extraction_mode')=='paper-preextract/1.0' or
                             (record.get('prepared_checkpoint') or {}).get('format')=='paper-preextract-prepared/1')
                    kind=material_kind(record)
                    role=None
                    if md_mode and (getattr(self,'_purpose_preclassified_only',False) or source.suffix.lower() in MATERIAL_DOCUMENT_FORMATS|IMAGE_SUFFIXES):
                        from material_roles import classify_document
                        if getattr(self,'_purpose_preclassified_only',False):
                            from purpose_review import preclassified_role
                            role=preclassified_role(record,self.config['purpose_review']) or dict(kind='unknown',action='pending',
                                confirmed=False,reason='前置用途回执缺失或源文件变化，保留原件等待分类')
                        else:
                            with self._global_control().stage('model',batch_id=rid,paper_id=item['id'],name=source.name):
                                role=classify_document(record,processor=self,work=work)
                        kind=role['kind'];item['intake_role']=role
                        if role['action']=='pending' and self.config.get('purpose_review',{}).get('enabled'):
                            body=bool(role.get('protected_body') or kind=='document')
                            note=work/'purpose-pending.md'
                            note.write_text(role['reason'],encoding='utf8')
                            item.update(status='pending',title='正文待复检' if body else '用途待确认',
                                content_format='paper_preextract' if body else 'purpose_pending',
                                reason=role['reason'],reason_kind='material_pending',business_decision='pending',
                                path=str(note),display_path=str(note),automatic_approval=False,
                                script_result=dict(status='pending',reason='用途尚未确认，原件保留'),
                                ai_result=dict(status='not_started',reason='用途复检未完成，尚未进入论文审核',stage=''))
                            stage('正文待复检' if body else '用途待确认',role['reason'])
                            continue
                    related_container=kind=='archive' and is_attachment_container(record,getattr(self,'_material_context',None))
                    if md_mode and (kind not in {'document','archive'} or related_container):
                        # Preserve each project relation, even if bytes match a
                        # source file in another project. These are never jobs
                        # for the paper model or six-slot delivery queue.
                        process_associated(self,source,record,work,item,stage,role=role)
                        continue
                    if self.config.get('extraction_mode')!='paper-preextract/1.0' and _is_uploaded_record(record,item['source_hash']):
                        manifest['events'].append({'status':'uploaded_skip','source':str(source),
                                                   **member_fields(record),
                                                   'input_source_path':record.get('input_source_path',str(source)),
                                                   'reason':'源文件已有已上传记录，本批跳过预处理，未调用 AI'})
                        continue
                    claim=getattr(self,'_claim_source',None)
                    if item['source_hash'] in known or (claim is not None and not claim(item['source_hash'])):
                        manifest['events'].append({'status':'duplicate','source':str(source),
                                                   **({'project_reference':duplicate_project_reference(record,item['source_hash'])} if md_mode and material_kind(record)=='document' else {}),
                                                   **member_fields(record),
                                                   'input_source_path':record.get('input_source_path',str(source)),
                                                   'reason':'已有相同源文件在其他批次完成或正在处理，本次跳过'})
                        continue
                    known.add(item['source_hash'])
                    if isinstance(self.agent,AgentClient):
                        self.agent.bind_stage_cache(self.stage_cache,item['source_hash'],VERSION)
                    if (self.config.get('extraction_mode')=='paper-preextract/1.0' or
                            (record.get('prepared_checkpoint') or {}).get('format')=='paper-preextract-prepared/1') and source.suffix.lower() not in {'.zip','.rar','.7z'}:
                        from preextract_workbench import process_item
                        process_item(self,source,record,work,run,item,tracker,stage)
                        known.add(item['source_hash'])
                        continue
                    from ai_resume import restore_prepared,save_prepared
                    if operation=='retry_ai':
                        original,package,prepared=restore_prepared(record,getattr(self,'_resume_root',self.store.root),VERSION,work)
                        raw=prepared['raw'];reliable=prepared['reliable'];screen=prepared['screen']
                        hint_title=prepared['hint_title'];hint_confidence=prepared['hint_confidence']
                        item.update(prepared['identity']);meta=prepared['meta'];meta['id']=item['id']
                        from evidence import VERSION as parser_version
                        if meta.get('parser_version')!=parser_version:
                            # Migrate only this private native preparation. Keep
                            # old checkpoints and all validated response caches.
                            identity={key:copy.deepcopy(meta[key]) for key in
                                      ('id','title','major','category','processing_version','original_source_sha256',
                                       'source_education','source_education_revision') if key in meta}
                            package=work/'package_parser_refreshed'
                            meta=build_package(original,package,identity,redact_content=False)
                            prepared['meta']=meta;prepared['_native_rebuilt']=True
                        # Native input remains cached; current admission/title rules
                        # must run before spending requests on a saved preparation.
                        if intake_enabled(self.config):
                            _,screen=self._material_screen(original,source,work,item,stage,raw=raw)
                        else:screen=coarse_screen(raw,reliable,source.name)
                        prepared['screen']=screen;item['screening']=screen
                        if screen['decision']=='reject':raise Rejected(screen['reason'],'screening')
                        hint_title=detect_title(raw,source.name)
                        if valid_title(hint_title):
                            item['title']=redactor()(hint_title);meta['title']=item['title']
                            item.setdefault('classification',{})['title']=item['title']
                            prepared['hint_title']=hint_title
                            prepared['identity']['title']=item['title']
                        previous_identity=copy.deepcopy(prepared['identity'])
                        selection=record if record.get('classification') else item
                        resolved=resolve_identity(raw,source_context(record,source),item['title'],
                                                  native_source=original,existing=selection)
                        apply_identity(item,resolved)
                        hint_confidence=item['classification']['confidence']
                        meta.update(major=item['major'],category=item['category'])
                        meta['classification']=copy.deepcopy(item['classification'])
                        prepared['hint_confidence']=hint_confidence
                        prepared['identity'].update({key:copy.deepcopy(item[key]) for key in
                                                     ('title','major','category','screening','classification')})
                        prepared['identity'].update({key:copy.deepcopy(item[key]) for key in
                            ('material_intake','material_intake_binding','material_processing') if key in item})
                        identity_changed=prepared['identity']!=previous_identity
                        source_refreshed=prepared.pop('_source_education_refreshed',False)
                        if prepared.pop('_native_rebuilt',False):
                            stage('脚本规则更新','从已校验原生文件重建正文与图表，替换旧清洗规则的中间结果')
                            screen=screen if intake_enabled(self.config) else coarse_screen(raw,reliable,source.name)
                            prepared['screen']=screen;item['screening']=screen
                            prepared['identity']['screening']=copy.deepcopy(screen)
                            if screen['decision']=='reject': raise Rejected(screen['reason'],'screening')
                            item['prepared_checkpoint']=save_prepared(work,original,package,item,prepared,VERSION)
                        elif identity_changed or source_refreshed:
                            item['prepared_checkpoint']=save_prepared(work,original,package,item,prepared,VERSION)
                        else:
                            item['prepared_checkpoint']=copy.deepcopy(record['prepared_checkpoint'])
                        item['previous_ai_reason']=record.get('previous_ai_reason','')
                        stage('AI 续跑','复用已保存的脚本结果，继续 AI 检查')
                    else:
                        original=work/('source'+source.suffix.lower());shutil.copy2(source,original)
                        if digest(original)!=item['source_hash']: raise AgentError('复制时源文件变化')
                        if original.suffix in {'.zip','.rar','.7z'}:
                            if md_mode or intake_enabled(self.config):
                                from material_intake import extract_material_archive
                                rows=extract_material_archive(original,work/'archive')
                            else:
                                if original.suffix=='.zip': validate_zip(original)
                                if str(LEGACY_ROOT) not in sys.path: sys.path.insert(0,str(LEGACY_ROOT))
                                from src.paper_cleaning.archive_extract import extract_archive
                                if original.suffix=='.rar':
                                    from archive_input import extract_rar
                                    rows=extract_rar(original,work,work/'archive')
                                else:rows=extract_archive(original,work,work/'archive')
                            valid=[r for r in rows if r['status']=='success']
                            if any(r['status'] not in {'success','archive_extracted'} for r in rows):
                                raise AgentError('压缩包存在无法解压的内容，未自动忽略')
                            if not valid:
                                raise MaterialPending('压缩包未取得可读成员，原件保留待解析') if md_mode or intake_enabled(self.config) else Rejected('压缩包中没有可处理的论文文件，原件保留','screening')
                            discovered=[]
                            for row in valid:
                                p=Path(row['extracted_path']).resolve()
                                if not p.is_relative_to(work.resolve()): raise AgentError('压缩包路径越界')
                                discovered.append({'path':str(p),'name':p.name,'format':p.suffix,'size':p.stat().st_size,
                                                   'input_source_path':record.get('input_source_path',str(source)),
                                                   'archive_member_path':'/'.join(v for v in (record.get('archive_member_path',''),p.relative_to((work/'archive').resolve()).as_posix()) if v),
                                                   'archive_source_hash':record.get('archive_source_hash',item['source_hash'])})
                            start=len(manifest.setdefault('discovered_inputs',[]))
                            manifest.setdefault('discovered_inputs',[]).extend(discovered)
                            manifest['source_count']=len(queue)+len(discovered)
                            self.store.save(manifest)
                            self._bind_discovered_task_sources(rid,manifest,discovered,start)
                            queue.extend(discovered)
                            if md_mode:
                                self._material_inputs=[*getattr(self,'_material_inputs',selected),*discovered]
                                self._material_context=MaterialContext(self._material_inputs)
                            continue
                        source_relation='original';derivation_method=None
                        if original.suffix in OFFICE_FORMATS:
                            stage('格式转换');original=convert_office(original,work/'converted')
                            source_relation='converted';derivation_method='convert_office'
                        elif original.suffix=='.docx':
                            from office_input import normalize_docx_container
                            original=normalize_docx_container(original,work/'converted_docx')
                            if digest(original)!=item['source_hash']:
                                source_relation='normalized';derivation_method='normalize_docx_container'
                        if intake_enabled(self.config):
                            material,screen=self._material_screen(original,source,work,item,stage)
                            raw=material['text'];reliable=material.get('reliable',True)
                            if original.suffix not in {'.docx','.pdf','.txt','.md'}:
                                from research_material import material_carrier
                                original=material_carrier(original,material,work)
                                source_relation='converted';derivation_method='research_material_carrier'
                        else:
                            stage('轻量解析与粗筛');raw,reliable=lightweight(original);screen=coarse_screen(raw,reliable,source.name)
                        from source_education import extract_source_education,source_education_fields
                        source_fields=source_education_fields(extract_source_education(original,original_source=source,
                            source_relation=source_relation,derivation_method=derivation_method,
                            derivation_record_id='native-preparation-'+item['id'] if source_relation!='original' else None))
                        item.update(copy.deepcopy(source_fields))
                        if checkpoint: checkpoint.paper_stage(item['id'],'screen')
                        hint_title=detect_title(raw,source.name)
                        item['title']=redactor()(hint_title) if valid_title(hint_title) else '未识别题名'
                        resolved=resolve_identity(raw,source_context(record,source),item['title'],
                                                  native_source=original,existing=record)
                        apply_identity(item,resolved)
                        item['screening']=screen
                        stage('初步专业分类',item['major']+'；依据：'+item['classification']['source'])
                        if checkpoint: checkpoint.paper_stage(item['id'],'classify')
                        if screen['decision']=='reject': raise Rejected(screen['reason'],'screening')
                        stage('图表提取','原生图片与表格先提取，随后再脱敏')
                        package=work/'package'
                        identity={'id':item['id'],'title':item['title'],'category':item['category'],'major':item['major'],
                                  'processing_version':VERSION,'original_source_sha256':item['source_hash'],
                                  **copy.deepcopy(source_fields)}
                        try:
                            meta=build_package(original,package,identity,redact_content=False)
                        except zipfile.BadZipFile:
                            if original.suffix.lower()!='.docx':raise
                            from office_integrity import recover_zero_crc_media
                            recovered=work/'source_crc_recovered.docx'
                            members=recover_zero_crc_media(original,recovered)
                            if not members:raise
                            item['source_recovery']={'kind':'validated_zero_media_crc','members':members,
                                                     'source_unchanged':True}
                            # Keep the failed partial package privately for diagnosis.
                            original=recovered;package=work/'package_recovered'
                            recovery_method=(derivation_method+'+' if derivation_method else '')+'recover_zero_crc_media'
                            source_fields=source_education_fields(extract_source_education(original,original_source=source,
                                source_relation=source_relation if source_relation!='original' else 'normalized',
                                derivation_method=recovery_method,
                                derivation_record_id='native-preparation-'+item['id']))
                            item.update(copy.deepcopy(source_fields));identity.update(copy.deepcopy(source_fields))
                            meta=build_package(original,package,identity,redact_content=False)
                        hint_confidence=item['classification']['confidence']
                        prepared={'raw':raw,'reliable':reliable,'screen':screen,'hint_title':hint_title,
                                  'hint_confidence':hint_confidence,'meta':meta,
                                  'identity':{key:copy.deepcopy(item[key]) for key in
                                              ('title','major','category','screening','classification',
                                               'source_education','source_education_revision')}}
                        prepared['identity'].update({key:copy.deepcopy(item[key]) for key in
                            ('material_intake','material_intake_binding','material_processing') if key in item})
                        item['prepared_checkpoint']=save_prepared(work,original,package,item,prepared,VERSION)
                    self.store.save(manifest)
                    meta['_defer_text_privacy']=True
                    from knowledge_package import paper_identity
                    paper_id=paper_identity(meta)[1]
                    meta=finish_evidence(meta,package,self.agent,original,work/'visual',int(self.config.get('max_pages',80)),stage,
                                         options,checkpoint,paper_id)
                    if checkpoint: checkpoint.paper_stage(item['id'],'parse')
                    from evidence_tasks import visible_text
                    body='\n'.join([b.get('text','') for b in meta['blocks'] if b['type']=='paragraph']+
                                   [c['text'] for b in meta['blocks'] if b['type']=='table' for c in b['cells']]+
                                   [b.get('ocr',{}).get('text','') for b in meta['blocks'] if b['type']=='image'])
                    final_screen=screen if intake_enabled(self.config) else coarse_screen(body,True)
                    if final_screen['decision']=='reject':
                        if not reliable:
                            raise Rejected('原文文字层不完整，恢复后仍不足以生成完整入库正文；'+final_screen['reason'],'evidence')
                        raise Rejected('清洗后'+final_screen['reason'],'screening')
                    if not intake_enabled(self.config) and (screen['decision']=='agent' or final_screen['decision']=='agent'):
                        stage('AI 基础文种判断','仅判断文种，复用已经提取的正文')
                        screened=ask(self.agent,'screen',{'text':body[:7000],'ending':body[-1500:],
                            'full_document_chars':final_screen['chars'],'rules':final_screen,'scope':'文种判断用片段，不是全文长度'})
                        if isinstance(self.agent,AgentClient):self.agent.accept_cached_result(screened)
                    stage('按专业分类整理')
                    taxonomy=load_legacy_taxonomy();directions={major:group for group,majors in taxonomy.items() for major in majors}
                    if self.config.get('research_profile',{}).get('enabled'):
                        from specialty_routing import load_catalog
                        for professional in load_catalog(Path(__file__).with_name('specialty_catalog.json'))['majors']:
                            directions.setdefault(professional['name'],professional['discipline'])
                    if item['major'] in directions and (hint_confidence in {'high','medium'} or item['classification'].get('locked')) and valid_title(item['title']):
                        classified={'major':item['major'],'title':item['title']}
                    else:
                        stage('AI 补充初步分类','仅补充未明确的专业或题名')
                        locked_major=item['major'] if item['classification'].get('locked') else None
                        classified=ask(self.agent,'classify',{'text':visible_text(meta)[:10000],'ending':body[-1000:],'majors':[locked_major] if locked_major else list(directions),
                            'material_intake':item.get('material_intake'),
                            'cover_title_context':redactor()(raw[:2500]),
                            'original_rule_major':item['major'],'original_rule_title':redactor()(hint_title),
                            'classification_rules_version':CLASSIFICATION_RULES_VERSION,
                            'scope':'初步分类用题名、开头与结尾片段，不评价学术质量'})
                        if locked_major:
                            if classified['major']!=locked_major:raise Rejected('补充题名不能变更原文或手动已确认的专业','classification')
                            item['classification']['title_method']='ai_fallback'
                        else:
                            item['classification']={'method':'ai_fallback','source':'content','confidence':'model_decision',
                                                    'major':classified['major'],'title':classified['title'],
                                                    'rules_version':CLASSIFICATION_RULES_VERSION}
                    if classified['major'] not in directions or not valid_title(classified['title']): raise Rejected('专业或题名无法可靠确定','classification')
                    from auto_evidence import text_privacy
                    meta.update(title=classified['title'],major=classified['major'],category=directions[classified['major']])
                    text_privacy(meta,self.agent,stage)
                    if not valid_title(meta['title']): raise Rejected('脱敏后的题名不符合入库要求','classification')
                    if isinstance(self.agent,AgentClient):self.agent.accept_cached_result(classified)
                    tracker.ai_complete()
                    item['classification'].update(title=meta['title'],major=meta['major'])
                    meta['classification']=item['classification'];meta['processing_metrics']=list(self.agent.processing_metrics)
                    stage('生成论文知识包','整理小节、原图、表格数据与绘图结构，不新增 AI 扫描')
                    if self.stop.is_set():raise AgentUnavailable('已停止，当前文件保留待重试')
                    export_package(package,meta,canonical_paper_id=True)
                    profile_result=None;profile_error=''
                    if self.config.get('research_profile',{}).get('enabled'):
                        from research_profile import ProfileProcessor,attach_profile
                        try:
                            if item.get('material_processing')=='research_material':
                                from research_material import process_material
                                profile_result=process_material(package,self.agent,self.store.root,stage)
                                item['research_material']=profile_result
                            else:
                                profile_result=ProfileProcessor(self.agent,self.store.root,progress=stage).process(package)
                                attach_profile(package,profile_result)
                        except Exception as exc:
                            if self.stop.is_set():raise
                            # Preserve all already-approved preprocessing. Only this new stage retries.
                            profile_error=type(exc).__name__+':'+str(exc)[:160] if str(exc).startswith(('profile_','profile_contract:')) else type(exc).__name__
                            stage('画像待补','预处理成果已保留；新增画像阶段待补，不重新解析原件')
                    final=run/'packages'/item['id'];final.parent.mkdir(exist_ok=True)
                    from knowledge_package import publish_knowledge
                    if self.stop.is_set():raise AgentUnavailable('已停止，当前文件保留待重试')
                    publish_knowledge(package,final)
                    item.update(title=meta['title'],major=meta['major'],category=meta['category'],status='pass',reason='基础筛选、初步分类、必要内容提取和自动脱敏通过',reason_kind='',
                                package_path=str(final),path=str(final/'ingest.md'),display_path=str(final/'document.md'),content_format='evidence_package',
                                knowledge_manifest_path=str(final/'manifest.json'),paper_id=meta['knowledge_package']['paper_id'],
                                reviewed_hash=digest(final/'ingest.md'),reviewed_package_hash=package_fingerprint(final),automatic_approval=True,
                                review_status='ai_passed',redaction_status='ai_passed',evaluation_ready=False)
                    if self.config.get('research_profile',{}).get('enabled'):
                        item.update(profile_status='ready' if profile_result else 'pending',profile_error=profile_error,
                            profile_admission=profile_result['admission']['status'] if profile_result else 'pending',
                            profile_completeness=profile_result['completeness']['status'] if profile_result else 'partial',
                            evaluation_ready=bool(profile_result and profile_result['admission']['status'] in ('recommended','limited')))
                    known.add(item['source_hash']);stage('通过','预处理通过，等待 RAGFlow 接收')
                    tracker.finish('passed')
                except __import__('global_task_control').GlobalPaused as exc:
                    batch_paused=getattr(exc,'reason_kind',None)=='batch_paused'
                    if batch_paused:
                        binding_paused=True
                        manifest['task_binding_attention']=dict(state='pending',reason_kind='batch_paused',reason=str(exc))
                    item.update(status='pending',reason='本任务暂停；已保存结果，等待继续' if batch_paused else '全局暂停；已保存结果，等待继续',
                        reason_kind='batch_paused' if batch_paused else 'global_paused',business_decision='pending')
                    dest=run/'pending'/(item['id']+'.md');dest.parent.mkdir(exist_ok=True)
                    dest.write_text('# 等待继续\n\n'+item['reason'],encoding='utf8')
                    item.update(path=str(dest),display_path=str(dest))
                except MaterialPending as exc:
                    item.update(status='pending',reason=redactor()(str(exc)),reason_kind='material_pending',business_decision='pending')
                    tracker.finish('pending',item['reason'])
                    dest=run/'pending'/(item['id']+'.md');dest.parent.mkdir(exist_ok=True)
                    dest.write_text('# 待解析\n\n'+item['reason']+'\n\n原文件保留，未判定为无学习价值。',encoding='utf8')
                    item.update(path=str(dest),display_path=str(dest));stage('待解析',item['reason'])
                except Rejected as exc:
                    item.update(status='reject',reason=redactor()(str(exc)),business_decision='reject',reason_kind=exc.kind,failure_position=exc.position)
                    tracker.finish('rejected',item['reason'])
                    if not valid_title(item['title']): item['title']='未通过文件'
                    dest=run/'rejected'/(item['id']+'.md');dest.parent.mkdir(exist_ok=True)
                    dest.write_text('# 不通过\n\n类型：'+item['reason_kind']+'\n\n'+item['reason']+'\n\n原文件保留。',encoding='utf-8')
                    item.update(path=str(dest),display_path=str(dest))
                    if item['script_result']['status']=='completed':
                        stage('脚本执行完成','AI 处理已形成结论，详见“AI 处理结果”')
                    else:
                        stage('不通过',item['reason'])
                except Exception as exc:
                    # No raw server response, model text, API key, or private document snippets in logs.
                    from processing_errors import describe_failure
                    failure=describe_failure(exc)
                    message=failure['message']
                    item.update(failure_code=failure['code'],failure_stage=item['stages'][-1]['stage'] if item['stages'] else '准备文件')
                    if failure.get('position'):item['failure_position']=failure['position']
                    kind='stopped' if self.stop.is_set() else 'technical'
                    item.update(status='reject',reason=message,reason_kind=kind,business_decision='reject')
                    tracker.finish('error',message,stopped=self.stop.is_set())
                    dest=run/'rejected'/(item['id']+'.md');dest.parent.mkdir(exist_ok=True)
                    dest.write_text('# 不通过\n\n类型：'+('用户停止' if kind=='stopped' else '技术处理失败')+'\n\n'+message+'\n\n原文件保留。',encoding='utf-8')
                    item.update(path=str(dest),display_path=str(dest))
                    if item.get('content_format')=='paper_preextract' and item['script_result']['status']=='completed':
                        stage('脚本执行完成','全文与图表材料已保留；AI 问题见“AI 处理结果”')
                    else:stage('不通过',message)
                finally:
                    if isinstance(self.agent,AgentClient):self.agent.discard_unaccepted_results()
                    item['processing_metrics']=list(getattr(self.agent,'processing_metrics',[]))
                    item['elapsed_seconds']=round(time.monotonic()-item_started,3)
                    manifest['active_items']=[]
                    if item['status'] in {'pass','reject'} or item['reason']: manifest['items'].append(item)
                    manifest['processed_sources']=index;self.store.save(manifest)
                    global_phases=getattr(self,'_global_stage_sequence',None)
                    if global_phases is not None:
                        global_phases.finish('failed' if item.get('reason_kind')=='technical' else 'complete',item.get('failure_code',''))
                        self._global_stage_sequence=None
                    self._enqueue_approved_after_save(rid,[item],emit)
                    if self.config.get('work_pool_enabled'):
                        manifest=self.store.load(rid)
                    callback(manifest.copy())
            manifest['status']='stopped' if binding_paused or self.stop.is_set() or not self._global_open() else 'completed'
            duplicate_count=sum(e.get('status') == 'duplicate' for e in manifest.get('events', []))
            resolve_duplicate_projects(manifest)
            from preextract_task_storage import mark_production
            mark_production(manifest,index==len(queue) and not binding_paused and not self.stop.is_set() and self._global_open())
            manifest['message']=f"通过 {sum(i['status']=='pass' for i in manifest['items'])}，不通过 {sum(i['status']=='reject' for i in manifest['items'])}"
            for code,label in [('associated','关联资料'),('pending','待解析'),('deleted','已删除')]:
                count=sum(i.get('status')==code for i in manifest['items'])
                if count:manifest['message']+=f'，{label} {count}'
            if duplicate_count:
                manifest['message'] += f'，重复跳过 {duplicate_count}'
            uploaded_skip_count=sum(e.get('status')=='uploaded_skip' for e in manifest.get('events', []))
            if uploaded_skip_count:
                manifest['message'] += f'，已上传跳过 {uploaded_skip_count}'
            if options.get('enabled'):
                manifest['concurrency']['llm_peak']=options['_llm_gate'].peak
        except Exception as exc:
            manifest['status']='stopped' if self.stop.is_set() else 'failed'
            message='本地结果保存或任务收尾失败：'+type(exc).__name__+'；已保存处理进度，可继续处理'
            manifest['message']=message
            if item is not None and item.get('status')!='pass':
                item.update(status='reject',reason_kind='stopped' if self.stop.is_set() else 'technical',
                            reason=message,automatic_approval=False)
                tracker.finish('error',message,stopped=self.stop.is_set())
            raise
        finally:
            manifest['finished_at']=datetime.now().isoformat(timespec='seconds');self.store.save(manifest)
            if manifest.get('status')=='completed' and self.rag_client and self.config.get('auto_upload',True) and not self.stop.is_set():
                from auto_ragflow import handoff
                self._task_handoff(rid,None,lambda:handoff(self.store,rid,self.rag_client,self.config.get('dataset_map',{}),emit,
                        self.config.get('auto_parse',True),self.config.get('legacy_dataset_map_fallback',True),options,
                        target_datasets=self.config.get('task_datasets'),upload_config=dict(self.config,_stop_event=self.stop)))
                manifest=self.store.load(rid)
                if self.stop.is_set():
                    manifest.update(status='stopped',message='已停止交付，已有处理结果和回执保留')
                    self.store.save(manifest)
            self.store.refresh_view(rid);manifest=self.store.load(rid);emit('任务结束',manifest['message'])
        return manifest
