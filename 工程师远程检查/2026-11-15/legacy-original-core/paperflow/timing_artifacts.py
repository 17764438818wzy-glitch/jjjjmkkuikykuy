"""Private immutable measurement files; never a source of business authority."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from .instrumentation import compact_stage_timing
from .package import save,sha


SCHEMA_VERSION=1
# Explicit read/write observation bound, independent of the unchanged IPC cap.
MAX_DETAIL_BYTES=16*1024*1024
REFERENCE=re.compile(r'private/timing/([0-9a-f]{32})/([0-9a-f]{32})\.json\Z')
BINDING_KEYS=('epoch','execution_id','attempt_id','job_id','unit_key','task_id','execution_kind')
SUMMARY_KEYS=('clock_id','epoch','elapsed_seconds','metrics_complete','stage_visit_count','graph_elapsed_seconds',
    'five_stage_seconds','five_stage_elapsed_seconds','five_stage_elapsed_complete','span_overflow',
    'span_limit','recorded_span_count','skipped_span_count')


def _hex(value,length):return isinstance(value,str) and re.fullmatch('[0-9a-f]{'+str(length)+'}',value) is not None


def configure_timing(client,root,task_id,**extra_identity):
    """Configure from trusted worker arguments + manager launch metadata.

    Setup failure is an observation failure, never a worker startup failure.
    Unscheduled runs are identified explicitly rather than inventing an attempt.
    """
    try:
        supplied=os.environ.get('PAPERFLOW_TIMING_CONTEXT')
        context=json.loads(supplied) if supplied else dict(schema_version=SCHEMA_VERSION,
            epoch=client.epoch,execution_id=client.execution_id,attempt_id=None,job_id=None,
            unit_key=task_id,execution_kind=extra_identity.get('execution_kind','graph'))
        if not isinstance(context,dict) or context.get('schema_version')!=SCHEMA_VERSION:raise ValueError('schema')
        if context.get('epoch')!=client.epoch or context.get('execution_id')!=client.execution_id:raise ValueError('binding')
        if not _hex(client.execution_id,32):raise ValueError('execution')
        if not isinstance(task_id,str) or not re.fullmatch('[A-Za-z0-9_-]{1,128}',task_id):raise ValueError('task')
        if not isinstance(context.get('unit_key'),str) or not 0<len(context['unit_key'])<=2048:raise ValueError('unit')
        if context.get('execution_kind') not in ('graph','delivery','cleanup'):raise ValueError('kind')
        if context.get('attempt_id') is not None and not _hex(context['attempt_id'],32):raise ValueError('attempt')
        if context.get('job_id') is not None and not _hex(context['job_id'],64):raise ValueError('job')
        if (context.get('attempt_id') is None)!=(context.get('job_id') is None):raise ValueError('attempt job')
        if context.get('task_id') not in (None,task_id):raise ValueError('task binding')
        identity={key:context.get(key) for key in BINDING_KEYS};identity['task_id']=task_id
        identity['attempt_binding']='scheduled' if identity['attempt_id'] is not None else 'unscheduled'
        for key in ('source_id','source_sha256','manifest_sha256','specialty_code','repair_request_id'):
            value=extra_identity.get(key)
            if value is not None:
                if not isinstance(value,str) or not 0<len(value)<=256:raise ValueError('identity')
                if key.endswith('sha256') and not _hex(value,64):raise ValueError('digest')
                identity[key]=value
        client.timing_context=dict(root=Path(root).resolve(),identity=identity)
        return True
    except Exception:
        client.timing_context=dict(error='invalid_timing_context')
        return False


def _safe_path(root,relative):
    match=REFERENCE.fullmatch(relative) if isinstance(relative,str) else None
    if match is None:raise ValueError('reference path')
    root=Path(root).resolve();path=root/relative
    cursor=root
    for component in Path(relative).parts:
        cursor=cursor/component
        try:info=cursor.lstat()
        except FileNotFoundError:continue
        if stat.S_ISLNK(info.st_mode) or getattr(info,'st_file_attributes',0)&getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',0x400):
            raise ValueError('reference link')
        if getattr(cursor,'is_junction',lambda:False)():raise ValueError('reference junction')
    if not path.resolve().is_relative_to((root/'private/timing').resolve()):raise ValueError('reference containment')
    return path


def _summary(timing):
    return {key:timing[key] for key in SUMMARY_KEYS if key in timing} if isinstance(timing,dict) else dict(metrics_complete=False)


def execution_observation(client,outcome,data,timing):
    """Return small summary+reference, or None for an unconfigured legacy client.

    Entire new I/O/serialization path is best effort. Replays reuse the exact
    projection, preserving manager's original execution-result replay contract.
    """
    if not hasattr(client,'timing_context'):return None
    summary=_summary(timing)
    try:
        context=client.timing_context
        if context.get('error'):raise ValueError('timing context')
        replay_key=json.dumps(dict(outcome=outcome,data=data),ensure_ascii=False,sort_keys=True,allow_nan=False,separators=(',',':'))
        cached=getattr(client,'_timing_artifact_report',None)
        if cached is not None and cached['key']==replay_key:return dict(cached['summary'])
        started=time.perf_counter()
        detail=dict(timing) if isinstance(timing,dict) else dict(metrics_complete=False)
        detail.setdefault('stages',[])
        # Even a bounded aggregate overflow must retain the actual stage visits
        # with their own incomplete leaves; storage cannot repair missing spans.
        metrics=getattr(client,'metrics',None)
        stages=tuple(getattr(metrics,'stages',()))
        detail['polling']=list(getattr(metrics,'polls',()))
        detail['remote_observations']=list(getattr(metrics,'observations',()))
        if stages:
            detail['stages']=[compact_stage_timing(stage) for stage in stages]
            detail['stage_visit_count']=len(stages)
        complete=detail.get('metrics_complete') is True and all(stage.get('metrics_complete') is True
            for stage in detail.get('stages',()))
        detail['metrics_complete']=complete
        envelope=dict(schema_version=SCHEMA_VERSION,identity=context['identity'],outcome=outcome,
            coverage='execution_terminal',metrics_complete=complete,timing=detail)
        failed=data.get('_failed_stage_timing')
        if isinstance(failed,dict):
            envelope['failed_stage_timing']=failed
            if failed.get('metrics_complete') is not True:envelope['metrics_complete']=detail['metrics_complete']=False
        # Validate finite JSON and upper bound before the atomic save. save uses
        # this same encoding shape; the post-write size/hash checks verify bytes.
        raw=json.dumps(envelope,ensure_ascii=False,allow_nan=False,indent=2).replace('\n',os.linesep).encode('utf-8')
        if len(raw)>MAX_DETAIL_BYTES:raise ValueError('timing artifact exceeds observation bound')
        relative='private/timing/'+client.execution_id+'/'+uuid.uuid4().hex+'.json'
        path=_safe_path(context['root'],relative)
        path.parent.mkdir(parents=True,exist_ok=True);_safe_path(context['root'],relative)
        save(path,envelope)
        _safe_path(context['root'],relative)
        size=path.stat().st_size;digest=sha(path)
        if size!=len(raw) or digest!=hashlib.sha256(raw).hexdigest():raise ValueError('artifact changed during write')
        reference=dict(schema_version=SCHEMA_VERSION,relative_path=relative,sha256=digest,bytes=size,**context['identity'])
        summary.update(metrics_complete=envelope['metrics_complete'],stage_visit_count=detail.get('stage_visit_count',0),
            details_status='available',details_ref=reference,artifact_write_seconds=time.perf_counter()-started)
    except Exception as exc:
        summary.update(metrics_complete=False,details_status='write_failed',detail_error=type(exc).__name__)
    # Cache failures too: a repeated lost-ack report remains byte-for-byte equal.
    try:client._timing_artifact_report=dict(key=replay_key,summary=dict(summary))
    except Exception:pass
    return summary


def read_details(root,summary,expected):
    """Read one byte buffer, validate hash+identity, then parse the same bytes.

    A failed observation is returned as data; callers keep the attempt/sample.
    """
    def failed(status):return dict(detail_status=status,metrics_complete=False,timing=None,failed_stage_timing=None)
    reference=summary.get('details_ref') if isinstance(summary,dict) else None
    if not isinstance(reference,dict):return failed('reference_missing')
    try:
        if reference.get('schema_version')!=SCHEMA_VERSION:return failed('invalid_reference')
        if not _hex(reference.get('sha256'),64) or type(reference.get('bytes')) is not int:return failed('invalid_reference')
        if not 0<reference['bytes']<=MAX_DETAIL_BYTES:return failed('invalid_reference')
        if any(key not in expected or reference.get(key)!=expected[key] for key in BINDING_KEYS):return failed('identity_mismatch')
        if any(reference.get(key)!=value for key,value in expected.items() if key!='schema_version'):return failed('identity_mismatch')
        match=REFERENCE.fullmatch(reference.get('relative_path',''))
        if match is None:return failed('invalid_reference')
        if match[1]!=expected['execution_id']:return failed('identity_mismatch')
        try:path=_safe_path(root,reference['relative_path'])
        except ValueError:return failed('invalid_reference')
        try:
            from .private_files import read_owned_bytes
            raw=read_owned_bytes(root,path,reference['bytes'])
        except FileNotFoundError:return failed('missing')
        if len(raw)!=reference['bytes']:return failed('length_mismatch')
        if hashlib.sha256(raw).hexdigest()!=reference['sha256']:return failed('hash_mismatch')
        envelope=json.loads(raw.decode('utf-8'),parse_constant=lambda value:(_ for _ in ()).throw(ValueError('nonfinite JSON')))
        if not isinstance(envelope,dict) or envelope.get('schema_version')!=SCHEMA_VERSION:return failed('invalid_file')
        identity=envelope.get('identity')
        if not isinstance(identity,dict) or any(identity.get(key)!=expected[key] for key in BINDING_KEYS):return failed('identity_mismatch')
        if any(identity.get(key)!=value for key,value in reference.items() if key in identity):return failed('identity_mismatch')
        timing=envelope.get('timing')
        if not isinstance(timing,dict) or timing.get('epoch')!=expected['epoch']:return failed('identity_mismatch')
        stages=timing.get('stages',())
        if not isinstance(stages,list):return failed('invalid_file')
        complete=summary.get('metrics_complete') is True and envelope.get('metrics_complete') is True and timing.get('metrics_complete') is True
        complete=complete and all(isinstance(stage,dict) and stage.get('metrics_complete') is True for stage in stages)
        failed_stage=envelope.get('failed_stage_timing')
        if failed_stage is not None and (not isinstance(failed_stage,dict) or failed_stage.get('metrics_complete') is not True):complete=False
        return dict(detail_status='available',metrics_complete=complete,timing=dict(timing,metrics_complete=complete),
            failed_stage_timing=failed_stage)
    except (OSError,ValueError,TypeError,KeyError,UnicodeError):return failed('invalid_file')
