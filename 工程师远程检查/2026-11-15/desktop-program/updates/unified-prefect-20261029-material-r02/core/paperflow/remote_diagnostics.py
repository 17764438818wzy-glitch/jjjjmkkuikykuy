"""Bounded control-plane evidence; never serialize commands, payloads or stderr."""
from contextlib import contextmanager
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
import sqlite3
import subprocess
import traceback
import time

PHASES={'ledger_read','owner_validate','remote_request','proof_validate','proof_persist',
    'job_metadata','job_lock','container_inspect','container_remove','exit_confirm','credential_cleanup',
    'execution_metadata','execution_inventory','startup_queue','control_queue','transfer_queue','container_observe','container_create','container_start','ssh_queue','ssh_auth','request_upload','request_response',
    'request_download','request_exit'}
OPERATIONS={'prepare','run','update','collect','health','status_job','reap_job','reap_execution'}
REASONS={'database_busy','database_error','timeout','ssh_queue_timeout','ssh_auth_timeout',
    'resource_queue_busy',
    'removal_in_progress','container_absent','docker_unavailable','permission_denied','command_failed',
    'path_absent','invalid_value','identity_mismatch','creation_unconfirmed','container_still_present',
    'remote_connection_error','connection_refused','connection_reset','host_key_failed','authentication_failed',
    'os_error','unexpected_error'}


class RemoteQueueBusy(TimeoutError):
    """A known resource wait ended before that operation was dispatched."""


def safe_context(value):
    result={}
    if not isinstance(value,dict):return result
    for key,item in value.items():
        if key=='phase' and isinstance(item,str) and item in PHASES:result[key]=item
        elif key=='operation' and isinstance(item,str) and item in OPERATIONS:result[key]=item
        elif key in ('request_id','job_id','execution_id','epoch') and isinstance(item,str) and re.fullmatch('[0-9a-f]{32}',item):result[key]=item
        elif key in ('elapsed_seconds','timeout_seconds','queue_seconds','auth_seconds','operation_seconds') and type(item) in (int,float) and 0<=item<=1e9:result[key]=item
    return result


def ssh_detail(error,returncode):
    text=error.decode(errors='replace').lower() if isinstance(error,bytes) else str(error).lower()
    reason=('host_key_failed' if 'host key verification failed' in text or 'host identification has changed' in text else
        'authentication_failed' if 'permission denied' in text else 'connection_refused' if 'connection refused' in text else
        'connection_reset' if 'connection reset' in text or 'connection closed' in text else 'remote_connection_error')
    return dict(type='SSHFailure',reason=reason,returncode=returncode)


@contextmanager
def stage(phase,**context):
    try:yield
    except Exception as exc:
        previous=getattr(exc,'remote_context',{})
        exc.remote_context=safe_context({'phase':phase,**context,**previous})
        raise


def safe_detail(value,depth=2):
    if not isinstance(value,dict):return {}
    result={}
    if isinstance(value.get('type'),str) and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,79}',value['type']):result['type']=value['type']
    if isinstance(value.get('reason'),str) and value['reason'] in REASONS:result['reason']=value['reason']
    for key in ('errno','returncode','sqlite_errorcode'):
        if type(value.get(key)) is int:result[key]=value[key]
    if value.get('diagnostic_write_failed') is True:result['diagnostic_write_failed']=True
    if isinstance(value.get('docker_operation'),str) and value['docker_operation'] in ('inspect','rm','ps','create','start'):result['docker_operation']=value['docker_operation']
    result['context']=safe_context(value.get('context'))
    if depth and isinstance(value.get('remote'),dict):result['remote']=safe_detail(value['remote'],depth-1)
    if isinstance(value.get('frames'),list):
        result['frames']=[dict(file=f['file'],line=f['line'],function=f['function']) for f in value['frames'][:8]
            if isinstance(f,dict) and re.fullmatch(r'[A-Za-z0-9_.-]{1,80}\.py',str(f.get('file','')))
            and type(f.get('line')) is int and 0<f['line']<1e6 and re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,79}',str(f.get('function','')))]
    return result


def describe(exc):
    reason='unexpected_error';message=str(exc)
    if isinstance(exc,RemoteQueueBusy):reason='resource_queue_busy'
    elif isinstance(exc,sqlite3.Error):reason='database_busy' if (getattr(exc,'sqlite_errorcode',0)&255)==sqlite3.SQLITE_BUSY else 'database_error'
    elif isinstance(exc,(TimeoutError,subprocess.TimeoutExpired)):
        reason='ssh_queue_timeout' if 'connection queue timed out' in message else 'ssh_auth_timeout' if 'authentication timed out' in message else 'timeout'
    elif isinstance(exc,subprocess.CalledProcessError):
        error=exc.stderr.decode(errors='replace') if isinstance(exc.stderr,bytes) else str(exc.stderr or '')
        reason=('removal_in_progress' if 'already in progress' in error else 'container_absent' if 'no such container' in error.lower() else
            'docker_unavailable' if 'connect to the docker daemon' in error.lower() else 'command_failed')
    elif isinstance(exc,FileNotFoundError):reason='path_absent'
    elif isinstance(exc,PermissionError):reason='permission_denied'
    elif isinstance(exc,ConnectionError):reason=('creation_unconfirmed' if message=='remote container creation is not confirmed complete'
        else 'container_still_present' if message in ('remote process space exit unconfirmed','failed creation conflicts with actual container') else 'remote_connection_error')
    elif isinstance(exc,ValueError):reason='identity_mismatch' if message in ('remote execution identity mismatch','remote container ownership mismatch','remote endpoint or image changed; original owner must be reconciled') else 'invalid_value'
    elif isinstance(exc,OSError):reason='os_error'
    result=safe_detail(dict(type=type(exc).__name__,reason=reason,errno=getattr(exc,'errno',None),
        returncode=getattr(exc,'returncode',None),sqlite_errorcode=getattr(exc,'sqlite_errorcode',None),
        context=getattr(exc,'remote_context',{})))
    command=getattr(exc,'cmd',None)
    if isinstance(command,(list,tuple)) and len(command)>1 and command[0]=='docker' and command[1] in ('inspect','rm','ps','create','start'):
        result['docker_operation']=command[1]
    result['frames']=[dict(file=Path(f.filename).name,line=f.lineno,function=f.name) for f in traceback.extract_tb(exc.__traceback__)[-8:]]
    remote=safe_detail(getattr(exc,'remote_error',None))
    if remote:result['remote']=remote
    return result


class ExitLog:
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.handler=RotatingFileHandler(self.path,maxBytes=512*1024,backupCount=2,encoding='utf-8',delay=True)
        self.handler.setFormatter(logging.Formatter('%(message)s'))
        self.closed=False;self.error=None
        self.handler.handleError=lambda record:setattr(self,'error','diagnostic_write_failed')
    def record(self,value):
        self.handler.acquire()
        try:
            if not self.closed:self.handler.handle(logging.LogRecord('paperflow.remote_exit',logging.ERROR,'',0,json.dumps(value,ensure_ascii=False),(),None))
        except Exception:self.error='diagnostic_write_failed'
        finally:self.handler.release()
    def close(self):
        self.handler.acquire()
        try:self.closed=True;self.handler.close()
        finally:self.handler.release()


def record_control_failure(root,phase,exc,*,epoch=None):
    """Reuse bounded, redacted diagnostics without changing control decisions."""
    log=None
    try:
        log=ExitLog(Path(root)/'control-errors.jsonl')
        log.record(dict(at=time.time(),phase=phase,epoch=epoch,error=describe(exc)))
    except Exception:pass  # A failed diagnostic must never replace the original failure.
    finally:
        if log:
            try:log.close()
            except Exception:pass
