from pathlib import Path
import time
import traceback


def is_infrastructure(exc):
    status=getattr(exc,'status_code',getattr(exc,'status',getattr(getattr(exc,'response',None),'status_code',None)))
    return (isinstance(exc,(ConnectionError,TimeoutError)) or status==429 or
            isinstance(status,int) and 500<=status<600 or
            type(exc).__name__ in ('RagflowConnectionError','RagflowTimeoutError'))


def controller_fields(config,task,stage,exc):
    trace=traceback.TracebackException.from_exception(exc)
    frame=trace.stack[-1] if trace.stack else None
    location=(Path(frame.filename).name+':'+str(frame.lineno)) if frame else stage
    log=config.root/'private/errors'/task['task_id']/f'{time.time_ns()}.log'
    log.parent.mkdir(parents=True,exist_ok=True)
    log.write_text(''.join(trace.format()),encoding='utf-8')
    return dict(stage='code_error',error_kind='controller',error=type(exc).__name__+'@'+location,
                traceback_private=str(log),resume_stage=stage,owner=None,lease_until=0,due=0)
