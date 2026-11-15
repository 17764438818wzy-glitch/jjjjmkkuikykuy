"""Small, non-secret failure facts and the existing bounded retry policy."""
from email.utils import parsedate_to_datetime
import math
import random
import time
import ssl
from urllib.error import URLError,HTTPError
from http.client import HTTPException


class ActionDeferred(ValueError):
    def __init__(self,reason):
        self.reason=reason
        super().__init__(reason)


class ServiceConfigurationError(RuntimeError):
    pass


def retry_after(value,*,now=None):
    now=time.time() if now is None else now
    try:
        seconds=float(value)
    except (TypeError,ValueError):
        try:seconds=parsedate_to_datetime(str(value)).timestamp()-now
        except (ValueError,TypeError,OverflowError):return 0
    return max(0,seconds) if math.isfinite(seconds) else 0


def backoff(round,*,retry_after=0,rng=random.random,base=20):
    ceiling=min(900,base*2**min(max(round-1,0),16))
    return max(retry_after,min(900,ceiling*(.8+.4*rng())))


def fault(exc):
    # Exception inheritance is insufficient: RemoteConflict is a ConnectionError.
    name=type(exc).__name__
    status=getattr(exc,'status_code',getattr(exc,'status',None))
    status=status if type(status) is int else None
    code=getattr(exc,'service_code',None)
    if isinstance(exc,ActionDeferred):category='deferred'
    elif name in ('ProcessingStopped','CancelledError','Cancelled'):category='stopped'
    elif name=='RemoteConflict':category='conflict'
    elif (isinstance(exc,ServiceConfigurationError) or isinstance(getattr(exc,'reason',exc),ssl.SSLError) or status in (401,403)
          or code in ('AUTH','MISSING_CREDENTIAL','INVALID_CREDENTIAL')):category='configuration'
    elif status in (408,429) or (status is not None and 500<=status<600):category='transient'
    elif (isinstance(exc,(ConnectionError,TimeoutError,HTTPException)) or
          (isinstance(exc,URLError) and not isinstance(exc,HTTPError)) or
          name in ('RagflowConnectionError','RagflowTimeoutError')):category='transient'
    elif isinstance(exc,ValueError) or name in ('PackageError','ProductionError','SyntaxRepairExhausted'):category='held'
    else:category='controller'
    result=dict(category=category,status=status,retry_after=retry_after(getattr(exc,'retry_after',0)),
                family='rate_limit' if status==429 else 'unavailable')
    if isinstance(exc,ActionDeferred):result['reason']=exc.reason
    return result
