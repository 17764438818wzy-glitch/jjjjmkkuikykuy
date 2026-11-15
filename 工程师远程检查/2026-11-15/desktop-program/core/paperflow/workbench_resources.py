"""Read-only host memory over the existing pinned, restricted Harness health RPC."""
from contextlib import contextmanager
import threading
import time
import math

from .remote_transport import request


def counter_health(endpoint):
    """A small, read-only projection; never exposes paths, requests or exceptions."""
    default = dict(state='unconfigured', available=False, generation=0, restarts=0, max_restarts=0,
                   failure_category=None, retry_in_seconds=None)
    if endpoint is None:
        return default
    value = endpoint.health_snapshot()
    states = {'ready', 'starting', 'backoff', 'failed', 'paused', 'closed'}
    categories = {'process_exited', 'timeout', 'artifact_drift', 'identity_mismatch', 'invalid_reply',
                  'startup_unconfirmed', 'exit_unconfirmed', 'health_unconfirmed', 'restart_limit'}
    state = value.get('state') if value.get('state') in states else 'failed'
    result = dict(default, state=state, available=state == 'ready' and value.get('available') is True)
    for key in ('generation', 'restarts', 'max_restarts'):
        v = value.get(key)
        if type(v) is int and 0 <= v <= (3 if key != 'generation' else 1000000):
            result[key] = v
    if value.get('failure_category') in categories:
        result['failure_category'] = value['failure_category']
    retry = value.get('retry_in_seconds')
    if type(retry) in (int, float) and math.isfinite(retry) and 0 <= retry <= 30:
        result['retry_in_seconds'] = retry
    return result


class ServerMemory:
    def __init__(self,settings):
        self.settings=settings
        self.lock=threading.Lock()
        self.value=None
        self.until=0

    @contextmanager
    def reserve(self):
        # Take the one slot before entering FastAPI's shared worker pool.
        if not self.lock.acquire(blocking=False):raise RuntimeError('服务器内存正在读取，请稍后刷新')
        try:yield
        finally:self.lock.release()

    def read(self):
        if self.value is not None and time.monotonic()<self.until:return dict(self.value)
        value=dict(state='unavailable',total_bytes=None,used_bytes=None,observed_at=None)
        if self.settings:
            try:
                header=dict(op='health',namespace='0'*64,execution_id='0'*32,epoch='0'*32,job_id='0'*32,
                    image=self.settings['image'],deployment_id=self.settings['deployment_id'])
                result=request(self.settings,header,timeout=3)
                memory=result['host_memory']
                if result.get('ready') is not True or result.get('image')!=self.settings['image']:
                    raise ValueError('memory endpoint identity mismatch')
                total,used,available=(memory[k] for k in ('total_bytes','used_bytes','available_bytes'))
                if (any(type(v) is not int for v in (total,used,available)) or total<=0
                        or not 0<=available<=total or used!=total-available):
                    raise ValueError('invalid host memory sample')
                value=dict(state='available',total_bytes=total,used_bytes=used,observed_at=time.time())
            except Exception:
                # A failed/old gateway never supplies zeroes or a stale success.
                pass
        self.value=value
        self.until=time.monotonic()+5
        return dict(value)
