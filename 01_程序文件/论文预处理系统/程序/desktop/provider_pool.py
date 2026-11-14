"""Shared, bounded provider capacity for one preprocessing process.

A complete model call retains its lane during validation and bounded retries.
This conservative accounting prevents retries from overshooting either quota.
The existing global LLM gate still limits actual simultaneous HTTP requests.
"""
import threading
import weakref
from pathlib import Path


class ProviderCancelled(RuntimeError):
    pass


class ProviderPool:
    def __init__(self, lan_limit, official_limit, strategy='lan_first'):
        if strategy not in ('lan_first', 'balanced', 'official_first'):
            raise ValueError('Unknown provider routing strategy')
        self.strategy = strategy
        self.limits = {'lan': int(lan_limit), 'official': int(official_limit)}
        if min(self.limits.values()) < 1:
            raise ValueError('Provider capacity must be positive')
        self.active = dict.fromkeys(self.limits, 0)
        self.peak = dict.fromkeys(self.limits, 0)
        self.selected = dict.fromkeys(self.limits, 0)
        self.condition = threading.Condition()

    def reserve(self, stop=None, *, exact=None, allow_official=True):
        lanes = [exact] if exact else ['lan', 'official'] if allow_official else ['lan']
        if not exact and allow_official and self.strategy == 'official_first':
            lanes = ['official', 'lan']
        if any(lane not in self.limits for lane in lanes):
            raise ValueError('Unknown provider lane')
        with self.condition:
            while True:
                if stop is not None and stop.is_set():
                    raise ProviderCancelled('Preprocessing stopped while waiting for model capacity')
                if not exact and allow_official and self.strategy == 'balanced':
                    # Recompute after every wake: route to the least occupied quota.
                    # Prefer official on ties so low load can use its lower latency.
                    lanes = sorted(['official', 'lan'], key=lambda name: self.active[name] / self.limits[name])
                for lane in lanes:
                    if self.active[lane] < self.limits[lane]:
                        self.active[lane] += 1
                        self.selected[lane] += 1
                        self.peak[lane] = max(self.peak[lane], self.active[lane])
                        return Lease(self, lane, stop)
                self.condition.wait(.1)

    def snapshot(self):
        with self.condition:
            return {name: dict(getattr(self, name)) for name in ('limits', 'active', 'peak', 'selected')}


class Lease:
    def __init__(self, pool, lane, stop):
        self.pool, self.lane, self.stop = pool, lane, stop
        self.released = False

    def switch(self, lane):
        if lane == self.lane:
            return
        self.release()
        replacement = self.pool.reserve(stop=self.stop, exact=lane)
        self.lane, self.released = replacement.lane, False
        replacement.released = True

    def release(self):
        with self.pool.condition:
            if not self.released:
                self.pool.active[self.lane] -= 1
                self.released = True
                self.pool.condition.notify_all()

    def __enter__(self): return self
    def __exit__(self, *args): self.release()


_pools = weakref.WeakValueDictionary()
_pool_lock = threading.Lock()


def shared_pool(root, routing):
    key = (str(Path(root).resolve()).casefold(), int(routing['lan_limit']), int(routing['official_limit']),
           routing.get('strategy', 'lan_first'))
    with _pool_lock:
        pool = _pools.get(key)
        if pool is None:
            pool = ProviderPool(key[1], key[2], strategy=key[3])
            _pools[key] = pool
        return pool
