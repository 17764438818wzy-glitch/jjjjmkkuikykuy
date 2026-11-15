"""Bounded lifecycle facts and proof of the previous local vocabulary's exit.

The existing broker owns all transitions under its short in-memory lock. This
module neither starts a daemon nor changes control, resource or write ledgers.
"""
import json
import subprocess


class CounterProcessExited(ValueError):
    """A dead transport is transient only after its old process exit is proved."""


class CounterProtocolError(ValueError):
    """A malformed or unbound reply is terminal, even if its process also dies."""


class RecoveryState:
    def __init__(self, enabled, delays):
        if (not isinstance(enabled, bool) or not 1 <= len(delays) <= 3 or
                any(type(v) not in (float, int) or not 0 < v <= 30 for v in delays)):
            raise ValueError('bounded exact-counter recovery required')
        self.enabled, self.delays = enabled, tuple(delays)
        self.state, self.generation, self.restarts = 'starting', 1, 0
        self.failure_category, self.retry_at = None, None

    def invalidate(self, category, now, *, terminal=False):
        if self.state in ('backoff', 'failed'):
            if terminal:
                self.state, self.failure_category, self.retry_at = 'failed', category, None
            return False
        self.generation += 1
        self.failure_category = category
        if terminal or not self.enabled:
            self.state, self.retry_at = 'failed', None
        elif self.restarts >= len(self.delays):
            self.state, self.failure_category, self.retry_at = 'failed', 'restart_limit', None
        else:
            self.state = 'backoff'
            self.retry_at = now + self.delays[self.restarts]
        return True

    def start_retry(self, now):
        if self.state != 'backoff' or now < self.retry_at:
            return False
        # The budget belongs to this broker lifetime. New work and successful
        # counts never reset it; exhausted services need a reviewed new owner.
        self.restarts += 1
        self.state, self.retry_at = 'starting', None
        return True


def helper_identity_valid(runtime):
    return (type(runtime.get('pid')) is int and runtime['pid'] > 0 and
            type(runtime.get('helper_start_ticks')) is int and runtime['helper_start_ticks'] > 0 and
            type(runtime.get('helper_pid_namespace')) is int and runtime['helper_pid_namespace'] > 0 and
            isinstance(runtime.get('helper_boot_id'), str) and bool(runtime['helper_boot_id']) and
            isinstance(runtime.get('worker_sha256'), str) and len(runtime['worker_sha256']) == 64)


def helper_exited(settings, runtime):
    """A bounded read in the same configured WSL/interpreter namespace.

    A Windows wsl.exe exit alone is insufficient. Missing identity, a changed
    namespace, or inability to inspect /proc returns unknown, never a pass.
    No signal is sent here and a reused Linux PID is not touched.
    """
    if not helper_identity_valid(runtime) or not settings.get('command'):
        return False
    script = """import json,os,sys
from pathlib import Path
pid,ticks,namespace,boot=sys.argv[1:]
same_context=(os.stat('/proc/self/ns/pid').st_ino==int(namespace) and Path('/proc/sys/kernel/random/boot_id').read_text().strip()==boot)
alive=None
if same_context:
    try:
        raw=Path('/proc/'+str(int(pid))+'/stat').read_text()
        fields=raw[raw.rfind(')')+2:].split()
        alive=int(fields[19])==int(ticks) and fields[0] not in ('Z','X')
    except FileNotFoundError:
        alive=False
print(json.dumps({'same_context':same_context,'alive':alive}))
"""
    try:
        result = subprocess.run([*settings['command'], '-c', script, str(runtime['pid']),
            str(runtime['helper_start_ticks']), str(runtime['helper_pid_namespace']), runtime['helper_boot_id']],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=5, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        value = json.loads(result.stdout)
        return result.returncode == 0 and value == dict(same_context=True, alive=False)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return False
