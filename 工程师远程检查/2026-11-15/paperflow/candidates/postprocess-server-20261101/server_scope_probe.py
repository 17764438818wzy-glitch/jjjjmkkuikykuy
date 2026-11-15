"""Exercise one real delegated cgroup with synthetic, non-paper evidence."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from uuid import uuid4, uuid5

from remote_jobs import NAMESPACE, RemoteJobs
from server_supervisor import ServerSupervisor


def main():
    cgroup = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
    if not cgroup.is_dir() or not os.access(cgroup, os.W_OK):
        raise RuntimeError('this probe needs its own writable delegated systemd scope')
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic-scope-only',
                    receipt_id='b' * 64, attempt_id='scope-test', parent_run_id=str(uuid4()),
                    task_run_id=str(uuid4()))
    job = dict(job_id=str(uuid5(NAMESPACE, identity['task_run_id'])), claim_id=str(uuid4()),
               identity=identity, generation=1, manifest_sha256='c' * 64,
               snapshot_sha256='d' * 64, code_sha256='e' * 64)
    with tempfile.TemporaryDirectory(prefix='paperflow-scope-test-') as temporary:
        root = Path(temporary)
        marker, release, artifact = root / 'child-ready', root / 'release', root / 'artifact'
        artifact.write_bytes(b'synthetic evidence')
        receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=identity,
                       claim_id=job['claim_id'], manifest_sha256=job['manifest_sha256'],
                       validation_status='passed', artifacts=[dict(path=str(artifact),
                       sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())])
        def validate(actual, result):
            return (actual == job and result == receipt and
                    hashlib.sha256(artifact.read_bytes()).hexdigest() == result['artifacts'][0]['sha256'])
        ledger = RemoteJobs(root / 'ledger.sqlite', capacity=1)
        ledger.initialize()
        ledger.reserve(job)
        supervisor = ServerSupervisor(ledger, cgroup_parent=cgroup, worker_uid=os.getuid(),
                                      worker_gid=os.getgid(), receipt_validator=validate)
        supervisor.initialize()
        # The short-lived leader forks and exits. The detached child remains in
        # its cgroup until released; the supervisor must keep the slot occupied.
        child_code = ('import os,sys,time,pathlib; pid=os.fork(); '
                      'sys.exit(0) if pid else None; os.setsid(); '
                      'pathlib.Path(sys.argv[1]).touch(); '
                      'deadline=time.monotonic()+12; '
                      'exec("while not pathlib.Path(sys.argv[2]).exists() and time.monotonic()<deadline:\\n time.sleep(.02)")')
        try:
            supervisor.launch(job, [sys.executable, '-c', child_code, str(marker), str(release)],
                              timeout_seconds=20)
            deadline = time.monotonic() + 8
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(.02)
            if not marker.exists():
                raise RuntimeError('synthetic detached child did not start')
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                current = supervisor.poll(job, receipt=receipt)
                if current['leader_alive'] is False:
                    break
                time.sleep(.02)
            if current['leader_alive'] is not False or current['server_alive'] is not True:
                raise RuntimeError('did not observe exited leader and live detached child')
            if current['state'] != 'running' or ledger.occupied() != 1:
                raise RuntimeError('a live child released its execution slot')
            release.touch()
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                current = supervisor.poll(job, receipt=receipt)
                if current['state'] == 'finished':
                    break
                time.sleep(.02)
            if current['state'] != 'finished' or ledger.occupied() != 0:
                raise RuntimeError('empty cgroup and verified receipt did not finish')
            held = supervisor.held_receipt(job)
            if json.loads(held['receipt_json']) != receipt:
                raise RuntimeError('durable synthetic receipt changed')
            print(json.dumps(dict(server_scope='passed', detached_child_blocked_release=True,
                                  final_scope_empty=True, held_receipt_verified=True)))
        finally:
            release.touch(exist_ok=True)
            deadline = time.monotonic() + 4
            path = cgroup / job['job_id']
            while path.exists() and time.monotonic() < deadline:
                try:
                    path.rmdir()
                except OSError:
                    time.sleep(.02)


if __name__ == '__main__':
    main()
