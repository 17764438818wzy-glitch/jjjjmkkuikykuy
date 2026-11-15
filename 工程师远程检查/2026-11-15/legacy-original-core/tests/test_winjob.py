import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

import pytest


def wait_until(check, seconds=5):
    end = time.monotonic() + seconds
    while not check():
        if time.monotonic() > end:
            raise AssertionError('process condition not reached')
        time.sleep(.02)


TREE = """
import json, os, subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'], creationflags=0x08000000)
Path(sys.argv[1]).write_text(json.dumps(dict(root=os.getpid(), child=child.pid)))
time.sleep(120)
"""


def test_creation_time_membership_and_suspended_start(tmp_path):
    from paperflow.winjob import Job, process_identity, identity_alive
    marker = tmp_path / 'started.json'
    job = Job('Local\\pf-test-' + uuid.uuid4().hex)
    process = None
    try:
        assert not job.inheritable
        process = job.spawn([sys.executable, '-c', TREE, str(marker)], cwd=tmp_path)
        assert not marker.exists()
        assert job.pids() == [process.pid]
        assert process_identity(process.pid) == process.identity
        process.resume()
        wait_until(marker.exists)
        pids = json.loads(marker.read_text())
        assert set(pids.values()).issubset(job.pids())
        identities = [process_identity(pid) for pid in job.pids()]
        job.terminate()
        wait_until(lambda: job.active_count() == 0)
        wait_until(lambda: all(not identity_alive(item) for item in identities))
    finally:
        job.close()
        if process:
            process.close()


def test_terminating_one_job_keeps_other_job_and_unrelated_process(tmp_path):
    from paperflow.winjob import Job
    jobs = [Job('Local\\pf-test-' + uuid.uuid4().hex) for _ in range(2)]
    processes = []
    unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'],
                                 creationflags=0x08000000)
    try:
        for index, job in enumerate(jobs):
            marker = tmp_path / f'{index}.json'
            process = job.spawn([sys.executable, '-c', TREE, str(marker)], cwd=tmp_path)
            processes.append(process)
            process.resume()
            wait_until(marker.exists)
        jobs[0].terminate()
        wait_until(lambda: jobs[0].active_count() == 0)
        assert jobs[1].active_count() >= 2  # venv redirector and conhost are also owned
        assert unrelated.poll() is None
    finally:
        for job in jobs:
            job.close()
        for process in processes:
            process.close()
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_manager_process_exit_closes_last_job_handle_and_kills_tree(tmp_path):
    from paperflow.winjob import Job, identity_alive
    marker = tmp_path / 'tree.json'
    info = tmp_path / 'identities.json'
    gate = tmp_path / 'exit'
    name = 'Local\\pf-test-' + uuid.uuid4().hex
    code = '''
import json, os, sys, time
from pathlib import Path
from paperflow.winjob import Job, process_identity
job=Job(sys.argv[1]); process=job.spawn([sys.executable,'-c',sys.argv[2],sys.argv[3]],cwd=Path.cwd()); process.resume()
while not Path(sys.argv[3]).exists(): time.sleep(.01)
Path(sys.argv[4]).write_text(json.dumps([process_identity(pid) for pid in job.pids()]))
while not Path(sys.argv[5]).exists(): time.sleep(.01)
os._exit(86)
'''
    owner = subprocess.Popen([sys.executable, '-c', code, name, TREE, str(marker), str(info), str(gate)],
                             creationflags=0x08000000)
    try:
        wait_until(info.exists)
        identities = json.loads(info.read_text())
        assert len(identities) >= 2
        gate.touch()
        assert owner.wait(timeout=5) == 86
        wait_until(lambda: all(not identity_alive(item) for item in identities))
        assert Job.open(name) is None
    finally:
        if owner.poll() is None:
            owner.kill()
        owner.wait(timeout=5)


def test_nested_job_is_compatible_and_breakaway_is_rejected(tmp_path):
    from paperflow.winjob import Job
    marker = tmp_path / 'nested.json'
    code = '''
import json, subprocess, sys
from pathlib import Path
from paperflow.winjob import Job
job=Job(sys.argv[2]); child=job.spawn([sys.executable,'-c','import time; time.sleep(120)'],cwd=Path.cwd()); child.resume()
try:
    extra=subprocess.Popen([sys.executable,'-c','import time;time.sleep(120)'],creationflags=0x01000000|0x08000000)
    extra_pid=extra.pid
except OSError:
    extra_pid=None
Path(sys.argv[1]).write_text(json.dumps(dict(child=child.pid,extra_pid=extra_pid)))
import time; time.sleep(120)
'''
    job = Job('Local\\pf-test-' + uuid.uuid4().hex)
    process = None
    try:
        process = job.spawn([sys.executable, '-c', code, str(marker), 'Local\\pf-test-' + uuid.uuid4().hex], cwd=Path.cwd())
        process.resume()
        wait_until(marker.exists)
        result = json.loads(marker.read_text())
        assert result['extra_pid'] is None or result['extra_pid'] in job.pids()
        assert result['child'] in job.pids()
    finally:
        job.close()
        if process:
            process.close()
