"""Real child-process crashes and SQLite writers; no graph, Job, LLM or RAG calls."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from paperflow.package import sha
from test_handoff import api, count, material, prepare


WORKER = r'''
from contextlib import contextmanager
import json, os, sys, time
from pathlib import Path
from paperflow.store import Store
from paperflow.handoff import PackageHandoff

config = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
h = PackageHandoff(Store(config['db']), config['root'])
thread, fingerprint = config['thread'], config['fingerprint']
if config['mode'] in ('busy', 'busy_timeout'):
    h.publish(thread, fingerprint)
Path(config['ready']).write_text('ready')
deadline = time.monotonic() + 15
while not Path(config['go']).exists():
    if time.monotonic() > deadline:
        raise TimeoutError('parent did not release test worker')
    time.sleep(.01)
mode = config['mode']
if mode == 'before_publish':
    os._exit(86)
h.publish(thread, fingerprint)
if mode == 'after_publish':
    os._exit(86)
if mode == 'in_transaction':
    original_write = h.ledger._write
    def crash_write(operation):
        def injected(c):
            c.create_function('test_crash', 0, lambda: os._exit(86))
            c.execute("CREATE TEMP TRIGGER process_death BEFORE INSERT ON specialty_tasks "
                      "WHEN NEW.specialty_code='120202' BEGIN SELECT test_crash(); END")
            return operation(c)
        return original_write(injected)
    h.ledger._write = crash_write
attempts = []
if mode in ('busy', 'busy_timeout'):
    original_db = h.ledger.store.db
    @contextmanager
    def observed_db(*, timeout=30):
        # Only count write attempts, immediately before BEGIN IMMEDIATE.
        with original_db(timeout=timeout) as c:
            yield c
    original_write = h.ledger._write
    @contextmanager
    def counted_db(*, timeout=30):
        attempts.append(1)
        Path(config['attempts']).write_text(str(len(attempts)))
        with original_db(timeout=timeout) as c:
            yield c
    def observed_write(operation):
        h.ledger.store.db = counted_db
        try:
            return original_write(operation)
        finally:
            h.ledger.store.db = original_db
    h.ledger._write = observed_write
started = time.monotonic()
try:
    result = h.resume(thread, fingerprint)
    if mode == 'after_commit':
        os._exit(86)
    output = dict(result=result, elapsed=time.monotonic()-started, attempts=len(attempts))
except Exception as exc:
    if mode != 'busy_timeout':
        raise
    output = dict(error=type(exc).__name__, elapsed=time.monotonic()-started, attempts=len(attempts))
print(json.dumps(output))
'''


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError('test process did not reach the required boundary')
        time.sleep(.01)


@pytest.fixture
def launch(tmp_path):
    processes = []

    def start(h, intent, mode, name, gate=None):
        config = dict(db=str(h.ledger.store.path), root=str(h.root),
                      thread=intent['thread_id'], fingerprint=intent['input_fingerprint'], mode=mode,
                      ready=str(tmp_path / (name + '.ready')), go=str(gate or tmp_path / (name + '.go')),
                      attempts=str(tmp_path / (name + '.attempts')))
        path = tmp_path / (name + '.json')
        path.write_text(json.dumps(config), encoding='utf-8')
        process = subprocess.Popen([sys.executable, '-c', WORKER, str(path)],
                                   cwd=Path(__file__).resolve().parents[1],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        processes.append(process)
        wait_until(lambda: Path(config['ready']).exists() or process.poll() is not None)
        assert process.poll() is None, process.communicate()
        return process, config

    yield start
    for process in processes:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def finish(process, expected=0):
    out, err = process.communicate(timeout=15)
    assert process.returncode == expected, (out, err)
    return json.loads(out) if out else None


@pytest.mark.parametrize('boundary', ['before_publish', 'after_publish', 'in_transaction', 'after_commit'])
def test_real_process_exit_and_restart_preserve_original_handoff(tmp_path, package_factory, launch, boundary):
    root, task, catalog = material(package_factory, sealed=False, codes=('120201', '120202'))
    catalog['majors'].append(dict(code='120202', paper_dataset_id='ds-b'))
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    child, config = launch(h, intent, boundary, boundary)
    Path(config['go']).touch()
    finish(child, 86)
    committed = boundary == 'after_commit'
    assert count(h, 'delivery_packages') == int(committed)
    assert count(h, 'specialty_tasks') == (2 if committed else 0)
    final = Path(intent['package_dir'])
    original = (final / 'manifest.json').read_bytes() if final.exists() else None
    reopened = api(tmp_path)
    result = reopened.resume(intent['thread_id'], intent['input_fingerprint'])
    if original is not None:
        assert (final / 'manifest.json').read_bytes() == original
    assert result['manifest_sha256'] == sha(final / 'manifest.json')
    assert count(reopened, 'delivery_packages') == count(reopened, 'handoff_results') == count(reopened, 'events') == 1
    assert count(reopened, 'specialty_tasks') == 2
    assert all(item['generation'] == 1 for item in reopened.ledger.expectations(task['task_id'], ['120201', '120202']).values())
    with reopened.ledger.store.db() as c:
        assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert c.execute('PRAGMA foreign_key_check').fetchall() == []


def test_three_processes_handoff_same_intent_once(tmp_path, package_factory, launch):
    root, task, catalog = material(package_factory, sealed=False)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    gate = tmp_path / 'shared.go'
    children = [launch(h, intent, 'normal', str(i), gate)[0] for i in range(3)]
    gate.touch()
    results = [finish(child)['result'] for child in children]
    assert results[0] == results[1] == results[2]
    for table in ('publication_intents', 'delivery_packages', 'specialty_tasks', 'handoff_results', 'events'):
        assert count(h, table) == 1
    assert h.ledger.expectations(task['task_id'], ['120201'])['120201']['generation'] == 1


def test_concurrent_versions_cannot_overwrite_same_expected_generation(tmp_path, package_factory, launch):
    h = api(tmp_path)
    gate = tmp_path / 'shared.go'
    children = []
    for revision in (1, 2):
        root, task, catalog = material(package_factory, f'r{revision:03d}')
        intent = prepare(h, root, task, catalog, thread=f'run-{revision}')
        children.append(launch(h, intent, 'normal', str(revision), gate)[0])
    gate.touch()
    results = [finish(child)['result'] for child in children]
    rows = [h.ledger.deliveries(result['manifest_sha256'])[0] for result in results]
    assert sorted(row['status'] for row in rows) == ['pending', 'superseded']
    winner = next(row['manifest_sha256'] for row in rows if row['status'] == 'pending')
    assert h.ledger.expectations(task['task_id'], ['120201']) == {
        '120201': dict(manifest_sha256=winner, generation=1)}
    assert count(h, 'delivery_packages') == count(h, 'specialty_tasks') == count(h, 'events') == 2


def test_busy_retry_rereads_freeze_committed_by_competing_writer(tmp_path, package_factory, launch):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    child, config = launch(h, intent, 'busy', 'busy')
    with h.ledger.store.db() as writer:
        writer.execute('BEGIN IMMEDIATE')
        writer.execute('INSERT INTO package_freezes VALUES(?,?,?)', (sha(root / 'manifest.json'), '*', 'during wait'))
        Path(config['go']).touch()
        attempts = Path(config['attempts'])
        wait_until(lambda: attempts.exists() and attempts.read_text().strip() in ('2', '3', '4', '5'))
    output = finish(child)
    assert output['attempts'] >= 2
    row = h.ledger.deliveries(output['result']['manifest_sha256'])[0]
    assert row['status'] == 'frozen'
    assert h.ledger.expectations(task['task_id'], ['120201'])['120201']['generation'] == 0


def test_write_lock_timeout_is_bounded_and_retains_recoverable_package(tmp_path, package_factory, launch):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    child, config = launch(h, intent, 'busy_timeout', 'timeout')
    with h.ledger.store.db() as writer:
        writer.execute('BEGIN IMMEDIATE')
        Path(config['go']).touch()
        output = finish(child)
    assert output['error'] == 'OperationalError'
    assert output['attempts'] == 5
    assert .4 < output['elapsed'] < 3
    assert count(h, 'delivery_packages') == 0
    assert Path(intent['package_dir']).is_dir()
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert result['status'] == 'registered'
