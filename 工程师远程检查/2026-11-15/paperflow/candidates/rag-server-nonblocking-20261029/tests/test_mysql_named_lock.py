"""Opt-in test of the native advisory lock against a disposable local MySQL.

The database starts with an empty temporary data directory and loopback-only,
ephemeral port. It never reads the machine's MySQL configuration or data.
"""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import uuid

import pymysql
import pytest
from playhouse.pool import PooledMySQLDatabase


ROOT = Path(__file__).resolve().parents[1]
MYSQLD = Path(r"D:\MySQL\mysql-8.0.46-winx64\bin\mysqld.exe")
pytestmark = pytest.mark.skipif(os.environ.get("RAG_ISOLATED_MYSQL") != "1", reason="opt-in disposable MySQL test")


@pytest.fixture(scope="module")
def isolated_mysql(tmp_path_factory):
    assert MYSQLD.is_file(), "The designated local MySQL binary must exist"
    base = MYSQLD.parent.parent
    test_dir = tmp_path_factory.mktemp("rag-isolated-mysql")
    data_dir = test_dir / "data"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    common = [str(MYSQLD), "--no-defaults", f"--basedir={base}", f"--datadir={data_dir}"]
    initialized = subprocess.run(common + ["--initialize-insecure", "--console"],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 timeout=45, creationflags=flags, text=True, errors="replace")
    assert initialized.returncode == 0, initialized.stdout
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    args = common + ["--console", "--bind-address=127.0.0.1", f"--port={port}",
                     "--skip-mysqlx", "--skip-log-bin", "--performance-schema=OFF",
                     "--innodb-buffer-pool-size=32M", "--max-connections=20"]
    log = (test_dir / "mysqld.log").open("w", encoding="utf-8")
    proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
    identity_verified = False
    try:
        until = time.monotonic() + 25
        while time.monotonic() < until:
            if proc.poll() is not None:
                log.flush()
                pytest.fail(f"Disposable MySQL exited: {(test_dir / 'mysqld.log').read_text(encoding='utf-8')}")
            try:
                conn = pymysql.connect(host="127.0.0.1", port=port, user="root", connect_timeout=1)
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT @@datadir, @@port")
                        actual_dir, actual_port = cur.fetchone()
                    assert os.path.normcase(os.path.normpath(actual_dir)) == os.path.normcase(os.path.normpath(str(data_dir)))
                    assert actual_port == port
                    identity_verified = True
                finally:
                    conn.close()
                break
            except pymysql.MySQLError:
                time.sleep(0.1)
        else:
            pytest.fail("Disposable MySQL did not start on its loopback port")
        yield port
    finally:
        if identity_verified:
            try:
                conn = pymysql.connect(host="127.0.0.1", port=port, user="root", connect_timeout=1)
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT @@datadir, @@port")
                        actual_dir, actual_port = cur.fetchone()
                        assert os.path.normcase(os.path.normpath(actual_dir)) == os.path.normcase(os.path.normpath(str(data_dir)))
                        assert actual_port == port
                        cur.execute("SHUTDOWN")
                finally:
                    conn.close()
            except pymysql.MySQLError:
                pass  # It may already have stopped; terminate our child below.
        if proc.poll() is None:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.terminate()
                proc.wait(timeout=5)
        log.close()


@pytest.fixture
def native_lock(isolated_mysql):
    source = ROOT / "baseline/api/db/db_models.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if getattr(node, "name", None) in {"with_retry", "MysqlDatabaseLock"}]
    namespace = {"wraps": wraps, "time": time, "logging": logging, "PooledMySQLDatabase": PooledMySQLDatabase}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    db = PooledMySQLDatabase("mysql", host="127.0.0.1", port=isolated_mysql, user="root",
                             max_connections=4, stale_timeout=60, autoconnect=True)
    try:
        yield namespace["MysqlDatabaseLock"], db
    finally:
        db.close_all()


def other_process_lock(port, name):
    code = """
import json, pymysql, sys
c=pymysql.connect(host='127.0.0.1',port=int(sys.argv[1]),user='root')
with c.cursor() as cursor:
    cursor.execute('SELECT GET_LOCK(%s, 0)', (sys.argv[2],))
    print(json.dumps(cursor.fetchone()[0]))
c.close()
"""
    completed = subprocess.run([sys.executable, "-c", code, str(port), name], capture_output=True,
                               text=True, timeout=10, check=True,
                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    return json.loads(completed.stdout)


def test_native_mysql_lock_excludes_another_process(isolated_mysql, native_lock):
    lock_cls, db = native_lock
    lock = lock_cls("rag-isolated-cross-process", timeout=1, db=db)
    with lock:
        assert other_process_lock(isolated_mysql, lock.lock_name) == 0
    assert other_process_lock(isolated_mysql, lock.lock_name) == 1


def test_nested_connection_context_returns_locked_connection_to_pool(native_lock):
    lock_cls, db = native_lock
    name = "rag-isolated-pooled-session"
    lock = lock_cls(name, timeout=1, db=db)
    lock.lock()
    original_id = db.execute_sql("SELECT CONNECTION_ID()").fetchone()[0]
    with db.connection_context():
        assert db.execute_sql("SELECT CONNECTION_ID()").fetchone()[0] == original_id
    assert db.is_closed()
    entered, release = threading.Event(), threading.Event()
    findings, errors = {}, []

    def another_thread():
        try:
            with db.connection_context():
                findings["id"] = db.execute_sql("SELECT CONNECTION_ID()").fetchone()[0]
                findings["same_lock_again"] = db.execute_sql("SELECT GET_LOCK(%s, 0)", (name,)).fetchone()[0]
                entered.set()
                assert release.wait(5)
                findings["release_1"] = db.execute_sql("SELECT RELEASE_LOCK(%s)", (name,)).fetchone()[0]
                findings["release_2"] = db.execute_sql("SELECT RELEASE_LOCK(%s)", (name,)).fetchone()[0]
        except BaseException as exc:
            errors.append(exc)
            entered.set()

    worker = threading.Thread(target=another_thread)
    worker.start()
    try:
        assert entered.wait(5)
        assert not errors
        assert findings["id"] == original_id
        assert findings["same_lock_again"] == 1
        new_id = db.execute_sql("SELECT CONNECTION_ID()").fetchone()[0]
        assert new_id != original_id
        assert db.execute_sql("SELECT RELEASE_LOCK(%s)", (name,)).fetchone()[0] == 0
    finally:
        release.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert not errors
    assert (findings["release_1"], findings["release_2"]) == (1, 1)


def test_lock_is_lost_when_its_connection_drops(isolated_mysql, native_lock):
    lock_cls, db = native_lock
    name = "rag-isolated-lost-session"
    lock = lock_cls(name, timeout=1, db=db)
    lock.lock()
    owner_id = db.execute_sql("SELECT CONNECTION_ID()").fetchone()[0]
    assert other_process_lock(isolated_mysql, name) == 0
    conn = pymysql.connect(host="127.0.0.1", port=isolated_mysql, user="root")
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"KILL CONNECTION {int(owner_id)}")
    finally:
        conn.close()
    assert other_process_lock(isolated_mysql, name) == 1


@pytest.fixture
def experiment_gate(isolated_mysql, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "experiment"))
    from durable_gate import DurableGate
    schema = "rag_fixture_" + uuid.uuid4().hex
    admin = pymysql.connect(host="127.0.0.1", port=isolated_mysql, user="root")
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE `{schema}`")
        admin.commit()
    finally:
        admin.close()
    connect = lambda: pymysql.connect(host="127.0.0.1", port=isolated_mysql, user="root", database=schema)
    gate = DurableGate(connect)
    gate.create_test_schema()
    return gate, schema


def start_chunk(gate, doc, op, *, chunk_key=None, payload=None):
    chunk_key = chunk_key or op
    digest = hashlib.sha256((payload or chunk_key).encode()).hexdigest()
    return gate.begin(doc, op, chunk_key=chunk_key, payload_digest=digest)


def test_durable_gate_blocks_mutations_after_unknown_write(experiment_gate):
    from durable_gate import Blocked
    gate, _ = experiment_gate
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert all(pool.map(lambda i: start_chunk(gate, "paper", f"chunk-{i}"), range(4)))
    assert start_chunk(gate, "paper", "chunk-0") is False
    assert gate.request_change("paper", "disable-1", action="disable", expected_version=0) is False
    with pytest.raises(Blocked):
        start_chunk(gate, "paper", "chunk-4")
    for i in range(3):
        gate.settle("paper", f"chunk-{i}", confirmed=True)
    gate.settle("paper", "chunk-3", confirmed=False)
    assert gate.request_change("paper", "disable-1", action="disable", expected_version=0) is False
    with pytest.raises(Blocked):
        gate.claim_change("paper", "disable-1")
    with pytest.raises(Blocked):
        gate.finish_change("paper", "disable-1")


def test_durable_pending_survives_a_writer_process_exit(isolated_mysql, experiment_gate):
    from durable_gate import Blocked
    gate, schema = experiment_gate
    code = """
import pymysql, sys
sys.path.insert(0, sys.argv[1])
from durable_gate import DurableGate
gate=DurableGate(lambda: pymysql.connect(host='127.0.0.1',port=int(sys.argv[2]),user='root',database=sys.argv[3]))
import hashlib
name='operation-from-exited-process'
assert gate.begin('paper',name,chunk_key=name,payload_digest=hashlib.sha256(name.encode()).hexdigest())
"""
    subprocess.run([sys.executable, "-c", code, str(ROOT / "experiment"), str(isolated_mysql), schema],
                   capture_output=True, text=True, timeout=10, check=True,
                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    assert gate.request_change("paper", "delete-1", action="delete", expected_version=0) is False
    assert start_chunk(gate, "paper", "operation-from-exited-process") is False
    with pytest.raises(Blocked):
        gate.finish_change("paper", "delete-1")


def test_durable_gate_drains_confirmed_writes_before_delete(experiment_gate):
    from durable_gate import Blocked
    gate, _ = experiment_gate
    assert start_chunk(gate, "paper", "chunk-1")
    assert start_chunk(gate, "paper", "chunk-2")
    assert not gate.request_change("paper", "delete-1", action="delete", expected_version=0)
    with pytest.raises(Blocked):
        gate.request_change("paper", "delete-2", action="delete", expected_version=0)
    gate.settle("paper", "chunk-1", confirmed=True)
    gate.settle("paper", "chunk-2", confirmed=True)
    assert gate.request_change("paper", "delete-1", action="delete", expected_version=0)
    assert gate.claim_change("paper", "delete-1")
    assert gate.claim_change("paper", "delete-1") is False
    gate.finish_change("paper", "delete-1")
    with pytest.raises(Blocked):
        start_chunk(gate, "paper", "chunk-3")


def test_write_and_change_race_cannot_both_start(experiment_gate):
    from durable_gate import Blocked
    gate, _ = experiment_gate
    for i in range(10):
        doc = f"race-{i}"
        barrier = threading.Barrier(2)

        def write():
            barrier.wait(timeout=5)
            try:
                return start_chunk(gate, doc, "chunk")
            except Blocked:
                return False

        def change():
            barrier.wait(timeout=5)
            return gate.request_change(doc, "disable", action="disable", expected_version=0)

        with ThreadPoolExecutor(max_workers=2) as pool:
            write_result = pool.submit(write)
            change_result = pool.submit(change)
            assert write_result.result(timeout=5) != change_result.result(timeout=5)


def test_completed_change_cannot_replay_after_a_newer_change(experiment_gate):
    from durable_gate import Blocked
    gate, _ = experiment_gate
    assert gate.request_change("paper", "disable-1", action="disable", expected_version=0)
    assert gate.claim_change("paper", "disable-1")
    gate.finish_change("paper", "disable-1")
    assert gate.request_change("paper", "enable-2", action="enable", expected_version=1)
    assert gate.claim_change("paper", "enable-2")
    gate.finish_change("paper", "enable-2")
    assert not gate.request_change("paper", "disable-1", action="disable", expected_version=0)
    with pytest.raises(Blocked):
        gate.request_change("paper", "disable-1", action="enable", expected_version=0)
    with pytest.raises(Blocked):
        gate.request_change("paper", "disable-new", action="disable", expected_version=0)


def test_unknown_chunk_is_bound_to_identity_and_blocks_new_writes(experiment_gate):
    from durable_gate import Blocked
    gate, _ = experiment_gate
    assert start_chunk(gate, "paper", "attempt-1", chunk_key="logical-chunk", payload="text-1")
    gate.settle("paper", "attempt-1", confirmed=False)
    assert not start_chunk(gate, "paper", "attempt-2", chunk_key="logical-chunk", payload="text-1")
    with pytest.raises(Blocked):
        start_chunk(gate, "paper", "attempt-2", chunk_key="logical-chunk", payload="different")
    with pytest.raises(Blocked):
        start_chunk(gate, "paper", "another-chunk")


def test_autocommit_connection_cannot_release_gate_lock_early(isolated_mysql, experiment_gate):
    from durable_gate import DurableGate
    _, schema = experiment_gate
    gate = DurableGate(lambda: pymysql.connect(host="127.0.0.1", port=isolated_mysql,
                                              user="root", database=schema, autocommit=True))
    original = gate._lock_doc
    writer_has_checked, release_writer, change_completed = threading.Event(), threading.Event(), threading.Event()
    values, errors = {}, []

    def held_for_interleaving(cur, doc):
        result = original(cur, doc)
        if threading.current_thread().name == "gate-writer":
            writer_has_checked.set()
            assert release_writer.wait(5)
        return result

    gate._lock_doc = held_for_interleaving

    def writer():
        try:
            values["write"] = start_chunk(gate, "paper", "chunk")
        except BaseException as exc:
            errors.append(exc)

    def changer():
        try:
            ready = gate.request_change("paper", "disable", action="disable", expected_version=0)
            values["change"] = ready
            values["claim"] = gate.claim_change("paper", "disable") if ready else False
        except BaseException as exc:
            errors.append(exc)
        finally:
            change_completed.set()

    one = threading.Thread(name="gate-writer", target=writer)
    two = threading.Thread(name="gate-changer", target=changer)
    one.start()
    try:
        assert writer_has_checked.wait(5)
        two.start()
        change_completed.wait(1.5)
    finally:
        release_writer.set()
        one.join(timeout=5)
        if two.ident is not None:
            two.join(timeout=5)
    assert not one.is_alive() and not two.is_alive()
    assert not errors
    assert values == {"write": True, "change": False, "claim": False}
