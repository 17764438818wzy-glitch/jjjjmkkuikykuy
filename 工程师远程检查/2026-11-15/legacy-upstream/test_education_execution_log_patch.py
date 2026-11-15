"""Offline control-flow checks, not a reproduction of MySQL lock scheduling."""
import ast
import logging
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest

EDUCATION = "ecf88dc4b82b11f1a8846d29005ec004"
LOCAL_EDUCATION = "3e8ef88cbc4e11f1a8e165c0e34ea629"

class OperationalError(Exception):
    pass

class LockDatabaseDouble:
    """SQL boundary double with a real connection to distinguish close from return-to-pool."""
    def __init__(self):
        self.physical_connection = sqlite3.connect(":memory:")
        self.sessions = [self.physical_connection]
        self.closed = False
        self.lock_owner = None
        self.swap_during_acquire = False
        self.swap_during_release = False
        self.acquire_result = (1,)
        self.release_result = (1,)
        self.acquire_error = None
        self.acquire_fetch_error = None
        self.release_error = None
        self.transaction_active = False
        self.queries = []
        self.returned_to_pool = False
        self.physically_closed = False

    def in_transaction(self):
        return self.transaction_active

    def connection(self):
        return self.physical_connection

    def is_closed(self):
        return self.closed

    @property
    def lock_held(self):
        if self.lock_owner is None:
            return False
        try:
            self.lock_owner.execute("select 1")
        except sqlite3.ProgrammingError:
            return False
        return True

    def swap_connection(self):
        # A reconnect can return the old session to the pool before using another.
        self.physical_connection = sqlite3.connect(":memory:")
        self.sessions.append(self.physical_connection)
        self.closed = False

    def execute_sql(self, sql, parameters):
        self.queries.append((sql, parameters))
        if sql == "SELECT GET_LOCK(%s, %s)":
            if self.swap_during_acquire:
                self.swap_connection()
            # Query/fetch errors may leave acquisition acknowledgement uncertain.
            if self.acquire_result and self.acquire_result[0] == 1:
                self.lock_owner = self.physical_connection
            if self.acquire_error is not None:
                raise self.acquire_error
            def fetchone():
                if self.acquire_fetch_error is not None:
                    raise self.acquire_fetch_error
                return self.acquire_result
            return SimpleNamespace(fetchone=fetchone)
        if sql == "SELECT RELEASE_LOCK(%s)":
            if self.swap_during_release:
                self.swap_connection()
            if self.release_error is not None:
                raise self.release_error
            result = self.release_result
            if result and result[0] == 1:
                if self.lock_owner is self.physical_connection:
                    self.lock_owner = None
                else:
                    result = (0,)
            return SimpleNamespace(fetchone=lambda: result)
        raise AssertionError(f"Unexpected SQL boundary: {sql}")

    def close(self):
        # A pooled close keeps the physical session, including any named lock.
        self.returned_to_pool = True

    def manual_close(self):
        self.physical_connection.close()
        self.physically_closed = True
        self.closed = True


def load_retry(database=None):
    installer = Path(__file__).parent / "server_patches/education_execution_log/install.py"
    installer_ast = ast.parse(installer.read_text(encoding="utf8"))
    literal = next(node.value for node in installer_ast.body
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "helper" for t in node.targets))
    source = ast.parse(ast.literal_eval(literal))
    node = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == "_persist_education_log_with_retry")
    namespace = {"OperationalError": OperationalError, "PipelineTaskType": SimpleNamespace(PARSE="parse"),
                 "logging": logging, "DB": database or LockDatabaseDouble(),
                 "time": SimpleNamespace(sleep=lambda _: None),
                 "random": SimpleNamespace(uniform=lambda lo, hi: lo)}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "candidate-helper", "exec"), namespace)
    return namespace[node.name]

class RetryCandidateTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("create table logs (id text primary key)")
        self.db.execute("insert into logs values ('old-log')")
        self.db.commit()
        self.calls = 0
        self.lock_db = LockDatabaseDouble()
        self.retry = load_retry(self.lock_db)
    def tearDown(self):
        self.db.close()
        for session in self.lock_db.sessions:
            session.close()
    def rows(self):
        return [row[0] for row in self.db.execute("select id from logs order by id")]
    def operation(self, failures, error_code=1213):
        def persist(lock_name=None):
            self.calls += 1
            with self.db:
                self.db.execute("insert into logs values ('same-execution-log')")
                self.db.execute("delete from logs where id = 'old-log'")
                if self.calls <= failures:
                    raise OperationalError(error_code, "injected transaction error after insert/prune")
            return "same-execution-log"
        return persist
    def test_education_retries_whole_rolled_back_transaction_with_same_log_id(self):
        try:
            result = self.retry(self.operation(1), EDUCATION, "parse")
        except OperationalError as error:
            self.fail(f"Final log was lost after transient 1213: {error.args[0]}")
        self.assertEqual(result, "same-execution-log")
        self.assertEqual(self.rows(), ["same-execution-log"])
        self.assertEqual(self.calls, 2)
        self.assertEqual(self.lock_db.queries, [
            ("SELECT GET_LOCK(%s, %s)", ("education-log:ecf88dc4b82b11f1a8846d29005ec004", 15)),
            ("SELECT RELEASE_LOCK(%s)", ("education-log:ecf88dc4b82b11f1a8846d29005ec004",)),
        ])
        self.assertFalse(self.lock_db.lock_held)

    def test_local_education_uses_the_same_log_retry(self):
        result = self.retry(self.operation(1), LOCAL_EDUCATION, "parse")
        self.assertEqual(result, "same-execution-log")
        self.assertEqual(self.calls, 2)
        self.assertEqual(self.rows(), ["same-execution-log"])
        self.assertFalse(self.lock_db.lock_held)

    def test_other_dataset_keeps_original_single_attempt(self):
        with self.assertRaises(OperationalError):
            self.retry(self.operation(1), "unrelated-finance-dataset", "parse")
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.rows(), ["old-log"])
        self.assertEqual(self.lock_db.queries, [])
    def test_non_parse_task_keeps_original_single_attempt(self):
        with self.assertRaises(OperationalError):
            self.retry(self.operation(1), EDUCATION, "graphrag")
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.rows(), ["old-log"])
        self.assertEqual(self.lock_db.queries, [])
    def test_outer_transaction_is_never_retried(self):
        self.retry.__globals__['DB'].in_transaction=lambda:True
        with self.assertRaises(OperationalError):
            self.retry(self.operation(1),EDUCATION,'parse')
        self.assertEqual(self.calls,1)
        self.assertEqual(self.rows(),['old-log'])
        self.assertEqual(self.lock_db.queries, [])
    def test_non_deadlock_error_is_not_retried(self):
        with self.assertRaises(OperationalError) as result:
            self.retry(self.operation(1, 1205), EDUCATION, "parse")
        self.assertEqual(result.exception.args[0], 1205)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.rows(), ["old-log"])
    def test_exhaustion_preserves_prior_log_and_propagates(self):
        with self.assertRaises(OperationalError):
            self.retry(self.operation(5), EDUCATION, "parse")
        self.assertEqual(self.calls, 5)
        self.assertEqual(self.rows(), ["old-log"])
    def test_no_error_writes_once(self):
        self.retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.rows(), ["same-execution-log"])

    def assert_session_discarded(self):
        self.assertFalse(self.lock_db.returned_to_pool, "A session holding an uncertain lock must not return to the pool")
        self.assertFalse(self.lock_db.lock_held)
        with self.assertRaises(sqlite3.ProgrammingError):
            self.lock_db.physical_connection.execute("select 1")

    def test_lock_timeout_never_starts_persistence(self):
        self.lock_db.acquire_result = (0,)
        with self.assertRaises(RuntimeError):
            self.retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.rows(), ["old-log"])
        self.assertEqual(len(self.lock_db.queries), 1)
        self.assertFalse(self.lock_db.lock_held)

    def test_null_lock_result_never_starts_persistence(self):
        self.lock_db.acquire_result = (None,)
        with self.assertRaises(RuntimeError):
            self.retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.rows(), ["old-log"])
        self.assertEqual(len(self.lock_db.queries), 1)

    def test_acquire_query_error_discards_session_and_preserves_exception(self):
        error = OperationalError(2013, "connection lost during named-lock acquisition")
        self.lock_db.acquire_error = error
        with self.assertRaises(OperationalError) as caught:
            self.retry(self.operation(0), EDUCATION, "parse")
        self.assertIs(caught.exception, error)
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.rows(), ["old-log"])
        self.assertEqual(len(self.lock_db.queries), 1)
        self.assert_session_discarded()

    def test_acquire_fetch_error_discards_possibly_locked_session(self):
        error = OperationalError(2013, "lock acquired but acknowledgement could not be read")
        self.lock_db.acquire_fetch_error = error
        with self.assertRaises(OperationalError) as caught:
            self.retry(self.operation(0), EDUCATION, "parse")
        self.assertIs(caught.exception, error)
        self.assertEqual(self.calls, 0)
        self.assertEqual(self.rows(), ["old-log"])
        self.assert_session_discarded()

    def test_release_error_preserves_acknowledged_commit_and_discards_pool_session(self):
        self.lock_db.release_error = OperationalError(2013, "connection lost while releasing named lock")
        with self.assertLogs(level="ERROR"):
            result = self.retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(result, "same-execution-log")
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.rows(), ["same-execution-log"])
        self.assert_session_discarded()

    def test_failed_release_result_preserves_commit_and_physically_discards(self):
        for release_result in [(0,), (None,), None]:
            with self.subTest(release_result=release_result):
                for session in self.lock_db.sessions:
                    session.close()
                self.lock_db = LockDatabaseDouble()
                self.lock_db.release_result = release_result
                retry = load_retry(self.lock_db)
                committed_result = object()
                with self.assertLogs(level="WARNING"):
                    result = retry(lambda *_: committed_result, EDUCATION, "parse")
                self.assertIs(result, committed_result)
                self.assert_session_discarded()

    def test_release_error_cannot_hide_persistence_failure(self):
        self.lock_db.release_error = RuntimeError("cleanup failed")
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(OperationalError) as caught:
                self.retry(self.operation(1, 1205), EDUCATION, "parse")
        self.assertEqual(caught.exception.args[0], 1205)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.rows(), ["old-log"])
        self.assert_session_discarded()

    def test_release_error_cannot_hide_exhausted_deadlock(self):
        self.lock_db.release_error = RuntimeError("cleanup failed")
        with self.assertLogs(level="WARNING"):
            with self.assertRaises(OperationalError) as caught:
                self.retry(self.operation(5), EDUCATION, "parse")
        self.assertEqual(caught.exception.args[0], 1213)
        self.assertEqual(self.calls, 5)
        self.assertEqual(self.rows(), ["old-log"])
        self.assert_session_discarded()

    def test_non_pooled_database_falls_back_to_physical_close(self):
        self.lock_db.release_error = RuntimeError("cleanup failed")
        plain_database = SimpleNamespace(
            in_transaction=self.lock_db.in_transaction,
            execute_sql=self.lock_db.execute_sql,
            close=self.lock_db.manual_close,
            connection=self.lock_db.connection,
            is_closed=self.lock_db.is_closed,
        )
        retry = load_retry(plain_database)
        with self.assertLogs(level="ERROR"):
            result = retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(result, "same-execution-log")
        self.assertEqual(self.rows(), ["same-execution-log"])
        self.assert_session_discarded()

    def test_begin_reconnect_closes_original_lock_session_without_hiding_guard_error(self):
        original_session = self.lock_db.physical_connection
        error = RuntimeError("Education execution-log connection changed before transaction")
        def persist(lock_name):
            self.assertEqual(lock_name, "education-log:ecf88dc4b82b11f1a8846d29005ec004")
            self.lock_db.swap_connection()
            raise error
        with self.assertLogs(level="WARNING"):
            with self.assertRaises(RuntimeError) as caught:
                self.retry(persist, EDUCATION, "parse")
        self.assertIs(caught.exception, error)
        self.assertEqual(self.rows(), ["old-log"])
        with self.assertRaises(sqlite3.ProgrammingError):
            original_session.execute("select 1")
        self.assertFalse(self.lock_db.lock_held)

    def test_release_reconnect_closes_original_session_and_preserves_commit(self):
        original_session = self.lock_db.physical_connection
        self.lock_db.swap_during_release = True
        with self.assertLogs(level="WARNING"):
            result = self.retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(result, "same-execution-log")
        self.assertEqual(self.rows(), ["same-execution-log"])
        with self.assertRaises(sqlite3.ProgrammingError):
            original_session.execute("select 1")
        self.assertFalse(self.lock_db.lock_held)

    def test_acquire_reconnect_tracks_session_that_actually_acquired_lock(self):
        original_unlocked_session = self.lock_db.physical_connection
        self.lock_db.swap_during_acquire = True
        result = self.retry(self.operation(0), EDUCATION, "parse")
        self.assertEqual(result, "same-execution-log")
        self.assertFalse(self.lock_db.lock_held)
        self.assertEqual(original_unlocked_session.execute("select 1").fetchone(), (1,))
        self.assertEqual(self.lock_db.physical_connection.execute("select 1").fetchone(), (1,))

    def test_successful_same_session_release_keeps_pool_connection_usable(self):
        original_session = self.lock_db.physical_connection
        self.retry(self.operation(0), EDUCATION, "parse")
        self.assertIs(self.lock_db.physical_connection, original_session)
        self.assertEqual(original_session.execute("select 1").fetchone(), (1,))
        self.assertFalse(self.lock_db.physically_closed)

    def test_original_object_reconnected_as_new_session_is_not_closed(self):
        resource = self.lock_db.physical_connection
        session_id = [101]
        original_session = SimpleNamespace(open=True, thread_id=lambda: session_id[0],
            close=resource.close, execute=resource.execute)
        self.lock_db.physical_connection = original_session
        self.lock_db.sessions.append(original_session)
        def persist(lock_name):
            self.lock_db.swap_connection()
            # The pool reused the original object for a different server session.
            session_id[0] = 202
            self.lock_db.lock_owner = None
            raise RuntimeError("Education execution-log connection changed before transaction")
        with self.assertLogs(level="WARNING"):
            with self.assertRaises(RuntimeError):
                self.retry(persist, EDUCATION, "parse")
        self.assertEqual(original_session.execute("select 1").fetchone(), (1,))
        self.assertEqual(self.rows(), ["old-log"])

if __name__ == "__main__":
    unittest.main()
