"""Isolated proof of a fail-closed document gate. Not a RAGFlow patch.

Only used against a disposable test database. A complete integration would
need every document mutation to share the gate, server operation identity,
and a transaction that includes the real SQL counters.
"""
from contextlib import contextmanager


class Blocked(RuntimeError):
    pass


class DurableGate:
    def __init__(self, connect):
        self.connect = connect

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            # Peewee's MySQL connections may use autocommit=True; make the
            # row lock last until commit regardless of the connection default.
            conn.begin()
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_test_schema(self):
        with self.transaction() as cur:
            cur.execute("CREATE TABLE docs (id VARCHAR(100) PRIMARY KEY, mode VARCHAR(20) NOT NULL, owner VARCHAR(100), version BIGINT NOT NULL DEFAULT 0) ENGINE=InnoDB")
            cur.execute("CREATE TABLE ops (doc_id VARCHAR(100) NOT NULL, op_id VARCHAR(100) NOT NULL, chunk_key VARCHAR(100) NOT NULL, digest CHAR(64) NOT NULL, state VARCHAR(20) NOT NULL, PRIMARY KEY (doc_id, op_id), UNIQUE KEY chunk_per_doc (doc_id,chunk_key)) ENGINE=InnoDB")
            cur.execute("CREATE TABLE changes (doc_id VARCHAR(100) NOT NULL, change_id VARCHAR(100) NOT NULL, action VARCHAR(20) NOT NULL, expected_version BIGINT NOT NULL, state VARCHAR(20) NOT NULL, PRIMARY KEY (doc_id,change_id)) ENGINE=InnoDB")

    @staticmethod
    def _lock_doc(cur, doc):
        # Take the row's exclusive lock during the upsert. INSERT IGNORE can
        # leave competing first writers waiting to upgrade shared locks.
        cur.execute("INSERT INTO docs (id,mode) VALUES (%s,'open') ON DUPLICATE KEY UPDATE id=docs.id", (doc,))
        cur.execute("SELECT mode,owner,version FROM docs WHERE id=%s FOR UPDATE", (doc,))
        return cur.fetchone()

    def begin(self, doc, op, *, chunk_key, payload_digest):
        """Return False for a previously recorded operation, never resend it."""
        with self.transaction() as cur:
            mode, _, _ = self._lock_doc(cur, doc)
            cur.execute("SELECT chunk_key,digest FROM ops WHERE doc_id=%s AND (op_id=%s OR chunk_key=%s)", (doc, op, chunk_key))
            rows = cur.fetchall()
            if rows:
                if len(rows) != 1 or rows[0] != (chunk_key, payload_digest):
                    raise Blocked("operation or chunk identity conflicts with its original payload")
                return False
            if mode != "open":
                raise Blocked("document is draining or closed")
            cur.execute("SELECT COUNT(*) FROM ops WHERE doc_id=%s AND state='unknown'", (doc,))
            if cur.fetchone()[0]:
                raise Blocked("document contains an unknown write")
            cur.execute("INSERT INTO ops (doc_id,op_id,chunk_key,digest,state) VALUES (%s,%s,%s,%s,'pending')",
                        (doc, op, chunk_key, payload_digest))
            return True

    def settle(self, doc, op, *, confirmed):
        """An unknown result stays blocking; this experiment cannot clear it."""
        with self.transaction() as cur:
            self._lock_doc(cur, doc)
            cur.execute("UPDATE ops SET state=%s WHERE doc_id=%s AND op_id=%s AND state='pending'",
                        ("done" if confirmed else "unknown", doc, op))
            if cur.rowcount != 1:
                raise Blocked("operation is missing or already settled")

    def request_change(self, doc, change, *, action, expected_version):
        """Drain new writes; False means at least one old write is unresolved."""
        with self.transaction() as cur:
            mode, owner, version = self._lock_doc(cur, doc)
            if action not in {"disable", "enable", "delete"}:
                raise Blocked("unknown document change")
            cur.execute("SELECT action,expected_version,state FROM changes WHERE doc_id=%s AND change_id=%s", (doc, change))
            previous = cur.fetchone()
            if previous:
                if previous[:2] != (action, expected_version):
                    raise Blocked("change identity conflicts with its original action or version")
                if previous[2] == "done":
                    return False
            elif expected_version != version:
                raise Blocked("change is based on an old document version")
            if mode == "open":
                if previous:
                    raise Blocked("unfinished change lost its owner")
                cur.execute("INSERT INTO changes (doc_id,change_id,action,expected_version,state) VALUES (%s,%s,%s,%s,'draining')",
                            (doc, change, action, expected_version))
                cur.execute("UPDATE docs SET mode='draining',owner=%s WHERE id=%s", (change, doc))
            elif mode == "changing" and owner == change:
                return False
            elif mode != "draining" or owner != change:
                raise Blocked("another change owns the document")
            cur.execute("SELECT COUNT(*) FROM ops WHERE doc_id=%s AND state IN ('pending','unknown')", (doc,))
            return cur.fetchone()[0] == 0

    def claim_change(self, doc, change):
        """Claim once; a lost response leaves the claim blocked for inspection."""
        with self.transaction() as cur:
            mode, owner, _ = self._lock_doc(cur, doc)
            if mode == "changing" and owner == change:
                return False
            if mode != "draining" or owner != change:
                raise Blocked("change does not own the document")
            cur.execute("SELECT COUNT(*) FROM ops WHERE doc_id=%s AND state IN ('pending','unknown')", (doc,))
            if cur.fetchone()[0]:
                raise Blocked("writes remain unresolved")
            cur.execute("UPDATE docs SET mode='changing' WHERE id=%s", (doc,))
            cur.execute("UPDATE changes SET state='changing' WHERE doc_id=%s AND change_id=%s AND state='draining'", (doc, change))
            if cur.rowcount != 1:
                raise Blocked("change record is missing or inconsistent")
            return True

    def finish_change(self, doc, change):
        with self.transaction() as cur:
            mode, owner, version = self._lock_doc(cur, doc)
            if mode != "changing" or owner != change:
                raise Blocked("change does not own the document")
            cur.execute("SELECT COUNT(*) FROM ops WHERE doc_id=%s AND state IN ('pending','unknown')", (doc,))
            if cur.fetchone()[0]:
                raise Blocked("writes remain unresolved")
            cur.execute("SELECT action,expected_version FROM changes WHERE doc_id=%s AND change_id=%s AND state='changing'", (doc, change))
            record = cur.fetchone()
            if not record or record[1] != version:
                raise Blocked("change record or version is inconsistent")
            cur.execute("UPDATE docs SET mode=%s,owner=NULL,version=version+1 WHERE id=%s", ("closed" if record[0] == "delete" else "open", doc))
            cur.execute("UPDATE changes SET state='done' WHERE doc_id=%s AND change_id=%s", (doc, change))
