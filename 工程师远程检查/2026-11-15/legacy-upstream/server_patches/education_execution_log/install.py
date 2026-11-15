"""Hash-gated repair of the deployed education execution-log transaction.

Only the named education dataset's parse logs use the lock and bounded retry.
Install after the existing runtime installers; rerunning is idempotent.
"""
import argparse
import ast
import hashlib
import os
from pathlib import Path
import tempfile

ORIGINAL = "d36a29b81db84b1ee3fc188d257c42656257d9cbe224d49f7e63a6242e1c51be"
PREVIOUS = "c771a25dfb0d5ac4af058eb5b5110296ea6b653d28b33d181418e4f57910d975"
PATCHED = "c2181d8a6344cd124c11975d595d800c9cc437103a6b26f567b77929fcb4d82a"
PREVIOUS_PATCHED = "d6e2ddf607ac7ceef479b1b06acbbdf6c675bd53f8b0c59a770e22b4b23f7450"

helper='''def _persist_education_log_with_retry(persist, kb_id, task_type):
    """Retry only a fully rolled-back education log transaction, never parsing."""
    if (kb_id not in ("ecf88dc4b82b11f1a8846d29005ec004",
                      "3e8ef88cbc4e11f1a8e165c0e34ea629")
            or task_type != PipelineTaskType.PARSE or DB.in_transaction()):
        return persist()
    lock_name = "education-log:" + kb_id
    def discard_connection():
        try:
            # Pooled DB.close only returns a session to the pool; manual_close
            # discards it, which releases a possibly retained named lock.
            getattr(DB, "manual_close", DB.close)()
        except Exception:
            logging.exception("Could not discard education execution-log connection")
    try:
        acquired = DB.execute_sql("SELECT GET_LOCK(%s, %s)", (lock_name, 15)).fetchone()
        lock_connection = DB.connection() if acquired and acquired[0] == 1 else None
        session_id = getattr(lock_connection, "thread_id", None)
        lock_session_id = session_id() if callable(session_id) else None
    except Exception:
        discard_connection()
        raise
    if not acquired or acquired[0] != 1:
        raise RuntimeError("Education execution-log writer is busy")
    try:
        for attempt in range(5):
            try:
                return persist(lock_name)
            except OperationalError as exc:
                if not exc.args or exc.args[0] != 1213 or attempt == 4:
                    raise
                logging.warning("Education log transaction deadlocked; retry %s/4", attempt + 1)
                # The complete atomic block has exited and rolled back before retry.
                time.sleep(random.uniform(0.05, 0.1) * (2 ** attempt))
    finally:
        try:
            released = DB.execute_sql("SELECT RELEASE_LOCK(%s)", (lock_name,)).fetchone()
            if not released or released[0] != 1:
                discard_connection()
                logging.warning("Education execution-log lock was not owned at release")
        except Exception:
            # Closing the connection releases any connection-scoped lock. Do not
            # turn an acknowledged log commit into a failed paper on cleanup.
            discard_connection()
            logging.exception("Could not release education execution-log lock")
        finally:
            try:
                # BEGIN or RELEASE_LOCK retries may have rebound DB to another
                # pooled session. Discard the original physical lock owner too.
                if (DB.is_closed() or DB.connection() is not lock_connection):
                    if (getattr(lock_connection, "open", True)
                            and (lock_session_id is None or lock_connection.thread_id() == lock_session_id)):
                        lock_connection.close()
            except Exception:
                logging.exception("Could not discard original education execution-log connection")


'''

def transform(original):
    patched = original.replace('import re\n', 'import re\nimport random\nimport time\n', 1)
    patched = patched.replace('from peewee import fn\n', 'from peewee import fn, OperationalError\n', 1)
    patched = patched.replace('class PipelineOperationLogService(', helper + 'class PipelineOperationLogService(', 1)
    start = patched.index('        with DB.atomic():', patched.index('        log["update_date"]'))
    end = patched.index('        return obj\n', start) + len('        return obj\n')
    block = patched[start:end]
    block = block.replace('        with DB.atomic():\n', '        with DB.atomic():\n            if lock_name:\n                # RetryingPooledMySQLDatabase.begin can reconnect before Peewee\n                # pushes the transaction. Never write after losing the named lock.\n                owned = DB.execute_sql("SELECT IS_USED_LOCK(%s) = CONNECTION_ID()", (lock_name,)).fetchone()\n                if not owned or owned[0] != 1:\n                    raise RuntimeError("Education execution-log connection changed before transaction")\n', 1)
    old_prune = '                keep_ids = [m.id for m in cls.model.select(cls.model.id).where(cls.model.kb_id == document.kb_id).order_by(cls.model.create_time.desc()).limit(limit)]\n\n                deleted = cls.model.delete().where(cls.model.kb_id == document.kb_id, cls.model.id.not_in(keep_ids)).execute()'
    new_prune = '                if lock_name:\n                    # Delete only the expired primary keys. NOT IN can scan and\n                    # lock thousands of unrelated, multi-megabyte log rows.\n                    expired_ids = [m.id for m in cls.model.select(cls.model.id)\n                        .where(cls.model.kb_id == document.kb_id)\n                        .order_by(cls.model.create_time.desc(), cls.model.id.desc())\n                        .offset(limit).limit(total - limit)]\n                    deleted = (cls.model.delete().where(cls.model.kb_id == document.kb_id,\n                        cls.model.id.in_(expired_ids)).execute() if expired_ids else 0)\n                else:\n                    keep_ids = [m.id for m in cls.model.select(cls.model.id).where(cls.model.kb_id == document.kb_id).order_by(cls.model.create_time.desc()).limit(limit)]\n                    deleted = cls.model.delete().where(cls.model.kb_id == document.kb_id, cls.model.id.not_in(keep_ids)).execute()'
    if old_prune not in block:
        raise ValueError('Prune block changed')
    block = block.replace(old_prune, new_prune, 1)
    patched = patched[:start] + '        def persist_log(lock_name=None):\n' + ''.join(('    ' + line if line.strip() else line for line in block.splitlines(True))) + '\n        return _persist_education_log_with_retry(persist_log, document.kb_id, task_type)\n' + patched[end:]
    ast.parse(patched)
    return patched


def install(target):
    current = target.read_bytes()
    digest = hashlib.sha256(current).hexdigest()
    if digest == PATCHED:
        return "already_installed"
    if digest in (PREVIOUS, PREVIOUS_PATCHED):
        current = target.with_name(target.name + ".education-log-original").read_bytes()
        digest = hashlib.sha256(current).hexdigest()
    if digest != ORIGINAL:
        raise ValueError("Execution-log source has changed; inspect before applying repair")
    updated = transform(current.decode("utf8")).encode("utf8")
    if hashlib.sha256(updated).hexdigest() != PATCHED:
        raise ValueError("Generated repair checksum mismatch")
    compile(updated, str(target), "exec")
    backup = target.with_name(target.name + ".education-log-original")
    if backup.exists() and backup.read_bytes() != current:
        raise ValueError("Existing backup differs from the verified source")
    if not backup.exists():
        backup.write_bytes(current)
    fd, temporary = tempfile.mkstemp(prefix=".education-log-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(updated)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, target.stat().st_mode)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return "installed"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=Path,
        default=Path("/ragflow/api/db/services/pipeline_operation_log_service.py"))
    args = parser.parse_args()
    print(install(args.target))
