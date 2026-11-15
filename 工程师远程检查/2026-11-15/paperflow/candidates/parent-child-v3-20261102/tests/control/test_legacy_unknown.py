"""An older uncertain first-step request still protects its source group."""

from pathlib import Path
import sqlite3

import pytest

from paperflow_v3.control.classification import check_legacy_source, first_step_modules


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_unresolved_legacy_source_is_not_sent_again(tmp_path):
    database = tmp_path / "classification" / "group-task-summaries" / "summary-index.sqlite3"
    database.parent.mkdir(parents=True)
    group = tmp_path / "source" / "paper"
    with sqlite3.connect(database) as db:
        db.executescript("""
            CREATE TABLE control_groups (task_id TEXT, seq INTEGER, group_root TEXT, source_digest TEXT);
            CREATE TABLE model_decisions (task_id TEXT, seq INTEGER, state TEXT, request_scope TEXT);
            CREATE TABLE deleted_unknown_requests (group_scope TEXT, request_scope TEXT, source_digest TEXT);
        """)
        db.execute("INSERT INTO control_groups VALUES ('old', 1, ?, 'digest')", (str(group),))
        db.execute("INSERT INTO model_decisions VALUES ('old', 1, 'unknown', NULL)")
    unknown = first_step_modules(FIRST_STEP)["control"].UnknownRequest
    with pytest.raises(unknown):
        check_legacy_source(FIRST_STEP, database.parents[1], group, "digest")
    check_legacy_source(FIRST_STEP, database.parents[1], tmp_path / "different", "other")

    with sqlite3.connect(database) as db:
        db.execute("DELETE FROM model_decisions")
        db.execute("INSERT INTO deleted_unknown_requests VALUES (?, NULL, 'digest')", ("local:" + group.as_posix(),))
    with pytest.raises(unknown):
        check_legacy_source(FIRST_STEP, database.parents[1], group, "digest")
