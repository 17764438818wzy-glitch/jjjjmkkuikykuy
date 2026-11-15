import json

import pytest

from test_repair_store import repair_case
from test_repair_review import publish_repair


def chain(case, tmp_path, *, finish_child=True):
    _, _, _, ledger, identity = case
    def report(code, missing):
        return ledger._write(lambda db: ledger.request_sql(
            db, identity, 'round3_chunk', 'missing_chunks', code, {'missing': missing}))
    first = report('120201', 'X')['repair_request_id']
    run = ledger._write(lambda db: ledger.start_sql(db, first))
    publish_repair(case, tmp_path, run, 'fixed-X')
    child = report('120202', 'Y')['followup_request_id']
    child_run = ledger._write(lambda db: ledger.start_sql(db, child))
    result = publish_repair(case, tmp_path, child_run, 'fixed-Y') if finish_child else None
    return report, first, child, child_run, result


def test_completed_followup_covers_same_evidence_from_another_specialty(repair_case, tmp_path):
    _, _, task, ledger, _ = repair_case
    report, first, child, _, fixed = chain(repair_case, tmp_path)
    assert report('120203', 'Y')['followup_request_id'] == child
    assert ledger.used(task) == 2
    links = ledger.rows('SELECT * FROM repair_report_followups')
    assert len(links) == 2
    assert all(row['resolved_manifest'] == fixed['manifest_sha256'] for row in links)
    assert {row['outcome'] for row in ledger.rows(
        'SELECT * FROM repair_targets WHERE repair_request_id=?', (first,))} == {'followup_registered'}


@pytest.mark.parametrize('replay', ['report', 'complete'])
def test_existing_stuck_link_can_settle_without_new_budget(repair_case, tmp_path, replay):
    _, _, task, ledger, _ = repair_case
    report, first, child, run, fixed = chain(repair_case, tmp_path)
    # Persisted state left by the previous implementation after a completed child.
    ledger._write(lambda db: (
        db.execute('UPDATE repair_report_followups SET resolved_manifest=NULL'),
        db.execute("UPDATE repair_targets SET outcome='reports_pending' WHERE repair_request_id=?", (first,)),
        db.execute("UPDATE repair_targets SET outcome='dependency_pending' WHERE repair_request_id=? AND specialty_code='120201'", (first,))))
    if replay == 'report':
        report('120202', 'Y')
    else:
        ledger._write(lambda db: ledger.complete_sql(db, {
            'repair_source': run['repair_source'], 'identity': {'task_id': run['task_id']}}, fixed))
    assert all(row['resolved_manifest'] == fixed['manifest_sha256']
               for row in ledger.rows('SELECT * FROM repair_report_followups'))
    outcomes = {row['specialty_code']: row['outcome'] for row in ledger.rows(
        'SELECT * FROM repair_targets WHERE repair_request_id=?', (first,))}
    assert outcomes == {'120201': 'dependency_pending', '120202': 'followup_registered', '120203': 'followup_registered'}
    assert ledger.used(task) == 2


@pytest.mark.parametrize('late_when', ['running', 'complete'])
def test_uncovered_new_evidence_stays_pending_when_child_completes(repair_case, tmp_path, late_when):
    _, _, task, ledger, _ = repair_case
    report, first, child, run, _ = chain(repair_case, tmp_path, finish_child=late_when == 'complete')
    report('120203', 'Z')
    if late_when == 'running':
        publish_repair(repair_case, tmp_path, run, 'fixed-Y')
    links = ledger.rows('SELECT * FROM repair_report_followups WHERE repair_request_id=?', (first,))
    assert next(row for row in links if json.loads(row['evidence']) == {'missing': 'Y'})['resolved_manifest']
    assert next(row for row in links if json.loads(row['evidence']) == {'missing': 'Z'})['resolved_manifest'] is None
    assert ledger.rows("SELECT 1 FROM repair_requests WHERE state='held_budget'")
    assert {row['outcome'] for row in ledger.rows(
        'SELECT * FROM repair_targets WHERE repair_request_id=?', (first,))} == {'reports_pending'}
    assert ledger.used(task) == 2
