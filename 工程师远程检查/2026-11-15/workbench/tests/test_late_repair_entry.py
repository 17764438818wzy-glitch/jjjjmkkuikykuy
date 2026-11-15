import shutil
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from paperflow.handoff import PackageHandoff
from paperflow.package import load, save, sha
from test_control import manager
from test_delivery_state import delivery_case, owner, begin
from test_delivery_flow import flow, IndexedRag
from test_winjob import wait_until


def completed_repair(case, tmp_path):
    m, config, task, source, commits, material, catalog = case
    identity = commits.ledger.context(source, '120202')['identity']
    first = commits.repairs._write(lambda db: commits.repairs.request_sql(
        db, identity, 'round3_chunk', 'missing_chunks', '120202', {'missing': 'X'}))['repair_request_id']
    run = commits.repairs._write(lambda db: commits.repairs.start_sql(db, first))
    output = tmp_path / 'fixed-X'
    shutil.copytree(material, output)
    for name in ('manifest.json', 'READY.json'):
        (output / name).unlink(missing_ok=True)
    chunks = load(output / 'chunks.json'); chunks['chunks'][-1]['content'] += ' fixed X'
    save(output / 'chunks.json', chunks)
    result = PackageHandoff(m.store, config.handoff_root).handoff(
        run['thread_id'], run, output, catalog, mapping_version=sha(config.catalog_path),
        expected_targets=run['expected_targets'], repair_source=run['repair_source'])
    current = (m, config, task, result['manifest_sha256'], commits, material, catalog)
    return current, first


def report_payload(identity, source, **extra):
    return dict(identity=identity, source_manifest=source, stage='round3_chunk',
                problem_type='missing_chunks', evidence={'missing': 'Y'}, **extra)


def test_current_worker_can_register_old_package_evidence_without_changing_versions(delivery_case, tmp_path):
    m, _, task, source, commits, *_ = delivery_case
    current, first = completed_repair(delivery_case, tmp_path)
    b = flow(current, tmp_path, IndexedRag(), '120202')
    before = commits.repairs.rows('SELECT * FROM target_versions')
    result = b.report_problem('round3_chunk', 'missing_chunks', {'missing': 'Y'}, source_manifest=source)
    assert result['repair_request_id'] == first
    child = commits.repairs.get(result['followup_request_id'])
    assert child['source_manifest'] == current[3] and child['state'] == 'queued'
    assert b.report_problem('round3_chunk', 'missing_chunks', {'missing': 'Y'}, source_manifest=source) == result
    assert commits.repairs.used(task) == 2
    assert commits.repairs.rows('SELECT * FROM target_versions') == before
    assert not commits.repairs.rows('SELECT * FROM specialty_active')
    assert not m.ledger.rows("SELECT * FROM control_actions WHERE kind IN ('llm','rag_read','rag_write')")


def test_old_worker_cannot_use_evidence_reference_to_regain_write_or_report_permission(delivery_case, tmp_path):
    m, _, _, source, commits, *_ = delivery_case
    old = flow(delivery_case, tmp_path, IndexedRag(), '120202')
    completed_repair(delivery_case, tmp_path)
    with pytest.raises(ValueError, match='identity|generation'):
        old.report_problem('round3_chunk', 'missing_chunks', {'missing': 'Y'}, source_manifest=source)
    with pytest.raises(ValueError, match='identity|generation'):
        old.actions.run('rag_write', dict(operation='http', method='POST', endpoint='/datasets/paper-two/documents',
            object_manifest=source, recovery=False, effect={}), lambda cancel: pytest.fail('old write ran'))
    assert len(commits.repairs.rows('SELECT * FROM repair_requests')) == 1


@pytest.mark.parametrize('fault', ['unknown', 'other_source', 'other_specialty', 'deleting'])
def test_late_report_cannot_cross_source_specialty_or_retention_boundary(delivery_case, tmp_path, package_factory, fault):
    m, config, task, source, commits, material, catalog = delivery_case
    current, _ = completed_repair(delivery_case, tmp_path)
    if fault == 'unknown':
        source = 'f' * 64
    elif fault == 'deleting':
        commits.repairs._write(lambda db: db.execute("UPDATE delivery_packages SET lifecycle='deleting' WHERE manifest_sha256=?", (source,)))
    else:
        foreign_task = task
        if fault == 'other_source':
            raw = tmp_path / 'other.txt'; raw.write_text('another article')
            foreign_task = m.store.register_source(raw, sha(raw))
        output, _, _ = package_factory('other-package', task_id=foreign_task['task_id'], source_sha=foreign_task['source_sha256'])
        for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
            value = load(output/name); value['source_id'] = foreign_task['source_id']; save(output/name, value)
        if fault == 'other_source':
            value = load(output/'profile.json'); value['routing_candidates'] = [{'code':'120202'}]; save(output/'profile.json', value)
        for name in ('manifest.json','READY.json'): (output/name).unlink()
        result = PackageHandoff(m.store, config.handoff_root).handoff('other-input', dict(foreign_task, revision=3),
            output, catalog, mapping_version=sha(config.catalog_path),
            expected_targets=commits.repairs.expectations(foreign_task['task_id'],
                ['120202'] if fault=='other_source' else ['120201']))
        source = result['manifest_sha256']
    b = flow(current, tmp_path, IndexedRag(), '120202')
    before = commits.repairs.rows('SELECT * FROM repair_reports')
    with pytest.raises(ValueError, match='source|specialty|retained'):
        b.report_problem('round3_chunk', 'missing_chunks', {'missing':'Y'}, source_manifest=source)
    assert commits.repairs.rows('SELECT * FROM repair_reports') == before
    assert commits.repairs.used(task) == 1


def test_late_report_blocked_by_pause_during_database_wait(delivery_case, tmp_path):
    m, _, task, source, commits, *_ = delivery_case
    current, _ = completed_repair(delivery_case, tmp_path)
    ex, identity = owner(current, tmp_path, '120202')
    begin(m, ex, identity, operation='late_repair_report')
    descriptor = commits.stage(ex.execution_id, report_payload(identity, source))
    with m.store.db() as db, ThreadPoolExecutor(1) as pool:
        db.execute('BEGIN IMMEDIATE')
        pending = pool.submit(commits.handle, ex.execution_id, 'late_repair_report', descriptor)
        time.sleep(.06); assert not pending.done()
        m.pause('late report waiting'); db.commit()
        with pytest.raises(ValueError): pending.result(timeout=5)
    assert commits.repairs.used(task) == 1
    assert not commits.repairs.rows('SELECT * FROM repair_report_followups')


@pytest.mark.parametrize('change', ['expected', 'retention'])
def test_late_report_rechecks_identity_and_source_in_final_transaction(delivery_case, tmp_path, change):
    m, _, task, source, commits, *_ = delivery_case
    current, _ = completed_repair(delivery_case, tmp_path)
    ex, identity = owner(current, tmp_path, '120202')
    begin(m, ex, identity, operation='late_repair_report')
    descriptor = commits.stage(ex.execution_id, report_payload(identity, source))
    if change == 'expected':
        commits.repairs._write(lambda db: db.execute(
            "UPDATE target_versions SET generation=generation+1 WHERE task_id=? AND specialty_code='120202'", (task['task_id'],)))
    else:
        commits.repairs._write(lambda db: db.execute("UPDATE delivery_packages SET lifecycle='deleting' WHERE manifest_sha256=?", (source,)))
    with pytest.raises(ValueError, match='identity|retained'):
        commits.handle(ex.execution_id, 'late_repair_report', descriptor)
    assert commits.repairs.used(task) == 1
    assert not commits.repairs.rows('SELECT * FROM repair_report_followups')


def test_explicit_report_only_launcher_uses_real_ipc_and_no_rag_acceptance(delivery_case, tmp_path):
    from paperflow.delivery_worker import launch_late_report
    m, config, task, source, commits, *_ = delivery_case
    current, first = completed_repair(delivery_case, tmp_path)
    assert not config.delivery_acceptance_report
    before = commits.repairs.rows('SELECT * FROM target_versions')
    # No server is listening on the configured endpoint; the launcher must stay local.
    receipts = []
    for _ in range(2):
        ex = launch_late_report(m, config, source, '120202', 'round3_chunk', 'missing_chunks', {'missing':'Y'})
        wait_until(lambda: ex.exit_saved, seconds=20)
        receipts.append(load(config.root/'delivery-results'/(ex.execution_id+'.json')))
    assert all(receipt['state']=='reported' for receipt in receipts), receipts
    assert receipts[0]['report']==receipts[1]['report']
    assert receipts[0]['report']['repair_request_id']==first
    assert receipts[0]['source_manifest']==source
    assert receipts[0]['identity']['manifest_sha256']==current[3]
    assert commits.repairs.used(task)==2
    assert commits.repairs.rows('SELECT * FROM target_versions')==before
    assert not m.ledger.rows("SELECT * FROM control_actions WHERE kind IN ('rag_read','rag_write','llm')")


def test_paused_report_launcher_cannot_register_or_charge(delivery_case, tmp_path):
    from paperflow.delivery_worker import launch_late_report
    m, config, task, source, commits, *_ = delivery_case
    completed_repair(delivery_case, tmp_path)
    m.pause('paused entry')
    with pytest.raises(ValueError):
        launch_late_report(m, config, source, '120202', 'round3_chunk', 'missing_chunks', {'missing':'Y'})
    assert commits.repairs.used(task)==1
    assert not commits.repairs.rows('SELECT * FROM repair_report_followups')
