"""Stage 1 uses real files/SQLite; never constructs an execution service."""
import json
from pathlib import Path
import sqlite3

import pytest

from paperflow.package import load, save, seal_package, sha
from paperflow.store import Store


def api(tmp_path):
    from paperflow.handoff import PackageHandoff
    return PackageHandoff(Store(tmp_path/'ledger.sqlite3'), tmp_path/'candidate')


def material(package_factory, name='r001', codes=('120201',), sealed=True):
    root, task, catalog = package_factory(name)
    task['revision'] = int(name[1:])
    profile = load(root/'profile.json')
    profile['routing_candidates'] = [dict(code=code) for code in codes]
    save(root/'profile.json', profile)
    seal_package(root, task, task['revision'])
    if not sealed:
        (root/'manifest.json').unlink()
        (root/'READY.json').unlink()
    return root, task, catalog


def prepare(h, root, task, catalog, thread='run-1', **kw):
    return h.prepare(thread, task, root, catalog, mapping_version='catalog-v1', **kw)


def count(h, table):
    with h.ledger.store.db() as c:
        return c.execute(f'SELECT count(*) FROM {table}').fetchone()[0]


def test_atomic_handoff_records_all_targets_and_preserves_existing_seal(tmp_path, package_factory):
    root, task, catalog = material(package_factory, codes=('120201', '120202', 'unknown'))
    catalog['majors'].append(dict(code='120202', paper_dataset_id='ds-b'))
    original = (root/'manifest.json').read_bytes()
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    assert count(h, 'delivery_packages') == count(h, 'specialty_tasks') == 0
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert result['status'] == 'registered'
    assert (Path(result['package_dir'])/'manifest.json').read_bytes() == original
    rows = h.ledger.deliveries(result['manifest_sha256'])
    assert [(r['specialty_code'], r['status']) for r in rows] == [
        ('120201', 'pending'), ('120202', 'pending'), ('unknown', 'mapping_pending')]
    assert rows[0]['mapping']['paper_dataset_id'] == 'paper-ds'
    assert rows[1]['mapping']['paper_dataset_id'] == 'ds-b'
    assert all(r['mapping_version'] == 'catalog-v1' for r in rows)
    assert count(h, 'package_references') >= 4
    assert count(h, 'handoff_results') == 1
    assert count(h, 'events') == 1
    assert count(h, 'tasks') == 0  # Stage 1 did not create/run old graph tasks.


def test_unsealed_intent_pins_revision_timestamp_and_deterministic_path(tmp_path, package_factory):
    root, task, catalog = material(package_factory, sealed=False)
    h = api(tmp_path)
    a = prepare(h, root, task, catalog, thread='unsafe/../运行')
    b = prepare(h, root, dict(task, revision=9), catalog, thread='unsafe/../运行')
    assert a == b
    result = h.resume(a['thread_id'], a['input_fingerprint'])
    path = Path(result['package_dir'])
    assert path.parent.parent == (tmp_path/'candidate'/'sealed').resolve()
    assert path.name == a['input_fingerprint'] and len(path.parent.name) == 64
    manifest = load(path/'manifest.json')
    assert manifest['revision'] == 'r001'
    assert manifest['created_at'] == a['created_at']
    assert h.resume(a['thread_id'], a['input_fingerprint']) == result


def test_replay_after_later_version_does_not_roll_back_desired_generation(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    first = prepare(h, root, task, catalog)
    old = h.resume(first['thread_id'], first['input_fingerprint'])
    expected = h.ledger.expectations(task['task_id'], ['120201'])
    newer, newtask, _ = material(package_factory, 'r002')
    second = prepare(h, newer, newtask, catalog, thread='run-2', expected_targets=expected)
    new = h.resume(second['thread_id'], second['input_fingerprint'])
    assert h.resume(first['thread_id'], first['input_fingerprint']) == old
    duplicate = prepare(h, root, task, catalog, thread='another-run')
    assert h.resume(duplicate['thread_id'], duplicate['input_fingerprint']) == old
    assert h.ledger.expectations(task['task_id'], ['120201']) == {
        '120201': dict(manifest_sha256=new['manifest_sha256'], generation=2)}
    assert count(h, 'delivery_packages') == count(h, 'specialty_tasks') == 2
    assert count(h, 'events') == 2


def test_late_prepare_cannot_treat_latest_desired_as_its_own_precondition(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    one = prepare(h, root, task, catalog)
    initial = h.resume(one['thread_id'], one['input_fingerprint'])
    later, task2, _ = material(package_factory, 'r002')
    two = prepare(h, later, task2, catalog, thread='late')
    late = h.resume(two['thread_id'], two['input_fingerprint'])
    assert h.ledger.deliveries(late['manifest_sha256'])[0]['status'] == 'superseded'
    assert h.ledger.expectations(task['task_id'], ['120201'])['120201'] == dict(
        manifest_sha256=initial['manifest_sha256'], generation=1)


def test_transaction_failure_rolls_back_package_targets_references_and_event(tmp_path, package_factory):
    root, task, catalog = material(package_factory, codes=('120201', '120202'))
    catalog['majors'].append(dict(code='120202', paper_dataset_id='ds-b'))
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    with h.ledger.store.db() as c:
        c.execute("CREATE TRIGGER injected_failure BEFORE INSERT ON specialty_tasks "
                  "WHEN NEW.specialty_code='120202' BEGIN SELECT RAISE(ABORT,'injected failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected failure'):
        h.resume(intent['thread_id'], intent['input_fingerprint'])
    digest = sha(Path(intent['package_dir'])/'manifest.json')
    for table in ('delivery_packages', 'specialty_tasks', 'target_versions',
                  'package_references', 'handoff_results', 'events'):
        assert count(h, table) == 0
    with h.ledger.store.db() as c:
        c.execute('DROP TRIGGER injected_failure')
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert result['manifest_sha256'] == digest
    assert count(h, 'specialty_tasks') == 2


def test_published_package_recovers_when_input_material_is_gone(tmp_path, package_factory):
    root, task, catalog = material(package_factory, sealed=False)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    h.publish(intent['thread_id'], intent['input_fingerprint'])
    original = (Path(intent['package_dir'])/'manifest.json').read_bytes()
    root.rename(root.with_name('input-no-longer-at-original-path'))
    assert count(h, 'specialty_tasks') == 0
    reopened = api(tmp_path)
    result = reopened.resume(intent['thread_id'], intent['input_fingerprint'])
    assert (Path(result['package_dir'])/'manifest.json').read_bytes() == original
    assert count(reopened, 'specialty_tasks') == 1


def test_same_manifest_conflicting_target_mapping_is_rejected(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    catalog['majors'][0]['paper_dataset_id'] = 'wrong-rebound-dataset'
    conflict = prepare(h, root, task, catalog, thread='conflicting-map')
    with pytest.raises(ValueError, match='conflict'):
        h.resume(conflict['thread_id'], conflict['input_fingerprint'])
    assert h.ledger.deliveries(result['manifest_sha256'])[0]['mapping']['paper_dataset_id'] == 'paper-ds'
    assert count(h, 'specialty_tasks') == count(h, 'events') == 1


def test_frozen_candidate_is_registered_but_not_set_as_desired(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    with h.ledger.store.db() as c:
        c.execute('INSERT INTO package_freezes VALUES(?,?,?)', (sha(root/'manifest.json'), '*', 'known bad'))
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert h.ledger.deliveries(result['manifest_sha256'])[0]['status'] == 'frozen'
    assert h.ledger.expectations(task['task_id'], ['120201']) == {
        '120201': dict(manifest_sha256=None, generation=0)}


def test_deletion_intent_prevents_registration(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    h.publish(intent['thread_id'], intent['input_fingerprint'])
    with h.ledger.store.db() as c:
        c.execute("UPDATE publication_intents SET lifecycle='deleting'")
    with pytest.raises(ValueError, match='delet'):
        h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert count(h, 'delivery_packages') == 0


def test_changed_published_bytes_are_not_resealed_or_registered(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    h.publish(intent['thread_id'], intent['input_fingerprint'])
    final = Path(intent['package_dir'])
    (final/'article.redacted.md').write_text('changed', encoding='utf-8')
    with pytest.raises(ValueError, match='hash|size'):
        h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert count(h, 'delivery_packages') == 0
    assert (final/'article.redacted.md').read_text() == 'changed'


def test_same_intent_changed_input_is_not_published(tmp_path, package_factory):
    root, task, catalog = material(package_factory, sealed=False)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    (root/'article.redacted.md').write_text('changed', encoding='utf-8')
    with pytest.raises(ValueError, match='input'):
        h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert not Path(intent['package_dir']).exists()


def test_missing_specialty_is_retained_without_fabricating_a_delivery(tmp_path, package_factory):
    root, task, catalog = material(package_factory, codes=())
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert result['status'] == 'awaiting_specialty'
    assert result['routing_issues'] == [dict(code=None, reason='no specialty supplied')]
    assert count(h, 'delivery_packages') == 1
    assert count(h, 'specialty_tasks') == 0


def test_source_identity_conflict_is_rejected_before_publish(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    with pytest.raises(ValueError, match='source_id'):
        prepare(h, root, dict(task, source_id='not-source1'), catalog)
    assert count(h, 'publication_intents') == 0


def test_control_and_repair_are_not_silently_enabled(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    with pytest.raises(ValueError, match='repair'):
        prepare(h, root, task, catalog, repair_source={'repair_request_id': 'future'})
    assert count(h, 'publication_intents') == 0
    from paperflow.handoff import PackageHandoff
    with pytest.raises(ValueError, match='Updated manuscript'):
        PackageHandoff(h.ledger.store, tmp_path/'Updated manuscript'/'candidate')


def test_connections_have_foreign_keys_and_bounded_whole_transaction_retry(tmp_path):
    h = api(tmp_path)
    attempts = []
    def work(c):
        assert c.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        assert c.execute('PRAGMA synchronous').fetchone()[0] == 2
        assert c.execute('PRAGMA foreign_keys').fetchone()[0] == 1
        assert c.execute('PRAGMA busy_timeout').fetchone()[0] <= 1000
        c.execute("INSERT INTO events(at,kind,data) VALUES(0,'retry','{}')")
        attempts.append(1)
        if len(attempts) == 1:
            error = sqlite3.OperationalError('injected snapshot busy')
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY_SNAPSHOT
            raise error
        return 'committed'
    assert h.ledger._write(work) == 'committed'
    assert len(attempts) == 2 and count(h, 'events') == 1


def test_image_with_implicit_storage_retains_missing_image_mapping(tmp_path, package_factory):
    root, task, catalog = material(package_factory)
    picture = root / 'assets' / 'chart.png'
    picture.parent.mkdir()
    picture.write_bytes(b'synthetic image bytes for file identity validation')
    assets = load(root / 'assets.json')
    asset = assets['items'][0]
    asset.pop('storage')
    asset.update(file='assets/chart.png', sha256=sha(picture))
    save(root / 'assets.json', assets)
    seal_package(root, task, 1)
    del catalog['majors'][0]['image_dataset_id']
    h = api(tmp_path)
    result = h.handoff('image-run', task, root, catalog, mapping_version='missing-image-map')
    assert result['routing_issues'] == [dict(code='120201', reason='specialty mapping incomplete')]
    assert h.ledger.deliveries(result['manifest_sha256'])[0]['status'] == 'mapping_pending'


@pytest.mark.parametrize('change', ['changed', 'missing'])
def test_all_manifest_files_remain_immutable_after_publish(tmp_path, package_factory, change):
    root, task, catalog = material(package_factory)
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    destination = h.publish(intent['thread_id'], intent['input_fingerprint'])
    auxiliary = destination / 'corrections.json'
    if change == 'changed':
        auxiliary.write_text('{"changed":true}', encoding='utf-8')
    else:
        auxiliary.unlink()
    with pytest.raises(ValueError, match='size|hash|missing'):
        h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert count(h, 'delivery_packages') == count(h, 'specialty_tasks') == 0


def test_existing_seal_pins_its_revision_instead_of_callers_default(tmp_path, package_factory):
    root, task, catalog = material(package_factory, 'r002')
    del task['revision']
    h = api(tmp_path)
    intent = prepare(h, root, task, catalog)
    assert intent['revision'] == 2
    result = h.resume(intent['thread_id'], intent['input_fingerprint'])
    assert load(Path(result['package_dir']) / 'manifest.json')['revision'] == 'r002'
