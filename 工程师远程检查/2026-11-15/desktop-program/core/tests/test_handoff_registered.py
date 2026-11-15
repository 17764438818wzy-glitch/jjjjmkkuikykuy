"""A repeated handoff must verify the ledger's actual package, not only its copy."""
import json
from pathlib import Path

import pytest

from paperflow.package import PackageError, validate_package
from test_handoff import api, count, material, prepare


def damage_package(path, fault, workspace):
    if fault == 'corrupt':
        (path / 'article.redacted.md').write_text('damaged package', encoding='utf-8')
    else:
        quarantined = path.with_name(path.name + '-unavailable')
        assert path.resolve().is_relative_to(workspace.resolve())
        assert quarantined.resolve().is_relative_to(workspace.resolve())
        path.rename(quarantined)


def records(h):
    with h.ledger.store.db() as c:
        return {table: [dict(row) for row in c.execute('SELECT * FROM ' + table)]
                for table in ('delivery_packages', 'specialty_tasks', 'target_versions',
                              'package_references', 'handoff_results', 'events')}


@pytest.mark.parametrize('fault', ['corrupt', 'missing'])
@pytest.mark.parametrize('already_handed_off', [False, True])
def test_repeated_run_rejects_unavailable_registered_package(
        tmp_path, package_factory, fault, already_handed_off):
    source, task, catalog = material(package_factory)
    h = api(tmp_path)
    first = h.handoff('first', task, source, catalog, mapping_version='catalog-v1')
    second = prepare(h, source, task, catalog, thread='second')
    h.publish(second['thread_id'], second['input_fingerprint'])
    if already_handed_off:
        assert h.resume(second['thread_id'], second['input_fingerprint']) == first
    previous = records(h)
    damage_package(Path(first['package_dir']), fault, tmp_path)

    with pytest.raises(PackageError, match='registered package') as error:
        h.resume(second['thread_id'], second['input_fingerprint'])

    assert error.value.check == 'identity'  # Existing graph must not send this to content repair.
    assert records(h) == previous  # No new refs/receipts, no path rebinding or reseal.
    validate_package(Path(second['package_dir']), task, catalog, strict_seal=True)
    validate_package(source, task, catalog, strict_seal=True)


@pytest.mark.parametrize('damage_winner', [False, True])
def test_competing_first_registration_is_verified_before_confirming_replay(
        tmp_path, package_factory, monkeypatch, damage_winner):
    source, task, catalog = material(package_factory)
    h, other = api(tmp_path), api(tmp_path)
    intent = prepare(h, source, task, catalog, thread='late')
    original_register = h.ledger.register
    winner = []

    def competing_registration(*args, **kwargs):
        if not winner:
            winner.append(other.handoff('winner', task, source, catalog, mapping_version='catalog-v1'))
            if damage_winner:
                damage_package(Path(winner[0]['package_dir']), 'corrupt', tmp_path)
        return original_register(*args, **kwargs)

    monkeypatch.setattr(h.ledger, 'register', competing_registration)
    if damage_winner:
        with pytest.raises(PackageError, match='registered package'):
            h.resume(intent['thread_id'], intent['input_fingerprint'])
        assert count(h, 'handoff_results') == 1
        assert count(h, 'package_references') == 4
    else:
        assert h.resume(intent['thread_id'], intent['input_fingerprint']) == winner[0]
        assert count(h, 'handoff_results') == 2
    assert count(h, 'delivery_packages') == count(h, 'specialty_tasks') == count(h, 'events') == 1
    assert h.ledger.expectations(task['task_id'], ['120201'])['120201']['generation'] == 1


def test_registered_location_is_rechecked_after_transaction_entry(
        tmp_path, package_factory, monkeypatch):
    source, task, catalog = material(package_factory)
    h = api(tmp_path)
    first = h.handoff('first', task, source, catalog, mapping_version='catalog-v1')
    second = prepare(h, source, task, catalog, thread='second')
    original_register = h.ledger.register
    moved = []
    missing = tmp_path / 'candidate' / 'sealed' / 'unavailable'

    def changed_registration(*args, **kwargs):
        if not moved:
            moved.append(True)
            changed_receipt = dict(first, package_dir=str(missing))
            with h.ledger.store.db() as c:
                c.execute('UPDATE delivery_packages SET package_dir=?,result_json=? WHERE manifest_sha256=?',
                          (str(missing), json.dumps(changed_receipt), first['manifest_sha256']))
        return original_register(*args, **kwargs)

    monkeypatch.setattr(h.ledger, 'register', changed_registration)
    with pytest.raises(PackageError, match='registered package'):
        h.resume(second['thread_id'], second['input_fingerprint'])
    assert count(h, 'handoff_results') == 1
    assert count(h, 'package_references') == 4
