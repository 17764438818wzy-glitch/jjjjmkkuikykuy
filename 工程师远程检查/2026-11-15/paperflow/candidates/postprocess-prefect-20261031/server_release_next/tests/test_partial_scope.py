"""Partial verification uses only synthetic folder and Prefect fixtures."""
import importlib.util
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


old = load('synthetic_partial_scope_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_scope_verifier.py')
modules, sample, scope = old.modules, old.sample, old.scope


def test_one_selected_folder_is_checked_even_when_another_handoff_is_unavailable(scope):
    sample, _, request = scope
    request['folders'], _ = old.populate_real_frozen_bindings(sample, 2)
    selected = request['folders'][0]['identity']
    all_waiting = sample.queue.list_waiting_llm()
    one = [item for item in all_waiting['items'] if item['group_id'] == selected['group_id']]
    sample.queue.list_waiting_llm = lambda: {'waiting_count': 1, 'unavailable_count': 1, 'items': one}
    source = load('synthetic_partial_scope', HERE / 'scope_verifier.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None)
    assert verifier(request) is False
    assert verifier.verify_selected(request, {(selected['task_id'], selected['group_id'])}) is True


def test_selected_scope_fails_if_its_own_material_changes(scope):
    sample, _, request = scope
    request['folders'], reading = old.populate_real_frozen_bindings(sample, 1)
    identity = request['folders'][0]['identity']
    source = load('synthetic_partial_scope_changed', HERE / 'scope_verifier.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None)
    assert verifier.verify_selected(request, {(identity['task_id'], identity['group_id'])}) is True
    Path(reading['path']).write_text('{}', encoding='utf8')
    with pytest.raises(ValueError):
        verifier.verify_selected(request, {(identity['task_id'], identity['group_id'])})
