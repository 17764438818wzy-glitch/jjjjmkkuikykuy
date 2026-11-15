"""Each synthetic registered pass is checked independently."""
import importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


fixtures = load('synthetic_partial_projection_fixtures', ROOT / 'remote_projection_next' / 'tests' / 'test_server_projection.py')


def test_individual_unavailable_version_does_not_discard_another_verified_receipt(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    first = fixtures.add_complete_pass(db, 1)
    second = fixtures.add_complete_pass(db, 2)
    inspector, calls = fixtures.make_inspector(db)
    actual_inspect = inspector._inspect_batch
    def inspect(requests):
        if requests[0]['job']['identity']['group_id'] == second['group_id']:
            raise ValueError('synthetic historical config unavailable')
        return actual_inspect(requests)
    inspector._inspect_batch = inspect
    class Scope:
        def verify_selected(self, request, keys):
            assert len(keys) == 1
            return True
    server = load('synthetic_partial_server_helpers', HERE / 'server_projection.py')
    candidate = load('synthetic_partial_projection', HERE / 'partial_projection.py')
    kwargs = dict(ledger_snapshot=fixtures.helpers.projection.snapshot,
        component_id_fn=fixtures.helpers.upstream.component_id, inspector=inspector,
        stage_store=inspector.synthetic_stage_store, scope=Scope(), server_helpers=server)
    ok = candidate.observe_one(db.path, (first['task_id'], first['group_id']), **kwargs)
    unknown = candidate.observe_one(db.path, (second['task_id'], second['group_id']), **kwargs)
    assert ok['available'] is True and ok['item']['group_id'] == first['group_id']
    assert ok['scope'] == 'verified_registered_subset' and ok['global_counts_available'] is False
    assert unknown['available'] is False
    assert ok['available'] is True and len(calls) == 1


def test_unknown_folder_does_not_trigger_server_inspection(tmp_path):
    db = fixtures.helpers.db.__wrapped__(tmp_path)
    identity = fixtures.helpers.add_folder(db, 1, job='execution_unknown', claim='unknown',
                                           outcome=('unresolved', 'outcome_unknown'), receipt=False)
    inspector, calls = fixtures.make_inspector(db)
    candidate = load('synthetic_partial_projection_unknown', HERE / 'partial_projection.py')
    server = load('synthetic_partial_server_unknown', HERE / 'server_projection.py')
    value = candidate.observe_one(db.path, (identity['task_id'], identity['group_id']),
        ledger_snapshot=fixtures.helpers.projection.snapshot, component_id_fn=fixtures.helpers.upstream.component_id,
        inspector=inspector, stage_store=inspector.synthetic_stage_store,
        scope=object(), server_helpers=server)
    assert value['available'] is False and calls == []
