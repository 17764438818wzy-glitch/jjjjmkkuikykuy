"""Isolated one-folder observation prototype; it is not wired to the page."""
import copy
from datetime import datetime, timezone
from pathlib import Path
import time

from remote_runtime_adapter import RemoteFolderStageStore, RemoteReceiptInspector


def registered_candidates(database, *, ledger_snapshot, component_id_fn, server_helpers):
    base = ledger_snapshot(database, component_id_fn=component_id_fn)
    if base.get('available') is not True:
        raise ValueError('registered results are unavailable')
    count = base.get('results', {}).get('counts', {}).get('passed')
    items = server_helpers._all_passed(ledger_snapshot, database, base['revision'], component_id_fn, count)
    return base, items


def observe_one(database, selected, *, ledger_snapshot, component_id_fn, inspector, stage_store,
                scope, server_helpers, max_observation_age_seconds=30):
    """Verify one currently registered pass without claiming anything about others."""
    started = time.monotonic()
    observed_at = datetime.now(timezone.utc).isoformat()
    phase = 'dependencies'
    try:
        if (not isinstance(selected, tuple) or len(selected) != 2 or any(not isinstance(x, str) or not x for x in selected)
                or type(max_observation_age_seconds) not in (int, float) or not 0 < max_observation_age_seconds <= 30
                or type(inspector) is not RemoteReceiptInspector or type(stage_store) is not RemoteFolderStageStore
                or stage_store.inspect_receipt is not inspector or not callable(getattr(scope, 'verify_selected', None))
                or Path(stage_store.database).resolve(strict=True) != Path(database).resolve(strict=True)):
            raise ValueError('a single folder and original observation dependencies are required')
        phase = 'journal'
        inspector.journal._ready(stage_store)
        phase = 'registration'
        base, items = registered_candidates(database, ledger_snapshot=ledger_snapshot,
            component_id_fn=component_id_fn, server_helpers=server_helpers)
        matched = [item for item in items if (item.get('task_id'), item.get('group_id')) == selected]
        if len(matched) != 1:
            raise ValueError('selected registered pass is unavailable')
        path, folders, triples, hashes = server_helpers._rows(database, matched)
        if len(triples) != 1 or len(hashes) != 1:
            raise ValueError('selected receipt is unavailable')
        request = {'schema': 'paperflow-server-projection-scope/1', 'database': path,
                   'ledger_revision': base['revision'], 'folders': folders,
                   'server_identity': copy.deepcopy(inspector.server)}
        phase = 'selected_source'
        argument = copy.deepcopy(request)
        if scope.verify_selected(argument, {selected}) is not True or argument != request:
            raise ValueError('selected source is unavailable')
        phase = 'selected_server_receipt'
        before = inspector._request(*triples[0])
        maps = inspector.inspect_many(triples, retain_maps=True)
        if (len(maps) != 1 or inspector._request(*triples[0]) != before
                or maps[0].get('receipt_canonical_sha256') != hashes[0]):
            raise ValueError('selected receipt or intent changed')
        server_helpers._require_complete_body_map(triples[0][0], maps[0])
        phase = 'selected_source_recheck'
        argument = copy.deepcopy(request)
        if scope.verify_selected(argument, {selected}) is not True or argument != request:
            raise ValueError('selected source changed during observation')
        inspector.journal._ready(stage_store)
        after = ledger_snapshot(database, component_id_fn=component_id_fn)
        if after.get('available') is not True or after.get('revision') != base['revision']:
            raise ValueError('registered ledger changed during observation')
        if not 0 <= time.monotonic() - started <= max_observation_age_seconds:
            raise ValueError('selected observation expired')
        item = copy.deepcopy(matched[0])
        item['reason'] = '已登记通过 · 该单服务器回执已核验'
        return {'available': True, 'scope': 'verified_registered_subset', 'global_counts_available': False,
                'item': item, 'as_of': observed_at, 'observed_monotonic': started,
                'valid_until_monotonic': started + max_observation_age_seconds,
                'ledger_revision': base['revision']}
    except Exception:
        return {'available': False, 'scope': 'verified_registered_subset', 'global_counts_available': False,
                'reason': phase + '_unavailable', 'as_of': observed_at}
