"""Inert, read-only adapter for authenticated server receipt observations.

The caller supplies the existing ledger projection, an actual
RemoteReceiptInspector and a trusted, bounded scope verifier. No worker,
network client, background thread, local artifact path, or database is created
here. A scope verifier must check the unique authoritative control database,
all original Prefect folder identities and frozen source bindings, plus the
configured server identity even for an empty result. Its True is meaningful
only when the actual implementation performs those checks.
"""
from contextlib import closing
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PureWindowsPath
import sqlite3
import time

from remote_runtime_adapter import RemoteFolderStageStore, RemoteReceiptInspector


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
SERVER_BASIS = 'paperflow-server-receipt-observation/1'


class Observation(dict):
    """JSON-compatible result with a private monotonic freshness witness."""
    def __init__(self, value, started, valid_for):
        super().__init__(value)
        self._server_observed_monotonic = started
        self._server_valid_for_seconds = valid_for

    def __deepcopy__(self, memo):
        result = Observation(copy.deepcopy(dict(self), memo), self._server_observed_monotonic,
                             self._server_valid_for_seconds)
        if hasattr(self, '_server_cache_order_monotonic'):
            result._server_cache_order_monotonic = self._server_cache_order_monotonic
        return result


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()


def _unavailable(reason, result_filter, page, page_size):
    results = {'available': False, 'counts': {'passed': None, 'failed': None, 'unrecognized': None},
               'partial': True, 'message': '后处理结果暂不可核验'}
    if result_filter is not None:
        results['list'] = dict(filter=result_filter, page=page, page_size=page_size,
                               total=None, has_next=False, items=[])
    return {'available': False, 'basis': 'postprocessing-registered-ledger/1', 'reason': reason,
            'current_delivery_verified': False, 'revision': None,
            'stage': {'available': False, 'counts': {'waiting_rag_upload': None,
                      'postprocessing_in_progress': None}, 'partial': True}, 'results': results}


def _rows(database, passed_items):
    """Capture metadata briefly; return original receipt JSON without opening artifacts."""
    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        folders = [dict(row) for row in db.execute(
            "SELECT q.* FROM llm_folder_queue q WHERE q.state='waiting_postprocessing' "
            'AND NOT EXISTS (SELECT 1 FROM task_deletions d WHERE d.task_id=q.task_id)')]
        rows = [dict(row) for row in db.execute(
            'SELECT c.*,p.receipt_json,p.receipt_sha256 FROM postprocessing_claims c '
            'JOIN postprocessing_receipts p ON p.claim_id=c.claim_id')]
    by_key = {}
    for row in rows:
        key = (row['task_id'], row['group_id'])
        if key in by_key:
            raise ValueError('duplicate receipt owner')
        by_key[key] = row
    triples, captured = [], []
    for item in passed_items:
        row = by_key.get((item['task_id'], item['group_id']))
        if not row or any(row.get(key) != item.get(key) for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')):
            raise ValueError('registered result and receipt identity differ')
        raw = row['receipt_json']
        if (not isinstance(raw, str) or hashlib.sha256(raw.encode('utf8')).hexdigest() != row['receipt_sha256']):
            raise ValueError('original receipt bytes changed')
        value = json.loads(raw)
        identity = {key: row[key] for key in FIELDS}
        triples.append((value, identity, row['claim_id']))
        captured.append(row['receipt_sha256'])
    folder_scope = [dict(identity={key: row[key] for key in FIELDS},
                         manifest_sha256=row['manifest_sha256'], generation=row['generation']) for row in folders]
    folder_scope.sort(key=lambda value: (value['identity']['task_id'], value['identity']['group_id']))
    if len({(row['identity']['task_id'], row['identity']['group_id']) for row in folder_scope}) != len(folder_scope):
        raise ValueError('duplicate folder identity')
    return str(path), folder_scope, triples, captured


def _all_passed(ledger_snapshot, database, revision, component_id_fn, total):
    if type(total) is not int or total < 0:
        raise ValueError('registered passing count unavailable')
    items = []
    for page in range(1, (total + 99) // 100 + 1):
        value = ledger_snapshot(database, result_filter='passed', page=page, page_size=100,
                                expected_revision=revision, component_id_fn=component_id_fn)
        if value.get('available') is not True or value.get('revision') != revision:
            raise ValueError('registered result page changed')
        listing = value['results']['list']
        if listing.get('total') != total or not isinstance(listing.get('items'), list):
            raise ValueError('registered result page incomplete')
        items.extend(listing['items'])
    if len(items) != total or len({(item['task_id'], item['group_id']) for item in items}) != total:
        raise ValueError('registered passing folders are incomplete')
    return items


def _unknown_results(value):
    value['stage']['counts']['waiting_postprocessing'] = None
    value['stage']['counts']['waiting_rag_upload'] = None
    value['stage']['counts']['postprocessing_in_progress'] = None
    value['results']['counts']['passed'] = None
    # Existing failed/unrecognized proof checks refer to the machine's files.
    # A registered failure can still be listed, but cannot be counted as verified.
    value['results']['counts']['failed'] = None
    value['results']['counts']['unrecognized'] = None
    listing = value['results'].get('list')
    if listing is not None:
        listing['total'] = None
        for item in listing.get('items', []):
            item['reason'] = (item.get('reason') or '已登记') + ' · 证据待核验'


def _require_complete_body_map(receipt, evidence):
    """Do not accept a truncated mapping of the original package references."""
    package = PureWindowsPath(receipt['body']['package_path'])
    expected = {}
    for row in receipt['artifacts']:
        path = PureWindowsPath(row['path'])
        try:
            relative = path.relative_to(package)
        except ValueError:
            continue
        name = '/'.join(relative.parts).casefold()
        if not name or name in expected:
            raise ValueError('duplicate package reference')
        expected[name] = row['sha256']
    mapped = {}
    for row in evidence['body_package']:
        name = row['relative_path'].casefold()
        if name in mapped or expected.get(name) != row['sha256']:
            raise ValueError('body package mapping changed')
        mapped[name] = row
    required_package = {'article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json',
                        'split-plan.json', 'source-map.json', 'manifest.json', 'ready.json'}
    if set(mapped) != set(expected) or not required_package <= set(mapped):
        raise ValueError('body package mapping is incomplete')
    publications = evidence.get('body_publications')
    if not isinstance(publications, list) or not publications:
        raise ValueError('body publications mapping is unavailable')
    publication_names = set()
    for row in publications:
        if not isinstance(row, dict) or not isinstance(row.get('path'), str):
            raise ValueError('invalid publication mapping')
        path = PureWindowsPath(row['path'])
        try:
            name = '/'.join(path.relative_to(package).parts).casefold()
        except ValueError as exc:
            raise ValueError('publication outside body package') from exc
        original = mapped.get(name)
        if (not original or name in publication_names or
                any(row.get(key) != original.get(key) for key in ('path', 'linux_path', 'sha256', 'bytes'))):
            raise ValueError('publication mapping changed')
        publication_names.add(name)
    # The existing server inspector already checks these exact manifest bytes
    # against the original receipt. Reuse its publication list rather than
    # duplicating the legacy program's internal-file policy here.
    manifest = json.loads(evidence['body_manifest_json'])
    declared = manifest.get('publication_files') if isinstance(manifest, dict) else None
    if (not isinstance(declared, list) or not declared or
            any(not isinstance(name, str) for name in declared) or
            len({name.casefold() for name in declared}) != len(declared) or
            publication_names != {name.casefold() for name in declared}):
        raise ValueError('body publications do not match the sealed manifest')


def snapshot(database, *, ledger_snapshot, component_id_fn, inspector, stage_store, verify_scope, probe_server=None,
             result_filter=None, page=1, page_size=20, expected_revision=None,
             max_observation_age_seconds=30):
    """Return a bounded folder observation; no verification is implied by injection alone."""
    if (not callable(ledger_snapshot) or type(max_observation_age_seconds) not in (int, float)
            or not 0 < max_observation_age_seconds <= 30):
        raise ValueError('ledger and bounded observation age are required')
    if result_filter is not None and page > 1 and expected_revision is None:
        return _unavailable('snapshot_revision_required', result_filter, page, page_size)
    started = time.monotonic()
    observed_at = datetime.now(timezone.utc).isoformat()
    base = ledger_snapshot(database, component_id_fn=component_id_fn)
    if base.get('available') is not True:
        return base
    base_revision = base['revision']
    value = ledger_snapshot(database, result_filter=result_filter, page=page, page_size=page_size,
                            expected_revision=base_revision, component_id_fn=component_id_fn)
    if value.get('available') is not True:
        return value
    value = copy.deepcopy(value)
    value['as_of'] = value['stage']['as_of'] = value['results']['as_of'] = observed_at
    original_waiting = value['stage']['counts']['waiting_rag_upload']
    original_postprocess_waiting = value['stage']['counts']['waiting_postprocessing']
    original_passed = value['results']['counts']['passed']
    _unknown_results(value)
    server_observation = {'available': False, 'basis': SERVER_BASIS, 'as_of': observed_at,
                          'source_revision': base_revision, 'waiting_rag_upload': None}
    map_digests = []
    phase = 'dependencies'
    try:
        # A duck-typed callback could assert success without validating the
        # original receipt and artifact map. Reuse the actual candidate verifier.
        if (type(inspector) is not RemoteReceiptInspector or type(stage_store) is not RemoteFolderStageStore
                or stage_store.inspect_receipt is not inspector or not callable(verify_scope)
                or Path(stage_store.database).resolve(strict=True) != Path(database).resolve(strict=True)):
            raise ValueError('trusted server inspector and scope verifier are required')
        # An empty batch has no per-receipt lookup. Check the same durable
        # intent journal and original task-gate binding even for an empty queue.
        phase = 'journal_verification'
        inspector.journal._ready(stage_store)
        if type(original_passed) is not int or original_passed > inspector.max_summary_receipts:
            raise ValueError('receipt observation exceeds the configured bound')
        phase = 'registered_scope'
        items = _all_passed(ledger_snapshot, database, base_revision, component_id_fn, original_passed)
        path, folders, triples, receipt_hashes = _rows(database, items)
        if len(folders) > 10000:
            raise ValueError('folder scope exceeds the observation bound')
        request = {'schema': 'paperflow-server-projection-scope/1', 'database': path,
                   'ledger_revision': base_revision, 'folders': folders,
                   'server_identity': copy.deepcopy(inspector.server)}
        phase = 'source_verification'
        argument = copy.deepcopy(request)
        if verify_scope(argument) is not True or argument != request:
            raise ValueError('authoritative source scope unavailable')
        if not triples:
            # inspect_many([]) performs no remote call. An explicit trusted,
            # authenticated round trip is required before reporting an empty queue.
            phase = 'server_probe'
            identity = copy.deepcopy(inspector.server)
            if not callable(probe_server) or probe_server(identity) is not True or identity != inspector.server:
                raise ValueError('empty queue server identity is not freshly observed')
        phase = 'server_receipt_verification'
        before = [inspector._request(value_, identity, claim) for value_, identity, claim in triples]
        maps = inspector.inspect_many(triples, retain_maps=True)
        if (len(maps) != len(triples) or
                [inspector._request(value_, identity, claim) for value_, identity, claim in triples] != before):
            raise ValueError('server intent changed during observation')
        phase = 'artifact_mapping'
        for evidence, triple, digest in zip(maps, triples, receipt_hashes):
            if (not isinstance(evidence, dict) or evidence.get('schema') != 'paperflow-linux-artifact-map/1'
                    or evidence.get('identity') != triple[1] or evidence.get('receipt_canonical_sha256') != digest):
                raise ValueError('server artifact mapping identity changed')
            _require_complete_body_map(triple[0], evidence)
            map_digests.append(_hash(evidence))
        phase = 'source_recheck'
        argument = copy.deepcopy(request)
        if verify_scope(argument) is not True or argument != request:
            raise ValueError('authoritative source changed during server observation')
        phase = 'journal_recheck'
        inspector.journal._ready(stage_store)
        phase = 'ledger_recheck'
        after = ledger_snapshot(database, component_id_fn=component_id_fn)
        if after.get('available') is not True or after.get('revision') != base_revision:
            raise ValueError('control ledger changed during server observation')
        phase = 'observation_age'
        elapsed = time.monotonic() - started
        if not 0 <= elapsed <= max_observation_age_seconds:
            raise ValueError('server observation expired')
        counts = value['stage']['counts']
        uncertain = (value['stage']['partial'] or counts['postprocessing_registered_inflight'] > 0
                     or counts['postprocessing_unresolved'] > 0)
        if uncertain:
            value['stage']['partial'] = True
        else:
            counts['waiting_postprocessing'] = original_postprocess_waiting
            value['stage']['counts']['waiting_rag_upload'] = original_waiting
            server_observation['waiting_rag_upload'] = original_waiting
        # Zero registered passes in a partial snapshot says nothing about the
        # unresolved folders. Display it as unknown rather than a numeric 0.
        displayed_passed = None if value['results']['partial'] and original_passed == 0 else original_passed
        value['results']['counts']['passed'] = displayed_passed
        if result_filter == 'passed':
            value['results']['list']['total'] = displayed_passed
            for item in value['results']['list']['items']:
                item['reason'] = '已登记通过 · 服务器回执已核验'
        server_observation['available'] = True
    except Exception:
        # Fixed phase names provide diagnostics without exposing paths, source
        # content, credentials, or exception messages to a page or log.
        server_observation['reason'] = phase + '_unavailable'
    value['server_observation'] = server_observation
    value['revision'] = _hash({'ledger': base_revision, 'server_available': server_observation['available'],
                               'maps': sorted(map_digests) if server_observation['available'] else []})
    server_observation['observation_revision'] = value['revision']
    if expected_revision is not None and expected_revision != value['revision']:
        return _unavailable('snapshot_changed', result_filter, page, page_size)
    return Observation(value, started, max_observation_age_seconds)
