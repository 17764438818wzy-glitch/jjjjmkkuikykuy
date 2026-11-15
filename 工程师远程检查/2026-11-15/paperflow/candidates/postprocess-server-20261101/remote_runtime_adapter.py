"""Candidate bridge from an existing Prefect folder stage to a remote executor.

Construction does not initialize stores, start workers, dispatch, or perform I/O
on a real folder. The caller must provide the existing verified input loader and
authenticated, independently validated transport/result operations. No LLM or
RAG calls belong in those callbacks. The result, validation and inspection
callbacks must use authenticated bounded metadata operations and MUST NOT copy
complete paper/output files back to this host. The independently implemented
business validator is still required; the remote integrity map alone is not it.
The trusted loader supplies backend.settings.path('catalog_path') separately
from inputs. Transfer receives trusted_files=[{path, sha256}] as local policy;
it must match these against independently pinned transport configuration and
must not infer authorization from manifest.files or widen allowed_roots.

The existing folder gate, task gate and Prefect slot remain authoritative;
this adapter adds no host-wide serialization or replacement folder orders.
Remote held-receipt recovery is also deliberately blocked pending an audited
reconciliation contract. This module does not provide production networking.
"""
from contextlib import closing, contextmanager
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import sqlite3
import threading
import time

import psutil

from folder_state import FolderStageStore, StageConflict as ReceiptConflict
from live_runtime import ResultsHooks
from postprocessing_results import ResultsStore, ResultConflict
from remote_coordinator import RemoteSubmissionJournal, _server_identity, process_remote_folder
from snapshot_prepare import counter_contract_from_runtime, prepare_manifest
from snapshot_wire import snapshot_sha
from stage_runtime import Coordinator, FIELDS, StageBoundHandoff, StageConflict, _identity


_OPTIONS = frozenset({'catalog', 'code_sha256', 'paperflow_root_windows',
    'business_workspace_windows', 'output_root_windows', 'runtime_template_sha256',
    'allowed_roots', 'max_json_file_bytes', 'max_total_json_bytes'})
_MAX_REMOTE_INSPECTION_BATCH = 32
_STAGE_SUMMARY_TABLES = (
    ('tasks', 'task_id'),
    ('task_deletions', 'task_id'),
    ('llm_folder_queue', 'task_id,group_id'),
    ('postprocessing_claims', 'task_id,group_id'),
    ('postprocessing_receipts', 'claim_id'),
)
_RESULT_SUMMARY_TABLES = _STAGE_SUMMARY_TABLES + (('postprocessing_results', 'task_id,group_id'),)


def _receipt_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _summary_snapshot(db, tables):
    """Fingerprint the local summary inputs within the caller's short read transaction."""
    digest = hashlib.sha256()
    for table, ordering in tables:
        digest.update(table.encode('ascii') + b'\0')
        for row in db.execute('SELECT * FROM ' + table + ' ORDER BY ' + ordering):
            encoded = repr(tuple(row)).encode('utf8', errors='surrogatepass')
            digest.update(len(encoded).to_bytes(8, 'big'))
            digest.update(encoded)
    return digest.digest()


def _require_summary_snapshot(store, tables, expected):
    with closing(store._connect(readonly=True)) as db:
        db.execute('BEGIN')
        actual = _summary_snapshot(db, tables)
    if actual != expected:
        raise ReceiptConflict('local summary inputs changed during remote inspection')
    # Even an empty summary cannot infer a safe zero from a missing or replaced
    # remote-intent journal. This is local metadata I/O, never remote I/O.
    store.inspect_receipt.journal._ready(store)


class RemoteReceiptInspector:
    """Trust boundary for authenticated *remote* receipt and artifact inspection.

    inspect_batch MUST use a pinned/authenticated server connection, independently
    read the durable server ledger and original receipt bytes, verify that the
    complete worker/cgroup has terminated, and run the reviewed Linux receipt
    inspector against the exact persisted job and frozen snapshot. It must also
    verify the actual approved business processor and its reported validation;
    artifact integrity alone is not business, privacy or RAG acceptance. It
    returns a bounded list of paperflow-authenticated-remote-result/1 objects.
    Only small metadata is returned; it must not download source/output files.
    The callback has to enforce its own bounded connection and read timeouts.

    These are requirements on an injected trusted implementation, NOT claims
    that the implementation is present or that this candidate is deployed.
    """
    def __init__(self, *, journal, server_identity, inspect_batch, max_summary_receipts,
                 inspection_batch_size=_MAX_REMOTE_INSPECTION_BATCH):
        if not isinstance(journal, RemoteSubmissionJournal) or not callable(inspect_batch):
            raise ValueError('the original journal and authenticated inspector are required')
        if type(max_summary_receipts) is not int or not 1 <= max_summary_receipts <= 10000:
            raise ValueError('an explicit bounded summary size is required')
        if (type(inspection_batch_size) is not int
                or not 1 <= inspection_batch_size <= _MAX_REMOTE_INSPECTION_BATCH):
            raise ValueError('inspection batches must not exceed the server limit')
        self.journal, self.server = journal, _server_identity(server_identity)
        self._inspect_batch, self.max_summary_receipts = inspect_batch, max_summary_receipts
        self.inspection_batch_size = inspection_batch_size
        self._local = threading.local()

    def _request(self, value, identity, claim_id):
        identity = _identity(identity)
        if (not isinstance(value, dict) or value.get('schema') != 'paperflow-postprocessing-receipt/1'
                or value.get('identity') != identity or value.get('claim_id') != claim_id
                or value.get('validation_status') != 'passed'
                or not isinstance(value.get('artifacts'), list) or not value['artifacts']):
            raise ReceiptConflict('original receipt identity or output coverage changed')
        raw = _receipt_json(value)
        with closing(self.journal._connect(readonly=True)) as db:
            row = db.execute('SELECT job_id FROM remote_start_intents WHERE claim_id=?', (claim_id,)).fetchone()
        intent = self.journal.get(row['job_id']) if row else None
        if not intent:
            raise ReceiptConflict('original remote start intent is unavailable')
        job = intent['job']
        if (intent['server_identity'] != self.server or job['identity'] != identity
                or job['claim_id'] != claim_id or job['manifest_sha256'] != value.get('manifest_sha256')):
            raise ReceiptConflict('remote job and original receipt are not bound')
        return dict(job=job, server_identity=copy.deepcopy(self.server), receipt_json=raw,
                    receipt_sha256=hashlib.sha256(raw.encode('utf8')).hexdigest())

    @staticmethod
    def _accepted(request, result):
        job, raw, digest = request['job'], request['receipt_json'], request['receipt_sha256']
        if (not isinstance(result, dict) or result.get('schema') != 'paperflow-authenticated-remote-result/1'
                or result.get('job') != job or result.get('server_identity') != request['server_identity']
                or result.get('receipt_json') != raw or result.get('receipt_sha256') != digest
                or result.get('ledger_state') != 'finished' or result.get('worker_terminated') is not True
                or result.get('business_validation') != 'processor_reported_pass'
                or result.get('business_code_sha256') != job['code_sha256']):
            raise ReceiptConflict('authenticated server result or business evidence is incomplete')
        evidence = result.get('artifact_map')
        if (not isinstance(evidence, dict) or evidence.get('schema') != 'paperflow-linux-artifact-map/1'
                or evidence.get('integrity_status') != 'passed' or evidence.get('job') != job
                or evidence.get('identity') != job['identity'] or evidence.get('receipt_canonical_sha256') != digest
                or evidence.get('rag_admission') != 'not_granted'
                or evidence.get('semantic_validation') != 'depends_on_reviewed_business_processor'
                or evidence.get('privacy_validation') != 'depends_on_reviewed_business_processor'
                or evidence.get('worker_termination') != 'caller_must_verify'
                or evidence.get('original_receipt_rewritten') is not False):
            raise ReceiptConflict('server integrity report or its limitations are unavailable')
        files, package, attachments = evidence.get('files'), evidence.get('body_package'), evidence.get('attachments')
        if not isinstance(files, list) or not files or not isinstance(package, list) or not package or not isinstance(attachments, list):
            raise ReceiptConflict('complete output mappings are unavailable')
        by_path = {}
        for row in files:
            if (not isinstance(row, dict) or not isinstance(row.get('path'), str)
                    or not PureWindowsPath(row['path']).is_absolute()
                    or '..' in PureWindowsPath(row['path']).parts
                    or not isinstance(row.get('linux_path'), str) or not PurePosixPath(row['linux_path']).is_absolute()
                    or '..' in PurePosixPath(row['linux_path']).parts
                    or not isinstance(row.get('sha256'), str) or len(row['sha256']) != 64
                    or any(c not in '0123456789abcdef' for c in row['sha256'])
                    or type(row.get('bytes')) is not int or row['bytes'] < 0):
                raise ReceiptConflict('invalid remote file mapping')
            key = str(PureWindowsPath(row['path'])).casefold()
            if key in by_path:
                raise ReceiptConflict('duplicate remote file mapping')
            by_path[key] = row
        original = json.loads(raw)
        original_refs = {}
        for ref in original['artifacts']:
            if not isinstance(ref, dict) or not isinstance(ref.get('path'), str):
                raise ReceiptConflict('invalid original artifact reference')
            mapped = by_path.get(str(PureWindowsPath(ref['path'])).casefold())
            if not mapped or mapped['sha256'] != ref.get('sha256'):
                raise ReceiptConflict('original output evidence is absent from remote inspection')
            key = str(PureWindowsPath(ref['path'])).casefold()
            if key in original_refs:
                raise ReceiptConflict('duplicate original artifact reference')
            original_refs[key] = ref
        body = original.get('body')
        if (not isinstance(body, dict) or not isinstance(body.get('package_path'), str)
                or not PureWindowsPath(body['package_path']).is_absolute()):
            raise ReceiptConflict('original body package path is invalid')
        body_path = PureWindowsPath(body['package_path'])
        expected_package = {key for key, ref in original_refs.items()
                            if PureWindowsPath(ref['path']).is_relative_to(body_path)}
        mapped_package = {key for key, row in by_path.items()
                          if PureWindowsPath(row['path']).is_relative_to(body_path)}
        names, package_paths, package_members = set(), set(), {}
        for row in package:
            if not isinstance(row, dict) or not isinstance(row.get('relative_path'), str):
                raise ReceiptConflict('invalid body package member')
            relative = row['relative_path']
            parts = relative.split('/')
            if (not relative or '\\' in relative or ':' in relative or any(p in ('', '.', '..') for p in parts)
                    or PurePosixPath(relative).is_absolute() or relative.casefold() in names):
                raise ReceiptConflict('unsafe or duplicate body package member')
            names.add(relative.casefold())
            mapped = by_path.get(str(PureWindowsPath(row.get('path', ''))).casefold())
            if (not mapped or PureWindowsPath(row.get('path', '')) != body_path.joinpath(*parts)
                    or any(mapped[k] != row.get(k) for k in ('path', 'linux_path', 'sha256', 'bytes'))
                    or (relative.casefold() == 'manifest.json' and row.get('sha256') != body.get('manifest_sha256'))):
                raise ReceiptConflict('body package member mapping changed')
            package_paths.add(str(PureWindowsPath(row['path'])).casefold())
            package_members[relative.casefold()] = row
        required = {'article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json',
                    'split-plan.json', 'source-map.json', 'manifest.json', 'ready.json'}
        if not required <= names or package_paths != expected_package or mapped_package != expected_package:
            raise ReceiptConflict('complete mandatory body package and original receipt coverage are required')

        def seal(field, relative):
            text = evidence.get(field)
            if not isinstance(text, str):
                raise ReceiptConflict('original body manifest and READY metadata are required')
            try:
                encoded = text.encode('utf8')
                member = package_members[relative]
                if (len(encoded) > 4 * 1024 * 1024 or len(encoded) != member['bytes']
                        or hashlib.sha256(encoded).hexdigest() != member['sha256']):
                    raise ReceiptConflict('body seal metadata does not match original receipt bytes')
                value = json.loads(encoded)
            except (ValueError, UnicodeError) as exc:
                raise ReceiptConflict('invalid original body seal metadata') from exc
            if not isinstance(value, dict):
                raise ReceiptConflict('body seal metadata must be an object')
            return value

        manifest = seal('body_manifest_json', 'manifest.json')
        ready = seal('body_ready_json', 'ready.json')
        if (manifest.get('schema') != 'paperflow/1.0' or ready.get('status') != 'ready'
                or ready.get('manifest') != 'manifest.json' or ready.get('manifest_sha256') != body['manifest_sha256']
                or any(not isinstance(manifest.get(k), str) or not manifest[k] or ready.get(k) != manifest[k]
                       for k in ('task_id', 'source_id', 'source_sha256'))):
            raise ReceiptConflict('original manifest and READY identity or seal binding changed')

        def member_name(value):
            if (not isinstance(value, str) or not value or '\\' in value or ':' in value
                    or any(p in ('', '.', '..') for p in value.split('/'))
                    or PurePosixPath(value).is_absolute()):
                raise ReceiptConflict('unsafe manifest or publication member')
            return value.casefold()

        entries = manifest.get('files')
        if not isinstance(entries, list) or not entries:
            raise ReceiptConflict('original manifest member list is unavailable')
        declared = {}
        for entry in entries:
            if not isinstance(entry, dict):
                raise ReceiptConflict('invalid original manifest member')
            key = member_name(entry.get('path'))
            if key in declared or key in {'manifest.json', 'ready.json'}:
                raise ReceiptConflict('duplicate or self-referential manifest member')
            mapped = package_members.get(key)
            if (not mapped or type(entry.get('bytes')) is not int or entry['bytes'] < 0
                    or entry.get('bytes') != mapped['bytes'] or entry.get('sha256') != mapped['sha256']):
                raise ReceiptConflict('original manifest member hash/size mapping changed')
            declared[key] = entry
        if set(declared) != names - {'manifest.json', 'ready.json'}:
            raise ReceiptConflict('manifest, original receipt and body package member sets differ')

        publications = manifest.get('publication_files')
        mapped_publications = evidence.get('body_publications')
        if (not isinstance(publications, list) or not publications or not isinstance(mapped_publications, list)
                or len(publications) != len(mapped_publications)):
            raise ReceiptConflict('complete original publication mappings are required')
        published = set()
        for relative, row in zip(publications, mapped_publications):
            key = member_name(relative)
            if key in published or key not in declared or relative != declared[key]['path']:
                raise ReceiptConflict('duplicate or undeclared original publication member')
            published.add(key)
            member = package_members[key]
            if (not isinstance(row, dict) or any(row.get(k) != member[k]
                                               for k in ('path', 'linux_path', 'sha256', 'bytes'))):
                raise ReceiptConflict('publication does not match the original manifest/receipt member')
        expected = original.get('attachments')
        if (not isinstance(expected, list) or len(expected) != len(attachments)
                or any(not isinstance(a, dict) or not isinstance(a.get('attachment_id'), str)
                       or not a['attachment_id'] for a in [*expected, *attachments])):
            raise ReceiptConflict('attachment mapping coverage changed')
        deliveries = {a['attachment_id']: a for a in expected}
        if (len(deliveries) != len(expected) or len({a['attachment_id'] for a in attachments}) != len(attachments)
                or {a['attachment_id'] for a in attachments} != set(deliveries)):
            raise ReceiptConflict('duplicate or missing attachment mapping')
        for attachment in attachments:
            delivery = deliveries[attachment['attachment_id']]
            for key, path_key, hash_key in (('source', 'source_path', 'source_sha256'),
                                             ('markdown', 'md_path', 'md_sha256')):
                row = attachment.get(key)
                if not isinstance(row, dict) or not isinstance(delivery.get(path_key), str):
                    raise ReceiptConflict('attachment source or markdown mapping is unavailable')
                mapped = by_path.get(str(PureWindowsPath(delivery[path_key])).casefold())
                if (not mapped or row.get('path') != delivery[path_key] or row.get('sha256') != delivery.get(hash_key)
                        or any(mapped[k] != row.get(k) for k in ('path', 'linux_path', 'sha256', 'bytes'))):
                    raise ReceiptConflict('attachment source or markdown mapping changed')
        return copy.deepcopy(evidence)

    def inspect_many(self, triples, *, retain_maps=True):
        requests = [self._request(value, identity, claim_id) for value, identity, claim_id in triples]
        return self._inspect_requests(requests, retain_maps=retain_maps)

    def _inspect_requests(self, requests, *, retain_maps):
        if not requests:
            return []
        accepted = []
        for start in range(0, len(requests), self.inspection_batch_size):
            batch = requests[start:start + self.inspection_batch_size]
            argument = copy.deepcopy(batch)
            results = self._inspect_batch(argument)
            if argument != batch or not isinstance(results, list) or len(results) != len(batch):
                raise ReceiptConflict('authenticated inspection changed or omitted a request')
            for request, result in zip(batch, results):
                evidence = self._accepted(request, result)
                accepted.append(evidence if retain_maps else True)
        return accepted

    def __call__(self, value, identity, claim_id):
        key = (_receipt_json(value), _receipt_json(identity), claim_id)
        prefetched = getattr(self._local, 'prefetched', None)
        if prefetched is not None:
            if key not in prefetched:
                raise ReceiptConflict('summary changed after its unlocked inspection')
            return copy.deepcopy(prefetched[key])
        return self.inspect_many([(value, identity, claim_id)])[0]

    @contextmanager
    def prefetched(self, rows, *, recheck_journal=False, accepted_evidence=None):
        triples = []
        if len(rows) > self.max_summary_receipts or getattr(self._local, 'prefetched', None) is not None:
            raise ReceiptConflict('summary inspection is too large or nested')
        for row in rows:
            identity = {key: row[key] for key in FIELDS}
            raw, digest = row['receipt_json'], row['receipt_sha256']
            if not isinstance(raw, str) or hashlib.sha256(raw.encode('utf8')).hexdigest() != digest:
                raise ReceiptConflict('saved receipt bytes changed')
            value = json.loads(raw)
            if _receipt_json(value) != raw:
                raise ReceiptConflict('saved original receipt encoding changed')
            triples.append((value, identity, row['claim_id']))
        # All batches must complete before any cache becomes visible. Summary
        # needs only proof of success, not a retained copy of every artifact map.
        requests = [self._request(value, identity, claim_id) for value, identity, claim_id in triples]
        if accepted_evidence is None:
            values = self._inspect_requests(requests, retain_maps=False)
        else:
            if (len(requests) != 1 or any(accepted_evidence.get(key) != requests[0][key]
                    for key in ('job', 'server_identity', 'receipt_json', 'receipt_sha256'))):
                raise ReceiptConflict('accepted result does not match the original receipt')
            values = [True]

        def require_journal_bindings():
            current = [self._request(value, identity, claim_id) for value, identity, claim_id in triples]
            if current != requests:
                raise ReceiptConflict('local remote-intent binding changed during summary')

        if recheck_journal:
            require_journal_bindings()
        cache = {(_receipt_json(value), _receipt_json(identity), claim): result
                 for (value, identity, claim), result in zip(triples, values)}
        self._local.prefetched = cache
        try:
            yield
            if recheck_journal:
                require_journal_bindings()
        finally:
            del self._local.prefetched


class RemoteFolderStageStore(FolderStageStore):
    def __init__(self, database, *, observe_run, observe_activity, admission_gate, inspect_receipt):
        super().__init__(database, observe_run=observe_run, observe_activity=observe_activity,
                         admission_gate=admission_gate)
        self.inspect_receipt = inspect_receipt

    def _inspect_receipt(self, value, identity, claim_id):
        return self.inspect_receipt(value, identity, claim_id)

    def summary(self):
        try:
            with closing(self._connect(readonly=True)) as db:
                db.execute('BEGIN')
                rows = db.execute('SELECT c.*,r.receipt_json,r.receipt_sha256 FROM postprocessing_claims c '
                    'LEFT JOIN postprocessing_receipts r ON c.claim_id=r.claim_id '
                    "WHERE c.state IN ('waiting_rag','receipt_held') LIMIT ?",
                    (self.inspect_receipt.max_summary_receipts + 1,)).fetchall()
                snapshot = _summary_snapshot(db, _STAGE_SUMMARY_TABLES)
            with self.inspect_receipt.prefetched(rows, recheck_journal=True):
                _require_summary_snapshot(self, _STAGE_SUMMARY_TABLES, snapshot)
                result = super().summary()
                _require_summary_snapshot(self, _STAGE_SUMMARY_TABLES, snapshot)
            # Journal revalidation itself takes time; check the control rows
            # once more after it finishes, before publishing any counts.
            _require_summary_snapshot(self, _STAGE_SUMMARY_TABLES, snapshot)
            return result
        except Exception:
            from folder_state import COUNTS
            return dict.fromkeys(COUNTS)


class RemoteResultsStore(ResultsStore):
    def record_pass(self, identity, result_id, claim_id, *, validate_receipt):
        # ResultsStore._pass_evidence normally inspects inside a SQLite read
        # transaction. Fetch and inspect first, then let the original method
        # re-read and compare the same receipt under its short transaction.
        try:
            if getattr(self.inspect_receipt._local, 'prefetched', None) is not None:
                return super().record_pass(identity, result_id, claim_id, validate_receipt=validate_receipt)
            with closing(self._connect(readonly=True)) as db:
                row = db.execute('SELECT c.*,p.receipt_json,p.receipt_sha256 FROM postprocessing_claims c '
                    'LEFT JOIN postprocessing_receipts p ON c.claim_id=p.claim_id WHERE c.claim_id=?',
                    (claim_id,)).fetchone()
            if (not row or any(row[k] != identity.get(k) for k in FIELDS)
                    or row['state'] not in {'waiting_rag', 'receipt_held'}):
                raise ResultConflict('accepted original claim is unavailable')
            with self.inspect_receipt.prefetched([row]):
                return super().record_pass(identity, result_id, claim_id, validate_receipt=validate_receipt)
        except Exception as exc:
            # The unlocked prefetch can fail before the base method gets a
            # chance to fence a receipt that has not been finally accepted.
            try:
                self._fence_unaccepted(identity, result_id, claim_id)
            except Exception as fence_error:
                exc.add_note('receipt fencing could not be confirmed: ' + type(fence_error).__name__)
            raise

    def summary(self, result_filter=None, page=1, page_size=20):
        # Keep the original argument checks and unavailable shape.
        if (result_filter not in {None, 'passed', 'failed', 'unrecognized'}
                or type(page) is not int or not 1 <= page <= 1_000_000
                or type(page_size) is not int or not 1 <= page_size <= 100):
            raise ValueError('invalid result filter or page')
        try:
            observed_from = datetime.now(timezone.utc).isoformat()
            with closing(self._connect(readonly=True)) as db:
                db.execute('BEGIN')
                rows = db.execute('SELECT c.*,p.receipt_json,p.receipt_sha256 FROM postprocessing_results r '
                    'JOIN postprocessing_claims c ON r.claim_id=c.claim_id '
                    'LEFT JOIN postprocessing_receipts p ON c.claim_id=p.claim_id '
                    "WHERE r.state='passed' LIMIT ?", (self.inspect_receipt.max_summary_receipts + 1,)).fetchall()
                snapshot = _summary_snapshot(db, _RESULT_SUMMARY_TABLES)
            with self.inspect_receipt.prefetched(rows, recheck_journal=True):
                _require_summary_snapshot(self, _RESULT_SUMMARY_TABLES, snapshot)
                result = super().summary(result_filter, page, page_size)
                _require_summary_snapshot(self, _RESULT_SUMMARY_TABLES, snapshot)
            _require_summary_snapshot(self, _RESULT_SUMMARY_TABLES, snapshot)
            if result['available']:
                result['as_of'] = observed_from
            return result
        except Exception:
            return self._unavailable(result_filter, page, page_size)


def make_remote_receipt_stores(*, database, admission_gate, journal, server_identity,
                               observe_run, inspect_batch, max_summary_receipts, observe_activity=None,
                               inspection_batch_size=_MAX_REMOTE_INSPECTION_BATCH):
    """Construct inert stores; authenticated remote inspection is caller supplied."""
    if not callable(admission_gate) or not callable(observe_run):
        raise ValueError('original task gate and real folder observation are required')
    inspector = RemoteReceiptInspector(journal=journal, server_identity=server_identity,
        inspect_batch=inspect_batch, max_summary_receipts=max_summary_receipts,
        inspection_batch_size=inspection_batch_size)
    stage = RemoteFolderStageStore(database, observe_run=observe_run, observe_activity=observe_activity,
                                   admission_gate=admission_gate, inspect_receipt=inspector)
    journal._ready(stage)
    results = RemoteResultsStore(database, admission_gate=admission_gate, inspect_receipt=inspector)
    return stage, results


class RemoteRuntimeCoordinator(Coordinator):
    """Preserve the original stage, result store, task gate and Prefect slot."""

    def __init__(self, *, base, stage_store, journal, server_identity,
                 load_inputs, snapshot_options, transfer_snapshot, validate_staging,
                 submit_once, wait_for_result, validate_result, admission_wait=None):
        if not isinstance(base, Coordinator) or isinstance(base, RemoteRuntimeCoordinator):
            raise ValueError('one existing Prefect stage coordinator is required')
        if (not isinstance(stage_store, RemoteFolderStageStore)
                or Path(stage_store.database).resolve() != base.database
                or stage_store.admission_gate is not base.task_gate
                or not callable(stage_store.observe_run)
                or not isinstance(stage_store.inspect_receipt, RemoteReceiptInspector)):
            raise ValueError('the remote folder store and task admission gate are required')
        if (not isinstance(base.results, ResultsHooks) or not isinstance(base.results.ledger, RemoteResultsStore)
                or Path(base.results.ledger.database).resolve() != base.database
                or base.results.ledger.admission_gate is not base.task_gate
                or base.results.ledger.inspect_receipt is not stage_store.inspect_receipt):
            raise ValueError('the result ledger must use the same remote receipt inspector and task gate')
        if not isinstance(journal, RemoteSubmissionJournal):
            raise ValueError('the existing remote journal is required')
        journal._ready(stage_store)
        if (stage_store.inspect_receipt.journal is not journal
                or stage_store.inspect_receipt.server != _server_identity(server_identity)):
            raise ValueError('receipt inspector journal/server binding changed')
        if not all(callable(fn) for fn in (load_inputs, transfer_snapshot, validate_staging,
                                           submit_once, wait_for_result, validate_result)):
            raise ValueError('verified input and authenticated remote callbacks are required')
        if (admission_wait is not None and (not isinstance(admission_wait, dict)
                or set(admission_wait) != {'wait', 'max_attempts'} or not callable(admission_wait['wait'])
                or (admission_wait['max_attempts'] is not None and
                    (type(admission_wait['max_attempts']) is not int
                     or not 1 <= admission_wait['max_attempts'] <= 1000)))):
            raise ValueError('explicit capacity wait is invalid')
        if not isinstance(snapshot_options, dict) or set(snapshot_options) != _OPTIONS:
            raise ValueError('explicit snapshot options and memory budgets are required')
        for key in ('max_json_file_bytes', 'max_total_json_bytes'):
            if type(snapshot_options[key]) is not int or snapshot_options[key] <= 0:
                raise ValueError('positive snapshot metadata budgets are required')
        if snapshot_options['max_total_json_bytes'] < snapshot_options['max_json_file_bytes']:
            raise ValueError('total metadata budget must cover one metadata file')
        self._stage_store, self._journal = stage_store, journal
        self._server = _server_identity(server_identity)
        self._load_inputs = load_inputs
        self._options = copy.deepcopy(snapshot_options)
        self._transfer, self._validate_staging = transfer_snapshot, validate_staging
        self._submit, self._wait, self._validate_result = submit_once, wait_for_result, validate_result
        self._admission_wait = None if admission_wait is None else dict(admission_wait)
        super().__init__(base.database, task_gate=base.task_gate, folder_gate=base.folder_gate,
            postprocess_is_open=base.postprocess_is_open, rag_is_held=base.rag_is_held,
            verify_upstream=base.verify_upstream, verify_execution=base.verify_execution,
            slot=base.slot, process_folder=self._process_remote, results=base.results)

    def reconcile_held(self, job_id, generation, *, store, validator_factory):
        # The inherited local recovery acquires its folder gate with the old
        # generation and has no authenticated remote reconciliation contract.
        # It must never turn an ambiguous remote result into another submission.
        raise StageConflict('remote held receipt reconciliation is not connected')

    def _stage_binding(self, item, job):
        """Only bounded local reads; suitable for the task admission gate."""
        with closing(sqlite3.connect(self.database.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE job_id=?',
                             (job['job_id'],)).fetchone()
        if (not row or job['job_id'] != item['job_id'] or row['state'] != 'executing'
                or any(row[key] != job['identity'][key] for key in FIELDS)
                or row['generation'] != job['generation'] or row['manifest_sha256'] != job['manifest_sha256']
                or any(key not in row.keys() or row[key] != item.get(key) for key in
                       ('source_llm_generation', 'source_retry_sha256', 'source_retry_prefect_sha256'))
                or row['folder_run_id'] != job['identity']['task_run_id']
                or row['task_key'] != item['task_key'] or row['owner_pid'] != os.getpid()
                or row['owner_started'] != psutil.Process(os.getpid()).create_time()):
            raise StageConflict('the original running Prefect stage ownership changed')
        return dict(job_id=row['job_id'], task_key=row['task_key'], owner_pid=row['owner_pid'],
                    owner_started=row['owner_started'], stage_updated_at_ns=row['updated_at_ns'])

    def _process_remote(self, identity, generation, item, accepted_hook, context_task):
        holder = {}

        def prepare(claim):
            detached_identity, detached_item = copy.deepcopy(identity), copy.deepcopy(item)
            bound = self._load_inputs(detached_identity, generation, detached_item, context_task)
            if detached_identity != identity or detached_item != item:
                raise StageConflict('input loader changed the original stage identity')
            if not isinstance(bound, dict) or set(bound) != {'inputs', 'counter_contract', 'trusted_catalog_path'}:
                raise StageConflict('original inputs, backend catalog and runtime counter contract are required')
            configured_catalog = Path(bound['trusted_catalog_path'])
            if not configured_catalog.is_absolute() or '..' in configured_catalog.parts:
                raise StageConflict('an independently configured exact catalog path is required')
            prepared = prepare_manifest(claim, bound['inputs'], counter_contract=bound['counter_contract'],
                                        trusted_catalog_path=configured_catalog,
                                        **self._options, defer_binary_hash=True, job_id=item['job_id'])
            holder['manifest'] = prepared['manifest']
            holder['trusted_files'] = copy.deepcopy(prepared['trusted_files'])
            return dict(snapshot_sha256=prepared['snapshot_sha256'], code_sha256=self._options['code_sha256'],
                        payload={'kind': 'in-process-frozen-manifest/1'})

        def transfer(job, snapshot):
            manifest = holder['manifest']
            if snapshot_sha(manifest) != job['snapshot_sha256']:
                raise StageConflict('the frozen input snapshot changed before transfer')
            trusted_files = copy.deepcopy(holder['trusted_files'])
            result = self._transfer(job, snapshot, manifest, verify_locally=False,
                                    trusted_files=trusted_files)
            if trusted_files != holder['trusted_files']:
                raise StageConflict('transfer changed the configured exact file authorization')
            if snapshot_sha(manifest) != job['snapshot_sha256']:
                raise StageConflict('the frozen input snapshot changed during transfer')
            return result

        def observe(claim, job, clock):
            if self.verify_execution(item, context_task) is not True:
                raise StageConflict('the actual Prefect stage is not running')
            return dict(identity=copy.deepcopy(identity), claim_id=claim['claim_id'],
                        generation=generation, job_id=job['job_id'], state='RUNNING',
                        observed_at_ns=time.time_ns(), clock=copy.deepcopy(clock),
                        binding=self._stage_binding(item, job))

        def bind(claim, job, observation):
            return observation['binding'] == self._stage_binding(item, job)

        capacity_wait = (None if self._admission_wait is None
                         else dict(self._admission_wait, check_gate=self._gates))
        return process_remote_folder(identity=identity, generation=generation,
            manifest_sha256=item['manifest_sha256'], store=self._stage_store,
            journal=self._journal, server_identity=self._server, prepare_snapshot=prepare,
            transfer_snapshot=transfer, validate_staging=self._validate_staging,
            observe_execution=observe, bind_execution=bind, submit_once=self._submit,
            wait_for_result=self._wait, validate_result=self._validate_result,
            accepted_hook=accepted_hook, admission_wait=capacity_wait, check_gate=self._gates,
            job_id=item['job_id'])


def make_stage_input_loader(*, database, client_factory, upstream_factory, backend_factory):
    """Build inputs through the existing Prefect-bound upstream handoff.

    Factories must select the already configured Prefect client, its original
    upstream binding and a backend with model invocation disabled.
    """
    database = Path(database).resolve(strict=True)
    if not all(callable(fn) for fn in (client_factory, upstream_factory, backend_factory)):
        raise ValueError('existing Prefect/upstream/backend factories are required')

    def load(identity, generation, item, context_task):
        from body_adapter import frozen_plan_evidence
        from generation_mainline_bridge import _snapshot
        from handoff_inputs import build_inputs
        from paperflow_prefect.llm_queue.folder_completion import expected_components, inspect_folder
        from preextract_scheduler_adapter import request_observation

        identity = _identity(identity)
        if (type(generation) is not int or generation < 1 or generation != item.get('generation')
                or any(item.get(key) != identity[key] for key in FIELDS)):
            raise StageConflict('the original stage input identity or generation changed')
        with client_factory() as client:
            binding = StageBoundHandoff(upstream_factory(client), client, item, context_task)
            verified = binding.load(identity, require_running=True)
            row, admissions = _snapshot(database, identity)
            if (row['manifest_sha256'] != item['manifest_sha256']
                    or row['generation'] != item['source_llm_generation']):
                raise StageConflict('the original LLM input or generation changed')
            backend = backend_factory(verified)
            if not hasattr(backend, 'invoke') or backend.invoke is not None:
                raise StageConflict('model invocation must be disabled for postprocessing input')
            # Resolve authority from the independently configured backend, never
            # from manifest/material paths or caller-supplied snapshot contents.
            try:
                catalog_path = Path(backend.settings.path('catalog_path'))
            except (AttributeError, TypeError, ValueError, KeyError) as exc:
                raise StageConflict('the trusted backend catalog configuration is unavailable') from exc
            if not catalog_path.is_absolute() or '..' in catalog_path.parts:
                raise StageConflict('the trusted backend catalog path must be absolute')
            counter_contract = counter_contract_from_runtime(backend.runtime)
            inputs = build_inputs(identity=identity, verified=verified, admissions=admissions,
                manifest_sha256=row['manifest_sha256'], expected_components=expected_components,
                inspect_folder=inspect_folder, observe_request=request_observation,
                frozen_plan=frozen_plan_evidence, attachment_work=backend.attachment_work)
            return dict(inputs=inputs, counter_contract=counter_contract, trusted_catalog_path=catalog_path)

    return load


def make_remote_coordinator(**kwargs):
    """Create an inert candidate; all stores and callbacks must already exist."""
    return RemoteRuntimeCoordinator(**kwargs)
