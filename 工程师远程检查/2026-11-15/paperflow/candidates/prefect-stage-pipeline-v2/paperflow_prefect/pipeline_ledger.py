"""Durable business evidence and send reservations, never a scheduler.

All Workers use this same local root. A claim holds ONLY an operation lock.
Paper identity/budget locks are short, synchronous file transactions. OS locks
release on death; persisted owner identity and reservations survive it. Prefect
owns queues, rate limits and concurrency. No method here performs an HTTP call.

Integration: set_dispatch before handing an envelope to Prefect; claim before
business work; reserve_send immediately before each HTTP; commit_result after
adapter validation. claim yields execute=False only after rehashing evidence.
Unknown sends require reconciliation; a recovery envelope alone cannot clear
unknown. reconcile_result accepts the reconciler's hashed identity-bound outcome proof.
inherit_legacy_retries must be called with ALL frozen work roots before recovery.
"""
from contextlib import contextmanager
from dataclasses import replace
import math
import os
from pathlib import Path
import re
from contextvars import ContextVar
import time
import uuid

from filelock import FileLock, Timeout
import psutil

from .pipeline_contracts import StageEnvelope, StageResult
from .records import identity, read_json, sha_file, write_json


class OperationBusy(RuntimeError): pass
class StaleDispatch(RuntimeError): pass
class EvidenceChanged(RuntimeError): pass
class SendBudgetExceeded(RuntimeError): pass
class SendAlreadyReserved(RuntimeError): pass
class OutcomeUnknown(RuntimeError): pass

_SUCCESS = {'complete', 'passed', 'verified', 'skipped'}
_OWNER_KEYS = {'instance_id', 'pid', 'process_create_time', 'task_run_id'}


def _text(value):
    if not isinstance(value, str) or not value.strip(): raise ValueError('nonempty identity required')
    return value


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-fA-F]{64}', value):
        raise ValueError('invalid sha256')
    return value.lower()


def _owner(value):
    if not isinstance(value, dict) or set(value) != _OWNER_KEYS: raise ValueError('invalid owner fields')
    _text(value['instance_id']); _text(value['task_run_id'])
    if type(value['pid']) is not int or value['pid'] != os.getpid(): raise ValueError('claim must own current process')
    created = value['process_create_time']
    if (isinstance(created, bool) or not isinstance(created, (int, float))
            or not math.isfinite(created) or abs(psutil.Process().create_time() - created) > .001):
        raise ValueError('process creation time mismatch')
    return dict(value)


def owner_alive(value):
    """PID reuse is not ownership. Access-denied is conservatively still live."""
    if not value: return False
    try:
        process = psutil.Process(value['pid'])
        return process.is_running() and abs(process.create_time()-value['process_create_time']) <= .001
    except psutil.NoSuchProcess: return False
    except (psutil.AccessDenied, KeyError, TypeError): return True


def _verify(refs):
    try:
        for ref in refs:
            path = Path(ref.path)
            if not path.is_absolute() or not path.is_file() or sha_file(path) != ref.sha256:
                raise EvidenceChanged('artifact bytes do not match saved reference')
    except OSError:
        raise EvidenceChanged('artifact cannot be read') from None


def _business(envelope):
    return {'paper_id': envelope.paper_id, 'stage': envelope.stage.value,
            'item_id': envelope.item_id, 'part_id': envelope.part_id,
            'contract_version': envelope.contract_version,
            'input_hashes': [ref.sha256 for ref in envelope.input_refs]}


class StageLedger:
    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self._active_claims = ContextVar('ledger_claims', default=None)

    def _operation_path(self, key):
        if not isinstance(key, str) or not re.fullmatch('op-[0-9a-f]{64}', key):
            raise ValueError('invalid operation key')
        return self.root/'operations'/(key+'.json')

    def _paper_path(self, paper_id):
        return self.root/'papers'/(identity(_text(paper_id))+'.json')

    def _lock(self, scope, key, timeout=-1):
        path = self.root/'locks'/scope/(identity(key)+'.lock')
        path.parent.mkdir(parents=True, exist_ok=True)
        return FileLock(str(path), timeout=timeout)

    def read_operation(self, operation_key) -> dict:
        path = self._operation_path(operation_key)
        try:
            record = read_json(path)
            if record.get('schema') != 'pipeline-operation/1' or record.get('operation_key') != operation_key:
                raise EvidenceChanged('invalid operation record')
            return record
        except FileNotFoundError: return {'operation_key': operation_key, 'state': 'pending'}
        except (ValueError, TypeError, AttributeError): raise EvidenceChanged('unreadable operation record') from None

    def _record(self, envelope):
        record = self.read_operation(envelope.operation_key)
        if 'schema' not in record:
            record.update(schema='pipeline-operation/1', business=_business(envelope),
                          envelope=envelope.to_dict(), dispatch_seq=envelope.dispatch_seq,
                          owner=None, attempts=[])
        elif record['business'] != _business(envelope):
            raise EvidenceChanged('operation identity binding changed')
        return record

    def _bind(self, envelope):
        # No lock spans paper processing. This only creates/checks the binding.
        with self._lock('identity', envelope.paper_id):
            path = self.root/'bindings'/(envelope.operation_key+'.json')
            binding = _business(envelope)
            if path.exists():
                if read_json(path) != binding: raise EvidenceChanged('operation bound to another material')
            else: write_json(path, binding)

    def set_dispatch(self, envelope: StageEnvelope):
        """Fence old envelopes; caller still owns Prefect submission/reconciliation.

        Does not permit replacing a live owner or erase a result/send outcome.
        Returns the persisted record for a durable dispatch-intent transaction.
        """
        try:
            with self._lock('operation', envelope.operation_key, 0):
                self._bind(envelope)
                record = self._record(envelope)
                if envelope.dispatch_seq < record['dispatch_seq']: raise StaleDispatch('old dispatch')
                if owner_alive(record.get('owner')): raise OperationBusy('owner still alive')
                record.update(dispatch_seq=envelope.dispatch_seq, envelope=envelope.to_dict())
                write_json(self._operation_path(envelope.operation_key), record)
                return record
        except Timeout: raise OperationBusy('operation is claimed') from None

    def _claims(self):
        return self._active_claims.get() or {}

    def _dispatch_status(self, envelope, record):
        cached = self._cached(record)
        live = owner_alive(record.get('owner')); sends = []
        for send in self.send_budget(envelope.paper_id)['reservations']:
            key = send.get('operation_key')
            if key == envelope.operation_key: sends.append(send)
            elif key and envelope.part_id is None:
                child = self.read_operation(key); business = child.get('business', {})
                if (business.get('paper_id') == envelope.paper_id and business.get('stage') == envelope.stage.value
                        and business.get('item_id') == envelope.item_id and business.get('part_id') is not None):
                    sends.append(send); live = live or owner_alive(child.get('owner'))
        remote_started = False
        for path in (self.root/'processes'/envelope.operation_key).glob('*/process.json'):
            process = read_json(path)
            if process.get('schema') != 'pipeline-stage-process/1': raise EvidenceChanged('unknown process evidence')
            live = live or any(owner_alive(p) for p in process.get('processes', []))
            remote_started |= envelope.stage.value in {'rag_body','rag_attachment','archive','cleanup'}
        return dict(record=record,result=cached,live=live,sent=bool(sends) or remote_started,reservations=sends)

    def dispatch_status(self, envelope):
        try:
            with self._lock('operation', envelope.operation_key, 0):
                record = self._record(envelope); _verify((envelope.receipt_ref,*envelope.input_refs))
                return self._dispatch_status(envelope,record)
        except Timeout: raise OperationBusy('operation is claimed') from None

    def advance_unsent_dispatch(self, envelope):
        """Atomic sequence fence AFTER caller proves native query and idle Worker.

        Release before apply_async so a fast consumer can claim. Sent/unknown/live
        operations cannot use this path, including known native child processes.
        """
        try:
            with self._lock('operation', envelope.operation_key, 0):
                record = self._record(envelope)
                if record['dispatch_seq'] != envelope.dispatch_seq: raise StaleDispatch('dispatch changed')
                status = self._dispatch_status(envelope,record)
                if status['live']: raise OperationBusy('owner or child remains alive')
                if status['sent'] or record['state'] not in {'pending','running','submitted'}:
                    raise OutcomeUnknown('cannot take unsent requeue path')
                new = replace(envelope,dispatch_seq=envelope.dispatch_seq+1)
                record.setdefault('dispatch_history',[]).append(dict(dispatch_seq=envelope.dispatch_seq,
                    state=record['state'],owner=record.get('owner'),fenced_at=time.time()))
                record.update(dispatch_seq=new.dispatch_seq,envelope=new.to_dict(),state='pending',owner=None)
                write_json(self._operation_path(envelope.operation_key),record)
                return new
        except Timeout: raise OperationBusy('operation is claimed') from None

    def mark_orphan_unknown(self, envelope):
        """Classify orphaned send without authorizing a resend or discarding evidence."""
        try:
            with self._lock('operation', envelope.operation_key, 0):
                record = self._record(envelope)
                if record['dispatch_seq'] != envelope.dispatch_seq: raise StaleDispatch('dispatch changed')
                status = self._dispatch_status(envelope,record)
                if status['live']: raise OperationBusy('owner or child remains alive')
                if status['result'] is None and status['sent']:
                    record.update(state='unknown',owner=None); write_json(self._operation_path(envelope.operation_key),record)
                return record
        except Timeout: raise OperationBusy('operation is claimed') from None

    def _cached(self, record):
        saved = record.get('result')
        if saved is None: return None
        if identity(saved) != record.get('result_sha256'): raise EvidenceChanged('result record changed')
        try: result = StageResult.from_dict(saved)
        except (ValueError, TypeError): raise EvidenceChanged('invalid saved result') from None
        _verify(result.evidence_refs)
        if result.state in _SUCCESS:
            if not result.evidence_refs: raise EvidenceChanged('successful result needs evidence')
            return saved
        return None

    def _send_count(self, envelope):
        with self._lock('budget', envelope.paper_id):
            budget = self._budget(envelope.paper_id)
            return sum(row.get('operation_key') == envelope.operation_key for row in budget['reservations'])

    @contextmanager
    def claim(self, envelope: StageEnvelope, owner: dict):
        owner = _owner(owner)
        lock = self._lock('operation', envelope.operation_key, 0)
        try: lock.acquire()
        except Timeout: raise OperationBusy('operation is claimed') from None
        registered = False
        try:
            self._bind(envelope)
            record = self._record(envelope)
            if envelope.dispatch_seq != record['dispatch_seq']: raise StaleDispatch('dispatch sequence mismatch')
            _verify((envelope.receipt_ref, *envelope.input_refs))
            cached = self._cached(record)
            if cached is not None:
                yield {'execute': False, 'result': cached, 'owner': None}
                return
            if owner_alive(record.get('owner')): raise OperationBusy('persisted owner still alive')
            baseline_sends = self._send_count(envelope)
            previous = record['attempts'][-1] if record['attempts'] else {}
            if (record['state'] == 'running' and previous.get('prior_state') in {'retryable_failed', 'failed'}
                    and previous.get('send_count_before') == baseline_sends):
                # Dead recovery owner never crossed a new reservation boundary.
                record['state'] = previous['prior_state']
            if record['state'] == 'unknown' or (baseline_sends and record['state'] not in {'retryable_failed', 'failed'}):
                record.update(state='unknown', owner=None)
                write_json(self._operation_path(envelope.operation_key), record)
                raise OutcomeUnknown('previous send requires reconciliation')
            prior_state = record['state']
            record.update(state='running', owner=owner, envelope=envelope.to_dict())
            record['attempts'].append({'owner': owner, 'dispatch_seq': envelope.dispatch_seq,
                                       'dispatch_kind': envelope.dispatch_kind.value, 'started_at': time.time(),
                                       'prior_state': prior_state, 'send_count_before': baseline_sends})
            write_json(self._operation_path(envelope.operation_key), record)
            claim_token = self._active_claims.set(dict(self._claims(), **{envelope.operation_key: (envelope, owner)}))
            registered = True
            yield {'execute': True, 'result': None, 'owner': owner}
        finally:
            try:
                if registered:
                    record = self.read_operation(envelope.operation_key)
                    if record['state'] == 'running':
                        if self._send_count(envelope) > baseline_sends:
                            record['state'] = 'unknown'
                        else:
                            record['state'] = prior_state if prior_state in {'retryable_failed', 'failed'} else 'pending'
                    record['owner'] = None
                    write_json(self._operation_path(envelope.operation_key), record)
            finally:
                if registered: self._active_claims.reset(claim_token)
                lock.release()

    def reconcile_result(self, envelope, result: StageResult, owner: dict):
        """Accept a known outcome with a hashed, identity-bound query proof.

        evidence_refs[0] is local JSON schema pipeline-reconciliation/1 with
        operation_key, paper_id, item_id, part_id, reservation_id,
        request_sha256 and outcome. It must bind the LAST reserved send for
        this operation. Task4/5 owns provider queries and interpretation; this
        method only verifies persisted identity and bytes, never queries/sends.
        """
        owner = _owner(owner)
        if result.operation_key != envelope.operation_key or not result.evidence_refs:
            raise EvidenceChanged('reconciliation needs original operation and evidence')
        if result.state not in {'complete', 'passed', 'verified', 'failed', 'retryable_failed', 'needs_attention'}:
            raise EvidenceChanged('reconciliation requires explicit known business outcome')
        try:
            with self._lock('operation', envelope.operation_key, 0):
                record = self._record(envelope)
                if record['dispatch_seq'] != envelope.dispatch_seq: raise StaleDispatch('dispatch changed')
                if owner_alive(record.get('owner')): raise OperationBusy('previous owner remains alive')
                if record['state'] not in {'unknown', 'needs_attention'}:
                    raise EvidenceChanged('only unresolved outcomes may be reconciled')
                _verify((envelope.receipt_ref, *envelope.input_refs, *result.evidence_refs))
                with self._lock('budget', envelope.paper_id):
                    budget = self._budget(envelope.paper_id)
                    sends = [r for r in budget['reservations'] if r.get('operation_key') == envelope.operation_key]
                if not sends: raise EvidenceChanged('no original send to reconcile')
                latest = sends[-1]
                proof = read_json(result.evidence_refs[0].path)
                expected = {'schema': 'pipeline-reconciliation/1', 'operation_key': envelope.operation_key,
                    'paper_id': envelope.paper_id, 'item_id': envelope.item_id, 'part_id': envelope.part_id,
                    'reservation_id': latest['reservation_id'], 'request_sha256': latest['request_sha256'],
                    'outcome': result.state}
                if not isinstance(proof, dict) or any(proof.get(k) != v for k, v in expected.items()):
                    raise EvidenceChanged('reconciliation proof identity or outcome mismatch')
                value = result.to_dict()
                record.setdefault('reconciliations', []).append({'previous_state': record['state'],
                    'previous_result': record.get('result'), 'result': value, 'owner': owner,
                    'reservation_id': latest['reservation_id'], 'reconciled_at': time.time()})
                record.update(state=result.state, result=value, result_sha256=identity(value), owner=None)
                write_json(self._operation_path(envelope.operation_key), record)
        except Timeout: raise OperationBusy('operation is claimed') from None
        except (OSError, ValueError, TypeError, KeyError):
            raise EvidenceChanged('reconciliation proof cannot be verified') from None

    def commit_result(self, envelope, result: StageResult) -> None:
        claim = self._claims().get(envelope.operation_key)
        if not claim or claim[0] != envelope: raise OperationBusy('commit requires this ledger owned claim')
        if result.operation_key != envelope.operation_key: raise EvidenceChanged('result operation mismatch')
        if result.state in _SUCCESS and not result.evidence_refs: raise EvidenceChanged('successful result needs evidence')
        _verify((envelope.receipt_ref, *envelope.input_refs, *result.evidence_refs))
        record = self._record(envelope)
        if record['dispatch_seq'] != envelope.dispatch_seq: raise StaleDispatch('dispatch changed')
        value = result.to_dict()
        record.setdefault('result_history', []).append({'previous_state': record['state'],
            'previous_result': record.get('result'), 'result': value, 'owner': claim[1], 'committed_at': time.time()})
        record.update(state=result.state, result=value, result_sha256=identity(value), completed_at=time.time())
        write_json(self._operation_path(envelope.operation_key), record)

    def _budget(self, paper_id):
        path = self._paper_path(paper_id)
        if not path.exists():
            return {'schema': 'pipeline-send-budget/1', 'paper_id': paper_id, 'reservations': [],
                    'legacy_roots': [], 'legacy_attempts': {}, 'legacy_evidence': {}, 'additional_used': 0}
        try:
            value = read_json(path)
            if value['schema'] != 'pipeline-send-budget/1' or value['paper_id'] != paper_id:
                raise ValueError('budget identity')
            return value
        except (KeyError, ValueError, TypeError): raise EvidenceChanged('unreadable budget') from None

    def _refresh_legacy(self, budget):
        attempts = {}; evidence = {}
        try:
            for name in budget['legacy_roots']:
                work = Path(name)
                if not work.is_dir(): raise EvidenceChanged('legacy root missing')
                linked = {}
                for pointer in work.rglob('active-revision.json'):
                    value = read_json(pointer)
                    target = Path(value.get('work_dir', '')).resolve()
                    revision = target/'revision.json'
                    if (value.get('schema') != 'preextract-active-revision/1'
                            or not target.is_relative_to(work) or not revision.is_file()
                            or sha_file(revision) != value.get('revision_sha256')):
                        raise EvidenceChanged('dangling or changed active revision')
                for state_path in work.rglob('partition-state.json'):
                    state = read_json(state_path)
                    for recovery in state.get('recoveries', []):
                        directory = (state_path.parent/recovery['directory']).resolve()
                        if not directory.is_relative_to(work) or not (directory/'revision.json').is_file():
                            raise EvidenceChanged('partition recovery history missing')
                for path in work.rglob('retry-intent.json'):
                    if path.parent.name != 'profile-recovery': raise EvidenceChanged('unrecognized retry history')
                    value = read_json(path)
                    parent = path.parents[2]
                    if (value.get('schema') != 'profile-engineering-attempt/1'
                            or value.get('attempt') != 1 or value.get('max_additional_attempts') != 1
                            or Path(value.get('root_work', '')).resolve() != parent.resolve()):
                        raise EvidenceChanged('unrecognized retry intent')
                    _sha(value['parent_request_sha256'])
                    evidence[str(path.resolve())] = sha_file(path)
                    key = str((parent/'revisions/profile-recovery-1').resolve())
                    linked[key] = value
                    attempts[key] = {'kind': 'retry-intent', 'evidence': str(path.resolve())}
                for path in work.rglob('revision.json'):
                    value = read_json(path)
                    key = str(path.parent.resolve())
                    evidence[str(path.resolve())] = sha_file(path)
                    if key in linked:
                        intent = linked[key]
                        hashes = intent.get('parent_hashes')
                        # The intent is reserved before native operation.lock.
                        # An unrelated revision can win the same directory name;
                        # only its source/parent evidence proves the same send.
                        if (value.get('schema') != 'preextract-authorized-revision/1'
                                or value.get('mode') != 'rerun' or value.get('recover_failed') is not True
                                or value.get('refresh_material')
                                or not isinstance(hashes, dict) or not hashes
                                or value.get('parent_hashes') != hashes
                                or _sha(value.get('source_sha256')) != _sha(intent.get('source_sha256'))):
                            raise EvidenceChanged('same-name revision is not linked to retry intent')
                        for digest in hashes.values():
                            if digest is not None: _sha(digest)
                    if value.get('schema') == 'preextract-authorized-revision/1':
                        if value.get('mode') == 'candidate_import': continue
                        if value.get('mode') != 'rerun': raise EvidenceChanged('unrecognized revision mode')
                        if Path(value.get('parent', '')).resolve() != path.parents[2].resolve():
                            raise EvidenceChanged('revision parent mismatch')
                    elif not {'id', 'directory', 'previous', 'evidence', 'dispatch_started'} <= value.keys():
                        raise EvidenceChanged('unrecognized revision schema')
                    if key not in linked: attempts[key] = {'kind': 'revision', 'evidence': str(path.resolve())}
                # A directory created before its revision record is unresolved, not free budget.
                for revisions in work.rglob('revisions'):
                    if not revisions.is_dir(): raise EvidenceChanged('invalid revisions directory')
                    for child in revisions.iterdir():
                        if not child.is_dir() or not (child/'revision.json').is_file():
                            if str(child.resolve()) not in linked: raise EvidenceChanged('incomplete revision history')
            for path, digest in budget['legacy_evidence'].items():
                if evidence.get(path) != digest: raise EvidenceChanged('previous retry evidence changed or disappeared')
            budget['legacy_evidence'].update(evidence)
            budget['legacy_attempts'].update(attempts)
            budget['additional_used'] = len(budget['legacy_attempts']) + sum(r['recovery'] for r in budget['reservations'])
        except (OSError, ValueError, TypeError, KeyError): raise EvidenceChanged('legacy retry evidence cannot be verified') from None

    def inherit_legacy_retries(self, paper_id, work_roots):
        """Register complete frozen historical roots, preserving hashes and spent slots.

        Must happen before any recovery. Unreadable/ambiguous history stays bound
        and blocks sends. Candidate import revisions do not consume HTTP budget;
        each rerun/partition revision conservatively consumes one reserved send.
        """
        roots = [str(Path(root).resolve()) for root in work_roots]
        with self._lock('budget', paper_id):
            budget = self._budget(paper_id)
            budget['legacy_roots'] = sorted(set(budget['legacy_roots']) | set(roots))
            # Persist binding BEFORE inspecting: a rejected migration cannot be bypassed.
            write_json(self._paper_path(paper_id), budget)
            self._refresh_legacy(budget)
            write_json(self._paper_path(paper_id), budget)
            return budget

    def send_budget(self, paper_id):
        with self._lock('budget', paper_id):
            budget = self._budget(paper_id)
            self._refresh_legacy(budget)
            write_json(self._paper_path(paper_id), budget)
            return budget

    def reserve_send(self, paper_id, item_id, part_id, request_sha256, recovery: bool) -> dict:
        """Durably consume BEFORE transport. Reservations never auto-refund/replay.

        recovery=True consumes one of TWO additional HTTPs for the whole paper.
        This is an attempt ledger, not a concurrency limit. It does not establish
        retry eligibility; the recovery policy must prove a known retryable result.
        """
        _text(paper_id); request_sha256 = _sha(request_sha256)
        for value in (item_id, part_id):
            if value is not None: _text(value)
        if type(recovery) is not bool: raise ValueError('recovery must be boolean')
        claims = [(env, own) for env, own in self._claims().values()
                  if (env.paper_id, env.item_id, env.part_id) == (paper_id, item_id, part_id)]
        if len(claims) > 1: raise OperationBusy('ambiguous claimed send identity')
        env, own = claims[0] if claims else (None, None)
        with self._lock('budget', paper_id):
            budget = self._budget(paper_id)
            self._refresh_legacy(budget)
            if recovery and budget['additional_used'] >= 2: raise SendBudgetExceeded('paper additional-send budget exhausted')
            if not recovery and any((r['item_id'], r['part_id'], r['request_sha256']) ==
                    (item_id, part_id, request_sha256) for r in budget['reservations']):
                raise SendAlreadyReserved('original request was already reserved; reconcile it')
            row = {'reservation_id': uuid.uuid4().hex, 'paper_id': paper_id,
                   'item_id': item_id, 'part_id': part_id, 'request_sha256': request_sha256,
                   'recovery': recovery, 'additional_number': budget['additional_used']+1 if recovery else 0,
                   'operation_key': env.operation_key if env else None,
                   'owner': own, 'reserved_at': time.time()}
            budget['reservations'].append(row)
            if recovery: budget['additional_used'] += 1
            write_json(self._paper_path(paper_id), budget)
            return row
