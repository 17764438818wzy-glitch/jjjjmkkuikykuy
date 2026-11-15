"""Pure business contracts. This module never imports Prefect or performs I/O.

Evidence is ONE paper snapshot, never a batch/global aggregation::

    {"paper_id": "stable-id", "batch_id": "registration-id",
     "submission": {"state": "complete"}, "whole_receipt_verified": True,
     "body_admitted": True,
     "stages": {"intake": {"state": "complete"}, ...},
     "attachments": {"inventory_state": "complete", "items": {
         "item-id": {"required": True, "attachment": {"state": "passed"},
                     "rag_attachment": {"state": "verified"}}}}}

Missing stage means pending. Only pending work is normally dispatchable: failed,
unknown, submitted, running and retryable_failed work needs recovery/reconciliation.
Intake complete means an admitted subset is valid; whole_receipt_verified may
still be false. That flag gates archive/cleanup/finish, never confirmed body work.
body_admitted is true only for a confirmed received body; absent/false blocks body
stages and final completion but permits attachment identification after intake.
For an explicitly complete EMPTY inventory, persist BOTH stages.attachment and
stages.rag_attachment as {"state":"skipped","reason":"empty_inventory"}.
ready_stages returns the relevant stage name to request those no-HTTP skip writes.
For nonempty inventories per-item states are authoritative; dispatch fans out
only pending eligible items. The aggregate stages.attachment/rag_attachment rows
cannot override any required item's failure. required defaults to true; explicit
values must be booleans, and only boolean false removes a required dependency.

"passed"/"verified" must only be written after business artifact/hash/identity
validation. RAG verified means ALL required targets and parent associations have
passed; archive verified means ALL NAS file hashes/readbacks; cleanup verified
means the authorized cleanup receipt. These pure functions do not inspect files
or validate remote claims. Adapters own that validation before building snapshots.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json
import ntpath
import re


class Stage(StrEnum):
    INTAKE = 'intake'
    PREPARE = 'prepare'
    PROFILE = 'profile'
    ATTACHMENT = 'attachment'
    PACKAGE = 'package'
    RAG_BODY = 'rag_body'
    RAG_ATTACHMENT = 'rag_attachment'
    ARCHIVE = 'archive'
    CLEANUP = 'cleanup'
    FINISH = 'finish'


class DispatchKind(StrEnum):
    NORMAL = 'normal'
    RECOVERY = 'recovery'


STAGES = tuple(stage.value for stage in Stage)
DISPATCH_KINDS = tuple(kind.value for kind in DispatchKind)
STATES = ('pending', 'submitted', 'running', 'complete', 'passed', 'verified',
          'skipped', 'failed', 'retryable_failed', 'unknown', 'needs_attention',
          'blocked', 'cancelled')


def _text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(field + ' must be a nonempty string')
    return value


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-fA-F]{64}', value):
        raise ValueError('sha256 must have exactly 64 hexadecimal characters')
    return value.lower()


def _operation(value):
    if not isinstance(value, str) or not re.fullmatch(r'op-[0-9a-f]{64}', value):
        raise ValueError('invalid canonical operation_key')
    return value


def _counter(value, field):
    if type(value) is not int or value < 0:
        raise ValueError(field + ' must be an integer >= 0')
    return value


@dataclass(frozen=True)
class ArtifactRef:
    """Lightweight local reference only. Hash syntax is checked, file bytes are not."""
    path: str
    sha256: str

    def __post_init__(self):
        _text(self.path, 'path')
        object.__setattr__(self, 'sha256', _digest(self.sha256))

    def to_dict(self):
        return {'path': self.path, 'sha256': self.sha256}

    @classmethod
    def from_dict(cls, value):
        return cls(**value)


def _ref(value):
    return value if isinstance(value, ArtifactRef) else ArtifactRef.from_dict(value)


@dataclass(frozen=True)
class StageEnvelope:
    """JSON-safe business envelope; to_dict/from_dict are the process wire format.

    receipt_ref and ordered input_refs accept ArtifactRef or {path,sha256}.
    control_generation has exactly global/batch/runtime nonnegative integer keys.
    item_id/part_id are explicit null or stable nonempty business IDs. Recovery
    keeps the original profile/attachment stage AND operation_key; task_key is
    model_recovery outside this envelope. Source SHA is resolved from the frozen
    receipt when constructing identity, never inferred from a scheduler path.
    """
    batch_id: str
    paper_id: str
    receipt_ref: ArtifactRef
    operation_key: str
    stage: Stage
    item_id: str | None
    part_id: str | None
    input_refs: tuple[ArtifactRef, ...]
    contract_version: str
    control_generation: dict[str, int]
    dispatch_seq: int
    dispatch_kind: DispatchKind

    def __post_init__(self):
        for field in ('batch_id', 'paper_id', 'contract_version'):
            _text(getattr(self, field), field)
        for field in ('item_id', 'part_id'):
            if getattr(self, field) is not None:
                _text(getattr(self, field), field)
        _operation(self.operation_key)
        object.__setattr__(self, 'stage', Stage(self.stage))
        object.__setattr__(self, 'dispatch_kind', DispatchKind(self.dispatch_kind))
        if self.dispatch_kind == DispatchKind.RECOVERY and self.stage not in (Stage.PROFILE, Stage.ATTACHMENT):
            raise ValueError('model recovery must retain profile/attachment business stage')
        _counter(self.dispatch_seq, 'dispatch_seq')
        generation = self.control_generation
        if not isinstance(generation, Mapping) or set(generation) != {'global', 'batch', 'runtime'}:
            raise ValueError('control_generation requires exactly global/batch/runtime')
        object.__setattr__(self, 'control_generation', {
            name: _counter(generation[name], 'control_generation.' + name)
            for name in ('global', 'batch', 'runtime')})
        object.__setattr__(self, 'receipt_ref', _ref(self.receipt_ref))
        object.__setattr__(self, 'input_refs', tuple(_ref(ref) for ref in self.input_refs))

    def to_dict(self):
        return {'batch_id': self.batch_id, 'paper_id': self.paper_id,
                'receipt_ref': self.receipt_ref.to_dict(), 'operation_key': self.operation_key,
                'stage': self.stage.value, 'item_id': self.item_id, 'part_id': self.part_id,
                'input_refs': [ref.to_dict() for ref in self.input_refs],
                'contract_version': self.contract_version,
                'control_generation': dict(self.control_generation),
                'dispatch_seq': self.dispatch_seq, 'dispatch_kind': self.dispatch_kind.value}

    @classmethod
    def from_dict(cls, value):
        return cls(**value)


@dataclass(frozen=True)
class StageResult:
    """Lightweight persisted references; next_stages are hints, recheck readiness.

    evidence_refs accepts ordered ArtifactRef/{path,sha256}; never store the full
    model reply, paper text, image data or credentials in this wire structure.
    state is a business state, not Prefect's task state. No recovery business stage
    exists; a recovered response completes the original operation_key.
    """
    operation_key: str
    state: str
    evidence_refs: tuple[ArtifactRef, ...]
    next_stages: tuple[Stage, ...]

    def __post_init__(self):
        _operation(self.operation_key)
        if self.state not in STATES:
            raise ValueError('invalid business result state')
        object.__setattr__(self, 'evidence_refs', tuple(_ref(ref) for ref in self.evidence_refs))
        object.__setattr__(self, 'next_stages', tuple(Stage(stage) for stage in self.next_stages))

    def to_dict(self):
        return {'operation_key': self.operation_key, 'state': self.state,
                'evidence_refs': [ref.to_dict() for ref in self.evidence_refs],
                'next_stages': [stage.value for stage in self.next_stages]}

    @classmethod
    def from_dict(cls, value):
        return cls(**value)


def operation_key(envelope_inputs: dict) -> str:
    """Hash exactly paper_id/source_sha256/stage/item_id/part_id/input hashes/contract.

    Required keys: paper_id (stable business identity), source_sha256 (this
    material's bytes), stage, contract_version (THIS stage's processing contract).
    item_id/part_id default to null. input_refs is an ordered list of ArtifactRef
    or {path,sha256}; only each sha256 enters identity, order is significant.
    Batch, receipt hash/path, display names, scheduler version, dispatch metadata,
    runtime generation and unrelated attachment inventory are excluded. Callers
    must put every relevant stage input in input_refs, never whole-receipt hashes
    in a body stage. A new attachment thus cannot invalidate a passed body.
    """
    stage = envelope_inputs['stage']
    if stage not in STAGES:
        raise ValueError('invalid business stage')
    hashes = []
    for ref in envelope_inputs.get('input_refs', ()):
        hashes.append(_digest(ref['sha256'] if isinstance(ref, Mapping) else ref.sha256))
    value = {'paper_id': _text(envelope_inputs['paper_id'], 'paper_id'),
             'source_sha256': _digest(envelope_inputs['source_sha256']),
             'stage': stage, 'contract_version': _text(envelope_inputs['contract_version'],
                                                      'contract_version'),
             'item_id': envelope_inputs.get('item_id'), 'part_id': envelope_inputs.get('part_id'),
             'input_hashes': hashes}
    for field in ('item_id', 'part_id'):
        if value[field] is not None:
            _text(value[field], field)
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return 'op-' + hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _state(record):
    return record.get('state', 'pending')


def _passed(record):
    return _state(record) in ('passed', 'verified')


def _complete(record):
    return _state(record) in ('complete', 'passed', 'verified')


def _empty_skip(record):
    return _state(record) == 'skipped' and record.get('reason') == 'empty_inventory'


def _view(evidence):
    _text(evidence['paper_id'], 'paper_id')
    stages = evidence.get('stages', {})
    inventory = evidence.get('attachments', {})
    items = inventory.get('items', {})
    for item in items.values():
        if 'required' in item and type(item['required']) is not bool:
            raise ValueError('attachment required must be a boolean when present')
    records = list(stages.values()) + [record for item in items.values()
        for record in (item.get('attachment', {}), item.get('rag_attachment', {}))]
    for record in records:
        if _state(record) not in STATES:
            raise ValueError('invalid stage evidence state')
        for field in ('paper_id', 'batch_id'):
            if field in record and record[field] != evidence.get(field):
                raise ValueError('foreign ' + field + ' in stage evidence')
    return stages, items, inventory.get('inventory_state') == 'complete'


def _milestones(evidence):
    stages, items, known = _view(evidence)
    required = [row for row in items.values() if row.get('required', True) is not False]
    if items:
        attachments_passed = known and all(_passed(row.get('attachment', {})) for row in required)
        attachments_rag = known and all(_state(row.get('rag_attachment', {})) == 'verified'
                                         for row in required)
    else:
        attachments_passed = known and _empty_skip(stages.get('attachment', {}))
        attachments_rag = attachments_passed and _empty_skip(stages.get('rag_attachment', {}))
    llm = evidence.get('body_admitted') is True and _passed(stages.get('profile', {})) and attachments_passed
    rag = llm and _state(stages.get('rag_body', {})) == 'verified' and attachments_rag
    whole_rag = rag and evidence.get('whole_receipt_verified') is True
    nas = whole_rag and _state(stages.get('archive', {})) == 'verified'
    cleanup = nas and _state(stages.get('cleanup', {})) == 'verified'
    finished = cleanup and _state(stages.get('finish', {})) == 'verified'
    return llm, rag, whole_rag, nas, cleanup, finished


def ready_stages(evidence: dict) -> list[str]:
    """Return eligible business stage names in STAGES order; never mutate evidence.

    attachment eligibility is per pending item after intake; rag_attachment is per
    pending item whose attachment passed AND this paper's rag_body verified.
    Failure of another item/paper/batch cannot prevent body stages. Dispatch must
    apply the SAME item predicate when expanding these names to item envelopes.
    No automatic retry/unknown resend is implied by readiness.
    """
    stages, items, known = _view(evidence)
    row = lambda stage: stages.get(stage, {})
    pending = lambda stage: _state(row(stage)) == 'pending'
    ready = set()
    if pending('intake'):
        ready.add('intake')
    admitted = _complete(row('intake'))
    body_admitted = evidence.get('body_admitted') is True
    if admitted and body_admitted:
        if pending('prepare'):
            ready.add('prepare')
        if pending('profile') and _complete(row('prepare')):
            ready.add('profile')
        if pending('package') and _passed(row('profile')):
            ready.add('package')
        if pending('rag_body') and _complete(row('package')):
            ready.add('rag_body')
    if admitted:
        if items and any(_state(item.get('attachment', {})) == 'pending'
                         for item in items.values()):
            ready.add('attachment')
        elif known and not items and pending('attachment'):
            ready.add('attachment')
        if body_admitted and _state(row('rag_body')) == 'verified':
            if items and any(_passed(item.get('attachment', {})) and
                             _state(item.get('rag_attachment', {})) == 'pending'
                             for item in items.values()):
                ready.add('rag_attachment')
            elif known and not items and _empty_skip(row('attachment')) and pending('rag_attachment'):
                ready.add('rag_attachment')
    _, _, whole_rag, nas, cleaned, _ = _milestones(evidence)
    for stage, allowed in (('archive', whole_rag), ('cleanup', nas), ('finish', cleaned)):
        if allowed and pending(stage):
            ready.add(stage)
    return [stage for stage in STAGES if stage in ready]


def paper_status(evidence: dict) -> str:
    """Business milestone, independent of Prefect submission Flow Completed.

    completed rechecks the whole gate even when a stale finish row says verified.
    needs_attention retains all successful sibling evidence; callers display the
    detailed stage/item states alongside this summary.
    """
    stages, items, _ = _view(evidence)
    llm, rag, _, nas, _, finished = _milestones(evidence)
    records = list(stages.values()) + [item.get(stage, {}) for item in items.values()
                                      for stage in ('attachment', 'rag_attachment')]
    if finished:
        return 'completed'
    if any(_state(row) in ('failed', 'unknown', 'needs_attention', 'blocked') for row in records):
        return 'needs_attention'
    if nas:
        return 'nas_verified'
    if rag:
        return 'rag_verified'
    if llm:
        return 'llm_passed'
    if any(_state(row) not in ('pending',) for stage, row in stages.items() if stage != 'intake'):
        return 'running'
    if any(_state(item.get(stage, {})) != 'pending' for item in items.values()
           for stage in ('attachment', 'rag_attachment')):
        return 'running'
    return 'submitted' if _complete(evidence.get('submission', {})) else 'pending'


def _windows_component(value):
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', value).strip().rstrip('. ')
    value = value or '_'
    if re.fullmatch(r'(CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])',
                    value.split('.')[0].rstrip(' '), flags=re.IGNORECASE):
        value = '_' + value
    return value


def delivery_names(paper_id, title, body_name, attachments, *, title_confirmed=False,
                   existing_names=()):
    """Plan new output names and preserve original_name, without renaming originals.

    attachments is an ordered list of {item_id,original_name}; item IDs must be
    unique. existing_names occupies the target directory namespace. Suffixes _2,
    _3... resolve case-insensitive collisions; persist the returned mapping before
    writing outputs and reuse it on recovery. Unconfirmed titles display 待补题目
    and use paper_id as the processing basename (never infer title from a person).
    """
    _text(paper_id, 'paper_id')
    confirmed = title_confirmed and isinstance(title, str) and bool(title.strip())
    base = _windows_component(title if confirmed else paper_id)
    used = {ntpath.basename(name).rstrip('. ').casefold() for name in existing_names}

    def allocate(stem, extension):
        extension = _windows_component('x' + extension)[1:]
        name = stem + extension
        index = 2
        while name.casefold() in used:
            name = stem + '_' + str(index) + extension
            index += 1
        used.add(name.casefold())
        return name

    body = None
    if body_name is not None:
        _text(body_name, 'body_name')
        body = {'original_name': body_name,
                'delivery_name': allocate(base, ntpath.splitext(ntpath.basename(body_name))[1])}
    result = {'display_title': title.strip() if confirmed else '待补题目',
              'title_confirmed': bool(confirmed), 'body': body, 'attachments': {}}
    for item in attachments:
        item_id = _text(item['item_id'], 'item_id')
        if item_id in result['attachments']:
            raise ValueError('duplicate attachment item_id')
        original = _text(item['original_name'], 'original_name')
        stem, extension = ntpath.splitext(ntpath.basename(original))
        result['attachments'][item_id] = {'original_name': original,
            'delivery_name': allocate(base + '_' + _windows_component(stem), extension)}
    return result
