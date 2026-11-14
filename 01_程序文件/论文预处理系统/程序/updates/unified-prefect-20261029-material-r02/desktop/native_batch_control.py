"""One WEB task's durable admission over the existing global stage ledger.

Control files never resume G or cancel an admitted HTTP stream. The normal
workbench serializes body execution; a shared body lock also protects duplicate
launches. Queued/remote delivery remains owned by the native delivery controls.
"""
from contextlib import contextmanager, nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

import psutil

from global_task_control import GlobalPaused, GlobalTaskControl, _owner

SCHEMA = 'native-batch-control/1'


class BatchPaused(GlobalPaused):
    reason_kind = 'batch_paused'


class BatchBodyBusy(BatchPaused):
    reason_kind = 'body_owner_busy'


def _identity(value):
    if not isinstance(value, str) or not re.fullmatch(r'WEB_[A-Za-z0-9_-]{1,120}', value):
        raise ValueError('stable WEB batch_id required')
    return value


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _sources(values):
    if isinstance(values, (str, bytes)):
        raise BatchPaused('本次实际来源集合无效；未建立交付绑定')
    try:
        result = sorted(set(values))
    except (TypeError, ValueError):
        raise BatchPaused('本次实际来源集合无效；未建立交付绑定') from None
    if not result or not all(_sha(value) for value in result):
        raise BatchPaused('本次实际来源SHA不完整；未建立交付绑定')
    return result


def _lineage(proofs):
    """Compare v2 immutable lineage, allowing the producer manifest to progress."""
    keys = ('source_sha256', 'input_source_sha256', 'relation', 'producer_run_id', 'source_path',
            'input_source_path', 'parent_source_sha256', 'parent_source_path', 'archive_member_path', 'method', 'record_id')
    result = {}
    for original in proofs:
        proof = dict(original)
        proof.setdefault('parent_source_sha256', proof['input_source_sha256'])
        proof.setdefault('parent_source_path', proof['input_source_path'])
        source = proof['source_sha256']
        if source in result:raise BatchPaused('派生来源证明不唯一；未建立ready')
        result[source] = {key:proof.get(key) for key in keys}
    return result


def _selection(records):
    from preextract_material import file_sha
    result = []
    for record in records:
        path = Path(record['path']).resolve()
        source = file_sha(path)
        if record.get('source_hash') and record['source_hash'] != source:
            raise BatchPaused('保存selected与实际来源字节不同；未换AUTO身份')
        result.append(dict(path=str(path), source_hash=source))
    if not result:raise BatchPaused('实际selected为空；未分配AUTO')
    return sorted(result, key=lambda row:(row['path'],row['source_hash']))


class _BatchAdmission:
    """Only ledger registration is allowed while G owns its short transaction."""
    def __init__(self, admitted, task, row, epoch, delivery):
        self.connection = admitted.connection
        self.admitted, self.task, self.row = admitted, task, row
        self.epoch, self.delivery = epoch, delivery
        self.registrations = []

    def begin(self, stage, **identity):
        binding = self.task._ready_binding(self.row, identity.get('batch_id'), self.epoch)
        self.task._check_delivery(self.delivery, binding, require_open=True)
        token = self.admitted.begin(stage, **identity)
        self.registrations.append((token, binding))
        return token


class NativeBatchControl:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.global_control = GlobalTaskControl(self.root)
        self.directory = self.root / 'private/native-batch-control'

    def _path(self, batch_id):
        return self.directory / (hashlib.sha256(_identity(batch_id).encode()).hexdigest() + '.json')

    def _read(self, batch_id):
        row = json.loads(self._path(batch_id).read_text(encoding='utf8'))
        if (not isinstance(row, dict) or row.get('schema') != SCHEMA or row.get('batch_id') != batch_id
                or type(row.get('control_revision')) is not int or row['control_revision'] < 0
                or row.get('intent') not in {'pending', 'running', 'paused'}
                or not _sha(row.get('source_manifest_sha256'))
                or not row.get('source_hashes') or not all(_sha(s) for s in row['source_hashes'])
                or not isinstance(row.get('commands'), dict) or not isinstance(row.get('run_bindings'), list)):
            raise ValueError('native batch control identity invalid')
        return row

    def _write(self, row):
        # Existing fsync + atomic replace used by durable native stop intents.
        from preextract_delivery_control import write_stop_identity
        self.directory.mkdir(parents=True, exist_ok=True)
        write_stop_identity(self._path(row['batch_id']), row)

    def register(self, batch_id, *, source_hashes, source_manifest_sha256):
        _identity(batch_id)
        hashes = sorted(set(source_hashes))
        if not hashes or not all(_sha(s) for s in hashes) or not _sha(source_manifest_sha256):
            raise ValueError('frozen source hashes and manifest digest required')
        with self.global_control._admission_lock():
            if self._path(batch_id).exists():
                row = self._read(batch_id)
                if (row['source_hashes'] != hashes or row['source_manifest_sha256'] != source_manifest_sha256):
                    raise ValueError('registered task input identity changed')
            else:
                self._write(dict(schema=SCHEMA, batch_id=batch_id, control_revision=0, intent='pending',
                    source_hashes=hashes, source_manifest_sha256=source_manifest_sha256,
                    commands={}, run_bindings=[], execution_owner=None, created_at=time.time()))
        return self.snapshot(batch_id)

    def _global_snapshot(self):
        value = self.global_control.snapshot()
        # G's DB-free legacy-open mode is not authority for managed WEB tasks.
        if not self.global_control.path.is_file():
            value = dict(value, admission_open=False, state='unknown')
        return value

    def _snapshot(self, row):
        gate = self._global_snapshot()
        runs = {binding['run_id'] for binding in row['run_bindings']}
        active = [r for r in gate.get('active', []) if r.get('batch_id') in runs]
        unknown = [r for r in gate.get('unknown', []) if r.get('batch_id') in runs]
        if runs and gate['state'] == 'unknown':
            unknown.append(dict(reason='global_stage_ledger_unavailable'))
        execution = row.get('execution_owner')
        if execution and not execution.get('finished_at'):
            execution = dict(execution, owner=_owner(execution['pid'], execution['create_time']))
            (active if execution['owner']['status'] == 'alive' else unknown).append(
                dict(stage='execution', batch_id=row['batch_id'], **execution))
        waiting = None
        if unknown:
            state, waiting = 'unknown', 'batch_owner_or_stage_unknown'
        elif row['intent'] == 'paused':
            state = 'draining' if active else 'paused'
        elif row['intent'] == 'pending':
            state = 'pending'
        elif gate['state'] == 'unknown':
            state, waiting = 'unknown', 'global_control_unavailable'
        else:
            state = 'running'
            if not gate['admission_open']:
                waiting = 'global_paused'
        bindings_pending = any(binding.get('native_revision') == row['control_revision']
            and (binding.get('delivery_binding') or {}).get('state') != 'ready'
            for binding in row['run_bindings'])
        if bindings_pending and row['intent'] == 'running' and not unknown:
            state, waiting = 'unknown', 'delivery_binding_pending'
        return dict(schema=SCHEMA, batch_id=row['batch_id'], control_revision=row['control_revision'],
            intent=row['intent'], state=state, observed_at=time.time(),
            admission_open=(row['intent'] == 'running' and gate['admission_open'] and not unknown and not bindings_pending),
            global_admission_open=gate['admission_open'], waiting_reason=waiting,
            active=active, unknown=unknown, execution_owner=execution, run_bindings=row['run_bindings'])

    def snapshot(self, batch_id):
        try:
            return self._snapshot(self._read(batch_id))
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            return dict(schema=SCHEMA, batch_id=_identity(batch_id), control_revision=None, intent=None,
                state='unknown', observed_at=time.time(), admission_open=False, global_admission_open=False,
                waiting_reason='batch_control_unavailable', active=[], unknown=[dict(reason='control_unavailable')],
                execution_owner=None, run_bindings=[])

    def command(self, batch_id, *, action, request_id, expected_revision):
        if action not in {'start', 'pause', 'resume'}:
            raise ValueError('invalid task action')
        if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 128:
            raise ValueError('task command request_id required')
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError('task control expected_revision required')
        signature = _hash(dict(batch_id=batch_id, action=action, expected_revision=expected_revision))
        with self.global_control._admission_lock():
            row = self._read(batch_id)
            prior = row['commands'].get(request_id)
            if prior:
                if prior['signature'] != signature:
                    raise ValueError('task command identity already used with different parameters')
                result_revision, duplicate = prior['revision'], True
            else:
                if row['control_revision'] != expected_revision:
                    raise BatchPaused('task control revision changed; refresh the task before another action')
                if action == 'start' and row['intent'] != 'pending':
                    raise BatchPaused('only a pending task can start; paused tasks require resume')
                if action == 'resume':
                    if row['intent'] not in {'paused', 'running'}:
                        raise BatchPaused('pending tasks must start before resume')
                    observed = self._snapshot(row)
                    if observed['active'] or observed['unknown']:
                        raise BatchPaused('task admitted work must settle before resume')
                row['intent'] = 'paused' if action == 'pause' else 'running'
                row['control_revision'] += 1
                result_revision, duplicate = row['control_revision'], False
                row['commands'][request_id] = dict(signature=signature, action=action,
                    revision=result_revision, applied_at=time.time())
                self._write(row)
        return dict(self.snapshot(batch_id), command_revision=result_revision, duplicate=duplicate)

    def assert_running(self, batch_id, *, epoch=None):
        try:
            row = self._read(batch_id)
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            raise BatchPaused('本任务控制记录不可用；已有结果保留，未准入新阶段') from None
        if (row['intent'] != 'running' or epoch is not None and row['control_revision'] != epoch):
            raise BatchPaused('本任务已暂停或控制版本已变化；已有结果保留')
        return row

    def _execution_row(self, batch_id, epoch):
        row = self.assert_running(batch_id, epoch=epoch)
        owner = row.get('execution_owner')
        if (type(epoch) is not int or not owner or owner.get('finished_at')
                or owner.get('revision') != epoch or owner.get('pid') != os.getpid()
                or abs(owner.get('create_time', -1) - psutil.Process().create_time()) >= .01):
            raise BatchPaused('native task execution owner identity changed')
        return row

    def allocate_run(self, batch_id, proposed_run_id, *, store_root, parent_run_id, selected, epoch):
        """Reenter only an explicitly unpublished identity, never scan history."""
        try:
            inputs = _selection(selected)
            store = str(Path(store_root).resolve())
            with self.global_control._admission_lock():
                row = self._execution_row(batch_id, epoch)
                found = [binding for binding in row['run_bindings']
                    if binding.get('store_root') == store and binding.get('parent_run_id') == parent_run_id
                    and (binding.get('allocation') or {}).get('state') == 'unpublished'
                    and (binding.get('allocation') or {}).get('selected_inputs') == inputs
                    and (binding.get('allocation') or {}).get('selected_identity_sha256') == _hash(inputs)]
                if len(found) > 1:raise BatchPaused('同一未发布AUTO身份不唯一，保留待核对')
                if found:
                    if Path(found[0]['manifest_path']).exists():
                        raise BatchPaused('未发布标记已有实际manifest，须先refresh_task_run核对')
                    return found[0]['run_id'], inputs
            return proposed_run_id, inputs
        except Exception as exc:
            if isinstance(exc, BatchPaused):raise
            raise BatchPaused('原AUTO分配身份无法核对；未新建替代身份') from exc

    def mark_manifest_published(self, batch_id, run_id, *, epoch):
        """Record a durable first manifest; this metadata never grants admission."""
        try:
            initial = self._read(batch_id)
            prior = next(value for value in initial['run_bindings'] if value['run_id'] == run_id)
            allocation = prior.get('allocation')
            if not allocation:return
            path = Path(prior['manifest_path']).resolve()
            if path != Path(prior['store_root']).resolve() / 'runs' / run_id / 'manifest.json':
                raise ValueError('published manifest location changed')
            raw = path.read_bytes(); manifest = json.loads(raw)
            if (manifest.get('run_id') != run_id or _selection(manifest.get('inputs', [])) != allocation['selected_inputs']
                    or allocation['selected_identity_sha256'] != _hash(allocation['selected_inputs'])):
                raise ValueError('published original inputs differ from allocated identity')
            with self.global_control._admission_lock():
                row = self._read(batch_id)
                current = next(value for value in row['run_bindings'] if value['run_id'] == run_id)
                if (current.get('native_revision') != epoch or current.get('allocation') != allocation
                        or any(current.get(key) != prior.get(key) for key in ('store_root','parent_run_id','manifest_path'))):
                    raise BatchPaused('发布期间AUTO身份变化；未改控制权限')
                if allocation['state'] == 'unpublished':
                    current['allocation'] = dict(allocation, state='published',published_at=time.time(),
                        first_manifest_sha256=hashlib.sha256(raw).hexdigest())
                    self._write(row)
        except Exception as exc:
            if isinstance(exc, BatchPaused):raise
            raise BatchPaused('已保存原manifest，发布事实待核对；未新建替代身份') from exc

    def sources_for_records(self, batch_id, records, *, parent_run_id=None):
        with self.global_control._admission_lock():
            row = self._read(batch_id)
        frozen = set(row['source_hashes'])
        runs = {binding['run_id']: binding for binding in row['run_bindings']}
        chain, known = set(), {}
        current = parent_run_id
        while current:
            if current in chain or current not in runs:
                raise BatchPaused('本次实际来源缺少已登记父AUTO链；未按同hash搜索归属')
            chain.add(current)
            for proof in runs[current].get('source_evidence', []):
                known[(proof['source_sha256'], str(Path(proof['source_path']).resolve()))] = proof
            current = runs[current].get('parent_run_id')
        from preextract_material import file_sha
        sources, evidence = [], {}
        for record in records:
            path = Path(record['path']).resolve()
            source = file_sha(path)
            if record.get('source_hash') and record['source_hash'] != source:
                raise BatchPaused('本次selected实际字节与保存来源不一致；未发起新阶段')
            sources.append(source)
            if source in frozen:continue
            proof = record.get('_native_task_source_evidence') or known.get((source, str(path)))
            if (not isinstance(proof, dict) or proof.get('source_sha256') != source
                    or str(Path(proof.get('source_path', '')).resolve()) != str(path)
                    or proof.get('producer_run_id') not in chain
                    or proof.get('input_source_sha256') not in frozen):
                raise BatchPaused('本次成员来源缺原生父链/归档证据；未借父批名单放行')
            # This producer hook consumes direct archive provenance. A new
            # multi-step conversion needs its existing full dual-SHA evidence.
            if proof.get('relation') != 'archive_member':
                raise BatchPaused('新转换来源尚无本入口的完整原生双SHA接线；结果保留')
            evidence[source] = proof
        return _sources(sources), self._refresh_evidence(evidence.values())

    def _delivery_snapshot(self, batch_id):
        # Never call a Q reader/command while holding G's admission lock.
        try:
            from preextract_task_delivery import task_delivery_snapshot
            value = task_delivery_snapshot(self.root, batch_id=batch_id)
            if (value.get('schema') != 'task-delivery-state/1' or value.get('batch_id') != batch_id
                    or value.get('state') == 'unknown'
                    or type(value.get('control_generation')) is not int
                    or type(value.get('mapping_revision')) is not int
                    or not isinstance(value.get('run_bindings'), list)):
                raise ValueError('task delivery state is not authoritative')
            return value
        except Exception as exc:
            if isinstance(exc, BatchPaused):raise
            raise BatchPaused('本任务交付绑定状态不可核对；原结果保留') from exc

    @staticmethod
    def _delivery_sources(value, run_id):
        found = [row for row in value['run_bindings'] if row.get('run_id') == run_id]
        if len(found) > 1:
            raise BatchPaused('交付AUTO映射不唯一；未扩大来源权限')
        return _sources(found[0]['source_hashes']) if found else []

    @staticmethod
    def _ready_binding(row, run_id, epoch):
        found = [binding for binding in row['run_bindings'] if binding.get('run_id') == run_id]
        if len(found) != 1:
            raise BatchPaused('本阶段没有本任务的真实AUTO绑定；未按全局范围执行')
        binding = found[0]
        sources = _sources(binding.get('source_hashes', ()))
        delivery = binding.get('delivery_binding') or {}
        if (binding.get('native_revision') != epoch or binding.get('sources_sha256') != _hash(sources)
                or delivery.get('state') != 'ready' or delivery.get('native_revision') != epoch
                or delivery.get('sources_sha256') != binding['sources_sha256']):
            raise BatchPaused('本AUTO交付绑定未完成或控制代次已变化；结果保留待续')
        return binding

    def _check_delivery(self, value, binding, *, require_open):
        expected = binding['delivery_binding']
        if (value['control_generation'] != expected['control_generation']
                or value['mapping_revision'] < expected['mapping_revision']
                or self._delivery_sources(value, binding['run_id']) != binding['source_hashes']):
            raise BatchPaused('本AUTO交付代次或精确来源已变化；未借其他引用放行')
        mapped = next(row for row in value['run_bindings'] if row.get('run_id') == binding['run_id'])
        if (not isinstance(mapped.get('source_bindings'), list)
                or _hash(mapped['source_bindings']) != (expected.get('ack') or {}).get('source_bindings_sha256')):
            raise BatchPaused('交付派生来源摘要已变化或缺失；未按同hash补造归属')
        controls = [row for row in value.get('run_controls', []) if row.get('run_id') == binding['run_id']]
        if require_open and (value.get('admission_open') is not True or len(controls) != 1
                or controls[0].get('paused') is not False or controls[0].get('stop_pending')):
            raise BatchPaused('本任务或实际AUTO的交付准入仍关闭；结果保留')

    def bind_run(self, batch_id, run_id, *, store_root, source_hashes, parent_run_id=None, epoch=None,
                 source_evidence=(), allow_extension=False, allocation_inputs=None):
        if not isinstance(run_id, str) or not re.fullmatch(r'AUTO_[A-Za-z0-9_-]{1,120}', run_id):
            raise ValueError('actual AUTO run identity required')
        sources = _sources(source_hashes)
        store_root = Path(store_root).resolve()
        if not store_root.is_relative_to(self.root):
            raise ValueError('native task child store is outside the registered data root')
        binding = dict(run_id=run_id, store_root=str(store_root), parent_run_id=parent_run_id,
                       manifest_path=str(store_root / 'runs' / run_id / 'manifest.json'))
        if isinstance(source_evidence, (str, bytes, dict)):
            raise BatchPaused('派生来源必须提供逐项原生证据，未按父hash冒充正文来源')
        evidence = [dict(proof) for proof in source_evidence]
        try:
            before = self._delivery_snapshot(batch_id)
        except BatchPaused:
            before = None
        pending_id = uuid.uuid4().hex
        with self.global_control._admission_lock():
            row = self._execution_row(batch_id, epoch)
            derived = {proof.get('source_sha256') for proof in evidence}
            if (len(derived) != len(evidence) or derived != set(sources) - set(row['source_hashes'])
                    or any(proof.get('input_source_sha256') not in row['source_hashes'] for proof in evidence)):
                raise BatchPaused('每个派生来源须有冻结根原稿证明；未扩大WEB原稿名单')
            if parent_run_id and parent_run_id not in {r['run_id'] for r in row['run_bindings']}:
                raise ValueError('native child has no registered parent AUTO run')
            prior = next((r for r in row['run_bindings'] if r['run_id'] == run_id), None)
            if prior and any(prior.get(key) != value for key, value in binding.items()):
                raise BatchPaused('原AUTO的store/父身份已变化；未重建交付身份')
            if prior and prior.get('source_hashes') != sources:
                if (not allow_extension or not set(prior.get('source_hashes', ())) < set(sources)
                        or not (set(sources) - set(prior.get('source_hashes', ()))) <= derived):
                    raise BatchPaused('原AUTO精确来源不一致；未缩小或扩大映射')
            if prior and prior.get('allocation'):
                binding['allocation'] = dict(prior['allocation'])
                if allocation_inputs is not None and binding['allocation']['selected_inputs'] != allocation_inputs:
                    raise BatchPaused('原AUTO实际selected身份不同；未换未发布来源')
            elif allocation_inputs is not None:
                if prior:raise BatchPaused('旧AUTO缺未发布事实，不能猜测其manifest尚未创建')
                binding['allocation'] = dict(state='unpublished', selected_inputs=allocation_inputs,
                    selected_identity_sha256=_hash(allocation_inputs), allocated_at=time.time())
            previous_pending = (prior or {}).get('delivery_binding') or {}
            if (previous_pending.get('state') == 'pending' and previous_pending.get('native_revision') == epoch
                    and previous_pending.get('sources_sha256') == _hash(sources)
                    and isinstance(previous_pending.get('token'), str) and previous_pending['token']):
                pending_id = previous_pending['token']
            binding.update(source_hashes=sources, source_evidence=evidence,
                sources_sha256=_hash(sources), native_revision=epoch,
                delivery_binding=dict(state='pending', token=pending_id, native_revision=epoch,
                    sources_sha256=_hash(sources), control_generation=before['control_generation'] if before else None,
                    mapping_revision=before['mapping_revision'] if before else None, started_at=time.time()))
            if prior:row['run_bindings'][row['run_bindings'].index(prior)] = binding
            else:row['run_bindings'].append(binding)
            self._write(row)
        if before is None:
            raise BatchPaused('已保存本AUTO待绑定身份；交付控制不可读，未进入新阶段')
        try:
            existing = self._delivery_sources(before, run_id)
            if existing and not set(existing) <= set(sources):
                raise BatchPaused('交付已有AUTO来源与本次实际选择不一致；未追加权限')
            from preextract_task_delivery import bind_task_delivery_run
            result = bind_task_delivery_run(self.root, batch_id=batch_id, run_id=run_id,
                source_hashes=sources, source_evidence=evidence, binding_token=pending_id)
            if (result.get('schema') != 'task-delivery-binding/2' or result.get('binding_state') != 'registered'
                    or result.get('batch_id') != batch_id or result.get('run_id') != run_id
                    or result.get('binding_token') != pending_id
                    or result.get('control_generation') != before['control_generation']
                    or type(result.get('mapping_revision')) is not int
                    or result['mapping_revision'] < before['mapping_revision']
                    or result.get('requested_source_hashes') != sources or result.get('source_hashes') != sources
                    or result.get('source_set_sha256') != _hash(sources)
                    or not isinstance(result.get('source_bindings'), list)
                    or result.get('source_bindings_sha256') != _hash(result['source_bindings'])
                    or _lineage(result['source_bindings']) != _lineage(evidence)
                    or (result.get('native_observation') or {}).get('control_revision') != epoch
                    or (result.get('native_observation') or {}).get('binding_token') != pending_id):
                raise BatchPaused('交付绑定未形成同代次精确映射；仍保留pending')
            with self.global_control._admission_lock():
                row = self._execution_row(batch_id, epoch)
                current = next((r for r in row['run_bindings'] if r.get('run_id') == run_id), None)
                if (not current or current != binding
                        or (current.get('delivery_binding') or {}).get('token') != pending_id):
                    raise BatchPaused('绑定期间本AUTO身份或代次变化；未覆盖控制意图')
                current['delivery_binding'] = dict(current['delivery_binding'], state='ready',
                    control_generation=result['control_generation'], mapping_revision=result['mapping_revision'],
                    ack=result,
                    ready_at=time.time())
                self._write(row)
            # This changes metadata only. Neither a bind nor its return grants
            # permission to start a stage after either controller has paused.
            return self.assert_delivery_ready(batch_id, run_id, epoch=epoch)
        except Exception as exc:
            if isinstance(exc, BatchPaused):raise
            raise BatchPaused('AUTO交付绑定未完成；已保存pending身份，未启动新阶段') from exc

    def assert_delivery_ready(self, batch_id, run_id, *, epoch, source_hashes=None):
        delivery = self._delivery_snapshot(batch_id)
        with self.global_control._admission_lock():
            row = self._execution_row(batch_id, epoch)
            binding = self._ready_binding(row, run_id, epoch)
            if source_hashes is not None and not set(_sources(source_hashes)) <= set(binding['source_hashes']):
                raise BatchPaused('交付成果来源不在本AUTO实际绑定中；未按全WEB放行')
            self._check_delivery(delivery, binding, require_open=True)
            if not self._global_snapshot()['admission_open']:
                raise BatchPaused('全局仍未准入；本AUTO结果保留')
        after = self._delivery_snapshot(batch_id)
        self._check_delivery(after, binding, require_open=True)
        return binding

    def bind_saved_run(self, batch_id, run_id, *, epoch):
        # Only an already registered identity is refreshable. No lineage scan
        # or another run's selected list may manufacture the old source set.
        with self.global_control._admission_lock():
            row = self._execution_row(batch_id, epoch)
            found = [value for value in row['run_bindings'] if value.get('run_id') == run_id]
            if len(found) != 1:
                raise BatchPaused('旧AUTO不属于本WEB已有绑定；未扫描历史补建')
            binding = dict(found[0])
        try:
            path = Path(binding['manifest_path']).resolve()
            store = Path(binding['store_root']).resolve()
            if (not store.is_relative_to(self.root) or path != store / 'runs' / run_id / 'manifest.json'):
                raise ValueError('saved run store identity changed')
            allocation = binding.get('allocation') or {}
            if allocation.get('state') == 'unpublished' and not path.exists():
                inputs = _selection(allocation.get('selected_inputs', []))
                if (inputs != allocation['selected_inputs'] or _hash(inputs) != allocation['selected_identity_sha256']
                        or _sources(record['source_hash'] for record in inputs) != binding['source_hashes']):
                    raise ValueError('unpublished exact selected identity changed')
                return self.bind_run(batch_id, run_id, store_root=store,parent_run_id=binding.get('parent_run_id'),
                    source_hashes=binding['source_hashes'],source_evidence=self._refresh_evidence(binding.get('source_evidence', ())),
                    epoch=epoch,allocation_inputs=inputs)
            if allocation.get('state') == 'unpublished':
                # A crash may follow atomic manifest save but precede its fact.
                # Validate that exact file; never overwrite or call it absent.
                self.mark_manifest_published(batch_id, run_id, epoch=binding['native_revision'])
            manifest = json.loads(path.read_text(encoding='utf8'))
            if manifest.get('run_id') != run_id:
                raise ValueError('saved run identity changed')
            from preextract_material import file_sha
            actual = []
            for record in manifest.get('inputs', []):
                actual.append(file_sha(Path(record['path'])))
            sources = _sources(binding.get('source_hashes', ()))
            evidence = self._refresh_evidence(binding.get('source_evidence', ()))
            if _sources([*actual, *(proof['source_sha256'] for proof in evidence)]) != sources:
                raise ValueError('saved AUTO input hashes no longer match its exact binding')
            if any(item.get('source_hash') and item['source_hash'] not in sources for item in manifest.get('items', [])):
                raise ValueError('saved AUTO item is outside its exact source binding')
            return self.bind_run(batch_id, run_id, store_root=store, parent_run_id=binding.get('parent_run_id'),
                                source_hashes=sources, source_evidence=evidence, epoch=epoch)
        except Exception as exc:
            if isinstance(exc, BatchPaused):raise
            raise BatchPaused('旧AUTO原manifest/来源无法重核；成果保留待明确续办') from exc

    @staticmethod
    def _refresh_evidence(evidence):
        refreshed = []
        for original in evidence:
            proof = dict(original)
            path = Path(proof['manifest_path']).resolve()
            raw = path.read_bytes()
            record = json.loads(raw)
            for part in proof['record_pointer'][1:].split('/'):
                part = part.replace('~1', '/').replace('~0', '~')
                record = record[int(part)] if isinstance(record, list) else record[part]
            if not isinstance(record, dict):raise ValueError('saved source evidence record changed')
            # Mutable run progress may change the manifest bytes. The original
            # pointer and lineage stay fixed; Q validates that record and files.
            proof['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
            refreshed.append(proof)
        return refreshed

    @contextmanager
    def execution(self, batch_id, selected, *, admission_guard=None):
        from filelock import FileLock, Timeout
        lock = FileLock(str(self.root / 'private/native-task-body.lock'), timeout=0)
        self.global_control.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            lock.acquire()
        except Timeout:
            raise BatchBodyBusy('已有正文任务持有执行器；本任务保留排队') from None
        owner = None
        try:
            # Input changes are checked before a new native body pool is built.
            row = self._read(batch_id)
            from preextract_material import file_sha
            allowed = set(row['source_hashes'])
            for record in selected:
                if file_sha(Path(record['path'])) not in allowed and record.get('archive_source_hash') not in allowed:
                    raise ValueError('task source is outside the frozen input manifest')
            # Match the WEB command path's WEB -> G lock order. Slow input
            # hashing stays above this short guard; no body work runs inside it.
            with admission_guard() if admission_guard is not None else nullcontext():
                with self.global_control._admission_lock():
                    row = self.assert_running(batch_id)
                    observed = self._snapshot(row)
                    if not observed['admission_open']:
                        raise BatchPaused('全局执行尚未准入，保留本任务排队')
                    if observed['active'] or observed['unknown']:
                        raise BatchBodyBusy('本任务原执行者或已准入阶段尚未收敛')
                    others = [r for r in self._global_snapshot().get('active', [])
                              if r.get('stage') in {'preparation', 'model', 'postprocess'}]
                    if others:
                        raise BatchBodyBusy('已有原生正文阶段运行；本任务等待既有执行器收敛')
                    owner = dict(token=uuid.uuid4().hex, revision=row['control_revision'], pid=os.getpid(),
                                 create_time=psutil.Process().create_time(), started_at=time.time())
                    row['execution_owner'] = owner
                    self._write(row)
            yield owner['revision']
        finally:
            try:
                if owner is not None:
                    with self.global_control._admission_lock():
                        row = self._read(batch_id)
                        if (row.get('execution_owner') or {}).get('token') != owner['token']:
                            raise ValueError('native task execution owner changed while finishing')
                        row['execution_owner']['finished_at'] = time.time()
                        self._write(row)
            finally:
                lock.release()


class BatchGlobalTaskControl(GlobalTaskControl):
    """Same G lock/SQLite ledger, narrowed to a registered WEB task and epoch."""
    def __init__(self, root, batch_id, *, epoch):
        super().__init__(root)
        self.batch_id, self.epoch = _identity(batch_id), epoch
        self.task = NativeBatchControl(root)

    def poll_admission(self):
        """Fresh advisory supply check; actual stages retain full admission."""
        try:
            if not self.path.is_file():return False
            if super().snapshot().get('admission_open') is not True:return False
            row=self.task._execution_row(self.batch_id,self.epoch)
            from preextract_delivery_queue import queue_path
            from preextract_task_delivery import _readonly,_task,_control
            path=queue_path(self.root)
            with _readonly(path) as db:
                _task(db,self.batch_id)
                if not db.execute('SELECT 1 FROM delivery_controls WHERE scope=?',('run:'+self.batch_id,)).fetchone():
                    return False
                control=_control(db,path,'run:'+self.batch_id)
                upload=_control(db,path,'*')
            if any(value['paused'] or value['stop_pending'] for value in (control,upload)):
                return False
            expected={binding.get('delivery_binding',{}).get('control_generation')
                for binding in row['run_bindings'] if binding.get('native_revision')==self.epoch}
            return not expected or expected=={control['generation']}
        except (OSError,RuntimeError,ValueError,TypeError,KeyError,AttributeError,sqlite3.Error):
            return False

    def snapshot(self):
        result = super().snapshot()
        task = self.task.snapshot(self.batch_id)
        result['admission_open'] = (result['admission_open'] and task['admission_open']
            and task['control_revision'] == self.epoch)
        result['task_control'] = task
        bindings = [row for row in task['run_bindings'] if row.get('native_revision') == self.epoch]
        if bindings:
            try:
                delivery = self.task._delivery_snapshot(self.batch_id)
                for binding in bindings:
                    self.task._ready_binding(task, binding['run_id'], self.epoch)
                    self.task._check_delivery(delivery, binding, require_open=True)
                result['task_delivery'] = delivery
            except BatchPaused:
                result['admission_open'] = False
                result['task_delivery_waiting_reason'] = 'delivery_binding_or_admission_unavailable'
        return result

    @contextmanager
    def admission(self):
        delivery = self.task._delivery_snapshot(self.batch_id)
        registered = None
        try:
            # Q observations are outside G. Only ledger registration occurs in
            # this context; all stage bodies and transport run after it exits.
            with super().admission() as admitted:
                if admitted.connection is None or type(self.epoch) is not int or self.epoch < 0:
                    raise BatchPaused('原生全局控制记录不可用；本任务未准入')
                row = self.task._execution_row(self.batch_id, self.epoch)
                registered = _BatchAdmission(admitted, self.task, row, self.epoch, delivery)
                yield registered
            after = self.task._delivery_snapshot(self.batch_id)
            for _token, binding in registered.registrations:
                self.task._check_delivery(after, binding, require_open=True)
        except BaseException as exc:
            if registered is not None:
                for token, _binding in registered.registrations:
                    try:self.finish(token, 'not_started', 'task_delivery_admission_changed')
                    except Exception as secondary:
                        try:
                            from processing_stop import record_failure
                            record_failure(self.root, secondary, stage='batch_admission_finish')
                        except Exception:pass
            if isinstance(exc, Exception) and not isinstance(exc, GlobalPaused):
                raise BatchPaused('本任务阶段准入状态不可核对；未启动新阶段') from exc
            raise

    def begin(self, stage, **identity):
        with self.admission() as admitted:
            return admitted.begin(stage, **identity)

    def dispatch(self, send, descriptor, *, run_id, purpose=False):
        with self.http_scope(run_id, descriptor['request_sha256'], purpose=purpose):
            return send()

    @contextmanager
    def http_scope(self, run_id, request_sha256, *, purpose=False):
        token = self.begin('model', batch_id=run_id, paper_id=request_sha256,
                           name='purpose_http_admission' if purpose else 'body_http_admission')
        failure = None
        try:
            yield
        except BaseException as exc:
            failure = exc
            raise
        finally:
            try:
                self.finish(token, 'failed' if failure else 'complete', type(failure).__name__ if failure else '')
            except Exception as secondary:
                if failure is None:
                    raise
                from processing_stop import record_failure
                record_failure(self.root, secondary, stage='batch_http_lease_finish', run_id=run_id)


@contextmanager
def agent_http_scope(agent, encoded):
    binding = agent.config.get('_native_task_batch')
    if not binding:
        yield
        return
    control = BatchGlobalTaskControl(binding['root'], binding['batch_id'],
                                    epoch=agent.config.get('_native_task_epoch'))
    with control.http_scope((agent.billing_context or {}).get('run_id'),
                            hashlib.sha256(encoded).hexdigest()):
        yield
