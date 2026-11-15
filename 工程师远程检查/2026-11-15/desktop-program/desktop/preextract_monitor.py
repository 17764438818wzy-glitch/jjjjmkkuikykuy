"""Read saved extraction evidence without importing any runner or network client.

``verified`` means an identity-bound, complete saved delivery receipt, not a new
server probe. Only the small manifest is hashed; source files and model payloads
are never opened. A failed/partial read raises so the UI can keep its last view.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re


class PreextractMonitorError(ValueError):
    """Safe, displayable error; never contains raw JSON or provider errors."""


_LABELS = {
    'pending': '待处理', 'preparing': '材料准备记录',
    'material_prepared': '材料已准备', 'adapting_images': '图片适配记录',
    'planning_input': '输入规划记录', 'extracting_once': '模型待响应（状态记录）',
    'extracting_parts': '分段读取（状态记录）', 'assembling_parts': '分段装配记录',
    'synthesizing_parts': '全文统合（状态记录）',
    'candidate_received': '响应已保存', 'validating_candidate': '候选校验记录',
    'packaging': '脱敏与封包记录', 'packaged': '已封包，待投递确认',
    'delivering': '投递／读回待确认', 'verified': '已入库并验证（保存回执）',
    'attention': '需要核对',
}
_STAGES = {
    'registered': 'pending', 'preparing': 'preparing',
    'material_prepared': 'material_prepared', 'adapting_images': 'adapting_images',
    'checking_request': 'planning_input', 'planning_input': 'planning_input',
    'extracting_once': 'extracting_once', 'extracting_parts': 'extracting_parts',
    'assembling_parts': 'assembling_parts', 'reconciling_parts': 'synthesizing_parts',
    'synthesizing_parts': 'synthesizing_parts',
    'candidate_received': 'candidate_received', 'candidate_imported': 'candidate_received',
    'reusing_saved_response': 'candidate_received',
    'validating_candidate': 'validating_candidate', 'candidate_validated': 'validating_candidate',
    'redacting_and_packaging': 'packaging', 'package_verified': 'packaged',
    'delivering_and_reading_back': 'delivering',
    'ingested_and_verified': 'attention', 'candidate_requires_attention': 'attention',
    'candidate_excluded': 'attention', 'saved_model_request_incomplete': 'attention',
    'stopped': 'attention', 'failed': 'attention',
}
_MODEL_STATES = {'request_in_flight', 'response_complete', 'partial_response',
                 'request_failed_or_uncertain', 'parts_pending', 'synthesis_pending', 'assembled'}
_MODEL_LABELS = {
    'not_requested': '未见请求记录', 'unknown': '请求状态待核对',
    'request_in_flight': '待响应（请求记录）', 'response_complete': '完整响应已保存',
    'partial_response': '响应未完整', 'request_failed_or_uncertain': '请求失败或结果未决',
    'parts_pending': '分段待完成', 'synthesis_pending': '统合待完成', 'assembled': '装配已保存',
}
_DELIVERY_STATES = {'pending', 'uncertain', 'complete'}
_DELIVERY_STATUS_LABELS = {
    'not_submitted': '未投递（无回执记录）', 'pending': '投递待确认',
    'uncertain': '投递结果未决', 'complete': '完整回执待核对', 'unknown': '回执状态待核对',
}
_DELIVERY_LABELS = {'registered': '已登记待投递', 'publishing': '上传待确认',
                    'readback': '内容读回待确认', 'activating': '启用待确认',
                    'retrieval': '检索待确认', 'verified': '保存的验证记录'}
_IDENTITY = ('task_id', 'source_id', 'source_sha256')


def _stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec='seconds') if value else ''


def _time(value):
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and 0 < value < 32503680000 else 0.0


def _text(value, limit=300):
    return str(value).replace('\r', ' ').replace('\n', ' ')[:limit] if isinstance(value, (str, int)) else ''


class PreextractMonitorReader:
    """Project one selection into paper rows, preserving every valid attempt."""

    def __init__(self, data_root):
        self.data_root = Path(data_root).resolve()

    def _inside(self, value, *, base=None):
        path = Path(value)
        if not path.is_absolute():
            path = (base or self.data_root) / path
        path = path.resolve()
        if not path.is_relative_to(self.data_root):
            raise PreextractMonitorError('监控路径超出数据目录，已拒绝读取')
        return path

    def _json(self, path, *, required=False, kind=dict):
        path = self._inside(path)
        try:
            before = path.stat()
            if before.st_size > 16 * 1024 * 1024:
                raise PreextractMonitorError('状态文件过大，保留上次结果')
            raw = path.read_bytes()
            after = path.stat()
            if (before.st_mtime_ns, before.st_size) != (after.st_mtime_ns, after.st_size):
                raise PreextractMonitorError('状态文件更新中，保留上次结果')
            value = json.loads(raw.decode('utf-8-sig'))
            if not isinstance(value, kind):
                raise PreextractMonitorError('状态文件结构不完整，保留上次结果')
            return value, after.st_mtime, raw
        except FileNotFoundError:
            if not required:
                return None, 0.0, b''
        except (OSError, UnicodeError, json.JSONDecodeError):
            pass
        raise PreextractMonitorError('状态文件暂时不可读或写入未完成：' + path.name)

    @staticmethod
    def _locator(item):
        checkpoint = item.get('prepared_checkpoint')
        return (checkpoint.get('path') if isinstance(checkpoint, dict) else None) or item.get('work_dir')

    def _selection(self, directory):
        prepared = self._inside(directory / 'prepared-selection.json')
        fallback = self._inside(directory / 'selection.json')
        path = prepared if prepared.is_file() else fallback
        rows, updated, _ = self._json(path, kind=list)
        if rows is None:
            return None
        if any(not isinstance(row, dict) for row in rows):
            raise PreextractMonitorError('批次名单结构不完整，保留上次结果')
        if any(not self._locator(row) for row in rows):
            if path == fallback:
                return None
            raise PreextractMonitorError('已准备名单缺少材料路径，保留上次结果')
        return path, rows, updated

    def discover(self):
        private = self._inside(self.data_root / 'private')
        if not private.exists():
            return []
        batches = []
        try:
            directories = sorted(private.iterdir(), key=lambda p: p.name, reverse=True)
        except OSError:
            raise PreextractMonitorError('批次目录暂时不可读，保留上次结果') from None
        for directory in directories:
            directory = self._inside(directory)
            if not directory.is_dir():
                continue
            selected = self._selection(directory)
            if selected is not None:
                path, rows, updated = selected
                # Validate every locator before exposing a usable batch.
                for row in rows:
                    self._work(row, directory)
                batches.append((updated, dict(id=directory.name, label=directory.name, path=str(path), count=len(rows))))
        return [batch for _, batch in sorted(batches, key=lambda entry: (entry[0], entry[1]['id']), reverse=True)]

    def _work(self, item, directory):
        checkpoint = item.get('prepared_checkpoint')
        if isinstance(checkpoint, dict) and checkpoint.get('path'):
            path = self._inside(checkpoint['path'], base=directory)
            if path.name != 'material.json':
                raise PreextractMonitorError('材料定位不是已知检查点，已拒绝读取')
            return path.parent
        return self._inside(item['work_dir'], base=directory)

    def read(self, batch_id):
        if not isinstance(batch_id, str) or batch_id in {'', '.', '..'} or any(c in batch_id for c in '/\\:'):
            raise PreextractMonitorError('批次标识无效')
        directory = self._inside(self.data_root / 'private' / batch_id)
        selected = self._selection(directory)
        if selected is None:
            raise PreextractMonitorError('未找到可定位的预提取批次')
        path, items, updated = selected
        rows = [self._row(item, directory, index) for index, item in enumerate(items, 1)]
        counts = dict(total=len(rows), packaged=sum(row['packaged'] for row in rows),
                      verified=sum(row['stage'] == 'verified' for row in rows),
                      attention=sum(row['stage'] == 'attention' for row in rows))
        counts['pending'] = counts['total'] - counts['verified'] - counts['attention']
        updated = max([updated] + [row.pop('_updated') for row in rows])
        return dict(batch_id=batch_id, label=batch_id, path=str(path), rows=rows,
                    counts=counts, updated_at=_time(updated),
                    evidence_scope='本地保存状态及回执；未发起模型请求或服务器复核')

    def _row(self, item, directory, number):
        work = self._work(item, directory)
        source = item.get('source_sha256')
        if not isinstance(source, str) or not re.fullmatch(r'[0-9a-fA-F]{64}', source):
            raise PreextractMonitorError('论文名单缺少有效来源身份，保留上次结果')
        active, active_time, _ = self._json(work / 'active-revision.json')
        active_path = None
        if active and active.get('schema') == 'preextract-active-revision/1' and active.get('source_sha256') == source:
            active_path = self._inside(active.get('work_dir', ''), base=work)
            if active_path.parent != work / 'revisions':
                active_path = None
        attempts = [self._attempt(work, source, name='原始尝试')]
        revisions = self._inside(work / 'revisions')
        try:
            children = sorted(revisions.iterdir()) if revisions.is_dir() else []
        except OSError:
            raise PreextractMonitorError('修订目录暂时不可读，保留上次结果') from None
        ignored = 0
        for child in children:
            child = self._inside(child)
            if not child.is_dir():
                continue
            meta, created, _ = self._json(child / 'revision.json')
            if not meta:
                continue
            parent = self._inside(meta.get('parent', ''), base=child)
            if (meta.get('schema') != 'preextract-authorized-revision/1' or parent != work
                    or child.parent != work / 'revisions' or meta.get('source_sha256') != source
                    or meta.get('mode') not in {'rerun', 'candidate_import'}):
                ignored += 1
                continue
            attempt = self._attempt(child, source, name='修订 ' + child.name, created=created)
            if not attempt.pop('_identity_ok'):
                ignored += 1
                continue
            attempt['_active'] = child == active_path
            attempts.append(attempt)
        attempts.sort(key=lambda row: (row['_order'], row.get('_active', False)))
        chosen = dict(attempts[-1])
        history = [{key: row[key] for key in ('attempt', 'stage', 'label', 'model_status',
            'delivery_status', 'model_label', 'delivery_label', 'verified_targets', 'total_targets', 'updated_at', 'work_dir')}
            for row in attempts]
        detail = [f"{row['attempt']}：{row['label']}；更新 {_stamp(row['updated_at']) or '未知'}" for row in history]
        if chosen.get('_detail'):
            detail.append(chosen['_detail'])
        if ignored:
            detail.append(f'另有 {ignored} 个修订身份不匹配，未用作当前进度')
        if active and not active_path:
            detail.append('激活修订指针未通过身份与路径核对')
        if active_time:
            chosen['_updated'] = max(chosen['_updated'], active_time)
        chosen.update(id=str(item.get('number', number)) + ':' + source[:16],
            number=item.get('number', number), name=_text(item.get('name') or item.get('label') or f'论文 {number}'),
            attempts=history, detail='\n'.join(detail), source_path=_text(item.get('path'), 2000))
        for key in ('_order', '_active', '_identity_ok', '_detail'):
            chosen.pop(key, None)
        return chosen

    def _attempt(self, work, source, *, name, created=0):
        status, status_time, _ = self._json(work / 'status.json')
        request, request_time, _ = self._json(work / 'model' / 'request-state.json')
        partition, partition_time, _ = self._json(work / 'model' / 'partition-state.json')
        receipt, receipt_time, _ = self._json(work / 'delivery-receipt.json')
        status, request, partition = status or {}, request or {}, partition or {}
        identity_ok = not status.get('source_sha256') or status['source_sha256'] == source
        raw_stage = status.get('stage')
        stage = _STAGES.get(raw_stage, 'pending')
        model = request.get('status') or partition.get('status')
        model = model if model in _MODEL_STATES else 'unknown' if model else 'not_requested'
        detail = ''
        if raw_stage is None and self._inside(work / 'material.json').is_file():
            stage = 'material_prepared'
        if model == 'request_in_flight' and stage not in {'extracting_parts', 'synthesizing_parts'}:
            stage = 'extracting_once'
        elif model in {'partial_response', 'request_failed_or_uncertain'}:
            stage = 'attention'
            detail = '模型结果未完整确认；已有响应与请求历史保留'
        elif model == 'response_complete' and stage in {'pending', 'material_prepared', 'extracting_once'}:
            stage = 'candidate_received'
        if partition:
            part_status, part_time, part_detail = self._parts(work, partition)
            request_time = max(request_time, part_time)
            detail = part_detail
            if part_status in {'partial_response', 'request_failed_or_uncertain'}:
                model, stage = part_status, 'attention'
            elif stage in {'pending', 'material_prepared', 'candidate_received'}:
                stage = 'synthesizing_parts' if partition.get('status') == 'synthesis_pending' else 'extracting_parts'
        package, package_time = self._package(work, source)
        packaged = bool(package)
        verified, total = 0, len(package['routes']) if package else 0
        delivery = 'not_submitted'
        if package and stage not in {'attention', 'delivering'}:
            stage = 'packaged'
        if receipt:
            delivery = receipt.get('status') if receipt.get('status') in _DELIVERY_STATES else 'unknown'
            bound = package and receipt.get('task') == package['task'] and receipt.get('manifest_sha256') == package['manifest_sha256']
            if bound:
                targets = receipt.get('targets', {})
                if not isinstance(targets, dict):
                    raise PreextractMonitorError('投递目标结构不完整，保留上次结果')
                verified = sum(self._target_verified(targets.get(route['code']), route, package['needs_images'])
                               for route in package['routes'])
            if bound and delivery == 'complete' and total and verified == total:
                stage = 'verified'
            elif not bound or delivery in {'complete', 'uncertain', 'unknown'}:
                stage = 'attention'
            else:
                stage = 'delivering'
            detail = ('封包与回执身份未完整匹配' if not bound else
                      _DELIVERY_LABELS.get(receipt.get('stage'), '投递状态记录')) + f'；完整目标验证 {verified}/{total}'
        elif raw_stage in {'ingested_and_verified', 'package_verified'} and not package:
            stage = 'attention'
            detail = '完成状态缺少完整封包或回执证据'
        if not identity_ok:
            stage, packaged, verified = 'attention', False, 0
            detail = '本尝试的来源身份与论文名单不一致'
        updated = max(status_time, request_time, partition_time, receipt_time, package_time, created,
                      _time(request.get('finished_at')), _time(request.get('started_at')))
        # Status file mtimes can be rewritten by retries of the old workspace.
        # A request's start (or the revision's creation) identifies the attempt.
        order = _time(request.get('started_at')) or created or status_time
        return dict(attempt=name, stage=stage, label=_LABELS[stage], model_status=model,
            model_label=_MODEL_LABELS[model],
            delivery_label=_LABELS['verified'] if stage == 'verified' else _DELIVERY_STATUS_LABELS[delivery],
            delivery_status=delivery, verified_targets=verified, total_targets=total,
            updated_at=_time(updated), work_dir=str(work), packaged=packaged,
            _updated=updated, _order=order, _identity_ok=identity_ok, _detail=detail)

    def _parts(self, work, partition):
        rows = partition.get('requests', [])
        recoveries = partition.get('recoveries', [])
        if not isinstance(rows, list) or not isinstance(recoveries, list):
            raise PreextractMonitorError('分段请求索引不完整，保留上次结果')
        completed, failed, waiting, updated = 0, None, 0, 0.0
        for row in rows:
            if not isinstance(row, dict) or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}', str(row.get('id', ''))):
                raise PreextractMonitorError('分段请求定位无效')
            directory = self._inside(work / 'model' / row['id'])
            for recovery in recoveries:
                if isinstance(recovery, dict) and recovery.get('id') == row['id'] and recovery.get('directory'):
                    directory = self._inside(recovery['directory'], base=work / 'model')
            if not directory.is_relative_to(work / 'model' / row['id']):
                raise PreextractMonitorError('分段修订路径不属于对应请求')
            state, timestamp, _ = self._json(directory / 'request-state.json')
            updated = max(updated, timestamp)
            state = state or {}
            value = state.get('status')
            if value == 'response_complete': completed += 1
            elif value in {'partial_response', 'request_failed_or_uncertain'}: failed = value
            else: waiting += 1
        detail = f'分段／统合已登记 {len(rows)} 个请求，完整响应 {completed}，待确认 {waiting}'
        return failed, updated, detail

    def _package(self, work, source):
        prepared, updated, _ = self._json(work / 'private' / 'prepared-publication.json')
        if prepared is None:
            prepared, updated, _ = self._json(work / 'publication.json')
        if not prepared:
            return None, updated
        task = prepared.get('task', {})
        validation = prepared.get('validation', {})
        if (not isinstance(task, dict) or not isinstance(validation, dict) or task.get('source_sha256') != source
                or any(not task.get(key) for key in _IDENTITY) or not isinstance(task.get('revision'), int)
                or validation.get('status') != 'publication_ready' or not prepared.get('package_path')):
            return None, updated
        package = self._inside(prepared['package_path'], base=work)
        ready, ready_time, _ = self._json(package / 'READY.json')
        manifest, manifest_time, manifest_raw = self._json(package / 'manifest.json')
        profile, profile_time, _ = self._json(package / 'profile.json')
        assets, assets_time, _ = self._json(package / 'assets.json')
        updated = max(updated, ready_time, manifest_time, profile_time, assets_time)
        if any(record is None for record in (ready, manifest, profile, assets)):
            return None, updated
        digest = validation.get('manifest_sha256')
        if (not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest)
                or ready.get('status') != 'ready' or ready.get('manifest_sha256') != digest
                or hashlib.sha256(manifest_raw).hexdigest() != digest
                or manifest.get('revision') != f"r{task['revision']:03d}"
                or any(record.get(key) != task[key] for record in (ready, manifest, profile, assets) for key in _IDENTITY)):
            return None, updated
        catalog = prepared.get('catalog', {})
        majors = catalog.get('majors', []) if isinstance(catalog, dict) else []
        candidates, asset_rows = profile.get('routing_candidates'), assets.get('items')
        if (not isinstance(majors, list) or not isinstance(candidates, list) or not isinstance(asset_rows, list)
                or any(not isinstance(row, dict) for row in majors + candidates + asset_rows)):
            return None, updated
        # Same immutable storage and final-routing semantics as package.resolve_routes
        # and preextract_handoff._receipt_complete, without importing their writers.
        modes = [row.get('storage', 'image' if row.get('file') else 'description') for row in asset_rows]
        if any(not isinstance(mode, str) for mode in modes):
            return None, updated
        needs_images = 'image' in modes
        entries = {row.get('code'): row for row in majors if isinstance(row.get('code'), str)}
        routes, seen = [], set()
        for candidate in candidates:
            code = candidate.get('code')
            if not isinstance(code, str) or code not in entries:
                return None, updated
            if code in seen:
                continue
            seen.add(code)
            route = entries[code]
            if not route.get('paper_dataset_id') or (needs_images and not route.get('image_dataset_id')):
                return None, updated
            routes.append(route)
        if not routes:
            return None, updated
        return dict(task=task, manifest_sha256=digest, routes=routes, needs_images=needs_images), updated

    @staticmethod
    def _target_verified(target, route, needs_images):
        if not isinstance(target, dict) or target.get('verified') is not True or target.get('retrieval_verified') is not True:
            return False
        roles = [('body', 'paper_dataset_id')]
        if needs_images:
            roles.append(('images', 'image_dataset_id'))
        return all(isinstance(target.get(role), dict) and target[role].get('document_id')
                   and target[role].get('dataset_id') == route[key] for role, key in roles)
