"""Validate a published group delivery for receipt without reading its source tree."""
import os
from pathlib import Path
import re

from .group_delivery import delivered_file, load_delivery
from .records import identity, read_json


def load_receipt_delivery(value, *, verify_files=True):
    """Check the immutable manifest and record per-file/evidence failures.

    Metadata failure rejects the package. A bad copy or sidecar remains an
    item/group problem so other confirmed files can still be registered.
    """
    path = Path(value).absolute()
    root = path if path.is_dir() else path.parent
    if any(part.name.casefold().startswith('.building-') for part in (root, *root.parents)):
        raise ValueError('临时构建目录不能作为交付目录')
    handoff_path, delivery = load_delivery(path, tolerate_evidence_errors=True, tolerate_file_errors=True)
    if delivery is None:
        raise ValueError('不是已发布的资料组交付目录')
    handoff = read_json(handoff_path)
    manifest = delivery['manifest']
    from .intake_handoff import _header
    library, group = _header(handoff, lexical_paths=True)
    from .group_classification import _group_id
    if handoff['group_id'] != _group_id(group, library):
        raise ValueError('交接资料组编号与来源不符')
    rows = ([handoff['body']] if handoff.get('body') else []) + [
        row for name in ('attachments', 'trash', 'unresolved') for row in handoff[name]]
    paths = set()
    for row in rows:
        relative = row['relative']
        if (not isinstance(relative, str) or not relative or '\\' in relative
                or Path(relative).is_absolute() or '..' in Path(relative).parts
                or relative == '.' or relative.casefold() in paths):
            raise ValueError('交付文件相对路径无效或重复')
        paths.add(relative.casefold())
        expected = os.path.normcase(os.path.normpath(str(group / relative)))
        actual = os.path.normcase(os.path.normpath(row['path']))
        if (expected != actual
                or row['file_id'] != identity([handoff['group_id'], relative.casefold()])[:24]):
            raise ValueError('交付文件与原始来源身份不符')
    ready = not handoff['unresolved'] and (handoff['body'] is not None or not handoff['attachments'])
    if (type(handoff.get('classification_complete')) is not bool
            or handoff['classification_complete'] != (not bool(handoff['unresolved']))
            or type(handoff.get('ready_for_receipt')) is not bool
            or handoff['ready_for_receipt'] != ready
            or any(type(manifest.get(k)) is not bool for k in
                   ('classification_complete', 'files_complete', 'ready_for_receipt'))
            or not isinstance(manifest.get('evidence_errors'), list)
            or not isinstance(manifest.get('evidence_records'), list)):
        raise ValueError('交付完整性字段与原清单不符')

    # Only declared, hash-verified sidecars may be used, including prior versions.
    valid_paths = {os.path.normcase(str(Path(p).resolve())) for p in delivery['evidence_records']}
    evidence_refs, seen = [], set()
    for ref in manifest['evidence_records']:
        try:
            relative = ref['path']
            parts = Path(relative).parts
            if (len(parts) != 3 or parts[0] != 'attachment-evidence'
                    or not re.fullmatch(r'[1-9][0-9]{0,8}\.json', parts[2])
                    or not re.fullmatch(r'[0-9a-f]{64}', ref['sha256'])):
                raise ValueError('附件证据路径或哈希无效')
            key = relative.casefold()
            if key in seen:
                raise ValueError('附件证据重复声明')
            seen.add(key)
            current = Path(delivery['root']) / relative
            if os.path.normcase(str(current.resolve())) in valid_paths:
                evidence_refs.append(dict(path=str(current), sha256=ref['sha256']))
        except (KeyError, TypeError, ValueError) as exc:
            delivery['evidence_issues'].append(dict(reference=ref.get('path') if isinstance(ref, dict) else None,
                                                    reason=type(exc).__name__))

    # Validate every declared sidecar, including those that do not match an
    # accepted item, and require one continuous declared chain per attachment.
    from .attachment_evidence_handoff import validate_evidence_handoff
    attachments = {row['file_id']: row for row in handoff['attachments']}
    versions = {file_id: {} for file_id in attachments}
    expected_keys = {identity([handoff['group_id'], handoff['handoff_id'], handoff['result_version'],
                               file_id, row['sha256']])[:32]: file_id for file_id,row in attachments.items()}
    declared_versions = {file_id:set() for file_id in attachments}
    invalid_declared = set()
    for ref in manifest['evidence_records']:
        if not isinstance(ref,dict) or not isinstance(ref.get('path'),str):
            continue
        parts=Path(ref['path']).parts
        if len(parts)==3 and parts[0]=='attachment-evidence' and parts[1] in expected_keys:
            if re.fullmatch(r'[1-9][0-9]{0,8}\.json',parts[2]):
                declared_versions[expected_keys[parts[1]]].add(int(Path(parts[2]).stem))
            else:
                invalid_declared.add(expected_keys[parts[1]])
    for issue in delivery['evidence_issues']:
        ref=issue.get('reference')
        if isinstance(ref,str):
            parts=Path(ref).parts
            if len(parts)==3 and parts[0]=='attachment-evidence' and parts[1] in expected_keys:
                issue['file_id']=expected_keys[parts[1]]
    for ref in evidence_refs:
        file_id = None
        try:
            record = read_json(ref['path'])
            source = record['source']
            file_id = source['file_id']
            row = attachments.get(file_id)
            if row is None:
                raise ValueError('附件证据没有匹配的原附件')
            expected_source = dict(handoff_id=handoff['handoff_id'],result_version=handoff['result_version'],
                                   file_id=file_id,source_sha256=row['sha256'])
            expected_key = identity([handoff['group_id'], handoff['handoff_id'], handoff['result_version'],
                                     file_id, row['sha256']])[:32]
            p = Path(ref['path'])
            version = record['evidence_version']
            if (record.get('group_id') != handoff['group_id'] or source != expected_source
                    or p.parent.name != expected_key or type(version) is not int or version < 1
                    or p.name != str(version)+'.json' or version in versions[file_id]):
                raise ValueError('附件证据版本或来源不符')
            validate_evidence_handoff(record, handoff, verify_files=False)
            versions[file_id][version] = ref
        except (OSError, ValueError, KeyError, TypeError) as exc:
            delivery['evidence_issues'].append(dict(reference=ref['path'],
                                                    file_id=file_id if isinstance(file_id, str) and file_id in attachments else None,
                                                    reason=type(exc).__name__))
    complete_evidence = []
    for file_id, chain in versions.items():
        declared=declared_versions[file_id]
        ordered=sorted(declared)
        consecutive=(bool(ordered) and ordered[0]==1 and
                     all(b==a+1 for a,b in zip(ordered,ordered[1:])))
        if file_id in invalid_declared or not chain or set(chain)!=declared or not consecutive:
            delivery['evidence_issues'].append(dict(reference=None, file_id=file_id,
                                                    reason='附件证据缺失或版本链不完整'))
        else:
            complete_evidence.extend(chain[n] for n in sorted(chain))

    verified_files, file_errors = {}, []
    if verify_files:
        for row in rows:
            try:
                verified_files[row['file_id']] = str(delivered_file(delivery, row))
            except (OSError, ValueError, KeyError, TypeError) as exc:
                declared_row = next((r for r in manifest['files'] if r.get('file_id') == row.get('file_id')), {})
                file_errors.append(dict(file_id=row.get('file_id'), relative=row.get('relative'),
                                        copied=declared_row.get('copied'), upstream_error=declared_row.get('error'),
                                        reason=str(exc) if isinstance(exc, ValueError) else type(exc).__name__))
    delivery.update(handoff_path=str(handoff_path), handoff=handoff,
                    verified_files=verified_files, file_errors=file_errors,
                    evidence_refs=complete_evidence)
    return delivery


def reload_receipt_delivery(reference, handoff=None, *, verify_files=True):
    from .records import sha_file
    path = Path(reference['path'])
    if path.name != 'delivery.json' or sha_file(path) != reference['sha256']:
        raise ValueError('接收回执的交付清单已变化')
    delivery = load_receipt_delivery(path, verify_files=verify_files)
    if (delivery['reference'] != reference
            or (handoff is not None and delivery['handoff'] != handoff)):
        raise ValueError('交付目录与接收回执不符')
    return delivery


def verify_history_copy(item, path):
    """Verify an earlier receipt's copy of the same registered source item."""
    from .intake_handoff import verify_received_file
    from .receipts import path_key
    target = path_key(path, lexical=True)
    if target == path_key(item['path'], lexical=True):
        verify_received_file(item)
        return True
    for ref in item.get('delivery_history', []):
        try:
            receipt = read_json(ref['path'])
            original = {k:v for k,v in receipt.items() if k not in {'receipt_id','record_path'}}
            if (receipt.get('receipt_id') != ref['receipt_id'] or identity(original) != ref['receipt_id']
                    or receipt.get('origin_group_id') != item.get('origin_group_id')
                    or not receipt.get('delivery_ref')):
                continue
            candidates = [old for old in receipt['items'] if old.get('state') == '已接收'
                and old.get('kind') == item['kind'] and old.get('file_id') == item['file_id']
                and isinstance(old.get('relative'), str) and old['relative'].casefold() == item['relative'].casefold()
                and old.get('sha256') == item['sha256']
                and old.get('bytes') == item['bytes'] and old.get('delivery_ref') == receipt['delivery_ref']
                and old.get('path') and path_key(old['path'], lexical=True) == target]
            if len(candidates) == 1:
                verify_received_file(candidates[0])
                return True
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return False
