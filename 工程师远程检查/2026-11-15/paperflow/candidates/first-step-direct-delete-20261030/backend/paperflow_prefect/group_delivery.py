"""One immutable folder per group/result; keep upstream identity unchanged."""
from pathlib import Path, PureWindowsPath
import os
import shutil
import uuid

from .records import identity, read_json, sha_file, write_json


SCHEMA = 'paperflow-group-delivery/1'


def _rows(handoff):
    return ([handoff['body']] if handoff.get('body') else []) + [
        row for key in ('attachments', 'trash', 'unresolved') for row in handoff[key]]


def _inside(root, relative):
    if (not isinstance(relative, str) or not relative or '\\' in relative
            or PureWindowsPath(relative).drive or Path(relative).is_absolute()
            or '..' in Path(relative).parts or relative == '.'):
        raise ValueError('交付目录中的相对路径无效')
    root = Path(root).absolute()
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError('交付文件越过目录边界')
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError('交付目录不能经过链接')
    return path


def load_delivery(value):
    """Accept a folder or its handoff.json; legacy JSON remains readable."""
    path = Path(value).absolute()
    if path.is_dir():
        root = path
    elif path.name in {'handoff.json', 'delivery.json'} and (path.parent / 'delivery.json').is_file():
        root = path.parent
    else:
        return path, None
    manifest_path = _inside(root, 'delivery.json')
    manifest = read_json(manifest_path)
    if (manifest.get('schema') != SCHEMA or manifest.get('delivery_id') !=
            identity({k: v for k, v in manifest.items() if k != 'delivery_id'})):
        raise ValueError('交付目录清单身份无效')
    handoff_path = _inside(root, 'handoff.json')
    if sha_file(handoff_path) != manifest['handoff_sha256']:
        raise ValueError('交付目录的分类清单已变化')
    h = read_json(handoff_path)
    from .intake_handoff import _header
    _header(h)
    if any(manifest[k] != h[k] for k in ('group_id', 'handoff_id', 'result_version')):
        raise ValueError('交付目录与资料组不符')
    expected = {row['file_id']: row for row in _rows(h)}
    files = manifest['files']
    if len(expected) != len(_rows(h)) or len(files) != len(expected) or {r['file_id'] for r in files} != set(expected):
        raise ValueError('交付目录文件清单不完整')
    for row in files:
        source = expected[row['file_id']]
        if (any(row[k] != source[k] for k in ('relative', 'sha256', 'bytes'))
                or type(row.get('copied')) is not bool):
            raise ValueError('交付副本与原文件身份不符')
        _inside(root, 'files/' + row['relative'])
    complete = all(row['copied'] for row in files)
    if (manifest['files_complete'] != complete
            or manifest['classification_complete'] != h['classification_complete']
            or manifest['ready_for_receipt'] != bool(h['ready_for_receipt'] and complete
                                                     and not manifest['evidence_errors'])):
        raise ValueError('交付目录的完整性状态与文件清单不符')
    evidence = []
    for ref in manifest['evidence_records']:
        p = _inside(root, ref['path'])
        if sha_file(p) != ref['sha256']:
            raise ValueError('交付附件证据已变化')
        evidence.append(str(p))
    return handoff_path, dict(root=str(root), manifest=manifest, evidence_records=evidence,
        reference=dict(path=str(manifest_path), sha256=sha_file(manifest_path)))


def checked_delivery(reference, handoff):
    path = Path(reference['path'])
    if path.name != 'delivery.json' or sha_file(path) != reference['sha256']:
        raise ValueError('已接收的交付目录清单已变化')
    handoff_path, delivery = load_delivery(path)
    if delivery is None or read_json(handoff_path) != handoff:
        raise ValueError('交付目录不属于本次接收清单')
    return delivery


def delivered_file(delivery, row):
    """Resolve only a declared, verified copy; never fall back to the source."""
    matches = [r for r in delivery['manifest']['files'] if r['file_id'] == row['file_id']]
    if len(matches) != 1 or any(matches[0][k] != row[k] for k in ('relative', 'sha256', 'bytes')):
        raise ValueError('文件不属于当前交付目录')
    if not matches[0]['copied']:
        raise ValueError('该文件未形成可核验的交付副本')
    path = _inside(delivery['root'], 'files/' + row['relative'])
    before = path.stat()
    if before.st_size != row['bytes'] or sha_file(path) != row['sha256']:
        raise ValueError('交付副本大小或哈希不符')
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError('交付副本核验期间发生变化')
    return path


def package_result(result, output_dir):
    """Publish copies last by directory rename; journal/model outcomes stay reusable."""
    key = 'handoff' if result.get('handoff') else 'unreceived'
    source = Path(result[key])
    handoff = read_json(source)
    root = Path(output_dir).resolve() / 'deliveries' / handoff['group_id']
    root.mkdir(parents=True, exist_ok=True)
    sidecars = {}
    for latest in result.get('evidence_handoffs', []):
        latest = Path(latest)
        for previous in latest.parent.glob('*.json'):
            if previous.stem.isdecimal() and int(previous.stem) <= int(latest.stem):
                relative = 'attachment-evidence/' + latest.parent.name + '/' + previous.name
                sidecars[relative] = (previous, sha_file(previous))
    package_key = identity([sha_file(source), sorted((k, v[1]) for k, v in sidecars.items()),
                            result.get('evidence_errors', [])])[:24]
    destination = root / package_key
    # A partial copy is retained as an immutable attempt, but does not prevent
    # a later copy-only recovery. Classification and model calls are not rerun.
    prior_packages = ([destination] if destination.exists() else []) + sorted(root.glob(package_key + '-*'))
    for prior in prior_packages:
        _, previous = load_delivery(prior)
        if previous['manifest']['files_complete']:
            destination = prior
            break
    else:
        if destination.exists():
            destination = root / (package_key + '-' + uuid.uuid4().hex[:12])
    if not destination.exists():
        staging = root / ('.building-' + uuid.uuid4().hex)
        staging.mkdir()
        (staging / 'files').mkdir()
        shutil.copyfile(source, staging / 'handoff.json')
        files = []
        for row in _rows(handoff):
            entry = {k: row[k] for k in ('file_id', 'relative', 'bytes', 'sha256')}
            entry.update(copied=False, error=None)
            try:
                from .intake_handoff import _safe_original
                original = _safe_original(row, Path(handoff['group_root']).resolve())
                target = _inside(staging, 'files/' + row['relative'])
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(original['path'], target)
                if target.stat().st_size != row['bytes'] or sha_file(target) != row['sha256']:
                    raise ValueError('复制过程中原文件发生变化')
                entry['copied'] = True
            except (OSError, ValueError, KeyError, TypeError) as exc:
                entry['error'] = type(exc).__name__
            files.append(entry)
        evidence = []
        for relative, (original, digest) in sorted(sidecars.items()):
            target = _inside(staging, relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(original, target)
            if sha_file(target) != digest:
                raise ValueError('复制期间附件证据发生变化')
            evidence.append(dict(path=relative, sha256=digest))
        manifest = dict(schema=SCHEMA, group_id=handoff['group_id'], handoff_id=handoff['handoff_id'],
            result_version=handoff['result_version'], handoff_sha256=sha_file(source), files=files,
            evidence_records=evidence, evidence_errors=result.get('evidence_errors', []),
            files_complete=all(r['copied'] for r in files),
            classification_complete=handoff['classification_complete'],
            ready_for_receipt=handoff['ready_for_receipt'] and all(r['copied'] for r in files)
                and not result.get('evidence_errors'))
        manifest['delivery_id'] = identity(manifest)
        write_json(staging / 'delivery.json', manifest)
        load_delivery(staging)
        os.rename(staging, destination)
    handoff_path, delivery = load_delivery(destination)
    for row in _rows(handoff):
        if next(r for r in delivery['manifest']['files'] if r['file_id'] == row['file_id'])['copied']:
            delivered_file(delivery, row)
    result['delivery_dir'] = str(destination)
    result['delivery_complete'] = delivery['manifest']['ready_for_receipt']
    result['delivery_errors'] = [r for r in delivery['manifest']['files'] if not r['copied']]
    result[key] = str(handoff_path)
    result['evidence_handoffs'] = delivery['evidence_records']
    return result
