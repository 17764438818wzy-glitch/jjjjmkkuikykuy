"""Recover only the pre-dispatch initialization transaction, preserving evidence."""
import hashlib
import json
import os
import uuid
from pathlib import Path

from filelock import FileLock
from preextract_material import write_json


def _json_bytes(value):
    # Match the existing write_json TextIO writer, including the native line
    # endings and no final newline. These hashes bind actual persisted bytes.
    return json.dumps(value, ensure_ascii=False, indent=2).replace('\n', os.linesep).encode('utf-8')


def _sha(blob):
    return hashlib.sha256(blob).hexdigest()


def json_sha(value):
    return _sha(_json_bytes(value))


def initialize_plan(model, files, *, binding):
    model = Path(model)
    if model.is_symlink() or (hasattr(model, 'is_junction') and model.is_junction()):
        raise ValueError('分段初始化不能使用链接目录')
    journal_path = model / 'partition-initialization.json'
    expected = {name: _json_bytes(value) for name, value in files.items()}
    fingerprints = {name: _sha(value) for name, value in expected.items()}
    with FileLock(str(model.parent / 'partition-initialization.lock'), timeout=0):
        if (model / 'partition-state.json').exists():
            raise ValueError('已有分段请求状态，不能重新初始化')
        journal = json.loads(journal_path.read_text(encoding='utf-8')) if journal_path.exists() else None
        if journal and (journal.get('binding') != binding or journal.get('files') != fingerprints
                        or journal.get('status') != 'initializing'):
            raise ValueError('分段初始化身份、计划或状态不一致，保留请求证据')
        expected_dirs = {str(Path(name).parent) for name in files if len(Path(name).parts) > 1}
        existing = []
        for path in model.rglob('*'):
            if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
                raise ValueError('分段初始化不能使用链接目录')
            relative = path.relative_to(model).as_posix()
            if relative.startswith('initialization-evidence/'):
                if not journal:raise ValueError('分段初始化保留证据尚无绑定日记')
                continue
            if path.is_dir():
                if relative not in expected_dirs | {'initialization-evidence'}:
                    raise ValueError('分段初始化发现未知请求目录，不能覆盖')
                continue
            if relative == journal_path.name:continue
            if relative == journal_path.name + '.tmp':
                raise ValueError('分段初始化日记未完整落盘，需核对绑定')
            target = relative[:-4] if relative.endswith('.tmp') else relative
            if target not in expected:
                raise ValueError('分段初始化发现模型请求或未知证据，不能自动重发')
            raw = path.read_bytes()
            if raw != expected[target] and not (relative.endswith('.tmp') and expected[target].startswith(raw)):
                raise ValueError('分段初始化残留与当前计划不一致，不能覆盖')
            existing.append((path, relative, raw))
        if journal is None:
            journal = dict(schema='preextract-partition-initialization/1', binding=binding,
                           files=fingerprints, status='initializing', preserved=[])
            write_json(journal_path, journal)
        for saved in journal['preserved']:
            backup = Path(saved['preserved_path']).resolve()
            if not backup.is_relative_to((model / 'initialization-evidence').resolve()) or _sha(backup.read_bytes()) != saved['sha256']:
                raise ValueError('分段初始化原始证据发生变化')
        for path, relative, raw in existing:
            if not relative.endswith('.tmp'):continue
            if any(row['path'] == relative and row['sha256'] == _sha(raw) for row in journal['preserved']):continue
            backup = model / 'initialization-evidence' / (uuid.uuid4().hex + '.bin')
            backup.parent.mkdir(exist_ok=True)
            with backup.open('xb') as output:
                output.write(raw);output.flush();os.fsync(output.fileno())
            journal['preserved'].append(dict(path=relative, sha256=_sha(raw), preserved_path=str(backup.resolve())))
            write_json(journal_path, journal)
        # State commits last. A missing state after a completed transaction is
        # not considered proof that a formerly dispatched request never ran.
        for name, value in files.items():
            path = model / name
            if not path.exists():write_json(path, value)
        journal['status'] = 'complete'
        write_json(journal_path, journal)
