"""Explicit, copy-only original archives. No imports from the paper scheduler.

The durable local registry is separate from the disposable query index. Ordinary
status reads never touch NAS; only the manual writer and a download do so.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import uuid

from .artifacts import ArtifactReader


MAX_BYTES = 512 * 1024 * 1024


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def checked_id(value, length=32):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{%d}' % length, value):
        raise ValueError('归档身份格式不正确')
    return value


def archival_inventory(sources, row):
    """Bind the current material to the saved source/task before retaining files."""
    files, warnings = sources.inventory(row)
    detail = row.get('detail', {})
    evidence = []
    def read(path):
        value = sources.small_json(path, with_hash=True)
        if not value:
            return None, None
        data, sha = value
        evidence.append({'path': str(path), 'sha256': sha})
        return data, sha
    try:
        work = sources.allowed(detail['work_dir'])
        material, material_sha = read(work / 'material.json')
        plan, _ = read(work / 'private/associated-upload-plan.json')
        if (not material or material.get('source', {}).get('source_sha256') != detail['source_hash']
                or not isinstance(material.get('attachments'), list)):
            raise ValueError('材料清单缺失或原稿身份不符')
        task = detail.get('task') or {}
        plan_bound = bool(plan and task and detail.get('package_hash') and
            plan.get('manifest_sha256') == detail['package_hash'] and
            plan.get('material_sha256') == material_sha and
            all(plan.get('task', {}).get(k) == task.get(k) for k in ('task_id', 'source_id', 'source_sha256', 'revision')) and
            task.get('source_sha256') == detail['source_hash'])
        if plan and not plan_bound:
            warnings.append('附件计划与保存的论文任务或材料清单身份不符，附件暂不归档')
        elif not plan and material['attachments']:
            warnings.append('附件计划缺失，保留当前材料副本但关联版本待核对')
        entries = []
        def visit(item):
            if len(entries) >= 300:
                raise ValueError('关联清单超过本期上限，尚未完整归档')
            entries.append(item)
            for member in item.get('members', []):
                visit(member)
        for item in material['attachments']:
            visit(item)
        identities = set()
        for item in entries:
            if not item.get('original_path'):
                continue
            path = sources.allowed(item['original_path'])
            if not path.is_relative_to(work / 'private'):
                raise ValueError('附件超出其论文保留目录')
            identities.add((str(path), item.get('sha256') or item.get('source_sha256')))
        if plan_bound:
            for file in plan.get('files', []):
                pair = (str(sources.allowed(file['original_path'])), file.get('sha256'))
                if pair not in identities:
                    warnings.append('附件计划存在未被材料清单确认的文件，未纳入归档')
        accepted = []
        for file in files:
            if file['kind'] == '附件':
                if (plan and not plan_bound) or (file['path'], file['expected_hash']) not in identities:
                    warnings.append('附件关联身份待核对：' + file['name'])
                    continue
                file['archive_identity_check'] = 'matched_bound_material' if plan_bound else 'current_source_material'
            accepted.append(file)
        # Preserve the original uploaded attachment archive as well as extracted
        # CAD/code/doc members. It is a separate role, not an extra delivered file.
        packages = {a['path'] for a in accepted}
        if not plan or plan_bound:
            for item in entries:
                container, sha = item.get('input_source_path'), item.get('archive_source_hash')
                if not container or not sha:
                    continue
                path = sources.allowed(container)
                if str(path) in packages:
                    continue
                packages.add(str(path))
                accepted.append(dict(path=str(path), name=path.name, kind='附件上传包',
                    expected_hash=checked_id(sha, 64), available=path.is_file(),
                    bytes=path.stat().st_size if path.is_file() else None,
                    archive_identity_check='matched_bound_material' if plan_bound else 'current_source_material'))
        return accepted, list(dict.fromkeys(warnings)), evidence
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        warnings.append('关联材料清单无法完整核对：' + str(exc))
        return [a for a in files if a['kind'] != '附件'], list(dict.fromkeys(warnings)), evidence


@contextmanager
def writer_lock(path):
    """An OS-released lock, so a crashed writer cannot leave a stale lease."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as stream:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b'0'); stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class ArchiveStore:
    def __init__(self, path):
        self.path = Path(path)

    def status(self, asset_id):
        checked_id(asset_id)
        if not self.path.is_file():
            return {'status': 'not_archived', 'files': [], 'warnings': []}
        with closing(sqlite3.connect(self.path.resolve().as_uri() + '?mode=ro', uri=True, timeout=1)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='archives'").fetchone():
                return {'status': 'not_archived', 'files': [], 'warnings': []}
            row = db.execute('SELECT payload FROM archives WHERE asset_id=?', (asset_id,)).fetchone()
        return json.loads(row[0]) if row else {'status': 'not_archived', 'files': [], 'warnings': []}

    def save(self, asset_id, value):
        checked_id(asset_id)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=5)) as db, db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS archives(asset_id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
            db.execute('INSERT OR REPLACE INTO archives VALUES(?,?)', (asset_id, encoded(value).decode('utf8')))

    def lifecycle(self, asset_id):
        """Saved per-paper storage progress. Never stat a source or NAS path."""
        checked_id(asset_id)
        if not self.path.is_file():return None
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='nas_jobs'").fetchone():return None
            row=db.execute('SELECT state,error,updated FROM nas_jobs WHERE asset_id=? ORDER BY updated DESC LIMIT 1',
                           (asset_id,)).fetchone()
        if not row:return None
        try:summary=json.loads(row[1])
        except (ValueError,TypeError):summary={}
        if not isinstance(summary,dict):summary={}
        reasons=[v.get('reason','') for v in summary.get('waiting',[])]
        if not summary and row[1]:reasons=[row[1]]
        return dict(state=row[0],updated=row[2],released_files=summary.get('released_files',0),
            removed_file_bytes=summary.get('released_bytes',0),reasons=list(dict.fromkeys(reasons)))


class ArchiveWriter:
    def __init__(self, store, sources, destination, *, reserve_bytes=1024**3, admit=None):
        self.store, self.sources = store, sources
        self.admit = admit
        self.root = Path(destination).resolve()
        self.reserve_bytes = reserve_bytes
        if any(self.root.is_relative_to(p) or p.is_relative_to(self.root) for p in sources.roots):
            raise ValueError('归档目录必须与原稿目录分离')

    def _path(self, relative):
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError('归档路径超出已登记目录')
        return path

    def _verify(self, path, sha, size):
        if path.stat().st_size != size or digest(path) != sha:
            raise ValueError('NAS 文件核验不符；保留异常文件，未覆盖')

    def _check_admission(self):
        if self.admit is not None and self.admit() is not True:
            raise InterruptedError('storage task admission changed')

    def _publish_object(self, stream, sha, size):
        self._check_admission()
        relative = f'objects/{sha[:2]}/{sha}'
        target = self._path(relative)
        if target.exists():
            self._verify(target, sha, size)
            return relative
        target.parent.mkdir(parents=True, exist_ok=True)
        pending = self._path('pending/' + uuid.uuid4().hex + '.part')
        pending.parent.mkdir(parents=True, exist_ok=True)
        try:
            with pending.open('xb') as out:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    self._check_admission()
                    out.write(block)
                out.flush(); os.fsync(out.fileno())
            self._verify(pending, sha, size)  # A fresh read through SMB, after close.
            # Windows rename does not overwrite an existing destination. All writers
            # on this PC also share the registry's OS lock.
            try:
                self._check_admission()
                pending.rename(target)
            except FileExistsError:
                self._verify(target, sha, size)
            self._verify(target, sha, size)
            return relative
        finally:
            if pending.exists():
                pending.unlink()  # Only our own temporary NAS copy, never a source.

    def _publish_manifest(self, asset_id, value):
        self._check_admission()
        data = encoded(value); sha = hashlib.sha256(data).hexdigest()
        relative = f'papers/{asset_id}/{sha}.json'
        path = self._path(relative)
        if path.exists():
            self._verify(path, sha, len(data))
            return relative, sha
        path.parent.mkdir(parents=True, exist_ok=True)
        pending = path.with_name(uuid.uuid4().hex + '.part')
        try:
            with pending.open('xb') as out:
                out.write(data); out.flush(); os.fsync(out.fileno())
            self._verify(pending, sha, len(data))
            try:
                self._check_admission()
                pending.rename(path)
            except FileExistsError:
                self._verify(path, sha, len(data))
            self._verify(path, sha, len(data))
        finally:
            if pending.exists():
                pending.unlink()
        return relative, sha

    def archive(self, row, *, max_bytes=MAX_BYTES):
        asset_id = checked_id(row.get('asset_id'))
        source_sha = checked_id(row.get('detail', {}).get('source_hash'), 64)
        expected_id = hashlib.sha256(('sha:' + source_sha).encode()).hexdigest()[:32]
        if not row.get('verified') or asset_id != expected_id:
            raise ValueError('必须使用已核验资产的原稿内容身份')
        if not 0 < max_bytes <= MAX_BYTES:
            raise ValueError('小批复制超出容量上限')
        with writer_lock(self.store.path.with_suffix('.lock')):
            self._check_admission()
            at = datetime.now(timezone.utc).isoformat()
            previous = self.store.status(asset_id)
            # Collapse retained file references rather than nesting one retry per
            # payload or losing a still-valid file after two incomplete attempts.
            ancestor = previous.get('previous_verified')
            if previous['status'] in {'verified', 'partial'} and ancestor:
                retained = {f['id']: f for f in ancestor['files']}
                retained.update({f['id']: f for f in previous['files']})
                previous = previous | {'files': list(retained.values())}
            previous = previous if previous['status'] in {'verified', 'partial'} else previous.get('previous_verified')
            if previous and Path(previous['root']) != self.root:
                raise ValueError('本期归档目标已固定；跨 NAS 迁移需单独核验')
            base = dict(asset_id=asset_id, root=str(self.root), attempted_at=at,
                        files=[], warnings=[], local_release_enabled=False)
            if previous:
                base['previous_verified'] = {k: v for k, v in previous.items() if k != 'previous_verified'}
            self.store.save(asset_id, base | {'status': 'copying'})
            try:
                result = self._copy(row, source_sha, max_bytes)
                saved = base | result | {'verified_at': datetime.now(timezone.utc).isoformat()}
                old_ids = {f['id'] for f in (previous or {}).get('files', [])}
                if old_ids.issubset({f['id'] for f in saved['files']}):
                    saved.pop('previous_verified', None)
                self._check_admission()
                self.store.save(asset_id, saved)
                return saved
            except (OSError, ValueError, TypeError) as exc:
                self.store.save(asset_id, base | {'status': 'error', 'error': str(exc)})
                raise

    def inventory(self, row):
        return archival_inventory(self.sources, row)

    def _copy(self, row, source_sha, max_bytes):
        inventory, warnings, evidence = self.inventory(row)
        originals = [a for a in inventory if a['kind'] in {'原始来源', '原稿保留副本'}]
        primary = next((a for a in originals if a['available']), None)
        if not primary:
            raise ValueError('原稿不可读取，不能完成归档')
        selected = [primary] + [a for a in inventory if a not in originals and a['available']]
        warnings = list(warnings)
        warnings.extend('文件缺失：' + a['name'] for a in inventory if a not in originals and not a['available'])
        if sum(a['bytes'] for a in selected) > max_bytes:
            raise ValueError('小批复制超出容量上限')
        self.root.mkdir(parents=True, exist_ok=True)
        required = sum(a['bytes'] for a in selected)
        if shutil.disk_usage(self.root).free < required + self.reserve_bytes:
            raise OSError('NAS 可用空间不足，保留电脑原稿')
        files = {}
        copied_bytes = 0
        for artifact in selected:
            self._check_admission()
            path = self.sources.allowed(artifact['path'])
            before = path.stat()
            with self.sources.download(artifact, max_bytes=max_bytes-copied_bytes) as stream:
                after = path.stat()
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise ValueError('源文件正在改变，稍后重试归档')
                sha = hashlib.file_digest(stream, 'sha256').hexdigest()
                size = stream.seek(0, 2); stream.seek(0)
                copied_bytes += size
                if size != before.st_size:
                    raise ValueError('源文件长度改变，稍后重试归档')
                kind = '原稿' if artifact is primary else artifact['kind']
                if kind == '原稿' and sha != source_sha:
                    raise ValueError('原稿哈希与论文身份不符')
                relative = self._publish_object(stream, sha, size)
            name = artifact['name']
            identity = hashlib.sha256(encoded([kind, name, sha])).hexdigest()[:32]
            value = files.setdefault(identity, dict(id=identity, name=name, kind=kind, sha256=sha,
                bytes=size, object_path=relative, sources=[],
                identity_check='matched_asset_source' if kind == '原稿' else
                    artifact.get('archive_identity_check', 'captured_at_archive')))
            value['sources'].append(str(path))
        file_list = sorted(files.values(), key=lambda a: (a['kind'], a['name'], a['sha256']))
        total = row.get('detail', {}).get('attachment', {}).get('total_count')
        if isinstance(total, int) and total > sum(a['kind'] == '附件' for a in file_list):
            warnings.append('登记的附件数量未全部对应到可归档原件')
        contract = row.get('detail', {}).get('attachment_contract') or {}
        if not contract.get('known'):
            warnings.append('附件清单尚未确认，当前只归档已找到的文件')
        elif contract.get('required') and not any(a['kind'] == '附件' for a in file_list):
            warnings.append('必需附件的原件尚未对应到归档清单')
        for observed in evidence:
            # This is an already registered byte identity, not a detail view.
            # Reuse the bounded original-file reader and its expected SHA check.
            path = self.sources.allowed(observed['path'])
            before = path.stat()
            with self.sources.download(dict(path=str(path),expected_hash=observed['sha256']),max_bytes=MAX_BYTES):
                after = path.stat()
                if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns) != (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
                    raise ValueError('归档期间关联材料清单改变，保留副本并等待重试')
        manifest = dict(schema_version=1, asset_id=row['asset_id'], title=row['title'],
                        source_sha256=source_sha, files=file_list, warnings=warnings,
                        scope='registered_originals', source_evidence=evidence, local_release_enabled=False)
        relative, sha = self._publish_manifest(row['asset_id'], manifest)
        return dict(status='partial' if warnings else 'verified', files=file_list, warnings=warnings,
                    manifest_path=relative, manifest_sha256=sha, input_bytes=copied_bytes,
                    source_sha256=source_sha)


class ArchiveReader:
    def __init__(self, store, runtime):
        self.store, self.runtime = store, runtime
        self.download_slots = threading.BoundedSemaphore(2)

    def summary(self, asset_id):
        value = self.store.status(asset_id)
        verified = value if value['status'] in {'verified', 'partial'} else value.get('previous_verified', value)
        files = {f['id']: f for f in value.get('previous_verified', {}).get('files', [])}
        files.update({f['id']: f for f in verified['files']})
        return {key: value[key] for key in ('status', 'verified_at', 'attempted_at', 'warnings') if key in value} | {
            'lifecycle': self.store.lifecycle(asset_id),
            'has_previous_verified': bool(value.get('previous_verified')),
            'saved_at': verified.get('verified_at'),
            'files': [{key: file[key] for key in ('id', 'name', 'kind', 'bytes', 'identity_check')} |
                {'url': f'/api/v1/archives/{asset_id}/files/{file["id"]}',
                 'historical': file['id'] not in {f['id'] for f in value['files']}}
                for file in files.values()]}

    def download(self, asset_id, file_id):
        value = self.store.status(asset_id)
        versions = [v for v in (value, value.get('previous_verified')) if v and v['status'] in {'verified', 'partial'}]
        found = next(((v, f) for v in versions for f in v['files'] if f['id'] == file_id), None)
        if not found:
            raise LookupError('此文件尚未完成归档核验')
        value, file = found
        if value['status'] not in {'verified', 'partial'} or not file:
            raise LookupError('此文件尚未完成归档核验')
        if not self.download_slots.acquire(blocking=False):
            raise BlockingIOError('已有两个文件正在读取，请稍后下载')
        try:
            sha = checked_id(file['sha256'], 64)
            root = Path(value['root'])
            artifact = dict(path=str(root / 'objects' / sha[:2] / sha),
                            name=file['name'], expected_hash=sha)
            # Reuse same-byte download verification and source-root confinement.
            stream = ArtifactReader([root], self.runtime).download(artifact, max_bytes=file['bytes'])
            return artifact, stream
        finally:
            self.download_slots.release()
