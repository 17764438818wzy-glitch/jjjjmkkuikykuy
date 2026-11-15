"""Bind official tusd uploads to a first-step task and restore its folder tree."""
from contextlib import closing
import filecmp
import hashlib
import json
import os
from pathlib import PurePosixPath
import shutil
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .first_step_control import ControlError, _relative, _uuid, discover_groups
from .records import identity


class TusFolderIntake:
    def __init__(self, control):
        self.control = control

    def authorize(self, method, tus_id, headers):
        """Bind normal browser tus operations to the task and upload window."""
        task_id = _uuid(headers.get('x-task-id'))
        upload_id = _uuid(headers.get('x-folder-upload-id'))
        writer_id = _uuid(headers.get('x-writer-id'))
        with closing(self.control.summary._connect()) as db:
            batch = self.control._upload(db, upload_id)
            if batch['task_id'] != task_id:
                raise ControlError('上传地址与当前工作间不一致', 403)
            if method == 'POST':
                if batch['state'] != 'sealed':
                    raise ControlError('上传清单尚未锁定')
            else:
                row = db.execute('SELECT * FROM tus_manifest WHERE upload_id=? AND tus_id=?',
                                 (upload_id, tus_id)).fetchone()
                if row is None or row['writer_id'] != writer_id:
                    raise ControlError('上传地址不属于当前工作间或窗口', 403)
                if method == 'PATCH' and batch['state'] != 'sealed':
                    raise ControlError('上传批次已结束')

    def release_failed_create(self, invocation_id):
        """Release a reservation only after its POST returned failure and no resource exists."""
        c = self.control
        with closing(c.summary._connect()) as db:
            row = db.execute('SELECT * FROM tus_manifest WHERE create_invocation_id=?', (invocation_id,)).fetchone()
        if not row:
            return False
        upload_id, tus_id = row['upload_id'], row['tus_id']
        with c._lock('upload', upload_id), closing(c.summary._connect()) as db:
            row = db.execute('SELECT * FROM tus_manifest WHERE create_invocation_id=?', (invocation_id,)).fetchone()
            if not row or row['tus_id'] != tus_id:
                return False
            request = Request(c.tus_backend_url + tus_id, method='HEAD', headers={'Tus-Resumable': '1.0.0'})
            try:
                with urlopen(request, timeout=3):
                    return False
            except HTTPError as exc:
                if exc.code != 404:
                    return False
            except OSError:
                return False
            blob = c.tus_dir / tus_id
            info = c.tus_dir / (tus_id + '.info')
            if info.exists() or info.is_symlink():
                return False
            archived = None
            if blob.exists() or blob.is_symlink():
                if blob.is_symlink() or not blob.is_file() or blob.stat().st_size != 0:
                    return False
                archived = c.data / 'uploads' / upload_id / 'failed-create' / tus_id
                archived.parent.mkdir(parents=True, exist_ok=True)
                if archived.exists():
                    return False
                os.rename(blob, archived)
            try:
                with db:
                    db.execute('BEGIN IMMEDIATE')
                    updated = db.execute("UPDATE tus_manifest SET tus_id=NULL,create_request_id=NULL,writer_id=NULL,"
                        "create_invocation_id=NULL,state='pending' WHERE create_invocation_id=? AND tus_id=? "
                        "AND state='uploading'", (invocation_id, tus_id)).rowcount
                    if updated:
                        db.execute('INSERT INTO retired_upload_creates (invocation_id) VALUES (?)', (invocation_id,))
                    elif archived is not None:
                        os.rename(archived, blob)
            except Exception:
                if archived is not None and archived.exists() and not blob.exists():
                    os.rename(archived, blob)
                raise
            return bool(updated)

    def manifest(self, upload_id, files):
        upload_id = _uuid(upload_id)
        if not files:
            raise ControlError('文件清单不能为空', 400)
        c = self.control
        with c._lock('upload', upload_id), closing(c.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            upload = c._upload(db, upload_id)
            if upload['state'] not in {'uploading', 'sealed', 'finished'}:
                raise ControlError('文件清单已锁定')
            rows, seen = [], set()
            for item in files:
                relative = _relative(item['relative_path'], upload['root_name']).as_posix()
                size = item['size']
                if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                    raise ControlError('文件大小无效', 400)
                key = relative.casefold()
                if key in seen:
                    raise ControlError('同一清单中有重复路径')
                seen.add(key)
                existing = db.execute('SELECT * FROM tus_manifest WHERE upload_id=? AND path_key=?',
                                      (upload_id, key)).fetchone()
                if existing:
                    if existing['relative_path'] != relative or existing['bytes'] != size:
                        raise ControlError('不同文件使用了同一路径')
                else:
                    if upload['state'] != 'uploading':
                        raise ControlError('文件清单已锁定')
                    rows.append((upload_id, key, relative, size))
            db.executemany('INSERT INTO tus_manifest (upload_id,path_key,relative_path,bytes) VALUES (?,?,?,?)', rows)
            count = db.execute('SELECT COUNT(*) FROM tus_manifest WHERE upload_id=?', (upload_id,)).fetchone()[0]
            db.execute('UPDATE tasks SET updated_at_ns=? WHERE task_id=?', (time.time_ns(), upload['task_id']))
            return {'upload_id': upload_id, 'expected_files': count}

    def seal(self, upload_id, expected_files):
        upload_id = _uuid(upload_id)
        if isinstance(expected_files, bool) or not isinstance(expected_files, int) or expected_files <= 0:
            raise ControlError('文件总数无效', 400)
        c = self.control
        with c._lock('upload', upload_id), closing(c.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            upload = c._upload(db, upload_id)
            if upload['state'] not in {'uploading', 'sealed', 'finished'}:
                raise ControlError('上传批次已经结束')
            count = db.execute('SELECT COUNT(*) FROM tus_manifest WHERE upload_id=?', (upload_id,)).fetchone()[0]
            if not count:
                raise ControlError('没有上传文件清单', 400)
            if count != expected_files or (upload['expected_files'] is not None and
                                          upload['expected_files'] != expected_files):
                raise ControlError('上传清单条数与选择的文件总数不一致')
            if upload['state'] != 'finished':
                db.execute("UPDATE uploads SET state='sealed',expected_files=? WHERE upload_id=?",
                           (expected_files, upload_id))
            return {'task_id': upload['task_id'], 'upload_id': upload_id,
                    'status': 'finished' if upload['state'] == 'finished' else 'sealed', 'expected_files': count}

    def read(self, upload_id, page, page_size):
        upload_id = _uuid(upload_id)
        c = self.control
        with closing(c.summary._connect()) as db:
            upload = c._upload(db, upload_id)
            count = db.execute('SELECT COUNT(*) FROM tus_manifest WHERE upload_id=?', (upload_id,)).fetchone()[0]
            received = db.execute("SELECT COUNT(*) FROM tus_manifest WHERE upload_id=? AND state='received'", (upload_id,)).fetchone()[0]
            rows = db.execute('SELECT * FROM tus_manifest WHERE upload_id=? ORDER BY path_key LIMIT ? OFFSET ?',
                              (upload_id, page_size, (page - 1) * page_size)).fetchall()
            return {'task_id': upload['task_id'], 'upload_id': upload_id, 'status': upload['state'],
                    'expected_files': count, 'received_files': received, 'page': page, 'page_size': page_size,
                    'items': [{'relative_path': r['relative_path'], 'size': r['bytes'], 'status': r['state'],
                               'tus_url': c.tus_base_url + r['tus_id'] if r['tus_id'] else None} for r in rows]}

    def hook(self, request):
        event = request.get('Event') or {}
        upload = event.get('Upload') or {}
        kind = request.get('Type')
        if kind not in {'pre-create', 'pre-finish'}:
            return {}
        meta = upload.get('MetaData') or {}
        try:
            task_id = _uuid(meta.get('task_id'))
            upload_id = _uuid(meta.get('folder_upload_id'))
            c = self.control
            with c._lock('upload', upload_id), closing(c.summary._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                batch = c._upload(db, upload_id)
                task = c._task(db, task_id)
                relative = _relative(meta.get('relativePath'), batch['root_name']).as_posix()
                row = db.execute('SELECT * FROM tus_manifest WHERE upload_id=? AND path_key=?',
                                 (upload_id, relative.casefold())).fetchone()
                if (batch['task_id'] != task_id or row is None or row['relative_path'] != relative or upload.get('SizeIsDeferred')
                        or upload.get('IsPartial') or upload.get('IsFinal') or upload.get('Size') != row['bytes']):
                    raise ControlError('上传文件与工作间清单不一致')
                if kind == 'pre-create':
                    if batch['state'] != 'sealed' or task['task_status'] != 'uploading':
                        raise ControlError('上传批次不处于上传状态')
                    headers = (event.get('HTTPRequest') or {}).get('Header') or {}
                    def header(name):
                        values = next((v for k, v in headers.items() if k.lower() == name), None)
                        return values[0] if isinstance(values, list) and values else values
                    request_id = header('x-request-id')
                    if not isinstance(request_id, str) or not request_id or len(request_id) > 240:
                        raise ControlError('上传请求缺少编号', 400)
                    writer_id = None
                    invocation_id = None
                    if c.tus_gateway_enabled:
                        writer_id = _uuid(header('x-writer-id'))
                        invocation_id = header('x-gateway-invocation')
                        if not isinstance(invocation_id, str) or len(invocation_id) != 32 or any(
                                char not in '0123456789abcdef' for char in invocation_id):
                            raise ControlError('上传调用编号无效', 400)
                        if _uuid(header('x-task-id')) != task_id or _uuid(header('x-folder-upload-id')) != upload_id:
                            raise ControlError('上传地址与当前工作间不一致', 403)
                        if db.execute('SELECT 1 FROM retired_upload_creates WHERE invocation_id=?',
                                      (invocation_id,)).fetchone():
                            raise ControlError('上传请求已失效，请重新上传')
                    if row['tus_id']:
                        if (invocation_id and row['create_invocation_id'] == invocation_id and
                                row['writer_id'] == writer_id and row['create_request_id'] == request_id):
                            return {'ChangeFileInfo': {'ID': row['tus_id']}}
                        raise ControlError('文件已有上传记录，不能重复创建')
                    tus_id = uuid.uuid4().hex
                    while (c.tus_dir / tus_id).exists() or (c.tus_dir / (tus_id + '.info')).exists():
                        tus_id = uuid.uuid4().hex
                    db.execute("UPDATE tus_manifest SET tus_id=?,create_request_id=?,writer_id=?,create_invocation_id=?,state='uploading' "
                               'WHERE upload_id=? AND path_key=?',
                               (tus_id, request_id, writer_id, invocation_id, upload_id, relative.casefold()))
                    return {'ChangeFileInfo': {'ID': tus_id}}
                if row['tus_id'] != upload.get('ID') or upload.get('Offset') != row['bytes']:
                    raise ControlError('文件尚未完整上传')
                if batch['state'] == 'finished' and row['state'] == 'received':
                    return {}
                if batch['state'] != 'sealed' or task['task_status'] != 'uploading':
                    raise ControlError('上传批次不处于上传状态')
                db.execute("UPDATE tus_manifest SET state='received' WHERE upload_id=? AND path_key=?",
                           (upload_id, relative.casefold()))
                return {}
        except ControlError as exc:
            if kind == 'pre-create':
                return {'RejectUpload': True, 'HTTPResponse': {'StatusCode': exc.status,
                        'Body': json.dumps({'detail': str(exc)}, ensure_ascii=False),
                        'Header': {'Content-Type': 'application/json'}}}
            raise

    def _restore(self, row, source):
        c = self.control
        tus_id = row['tus_id']
        if not tus_id:
            raise ControlError('仍有文件尚未上传')
        status = c._tus_head(tus_id)
        expected = {'task_id': row['task_id'], 'folder_upload_id': row['upload_id'],
                    'relativePath': row['relative_path']}
        if (status['offset'] != row['bytes'] or status['length'] != row['bytes'] or
                any(status['metadata'].get(k) != v for k, v in expected.items())):
            raise ControlError('文件上传尚未完成或归属不一致')
        blob = c.tus_dir / tus_id
        if not blob.is_file() or blob.is_symlink() or blob.stat().st_size != row['bytes']:
            raise ControlError('上传文件未找到或大小不一致')
        relative = _relative(row['relative_path'])
        destination = source.joinpath(*relative.parts)
        if not destination.resolve().is_relative_to(source.resolve()):
            raise ControlError('上传路径越过目录边界', 400)
        for parent in (destination.parent, *destination.parent.parents):
            if parent == source.parent:
                break
            if parent.is_symlink() or parent.is_junction():
                raise ControlError('上传路径包含链接', 400)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if not destination.is_file() or not filecmp.cmp(blob, destination, shallow=False):
                raise ControlError('还原目标存在不同文件，未覆盖')
            return
        staging = source.parent / 'staging'
        staging.mkdir(parents=True, exist_ok=True)
        temp = staging / (uuid.uuid4().hex + '.tmp')
        try:
            with blob.open('rb') as incoming, temp.open('xb') as outgoing:
                shutil.copyfileobj(incoming, outgoing, length=1024 * 1024)
                outgoing.flush()
                os.fsync(outgoing.fileno())
            try:
                os.link(temp, destination)
            except FileExistsError:
                if not filecmp.cmp(blob, destination, shallow=False):
                    raise ControlError('还原目标存在不同文件，未覆盖') from None
        finally:
            temp.unlink(missing_ok=True)

    def resume_info(self, upload_id, relative_path):
        """Return checksums for the received prefix; the client must compare before resuming."""
        upload_id = _uuid(upload_id)
        c = self.control
        with c._lock('upload', upload_id), closing(c.summary._connect()) as db:
            batch = c._upload(db, upload_id)
            if batch['state'] != 'sealed':
                raise ControlError('上传批次不处于待续传状态')
            relative = _relative(relative_path, batch['root_name']).as_posix()
            row = db.execute('SELECT * FROM tus_manifest WHERE upload_id=? AND path_key=?',
                             (upload_id, relative.casefold())).fetchone()
            if row is None or row['relative_path'] != relative or not row['tus_id']:
                raise ControlError('没有可核对的上传记录', 404)
            status = c._tus_head(row['tus_id'])
            expected = {'task_id': batch['task_id'], 'folder_upload_id': upload_id, 'relativePath': relative}
            if (status['length'] != row['bytes'] or not 0 <= status['offset'] <= row['bytes'] or
                    any(status['metadata'].get(k) != v for k, v in expected.items())):
                raise ControlError('续传文件归属或长度不一致')
            blob = c.tus_dir / row['tus_id']
            if not blob.is_file() or blob.is_symlink():
                raise ControlError('续传文件尚未确认')
            before = blob.stat()
            if before.st_size != status['offset']:
                raise ControlError('上传仍在变化，请暂停后核对')
            block_size, hashes = 4 * 1024 * 1024, []
            with blob.open('rb') as stream:
                while chunk := stream.read(block_size):
                    hashes.append(hashlib.sha256(chunk).hexdigest())
            after, last = blob.stat(), c._tus_head(row['tus_id'])
            if ((before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
                    or last != status):
                raise ControlError('上传仍在变化，请暂停后核对')
            return {'task_id': batch['task_id'], 'upload_id': upload_id, 'relative_path': relative,
                    'offset': status['offset'], 'length': status['length'], 'block_size': block_size,
                    'prefix_block_sha256': hashes, 'tus_url': c.tus_base_url + row['tus_id']}

    def finish(self, upload_id):
        upload_id = _uuid(upload_id)
        c = self.control
        with c._lock('upload', upload_id), closing(c.summary._connect()) as db:
            batch = c._upload(db, upload_id)
            if batch['state'] == 'finished':
                result = c.summary.read(batch['task_id'], None, 1, 1)
                return {name: result[name] for name in ('task_id', 'task_status', 'discovered_groups',
                        'total_groups', 'ungrouped_files', 'control_revision')}
            if batch['state'] != 'sealed':
                raise ControlError('请先锁定上传清单')
            rows = [dict(r, task_id=batch['task_id']) for r in db.execute(
                'SELECT * FROM tus_manifest WHERE upload_id=? ORDER BY path_key', (upload_id,))]
            if batch['expected_files'] is None or len(rows) != batch['expected_files']:
                raise ControlError('上传清单条数与选择的文件总数不一致')
            source = c.data / 'uploads' / upload_id / 'source'
            source.mkdir(parents=True, exist_ok=True)
            for row in rows:
                self._restore(row, source)
            groups, ungrouped = discover_groups([r['relative_path'] for r in rows], batch['root_name'])
            if len(groups) > 30000:
                raise ControlError('候选资料组超过 30000 组', 422)
            with db:
                db.execute('BEGIN IMMEDIATE')
                batch = c._upload(db, upload_id)
                task = c._task(db, batch['task_id'])
                if batch['state'] != 'sealed' or task['task_status'] != 'uploading':
                    raise ControlError('上传状态已变化')
                state = 'ready' if groups else 'needs_review'
                known = int(bool(groups) and not ungrouped)
                db.execute("UPDATE tasks SET task_status=?,total_groups=?,discovered_groups=?,total_known=?,"
                           "ungrouped_files=?,uploaded_files=?,desired_state='paused',updated_at_ns=?,"
                           'control_revision=control_revision+1 WHERE task_id=?',
                           (state, len(groups), len(groups), known, json.dumps(ungrouped, ensure_ascii=False), len(rows),
                            time.time_ns(), batch['task_id']))
                db.executemany('INSERT INTO control_groups (task_id,seq,group_root,state,group_id) VALUES (?,?,?,?,?)',
                    [(batch['task_id'], i, str(source.joinpath(*PurePosixPath(group).parts)), 'pending',
                      identity(['source-group/1', str(source).casefold(), group.casefold()])[:32])
                     for i, group in enumerate(groups)])
                db.execute("UPDATE tus_manifest SET state='received' WHERE upload_id=?", (upload_id,))
                db.execute("UPDATE uploads SET state='finished' WHERE upload_id=?", (upload_id,))
                task = c._task(db, batch['task_id'])
                return {'task_id': batch['task_id'], 'task_status': state, 'discovered_groups': len(groups),
                        'total_groups': len(groups) if known else None, 'ungrouped_files': ungrouped,
                        'control_revision': task['control_revision']}
