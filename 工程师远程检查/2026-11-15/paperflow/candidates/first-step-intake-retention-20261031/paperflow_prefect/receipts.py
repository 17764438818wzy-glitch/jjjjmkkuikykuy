"""Append-only local receipt identities. This module never dispatches work."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import uuid

from .records import identity, read_json


class ReceiptError(ValueError):
    pass


def path_key(path, *, lexical=False):
    value = os.path.abspath(os.fspath(path)) if lexical else str(Path(path).resolve())
    return os.path.normcase(value).casefold()


def identifier(value, label='编号'):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,159}', value):
        raise ReceiptError(label + '无效')
    return value


_DDL = '''
CREATE TABLE IF NOT EXISTS groups(
 canonical_id TEXT PRIMARY KEY, origin_path TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS aliases(
 group_id TEXT PRIMARY KEY, canonical_id TEXT NOT NULL REFERENCES groups(canonical_id),
 source_root TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS aliases_root ON aliases(source_root);
CREATE TABLE IF NOT EXISTS papers(
 canonical_id TEXT PRIMARY KEY REFERENCES groups(canonical_id), paper_id TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS files(
 canonical_id TEXT NOT NULL REFERENCES groups(canonical_id), file_key TEXT NOT NULL,
 kind TEXT NOT NULL, official_id TEXT NOT NULL, sequence INTEGER,
 PRIMARY KEY(canonical_id,file_key), UNIQUE(canonical_id,official_id),
 UNIQUE(canonical_id,sequence));
CREATE TABLE IF NOT EXISTS file_aliases(
 canonical_id TEXT NOT NULL, upstream_file_id TEXT NOT NULL, file_key TEXT NOT NULL,
 PRIMARY KEY(canonical_id,upstream_file_id),
 FOREIGN KEY(canonical_id,file_key) REFERENCES files(canonical_id,file_key));
CREATE TABLE IF NOT EXISTS file_versions(
 canonical_id TEXT NOT NULL, file_key TEXT NOT NULL, source_sha256 TEXT NOT NULL,
 source_json TEXT NOT NULL, PRIMARY KEY(canonical_id,file_key,source_sha256),
 FOREIGN KEY(canonical_id,file_key) REFERENCES files(canonical_id,file_key));
CREATE TABLE IF NOT EXISTS history_links(
 canonical_id TEXT NOT NULL, file_key TEXT NOT NULL, source_sha256 TEXT NOT NULL,
 work_dir TEXT NOT NULL, kind TEXT NOT NULL,
 PRIMARY KEY(canonical_id,file_key,source_sha256,work_dir));
CREATE TABLE IF NOT EXISTS evidence_versions(
 source_key TEXT NOT NULL, evidence_version INTEGER NOT NULL, canonical_sha256 TEXT NOT NULL,
 PRIMARY KEY(source_key,evidence_version));
CREATE TABLE IF NOT EXISTS receipt_events(
 receipt_id TEXT PRIMARY KEY, canonical_id TEXT NOT NULL, input_key TEXT NOT NULL,
 payload TEXT NOT NULL, created REAL NOT NULL);
CREATE INDEX IF NOT EXISTS receipt_inputs ON receipt_events(canonical_id,input_key,created);
'''


class ReceiptStore:
    def __init__(self, root, *, history_roots=(), evidence_validator=None):
        # Construction is read-only. Validate the output against the source before mkdir.
        self.root = Path(root).resolve()
        self.history_roots = tuple(Path(p).resolve() for p in history_roots)
        self.evidence_validator = evidence_validator

    def check_output(self, library, *, lexical=False):
        library = Path(os.path.abspath(os.fspath(library))) if lexical else Path(library).resolve()
        if self.root == library or self.root.is_relative_to(library) or library.is_relative_to(self.root):
            raise ReceiptError('接收记录不能写入原始资料库')
        for root in self.history_roots:
            if self.root == root or self.root.is_relative_to(root) or root.is_relative_to(self.root):
                raise ReceiptError('接收记录不能写入历史数据目录')
        for p in (self.root, *self.root.parents):
            if p.is_symlink() or p.is_junction():
                raise ReceiptError('接收记录目录不能经过链接')

    @contextmanager
    def transaction(self):
        self.root.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.root/'receipts.sqlite3', timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.executescript(_DDL)
            db.execute('BEGIN IMMEDIATE')
            try:
                yield db
            except BaseException:
                db.rollback()
                raise
            else:
                db.commit()
        finally:
            db.close()

    def group(self, db, handoff, legacy, *, origin_group_id=None, previous_group_root=None, lexical_source=False):
        gid = identifier(handoff['group_id'], '资料组编号')
        current = path_key(handoff['group_root'],lexical=lexical_source)
        row = db.execute('SELECT canonical_id,source_root FROM aliases WHERE group_id=?',(gid,)).fetchone()
        existing = row['canonical_id'] if row else None
        explicit = None
        if origin_group_id is not None:
            identifier(origin_group_id, '原资料组编号')
            old = db.execute('SELECT canonical_id FROM aliases WHERE group_id=?',(origin_group_id,)).fetchone()
            if old is None:
                old = db.execute('SELECT canonical_id FROM groups WHERE canonical_id=?',(origin_group_id,)).fetchone()
            if old is None:
                raise ReceiptError('原资料组映射不存在，不能凭哈希合并')
            explicit = old['canonical_id']
        elif previous_group_root is not None:
            old = db.execute('SELECT canonical_id FROM aliases WHERE source_root=?',
                             (path_key(previous_group_root,lexical=lexical_source),)).fetchall()
            keys = {r['canonical_id'] for r in old}
            if len(keys)>1: raise ReceiptError('原目录对应多个资料组')
            explicit = next(iter(keys),None)
            if explicit is None and not legacy.get('paper_id'):
                raise ReceiptError('原目录没有可核验的历史映射')
        if existing and explicit and existing != explicit:
            raise ReceiptError('资料组映射冲突')
        if existing and row['source_root'] != current and explicit != existing:
            raise ReceiptError('资料组编号对应其他来源目录，需要明确迁移映射')
        canonical = existing or explicit
        if canonical is None:
            same = db.execute('SELECT canonical_id FROM aliases WHERE source_root=?',(current,)).fetchall()
            keys={r['canonical_id'] for r in same}
            if len(keys)>1: raise ReceiptError('当前目录对应多个资料组')
            canonical=next(iter(keys),None)
        legacy_id=legacy.get('paper_id')
        if legacy_id:
            owner=db.execute('SELECT canonical_id FROM papers WHERE paper_id=?',(legacy_id,)).fetchone()
            if owner:
                owner_path=db.execute('SELECT origin_path FROM groups WHERE canonical_id=?',
                                      (owner['canonical_id'],)).fetchone()['origin_path']
                allowed={current,path_key(legacy['person_dir'],lexical=lexical_source)}
                if previous_group_root is not None:allowed.add(path_key(previous_group_root,lexical=lexical_source))
                if canonical and canonical != owner['canonical_id']:
                    raise ReceiptError('历史论文编号属于其他资料组')
                if not canonical and owner_path not in allowed:
                    raise ReceiptError('历史论文编号归属冲突')
                canonical=owner['canonical_id']
        canonical=canonical or gid
        origin=path_key(previous_group_root or legacy.get('person_dir') or handoff['group_root'],lexical=lexical_source)
        db.execute('INSERT OR IGNORE INTO groups VALUES(?,?)',(canonical,origin))
        db.execute('INSERT OR IGNORE INTO aliases VALUES(?,?,?)',(gid,canonical,current))
        owner=db.execute('SELECT canonical_id FROM aliases WHERE group_id=?',(gid,)).fetchone()
        if owner['canonical_id']!=canonical:raise ReceiptError('资料组别名冲突')
        return canonical

    def mapped_source_roots(self, db, origin_group_id):
        """Read source ownership already recorded for an explicit migration."""
        if origin_group_id is None:return ()
        identifier(origin_group_id,'原资料组编号')
        row=db.execute('SELECT canonical_id FROM aliases WHERE group_id=?',(origin_group_id,)).fetchone()
        if row is None:
            row=db.execute('SELECT canonical_id FROM groups WHERE canonical_id=?',(origin_group_id,)).fetchone()
        if row is None:raise ReceiptError('原资料组映射不存在，不能凭哈希合并')
        canonical=row['canonical_id']
        paths={r['source_root'] for r in db.execute('SELECT source_root FROM aliases WHERE canonical_id=?',(canonical,))}
        paths.add(db.execute('SELECT origin_path FROM groups WHERE canonical_id=?',(canonical,)).fetchone()['origin_path'])
        return tuple(sorted(paths))

    def paper(self, db, canonical, handoff, legacy, *, needed):
        row=db.execute('SELECT paper_id FROM papers WHERE canonical_id=?',(canonical,)).fetchone()
        old=row['paper_id'] if row else None
        incoming=handoff.get('existing_paper_id')
        historic=legacy.get('paper_id')
        for pid in (incoming,historic):
            if pid is not None:identifier(pid,'论文编号')
        if incoming and incoming not in {old,historic}:
            raise ReceiptError('上游历史论文编号没有匹配的来源记录')
        if old and historic and old!=historic:
            raise ReceiptError('已登记编号与新找到的历史编号冲突，不能重新编号')
        pid=old or historic
        if not pid and needed:pid='PF_'+uuid.uuid4().hex[:24]
        if pid and not old:
            owner=db.execute('SELECT canonical_id FROM papers WHERE paper_id=?',(pid,)).fetchone()
            if owner and owner['canonical_id']!=canonical:
                raise ReceiptError('论文编号已属于其他资料组')
            db.execute('INSERT INTO papers VALUES(?,?)',(canonical,pid))
        return pid

    def reserve_file(self, db, canonical, kind, source, legacy_id=None, legacy_sequence=None):
        key=kind+':'+source['relative'].casefold()
        alias=db.execute('SELECT file_key FROM file_aliases WHERE canonical_id=? AND upstream_file_id=?',
                         (canonical,source['file_id'])).fetchone()
        if alias and alias['file_key']!=key:
            raise ReceiptError('上游文件编号已绑定其他原件位置')
        row=db.execute('SELECT * FROM files WHERE canonical_id=? AND file_key=?',(canonical,key)).fetchone()
        if row is None:
            official=identifier(legacy_id,'历史文件编号') if legacy_id else (
                'ATT_' if kind=='attachment' else 'BODY_')+uuid.uuid4().hex[:24]
            sequence=None
            if kind=='attachment':
                sequence=legacy_sequence if legacy_sequence is not None else db.execute(
                    'SELECT COALESCE(MAX(sequence),0)+1 FROM files WHERE canonical_id=?',(canonical,)).fetchone()[0]
                if type(sequence) is not int or sequence<1:raise ReceiptError('历史附件序号无效')
            try:
                db.execute('INSERT INTO files VALUES(?,?,?,?,?)',(canonical,key,kind,official,sequence))
            except sqlite3.IntegrityError as exc:
                raise ReceiptError('正式文件编号已绑定其他原件') from exc
            row=db.execute('SELECT * FROM files WHERE canonical_id=? AND file_key=?',(canonical,key)).fetchone()
        elif legacy_id and row['official_id']!=legacy_id:
            raise ReceiptError('已登记附件编号与历史映射冲突，不能重新编号')
        elif legacy_sequence is not None and row['sequence']!=legacy_sequence:
            raise ReceiptError('已登记附件序号与历史映射冲突，不能重新分配')
        return dict(row)

    def file(self, db, canonical, kind, source, legacy_id=None, legacy_sequence=None):
        row=self.reserve_file(db,canonical,kind,source,legacy_id,legacy_sequence)
        key=row['file_key']
        db.execute('INSERT OR IGNORE INTO file_aliases VALUES(?,?,?)',(canonical,source['file_id'],key))
        old=db.execute('SELECT 1 FROM file_versions WHERE canonical_id=? AND file_key=? AND source_sha256=?',
                       (canonical,key,source['sha256'])).fetchone()
        db.execute('INSERT OR IGNORE INTO file_versions VALUES(?,?,?,?)',
                   (canonical,key,source['sha256'],json.dumps(source,ensure_ascii=False,sort_keys=True)))
        return row,old is None

    def history_paths(self, db, canonical, file_key, source_sha256, kind, paths):
        for path in paths:
            db.execute('INSERT OR IGNORE INTO history_links VALUES(?,?,?,?,?)',
                       (canonical,file_key,source_sha256,str(Path(path).resolve()),kind))
        return [r['work_dir'] for r in db.execute(
            'SELECT work_dir FROM history_links WHERE canonical_id=? AND file_key=? AND source_sha256=? ORDER BY work_dir',
            (canonical,file_key,source_sha256))]

    def prior(self, db, canonical, input_key):
        row=db.execute('SELECT payload FROM receipt_events WHERE canonical_id=? AND input_key=? ORDER BY created DESC LIMIT 1',
                       (canonical,input_key)).fetchone()
        return json.loads(row['payload']) if row else None

    def prior_delivery_receipts(self, db, canonical, kind, official_id, relative, source_sha256):
        """Find immutable prior receipts for the same registered original."""
        refs={}
        for record in db.execute('SELECT payload FROM receipt_events WHERE canonical_id=?',(canonical,)):
            value=json.loads(record['payload'])
            receipt_id=value.get('receipt_id')
            original={k:v for k,v in value.items() if k not in {'receipt_id','record_path'}}
            if (value.get('schema')!='paperflow-receipt/1' or value.get('origin_group_id')!=canonical
                    or not value.get('delivery_ref') or identity(original)!=receipt_id):
                continue
            if any(item.get('state')=='已接收' and item.get('kind')==kind
                   and item.get('file_id')==official_id and isinstance(item.get('relative'),str)
                   and item['relative'].casefold()==relative.casefold()
                   and item.get('sha256')==source_sha256 and item.get('delivery_ref')==value['delivery_ref']
                   for item in value.get('items',[])):
                refs[receipt_id]=dict(path=value['record_path'],receipt_id=receipt_id)
        return [refs[key] for key in sorted(refs)]

    def evidence(self, db, reference):
        key=identity([reference['group_id'],reference['source']])
        version=reference['evidence_version']
        digest=reference.pop('_canonical_sha256')
        old=db.execute('SELECT canonical_sha256 FROM evidence_versions WHERE source_key=? AND evidence_version=?',
                       (key,version)).fetchone()
        if old and old['canonical_sha256']!=digest:
            raise ReceiptError('同一证据版本已有不同内容，不能覆盖或冒充补证据')
        db.execute('INSERT OR IGNORE INTO evidence_versions VALUES(?,?,?)',(key,version,digest))
        return reference

    def record(self, db, canonical, input_key, payload):
        payload['receipt_id']=identity(payload)
        payload['record_path']=str(self.root/'receipts'/(payload['receipt_id']+'.json'))
        encoded=json.dumps(payload,ensure_ascii=False,sort_keys=True)
        db.execute('INSERT OR IGNORE INTO receipt_events VALUES(?,?,?,?,?)',
                   (payload['receipt_id'],canonical,input_key,encoded,time.time()))
        return payload

    def materialize(self, payload):
        path=self.root/'receipts'/(payload['receipt_id']+'.json')
        path.parent.mkdir(parents=True,exist_ok=True)
        if path.exists():
            if read_json(path)!=payload:raise ReceiptError('已有接收回执内容不同，不能覆盖')
            return payload
        # Exclusive creation; SQLite is already committed, so interrupted output is repairable.
        tmp=path.with_name('.'+path.name+'.'+uuid.uuid4().hex+'.tmp')
        try:
            with tmp.open('x',encoding='utf8') as stream:
                json.dump(payload,stream,ensure_ascii=False,indent=2,sort_keys=True)
                stream.flush();os.fsync(stream.fileno())
            try:os.link(tmp,path)
            except FileExistsError:
                if read_json(path)!=payload:raise ReceiptError('已有接收回执不能覆盖')
        finally:
            tmp.unlink(missing_ok=True)
        return payload

    def receive(self,handoff=None,**kwargs):
        from .intake_handoff import receive_handoff
        return receive_handoff(handoff,self,**kwargs)
