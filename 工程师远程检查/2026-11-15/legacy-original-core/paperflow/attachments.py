"""Per-file attachment facts; original bytes and model judgments stay separate."""
from pathlib import Path
import hashlib
import json
import shutil
import tempfile

from .handoff_store import HandoffLedger,canonical
from .package import load,save,sha


DRAWING_EXTENSIONS={'.dwg','.dxf','.step','.stp','.iges','.igs','.sldprt','.sldasm','.prt','.asm','.stl','.obj','.3dm','.f3d'}
CODE_EXTENSIONS={'.py','.js','.ts','.java','.c','.cpp','.h','.hpp','.cs','.go','.rs','.m','.r','.sql','.ipynb','.ino','.v','.vhd'}
ARCHIVE_EXTENSIONS={'.zip','.rar','.7z'}
IMAGE_EXTENSIONS={'.png','.jpg','.jpeg','.gif','.webp','.bmp'}
SPREADSHEET_EXTENSIONS={'.xlsx','.xls','.xlsm','.csv','.tsv'}
DATA_EXTENSIONS={'.json'}
MATERIAL_EXTENSIONS=IMAGE_EXTENSIONS|SPREADSHEET_EXTENSIONS|DATA_EXTENSIONS
ATTACHMENT_EXTENSIONS=DRAWING_EXTENSIONS|CODE_EXTENSIONS|ARCHIVE_EXTENSIONS|MATERIAL_EXTENSIONS|{'.exe','.dll','.bin','.hex'}


def attachment_target(targets,kind):
    """Reuse an approved common archive, never choose between distinct routes."""
    if kind in targets:return kind,targets[kind]
    if (kind in ('image','spreadsheet','data') and targets.get('drawing')
            and targets.get('drawing')==targets.get('software')):
        return 'drawing',targets['drawing']
    return None,{}


def is_candidate(path):
    return Path(path).suffix.lower() in ATTACHMENT_EXTENSIONS


def attachment_note(profile):
    value=profile.get('attachment')
    if value is None:return None
    if not isinstance(value,dict) or value.get('kind') not in ('drawing','software','image','spreadsheet','data'):
        raise ValueError('attachment requires a supported material kind')
    description=value.get('description')
    if not isinstance(description,str) or not description.strip():raise ValueError('attachment description missing')
    title=value.get('related_paper_title')
    if title is not None and (not isinstance(title,str) or not title.strip()):title=None
    return dict(kind=value['kind'],description=description.strip(),related_paper_title=title,
                association_status='pending')


def prepare_package(config,task):
    """Copy exact original bytes under a retained root, outside temporary cleanup roots."""
    profile=load(Path(task['draft_dir'])/'profile.json')
    note=attachment_note(profile)
    if note is None:raise ValueError('attachment decision required')
    source=Path(task['source_path'])
    if not is_candidate(source):raise ValueError('attachment input required')
    if sha(source)!=task['source_sha256']:raise ValueError('original source changed')
    ident={k:task[k] for k in ('task_id','source_id','source_sha256')}
    aid='att_'+hashlib.sha256(canonical(ident).encode()).hexdigest()[:32]
    root=config.root/'retained-attachments'/aid
    original='original'+source.suffix.lower()
    manifest=dict(identity=ident,attachment_id=aid,original=original,note=note,
                  original_sha256=task['source_sha256'],bytes=source.stat().st_size)
    if root.exists():
        if load(root/'manifest.json')!=manifest or sha(root/original)!=task['source_sha256']:
            raise ValueError('retained attachment changed')
        return root
    root.parent.mkdir(parents=True,exist_ok=True)
    # Only a fully verified directory is published. A killed copy leaves an
    # unreferenced scratch directory, never an apparently sealed attachment.
    with tempfile.TemporaryDirectory(prefix='.preparing-'+aid+'-',dir=root.parent) as scratch:
        staging=Path(scratch)
        shutil.copy2(source,staging/original)
        if sha(source)!=task['source_sha256'] or sha(staging/original)!=task['source_sha256']:
            raise ValueError('original changed while copying')
        save(staging/'profile.json',profile)
        save(staging/'manifest.json',manifest)
        staging.rename(root)
    return root


def verify_package(config,task,path,manifest_sha256):
    root=Path(path).resolve()
    if not root.is_relative_to((config.root/'retained-attachments').resolve()):raise ValueError('attachment package outside retained root')
    if sha(root/'manifest.json')!=manifest_sha256:raise ValueError('attachment manifest changed')
    manifest=load(root/'manifest.json')
    if manifest['identity']!={k:task[k] for k in ('task_id','source_id','source_sha256')}:raise ValueError('attachment source identity mismatch')
    original=Path(manifest['original'])
    if original.name!=str(original) or sha(root/original)!=task['source_sha256']:raise ValueError('attachment original changed')
    if attachment_note(load(root/'profile.json'))!=manifest['note']:raise ValueError('attachment note changed')
    if sha(task['source_path'])!=task['source_sha256']:raise ValueError('original source changed')
    return manifest


class AttachmentLedger(HandoffLedger):
    def __init__(self,store):
        super().__init__(store)
        self._write(lambda c:c.execute('''CREATE TABLE IF NOT EXISTS attachment_tasks (
            attachment_id TEXT PRIMARY KEY,task_id TEXT NOT NULL UNIQUE,source_id TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,package_dir TEXT NOT NULL,manifest_sha256 TEXT NOT NULL,
            category TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL)'''))

    def item(self,attachment_id):
        with self.store.db(timeout=.1) as c:rows=[dict(r) for r in c.execute('SELECT * FROM attachment_tasks WHERE attachment_id=?',(attachment_id,))]
        if len(rows)!=1:raise ValueError('registered attachment required')
        return rows[0]

    def register_sql(self,c,task,root,manifest,targets,manifest_sha256,endpoint):
        if not c.execute('SELECT 1 FROM sources WHERE path=? AND task_id=?',(task['source_path'],task['task_id'])).fetchone():
            raise ValueError('attachment source superseded')
        _,target=attachment_target(targets,manifest['note']['kind'])
        data=dict(identity=dict(attachment_id=manifest['attachment_id'],generation=0,**manifest['identity']),
                  note=manifest['note'],target=target,endpoint=endpoint.rstrip('/'),original=manifest['original'],
                  bytes=manifest['bytes'],remote={},association=dict(status='pending',
                  related_paper_title=manifest['note']['related_paper_title'],body_document_id=None))
        root=str(Path(root).resolve());digest=manifest_sha256
        prior=c.execute('SELECT * FROM attachment_tasks WHERE attachment_id=?',(manifest['attachment_id'],)).fetchone()
        if prior and (prior['manifest_sha256']!=digest or prior['package_dir']!=root):raise ValueError('attachment handoff replay conflict')
        state='pending' if target.get('parent_id') and target.get('dataset_id') else 'configuration_pending'
        c.execute('INSERT OR IGNORE INTO attachment_tasks VALUES(?,?,?,?,?,?,?,?,?)',
            (manifest['attachment_id'],task['task_id'],task['source_id'],task['source_sha256'],root,digest,
             manifest['note']['kind'],state,canonical(data)))
        return dict(attachment_id=manifest['attachment_id'],state=(prior['state'] if prior else state))


def attachment_status(store,task_id):
    """Operator facts, without claiming a search test or a confirmed paper link."""
    with store.db(timeout=.1) as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='attachment_tasks'").fetchone():return None
        row=c.execute('SELECT * FROM attachment_tasks WHERE task_id=?',(task_id,)).fetchone()
        if row is None:return None
        pending=c.execute("SELECT COUNT(*) FROM control_actions WHERE state IN ('unresolved','unknown') "
            "AND json_extract(request,'$.resource.identity.attachment_id')=?",(row['attachment_id'],)).fetchone()[0]
        data=json.loads(row['data'])
    return dict(task_id=task_id,attachment_id=row['attachment_id'],state=row['state'],
        complete=row['state']=='complete' and not pending,pending_reconcile=pending,
        verification='original_and_note_readback' if row['state']=='complete' else 'pending',
        retrieval='not_checked',association=data['association']['status'],remote=data['remote'])
