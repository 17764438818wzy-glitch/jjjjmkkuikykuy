"""Provenance and exact local inventories; never delete an unregistered directory."""
import hashlib
import json
import os
from pathlib import Path

from .handoff_store import HandoffLedger,canonical
from .package import sha

ASSET_SCHEMA='''CREATE TABLE IF NOT EXISTS workflow_artifacts (
    path TEXT PRIMARY KEY,task_id TEXT NOT NULL,thread_id TEXT NOT NULL,kind TEXT NOT NULL)'''


def overlaps(left,right):
    # Stored paths have been resolved at the filesystem boundary; SQL checks are lexical.
    # A separator boundary excludes siblings such as "paper" and "paper-2".
    # Keep pathlib's Unicode/case semantics (Windows normcase differs) without
    # constructing every parent Path while holding the SQLite writer.
    a,b=(str(Path(os.path.abspath(path))) for path in (left,right))
    if os.name=='nt':a,b=a.lower(),b.lower()
    return a==b or a.startswith(b.rstrip(os.sep)+os.sep) or b.startswith(a.rstrip(os.sep)+os.sep)


def check_path_reference(c,path):
    if c.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_items'").fetchone():
        for row in c.execute("SELECT resource FROM cleanup_items WHERE kind='local' AND state IN ('deleting','pending_reconcile','deleted')"):
            if overlaps(path,json.loads(row[0])['path']):raise ValueError('material path deleting')


def defer_local_cleanup(c,task_id):
    """Serialize new repair responsibility/graph admission with a saved delete intent."""
    if c.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_items'").fetchone():
        if c.execute("SELECT 1 FROM cleanup_items WHERE task_id=? AND kind='local' "
                "AND state IN ('deleting','pending_reconcile')",(task_id,)).fetchone():
            from .scheduling_policy import ActionDeferred
            raise ActionDeferred('graph materials currently deleting')


def track_artifact(config,task,path,kind):
    ledger_path=getattr(config,'processing_ledger_path',None)
    if not ledger_path:return
    from .store import Store
    ledger=HandoffLedger(Store(ledger_path))
    def write(c):
        c.execute(ASSET_SCHEMA);check_path_reference(c,path)
        c.execute('INSERT OR IGNORE INTO workflow_artifacts VALUES(?,?,?,?)',
            (str(Path(path).resolve()),task['task_id'],task.get('thread_id',task['task_id']),kind))
    ledger._write(write)


def inventory(path,roots,originals):
    # Resolve containment and reject every reparse component before enumerating or deleting.
    path=Path(path).absolute()
    for parent in (path,*path.parents):
        if parent.is_symlink() or parent.is_junction():raise ValueError('cleanup links forbidden')
    resolved=path.resolve()
    if not any(resolved!=Path(root).resolve() and resolved.is_relative_to(Path(root).resolve()) for root in roots):
        raise ValueError('cleanup outside owned product roots')
    if any(overlaps(resolved,p) for p in originals):raise ValueError('original material protected')
    if not resolved.is_dir():raise ValueError('owned directory missing before delete intent')
    files={};directories=[]
    for p in sorted(resolved.rglob('*')):
        if p.is_symlink() or p.is_junction():raise ValueError('cleanup links forbidden')
        name=p.relative_to(resolved).as_posix()
        if p.is_dir():directories.append(name)
        elif p.is_file():files[name]=dict(sha256=sha(p),bytes=p.stat().st_size)
        else:raise ValueError('unsupported cleanup entry')
    return dict(path=str(resolved),files=files,directories=directories)


def delete_inventory(resource,roots,originals,cancel):
    path=Path(resource['path'])
    # Absence is accepted only by a caller holding the saved deleting intent.
    if not path.exists():return dict(absent=True,files=0)
    actual=inventory(path,roots,originals)
    if any(resource['files'].get(name)!=value for name,value in actual['files'].items()):
        raise ValueError('owned cleanup file changed or unexpected')
    if not set(actual['directories'])<=set(resource['directories']):raise ValueError('unexpected cleanup directory')
    removed=0
    for name in actual['files']:
        if cancel.is_set():raise ValueError('cleanup stopped')
        p=path/name
        # Recheck links and content immediately before each unlink; never use rmtree.
        for parent in (p,*p.parents):
            if parent.is_symlink() or parent.is_junction():raise ValueError('cleanup links forbidden')
        if sha(p)!=resource['files'][name]['sha256']:raise ValueError('cleanup content changed')
        p.unlink();removed+=1
    for name in sorted(actual['directories'],key=lambda n:len(Path(n).parts),reverse=True):
        (path/name).rmdir()
    path.rmdir()
    return dict(absent=not path.exists(),files=removed)
