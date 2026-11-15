"""Per-name physical removal with durable, non-replayed intents.

Only the backend's exact authority/reference/NAS context may call this engine.
The journal detects truncation, missing events and accidental edits; it is not
a security boundary against an administrator rewriting all local evidence.
"""
from contextlib import ExitStack
import hashlib,json,os,time
from pathlib import Path
from filelock import FileLock
from paperflow.original_archive.lifecycle import locked_file,safe_file,verify_stream
from .namespace_guard import hold_directories
from .hardlinks import enumerate_links
from .streams import require_default_stream_only
from .file_identity import identity_from_stream


def encoded(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf8')
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()
def names(paths):return {os.path.normcase(str(Path(p))) for p in paths}
def durable_new(path,value):
    with Path(path).open('xb') as stream:
        stream.write(encoded(value));stream.flush();os.fsync(stream.fileno())


class ReleaseJournal:
    def __init__(self,folder,authority_sha256,groups,roots):
        self.folder=Path(folder);self.roots=tuple(Path(r).resolve(strict=True) for r in roots)
        if len(authority_sha256)!=64 or not groups:raise ValueError('finite bound release plan required')
        self.header=dict(schema='paper-physical-release/1',authority_sha256=authority_sha256,
                         groups=groups,roots=list(map(str,self.roots)))
        self.groups=groups
        all_names=[n['path'] for g in groups for n in g['names']]
        if len(names(all_names))!=len(all_names):raise ValueError('duplicate release name')
        for g in groups:
            if not g['names'] or len(g['sha256'])!=64 or type(g['bytes']) is not int:raise ValueError('invalid file group')
            for n in g['names']:
                if str(safe_file(n['path'],self.roots))!=str(Path(n['path'])):raise ValueError('release path changed')
                if n['kind'] not in {'origin','backup','attachment','uploaded','intermediate'}:raise ValueError('unknown release role')
        if self.folder.exists():
            if not (self.folder/'plan.json').is_file():raise ValueError('unrelated journal directory')
            if (self.folder/'plan.json').read_bytes()!=encoded(self.header):raise ValueError('release plan binding changed')
        else:
            self.folder.mkdir(parents=True)
            durable_new(self.folder/'plan.json',self.header)
        self._events()

    def _events(self):
        if (self.folder/'plan.json').read_bytes()!=encoded(self.header):raise ValueError('release plan changed')
        known={'plan.json','revoked.json'};events=[];previous=digest(self.header)
        files=sorted(self.folder.glob('event-*.json'))
        for number,path in enumerate(files,1):
            known.add(path.name)
            if path.name!=f'event-{number:08}.json':raise ValueError('release event sequence missing')
            event=json.loads(path.read_bytes())
            if event['previous']!=previous or event['sequence']!=number:raise ValueError('release event chain differs')
            previous=hashlib.sha256(path.read_bytes()).hexdigest();events.append(event)
        if any(p.name not in known or not p.is_file() or p.is_symlink() for p in self.folder.iterdir()):
            raise ValueError('unknown release journal entry')
        self.events,self.previous=events,previous
        return events

    def _event(self,stage,path='',**details):
        self._events();number=len(self.events)+1
        value=dict(sequence=number,previous=self.previous,stage=stage,path=str(path),at=time.time(),**details)
        durable_new(self.folder/f'event-{number:08}.json',value);self._events()

    def checkpoint(self,stage):pass

    def _completed(self):
        completed=set();pending=set()
        for e in self._events():
            if e['stage']=='unlink_intent':pending.add(e['path'])
            if e['stage']=='unlink_complete':
                if e['path'] not in pending:raise ValueError('completion without original intent')
                pending.remove(e['path']);completed.add(e['path'])
        if pending:raise ValueError('uncertain unlink intent requires reconciliation; never replay')
        return completed

    def _content(self,stream,group):
        info=os.fstat(stream.fileno())
        if (info.st_dev,info.st_ino)!=(group['device'],group['inode']):raise ValueError('file object changed')
        verify_stream(stream,group['sha256'],group['bytes'])

    def _inventory(self,stream,path,group,remaining):
        self._content(stream,group);require_default_stream_only(path)
        if os.fstat(stream.fileno()).st_nlink!=len(remaining) or names(enumerate_links(path))!=names(remaining):
            raise ValueError('unregistered hardlink or missing registered name')

    def remove(self,scope):
        with FileLock(str(self.folder)+'.lock',timeout=0):
            if (self.folder/'revoked.json').exists():raise ValueError('release scope revoked by restoration')
            completed=self._completed()
            # Whole-plan validation precedes the first irreversible effect.
            for group in self.groups:
                remaining=[Path(n['path']) for n in group['names'] if n['path'] not in completed]
                if not remaining:continue
                with locked_file(remaining[0]) as (stream,_):self._inventory(stream,remaining[0],group,remaining)
            for group in self.groups:
                parents=list({Path(n['path']).parent for n in group['names']})
                for n in group['names']:
                    path=Path(n['path'])
                    if str(path) in completed:continue
                    remaining=[Path(x['path']) for x in group['names'] if x['path'] not in completed]
                    with scope.commit_guard(),hold_directories(parents,list(self.roots)) as guard:
                        scope.assert_open()
                        with locked_file(path,delete=True) as (stream,unlink):
                            self._inventory(stream,path,group,remaining);guard.assert_held()
                            self._event('unlink_intent',path,file_identity=identity_from_stream(stream))
                            self.checkpoint('after_intent');scope.assert_open();unlink()
                        guard.assert_held()
                        if os.path.lexists(path):raise ValueError('native unlink result not confirmed')
                        remaining.remove(path)
                        if remaining:
                            with locked_file(remaining[0]) as (stream,_):self._inventory(stream,remaining[0],group,remaining)
                        self._event('unlink_complete',path,physical_space_reclaimed='not_measured')
                        completed.add(str(path));self.checkpoint('after_complete')
            counts={k:0 for k in ('original_names_removed','attachment_names_removed','backup_names_removed','uploaded_names_removed','intermediate_files_removed')}
            keys=dict(origin='original_names_removed',attachment='attachment_names_removed',backup='backup_names_removed',uploaded='uploaded_names_removed',intermediate='intermediate_files_removed')
            for g in self.groups:
                for n in g['names']:
                    if n['path'] in completed:counts[keys[n['kind']]]+=1
            residual=sum(os.path.lexists(n['path']) for g in self.groups for n in g['names'])
            return dict(status='complete' if residual==0 else 'held',eligible_residual_files=residual,
                        **counts,physical_space_reclaimed='not_measured',journal_path=str(self.folder),
                        journal_head_sha256=self.previous,plan_sha256=digest(self.header))

    def restore(self,scope,*,nas_streams=None):
        """Restore known missing names without overwrite; close the old delete plan.

        With no surviving original object, caller must hold a fresh exact NAS
        readback and supply its streams. Unknown intents never auto-restore.
        """
        with FileLock(str(self.folder)+'.lock',timeout=0):
            completed=self._completed()
            pending=set()
            for event in self.events:
                if event['stage'] in {'restore_intent','rehydrate_intent'}:pending.add(event['path'])
                elif event['stage'] in {'restore_complete','rehydrate_complete'}:pending.discard(event['path'])
            if pending:raise ValueError('uncertain restore intent; no replay or overwrite')
            if any(e['stage']=='restored' for e in self.events):
                return dict(status='restored',old_delete_scope_revoked=True,journal_path=str(self.folder))
            with scope.commit_guard():
                scope.assert_open()
                if not (self.folder/'revoked.json').exists():durable_new(self.folder/'revoked.json',dict(plan_sha256=digest(self.header),at=time.time()))
            for group in self.groups:
                if all(n['kind']=='intermediate' for n in group['names']):continue
                paths=[Path(n['path']) for n in group['names']];present=[p for p in paths if p.exists()]
                missing=[p for p in paths if not p.exists()]
                if any(str(p) not in completed for p in missing):raise ValueError('unrecorded missing name')
                with scope.commit_guard(),hold_directories(list({p.parent for p in paths}),list(self.roots),lock_children=False) as guard:
                    if present:
                        info=present[0].stat()
                        prior=[e for e in self.events if e['stage']=='rehydrate_complete' and e['path'] in map(str,paths)]
                        expected=(prior[-1]['device'],prior[-1]['inode']) if prior else (group['device'],group['inode'])
                        if (info.st_dev,info.st_ino)!=expected:raise ValueError('surviving source object replaced')
                    if not present:
                        if not nas_streams or group['sha256'] not in nas_streams:raise ValueError('exact NAS readback required to rehydrate')
                        source=nas_streams[group['sha256']];verify_stream(source,group['sha256'],group['bytes'])
                        target=missing.pop(0);self._event('rehydrate_intent',target)
                        with target.open('xb') as out:
                            while block:=source.read(1024*1024):out.write(block)
                            out.flush();os.fsync(out.fileno())
                        info=target.stat();self._event('rehydrate_complete',target,device=info.st_dev,inode=info.st_ino);present.append(target)
                    with locked_file(present[0]) as (stream,_):
                        info=os.fstat(stream.fileno())
                        prior=[e for e in self.events if e['stage']=='rehydrate_complete' and e['path'] in map(str,paths)]
                        expected=(prior[-1]['device'],prior[-1]['inode']) if prior else (group['device'],group['inode'])
                        if (info.st_dev,info.st_ino)!=expected:raise ValueError('opened restoration source object changed')
                        verify_stream(stream,group['sha256'],group['bytes'])
                        for p in present[1:]:
                            if not os.path.samefile(p,present[0]):raise ValueError('existing restore destination is unrelated')
                        for target in missing:
                            self._event('restore_intent',target);guard.link_from_stream(stream,target)
                            with locked_file(target) as (other,_):verify_stream(other,group['sha256'],group['bytes'])
                            self._event('restore_complete',target)
                with hold_directories(list({p.parent for p in paths}),list(self.roots)) as guard:
                    if names(enumerate_links(paths[0]))!=names(paths):raise ValueError('restored link inventory differs')
                    guard.assert_held()
            self._event('restored',old_delete_scope_revoked=True)
            return dict(status='restored',old_delete_scope_revoked=True,journal_path=str(self.folder))
