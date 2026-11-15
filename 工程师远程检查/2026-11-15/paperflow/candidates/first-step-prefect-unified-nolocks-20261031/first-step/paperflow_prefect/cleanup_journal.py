"""Narrow, resumable adapter over the existing per-name release journal."""
from contextlib import nullcontext
import hashlib
import os
from pathlib import Path
from filelock import FileLock
from ._release.release import ReleaseJournal, digest
from ._release.namespace_guard import hold_directories
from ._release.file_identity import identity_from_stream
from .records import read_json, write_json
from paperflow.original_archive.lifecycle import locked_file


class CleanupJournal(ReleaseJournal):
    def __init__(self, folder, authority_sha256, groups, roots, *, allow_intermediates=False):
        allowed = {'origin', 'attachment'} | ({'intermediate'} if allow_intermediates else set())
        if any(n['kind'] not in allowed for g in groups for n in g['names']):
            raise ValueError('only body and attachment names may be cleaned')
        super().__init__(folder, authority_sha256, groups, roots)
        self.head_path = self.folder.parent / 'journal-head.json'
        if not self.head_path.exists():
            if self.events:
                raise ValueError('journal head missing')
            write_json(self.head_path, {'sequence': 0, 'sha256': digest(self.header)})

    def _events(self):
        events = super()._events()
        head_path = self.folder.parent / 'journal-head.json'
        if head_path.exists():
            head = read_json(head_path)
            number = head['sequence']
            if type(number) is not int or not 0 <= number <= len(events):
                raise ValueError('journal tail is missing')
            expected = (hashlib.sha256((self.folder / f'event-{number:08}.json').read_bytes()).hexdigest()
                        if number else digest(self.header))
            if head['sha256'] != expected:
                raise ValueError('journal head differs')
            # An append fsynced before the atomic head replacement can be replayed
            # as evidence, never as an unverified filesystem operation.
            if len(events) > number + 1:
                raise ValueError('journal head is inconsistent')
        elif events:
            raise ValueError('journal head missing')
        return events

    def _event(self, stage, path='', **details):
        super()._event(stage, path, **details)
        write_json(self.head_path, {'sequence': len(self.events), 'sha256': self.previous})

    def _content(self, stream, group):
        if identity_from_stream(stream) != group['object_identity']:
            raise ValueError('file_object_changed')
        stream.seek(0)
        super()._content(stream, group)

    def states(self):
        known = {n['path']: g for g in self.groups for n in g['names']}
        complete, retained, pending = {}, {}, {}
        for event in self._events():
            path, stage = event['path'], event['stage']
            if path not in known:
                raise ValueError('journal path outside frozen plan')
            if stage == 'unlink_intent':
                if path in pending or path in complete or path in retained:
                    raise ValueError('duplicate delete intent')
                if event['file_identity'] != known[path]['object_identity']:
                    raise ValueError('intent object differs from frozen plan')
                pending[path] = event
            elif stage in {'unlink_complete', 'unlink_reconciled_absent', 'unlink_not_performed'}:
                if path not in pending:
                    raise ValueError('resolution without intent')
                pending.pop(path)
                if stage != 'unlink_not_performed':
                    complete[path] = stage
            elif stage == 'retained':
                if path in complete:
                    raise ValueError('completed file cannot become an old retained file')
                pending.pop(path, None)
                retained[path] = event['reason']
            else:
                raise ValueError('unknown cleanup event')
        return complete, retained, pending

    def snapshot(self):
        complete, retained, pending = self.states()
        unknown = {p: reason for p, reason in retained.items() if reason == 'missing_without_intent'}
        retained = {p: reason for p, reason in retained.items() if p not in unknown}
        total = sum(len(g['names']) for g in self.groups)
        unresolved = total - len(complete) - len(retained)
        return {'status': 'unknown' if unknown else ('incomplete' if unresolved else ('retained' if retained else 'complete')),
                'removed_files': sum(s == 'unlink_complete' for s in complete.values()),
                'reconciled_absent_files': sum(s == 'unlink_reconciled_absent' for s in complete.values()),
                'retained': [{'path': p, 'reason': reason} for p, reason in retained.items()],
                'unknown_files': [{'path': p, 'reason': reason} for p, reason in unknown.items()],
                'pending_files': unresolved, 'pending_intents': list(pending),
                'completed_paths': list(complete), 'journal_head_sha256': self.previous,
                'plan_sha256': digest(self.header), 'physical_space_reclaimed': 'not_measured'}

    def remove(self, scope, checkpoint=None):
        checkpoint = checkpoint or (lambda stage, path: None)
        deferred = []
        with FileLock(str(self.folder) + '.lock', timeout=0):
            self.states()
            # Repair only a fully readable, chained event whose head update was
            # interrupted; a missing/truncated event remains an error.
            write_json(self.head_path, {'sequence': len(self.events), 'sha256': self.previous})
            for group in self.groups:
                parents = list({Path(n['path']).parent for n in group['names']})
                for row in group['names']:
                    path = Path(row['path'])
                    complete, retained, pending = self.states()
                    if str(path) in complete or str(path) in retained:
                        continue
                    remaining = [Path(n['path']) for n in group['names'] if n['path'] not in complete]
                    with scope.commit_guard():
                        scope.assert_open()
                        try:
                            with hold_directories(parents, list(self.roots)) as guard:
                                if not os.path.lexists(path):
                                    if str(path) in pending:
                                        guard.assert_held()
                                        self._event('unlink_reconciled_absent', path,
                                                    physical_space_reclaimed='not_measured')
                                    else:
                                        self._event('retained', path, reason='missing_without_intent')
                                    continue
                                # All content/object/hardlink checks share the actual
                                # deletion handle. No pathname unlink fallback exists.
                                with locked_file(path, delete=True) as (stream, unlink):
                                    try:
                                        self._inventory(stream, path, group, remaining)
                                    except ValueError as exc:
                                        self._event('retained', path, reason=str(exc))
                                        continue
                                    guard.assert_held()
                                    if str(path) in pending:
                                        self._event('unlink_not_performed', path)
                                    self._event('unlink_intent', path, file_identity=identity_from_stream(stream))
                                    checkpoint('after_intent', str(path))
                                    scope.assert_open()
                                    unlink()
                                guard.assert_held()
                                if os.path.lexists(path):
                                    raise ValueError('native unlink result not confirmed')
                                checkpoint('after_unlink', str(path))
                                self._event('unlink_complete', path, physical_space_reclaimed='not_measured')
                                checkpoint('after_complete', str(path))
                        except OSError as exc:
                            # Busy files remain eligible for later reconciliation;
                            # never turn a permission/IO failure into success.
                            deferred.append({'path': str(path), 'error_type': type(exc).__name__})
            result = self.snapshot()
            result['deferred'] = deferred
            return result

    def restore(self, *args, **kwargs):
        raise PermissionError('restoration is outside this cleanup adapter')
