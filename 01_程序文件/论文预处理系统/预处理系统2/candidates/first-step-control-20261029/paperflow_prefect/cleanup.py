"""Explicitly enabled cleanup of the already verified, frozen library files."""
from contextlib import contextmanager, ExitStack
import os
from pathlib import Path
from filelock import FileLock
from .intake import files_below, paper_identity, verify_inventory
from .records import Records, identity, read_json, sha_file, write_json
from .cleanup_evidence import validate_scope, capture_groups, upstream_evidence, hold_evidence, verified_nas


class CleanupCoordinator:
    def __init__(self, data_root, library_root, destination, *, enabled=False):
        self.data = Path(data_root).resolve()
        self.library = Path(library_root).resolve()
        # Keep intake/index lookup local. Resolve and open NAS only at the
        # explicit archive verification boundary, not on every input scan.
        self.destination = Path(destination).absolute()
        self.enabled = enabled is True
        for a, b in ((self.data, self.library), (self.destination, self.library), (self.data, self.destination)):
            if a == b or a.is_relative_to(b) or b.is_relative_to(a):
                raise ValueError('records, input library and archive must be separate')

    def _work(self, work):
        work = Path(work).resolve()
        if work == self.data or not work.is_relative_to(self.data):
            raise ValueError('cleanup records outside bound data root')
        return work

    def _index(self, paper):
        return self.data / 'cleanup-index' / (paper['paper_id'] + '.json')

    def _location_index(self, paper):
        key = paper_identity(paper['person_dir'], self.library)
        return self.data / 'cleanup-index/by-location' / (key + '.json')

    def _index_value(self, work, association):
        paper = association['paper']
        return {'schema': 'prefect-cleanup-index/1', 'paper_id': paper['paper_id'],
                'person_dir': paper['person_dir'], 'work': str(work),
                'association_sha256': identity(association)}

    def freeze(self, paper, work, *, intermediates=None):
        if not self.enabled:
            raise PermissionError('local cleanup is disabled')
        work = self._work(work)
        association_path = work / 'cleanup/association.json'
        if association_path.exists():
            association = self._load(work, require_index=False)
            if association['paper'] != paper:
                raise ValueError('cleanup frozen paper changed')
            if intermediates is not None and identity(intermediates) != association.get('intermediate_request_sha256'):
                raise ValueError('cleanup frozen intermediate list changed')
        else:
            validate_scope(paper, self.library)
            verify_inventory(paper)
            evidence = upstream_evidence(paper, work, self.data)
            with verified_nas(paper, work, self.destination):
                from .cleanup_intermediates import freeze_intermediates
                caches, retained, proofs = freeze_intermediates(paper, work, self.data, intermediates or [], evidence)
                evidence.update(proofs)
                groups = capture_groups(paper, extra_files=caches)
                association = {'schema': 'prefect-cleanup/1', 'paper': paper,
                    'library_root': str(self.library), 'data_root': str(self.data),
                    'destination': str(self.destination), 'work': str(work),
                    'evidence': evidence, 'groups': groups, 'intermediates': caches,
                    'intermediate_request_sha256': identity(intermediates or []),
                    'intermediate_retained': retained,
                    'intermediate_roots': sorted({str(Path(r['path']).parent) for r in caches})}
                if paper.get('receipt_schema') == 'paperflow-receipt/1':
                    association['observed_source_paths'] = [str(p) for p in files_below(paper['person_dir'])]
                write_json(association_path, association)
                Records(work).complete('cleanup-association', {'association_sha256': identity(association)},
                                       [association_path])
        self._journal(work, association)
        index = self._index_value(work, association)
        index_path = self._index(paper)
        if index_path.exists() and read_json(index_path) != index:
            raise ValueError('another frozen cleanup revision owns this person')
        write_json(index_path, index)
        if paper.get('receipt_schema') == 'paperflow-receipt/1':
            location_path = self._location_index(paper)
            if location_path.exists() and read_json(location_path) != index:
                raise ValueError('another formal paper owns the frozen location')
            write_json(location_path, index)
        return association

    def _load(self, work, require_index=True):
        work = self._work(work)
        record = Records(work).cached('cleanup-association')
        if not record:
            raise ValueError('cleanup association not committed')
        association = read_json(work / 'cleanup/association.json')
        if record['association_sha256'] != identity(association):
            raise ValueError('cleanup association changed')
        if (association['work'] != str(work) or association['data_root'] != str(self.data) or
                association['library_root'] != str(self.library) or
                association['destination'] != str(self.destination)):
            raise ValueError('cleanup roots changed')
        validate_scope(association['paper'], self.library)
        if require_index:
            index = read_json(self._index(association['paper']))
            if index != self._index_value(work, association):
                raise ValueError('cleanup index differs')
            if association['paper'].get('receipt_schema') == 'paperflow-receipt/1':
                if read_json(self._location_index(association['paper'])) != index:
                    raise ValueError('cleanup location mapping differs')
        for name, digest in association['evidence'].items():
            path = Path(name)
            if not path.resolve().is_relative_to(self.data) or sha_file(path) != digest:
                raise ValueError('cleanup evidence changed')
        return association

    @contextmanager
    def _hold_bindings(self, work, association):
        from paperflow.original_archive.lifecycle import locked_file
        paths = [work / 'cleanup/association.json', Records(work).path('cleanup-association'),
                 self._index(association['paper'])]
        if association['paper'].get('receipt_schema') == 'paperflow-receipt/1':
            paths.append(self._location_index(association['paper']))
        with ExitStack() as stack:
            for path in paths:
                stack.enter_context(locked_file(path))
            # Validate again only after every recovery binding is pinned. A
            # replacement between initial lookup and handle acquisition cannot
            # silently become the authority for subsequent deletions.
            if self._load(work) != association:
                raise ValueError('cleanup binding changed before deletion')
            yield

    def _journal(self, work, association):
        from .cleanup_journal import CleanupJournal
        return CleanupJournal(Path(work) / 'cleanup/journal', identity(association),
                              association['groups'], [self.library, *association.get('intermediate_roots', [])],
                              allow_intermediates=bool(association.get('intermediates')))

    def _retained_intermediates(self, result, association):
        result['retained'].extend(association.get('intermediate_retained', []))
        result['unlisted_source_files'] = sorted(set(association.get('observed_source_paths', [])) -
                                                {r['path'] for r in association['paper']['archive_files']})
        if result['retained'] and result['status'] == 'complete':
            result['status'] = 'retained'
        return result

    def _new_files(self, association, completed_paths):
        paper = association['paper']
        if paper.get('receipt_schema') == 'paperflow-receipt/1':
            current = {str(p) for p in files_below(paper['person_dir'])}
            original = set(association['observed_source_paths'])
        else:
            current = {str(p) for name in ('正文', '附件') for p in files_below(Path(paper['person_dir']) / name)}
            original = {r['path'] for r in paper['archive_files']}
        # A path reappearing after a committed removal belongs to new material,
        # even when its bytes happen to match the old content.
        return sorted((current - original) | (current & set(completed_paths)))

    def inspect(self, work):
        work = self._work(work)
        association = self._load(work)
        result = self._journal(work, association).snapshot()
        self._retained_intermediates(result, association)
        result['new_files'] = self._new_files(association, result['completed_paths'])
        if result['new_files'] and result['status'] == 'complete':
            result['status'] = 'retained'
        return result

    def lookup(self, person_dir):
        person = Path(person_dir).resolve()
        paper_id = paper_identity(person, self.library)
        index_path = self.data / 'cleanup-index' / (paper_id + '.json')
        location_path = self.data / 'cleanup-index/by-location' / (paper_id + '.json')
        if location_path.exists():
            index_path = location_path
        if not index_path.exists():
            return None
        index = read_json(index_path)
        if ((index_path != location_path and index['paper_id'] != paper_id) or index['person_dir'] != str(person)):
            raise ValueError('cleanup scan identity differs')
        association = self._load(index['work'])
        if index != self._index_value(Path(index['work']), association):
            raise ValueError('cleanup scan mapping differs')
        result = self.inspect(index['work'])
        action = 'complete' if result['status'] == 'complete' else 'resume_cleanup'
        return {'paper': association['paper'], 'work': association['work'],
                'action': action, 'cleanup': result}

    def recovery_directories(self):
        """Discover formal H2.1 papers from committed bindings, not leftovers."""
        root = self.data / 'cleanup-index/by-location'
        result = []
        for path in files_below(root):
            if path.parent != root or path.suffix != '.json':
                continue
            value = read_json(path)
            person = Path(value['person_dir']).resolve()
            expected = root / (paper_identity(person, self.library) + '.json')
            if path != expected or not self.lookup(person):
                raise ValueError('invalid cleanup recovery discovery mapping')
            result.append(str(person))
        return sorted(set(result))

    def run(self, work, *, checkpoint=None, stop=None):
        if not self.enabled:
            raise PermissionError('local cleanup is disabled')
        work = self._work(work)
        with FileLock(work / 'cleanup-operation.lock', timeout=0):
            association = self._load(work)
            coordinator = self
            class Scope:
                def assert_open(self):
                    if not coordinator.enabled or (stop is not None and stop.is_set()):
                        raise InterruptedError('cleanup stopped before deletion')
                @contextmanager
                def commit_guard(self):
                    self.assert_open()
                    yield
            with self._hold_bindings(work, association), hold_evidence(association), \
                    verified_nas(association['paper'], work, self.destination):
                journal = self._journal(work, association)
                result = journal.remove(Scope(), checkpoint=checkpoint)
                self._retained_intermediates(result, association)
                result['new_files'] = self._new_files(association, result['completed_paths'])
                if result['new_files'] and result['status'] == 'complete':
                    result['status'] = 'retained'
                result.update(paper_id=association['paper']['paper_id'], revision=association['paper']['revision'],
                              association_sha256=identity(association), local_folder_retained=True)
                path = work / 'cleanup/receipt.json'
                write_json(path, result)
                Records(work).complete('cleanup', result, [path])
                return result
