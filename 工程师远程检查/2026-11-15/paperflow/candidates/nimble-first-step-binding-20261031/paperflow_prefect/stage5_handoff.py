"""Stage4 admission -> immutable evidence -> native NAS -> frozen-file cleanup."""
from pathlib import Path
from filelock import FileLock
from .archive import archive_originals
from .cleanup import CleanupCoordinator
from .cleanup_evidence import validate_scope, verified_nas
from .records import Records, identity, read_json, sha_file, write_json
from .stage5_evidence import (execution_inventory, frozen_handoff, held_handoff_evidence,
                              snapshot_handoff)


def admitted_handoff(work):
    work = Path(work).resolve()
    frozen = frozen_handoff(work)
    admission = Records(work).cached('stage4-admission')
    handoff = frozen['handoff']
    if (not admission or admission.get('status') != 'ready' or
            admission.get('handoff_sha256') != identity(handoff) or
            admission.get('validation_scope') != handoff['validation_scope'] or
            admission.get('inventory_sha256') != identity(frozen['paper'])):
        raise ValueError('whole-paper stage4 admission is missing or rebound')
    return frozen


def accepted_evidence(paper, work, data_root):
    work, data_root = Path(work).resolve(), Path(data_root).resolve()
    frozen = admitted_handoff(work)
    if frozen['paper'] != paper:
        raise ValueError('stage5 paper differs from admitted frozen inventory')
    records = Records(work)
    nas = records.cached('nas')
    if (not nas or nas.get('status') != 'verified' or
            any(nas.get(key) != paper[key] for key in ('paper_id', 'revision'))):
        raise ValueError('whole-paper RAG and NAS must both be verified before cleanup')
    paths = {work / 'inventory.json', work / 'archive/original-registration.json'}
    for stage in ('stage4-handoff', 'stage4-admission', 'nas'):
        if not records.cached(stage):
            raise ValueError('missing immutable stage5 evidence')
        state = records.status(stage)
        if not state['artifacts']:
            raise ValueError('stage5 evidence has no retained artifacts')
        paths.add(records.path(stage))
        paths.update(Path(p) for p in state['artifacts'])
    if any(not p.resolve().is_relative_to(data_root) for p in paths):
        raise ValueError('stage5 evidence must stay in bound records')
    return {str(p.resolve()): sha_file(p) for p in paths}


def finish_stage5(work, coordinator):
    work = coordinator._work(work)
    frozen = admitted_handoff(work)
    paper, handoff = frozen['paper'], frozen['handoff']
    records = Records(work)
    nas = records.cached('nas')
    current, saved = coordinator.inspect(work), records.cached('cleanup')
    if not nas or nas.get('status') != 'verified' or not saved:
        raise ValueError('archive or cleanup receipt is missing')
    if saved['journal_head_sha256'] != current['journal_head_sha256']:
        raise ValueError('cleanup receipt is not committed at the current journal head')
    result = dict(status='complete' if current['status'] == 'complete' else 'incomplete',
                  paper_id=paper['paper_id'], revision=paper['revision'], handoff_id=handoff['handoff_id'],
                  validation_scope=handoff['validation_scope'], archive_status='verified', cleanup=current,
                  local_folder_retained=True, local_originals='cleaned' if current['status'] == 'complete' else 'see_cleanup',
                  retained_intermediates=handoff.get('retained_intermediates', []),
                  evidence='synthetic_rag' if handoff['validation_scope'] == 'isolated_fixture' else 'stage4_verified')
    if result['status'] != 'complete':
        result['failures'] = {'cleanup': current['status']}
    path = work / ('completed.json' if result['status'] == 'complete' else 'completion-status.json')
    write_json(path, result)
    if result['status'] == 'complete':
        records.complete('complete', dict(status='complete', record=str(path)), [path])
    result['record'] = str(path)
    return result


class Stage5Receiver:
    def __init__(self, data_root, library_root, destination, *, enabled=False,
                 isolation_root=None, reserve_bytes=2 * 1024**3):
        self.coordinator = CleanupCoordinator(data_root, library_root, destination, enabled=enabled)
        self.isolation = Path(isolation_root).resolve(strict=True) if isolation_root is not None else None
        self.reserve_bytes = reserve_bytes

    def _enabled(self):
        if not self.coordinator.enabled:
            raise PermissionError('stage5 local execution is disabled')

    def receive(self, handoff):
        self._enabled()
        from .nas_handoff import validate_nas_handoff
        if not isinstance(handoff, dict):
            handoff = read_json(handoff)
        checked = validate_nas_handoff(handoff, verify_files=True)
        if checked['status'] != 'ready':
            return dict(status='not_ready', reasons=checked['reasons'], nas_operations=0, removed_files=0)
        paper = execution_inventory(checked)
        validate_scope(paper, self.coordinator.library)
        if checked['validation_scope'] == 'isolated_fixture':
            if (self.isolation is None or not self.coordinator.library.is_relative_to(self.isolation) or
                    not self.coordinator.data.is_relative_to(self.isolation)):
                raise PermissionError('synthetic RAG proof requires explicitly bound isolated local roots')
        elif checked['validation_scope'] != 'production':
            raise ValueError('unknown stage4 validation scope')
        work = self.coordinator.data / 'stage5' / paper['paper_id'] / paper['revision'] / checked['handoff_id']
        work = self.coordinator._work(work)
        work.mkdir(parents=True, exist_ok=True)
        with FileLock(work / 'stage5-operation.lock', timeout=0), held_handoff_evidence(checked):
            if validate_nas_handoff(checked, verify_files=True) != checked:
                raise ValueError('stage4 handoff changed while accepting')
            frozen = snapshot_handoff(checked, work)
            admission = dict(status='ready', handoff_sha256=identity(checked),
                             inventory_sha256=identity(frozen['paper']), validation_scope=checked['validation_scope'])
            path = work / 'stage4/admission.json'
            records = Records(work)
            if records.path('stage4-admission').exists():
                try:
                    cached = records.cached('stage4-admission')
                except (KeyError, TypeError) as exc:
                    raise ValueError('existing stage4 admission is corrupt') from exc
                if cached != admission:
                    raise ValueError('existing stage4 admission differs')
                admitted_handoff(work)
            else:
                if (work / 'cleanup/association.json').exists():
                    raise ValueError('frozen cleanup admission is missing')
                if path.exists():
                    if read_json(path) != admission:
                        raise ValueError('uncommitted stage4 admission differs')
                else:
                    write_json(path, admission)
                records.complete('stage4-admission', admission, [path])
        return dict(status='ready', work=str(work), paper_id=paper['paper_id'], revision=paper['revision'])

    def archive(self, work):
        self._enabled()
        work = self.coordinator._work(work)
        with FileLock(work / 'stage5-operation.lock', timeout=0), FileLock(work / 'cleanup-operation.lock', timeout=0):
            frozen = admitted_handoff(work)
            records = Records(work)
            existing = records.cached('nas')
            if existing:
                with verified_nas(frozen['paper'], work, self.coordinator.destination):
                    return existing
            result = archive_originals(frozen['paper'], work / 'archive', self.coordinator.destination,
                                       reserve_bytes=self.reserve_bytes)
            records.complete('nas', result, [work / 'archive/nas-receipt.json'])
            return result

    def cleanup(self, work, *, checkpoint=None, stop=None):
        self._enabled()
        work = self.coordinator._work(work)
        with FileLock(work / 'stage5-operation.lock', timeout=0):
            frozen = admitted_handoff(work)
            if not (work / 'cleanup/association.json').exists():
                self.coordinator.freeze(frozen['paper'], work, intermediates=frozen['handoff'].get('intermediates', []))
            current = self.coordinator.inspect(work)
            saved = Records(work).cached('cleanup')
            if not (current['pending_files'] == 0 and saved and
                    saved['journal_head_sha256'] == current['journal_head_sha256']):
                self.coordinator.run(work, checkpoint=checkpoint, stop=stop)
            return finish_stage5(work, self.coordinator)

    def resume(self, person_dir):
        self._enabled()
        recovered = self.coordinator.lookup(person_dir)
        if recovered is None:
            return dict(status='not_received', nas_operations=0, removed_files=0)
        return self.cleanup(recovered['work'])
