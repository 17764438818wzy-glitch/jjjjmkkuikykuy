"""Stage-1 local handoff, deliberately disconnected from graph/service/intake."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory

from .handoff_store import HandoffLedger, RegistrationChanged, canonical
from .package import (CORE_NAMES, PackageError, inside, load, publish_package,
                      resolve_routes, seal_package, sha, validate_package)


def _digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def _files(root):
    """Fingerprint exact input bytes; no links to files outside the input tree."""
    if not root.is_dir() or root.is_symlink() or root.is_junction():
        raise ValueError('input must be an ordinary material directory')
    rows = []
    for path in sorted(root.rglob('*')):
        if path.is_symlink() or path.is_junction():
            raise ValueError('input links are not supported')
        relative = path.relative_to(root).as_posix()
        inside(root, relative)
        if path.is_file():
            rows.append(dict(path=relative, bytes=path.stat().st_size, sha256=sha(path)))
    if not rows:
        raise ValueError('empty input')
    return rows


def _identity(root, identity, sealed):
    names = [name for name in CORE_NAMES if name.endswith('.json')]
    if sealed:
        names += ['manifest.json', 'READY.json']
    for name in names:
        value = load(root / name)
        if not isinstance(value, dict):
            raise ValueError('input is not a final material object: ' + name)
        for field, expected in identity.items():
            if (sealed and name in ('manifest.json', 'READY.json')) or field in value:
                if value.get(field) != expected:
                    raise ValueError('identity mismatch: ' + field)


def _snapshot(profile, assets, catalog):
    routes, issues = resolve_routes(profile, assets, catalog)
    entries = {row['code']: row for row in catalog.get('majors', [])}
    targets = {row['code']: dict(code=row['code'], mapping=row, issue=None) for row in routes}
    for issue in issues:
        code = issue['code']
        if code is not None and code != '':
            targets[code] = dict(code=code, mapping=entries.get(code, {}), issue=issue['reason'])
    if any(not isinstance(code, str) or not code.strip() for code in targets):
        raise ValueError('specialty code must be a nonempty string')
    return dict(targets=[targets[code] for code in sorted(targets)], issues=issues)


def _expectations(snapshot, supplied):
    codes = {target['code'] for target in snapshot['targets']}
    result = supplied if supplied is not None else {
        code: dict(manifest_sha256=None, generation=0) for code in codes}
    if set(result) != codes:
        raise ValueError('expected_targets must cover the exact target snapshot')
    for value in result.values():
        digest, generation = value.get('manifest_sha256'), value.get('generation')
        if (set(value) != {'manifest_sha256', 'generation'} or type(generation) is not int
                or generation < 0 or (digest is None) != (generation == 0)
                or (digest is not None and (not isinstance(digest, str) or len(digest) != 64
                                           or any(ch not in '0123456789abcdef' for ch in digest)))):
            raise ValueError('invalid expected manifest/generation')
    return result


class PackageHandoff:
    """A receipt confirms registration history, never current readiness/admission.

    Public entry points do no network/model calls and create no execution workers.
    Recovery may call resume using only the persisted (thread_id, fingerprint).
    """

    def __init__(self, store, candidate_root):
        self.root = Path(candidate_root).resolve()
        if any(part.casefold() == 'updated manuscript' for part in self.root.parts):
            raise ValueError('candidate root must stay outside Updated manuscript')
        self.ledger = HandoffLedger(store)

    def prepare(self, thread_id, task, material_dir, catalog, *, mapping_version,
                expected_targets=None, repair_source=None):
        if repair_source is not None and (set(repair_source)!={'repair_request_id','manifest_sha256'}
                or repair_source['repair_request_id']!=thread_id):raise ValueError('repair run binding mismatch')
        if not isinstance(thread_id, str) or not thread_id.strip():
            raise ValueError('thread_id is required')
        if not isinstance(mapping_version, str) or not mapping_version.strip():
            raise ValueError('mapping_version is required')
        identity = {key: task[key] for key in ('task_id', 'source_id', 'source_sha256')}
        if any(not isinstance(value, str) or not value.strip() for value in identity.values()):
            raise ValueError('complete article identity is required')
        if task.get('delivery_task_id', identity['task_id']) != identity['task_id']:
            raise NotImplementedError('legacy cross-task adoption belongs to stage 8')
        revision = task.get('revision', 1)
        if type(revision) is not int or revision < 1:
            raise ValueError('positive revision is required')
        root = Path(material_dir).resolve()
        files = _files(root)
        sealed = (root / 'manifest.json').exists() or (root / 'READY.json').exists()
        _identity(root, identity, sealed)
        if sealed:
            validated = validate_package(root, identity, catalog, strict_seal=True)
            fixed_revision = re.fullmatch(r'r([0-9]+)', str(validated['manifest'].get('revision', '')))
            if not fixed_revision or int(fixed_revision[1]) < 1:
                raise ValueError('invalid sealed revision')
            revision = int(fixed_revision[1])
            profile, assets = validated['profile'], validated['assets']
        else:
            profile = load(root / 'profile.json')
            assets = load(root / 'assets.json')['items']
        snapshot = _snapshot(profile, assets, catalog)
        expectations = _expectations(snapshot, expected_targets)
        history = task.get('processing_history', [])
        fingerprint = _digest(dict(identity=identity, files=files, target_snapshot=snapshot,
                                   mapping_version=mapping_version, protocol='paperflow/1.0',
                                   processing_history=history, repair_source=repair_source,
                                   stage_dependencies=task.get('stage_dependencies',{})))
        destination = self.root / 'sealed' / hashlib.sha256(thread_id.encode('utf-8')).hexdigest() / fingerprint
        data = dict(thread_id=thread_id, input_fingerprint=fingerprint,
                    identity=identity, material_dir=str(root), material_files=files,
                    sealed=sealed, revision=revision, created_at=datetime.now(timezone.utc).isoformat(),
                    processing_history=history, target_snapshot=snapshot,
                    catalog=catalog, mapping_version=mapping_version,
                    expected_targets=expectations, package_dir=str(destination),repair_source=repair_source,
                    stage_dependencies=task.get('stage_dependencies',{}))
        return self.ledger.prepare(data)

    def _published(self, intent):
        destination = Path(intent['package_dir'])
        self.ledger._retained(intent)
        if not destination.is_relative_to(self.root / 'sealed'):
            raise ValueError('publication root conflict')
        manifest_bytes = (destination / 'manifest.json').read_bytes()
        if (intent['manifest_sha256'] is None or manifest_bytes != intent['manifest_bytes']
                or sha(destination / 'manifest.json') != intent['manifest_sha256']):
            raise ValueError('published manifest hash conflict')
        _identity(destination, intent['identity'], True)
        validated = validate_package(destination, intent['identity'], intent['catalog'], strict_seal=True)
        if _snapshot(validated['profile'], validated['assets'], intent['catalog']) != intent['target_snapshot']:
            raise ValueError('published target snapshot conflict')
        return validated, manifest_bytes

    def publish(self, thread_id, fingerprint):
        intent = self.ledger.intent(thread_id, fingerprint)
        self.ledger._retained(intent)
        destination = Path(intent['package_dir'])
        if not destination.is_relative_to(self.root / 'sealed'):
            raise ValueError('publication root conflict')
        if destination.exists():
            self._published(intent)
            return destination
        source = Path(intent['material_dir'])
        if _files(source) != intent['material_files']:
            raise ValueError('input changed after publication intent')
        work_root = self.root / 'staging'
        work_root.mkdir(parents=True, exist_ok=True)
        # Only our own generated staging directory is removed on normal exit.
        with TemporaryDirectory(prefix='handoff-', dir=work_root) as temporary:
            from .cleanup_paths import ASSET_SCHEMA
            def track(c):
                c.execute(ASSET_SCHEMA)
                self.ledger._retained(self.ledger._intent(c,thread_id,fingerprint))
                c.execute('INSERT OR IGNORE INTO workflow_artifacts VALUES(?,?,?,?)',
                    (str(Path(temporary).resolve()),intent['identity']['task_id'],thread_id,'staging'))
            self.ledger._write(track)
            staging = Path(temporary) / 'material'
            shutil.copytree(source, staging)
            if _files(staging) != intent['material_files']:
                raise ValueError('input changed during staging copy')
            if not intent['sealed']:
                task = dict(intent['identity'], processing_history=intent['processing_history'])
                seal_package(staging, task, intent['revision'], created_at=intent['created_at'])
            _identity(staging, intent['identity'], True)
            validated = validate_package(staging, intent['identity'], intent['catalog'], strict_seal=True)
            if _snapshot(validated['profile'], validated['assets'], intent['catalog']) != intent['target_snapshot']:
                raise ValueError('sealed target snapshot conflict')
            manifest_bytes = (staging / 'manifest.json').read_bytes()
            self.ledger.bind_manifest(thread_id, fingerprint, validated['manifest_sha256'], manifest_bytes)
            try:
                publish_package(staging, destination)
            except (OSError, PackageError):
                # A concurrent publisher can win the rename. Accept only identical bytes.
                if not destination.exists():
                    raise
                self._published(self.ledger.intent(thread_id, fingerprint))
        self._published(self.ledger.intent(thread_id, fingerprint))
        return destination

    def resume(self, thread_id, fingerprint):
        self.publish(thread_id, fingerprint)
        for attempt in range(5):
            intent, validated, manifest_bytes, registered = self.verify_registration(thread_id, fingerprint)
            try:
                return self.ledger.register(thread_id, fingerprint, validated, manifest_bytes,
                                            verified_registration=registered)
            except RegistrationChanged:
                if attempt == 4:
                    raise

    def verify_registration(self, thread_id, fingerprint):
        """Verify both sealed and authoritative registered directories, outside write transactions."""
        intent = self.ledger.intent(thread_id, fingerprint)
        validated, manifest_bytes = self._published(intent)
        registered = self.ledger.registration(validated['manifest_sha256'])
        if registered is not None:
            if registered['lifecycle'] == 'deleting':raise ValueError('package is deleting')
            try:
                self._published(dict(intent, package_dir=registered['package_dir']))
            except (OSError, ValueError) as exc:
                raise PackageError('registered package invalid: ' + str(exc), 'identity') from exc
        return intent, validated, manifest_bytes, registered

    def handoff(self, thread_id, task, material_dir, catalog, **options):
        intent = self.prepare(thread_id, task, material_dir, catalog, **options)
        return self.resume(intent['thread_id'], intent['input_fingerprint'])
