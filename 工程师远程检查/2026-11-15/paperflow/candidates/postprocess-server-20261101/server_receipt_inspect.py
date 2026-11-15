"""Read-only Linux integrity inspection of one frozen postprocessing receipt.

This is an artifact/path verifier, NOT a substitute for the reviewed offline
business processor, its semantic checks, privacy policy or original coordinator.
It grants neither job completion nor RAG admission. The caller must independently
prove the same job's terminated execution and authenticated receipt provenance.
Only declared F: inputs and this identity's folder below a separate short F:
output root are readable. Original receipt/manifest bytes are never rewritten.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import stat

from remote_jobs import _validated
from snapshot_wire import snapshot_sha
from windows_snapshot import EvidenceMap, _windows_name


class ReceiptInspectionError(ValueError):
    pass


def _require(good, detail):
    if not good: raise ReceiptInspectionError(detail)


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()


def _win(value):
    path, _, parts = _windows_name(value)
    _require(path.drive.lower() == 'f:' and parts, 'only explicit F: paths are supported')
    return path


def _key(value): return str(_win(value)).lower()


def _body_input_fingerprint(manifest):
    """Match body_adapter.process_body using its frozen Windows context.

    The producer must freeze the ORIGINAL ``str(Path(core).resolve())`` spelling.
    Linux must not resolve that Windows string against its own working directory.
    inspect_receipt binds these fields to the original job.snapshot_sha256 before
    this helper is used. No fallback or context inference is permitted.
    """
    try:
        catalog, root = manifest['catalog'], manifest['paperflow_root_windows']
        _require(isinstance(catalog, dict) and catalog, 'missing frozen processor context catalog')
        _require(isinstance(root, str), 'missing frozen processor context core path')
        path = _win(root)
        _require(path.drive == 'F:' and path.name == 'core' and str(path) == root,
            'noncanonical frozen processor context core path')
        evidence = manifest['inputs']['body']
        _require(isinstance(evidence, dict), 'missing frozen processor context evidence')
        return _hash(dict(evidence=evidence, catalog=catalog, paperflow_root=root))
    except (KeyError, TypeError, ValueError) as exc:
        raise ReceiptInspectionError('missing or invalid frozen processor context') from exc


def _reference(ref):
    _require(isinstance(ref, dict) and isinstance(ref.get('path'), str)
        and isinstance(ref.get('sha256'), str) and re.fullmatch('[a-f0-9]{64}', ref['sha256']),
        'invalid file reference')
    _win(ref['path'])
    if 'bytes' in ref:
        _require(type(ref['bytes']) is int and ref['bytes'] >= 0, 'invalid reference size')
    return ref


class _LinuxSnapshot:
    """Pin the private root and rewalk every component with O_NOFOLLOW."""
    def __init__(self, root):
        _require(os.name == 'posix' and hasattr(os, 'O_NOFOLLOW')
            and os.open in os.supports_dir_fd, 'Linux no-follow inspection is required')
        self.root = Path(root)
        _require(self.root.is_absolute() and '..' not in self.root.parts, 'absolute private snapshot root required')
        self.fd = self._open_root()
        info = os.fstat(self.fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            os.close(self.fd)
            raise ReceiptInspectionError('snapshot root must be private to the inspector')
        self.identity = (info.st_dev, info.st_ino)

    def _open_root(self):
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for part in self.root.parts[1:]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd); fd = child
            return fd
        except BaseException:
            os.close(fd); raise

    def close(self):
        if self.fd is not None:
            os.close(self.fd); self.fd = None

    def visible(self):
        fd = self._open_root()
        try:
            info = os.fstat(fd)
            _require((info.st_dev, info.st_ino) == self.identity, 'snapshot root changed')
        finally: os.close(fd)

    def _parent(self, parts):
        fd = os.dup(self.fd)
        try:
            for part in parts:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd); fd = child
            return fd
        except BaseException:
            os.close(fd); raise

    def read(self, path, ref, *, json_value=False):
        parts = ('drives', 'f', *_win(path).parts[1:])
        self.visible()
        parent = self._parent(parts[:-1])
        try:
            fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            with os.fdopen(fd, 'rb') as stream:
                before = os.fstat(stream.fileno())
                _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1,
                    'evidence must be an independent regular file')
                hasher, chunks, size = hashlib.sha256(), [], 0
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    hasher.update(chunk); size += len(chunk)
                    if json_value == 'raw':
                        _require(size <= 4 * 1024 * 1024, 'body seal metadata exceeds its bound')
                    if json_value: chunks.append(chunk)
                after = os.fstat(stream.fileno())
            _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) and after.st_nlink == 1,
                'evidence changed during inspection')
            current_parent = self._parent(parts[:-1])
            try:
                current = os.stat(parts[-1], dir_fd=current_parent, follow_symlinks=False)
                _require(stat.S_ISREG(current.st_mode) and current.st_nlink == 1
                    and (current.st_dev, current.st_ino) == (after.st_dev, after.st_ino), 'evidence path changed')
            finally: os.close(current_parent)
            self.visible()
            _require(hasher.hexdigest() == ref['sha256']
                and ('bytes' not in ref or size == ref['bytes']), 'evidence bytes or hash changed')
            row = dict(path=path, linux_path=str(self.root.joinpath(*parts)), sha256=hasher.hexdigest(), bytes=size)
            if json_value:
                raw = b''.join(chunks)
                if json_value == 'raw':
                    # Preserve the original UTF-8 byte layout so the receiver
                    # can check it against the original sealed artifact hash.
                    return row, raw.decode('utf8')
                value = json.loads(raw)
                _require(isinstance(value, dict), 'artifact JSON must be an object')
                return row, value
            return row, None
        finally: os.close(parent)


class _Inspection:
    def __init__(self, fs, inputs, folder, declared, mapper):
        self.fs, self.inputs, self.folder = fs, inputs, folder
        self.mapper = mapper
        self.declared = {_key(row['path']): row for row in declared}
        self.rows = {}

    def read(self, ref, *, input_only=False, json_value=False):
        _reference(ref)
        key = _key(ref['path'])
        declared = self.declared.get(key)
        if declared:
            _require(ref['sha256'] == declared['sha256']
                and _win(ref['path']).parts[1:] == _win(declared['path']).parts[1:], 'input binding changed')
            checked = dict(declared)
            kind = 'input'
        else:
            _require(not input_only and _win(ref['path']).is_relative_to(self.folder),
                'undeclared input or result outside current job')
            checked, kind = ref, 'output'
        row, value = self.fs.read(ref['path'], checked, json_value=json_value)
        if 'bytes' in ref: _require(row['bytes'] == ref['bytes'], 'reference size mismatch')
        # Inputs, public outputs and private outputs share one Windows namespace,
        # even though the Linux filesystem can store incompatible spellings.
        self.mapper.bind(row['path'], row['sha256'], row['bytes'])
        if key in self.rows:
            _require(self.rows[key]['sha256'] == row['sha256'], 'conflicting file hash')
        self.rows[key] = dict(row, kind=kind)
        return value

    def original_refs(self, value):
        """References from trusted input structures only; never model answers."""
        if isinstance(value, list):
            for item in value: self.original_refs(item)
        elif isinstance(value, dict):
            if isinstance(value.get('path'), str) and 'sha256' in value:
                # Relative archive/member names are facts, not local file reads.
                if PureWindowsPath(value['path']).is_absolute(): self.read(value, input_only=True)
            for field, hash_field in [('file', 'source_sha256'), ('text_file', 'text_sha256'),
                    ('index_file', 'index_sha256'), ('image_path', 'sha256'), ('original_image_path', 'original_sha256')]:
                if value.get(field) is not None:
                    self.read(dict(path=value[field], sha256=value.get(hash_field)), input_only=True)
            for table in ('file_hashes', 'retained_file_hashes'):
                for path, sha in value.get(table, {}).items():
                    self.read(dict(path=path, sha256=sha), input_only=True)
            # Actual request.text may contain a JSON material projection. This
            # is only applied to request objects, not arbitrary content strings.
            if isinstance(value.get('messages'), list):
                messages = value['messages']
                if len(messages) > 1 and isinstance(messages[1].get('content'), list):
                    content = messages[1]['content']
                    if content and content[0].get('type') == 'text':
                        projection = json.loads(content[0]['text'])
                        if isinstance(projection, dict) and isinstance(projection.get('material'), dict):
                            self.original_refs(projection['material'])
            for item in value.values():
                if isinstance(item, (dict, list)): self.original_refs(item)

    def original_json(self, ref):
        value = self.read(ref, input_only=True, json_value=True)
        self.original_refs(value)
        return value


def inspect_receipt(*, job, manifest, receipt, snapshot_root, output_root_windows):
    """Return Linux SHA/size mappings after integrity checks, without changing input.

    A passed result is NOT semantic/privacy acceptance or RAG admission. Receipt
    authenticity and a terminated worker remain mandatory caller obligations.
    The returned canonical receipt hash does not claim original JSON byte layout.
    """
    fs = None
    try:
        _validated(job)
        _require(isinstance(manifest, dict) and manifest.get('schema') == 'paperflow-remote-snapshot/1'
            and snapshot_sha(manifest) == job['snapshot_sha256']
            and all(manifest.get(k) == job[k] for k in ('job_id','claim_id','identity','generation','manifest_sha256','code_sha256')),
            'frozen manifest identity mismatch')
        identity = job['identity']; inputs = manifest['inputs']
        _require(inputs.get('identity') == identity and inputs.get('manifest_sha256') == job['manifest_sha256']
            and inputs.get('body', {}).get('identity') == identity, 'original input identity mismatch')
        expected_body_fingerprint = _body_input_fingerprint(manifest)
        _require(isinstance(receipt, dict) and receipt.get('schema') == 'paperflow-postprocessing-receipt/1'
            and receipt.get('identity') == identity and receipt.get('claim_id') == job['claim_id']
            and receipt.get('manifest_sha256') == job['manifest_sha256']
            and receipt.get('validation_status') == 'passed', 'folder receipt identity mismatch')
        output = _win(output_root_windows)
        _require(len(str(output)) <= 64, 'short Windows output root required')
        folder = output / _hash(identity)
        declared = manifest['files']
        _require(isinstance(declared, list) and declared, 'frozen input files missing')
        seen, mapper = set(), EvidenceMap(Path(snapshot_root))
        for ref in declared:
            _reference(ref)
            key = _key(ref['path'])
            _require(key not in seen and type(ref.get('bytes')) is int and ref['bytes'] >= 0,
                'duplicate or incomplete input declaration')
            _require(not _win(ref['path']).is_relative_to(output), 'output overlaps original inputs')
            seen.add(key); mapper.bind(ref['path'], ref['sha256'], ref['bytes'])
        artifact_refs = receipt['artifacts']
        _require(isinstance(artifact_refs, list) and artifact_refs, 'folder artifact coverage missing')
        artifact_index = {}
        for ref in artifact_refs:
            _reference(ref); key = _key(ref['path'])
            _require(key not in artifact_index, 'duplicate folder artifact')
            _require(key in seen or _win(ref['path']).is_relative_to(folder), 'result outside current job')
            artifact_index[key] = ref
        expected_attachments = inputs['attachments']
        _require(isinstance(expected_attachments, list)
            and all(a.get('ticket') == identity and isinstance(a.get('attachment_id'), str) and a['attachment_id'] for a in expected_attachments),
            'original attachment identity mismatch')
        ids = [a['attachment_id'] for a in expected_attachments]
        received = receipt['attachments']
        _require(len(ids) == len(set(ids)) and isinstance(received, list)
            and len(received) == len(ids) and {a['attachment_id'] for a in received} == set(ids),
            'incomplete or duplicate attachment coverage')
        _require(_win(receipt['body']['package_path']) == folder / 'body/publication', 'body package outside current job')
        fs = _LinuxSnapshot(snapshot_root)
        check = _Inspection(fs, inputs, folder, declared, mapper)
        for ref in declared: check.read(ref, input_only=True)
        check.original_refs(inputs)
        body_input = inputs['body']
        original_receipt = check.original_json(body_input['receipt'])
        _require(original_receipt.get('schema') == 'paperflow-receipt/1'
            and all(original_receipt.get(k) == identity[k] for k in ('group_id','paper_id','receipt_id')),
            'original receipt identity mismatch')
        original_rows = original_receipt['inventory']['attachments']
        original_by_id = {row['file_id']: row for row in original_rows}
        _require(len(original_by_id) == len(original_rows) and set(original_by_id) == set(ids),
            'original receipt attachment coverage mismatch')
        original_body = original_receipt['inventory']['body']
        _require(_key(original_body['path']) == _key(body_input['source']['path'])
            and original_body['sha256'] == body_input['source']['sha256'], 'original receipt body source mismatch')
        for row in original_rows:
            for ref in row.get('evidence_records', []): check.original_json(ref)
        for key in ('material', 'reading_material'):
            if key in body_input: check.original_json(body_input[key])
        plan = body_input.get('plan', {})
        for ref in plan.get('files', {}).values(): check.original_json(ref)
        for row in body_input.get('responses', []):
            for key in ('request', 'state', 'request_images'):
                if row.get(key): check.original_json(row[key])
        for item in expected_attachments:
            for key in ('evidence', 'request', 'request_state'):
                if item.get('model_refs', {}).get(key): check.original_json(item['model_refs'][key])
        for ref in artifact_refs: check.read(ref)
        expected_artifacts = set()
        def artifact(name, *, json_value=False):
            key = _key(str(name)); _require(key in artifact_index, 'required artifact omitted from receipt')
            expected_artifacts.add(key)
            return check.read(artifact_index[key], json_value=json_value)
        body_result = artifact(folder / 'body/body-result.json', json_value=True)
        _require(body_result.get('schema') == 'body-postprocess-result/1'
            and body_result.get('status') == 'body_package_verified' and body_result.get('identity') == identity
            and body_result.get('package_path') == receipt['body']['package_path']
            and body_result.get('manifest_sha256') == receipt['body']['manifest_sha256']
            and body_result.get('model_requests_added') == 0 and body_result.get('original_responses_retained') is True
            and body_result.get('input_fingerprint') == expected_body_fingerprint
            and isinstance(body_result.get('private_artifacts'), dict) and body_result['private_artifacts'],
            'body result identity mismatch')
        for ref in body_result.get('private_artifacts', {}).values():
            _require(_win(ref['path']).is_relative_to(folder / 'body/private'), 'private body artifact escaped')
            check.read(ref)
        package = folder / 'body/publication'
        package_manifest = artifact(package / 'manifest.json', json_value=True)
        ready = artifact(package / 'READY.json', json_value=True)
        manifest_ref = artifact_index[_key(str(package / 'manifest.json'))]
        ready_ref = artifact_index[_key(str(package / 'READY.json'))]
        manifest_text = check.read(manifest_ref, json_value='raw')
        ready_text = check.read(ready_ref, json_value='raw')
        task = body_result['task']
        _require(package_manifest.get('schema') == 'paperflow/1.0'
            and manifest_ref['sha256'] == receipt['body']['manifest_sha256']
            and ready.get('status') == 'ready' and ready.get('manifest') == 'manifest.json'
            and ready.get('manifest_sha256') == manifest_ref['sha256']
            and all(package_manifest.get(k) == ready.get(k) == task.get(k) for k in ('task_id','source_id','source_sha256'))
            and all(isinstance(task.get(k), str) and task[k] for k in ('task_id','source_id','source_sha256'))
            and task.get('source_sha256') == body_input['source']['sha256'], 'body package seal or task mismatch')
        members, relative_names, ordered_names = {}, set(), []
        publications = package_manifest.get('publication_files')
        for entry in package_manifest['files']:
            relative = entry['path']
            _require(isinstance(relative, str) and relative and '\\' not in relative and ':' not in relative
                and not PurePosixPath(relative).is_absolute() and all(p not in ('', '.', '..') for p in relative.split('/'))
                and relative not in ('manifest.json','READY.json'), 'unsafe package member path')
            name = package.joinpath(*PurePosixPath(relative).parts); key = _key(str(name))
            _require(key not in members and type(entry.get('bytes')) is int, 'duplicate or incomplete package member')
            artifact(name)
            _require(artifact_index[key]['sha256'] == entry['sha256'], 'package member hash mismatch')
            check.read(dict(entry, path=str(name)))
            members[key] = check.rows[key]
            relative_names.add(relative)
            ordered_names.append(relative)
        _require(isinstance(publications, list) and publications and len(publications) == len(set(publications))
            and all(isinstance(p, str) and p in relative_names for p in publications),
            'publication file coverage missing')
        body_publications = [copy.deepcopy(members[_key(str(package / p))]) for p in publications]
        body_package = []
        for relative in [*ordered_names, 'manifest.json', 'READY.json']:
            name = package.joinpath(*PurePosixPath(relative).parts)
            row = check.rows[_key(str(name))]
            body_package.append(dict(relative_path=relative, **{
                field: row[field] for field in ('path', 'linux_path', 'sha256', 'bytes')}))
        attachment_mappings = []
        for item, delivery in zip(expected_attachments, received):
            ident = item['attachment_id']
            _require(delivery.get('attachment_id') == ident and delivery.get('paper_id') == identity['paper_id'],
                'attachment delivery order or identity mismatch')
            directory = folder / 'attachments' / _hash(ident)
            result = artifact(directory / 'receipt.json', json_value=True)
            _require(result.get('schema') == 'attachment-postprocess-result/1' and result.get('status') == 'complete'
                and result.get('ticket') == identity and result.get('delivery') == delivery
                and result.get('binding', {}).get('ticket') == identity
                and result.get('binding', {}).get('handoff') == item
                and result.get('raw_response') == item['model_refs']['response']
                and result.get('model_requests_added') == 0, 'attachment result input binding mismatch')
            source_ref = dict(path=delivery['source_path'], sha256=delivery['source_sha256'])
            original = original_by_id[ident]
            _require(_key(source_ref['path']) in check.declared
                and _key(source_ref['path']) == _key(original['path']) and source_ref['sha256'] == original['sha256']
                and result['binding'].get('source_path') == source_ref['path']
                and result['binding'].get('source_sha256') == source_ref['sha256'], 'attachment source was not frozen or changed owner')
            artifact(source_ref['path']); check.read(source_ref, input_only=True)
            md_ref = dict(path=delivery['md_path'], sha256=delivery['md_sha256'])
            _require(_win(md_ref['path']).is_relative_to(directory), 'attachment MD escaped its output')
            for ref in result['artifacts'].values():
                _require(_win(ref['path']).is_relative_to(directory), 'attachment artifact escaped its output')
                artifact(ref['path']); check.read(ref)
            _require(any(ref == md_ref for ref in result['artifacts'].values()), 'attachment MD missing from artifact map')
            check.read(md_ref)
            attachment_mappings.append(dict(attachment_id=ident, markdown=copy.deepcopy(check.rows[_key(md_ref['path'])]),
                source=copy.deepcopy(check.rows[_key(source_ref['path'])]),
                reported_privacy_redaction=result.get('privacy_redaction')))
        _require(expected_artifacts == set(artifact_index), 'unexpected or incomplete folder artifact coverage')
        # Recheck bytes/visible path identities after inspecting all dependencies.
        for row in list(check.rows.values()): check.read(row, input_only=row['kind'] == 'input')
        fs.visible()
        return dict(schema='paperflow-linux-artifact-map/1', integrity_status='passed',
            job=copy.deepcopy(job), identity=copy.deepcopy(identity), receipt_canonical_sha256=_hash(receipt),
            files=list(check.rows.values()), body_publications=body_publications, body_package=body_package,
            body_manifest_json=manifest_text, body_ready_json=ready_text,
            attachments=attachment_mappings,
            rag_admission='not_granted', semantic_validation='depends_on_reviewed_business_processor',
            privacy_validation='depends_on_reviewed_business_processor',
            worker_termination='caller_must_verify', original_receipt_rewritten=False)
    except ReceiptInspectionError:
        raise
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError) as exc:
        raise ReceiptInspectionError('incomplete, unsafe or changed receipt evidence: ' + str(exc)) from exc
    finally:
        if fs is not None: fs.close()
