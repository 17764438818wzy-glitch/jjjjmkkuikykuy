"""Read-only dependency closure for ONE already admitted ``build_inputs`` result.

The caller supplies narrowly trusted allowed_roots on the original Windows host.
No discovery, globbing, response-path traversal, JSON rewriting or model calls.
Original path strings, identities and file bytes remain authoritative. A complete
result means this supported dependency graph was collected, not that publication,
registration, LLM semantics, transfer or remote execution succeeded.

Supported: H2.1 paperflow-receipt/1 and group-handoff/2, group-delivery/1,
preextract-material/1.0, frozen single/partition file descriptors /1 and request
image checkpoints /1, simple named-delivery-input/1, attachment-evidence-handoff/1
with current_read provenance. Historical provenance/request records, legacy
inventory evidence, generated/override title proof chains and future schemas are
intentionally blocked: their dependency closure has not been reviewed.
Current origin aliases are collected only under allowed_roots and the registered
handoff's exact byte hash; their entry_ref names the upstream source file_id.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from windows_snapshot import _windows_name
from source_handle import check_opened_source

IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
            'parent_run_id', 'task_run_id')
SHA = re.compile(r'[a-f0-9]{64}')


class CollectionError(ValueError):
    pass


def require(ok, reason):
    if not ok:
        raise CollectionError(reason)


def digest(value, *, sort=True):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=sort,
        separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()


class _Collector:
    def __init__(self, roots, *, defer_binary_hash=False, max_json_file_bytes=None,
                 max_total_json_bytes=None, trusted_catalog_path=None):
        require(isinstance(roots, (list, tuple)) and roots, 'allowed roots required')
        self.roots = []
        for root in roots:
            path = Path(root)
            require(path.is_absolute(), 'allowed root must be absolute')
            self.no_links(path)
            require(path.is_dir(), 'allowed root is missing')
            self.roots.append(path.resolve(strict=True))
        self.files = {}
        self.defer_binary_hash = defer_binary_hash
        self.max_json_file_bytes = max_json_file_bytes
        self.max_total_json_bytes = max_total_json_bytes
        self.json_sizes = {}
        self.catalog_path = None
        self.catalog_evidence = None
        if trusted_catalog_path is not None:
            path = Path(trusted_catalog_path)
            require(path.is_absolute() and '..' not in path.parts,
                    'trusted catalog must be an exact absolute file path')
            self.catalog_path = path

    @staticmethod
    def no_links(path):
        for item in (path, *path.parents):
            info = item.lstat()
            require(not stat.S_ISLNK(info.st_mode)
                and not getattr(info, 'st_file_attributes', 0) & 0x400,
                'link or reparse point in evidence path')

    def path(self, name, *, catalog_file=False):
        require(isinstance(name, str) and name, 'invalid evidence path')
        if os.name == 'nt':
            _windows_name(name)
        path = Path(name)
        require(path.is_absolute() and '..' not in path.parts, 'invalid absolute evidence path')
        # Check lexical containment BEFORE stat/open/resolve of an untrusted path.
        exact = catalog_file and self.catalog_path is not None and path == self.catalog_path
        require(exact or any(path.is_relative_to(root) for root in self.roots),
                'evidence path outside allowed roots')
        self.no_links(path)
        real = path.resolve(strict=True)
        require((exact and real == self.catalog_path)
                or any(real.is_relative_to(root) for root in self.roots),
                'evidence resolved outside allowed roots')
        return path

    def read(self, ref, role, *, json_value=False, catalog_file=False):
        require(isinstance(ref, dict) and isinstance(ref.get('sha256'), str)
                and SHA.fullmatch(ref['sha256']), 'missing evidence hash reference')
        path = self.path(ref.get('path'), catalog_file=catalog_file)
        info = path.stat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                'nonregular or hard-linked evidence file')
        if json_value:
            key = str(path).lower() if os.name == 'nt' else str(path)
            if self.max_json_file_bytes is not None:
                require(info.st_size <= self.max_json_file_bytes, 'metadata file exceeds host memory budget')
            if key not in self.json_sizes:
                total = sum(self.json_sizes.values()) + info.st_size
                if self.max_total_json_bytes is not None:
                    require(total <= self.max_total_json_bytes, 'metadata total exceeds host memory budget')
                self.json_sizes[key] = info.st_size
        with path.open('rb') as stream:
            opened = os.fstat(stream.fileno())
            require((info.st_dev, info.st_ino) == (opened.st_dev, opened.st_ino) and opened.st_nlink == 1,
                    'evidence changed on opening')
            check_opened_source(stream.fileno(), path,
                [*self.roots, self.catalog_path] if catalog_file else self.roots)
            hasher, size, chunks = hashlib.sha256(), 0, []
            if json_value or catalog_file or not self.defer_binary_hash:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    hasher.update(chunk)
                    size += len(chunk)
                    if json_value: chunks.append(chunk)
            else:
                # The authenticated server must hash the bytes before launch.
                # Do not load/hash the large PDF or attachment on the host.
                size = opened.st_size
            after = os.fstat(stream.fileno())
        require((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) and after.st_nlink == 1,
                'evidence changed while reading')
        if json_value or catalog_file or not self.defer_binary_hash:
            require(hasher.hexdigest() == ref['sha256'], 'evidence bytes changed')
        if 'bytes' in ref:
            require(type(ref['bytes']) is int and ref['bytes'] == size, 'evidence size changed')
        key = str(path).lower() if os.name == 'nt' else str(path)
        entry = self.files.get(key)
        if entry:
            require(entry['sha256'] == ref['sha256'], 'conflicting evidence hash')
            if ref['path'] not in entry['names']: entry['names'].append(ref['path'])
            if role not in entry['roles']: entry['roles'].append(role)
        else:
            self.files[key] = dict(path=ref['path'], sha256=ref['sha256'], bytes=size,
                names=[ref['path']], roles=[role])
        if json_value:
            value = json.loads(b''.join(chunks))
            require(isinstance(value, dict), 'evidence JSON must be an object')
            return value
        return None

    def image(self, asset, role):
        if asset.get('image_path'):
            self.read(dict(path=asset['image_path'], sha256=asset.get('sha256')), role)
        if asset.get('original_image_path'):
            self.read(dict(path=asset['original_image_path'], sha256=asset.get('original_sha256')), role + '.original')

    def material(self, bundle, role, *, projection=False):
        # The real material_view deliberately removes version and local paths.
        # Full synthetic/older requests retain the complete material instead.
        stripped = projection and 'version' not in bundle
        require(stripped or bundle.get('version') == 'preextract-material/1.0', 'unsupported material version')
        source = bundle['source']
        if not stripped or source.get('file') is not None:
            self.read(dict(path=source.get('file'), sha256=source.get('source_sha256')), role + '.source')
        for field in ('text', 'index'):
            # material_view keeps these digests while deliberately removing
            # local paths. The saved request and frozen plan still bind those
            # bytes; a pathless projection digest is not another file to open.
            if (source.get(field + '_file') is not None
                    or (not stripped and source.get(field + '_sha256') is not None)):
                self.read(dict(path=source.get(field + '_file'), sha256=source.get(field + '_sha256')),
                          role + '.' + field)
        if role == 'body.material' and self.catalog_path is not None:
            # Only the caller's configured file can acquire this exception.
            # The material's own path declarations never expand roots.
            hashes = bundle.get('file_hashes') or {}
            matches = [(p, h) for p, h in hashes.items() if Path(p) == self.catalog_path]
            expected = source.get('catalog_sha256')
            require(isinstance(expected, str) and SHA.fullmatch(expected)
                    and len(matches) == 1 and matches[0][1] == expected,
                    'material does not prove the trusted catalog binding')
            name, sha = matches[0]
            value = self.read(dict(path=name, sha256=sha), role + '.catalog',
                              json_value=True, catalog_file=True)
            self.catalog_evidence = dict(path=name, sha256=sha, value=value)
        for path, sha in bundle.get('file_hashes', {}).items():
            exact = self.catalog_path is not None and Path(path) == self.catalog_path
            if exact:
                require(self.catalog_evidence is not None
                    and source.get('catalog_sha256') == sha == self.catalog_evidence['sha256'],
                    'material catalog binding changed')
            self.read(dict(path=path, sha256=sha), role + '.file_hashes', catalog_file=exact)
        for asset in bundle.get('assets', []): self.image(asset, role + '.asset')
        for attachment in bundle.get('attachments', []):
            if stripped:
                require(not any(attachment.get(k) for k in ('path', 'file', 'retained_file_hashes')),
                        'unsupported local attachment in request projection')
                continue
            hashes = attachment.get('retained_file_hashes')
            require(isinstance(hashes, dict) and hashes,
                    'unsupported embedded attachment without retained hashes')
            for path, sha in hashes.items():
                self.read(dict(path=path, sha256=sha), role + '.embedded_attachment')

    def request(self, request, role, *, body):
        messages = request.get('messages')
        require(isinstance(messages, list) and len(messages) >= 2, 'unsupported request layout')
        content = messages[1].get('content')
        require(isinstance(content, list) and content, 'unsupported request content')
        if body:
            require(content[0].get('type') == 'text', 'unsupported body request projection')
            projection = json.loads(content[0]['text'])
            require(isinstance(projection, dict) and isinstance(projection.get('material'), dict),
                    'unsupported body request projection')
            # The JSON embedded in text is request evidence, never a model answer.
            self.material(projection['material'], role + '.projection', projection=True)
        else:
            require(content[0].get('type') == 'text', 'unsupported attachment request projection')
            projection = json.loads(content[0]['text'])
            require(isinstance(projection, dict) and projection.get('schema') in
                    ('attachment-model-input/1', 'attachment-evidence/1'),
                    'unsupported attachment request projection')
            if projection['schema'] == 'attachment-evidence/1':
                for image in projection.get('images', []): self.read(image, role + '.projection.image')
        for part in content:
            if part.get('type') == 'image_url':
                require(str(part.get('image_url', {}).get('url', '')).startswith('data:image/'),
                        'unsupported external request image URL')
            else:
                require(part.get('type') == 'text', 'unsupported request content item')

    def response(self, refs, role, *, body):
        request = self.read(refs['request'], role + '.request', json_value=True)
        state = self.read(refs['state'], role + '.state', json_value=True)
        self.read(refs['events'], role + '.events')
        self.read(refs['response'], role + '.response')  # Never parse answer paths.
        directory = Path(refs['request']['path']).parent
        for key, filename in [('request', 'request.json'), ('state', 'request-state.json'),
                              ('events', 'response-events.jsonl'), ('response', 'response.txt')]:
            require(Path(refs[key]['path']) == directory / filename, 'saved response layout mismatch')
        require(state.get('status') == 'response_complete' and state.get('finish_reason') == 'stop'
                and state.get('request_sha256') == digest(request, sort=False)
                and state.get('response_sha256') == refs['response']['sha256'],
                'saved response identity incomplete')
        self.request(request, role, body=body)

    def receipt(self, ref, identity):
        receipt = self.read(ref, 'receipt', json_value=True)
        require(receipt.get('schema') == 'paperflow-receipt/1', 'unsupported receipt schema')
        require(all(receipt.get(k) == identity[k] for k in ('group_id', 'paper_id', 'receipt_id')),
                'receipt identity mismatch')
        handoff = self.read(receipt['handoff_ref'], 'receipt.handoff', json_value=True)
        require(handoff.get('schema') == 'paperflow-group-handoff/2', 'unsupported handoff schema')
        for row in [receipt['inventory']['body'], *receipt['inventory']['attachments']]:
            require(not row.get('unverified_evidence'), 'unverified received evidence')
            self.read(row, 'receipt.received_file')
            item_handoff = self.read(row['handoff_ref'], 'receipt.file_handoff', json_value=True)
            require(row['handoff_ref']['sha256'] == receipt['handoff_ref']['sha256']
                    and item_handoff == handoff, 'received file handoff differs from its receipt')
            if row.get('legacy_inventory_refs'):
                raise CollectionError('unsupported legacy inventory evidence')
            versions = []
            for evidence_ref in row.get('evidence_records', []):
                record = self.read(evidence_ref, 'attachment.sidecar', json_value=True)
                require(record.get('schema') == 'attachment-evidence-handoff/1',
                        'unsupported attachment evidence schema')
                require(dict(record['source'], group_id=record['group_id']) == row['source_binding'],
                        'attachment sidecar identity mismatch')
                versions.append(record['evidence_version'])
                self.sidecar(record, handoff=handoff, handoff_ref=row['handoff_ref'], received=row)
            require(not versions or sorted(versions) == list(range(1, max(versions) + 1)),
                    'attachment evidence history incomplete')
        if receipt.get('delivery_ref'):
            self.delivery(receipt['delivery_ref'])
        return receipt

    def sidecar(self, record, *, handoff, handoff_ref, received=None):
        require(not record.get('request_records') and not record.get('existing_identification'),
                'unsupported historical attachment request evidence')
        # Reuse the actual upstream schema/identity contract. verify_files=False
        # performs no source or historical reads: received bytes are checked via
        # the trusted receipt/delivery, not by reopening original paper paths.
        try:
            from paperflow_prefect.attachment_evidence_handoff import validate_evidence_handoff
        except ImportError as exc:
            raise CollectionError('shared attachment evidence validator is unavailable') from exc
        validate_evidence_handoff(record, handoff, verify_files=False)
        source = record['source']
        matches = [row for row in handoff['attachments'] if row['file_id'] == source['file_id']]
        require(len(matches) == 1, 'current attachment has no unique upstream handoff entry')
        original = matches[0]
        if received is not None:
            require(received.get('upstream_file_id') == source['file_id']
                    and received.get('source_binding') == dict(source, group_id=record['group_id'])
                    and received.get('sha256') == source['source_sha256']
                    and received.get('bytes') == original['bytes'],
                    'current attachment does not match registered upstream source')
            name = received.get('source_path') if received.get('delivery_ref') else received.get('path')
            require(isinstance(name, str)
                    and os.path.normcase(os.path.normpath(name))
                    == os.path.normcase(os.path.normpath(original['path'])),
                    'current attachment original source path changed')
        listing = record['archive_listing']
        items = [*record['text_samples'], *record['images']]
        if listing.get('provenance') is not None:
            items.append(listing)
        else:
            require(listing['status'] in {'not_archive', 'not_read', 'unavailable'},
                    'available attachment listing lacks provenance')
        if record['decision_basis'] is not None:
            items.append(record['decision_basis'])
        for item in items:
            provenance = item.get('provenance')
            require(isinstance(provenance, dict) and provenance.get('kind') == 'current_read',
                    'unsupported historical attachment provenance')
            require(provenance.get('run_ref') is None, 'unsupported current attachment run reference')
            # entry_ref names the original handoff row, NOT the subsequently
            # registered attachment/file_id. Current classification records may
            # have no digest themselves; the receipt independently pins it.
            require(provenance.get('entry_ref') in (None, source['file_id']),
                    'current attachment provenance entry mismatch')
            reference, sha = provenance.get('record_ref'), provenance.get('record_sha256')
            if reference is None:
                require(sha is None, 'current attachment digest has no source reference')
                continue
            require(provenance.get('entry_ref') == source['file_id']
                    and sha in (None, handoff_ref['sha256']),
                    'current attachment provenance source or hash mismatch')
            current = self.read(dict(path=reference, sha256=handoff_ref['sha256']),
                                'attachment.sidecar.current_handoff', json_value=True)
            require(current == handoff, 'current provenance is not the registered original handoff')
        for image in record['images']:
            self.read(image, 'attachment.sidecar.image')

    def delivery(self, ref):
        value = self.read(ref, 'receipt.delivery', json_value=True)
        require(value.get('schema') == 'paperflow-group-delivery/1', 'unsupported delivery schema')
        require(value.get('delivery_id') == digest({k:v for k,v in value.items() if k != 'delivery_id'}),
                'delivery identity mismatch')
        root = Path(ref['path']).parent
        handoff_ref = dict(path=str(root / 'handoff.json'), sha256=value['handoff_sha256'])
        handoff = self.read(handoff_ref, 'delivery.handoff', json_value=True)
        for row in value['files']:
            require(row.get('copied') is True, 'delivery file incomplete')
            relative = Path(row['relative'])
            require(not relative.is_absolute() and not relative.drive and '..' not in relative.parts,
                    'delivery path outside root')
            self.read(dict(row, path=str(root / 'files' / relative)), 'delivery.file')
        for row in value['evidence_records']:
            relative = Path(row['path'])
            require(not relative.is_absolute() and not relative.drive and '..' not in relative.parts,
                    'delivery evidence outside root')
            record = self.read(dict(row, path=str(root / relative)), 'delivery.sidecar', json_value=True)
            require(record.get('schema') == 'attachment-evidence-handoff/1', 'unsupported delivery sidecar')
            self.sidecar(record, handoff=handoff, handoff_ref=handoff_ref)

    def plan(self, descriptor, identity):
        require(descriptor.get('schema') == 'body-frozen-plan-files/1', 'unsupported frozen plan descriptor')
        mode = identity.get('mode')
        require(mode == descriptor.get('mode') and mode in ('single', 'partitioned'), 'unsupported frozen plan mode')
        work, refs = self.path(descriptor['work']), descriptor['files']
        if mode == 'single':
            expected = {'single': 'private/llm-frozen-single-plan.json'}
        else:
            expected = dict(metadata='private/llm-frozen-plan-metadata.json',
                partition_plan='model/partition-plan.json', partition_state='model/partition-state.json',
                partition_material='model/partition-material.json')
            for index in range(1, identity['part_count'] + 1):
                ident = f'part-{index:03d}'
                expected[ident + '.request'] = f'model/{ident}/planned-request.json'
                expected[ident + '.material'] = f'model/{ident}/material.json'
        require(set(refs) == set(expected), 'frozen plan files incomplete')
        values = {}
        for key, relative in expected.items():
            require(Path(refs[key]['path']) == work / relative, 'frozen plan path ownership mismatch')
            values[key] = self.read(refs[key], 'body.plan.' + key, json_value=True)
        if mode == 'single':
            saved = values['single']
            require(saved.get('schema') == 'paperflow-raw-frozen-single-plan/1', 'unsupported frozen single schema')
            plan = saved['plan']
            require(saved.get('plan_sha256') == digest(plan), 'frozen single plan changed')
            parts = plan['parts']
        else:
            metadata = values['metadata']
            require(metadata.get('schema') == 'paperflow-raw-frozen-plan-metadata/1'
                and values['partition_plan'].get('schema') == 'preextract-partition-run/1'
                and values['partition_state'].get('schema') == 'preextract-partition-run/1',
                'unsupported frozen partition schema')
            frozen, state = values['partition_plan'], values['partition_state']
            require(state.get('plan_sha256') == refs['partition_plan']['sha256']
                and frozen.get('material_sha256') == refs['partition_material']['sha256'],
                'frozen partition hash mismatch')
            self.material(values['partition_material'], 'body.plan.partition_material')
            parts = []
            require(len(frozen['parts']) == len(metadata['part_extras']) == identity['part_count'],
                    'frozen partition count mismatch')
            require(len(state.get('requests', [])) == identity['part_count'], 'partition request state incomplete')
            for row, extra, request_row in zip(frozen['parts'], metadata['part_extras'], state['requests']):
                ident = row['id']
                require(extra['id'] == request_row['id'] == ident
                    and row['material_sha256'] == refs[ident + '.material']['sha256']
                    and row['request_sha256'] == request_row['request_sha256'] == digest(values[ident + '.request'], sort=False),
                    'partition request identity mismatch')
                parts.append(dict(extra['extra'], id=ident, body=values[ident + '.request'],
                    bundle=values[ident + '.material'], ranges=row['ranges'], asset_ids=row['asset_ids'], capacity=row['capacity']))
            plan = dict(metadata['top'], parts=parts)
        require(digest(plan) == identity['plan_sha256'], 'frozen plan identity mismatch')
        require(len(parts) == identity['part_count'], 'frozen plan count mismatch')
        for part in parts:
            self.material(part['bundle'], 'body.plan.material')
            self.request(part['body'], 'body.plan.request', body=True)
        return plan


def collect_inputs(inputs, *, allowed_roots, defer_binary_hash=False,
                   max_json_file_bytes=None, max_total_json_bytes=None,
                   trusted_catalog_path=None):
    """Return a verified file inventory; raises CollectionError on incomplete closure.

    ``inputs`` MUST come from upstream ``build_inputs`` and not a model answer.
    allowed_roots are an independent caller trust boundary, not extracted from
    inputs. Each file has path (original string), sha256, bytes, names and roles.
    trusted_catalog_path must come from independently trusted backend settings,
    never from inputs. It grants only this file after the original material's
    catalog SHA and file_hashes entry agree; its bytes are always hashed here.
    No filesystem writes; the caller may bind these files to EvidenceMap later.
    """
    try:
        require(type(defer_binary_hash) is bool, 'invalid binary hash policy')
        require(all(value is None or (type(value) is int and value > 0)
                    for value in (max_json_file_bytes, max_total_json_bytes)), 'invalid metadata memory budget')
        require(isinstance(inputs, dict), 'invalid inputs')
        identity = inputs['identity']
        require(isinstance(identity, dict) and set(identity) == set(IDENTITY)
            and all(isinstance(v, str) and v for v in identity.values()), 'incomplete folder identity')
        require(isinstance(inputs['manifest_sha256'], str) and SHA.fullmatch(inputs['manifest_sha256']),
                'missing original manifest hash')
        collector = _Collector(allowed_roots, defer_binary_hash=defer_binary_hash,
                               max_json_file_bytes=max_json_file_bytes, max_total_json_bytes=max_total_json_bytes,
                               trusted_catalog_path=trusted_catalog_path)
        body = inputs['body']
        require(body.get('schema') == 'body-postprocess-input/1' and body['identity'] == identity,
                'body identity mismatch')
        receipt = collector.receipt(body['receipt'], identity)
        collector.read(body['source'], 'body.source')
        materials = []
        for key in ('material', 'reading_material'):
            value = collector.read(body[key], 'body.' + key, json_value=True)
            collector.material(value, 'body.' + key)
            require(value['source']['source_sha256'] == body['source']['sha256'],
                    'body material source identity mismatch')
            materials.append(value)
        needs_named = any(Path(v['source']['file']) != Path(body['source']['path']) for v in materials)
        require(not needs_named or isinstance(body.get('named_source'), dict), 'missing named source proof')
        plan = collector.plan(body['plan'], body['plan_identity'])
        responses = body['responses']
        require(len(responses) == len(plan['parts']), 'missing body response')
        require(len(body['plan_identity']['parts']) == len(plan['parts']), 'missing frozen part identity')
        for index, (row, part) in enumerate(zip(responses, plan['parts']), 1):
            # Whole-body raw plans need not duplicate their single part ID.
            # Bind the response to the ORIGINAL receipt's canonical ordinal;
            # never insert an ID into the raw plan or change its sealed hash.
            ident = f'part-{index:03d}'
            raw_ident = part.get('id', ident) if body['plan_identity']['mode'] == 'single' else part.get('id')
            require(body['plan_identity']['parts'][index - 1].get('id') == ident
                    and raw_ident == ident and row['identity'] == identity and row['part_id'] == ident,
                    'body response identity mismatch')
            collector.response(row, 'body.' + row['part_id'], body=True)
            request = collector.read(row['request'], 'body.plan.request_binding', json_value=True)
            require(request == part['body'], 'request differs from frozen plan')
            images_work = collector.path(row['images_work'])
            checkpoint_path = images_work / 'private/request-images.json'
            if checkpoint_path.exists():
                require(isinstance(row.get('request_images'), dict)
                    and Path(row['request_images']['path']) == checkpoint_path,
                    'unbound image checkpoint')
            else:
                require(row.get('request_images') is None, 'missing image checkpoint')
            if row.get('request_images'):
                checkpoint = collector.read(row['request_images'], 'body.request_images', json_value=True)
                require(checkpoint.get('schema') == 'preextract-request-images/1', 'unsupported request images schema')
                assets = [a for a in part['bundle'].get('assets', []) if a.get('image_path')]
                require(checkpoint.get('material_sha256') == digest(part['bundle'])
                    and [i.get('id') for i in checkpoint['images']] == [a['id'] for a in assets],
                    'image checkpoint material identity mismatch')
                for image, asset in zip(checkpoint['images'], assets):
                    binding = image['binding']
                    require(isinstance(binding, dict) and binding.get('image_path')
                        and binding.get('sha256') and binding.get('width') and binding.get('height')
                        and image.get('source_sha256') == asset['sha256']
                        and image.get('source_width') == asset.get('width')
                        and image.get('source_height') == asset.get('height'), 'image checkpoint binding incomplete')
                    if image.get('adjusted'):
                        require(Path(binding['image_path']).is_relative_to(images_work / 'private/request-images'),
                                'image checkpoint rendition outside owner')
                    collector.image(binding, 'body.sent_image')
        if body.get('named_source'):
            named = body['named_source']
            work = collector.path(named['work'])
            require(work == Path(body['plan']['work']) and Path(named['proof']['path']) == work / 'named-input-proof.json',
                    'named source proof ownership mismatch')
            proof = collector.read(named['proof'], 'body.named_source', json_value=True)
            require(proof.get('schema') == 'named-delivery-input/1', 'unsupported named source proof')
            require(proof.get('paper_id') == identity['paper_id'] and proof.get('kind') == 'body'
                and proof.get('file_id') == receipt['inventory']['body']['file_id']
                and proof.get('source_path') == body['source']['path']
                and proof.get('source_sha256') == body['source']['sha256'], 'named source binding mismatch')
            target = Path(proof['delivery_path'])
            require(target.parent == work / 'named-input' and target.name == proof['display_name']
                and all(Path(v['source']['file']) == target for v in materials)
                and materials[0].get('file_hashes', {}).get(named['proof']['path']) == named['proof']['sha256'],
                'named source material proof mismatch')
            require(not proof.get('generated_title_sha256') and not proof.get('title_override_sha256'),
                    'unsupported generated or override title evidence closure')
            collector.read(dict(path=proof['delivery_path'], sha256=proof['source_sha256'], bytes=proof['bytes']),
                           'body.named_copy')
        attachments = inputs['attachments']
        expected = {row['file_id']: row for row in receipt['inventory']['attachments']}
        require(len(attachments) == len(expected) and {a['attachment_id'] for a in attachments} == set(expected),
                'attachment set incomplete')
        for attachment in attachments:
            require(attachment.get('schema') == 'attachment-postprocess-input/1'
                and attachment['ticket'] == identity and attachment['receipt_ref'] == body['receipt'],
                'attachment identity mismatch')
            refs = attachment['model_refs']
            evidence = collector.read(refs['evidence'], 'attachment.evidence', json_value=True)
            require(evidence.get('schema') == 'attachment-evidence/1'
                and evidence.get('model_input_version') in (None, 1), 'unsupported attachment model evidence')
            require(evidence['source_sha256'] == expected[attachment['attachment_id']]['sha256'],
                    'attachment evidence source mismatch')
            for image in evidence.get('images', []): collector.read(image, 'attachment.image')
            collector.response(dict(request=refs['request'], state=refs['request_state'],
                events=refs['response_events'], response=refs['response']), 'attachment.model', body=False)
        # Recheck every file after traversal to reject a mixed, mutable snapshot.
        for entry in list(collector.files.values()):
            exact = (collector.catalog_evidence is not None
                     and Path(entry['path']) == collector.catalog_path)
            collector.read(entry, 'final_recheck', catalog_file=exact)
        result = dict(schema='postprocess-input-dependencies/1', status='complete',
            identity=copy.deepcopy(identity), manifest_sha256=inputs['manifest_sha256'],
            inputs=copy.deepcopy(inputs), files=list(collector.files.values()),
            model_requests_added=0, evidence_scope=('supported_dependency_graph_binary_hash_deferred'
                if defer_binary_hash else 'supported_dependency_closure_only'))
        if collector.catalog_evidence is not None:
            result['catalog_evidence'] = copy.deepcopy(collector.catalog_evidence)
        return result
    except CollectionError:
        raise
    except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
        raise CollectionError('missing, invalid or unsupported evidence: ' + str(exc)) from exc
