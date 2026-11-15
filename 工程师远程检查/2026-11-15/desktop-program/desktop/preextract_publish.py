"""Deterministic publication of a validated paper-preextract candidate.

No model calls, scheduler startup, credential access, or network at import time.
Private source data and execution plans never enter the publication directory.
"""
from __future__ import annotations

import copy
import difflib
import hashlib
import importlib
import json
import re
import sys
import uuid
from pathlib import Path


PROFILE_FIELDS = {'material_type', 'material_disposition', 'article', 'education_level',
    'applicable_level', 'research_profile', 'learning_units', 'migration_boundary',
    'routing_candidates', 'additional_related_specialties'}
REF_FIELDS = {'file_ref', 'block_id', 'section_id', 'asset_id', 'start', 'end',
              'source_kind', 'locator_hint'}
ASSET_FIELDS = {'id', 'kind', 'caption', 'source_image_id', 'section_ids', 'unit_ids',
    'source_refs', 'meaning', 'purpose', 'materials_needed', 'organization', 'storage',
    'storage_reason', 'evidence_basis', 'limits'}
PUBLICATION_VERSION = 'preextract-publication/1.6'


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf8')
    temporary.replace(path)


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _paperflow(root):
    from project_paths import resolve_core
    root = resolve_core(root)
    _require((root / 'paperflow' / 'package.py').is_file(), 'paperflow package unavailable')
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    package = importlib.import_module('paperflow.package')
    _require(Path(package.__file__).resolve().parent.parent == root, 'paperflow import root conflict')
    return package


def _pointer(root, pointer):
    _require(isinstance(pointer, str) and pointer.startswith('/'), 'redaction pointer invalid')
    parts = [x.replace('~1', '/').replace('~0', '~') for x in pointer[1:].split('/')]
    _require(parts[0] in {'profile', 'tree', 'assets', 'gaps', 'attachments'}, 'redaction pointer is private or unknown')
    parent = root
    try:
        for part in parts[:-1]:
            parent = parent[int(part)] if isinstance(parent, list) else parent[part]
        key = int(parts[-1]) if isinstance(parent, list) else parts[-1]
        value = parent[key]
    except (KeyError, IndexError, TypeError, ValueError):
        raise ValueError('redaction pointer cannot be located') from None
    _require(isinstance(value, str), 'redaction pointer must select text')
    return parent, key, value


def _text_edits(candidate, blocks, metadata=(), *, plan=None, best_effort=False):
    """Validate every coordinate against immutable text, then apply backwards."""
    by_id = {b['id']: b for b in blocks}
    groups = {}
    entities = {}
    images = []
    audit = []
    for row in candidate.get('redaction_plan', []) if plan is None else plan:
        _require(isinstance(row, dict) and row.get('location_status') == 'located', 'redaction location unresolved')
        action, scope = row.get('action'), row.get('scope')
        _require(action in {'replace', 'delete', 'mask'}, 'redaction action unknown')
        replacement = '' if action == 'delete' else row.get('replacement')
        entity = row.get('entity_key')
        if action != 'mask':
            _require(isinstance(replacement, str), 'redaction replacement missing')
            if entity and action == 'replace' and not best_effort:
                _require(entity not in entities or entities[entity] == replacement, 'redaction entity replacement conflict')
                entities[entity] = replacement
        if scope == 'image':
            _require(action == 'mask', 'image redaction requires mask')
            images.append(row)
            continue
        _require(action in {'replace', 'delete'}, 'text redaction action invalid')
        expected = row.get('expected_text')
        _require(isinstance(expected, str) and expected, 'redaction expected text missing')
        if scope == 'source_text':
            refs = row.get('source_refs')
            _require(isinstance(refs, list) and bool(refs), 'redaction source references missing')
            for ref in refs:
                bid, start, end = ref.get('block_id'), ref.get('start'), ref.get('end')
                _require(bid in by_id and type(start) is int and type(end) is int,
                         'redaction source range unknown')
                text = by_id[bid]['text']
                _require(0 <= start < end <= len(text) and text[start:end] == expected,
                         'redaction source range mismatch')
                groups.setdefault(('block', bid), []).append((start, end, expected, replacement))
        elif scope in {'generated_field', 'metadata', 'filename'}:
            pointer = row.get('target_pointer')
            selector = row.get('metadata_selector')
            if scope == 'metadata':
                _require(isinstance(selector, dict) and set(selector) == {'file_ref', 'part', 'selector', 'attribute'},
                         'redaction metadata selector invalid')
                matches = [item for item in metadata if item.get('selector') == selector]
                _require(len(matches) == 1 and isinstance(matches[0].get('value'), str)
                         and matches[0]['value'].count(expected) == 1, 'redaction metadata target missing or ambiguous')
                # The white-listed JSON publication has no original Office/PDF
                # metadata. Verify the supplied target, then record its omission.
                audit.append(dict(id=row.get('id'), scope=scope, status='private_metadata_omitted'))
                continue
            # Public output filenames are fixed program names; an explicitly
            # selected original filename is omitted by construction.
            if scope == 'filename' and not pointer and Path(str(candidate.get('source', {}).get('file', ''))).name.count(expected) == 1:
                audit.append(dict(id=row.get('id'), scope=scope, status='private_filename_omitted'))
                continue
            parent, key, text = _pointer(candidate, pointer)
            _require(text.count(expected) == 1, 'redaction generated text is missing or ambiguous')
            start = text.index(expected)
            groups.setdefault(('pointer', pointer), []).append((start, start + len(expected), expected, replacement))
        else:
            raise ValueError('redaction scope unknown')
        audit.append(dict(id=row.get('id'), scope=scope, status='applied'))
    mapping = []
    for (kind, target), edits in groups.items():
        edits = sorted(set(edits))
        _require(all(a[1] <= b[0] for a, b in zip(edits, edits[1:])), 'redaction overlapping text ranges')
        if kind == 'block':
            parent, key, text = by_id[target], 'text', by_id[target]['text']
        else:
            parent, key, text = _pointer(candidate, target)
        for start, end, expected, replacement in reversed(edits):
            _require(text[start:end] == expected, 'redaction text changed during execution')
            text = text[:start] + replacement + text[end:]
        parent[key] = text
        if kind == 'block':
            delta = 0
            for start, end, _, replacement in edits:
                mapping.append(dict(block_id=target, original_start=start, original_end=end,
                    public_start=start + delta, public_end=start + delta + len(replacement)))
                delta += len(replacement) - (end - start)
    return images, mapping, audit


def _frontmatter_edits(blocks, originals, mapping):
    """Reuse the existing bounded identity rules on the publication copy.

    Compose coordinates from the immutable source to the final text. Model
    offsets were already executed against the original, before these rules.
    """
    from frontmatter_privacy import redact_frontmatter_blocks
    before = {row['id']: row['text'] for row in blocks}
    meta = {'blocks': blocks}
    redact_frontmatter_blocks(meta)
    blocks[:] = meta['blocks']
    changed = {row['id'] for row in blocks if before[row['id']] != row['text']}
    original_text = {row['id']: row['text'] for row in originals}
    result = [row for row in mapping if row['block_id'] not in changed]
    for row in blocks:
        if row['id'] not in changed:
            continue
        for tag, a, b, x, y in difflib.SequenceMatcher(
                None, original_text[row['id']], row['text'], autojunk=False).get_opcodes():
            if tag != 'equal':
                result.append(dict(block_id=row['id'], original_start=a, original_end=b,
                                   public_start=x, public_end=y))
    return result, len(changed)


def _public_refs(value, source, file_refs):
    """Keep source coordinates; replace private file bindings with stable IDs."""
    if isinstance(value, list):
        return [_public_refs(v, source, file_refs) for v in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key == 'source_refs' and isinstance(item, list):
            refs = []
            for raw in item:
                _require(isinstance(raw, dict), 'public source reference invalid')
                ref = {k: copy.deepcopy(v) for k, v in raw.items() if k in REF_FIELDS}
                binding = ref.get('file_ref')
                if binding in (None, 'main', source.get('source_id'), source.get('file'), source.get('text_file')):
                    ref['file_ref'] = 'main'
                elif binding in file_refs:
                    ref['file_ref'] = file_refs[binding]
                else:
                    _require(not re.search(r'(^[A-Za-z]:[\\/]|^[/\\])', str(binding)), 'private file reference in public data')
                refs.append(ref)
            result[key] = refs
        else:
            result[key] = _public_refs(item, source, file_refs)
    return result


def _mask_images(candidate, bundle, rows, directory, *, preserve_extras=False):
    from PIL import Image, ImageDraw
    from preextract_images import fit_image
    sources = {a['id']: a for a in bundle.get('assets', [])}
    aliases = {a['id']: a.get('source_image_id') for a in candidate.get('assets', {}).get('items', [])}
    masks = {}
    for row in rows:
        binding = row.get('image_ref')
        _require(isinstance(binding, dict), 'image redaction binding missing')
        aid = binding.get('asset_id') or binding.get('id') or binding.get('source_image_id')
        if aid not in sources and aliases.get(aid) in sources:
            aid = aliases[aid]
        _require(aid in sources, 'image redaction source unknown')
        original = sources[aid]
        digest = binding.get('rendition_sha256') or binding.get('sha256')
        _require(digest == original.get('sha256') and all(binding.get(k) == original.get(k) for k in ('width', 'height')),
                 'image redaction version or dimensions changed')
        rectangles = row.get('redactions')
        _require(isinstance(rectangles, list) and bool(rectangles), 'image redaction rectangles missing')
        for rect in rectangles:
            if all(k in rect for k in ('x0', 'y0', 'x1', 'y1')):
                box = [rect[k] for k in ('x0', 'y0', 'x1', 'y1')]
            elif 'bbox' in rect:
                box = rect['bbox']
            else:
                try:
                    box = [rect['x'], rect['y'], rect['x'] + rect['width'], rect['y'] + rect['height']]
                except (KeyError, TypeError):
                    raise ValueError('image redaction rectangle invalid') from None
            _require(isinstance(box, (list, tuple)) and len(box) == 4 and all(type(v) is int for v in box),
                     'image redaction rectangle must use integer pixels')
            x0, y0, x1, y1 = box
            _require(0 <= x0 < x1 <= original['width'] and 0 <= y0 < y1 <= original['height'],
                     'image redaction rectangle outside image')
            masks.setdefault(aid, []).append(box)
    published = []
    for item in candidate.get('assets', {}).get('items', []):
        asset = {k: copy.deepcopy(v) for k, v in item.items() if preserve_extras or k in ASSET_FIELDS}
        mode = asset.get('storage')
        _require(mode in {'image', 'description', 'omit'}, 'asset storage decision unresolved')
        asset['section_id'] = next(iter(asset.get('section_ids') or []), None)
        asset['description'] = _readable({k: asset[k] for k in
            ('meaning', 'purpose', 'materials_needed', 'organization', 'storage_reason', 'limits') if k in asset})
        if mode == 'description':
            _require(bool(asset['description'].strip()), 'asset description missing')
        if mode == 'image':
            aid = asset.get('source_image_id') or asset['id']
            _require(aid in sources, 'retained image source missing')
            source = sources[aid]
            path = Path(source.get('image_path') or '')
            _require(path.is_file(), 'retained image file missing')
            blob = path.read_bytes()
            _require(_sha(blob) == source.get('sha256'), 'retained image hash changed')
            with Image.open(path) as im:
                _require(im.size == (source.get('width'), source.get('height')), 'retained image size changed')
                # Composite transparency on white; direct RGB conversion can
                # turn transparent diagram backgrounds black. Coordinates still
                # address this exact model-visible version before any resize.
                rgba = im.convert('RGBA')
                picture = Image.new('RGB', im.size, 'white')
                picture.paste(rgba, mask=rgba.getchannel('A'))
            draw = ImageDraw.Draw(picture)
            for x0, y0, x1, y1 in masks.get(aid, []):
                draw.rectangle((x0, y0, x1 - 1, y1 - 1), fill='black')
            # Mask first, then check size/bytes again: PNG re-encoding of a JPEG
            # can exceed the channel budget even when the input was compliant.
            output, image_check = fit_image(picture)
            extension = '.png' if image_check['format'] == 'PNG' else '.jpg'
            public_name = 'assets/image_' + _sha(str(asset['id']).encode())[:20] + extension
            target = directory / public_name
            target.parent.mkdir(parents=True, exist_ok=True)
            # Re-encode pixels, dropping private EXIF/comments and source names.
            target.write_bytes(output)
            asset.update(file=public_name, sha256=_sha(output), width=image_check['width'], height=image_check['height'])
        published.append(asset)
    retained = {a.get('source_image_id') or a['id'] for a in published if a.get('storage') == 'image'}
    _require(all(aid in retained for aid in masks), 'image redaction selected an unpublished image')
    return published


def _readable(value, prefix=''):
    """Mechanical full-field projection; every scalar remains in retrieval text."""
    lines = []
    if isinstance(value, dict):
        for key, child in value.items():
            lines.append(_readable(child, prefix + str(key) + '.'))
    elif isinstance(value, list):
        if not value:
            lines.append(prefix.rstrip('.') + ': []')
        for index, child in enumerate(value):
            lines.append(_readable(child, prefix + str(index) + '.'))
    else:
        rendered = 'null' if value is None else str(value).lower() if isinstance(value, bool) else str(value)
        lines.append(prefix.rstrip('.') + ': ' + rendered)
    return '\n'.join(lines)


def _chapter_groups(nodes):
    """Group the existing Tree through level two without altering its nodes."""
    by_id = {node['id']: node for node in nodes}
    groups = {}
    for index, node in enumerate(nodes):
        path, seen, current = [], set(), node
        while current is not None and current['id'] not in seen:
            seen.add(current['id'])
            path.append(current)
            current = by_id.get(current.get('parent_id'))
        # Ambiguous ancestry must not be guessed from a numbered title.
        owner = node if current is not None else list(reversed(path))[min(1, len(path) - 1)]
        group = groups.setdefault(owner['id'], dict(owner=owner, members=[]))
        group['members'].append((index, node))
    return list(groups.values())


class _Budget:
    def __init__(self, counter):
        _require(counter is not None and callable(getattr(counter, 'count_many', None)), 'exact token counter required')
        self.counter = counter
        self.model_budget = getattr(counter, 'model_budget', 2000)
        self.cl_limit = getattr(counter, 'cl_limit', 2038)
        _require(type(self.model_budget) is int and 1 <= self.model_budget <= 2000, 'token model budget invalid')
        _require(type(self.cl_limit) is int and 1 <= self.cl_limit <= 8191, 'token middleware budget invalid')
        self.cache = {}

    def count(self, text):
        if text not in self.cache:
            values = self.counter.count_many([text])
            _require(isinstance(values, list) and len(values) == 1 and
                     all(type(values[0].get(k)) is int and values[0][k] >= 0 for k in ('model_tokens', 'cl100k_tokens')),
                     'exact token counter response invalid')
            self.cache[text] = values[0]
        return self.cache[text]

    def fits(self, text):
        counts = self.count(text)
        return counts['model_tokens'] <= self.model_budget and counts['cl100k_tokens'] <= self.cl_limit

    def split(self, text, render, preferred=(), together=()):
        """Keep whole paragraphs/sentences where possible, preserving all bytes."""
        _require(isinstance(text, str) and bool(text), 'empty text cannot be split')
        parts, start = [], 0
        boundaries = {m.end() for m in re.finditer(r'\n\s*\n|[。！？.!?][”’"\'）)]*\s*|\n', text)}
        boundaries.update(p for p in preferred if type(p) is int and 0 < p < len(text))
        boundaries.update(point for interval in together for point in interval if 0 < point < len(text))
        boundaries.add(len(text))
        while start < len(text):
            if self.fits(render(len(parts), text[start:])):
                parts.append(text[start:])
                break
            lo, hi, best = start + 1, len(text), start
            while lo <= hi:
                mid = (lo + hi) // 2
                if self.fits(render(len(parts), text[start:mid])):
                    best, lo = mid, mid + 1
                else:
                    hi = mid - 1
            _require(best > start, 'token budget cannot fit required context')
            natural = [x for x in boundaries if start < x <= best and text[start:x].strip()]
            end = max(natural) if natural else best
            # If a complete adjacent group fits by itself, move the boundary
            # before it rather than spending its budget on preceding prose.
            for first, last in sorted(together):
                if start < first < end < last and self.fits(render(len(parts), text[first:last])):
                    end = first
            _require(self.fits(render(len(parts), text[start:end])), 'split token count changed')
            parts.append(text[start:end])
            start = end
        _require(''.join(parts) == text, 'split text coverage changed')
        return parts


def _private_paths(source, bundle):
    values = [source.get(k) for k in ('file', 'text_file', 'index_file')]
    values += [a.get(k) for a in bundle.get('assets', [])
               for k in ('image_path', 'original_image_path', 'local_path')]
    return {variant for v in values if isinstance(v, str) and v for variant in (v, v.replace('\\', '/'))
            if re.search(r'(^[A-Za-z]:[\\/]|^[/\\])', variant)}


def _assert_private_source_omitted(root, source, bundle, plan=()):
    """Known original file bindings must never reach public text or metadata."""
    filenames = {Path(value.replace('\\', '/')).name
                 for value in (source.get('file'), bundle.get('original_filename'))
                 if isinstance(value, str) and value}
    private = _private_paths(source, bundle) | filenames
    # A filename plan is private even under best-effort redaction. Only trust
    # a multi-character literal actually present in the authoritative filename.
    private.update(row['expected_text'] for row in plan if isinstance(row, dict)
        and row.get('scope') == 'filename' and isinstance(row.get('expected_text'), str)
        and len(row['expected_text']) >= 2
        and any(row['expected_text'] in name for name in filenames))
    for path in Path(root).rglob('*'):
        if not path.is_file() or path.suffix not in {'.json', '.md'}:
            continue
        text = path.read_text(encoding='utf8')
        if path.suffix == '.json':
            # Match decoded values too: older JSON may escape every Unicode
            # character, while the publisher sends the decoded public fields.
            text += '\n' + _compact(json.loads(text))
        _require(not any(value in text or json.dumps(value, ensure_ascii=False)[1:-1] in text
                         for value in private), 'private source path or filename found in publication')


def _assert_no_unplanned_identity(public, bundle, plan, *, strict=True, public_blocks=None):
    originals = {row.get('expected_text') for row in plan if row.get('scope') != 'image'
                 and isinstance(row.get('expected_text'), str) and row['expected_text']}
    blocks = {b['id']: b for b in bundle.get('blocks', [])}
    findings = []

    def found(path):
        if strict:
            raise ValueError('redaction original remains in ' + path)
        if not any(row['path'] == path for row in findings):
            findings.append(dict(code='redaction_residual', path=path))
    edited = {}
    for row in plan:
        if row.get('scope') == 'source_text':
            for ref in row.get('source_refs', []):
                edited.setdefault(ref.get('block_id'), []).append((ref.get('start'), ref.get('end')))
    def cited_literal(value, original, refs):
        for ref in refs:
            if ref.get('source_kind') != 'cited_work' or ref.get('block_id') not in blocks:
                continue
            raw = blocks[ref['block_id']]['text']
            first = ref.get('start') if type(ref.get('start')) is int else 0
            last = ref.get('end') if type(ref.get('end')) is int else len(raw)
            if not 0 <= first <= last <= len(raw):
                continue
            if any(type(a) is int and type(b) is int and max(first, a) < min(last, b)
                   for a, b in edited.get(ref['block_id'], [])):
                continue
            quote = raw[first:last].strip()
            if original in quote and (value.strip() in quote or quote in value):
                return True
        return False
    def inspect(value, path='', refs=()):
        if isinstance(value, dict):
            references = value.get('source_refs', refs)
            for key, item in value.items():
                inspect(item, path + '/' + str(key), references)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                inspect(item, path + '/' + str(index), refs)
        elif isinstance(value, str):
            for original in originals:
                if original in value and not cited_literal(value, original, refs):
                    found('generated field ' + path)
    inspect(public)
    citations=[]
    def collect(value):
        if isinstance(value,dict):
            citations.extend(ref for ref in value.get('source_refs',[]) if isinstance(ref,dict) and ref.get('source_kind')=='cited_work')
            for child in value.values():collect(child)
        elif isinstance(value,list):
            for child in value:collect(child)
    collect(public)
    body = blocks if public_blocks is None else {row['id']: row for row in public_blocks}
    for bid,block in body.items():
        raw=block['text']
        for original in originals:
            for match in re.finditer(re.escape(original),raw):
                first,last=match.span()
                if public_blocks is None and any(type(a) is int and type(b) is int and a<=first and last<=b for a,b in edited.get(bid,[])):
                    continue
                if public_blocks is None and any(ref.get('block_id')==bid and
                       (ref['start'] if type(ref.get('start')) is int else 0)<=first and
                       last<=(ref['end'] if type(ref.get('end')) is int else len(raw)) for ref in citations):
                    continue
                # Only known private literals are checked. No guessed identities,
                # blanket replacement or silent semantic repair is performed.
                found(f'body block {bid} at {first}:{last}')
    return findings


def _body_groups(candidate, bundle, positions, edits, *, best_effort=False, warnings=None):
    by_id = {p['block_id']: p for p in positions}
    originals = {b['id']: b for b in bundle['blocks']}
    assets = {a['id']: a for a in bundle.get('assets', [])}
    units = {u['id']: u for u in candidate['profile'].get('learning_units', [])}
    def public_offset(bid, offset, side):
        value = offset
        for edit in edits:
            if edit['block_id'] != bid:
                continue
            if edit['original_start'] < offset < edit['original_end']:
                if best_effort:
                    if warnings is not None:
                        warnings.append(dict(code='redacted_group_boundary', block_id=bid, offset=offset,
                                             execution='expanded_to_replacement_' + side))
                    return by_id[bid]['public_start'] + edit['public_' + side]
                raise ValueError('semantic group boundary intersects a redaction')
            if offset >= edit['original_end']:
                value += (edit['public_end'] - edit['public_start']) - (edit['original_end'] - edit['original_start'])
        return by_id[bid]['public_start'] + value
    groups = []
    for field, prefix in (('semantic_groups', 'semantic'), ('keep_together', 'together')):
        for index, group in enumerate(candidate.get('segmentation', {}).get(field, [])):
            refs = list(group.get('source_refs') or [])
            if not refs:
                refs = [ref for uid in group.get('unit_ids', []) if uid in units for ref in units[uid].get('source_refs', [])]
            ranges, section_ids, unavailable = [], [], False
            for ref in refs:
                bids = [ref['block_id']] if ref.get('block_id') else []
                if not bids and ref.get('section_id'):
                    bids = [p['block_id'] for p in positions if p.get('section_id') == ref['section_id']]
                if not bids and ref.get('asset_id') in assets:
                    bids = assets[ref['asset_id']].get('block_ids') or []
                if best_effort and (not bids or any(bid not in by_id for bid in bids)):
                    unavailable = True
                    break
                _require(bool(bids) and all(bid in by_id for bid in bids), 'semantic group source cannot be located')
                for bid in bids:
                    start = ref.get('start') if ref.get('block_id') and type(ref.get('start')) is int else 0
                    end = ref.get('end') if ref.get('block_id') and type(ref.get('end')) is int else len(originals[bid]['text'])
                    if best_effort and not 0 <= start <= end <= len(originals[bid]['text']):
                        unavailable = True
                        break
                    _require(0 <= start <= end <= len(originals[bid]['text']), 'semantic group source range invalid')
                    ranges.append((public_offset(bid, start, 'start'), public_offset(bid, end, 'end')))
                    sid = by_id[bid].get('section_id')
                    if sid and sid not in section_ids:
                        section_ids.append(sid)
                if unavailable:break
            if best_effort and (unavailable or not ranges):
                if warnings is not None:
                    warnings.append(dict(code='unlocated_optional_group', field=field, index=index,
                                         execution='skipped_optional_hint'))
                continue
            _require(bool(ranges), 'semantic group has no executable source range')
            groups.append(dict(id=prefix + '_' + f'{index:06d}', kind=field,
                public_start=min(p[0] for p in ranges), public_end=max(p[1] for p in ranges),
                reason=group.get('reason'), section_ids=section_ids, unit_ids=group.get('unit_ids') or []))
    return groups


def prepare_publication(candidate, bundle, out_dir, *, paperflow_root, catalog, counter,
                        redaction_policy='strict', candidate_validation=None, admission_policy='strict'):
    """Build one fresh immutable public package from one validated candidate."""
    package = _paperflow(paperflow_root)
    from paperflow.rag import RagPublisher
    from paperflow.structure_split import split_projection
    from paperflow.structure_plan import validate_plan
    from paperflow.embedding_admission import renderer_fingerprint
    _require(redaction_policy in {'strict', 'best_effort'}, 'redaction policy unknown')
    _require(admission_policy in {'strict', 'learning'}, 'admission policy unknown')
    best_effort = redaction_policy == 'best_effort'
    validation = candidate_validation
    admitted_partial = False
    if best_effort or admission_policy == 'learning':
        _require(isinstance(validation, dict) and validation.get('valid') is True
                 and validation.get('candidate') == candidate
                 and validation.get('admission', {}).get('policy', 'strict') == admission_policy
                 and validation.get('admission', {}).get('redaction_policy') == redaction_policy,
                 'best-effort publication requires matching candidate validation')
        admitted_partial = validation['admission'].get('allow_partial') is True
        # Recheck against current material; a stale or edited execution plan
        # cannot authorize publication after source/image facts change.
        from preextract_contract import validate_candidate
        validation = validate_candidate(candidate, bundle, catalog, redaction_policy=redaction_policy,
                                        admission_policy=admission_policy)
        # Normalizing a malformed privacy field can remove the original warning
        # which explained a model's partial label. Retain that already-checked
        # admission context, while every current material/content error still
        # blocks. The model label itself is never changed to filled.
        current_errors = [row for row in validation['errors']
                          if not (admitted_partial and row['code'] == 'candidate_incomplete')]
        _require(not current_errors, 'candidate validation changed before publication')
    _require(candidate.get('schema_version') == 'paper-preextract/1.0', 'preextract schema unknown')
    _require(candidate.get('record_kind') == 'filled_candidate' or
             ((best_effort or admission_policy == 'learning') and admitted_partial), 'candidate is partial or unfilled')
    _require(bundle.get('version') == 'preextract-material/1.0', 'material bundle version unknown')
    source = bundle['source']
    _require(all(candidate.get('source', {}).get(k) == source.get(k) for k in ('source_id', 'source_sha256')),
             'candidate source identity changed')
    digest = source.get('source_sha256')
    _require(isinstance(digest, str) and re.fullmatch('[a-f0-9]{64}', digest), 'source hash missing')
    source_path = Path(str(source.get('file') or ''))
    source_verified = source_path.is_file()
    if source_verified:
        _require(_sha(source_path.read_bytes()) == digest, 'original source version changed')
    _require(candidate.get('profile', {}).get('material_disposition', {}).get('decision') == 'keep'
             or (admission_policy == 'learning' and validation.get('admission', {}).get('usable_unit_ids')),
             'material is excluded or uncertain')
    # The contract validator resolves important evidence gaps and marks those
    # candidates partial. Do not reinterpret free-text impact as a new semantic
    # admission rule or reject faithfully reported source-not-stated limits.
    root = Path(out_dir).resolve()
    _require(not root.exists(), 'publication directory already exists; choose a new version')
    c = copy.deepcopy(candidate)
    blocks = copy.deepcopy(bundle['blocks'])
    _require(blocks and len({b['id'] for b in blocks}) == len(blocks), 'body blocks missing or duplicated')
    _require(all(isinstance(b.get('text'), str) for b in blocks), 'body block text invalid')
    image_plans, offset_map, redaction_audit = _text_edits(c, blocks, bundle.get('metadata', []),
        plan=validation['executable_redactions'] if best_effort else None, best_effort=best_effort)
    frontmatter_blocks = 0
    if best_effort:
        offset_map, frontmatter_blocks = _frontmatter_edits(blocks, bundle['blocks'], offset_map)
    file_refs = {a.get('file_ref'): 'attachment_' + str(i + 1) for i, a in enumerate(bundle.get('attachments', [])) if a.get('file_ref')}
    public_input = {k: c[k] for k in ('profile', 'tree', 'assets', 'gaps', 'attachments', 'segmentation') if k in c}
    if admission_policy == 'learning':
        root_extra = {k: copy.deepcopy(v) for k, v in c.items() if k not in {
            'profile', 'tree', 'assets', 'gaps', 'attachments', 'segmentation', 'source', 'redaction_plan', 'schema_version', 'record_kind'}}
        containers = [dict(path=row['path'], original=row['original']) for row in candidate_validation.get('compatibility_report', [])
                      if row.get('code') == 'normalized_container' and row.get('original') not in (None, '', [], {})]
        if root_extra or containers:
            public_input = copy.deepcopy(public_input)
            public_input['profile']['program_retained_fields'] = dict(root_fields=root_extra, nonstandard_containers=containers)
    public = _public_refs(public_input, source, file_refs)
    residual_findings = _assert_no_unplanned_identity(public, bundle, candidate.get('redaction_plan', []),
        strict=not best_effort, public_blocks=blocks if best_effort else None)
    redaction_report = copy.deepcopy(candidate_validation['redaction_report']) if best_effort else dict(policy='strict')
    redaction_report.update(frontmatter_blocks=frontmatter_blocks, residual_findings=residual_findings,
                            executed=redaction_audit, image_plans=len(image_plans))
    profile = {k: v for k, v in public['profile'].items() if admission_policy == 'learning' or k in PROFILE_FIELDS}
    # Select title-based public names before sealing; upload identity stays stable.
    presentation = package.article_presentation(profile, 'article.md')
    if presentation:
        profile['article'].update(public_file_name=presentation['display_name'],
                                  document_name_policy='paper-title/1')
    tree = copy.deepcopy(public.get('tree', {'nodes': []}))
    # Preserve gaps and attachment decisions as public learning metadata; source
    # paths and all private execution plans stay outside this object.
    profile['gaps'] = public.get('gaps', [])
    profile['attachments'] = public.get('attachments', [])
    for attachment in profile['attachments']:
        value = attachment.get('file_ref')
        if value in file_refs:
            attachment['file_ref'] = file_refs[value]
    ident = dict(task_id='PRE_' + digest[:24] + '_' + uuid.uuid4().hex[:8],
                 source_id=source['source_id'], source_sha256=digest)
    task = dict(ident, revision=1)
    profile.update(ident)
    tree.update(ident)
    # Single source of truth, no summary substitution. Separators are stable and
    # recorded with the block-to-public offsets below.
    body_parts, positions, cursor = [], [], 0
    for i, block in enumerate(blocks):
        text = block['text'] + ('\n\n' if i < len(blocks) - 1 else '')
        body_parts.append(text)
        positions.append(dict(block_id=block['id'], section_id=block.get('section_id'),
                              public_start=cursor, public_end=cursor + len(block['text'])))
        cursor += len(text)
    article = ''.join(body_parts)
    _require(article.strip(), 'body text empty')
    segmentation_warnings = copy.deepcopy(validation.get('segmentation_warnings', [])) if best_effort else []
    groups = _body_groups(c, bundle, positions, offset_map, best_effort=best_effort, warnings=segmentation_warnings)
    budget = _Budget(counter)
    placeholder = 'f' * 64
    provisional = dict(manifest_sha256=placeholder, profile=profile, tree=tree, assets=[], chunks=[], routes=[])
    def render(rid, text):
        # The seal is not known until packing. Leave deterministic room for its
        # less-compressible hex tokens, then qualify the actual final input.
        return RagPublisher.render(task, provisional, (rid, text, None)) + ' reserve' * 64
    suggestions = []
    by_block = {row['block_id']: row for row in positions}
    edits_by_block = {}
    for change in offset_map:
        edits_by_block.setdefault(change['block_id'], []).append(change)
    split_groups = ([dict(split_points=validation['executable_split_points'])] if best_effort
                    else c.get('segmentation', {}).get('split_suggestions', []))
    for group in split_groups:
        for point in group.get('split_points', []):
            bid, offset = point.get('block_id'), point.get('offset')
            _require(bid in by_block and type(offset) is int and 0 <= offset <= len(next(b['text'] for b in bundle['blocks'] if b['id'] == bid)),
                     'segmentation source point invalid')
            adjusted = offset
            for edit in edits_by_block.get(bid, []):
                if edit['original_start'] < offset < edit['original_end']:
                    if best_effort:
                        segmentation_warnings.append(dict(code='redacted_split_hint', block_id=bid, offset=offset,
                                                          execution='skipped_optional_hint'))
                        adjusted = None
                        break
                    raise ValueError('segmentation point intersects a redaction')
                if offset >= edit['original_end']:
                    adjusted += (edit['public_end'] - edit['public_start']) - (edit['original_end'] - edit['original_start'])
            if adjusted is not None:
                suggestions.append(by_block[bid]['public_start'] + adjusted)
    chunks = []
    body = budget.split(article, lambda i, text: render(f'body_{i:06d}', text), suggestions,
                        [(g['public_start'], g['public_end']) for g in groups])
    cursor = 0
    for i, text in enumerate(body):
        end = cursor + len(text)
        sections = list(dict.fromkeys(p['section_id'] for p in positions if p.get('section_id') and p['public_start'] < end and p['public_end'] > cursor))
        group_ids = [g['id'] for g in groups if g['public_start'] < end and g['public_end'] > cursor]
        rid = f'body_{group_ids[0]}_{i:06d}' if group_ids else f'body_{i:06d}'
        chunks.append(dict(id=rid, kind='body', content=text, section_ids=sections, asset_ids=[],
                           char_start=cursor, char_end=end, group_ids=group_ids))
        cursor = end
    title = profile.get('article', {}).get('title') or '论文'
    def linked_limits(pointer):
        return [gap['use_limit'] for gap in profile.get('gaps', [])
                if isinstance(gap.get('use_limit'), str) and gap['use_limit'].strip()
                and (not gap.get('target_pointer')
                     or str(gap['target_pointer']).startswith('/source/')
                     or gap['target_pointer'] in ('/profile/research_profile', '/assets/items')
                     or gap['target_pointer'] == pointer
                     or str(gap['target_pointer']).startswith(pointer + '/')
                     or pointer.startswith(str(gap['target_pointer']) + '/'))]
    def limits_context(limits):
        return ''.join('使用限制：' + limit + '\n' for limit in dict.fromkeys(limits))
    asset_limits = {asset['id']: linked_limits('/assets/items/' + str(index)) + (asset.get('limits') or [])
                    for index, asset in enumerate(public.get('assets', {}).get('items', []))}
    for unit_index, unit in enumerate(profile.get('learning_units', [])):
        uid = unit.get('id')
        _require(isinstance(uid, str) and uid, 'learning unit identity missing')
        context = f'论文：{title}\n学习单元：{uid} {unit.get("title") or ""}\n问题：{unit.get("question") or ""}\n'
        unit_pointer = '/profile/learning_units/' + str(unit_index)
        limits = linked_limits(unit_pointer)
        for asset in public.get('assets', {}).get('items', []):
            if uid in (asset.get('unit_ids') or []) or asset['id'] in (unit.get('asset_ids') or []):
                limits.extend(asset_limits[asset['id']])
        context += limits_context(limits)
        text = _readable(unit)
        prefix = 'learning_' + _sha(uid.encode())[:16] + '_'
        parts = budget.split(text, lambda i, part: render(prefix + f'{i:06d}', context + part))
        for i, part in enumerate(parts):
            chunks.append(dict(id=prefix + f'{i:06d}', kind='learning', content=context + part,
                unit_ids=[uid], section_ids=unit.get('section_ids') or [], asset_ids=unit.get('asset_ids') or [],
                part_index=i, part_count=len(parts), unit_projection_sha256=_sha(text.encode()),
                source_refs=unit.get('source_refs') or []))
    def add_projected(kind, identity, value, context, *, section_ids=(), asset_ids=(), unit_ids=()):
        text = _readable(value)
        prefix = kind + '_' + _sha(identity.encode())[:16] + '_'
        parts = budget.split(text, lambda i, part: render(prefix + f'{i:06d}', context + part))
        for index, part in enumerate(parts):
            chunks.append(dict(id=prefix + f'{index:06d}', kind=kind, content=context + part,
                section_ids=list(section_ids), asset_ids=list(asset_ids), unit_ids=list(unit_ids),
                part_index=index, part_count=len(parts), projection_sha256=_sha(text.encode())))
    for group in groups:
        related = [row['id'] for row in chunks if row['kind'] == 'body' and group['id'] in row.get('group_ids', [])]
        context = f'论文：{title}\n连续材料组：{group["id"]}\n'
        add_projected('continuity', group['id'], dict(group, body_chunk_ids=related), context,
                      section_ids=group['section_ids'], unit_ids=group['unit_ids'])
    chapter_groups = _chapter_groups(tree.get('nodes', []))
    for group in chapter_groups:
        node = group['owner']
        sid = node.get('id')
        _require(isinstance(sid, str) and sid, 'chapter identity missing')
        members = [member for _, member in group['members']]
        limits = [limit for index, _ in group['members'] for limit in linked_limits('/tree/nodes/' + str(index))]
        context = f'论文：{title}\n章节：{sid} {node.get("title") or ""}\n' + limits_context(limits)
        value = members[0] if len(members) == 1 else dict(nodes=members)
        add_projected('chapter', sid, value, context,
                       section_ids=[member['id'] for member in members],
                       unit_ids=list(dict.fromkeys(uid for member in members for uid in (member.get('unit_ids') or []))))
    global_learning = {k: profile[k] for k in ('research_profile', 'applicable_level', 'migration_boundary', 'gaps', 'attachments')
                       if profile.get(k)}
    if admission_policy == 'learning':
        global_learning.update({k: v for k, v in profile.items()
                                if k not in PROFILE_FIELDS | {'task_id', 'source_id', 'source_sha256', 'revision', 'schema'}})
    if global_learning:
        add_projected('research', 'research', global_learning, f'论文：{title}\n整体研究与迁移条件\n')
    # No output directory is created until all text plans have been checked.
    root.mkdir(parents=True)
    try:
        assets = _mask_images(public, bundle, image_plans, root, preserve_extras=admission_policy == 'learning')
        seen_image_hashes = set()
        chart_projections = []
        for asset in assets:
            if asset['storage'] == 'omit':
                continue
            aid = asset['id']
            context = f'论文：{title}\n图表：{aid} {asset.get("caption") or ""}\n'
            context += limits_context(asset_limits.get(aid, []))
            value = {k: asset[k] for k in sorted(set(asset) if admission_policy == 'learning' else ASSET_FIELDS) if k in asset}
            text = _readable(value)
            image_hash = asset.get('sha256') if asset['storage'] == 'image' else None
            if image_hash and image_hash in seen_image_hashes:
                # The unchanged uploader sends identical pixels once. A second
                # use of those pixels still needs its own searchable meaning.
                add_projected('chart', aid, value, context,
                    section_ids=asset.get('section_ids') or [], asset_ids=[aid], unit_ids=asset.get('unit_ids') or [])
                chart_projections.append(dict(asset_id=aid, first_record='chart',
                    full_projection_sha256=_sha(text.encode()), reassembly_verified=True))
                continue
            if image_hash:
                seen_image_hashes.add(image_hash)
            # Reuse the existing asset record as the first chart text segment.
            # Keep every original MD field in metadata and in this projection;
            # only over-budget continuations need separate chart records.
            def anchor(description):
                projected = {k: asset.get(k) for k in ('id', 'kind', 'caption', 'section_id') if asset.get(k) is not None}
                projected['description'] = description
                return render('asset_' + aid, json.dumps(projected, ensure_ascii=False, separators=(',', ':')))
            prefix = 'chart_' + _sha(aid.encode())[:16] + '_'
            suffix = '' if budget.fits(anchor(context + text)) else '\n后续图表说明见关联切片：' + aid
            parts = budget.split(text, lambda i, part: anchor(context + part + suffix) if i == 0
                                 else render(prefix + f'{i:06d}', context + part))
            asset['description'] = context + parts[0] + suffix
            for index, part in enumerate(parts[1:], 1):
                chunks.append(dict(id=prefix + f'{index:06d}', kind='chart', content=context + part,
                    section_ids=asset.get('section_ids') or [], asset_ids=[aid], unit_ids=asset.get('unit_ids') or [],
                    part_index=index, part_count=len(parts), projection_sha256=_sha(text.encode()),
                    continuation_of='asset_' + aid))
            chart_projections.append(dict(asset_id=aid, first_record='asset_' + aid, parts=len(parts),
                full_projection_sha256=_sha(text.encode()), reassembly_verified=''.join(parts) == text))
        routes, pending = package.resolve_routes(profile, assets, catalog)
        _require(routes and not pending, 'specialty mapping incomplete')
        provisional.update(assets=assets, chunks=chunks, routes=routes, root=str(root),
                           manifest=dict(ident, revision='r001'))
        # Keep the existing small structural index while preserving full profile
        # in metadata. Oversize structure uses its existing pure split-plan code.
        records = RagPublisher(None).raw_records(provisional)
        policy_id = _sha(_compact(dict(counter=getattr(counter, 'identity', None),
                                      model_budget=budget.model_budget, cl_limit=budget.cl_limit)).encode())
        binding = dict(ident, revision=1)
        structures = {}
        for rid, text, _ in records['body']:
            if rid not in {'__profile', '__tree'}:
                continue
            structure = split_projection(rid[2:], json.loads(text), article_title=title,
                tree_nodes=tree.get('nodes', []), render=render, count_many=counter.count_many,
                model_budget=budget.model_budget, cl_limit=budget.cl_limit, policy_id=policy_id, binding=binding)
            structure['records'] = [list(record) for record in structure['records']]
            structure['record_hashes'] = {r: _sha(t.encode()) for r, t in structure['records']}
            structures[rid] = structure
        plan = dict(schema='paperflow-structure-plan/1', binding=binding, policy_id=policy_id,
            renderer_fingerprint=renderer_fingerprint(), planning_manifest_sha256=placeholder,
            qualification=False, structures=structures, article_title_source_path=['article', 'title'] if profile.get('article', {}).get('title') else None)
        plan['plan_sha256'] = _sha(_compact(plan).encode())
        validate_plan(task, provisional, plan)
        (root / 'article.redacted.md').write_text(article, encoding='utf8', newline='')
        _write_json(root / 'profile.json', profile)
        _write_json(root / 'tree.json', tree)
        _write_json(root / 'assets.json', dict(ident, items=assets))
        _write_json(root / 'chunks.json', dict(ident, chunks=chunks))
        _write_json(root / 'split-plan.json', plan)
        # Coordinates contain no original values or private paths.
        _write_json(root / 'source-map.json', dict(source_sha256=digest, blocks=positions, text_edits=offset_map, groups=groups,
                                                   reference_coordinate_system='private_source_block_unicode_codepoints'))
        _assert_private_source_omitted(root, source, bundle, candidate.get('redaction_plan', []))
        package.seal_package(root, task, 1)
        packet = package.validate_package(root, task, catalog, strict_seal=True)
        _require((root / 'article.redacted.md').read_text(encoding='utf8') == article,
                 'package normalization changed planned text')
        _require(packet['profile'] == profile and packet['tree'] == tree, 'package normalization changed planned metadata')
        checks = []
        for role, records in RagPublisher(None).records(packet).items():
            for rid, text, asset in records:
                final = RagPublisher.render(task, packet, (rid, text, asset))
                counts = budget.count(final)
                _require(budget.fits(final), 'final embedding input exceeds token budget')
                checks.append(dict(record_id=rid, role=role, input_sha256=_sha(final.encode()), **counts))
        for route in packet['routes']:
            for role, records in RagPublisher(None).records(packet).items():
                if records:
                    name = RagPublisher.document_names(task, packet, route['code'], role)[1]
                    _require(budget.fits(name), 'embedding document title exceeds token budget')
        if source_verified:
            _require(_sha(source_path.read_bytes()) == digest, 'original source changed during publication')
        validation = dict(status='publication_ready', body_exact_reassembly=''.join(r['content'] for r in packet['chunks'] if r['kind'] == 'body') == article,
            publication_version=PUBLICATION_VERSION,
            retrieval_organization=dict(chapter_depth=2, chapter_nodes=len(tree.get('nodes', [])),
                chapter_groups=len(chapter_groups), chart_projections=chart_projections),
            admission=dict(policy=admission_policy,
                           usable_unit_ids=(validation or {}).get('admission', {}).get('usable_unit_ids', [])),
            learning_units=len(profile.get('learning_units', [])), learning_chunks=sum(r['kind'] == 'learning' for r in chunks),
            redaction_actions=len(redaction_audit) + len(image_plans), source_unchanged=True if source_verified else None,
            redaction=redaction_report, model_record_kind=candidate.get('record_kind'),
            embedding=dict(admission_verified=True, counter_identity=getattr(counter, 'identity', None),
                model_budget=budget.model_budget, cl_limit=budget.cl_limit, records=checks),
            manifest_sha256=packet['manifest_sha256'],
            segmentation_warnings=segmentation_warnings + [dict(row, execution='reported_not_interpreted')
                for row in public.get('segmentation', {}).get('special_cases', [])])
        return dict(package_path=str(root), task=task, validation=validation, packet=packet,
                    paperflow_root=str(Path(paperflow_root).resolve()), catalog=copy.deepcopy(catalog))
    except BaseException:
        # Preserve diagnostics and partial artifacts, but never advertise them
        # as a qualified immutable handoff.
        (root / 'READY.json').unlink(missing_ok=True)
        raise


def reconcile_publication(prepared, *, receipt_path, client):
    """Explicitly finish existing documents; never publish, repair or upload.

    The desktop handoff owns the operation lock and service/route binding.
    All remote content must pass exact readback before enabling any document.
    """
    import time
    receipt_path = Path(receipt_path)
    existing = json.loads(receipt_path.read_text(encoding='utf8'))
    package = _paperflow(prepared['paperflow_root'])
    from paperflow.delivery import DeliveryPublisher
    task = prepared['task']
    packet = package.validate_package(prepared['package_path'], task, prepared['catalog'], strict_seal=True)
    _require(prepared['validation']['embedding']['admission_verified'], 'publication admission is missing')
    _require(existing.get('status') in {'pending', 'uncertain', 'complete'}, 'no reconcilable delivery receipt')
    _require(existing.get('task') == task and existing.get('manifest_sha256') == packet['manifest_sha256']
             == prepared['validation']['manifest_sha256'], 'delivery receipt belongs to another task or package')
    targets = copy.deepcopy(existing.get('targets', {}))
    _require(set(targets) == {r['code'] for r in packet['routes']}, 'original delivery targets are incomplete')
    checker = DeliveryPublisher(client)
    roles = {role for role, records in checker.records(packet).items() if records}
    for route in packet['routes']:
        for role in ('body', 'images'):
            obj = targets[route['code']].get(role)
            if role not in roles:
                _require(not obj, 'unexpected delivery role in receipt')
                continue
            expected = dict(paperflow_task=task['task_id'], paperflow_source_sha256=task['source_sha256'],
                paperflow_manifest_sha256=packet['manifest_sha256'], paperflow_revision=str(task['revision']),
                paperflow_specialty=route['code'], paperflow_role=role)
            _require(isinstance(obj, dict) and obj.get('identity') == expected,
                     'original document receipt identity changed')
    # No local receipt or remote availability changes if this readback fails.
    checker.verify(task, packet, targets)
    archived = receipt_path.with_name(receipt_path.stem + '.before-reconcile.' + str(time.time_ns()) + '.json')
    archived.write_bytes(receipt_path.read_bytes())
    state = copy.deepcopy(existing)
    state.update(status='pending', stage='activating', targets=targets,
        reconciliation=dict(previous_receipt=str(archived), exact_readback=True, no_upload=True))
    state.pop('error_type', None)
    _write_json(receipt_path, state)
    def persist(receipts):
        state['targets'] = receipts
        _write_json(receipt_path, state)
    checker.on_change = persist
    try:
        checker.set_enabled(targets, True)
        state['stage'] = 'retrieval'
        _write_json(receipt_path, state)
        checker.verify_retrieval(task, packet, targets)
        state.update(status='complete', stage='verified', source_retained=True,
                     cleanup='not_requested', last_remote_verified_at=time.time())
        _write_json(receipt_path, state)
        return state
    except BaseException as exc:
        state.update(status='uncertain', error_type=type(exc).__name__)
        _write_json(receipt_path, state)
        raise


def deliver_publication(prepared, *, config, receipt_path, client=None):
    """One bounded direct delivery; uncertain writes require explicit reconciliation."""
    receipt_path = Path(receipt_path)
    existing=None
    if receipt_path.exists():
        existing = json.loads(receipt_path.read_text(encoding='utf8'))
        if existing.get('status') != 'complete':
            raise ValueError('pending or uncertain delivery requires read-only reconciliation before retry')
        _require(existing.get('manifest_sha256') == prepared.get('validation', {}).get('manifest_sha256'),
                 'complete delivery receipt belongs to another package')
    package = _paperflow(prepared['paperflow_root'])
    from paperflow.rag import RagPublisher, native_client
    _require(prepared['validation']['embedding']['admission_verified'], 'publication admission is missing')
    task = prepared['task']
    packet = package.validate_package(prepared['package_path'], task, prepared['catalog'], strict_seal=True)
    _require(packet['manifest_sha256'] == prepared['validation']['manifest_sha256'], 'qualified package changed')
    state = dict(schema='paper-preextract-delivery/1.0', status='pending', stage='registered', task=task,
                 manifest_sha256=packet['manifest_sha256'], targets={})
    active_client=client if client is not None else native_client(config)
    publishing_packet=packet
    if existing:
        # Absence must be proved against the complete current inventory. A
        # timeout/permission error is not deletion and must never clear receipts.
        checker=RagPublisher(active_client)
        inventories={};missing=[]
        for route in packet['routes']:
            target=existing.get('targets',{}).get(route['code'],{})
            for role,records in checker.records(packet).items():
                if not records:continue
                obj=target.get(role,{})
                ds=route['paper_dataset_id' if role=='body' else 'image_dataset_id']
                _require(obj.get('dataset_id')==ds and obj.get('document_id'), 'complete receipt target is incomplete')
                if ds not in inventories:inventories[ds]={row['id'] for row in checker.documents(ds)}
                if obj['document_id'] not in inventories[ds]:
                    missing.append(dict(code=route['code'],role=role,dataset_id=ds,document_id=obj['document_id']))
        if not missing:
            # Existing files alone do not prove that their contents or index
            # survived. Reuse the mature readback and retrieval checks.
            checker.verify(task,packet,copy.deepcopy(existing['targets']))
            checker.verify_retrieval(task,packet,copy.deepcopy(existing['targets']))
            import time
            existing['last_remote_verified_at']=time.time()
            _write_json(receipt_path,existing)
            return existing
        affected={row['code'] for row in missing}
        intact_packet=dict(packet,routes=[route for route in packet['routes'] if route['code'] not in affected])
        checker.verify(task,intact_packet,copy.deepcopy(existing['targets']))
        checker.verify_retrieval(task,intact_packet,copy.deepcopy(existing['targets']))
        publishing_packet=dict(packet,routes=[route for route in packet['routes'] if route['code'] in affected])
        import time
        archived=receipt_path.with_name(receipt_path.stem+'.'+str(time.time_ns())+receipt_path.suffix)
        archived.write_bytes(receipt_path.read_bytes())
        state['targets']=copy.deepcopy(existing['targets'])
        for row in missing:
            target=state['targets'][row['code']]
            target.pop(row['role'])
            target.pop('verified',None);target.pop('retrieval_verified',None)
        state['remote_reconciliation']=dict(missing=missing,previous_receipt=str(archived),
            basis='complete remote document inventories; local source and model result reused')
    # Recheck the actual sealed package before any new write, including older
    # cached packages. A completed receipt's read-only verification above keeps
    # its existing behavior and never changes the old seal or delivery identity.
    material_path = receipt_path.parent / 'material.json'
    if material_path.exists():
        material = json.loads(material_path.read_text(encoding='utf8'))
        _require(isinstance(material, dict) and material.get('version') == 'preextract-material/1.0'
                 and isinstance(material.get('source'), dict), 'private source material invalid')
        _require(material['source'].get('source_sha256') == task['source_sha256'],
                 'private source material identity changed')
        candidate_path = receipt_path.parent / 'private' / 'candidate.json'
        candidate = json.loads(candidate_path.read_text(encoding='utf8')) if candidate_path.exists() else {}
        _require(isinstance(candidate, dict), 'private source candidate invalid')
        _assert_private_source_omitted(packet['root'], material['source'], material,
                                       candidate.get('redaction_plan') or [])
    _write_json(receipt_path, state)
    def persist(receipts):
        state['targets'] = receipts
        _write_json(receipt_path, state)
    try:
        from preextract_batch_delivery import delivery_publisher
        publisher = (RagPublisher(active_client, on_change=persist) if existing else
                     delivery_publisher(active_client, persist, prepared, packet))
        state['transport'] = ('native_body_batch/1' if hasattr(publisher, 'config')
                              else 'direct_chunks/1')
        state['stage'] = 'publishing'
        _write_json(receipt_path, state)
        publisher.publish(task, publishing_packet, state['targets'])
        state['stage'] = 'readback'
        _write_json(receipt_path, state)
        publisher.verify(task, packet, state['targets'])
        state['stage'] = 'activating'
        _write_json(receipt_path, state)
        publisher.set_enabled({route['code']:state['targets'][route['code']] for route in publishing_packet['routes']}, True)
        state['stage'] = 'retrieval'
        _write_json(receipt_path, state)
        publisher.verify_retrieval(task, packet, state['targets'])
        state.update(status='complete', stage='verified', source_retained=True, cleanup='not_requested')
        _write_json(receipt_path, state)
        return state
    except BaseException as exc:
        state.update(status='uncertain', error_type=type(exc).__name__)
        _write_json(receipt_path, state)
        raise
