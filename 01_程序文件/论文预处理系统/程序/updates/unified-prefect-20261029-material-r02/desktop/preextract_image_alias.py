"""Process-private proof of a payload alias, never an alias of source locations.

Only restore_request_images issues proof after checking actual saved request
bytes. Nothing in a candidate, material JSON or adaptation sidecar is authority.
"""
import hashlib
import json


_SEAL = object()
_LOCATION = ('id', 'kind', 'position', 'block_ids', 'section_ids', 'display_crop')
_VERSION = ('image_path', 'sha256', 'width', 'height')


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _source(bundle):
    source = bundle.get('source') or {}
    return {key: source.get(key) for key in
            ('source_id', 'source_sha256', 'text_sha256', 'index_sha256', 'catalog_sha256')} | {
        'file_ref': bundle.get('file_ref', 'main'),
        'associated_materials_sha256': bundle.get('associated_materials_sha256')}


def _location(asset):
    return {key: asset.get(key) for key in _LOCATION}


def _version(asset):
    return {key: asset.get(key) for key in _VERSION}


class _RequestProof:
    __slots__ = ('payload', 'seal')

    def __init__(self, payload, seal):
        if seal is not _SEAL:
            raise TypeError('request image proof is private')
        # Immutable snapshot; later mutation of bundle/group dicts cannot add IDs.
        self.payload, self.seal = _json(payload), seal


class AliasContext:
    """An empty output collector; JSON cannot construct an issued proof."""
    __slots__ = ('__proofs',)

    def __init__(self):
        self.__proofs = ()

    def _record(self, payload, seal):
        if seal is not _SEAL:
            raise TypeError('request image proof is private')
        self.__proofs += (_RequestProof(payload, seal),)

    def permits(self, bundle, own, selected):
        from preextract_images import _verified_image
        from preextract_material import file_sha
        for issued in self.__proofs:
            if type(issued) is not _RequestProof or issued.seal is not _SEAL:
                continue
            proof = json.loads(issued.payload)
            if proof['source'] != _source(bundle):
                continue
            members = proof['members']
            left, right = members.get(own.get('id')), members.get(selected.get('id'))
            if not left or not right:
                continue
            # One direct group in one actual request. Never union/transitively
            # close groups collected from different parts or ancestors.
            if not any(own['id'] in group and selected['id'] in group for group in proof['groups']):
                continue
            if any(_json(_location(asset)) != _json(member['location']) or
                   _version(asset) != member['request_version']
                   for asset, member in ((own, left), (selected, right))):
                continue
            originals = (left['original'], right['original'])
            if any(not originals[0].get(key) or originals[0][key] != originals[1].get(key)
                   for key in ('sha256', 'width', 'height')):
                continue
            try:
                if any(file_sha(path) != digest for path, digest in proof['files'].items()):
                    continue
                # Recheck both physical originals and both sent renditions; an
                # old in-memory proof cannot authorize drift before publication.
                for asset in (*originals, own, selected):
                    _verified_image(asset)
            except (OSError, KeyError, ValueError, TypeError):
                continue
            return True
        return False


def _record_verified_request(context, bundle, original_bundle, request, groups, declared, work):
    """Internal producer, called only after restore's full byte verification."""
    if context is None:
        return
    if type(context) is not AliasContext:
        raise TypeError('private image alias context required')
    from pathlib import Path
    work = Path(work)
    saved = [path for path in (work / 'model/request.json', work / 'request.json') if path.is_file()]
    if len(saved) != 1:
        return  # A planned request or a JSON sidecar never issues proof.
    signature = hashlib.sha256(json.dumps(request, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
    actual = json.loads(saved[0].read_text(encoding='utf8'))
    if hashlib.sha256(json.dumps(actual, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest() != signature:
        raise ValueError('image alias saved request identity changed')
    from preextract_request_control import verified_saved_state
    from preextract_material import file_sha
    state = verified_saved_state(saved[0].parent)
    if (state.get('status') != 'response_complete' or state.get('request_sha256') != signature
            or file_sha(saved[0].parent / 'response.txt') != state.get('response_sha256')):
        return
    from preextract_images import _verified_image
    if _source(bundle) != _source(original_bundle):
        raise ValueError('image alias original source changed')
    projection = json.loads(request['messages'][1]['content'][0]['text'])['material']
    if _source(projection) != _source(bundle):
        raise ValueError('image alias request source changed')
    source_sha = _source(bundle).get('source_sha256')
    if not isinstance(source_sha, str) or len(source_sha) != 64:
        raise ValueError('image alias source identity missing')
    original_rows = original_bundle.get('assets', [])
    originals = {a['id']: a for a in original_rows}
    if len(originals) != len(original_rows):
        raise ValueError('image alias original identities are ambiguous')
    members = {}
    for asset in bundle.get('assets', []):
        if not asset.get('image_path'):
            continue
        original = originals.get(asset['id'])
        sent = declared.get(asset['id'])
        if (not original or not sent or _json(_location(original)) != _json(_location(asset))
                or _json(_location(sent)) != _json(_location(asset))):
            continue
        # An already adapted subset is not an original. The caller must supply
        # the full registered original_bundle, not trust adaptation.source_sha.
        if original.get('image_adaptation'):
            continue
        _verified_image(original)
        adaptation = asset.get('image_adaptation')
        if adaptation:
            if any(adaptation.get('source_' + key) != original.get(key)
                   for key in ('sha256', 'width', 'height')):
                raise ValueError('image alias adaptation original version changed')
        elif any(asset.get(key) != original.get(key) for key in ('sha256', 'width', 'height')):
            raise ValueError('image alias original rendition changed')
        members[asset['id']] = dict(location=_location(asset), request_version=_version(asset),
                                   original=_version(original))
    files = {str(path): file_sha(path) for path in
             (saved[0], saved[0].with_name('request-state.json'), saved[0].with_name('response.txt'),
              saved[0].with_name('response-events.jsonl'), saved[0].with_name('adopted-response.json')) if path.is_file()}
    context._record(dict(source=_source(bundle), request_sha256=signature, files=files,
                         groups=[list(group['asset_ids']) for group in groups], members=members), _SEAL)


def _owned_refs(row, own, bundle, partition_material=None):
    """Stricter than learning-mode diagnostics; never clears or repairs refs."""
    if not isinstance(row, dict) or row.get('id') != own.get('id'):
        return False
    refs = row.get('source_refs')
    if not isinstance(refs, list) or not refs:
        return False
    primary = {value for value in (bundle.get('file_ref', 'main'),
        *(bundle.get('source', {}).get(key) for key in ('source_id', 'file', 'text_file'))) if isinstance(value, str)}
    blocks = {b['id']: b for b in bundle.get('blocks', [])}
    if partition_material is not None and _source(partition_material) == _source(bundle):
        for block in partition_material.get('blocks', []):
            blocks.setdefault(block['id'], block)
    own_blocks, own_sections = set(own.get('block_ids') or []), set(own.get('section_ids') or [])
    row_sections = row.get('section_ids')
    if (not isinstance(row_sections, list)
            or any(not isinstance(sid, str) or sid not in own_sections for sid in row_sections)):
        return False
    anchored = False
    for ref in refs:
        if not isinstance(ref, dict):
            return False
        file_ref, bid, sid, aid = (ref.get(key) for key in ('file_ref', 'block_id', 'section_id', 'asset_id'))
        if any(value is not None and not isinstance(value, str) for value in (file_ref, bid, sid, aid)):
            return False
        if file_ref is not None and file_ref not in primary:
            return False
        if aid is not None and aid != own['id']:
            return False
        if sid is not None and sid not in own_sections:
            return False
        block = blocks.get(bid) if bid is not None else None
        if bid is not None:
            if block is None:
                return False
            span = block.get('source_span') or {}
            if bid not in own_blocks and span.get('block_id') not in own_blocks:
                return False
            if bid not in own_blocks:
                original = blocks.get(span.get('block_id'))
                a, z = span.get('start'), span.get('end')
                if (not original or type(a) is not int or type(z) is not int
                        or not 0 <= a <= z <= len(original.get('text', ''))
                        or original.get('text', '')[a:z] != block.get('text', '')):
                    return False
            if block.get('section_id') not in own_sections or (sid is not None and sid != block.get('section_id')):
                return False
        start, end = ref.get('start'), ref.get('end')
        if start is not None or end is not None:
            if (not block or type(start) is not int or type(end) is not int
                    or not 0 <= start < end <= len(block.get('text', ''))):
                return False
        anchored = anchored or aid == own['id'] or block is not None
    return anchored


def _mask_dependency(original_rows, current_rows, redactions, own, bundle):
    # Publication uses mask IDs. Do not introduce a new alias that could make a
    # mask attached to an omitted duplicate disappear while its pixels publish.
    ids = {a['id'] for a in bundle.get('assets', []) if a.get('image_path') and
           all(a.get(key) == own.get(key) for key in ('sha256', 'width', 'height'))}
    rows = [item for source in (original_rows, current_rows) if isinstance(source, list)
            for item in source if isinstance(item, dict)]
    public = any(item.get('storage') == 'image' and
                 ((isinstance(item.get('id'), str) and item['id'] in ids) or (isinstance(item.get('source_image_id'), str)
                                           and item['source_image_id'] in ids)) for item in rows)
    if not public:
        return False
    return any(isinstance(plan, dict) and plan.get('scope') == 'image'
               and isinstance(plan.get('image_ref'), dict)
               and any(isinstance(plan['image_ref'].get(key), str) and plan['image_ref'][key] in ids
                       for key in ('asset_id', 'id', 'source_image_id')) for plan in redactions)


def verified_alias(context, original_rows, row, own, selected, bundle, *, partition_material=None,
                   current_rows=None, original_redactions=None, current_redactions=None):
    if type(context) is not AliasContext or not isinstance(original_rows, list):
        return False
    matches = [item for item in original_rows if isinstance(item, dict) and item.get('id') == own.get('id')]
    if len(matches) != 1 or matches[0].get('source_image_id') != selected.get('id'):
        return False
    redactions = [plan for source in (original_redactions, current_redactions) if isinstance(source, list)
                  for plan in source]
    if _mask_dependency(original_rows, current_rows, redactions, own, bundle):
        return False
    return (_owned_refs(matches[0], own, bundle, partition_material)
            and _owned_refs(row, own, bundle, partition_material)
            and context.permits(bundle, own, selected))
