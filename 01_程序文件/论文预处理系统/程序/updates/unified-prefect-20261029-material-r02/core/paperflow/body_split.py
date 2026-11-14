"""Pure, source-bound integer-line body splitting; callers own sealing and handoff."""
import copy
import hashlib
import re

from .embedding_admission import AdmissionPolicy, EmbeddingInput, digest, evaluate
from .rag import RagPublisher


class BodySplitHold(ValueError):
    """This article cannot be transformed within its authorized body scope."""


def _sha(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _require(condition, reason):
    if not condition:
        raise BodySplitHold(reason)


def _boundary(lines, end, parent_end):
    if end == parent_end or not lines[end - 1].strip():
        return 'paragraph'
    if re.search(r'[。！？.!?][”’"\'）)]*\s*$', lines[end - 1]):
        return 'sentence'
    return 'line'


def _validate_source(task, source, target, article, scope):
    _require(scope.get('schema') == 'paperflow-body-split-scope/1', 'body scope schema mismatch')
    _require(scope.get('scope_sha256') == digest({k: v for k, v in scope.items() if k != 'scope_sha256'}),
             'body scope hash mismatch')
    _require(scope.get('boundary_mode') in ('semantic', 'ordinary'), 'body boundary mode unknown')
    _require(source.get('manifest_sha256') == scope.get('source_manifest_sha256'), 'body source manifest changed')
    _require(source['manifest'].get('revision') == scope.get('source_revision'), 'body source revision changed')
    _require(_sha(article) == scope.get('article_text_sha256'), 'body article changed')
    _require(digest(source['chunks']) == scope.get('chunks_rows_sha256'), 'body source rows changed')
    for key, expected in scope.get('identity', {}).items():
        _require(key in ('task_id', 'source_id', 'source_sha256') and expected
                 and task.get(key) == source['manifest'].get(key) == target['manifest'].get(key) == expected,
                 'body identity changed')
    _require(set(scope.get('identity', {})) == {'task_id', 'source_id', 'source_sha256'}, 'body identity missing')
    final_hash = target.get('manifest_sha256', '')
    _require(isinstance(final_hash, str) and re.fullmatch('[0-9a-f]{64}', final_hash)
             and final_hash != '0' * 64 and final_hash != source['manifest_sha256'], 'body render seal missing')
    revision = task.get('revision')
    _require(type(revision) is int and revision > int(scope['source_revision'].removeprefix('r'))
             and target['manifest'].get('revision') == f'r{revision:03d}', 'body target revision mismatch')
    source_files = {entry['path']: entry.get('sha256') for entry in source['manifest'].get('files', [])
                    if isinstance(entry, dict)}
    _require(all(source_files.get(name) == value for name, value in scope.get('core_hashes', {}).items()),
             'body source file hashes changed')
    rows = source['chunks']
    ids = [row.get('id') for row in rows]
    _require(all(isinstance(rid, str) and rid for rid in ids) and len(ids) == len(set(ids)), 'body source ids invalid')
    lines = article.splitlines(keepends=True)
    next_line = 1
    for row in rows:
        if row.get('kind') != 'body':
            continue
        first, last = row.get('line_start'), row.get('line_end')
        _require(type(first) is int and type(last) is int and first == next_line and first <= last <= len(lines),
                 'body source ranges are not complete and ordered')
        _require(row.get('content') == ''.join(lines[first - 1:last]), 'body source line text changed')
        next_line = last + 1
    _require(next_line == len(lines) + 1, 'body source ranges omit text')
    parents = scope.get('parents', [])
    _require(parents and len({p['record_id'] for p in parents}) == len(parents), 'body parent scope invalid')
    by_id = {row['id']: row for row in rows}
    for parent in parents:
        row = by_id.get(parent['record_id'])
        _require(row is not None and row.get('kind') == 'body'
                 and row.get('line_start') == parent.get('line_start') and row.get('line_end') == parent.get('line_end')
                 and _sha(row.get('content', '')) == parent.get('content_sha256'), 'body parent scope changed')
    return lines


def _previous_ranges(lines, parents, previous, mode):
    if not previous:
        return {rid: [(parent['line_start'], parent['line_end'])] for rid, parent in parents.items()}
    grouped = {rid: [] for rid in parents}
    _require(isinstance(previous, list), 'body previous partition invalid')
    for part in previous:
        _require(isinstance(part, dict) and part.get('parent_record_id') in parents, 'body previous parent unknown')
        rid = part['parent_record_id']
        parent = parents[rid]
        first, last = part.get('line_start'), part.get('line_end')
        expected = grouped[rid][-1][1] + 1 if grouped[rid] else parent['line_start']
        _require(type(first) is int and type(last) is int and first == expected and first <= last <= parent['line_end'],
                 'body previous ranges changed')
        _require(part.get('record_id') == rid + '.l' + str(first).zfill(6)
                 and part.get('content_sha256') == _sha(''.join(lines[first - 1:last])), 'body previous content changed')
        boundary = _boundary(lines, last, parent['line_end'])
        _require(part.get('boundary') == boundary and (mode == 'ordinary' or boundary != 'line'),
                 'body previous boundary changed')
        grouped[rid].append((first, last))
    _require(all(ranges and ranges[-1][1] == parents[rid]['line_end'] for rid, ranges in grouped.items()),
             'body previous partition incomplete')
    return grouped


def build_body_chunks(task, source_packet, render_packet, article_text, scope, *, policy, counter,
                      previous_parts=None):
    """Return new chunk values and provisional counts without changing either packet.

    Only the caller's sealed render_packet supplies the current identity prefix.
    Full-package admission must run again after the returned chunks are sealed.
    """
    lines = _validate_source(task, source_packet, render_packet, article_text, scope)
    _require(isinstance(policy, AdmissionPolicy), 'body policy is unverified')
    parents = {row['record_id']: row for row in scope['parents']}
    if previous_parts is not None:
        _require(isinstance(previous_parts, list) and bool(previous_parts), 'body previous partition missing')
    previous = scope.get('seed_parts') if previous_parts is None else previous_parts
    ranges = _previous_ranges(lines, parents, previous, scope['boundary_mode'])
    code = render_packet['routes'][0]['code']
    all_parts = []
    result = []
    reserved = {row['id'] for row in source_packet['chunks']} | {'__profile', '__tree'}
    from .package import asset_storage_mode
    reserved.update('asset_' + asset['id'] for asset in source_packet['assets']
                    if asset_storage_mode(asset) != 'omit')
    measured = {}

    def measure_intervals(intervals):
        missing = [key for key in intervals if key not in measured]
        if missing:
            entries = []
            for parent_id, start, end in missing:
                rid = parent_id + '.l' + str(start).zfill(6)
                _require(rid not in reserved, 'body child id collision')
                entries.append(EmbeddingInput(code, 'body', rid, 'body', 'native_content',
                               RagPublisher.render(task, render_packet, (rid, ''.join(lines[start - 1:end]), None))))
            checked = evaluate(entries, policy, counter, observed_path=policy.path_fingerprint,
                               observed_renderer=policy.renderer_fingerprint)
            _require(not checked['issues'], 'body counter is unverified')
            measured.update(zip(missing, checked['records']))
        return [measured[key] for key in intervals]

    # One request checks the known source/previous partitions for this article.
    # Only overlong pieces need additional boundary searches.
    measure_intervals([(rid, start, end) for rid, segments in ranges.items() for start, end in segments])
    for row in source_packet['chunks']:
        if row['id'] not in parents:
            result.append(copy.deepcopy(row))
            continue
        first, last = row['line_start'], row['line_end']
        cache = {}
        dead = set()

        def search(start, limit):
            if start == limit + 1:
                return []
            if start in dead:
                return None
            rid = row['id'] + '.l' + str(start).zfill(6)
            _require(rid not in reserved, 'body child id collision')
            ends = [end for end in range(start, limit + 1)
                    if scope['boundary_mode'] == 'ordinary' or _boundary(lines, end, last) != 'line']

            def measure(endpoints):
                if not endpoints:
                    return {}
                checked = measure_intervals([(row['id'], start, end) for end in endpoints])
                return {end: item for end, item in zip(endpoints, checked) if not item['issues']}

            # Keep an already permitted part intact when its actual current
            # prefix fits; only failed parts search finer authorized boundaries.
            cache[start] = measure([limit]) if limit in ends else {}
            if limit in cache[start] and ''.join(lines[start - 1:limit]).strip():
                return [(start, limit, cache[start][limit])]
            cache[start].update(measure([end for end in ends if end != limit]))
            order = {'paragraph': 0, 'sentence': 1, 'line': 2}
            for end in sorted(cache[start], key=lambda value: (order[_boundary(lines, value, last)], -value)):
                if not ''.join(lines[start - 1:end]).strip():
                    continue
                following = search(end + 1, limit)
                if following is not None:
                    return [(start, end, cache[start][end])] + following
            dead.add(start)
            return None

        chosen = []
        for segment_start, segment_end in ranges[row['id']]:
            partition = search(segment_start, segment_end)
            _require(partition is not None, 'no permitted integer-line partition')
            chosen.extend(partition)
        fragments = []
        for start, end, counts in chosen:
            content = ''.join(lines[start - 1:end])
            fragment = dict(copy.deepcopy(row), id=counts['record_id'], line_start=start, line_end=end, content=content)
            fragments.append(fragment)
            all_parts.append(dict(parent_record_id=row['id'], record_id=fragment['id'], line_start=start, line_end=end,
                                  boundary=_boundary(lines, end, last), content_sha256=_sha(content),
                                  model_tokens=counts['model_tokens'], cl100k_tokens=counts['cl100k_tokens']))
        _require(''.join(part['content'] for part in fragments) == row['content'], 'body parent reassembly changed')
        result.extend(fragments)
    body = [row for row in result if row.get('kind') == 'body']
    _require(''.join(row['content'] for row in body) == article_text, 'whole article reassembly changed')
    return dict(chunks=result, evidence=dict(schema='paperflow-body-split-evidence/1', qualification=False,
                source_manifest_sha256=source_packet['manifest_sha256'],
                planning_manifest_sha256=render_packet['manifest_sha256'], policy_fingerprint=policy.fingerprint,
                scope_sha256=scope['scope_sha256'], final_identity_verified=False, admission_verified=False,
                parent_count=len(parents), part_count=len(all_parts), parts=all_parts,
                exact_reassembly=True, ordered_ranges=True))
