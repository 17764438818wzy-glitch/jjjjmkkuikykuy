"""Reorganize sealed results without extraction, image encoding or learning cuts."""
from __future__ import annotations

import collections
import copy
import json
import shutil
from pathlib import Path
from uuid import uuid4

from preextract_publish import (ASSET_FIELDS, PUBLICATION_VERSION, _Budget,
    _chapter_groups, _compact, _paperflow, _readable, _require, _sha, _write_json)


def _projection(rows):
    """Recover exact legacy text and context using the stored payload digest."""
    rows = sorted(rows, key=lambda row: row['part_index'])
    _require(bool(rows), 'original projection missing')
    _require([row['part_index'] for row in rows] == list(range(len(rows))), 'original projection parts missing')
    _require(all(row['part_count'] == len(rows) for row in rows), 'original projection part count changed')
    digest = rows[0]['projection_sha256']
    _require(all(row['projection_sha256'] == digest for row in rows), 'original projection digests differ')
    first = rows[0]['content']
    for start in [0] + [i + 1 for i, char in enumerate(first) if char == '\n']:
        context = first[:start]
        if all(row['content'].startswith(context) for row in rows):
            text = ''.join(row['content'][start:] for row in rows)
            if _sha(text.encode()) == digest:
                return context, text
    raise ValueError('original projection cannot be reassembled')


def consolidate_publication(prepared, out_dir, *, counter):
    """Build a fresh immutable package; never replace the old prepared pointer."""
    package = _paperflow(prepared['paperflow_root'])
    from paperflow.rag import RagPublisher, compact
    from paperflow.structure_split import split_projection
    from paperflow.structure_plan import validate_plan
    from paperflow.embedding_admission import renderer_fingerprint

    old_root = Path(prepared['package_path']).resolve()
    old = package.validate_package(old_root, prepared['task'], prepared['catalog'], strict_seal=True)
    report = prepared['validation']
    _require(old['manifest_sha256'] == report['manifest_sha256'], 'existing sealed publication changed')
    _require(report.get('embedding', {}).get('admission_verified') is True, 'existing publication admission missing')
    budget = _Budget(counter)
    _require(getattr(counter, 'identity', None) == report['embedding']['counter_identity'], 'existing tokenizer identity changed')
    _require((budget.model_budget, budget.cl_limit) ==
             (report['embedding']['model_budget'], report['embedding']['cl_limit']), 'existing token budget changed')
    root = Path(out_dir).resolve()
    _require(not root.exists() and not root.is_relative_to(old_root), 'choose a fresh publication directory')
    frozen = {'body', 'learning', 'research', 'continuity'}
    _require(all(row['kind'] in frozen | {'chapter', 'chart'} for row in old['chunks']), 'unknown original chunk kind')
    task = dict(prepared['task'], task_id='PRE_' + prepared['task']['source_sha256'][:24] + '_' + uuid4().hex[:8])
    ident = {key:task[key] for key in ('task_id', 'source_id', 'source_sha256')}
    profile, tree, assets = [copy.deepcopy(old[key]) for key in ('profile', 'tree', 'assets')]
    profile['task_id'] = tree['task_id'] = task['task_id']
    chunks = [copy.deepcopy(row) for row in old['chunks'] if row['kind'] in frozen]
    provisional = dict(manifest_sha256='f' * 64, profile=profile, tree=tree, assets=assets,
        chunks=chunks, routes=copy.deepcopy(old['routes']), root=str(root), manifest=dict(ident, revision='r001'))

    def render(rid, text):
        return RagPublisher.render(task, provisional, (rid, text, None)) + ' reserve' * 64

    def project(kind, identity, text, context, **metadata):
        prefix = kind + '_' + _sha(identity.encode())[:16] + '_'
        parts = budget.split(text, lambda i, part: render(prefix + f'{i:06d}', context + part))
        chunks.extend(dict(id=prefix + f'{i:06d}', kind=kind, content=context + part,
            part_index=i, part_count=len(parts), projection_sha256=_sha(text.encode()), **metadata)
            for i, part in enumerate(parts))

    title = profile.get('article', {}).get('title') or '论文'
    chapters, charts = collections.defaultdict(list), collections.defaultdict(list)
    for row in old['chunks']:
        if row['kind'] == 'chapter':
            _require(len(row['section_ids']) == 1, 'existing chapter projection already grouped; preserve it')
            chapters[row['section_ids'][0]].append(row)
        elif row['kind'] == 'chart':
            _require(len(row['asset_ids']) == 1, 'original chart identity ambiguous')
            charts[row['asset_ids'][0]].append(row)
    contexts = {}
    for node in tree.get('nodes', []):
        context, text = _projection(chapters[node['id']])
        _require(text == _readable(node), 'original chapter metadata differs from its projection')
        contexts[node['id']] = context
    _require(set(chapters) == {node['id'] for node in tree.get('nodes', [])}, 'original chapter coverage changed')
    groups = _chapter_groups(tree.get('nodes', []))
    for group in groups:
        owner = group['owner']
        members = [node for _, node in group['members']]
        tails = []
        for node in members:
            header = f'论文：{title}\n章节：{node["id"]} {node.get("title") or ""}\n'
            _require(contexts[node['id']].startswith(header), 'original chapter context unknown')
            tail = contexts[node['id']][len(header):]
            if tail and tail not in tails:
                tails.append(tail)
        context = f'论文：{title}\n章节：{owner["id"]} {owner.get("title") or ""}\n' + ''.join(tails)
        project('chapter', owner['id'], _readable(members[0] if len(members) == 1 else dict(nodes=members)), context,
            section_ids=[node['id'] for node in members], asset_ids=[],
            unit_ids=list(dict.fromkeys(uid for node in members for uid in node.get('unit_ids', []))))

    seen_images, represented, chart_audit = set(), set(), []
    for asset in assets:
        if asset['storage'] == 'omit':
            continue
        aid = asset['id']
        context, text = _projection(charts[aid])
        value = {key:asset[key] for key in sorted(ASSET_FIELDS) if key in asset}
        _require(collections.Counter(text.splitlines()) == collections.Counter(_readable(value).splitlines()),
                 'original chart field coverage differs from full metadata')
        _require(all(line in text for line in asset.get('description', '').splitlines() if line.strip()),
                 'original asset description contains additional content')
        represented.add(aid)
        image_hash = asset.get('sha256') if asset['storage'] == 'image' else None
        if image_hash and image_hash in seen_images:
            chunks.extend(copy.deepcopy(charts[aid]))
            chart_audit.append(dict(asset_id=aid, first_record='chart', reassembly_verified=True))
            continue
        if image_hash:
            seen_images.add(image_hash)

        def anchor(description):
            projected = {key:asset[key] for key in ('id', 'kind', 'caption', 'description', 'note',
                'section_id', 'source_location', 'body_anchor') if asset.get(key) is not None}
            projected['description'] = description
            return render('asset_' + aid, compact(projected))

        prefix = 'chart_' + _sha(aid.encode())[:16] + '_'
        suffix = '' if budget.fits(anchor(context + text)) else '\n后续图表说明见关联切片：' + aid
        parts = budget.split(text, lambda i, part: anchor(context + part + suffix) if i == 0
                             else render(prefix + f'{i:06d}', context + part))
        asset['description'] = context + parts[0] + suffix
        chunks.extend(dict(id=prefix + f'{i:06d}', kind='chart', content=context + part,
            section_ids=asset.get('section_ids') or [], asset_ids=[aid], unit_ids=asset.get('unit_ids') or [],
            source_refs=copy.deepcopy(asset.get('source_refs') or []), part_index=i, part_count=len(parts),
            projection_sha256=_sha(text.encode()), continuation_of='asset_' + aid)
            for i, part in enumerate(parts[1:], 1))
        _require(''.join(parts) == text, 'original chart payload changed')
        chart_audit.append(dict(asset_id=aid, first_record='asset_' + aid, parts=len(parts),
            full_projection_sha256=_sha(text.encode()), reassembly_verified=True))
    _require(represented == set(charts), 'original chart coverage changed')

    structures = {}
    policy_id = _sha(_compact(dict(counter=getattr(counter, 'identity', None),
        model_budget=budget.model_budget, cl_limit=budget.cl_limit)).encode())
    for rid, text, _ in RagPublisher(None).raw_records(provisional)['body']:
        if rid not in {'__profile', '__tree'}:
            continue
        structure = split_projection(rid[2:], json.loads(text), article_title=title, tree_nodes=tree.get('nodes', []),
            render=render, count_many=counter.count_many, model_budget=budget.model_budget,
            cl_limit=budget.cl_limit, policy_id=policy_id, binding=task)
        structure['records'] = [list(record) for record in structure['records']]
        structure['record_hashes'] = {rid:_sha(text.encode()) for rid, text in structure['records']}
        structures[rid] = structure
    plan = dict(schema='paperflow-structure-plan/1', binding=task, policy_id=policy_id,
        renderer_fingerprint=renderer_fingerprint(), planning_manifest_sha256='f' * 64, qualification=False,
        structures=structures, article_title_source_path=['article', 'title'] if profile.get('article', {}).get('title') else None)
    plan['plan_sha256'] = _sha(_compact(plan).encode())
    validate_plan(task, provisional, plan)
    root.mkdir(parents=True)
    try:
        copied = ['article.redacted.md', 'source-map.json'] + [entry['path'] for entry in old['manifest']['files']
            if entry['path'].startswith('assets/')]
        for name in copied:
            source = (old_root / name).resolve()
            _require(source.is_relative_to(old_root), 'original asset path escaped publication')
            destination = root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        for name, value in [('profile.json', profile), ('tree.json', tree), ('assets.json', dict(ident, items=assets)),
                            ('chunks.json', dict(ident, chunks=chunks)), ('split-plan.json', plan)]:
            _write_json(root / name, value)
        package.seal_package(root, task, task['revision'])
        packet = package.validate_package(root, task, prepared['catalog'], strict_seal=True)
        _require(packet['profile'] == profile and packet['tree'] == tree and packet['assets'] == assets,
                 'reorganization changed full metadata')
        _require([row for row in packet['chunks'] if row['kind'] in frozen] ==
                 [row for row in old['chunks'] if row['kind'] in frozen], 'reorganization changed original learning/body rows')
        _require(all((old_root / name).read_bytes() == (root / name).read_bytes() for name in copied),
                 'reorganization changed original text, sources or image bytes')
        checks = []
        for role, records in RagPublisher(None).records(packet).items():
            inputs = [RagPublisher.render(task, packet, record) for record in records]
            values = counter.count_many(inputs) if inputs else []
            _require(len(values) == len(inputs), 'exact token counter response invalid')
            for record, text, counts in zip(records, inputs, values):
                _require(counts['model_tokens'] <= budget.model_budget and counts['cl100k_tokens'] <= budget.cl_limit,
                         'final embedding input exceeds unchanged token budget')
                checks.append(dict(record_id=record[0], role=role, input_sha256=_sha(text.encode()), **counts))
        for route in packet['routes']:
            for role, records in RagPublisher(None).records(packet).items():
                if records:
                    _require(budget.fits(RagPublisher.document_names(task, packet, route['code'], role)[1]),
                             'embedding document title exceeds token budget')
        fresh_report = copy.deepcopy(report)
        fresh_report.update(publication_version=PUBLICATION_VERSION, manifest_sha256=packet['manifest_sha256'],
            retrieval_organization=dict(chapter_depth=2, chapter_nodes=len(tree.get('nodes', [])),
                chapter_groups=len(groups), chart_projections=chart_audit),
            reuse=dict(schema='preextract-sealed-reuse/1', source_manifest_sha256=old['manifest_sha256'],
                source_task=copy.deepcopy(prepared['task']), frozen_chunk_rows=True, image_bytes_unchanged=True,
                profile_and_tree_preserved=True, source_map_bytes_unchanged=True),
            embedding=dict(admission_verified=True, counter_identity=getattr(counter, 'identity', None),
                model_budget=budget.model_budget, cl_limit=budget.cl_limit, records=checks))
        return dict(package_path=str(root), task=task, validation=fresh_report, packet=packet,
                    paperflow_root=prepared['paperflow_root'], catalog=copy.deepcopy(prepared['catalog']))
    except BaseException:
        (root / 'READY.json').unlink(missing_ok=True)
        raise
