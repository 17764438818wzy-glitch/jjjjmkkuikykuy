"""Repair a completed education preprocessing batch without changing its originals.

The already-running GUI process still uses its old in-memory exporter. This
command waits for a terminal batch, creates checked copies, then repoints the
batch manifest one item at a time. A failed copy leaves the original usable.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path

from engine import Store, digest
from evidence import package_fingerprint
from knowledge_package import knowledge_files
from pilot_package_copy import prepare_pilot_copy


def canonicalize_batch(store: Store, run_id: str, *, apply: bool = False) -> dict[str, int]:
    batch = store.load(run_id)
    if not batch or batch.get('run_id') != run_id:
        raise ValueError('批次清单缺失或编号不一致')
    if apply and batch.get('status') != 'completed':
        raise ValueError('批次尚未正常结束；当前批次只可预览，不可改写路径')
    run = store.manifest_path(run_id).parent.resolve()
    originals = run / 'packages'
    copies = run / 'canonical_packages'
    planned = []
    for item in batch.get('items') or []:
        if item.get('status') != 'pass' or item.get('category') != '教育教学类':
            continue
        source = Path(item['package_path']).resolve()
        if not source.is_relative_to(run):
            raise ValueError('知识包路径不在当前批次内：' + item['id'])
        knowledge_files(source)
        package_manifest = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
        if package_manifest.get('document_id') != item['id'] or package_manifest.get('paper_id') != item.get('paper_id'):
            raise ValueError('知识包与批次记录的身份不一致：' + item['id'])
        if digest(source / 'ingest.md') != item.get('reviewed_hash') or package_fingerprint(source) != item.get('reviewed_package_hash'):
            raise ValueError('知识包已变化，不能自动迁移：' + item['id'])
        try:
            knowledge_files(source, require_stable_paper_id=True)
        except ValueError:
            if source.parent != originals:
                raise ValueError('待修复知识包不在原始 packages 目录：' + item['id'])
            if item.get('uploads') or item.get('handoff_status') == 'uploaded':
                raise ValueError('论文已上传，必须先核对远端回执：' + item['id'])
            planned.append((item, source, package_manifest))
    if not apply:
        return {'needs_repair': len(planned), 'repaired': 0}

    repaired = 0
    for item, source, package_manifest in planned:
        destination = copies / item['id']
        if not destination.exists():
            copies.mkdir(exist_ok=True)
            temporary = copies / ('.' + item['id'] + '.' + uuid.uuid4().hex + '.tmp')
            prepare_pilot_copy(source, temporary)
            os.replace(temporary, destination)
        knowledge_files(destination, require_stable_paper_id=True)
        copied_manifest = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
        if copied_manifest['paper_id'] != package_manifest['paper_id'] or copied_manifest['document_id'] != item['id']:
            raise ValueError('规范副本身份不一致：' + item['id'])
        item.update(package_path=str(destination), path=str(destination / 'ingest.md'),
                    display_path=str(destination / 'document.md'),
                    knowledge_manifest_path=str(destination / 'manifest.json'),
                    reviewed_hash=digest(destination / 'ingest.md'),
                    reviewed_package_hash=package_fingerprint(destination))
        store.save(batch)
        repaired += 1
    return {'needs_repair': len(planned), 'repaired': repaired}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='完成批次的教育知识包身份修复')
    parser.add_argument('run_id')
    parser.add_argument('--data-root', type=Path, default=Path(__file__).resolve().parent / 'data')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(canonicalize_batch(Store(args.data_root), args.run_id, apply=args.apply))
