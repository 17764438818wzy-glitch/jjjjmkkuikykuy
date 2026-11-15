import hashlib
import json
from pathlib import Path

import pytest
import uuid


def pytest_configure(config):
    if not config.option.basetemp:
        # pytest 只可删除自己的全新目录，不复用另一次运行的 basetemp。
        config.option.basetemp=str(Path(__file__).parent.parent/'.tmp'/('pytest-'+uuid.uuid4().hex))


@pytest.fixture
def package_factory(tmp_path):
    def make(name='r01', *, source_sha='a' * 64, task_id='task1'):
        root = tmp_path / name
        root.mkdir()
        ident = dict(task_id=task_id, source_id='source1', source_sha256=source_sha)
        files = {
            'article.redacted.md': '# 研究方法\n通过指标分解解释盈利能力。',
            'profile.json': dict(**ident, material_disposition='keep', article={'title': '方法范例'},
                                 routing_candidates=[{'code': '120201', 'name': '工商管理', 'reason': '研究方法'}],
                                 learning_units=[{'id': 'u1', 'title': '指标分解', 'steps': ['取得新数据', '计算指标'], 'section_ids': ['s1']}]),
            'tree.json': dict(**ident, nodes=[{'id': 's1', 'title': '研究方法', 'parent_id': None}]),
            'assets.json': dict(**ident, items=[{'id': 'fig1', 'kind': 'image', 'storage': 'description',
                                  'section_id': 's1', 'source_location': {'page': 3},
                                  'description': '通过销售净利率、总资产周转率、权益乘数分解净资产收益率。'}]),
            'chunks.json': dict(**ident, chunks=[{'id': 'body1', 'kind': 'body', 'section_ids': ['s1'],
                                  'asset_ids': [], 'content': '# 研究方法\n通过指标分解解释盈利能力。'},
                                 {'id': 'c1', 'kind': 'learning', 'section_ids': ['s1'],
                                  'asset_ids': ['fig1'], 'content': '指标分解用于分析盈利能力；新写作需要重新获取财务数据。'}]),
            'review.json': dict(**ident, unresolved_issues=[]),
            'corrections.json': dict(**ident, operations=[], final_routing_candidates=[{'code': '120201', 'name': '工商管理'}]),
        }
        manifest = dict(**ident, schema='paperflow/1.0', revision=name, files=[],
                        publication_files=['article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json'])
        for name_, value in files.items():
            data = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
            p = root / name_
            p.write_text(data, encoding='utf-8')
            manifest['files'].append({'path': name_, 'bytes': p.stat().st_size,
                                      'sha256': hashlib.sha256(p.read_bytes()).hexdigest()})
        p = root / 'manifest.json'
        p.write_text(json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
        ready = dict(**ident, manifest='manifest.json', manifest_sha256=hashlib.sha256(p.read_bytes()).hexdigest())
        (root / 'READY.json').write_text(json.dumps(ready), encoding='utf-8')
        return root, ident, {'majors': [{'code': '120201', 'name': '工商管理', 'paper_dataset_id': 'paper-ds', 'image_dataset_id': 'image-ds'}]}
    return make
