from pathlib import Path
from urllib.parse import unquote

import pytest

from auto_view import build_view
from engine import Store
from human_view import LINK, _rebase


def test_existing_numeric_resource_takes_precedence_over_citation_shape(tmp_path):
    (tmp_path / '2024').write_text('real evidence', encoding='utf-8')
    issues = []
    result = _rebase('作者[2](2024)提出', tmp_path / 'document.md',
                     tmp_path / 'view' / 'document.md', tmp_path, issues)
    assert '[2](<../2024>)' in result
    assert issues == []


def test_numbered_citations_with_parenthesized_years_are_not_missing_links(tmp_path):
    source = tmp_path / 'document.md'
    target = tmp_path / 'view' / 'document.md'
    body = ('Alexander Wall[2](1919)的比率分析理论。\n'
            'Robert Kaplan和David Norton[7](1992)提出平衡计分卡理论。')
    issues = []

    rebased = _rebase(body, source, target, tmp_path, issues)

    assert issues == []
    assert rebased.replace(r'\[', '[').replace(r'\]', ']') == body


def test_numbered_label_with_a_non_year_target_is_still_validated(tmp_path):
    issues = []

    _rebase('[2](missing-file)', tmp_path / 'document.md',
            tmp_path / 'view.md', tmp_path, issues)

    assert issues == ['图片或证据链接不可用：本地资源缺失']


def test_standalone_numeric_year_link_is_still_validated_when_missing(tmp_path):
    issues = []

    _rebase('[2](2024)', tmp_path / 'document.md',
            tmp_path / 'view.md', tmp_path, issues)

    assert issues == ['图片或证据链接不可用：本地资源缺失']


@pytest.mark.parametrize('body', [
    '[2](2024)',
    '![2](2024)',
    '[2](<2024>)',
    '[2](./2024)',
])
def test_existing_numeric_and_explicit_file_links_are_rebased(tmp_path, body):
    source = tmp_path / 'document.md'
    target = tmp_path / 'view' / 'document.md'
    asset = tmp_path / '2024'
    asset.write_text('published evidence', encoding='utf-8')
    issues = []

    rebased = _rebase(body, source, target, tmp_path, issues)

    match = LINK.search(rebased)
    assert issues == []
    assert match is not None
    linked = Path(unquote(match['target'].strip('<>')))
    assert (target.parent / linked).resolve() == asset.resolve()


def test_automatic_view_keeps_author_year_citations_in_the_passed_document(tmp_path):
    store = Store(tmp_path / 'data')
    run_id = 'AUTO_20260921_browse_link_105'
    package = store.runs / run_id / 'packages' / 'paper'
    package.mkdir(parents=True)
    body = ('Alexander Wall[2](1919)的比率分析理论。\n'
            'Robert Kaplan和David Norton[7](1992)提出平衡计分卡理论。')
    (package / 'document.md').write_text(body, encoding='utf-8')
    (package / 'ingest.md').write_text(body, encoding='utf-8')
    manifest = {
        'run_id': run_id,
        'pipeline': 'automatic',
        'status': 'completed',
        'started_at': '2026-09-21',
        'events': [],
        'items': [{
            'id': 'AUTO_browse_link_105',
            'title': '企业财务报表分析研究',
            'major': '财务管理',
            'status': 'pass',
            'automatic_approval': True,
            'reason': '',
            'package_path': str(package),
            'path': str(package / 'ingest.md'),
            'display_path': str(package / 'document.md'),
        }],
    }
    store.save(manifest)

    result = build_view(store, manifest)

    document = next((Path(result['path']) / '通过').rglob('*.md'))
    rendered = document.read_text(encoding='utf-8')
    assert result['counts'] == {'通过': 1, '不通过': 0}
    assert result['issues'] == []
    assert rendered.replace(r'\[', '[').replace(r'\]', ']').endswith(body)
