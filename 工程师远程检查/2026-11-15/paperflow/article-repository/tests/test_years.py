import copy

from fastapi.testclient import TestClient
import pytest

from test_catalog import catalog, record


def dated(folder, year, category='engineering_system'):
    value = record(folder, '机械系统设计' + folder)
    value['profile']['article']['year'] = year
    value['profile']['research_profile']['research_type'] = category
    value['source_sha256'] = 'a' * 64
    return value


def test_year_filter_combines_with_category_and_excludes_other_entry_kinds(catalog):
    first = catalog.register(dated('one', '2022'))
    catalog.register(dated('two', '2023'))
    catalog.register(dated('three', None))
    other = dated('attachment', '2020'); other['entry_kind'] = 'attachment'
    catalog.register(other)
    result = catalog.search(year='2022', research_type='engineering_system', major='01')
    assert result['total'] == 1
    assert result['rows'][0]['id'] == first['id']
    assert result['rows'][0]['year'] == '2022'
    assert catalog.search(year='2022', research_type='creative_design')['total'] == 0
    assert catalog.search(year='unclassified')['total'] == 1
    assert catalog.years() == ['2023', '2022']


@pytest.mark.parametrize('year', ['22', '2022 OR 1=1', '3000', '2022.0'])
def test_invalid_year_query_rejected(catalog, year):
    with pytest.raises(ValueError, match='年份'):
        catalog.search(year=year)


def test_same_body_refresh_keeps_year_and_type_but_changed_body_drops_both(catalog):
    source = dated('one', '2022')
    item = catalog.register(source)
    incoming = copy.deepcopy(source)
    incoming['profile']['article'].pop('year')
    incoming['profile']['research_profile'].pop('research_type')
    saved = catalog.register(incoming)
    assert saved['year'] == '2022'
    assert saved['research_type'] == 'engineering_system'
    assert saved['revision'] == item['revision']
    incoming['source_sha256'] = 'b' * 64
    saved = catalog.register(incoming)
    assert saved['year'] is None
    assert saved['research_type'] is None


def test_legacy_profile_retains_year_with_no_hash_and_keeps_explicit_new_year(catalog):
    source = dated('legacy', '2022'); source.pop('source_sha256')
    item = catalog.register(source)
    incoming = copy.deepcopy(source)
    incoming['profile']['article'].pop('year')
    incoming['profile']['research_profile'].pop('research_type')
    assert catalog.register(incoming)['revision'] == item['revision']
    incoming['profile']['article']['year'] = '2023'
    assert catalog.register(incoming)['year'] == '2023'


def test_api_exposes_year_filter_and_options(catalog, tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    catalog.register(dated('one', '2022'))
    catalog.register(dated('two', None))
    client = TestClient(create_app(catalog, Files(str(tmp_path)), object()))
    result = client.get('/api/library/articles', params={'year': '2022'})
    assert result.status_code == 200
    assert result.json()['total'] == 1
    assert result.json()['rows'][0]['year'] == '2022'
    assert client.get('/api/library/years').json() == ['2022']
    assert client.get('/api/library/articles', params={'year': 'invalid'}).status_code == 422


def test_invalid_saved_year_is_unclassified(catalog):
    for i, year in enumerate(['unknown', '3000', True, [], None]):
        catalog.register(dated(str(i), year))
    assert catalog.search(year='unclassified')['total'] == 5
    assert catalog.years() == []


def test_future_import_gets_one_year_without_changing_learning_units(catalog):
    source = record('某同学_2018511999')
    before = copy.deepcopy(source['profile']['learning_units'])
    saved = catalog.register(source)
    assert saved['year'] == '2022'
    assert catalog.raw(saved['id'])['base']['profile']['learning_units'] == before
    assert 'year' not in saved['fields']


def test_explicit_unknown_year_is_preserved_on_future_import_and_refresh(catalog):
    source = record('某同学_2018511999')
    source['profile']['article']['year'] = None
    saved = catalog.register(source)
    assert saved['year'] is None
    source['profile']['article'].pop('year')
    assert catalog.register(source)['year'] is None
