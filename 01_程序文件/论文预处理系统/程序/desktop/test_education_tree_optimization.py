import copy

from education_tree_optimization import graph_only_group, graph_only_pipeline, title_only_group


def test_graph_only_group_keeps_existing_graph_configuration():
    graph = {'name': '研究路径', 'kind': 'knowledge_graph', 'config': {'fields': ['a']}}
    source = {'name': '原组', 'templates': [
        {'name': '章节', 'kind': 'page_index', 'config': {'fields': ['title', 'fact']}}, graph]}
    result = graph_only_group(source)
    assert result['templates'] == [graph]
    assert result['templates'][0] is not graph
    assert source['templates'][0]['kind'] == 'page_index'


def test_graph_only_pipeline_changes_only_structure_group():
    source = {'dsl': {'components': {
        'Compiler:PaperStructure': {'obj': {'params': {'compilation_template_group_id': 'old', 'x': 2}}},
        'Extractor:EducationAnnotation': {'obj': {'params': {'model': 'deepseek-flash'}}},
        'Compiler:PaperWiki': {'obj': {'params': {'compilation_template_group_id': 'wiki-id'}}},
    }, 'graph': {'nodes': [{'id': 'Compiler:PaperStructure', 'data': {'form': {
        'compilation_template_group_id': 'old', 'x': 2}}}]}}}
    original = copy.deepcopy(source)
    result = graph_only_pipeline(source, 'new')
    assert result['components']['Compiler:PaperStructure']['obj']['params']['compilation_template_group_id'] == 'new'
    assert result['graph']['nodes'][0]['data']['form']['compilation_template_group_id'] == 'new'
    assert result['components']['Extractor:EducationAnnotation'] == original['dsl']['components']['Extractor:EducationAnnotation']
    assert result['components']['Compiler:PaperWiki'] == original['dsl']['components']['Compiler:PaperWiki']
    assert source == original


def test_title_only_group_preserves_native_page_index_and_graph():
    graph = {'name': '研究路径', 'kind': 'knowledge_graph', 'config': {'fields': ['a']}}
    page = {'name': '章节', 'kind': 'page_index', 'config': {
        'entity': {'description': 'old', 'fields': [
            {'type': 'title', 'description': 'old', 'rule': 'old'},
            {'type': 'fact', 'description': 'old', 'rule': 'old'},
            {'type': 'conclusion', 'description': 'old', 'rule': 'old'}]},
        'relation': {'description': 'old', 'fields': [
            {'type': 'include', 'description': 'old', 'rule': 'old'}]},
        'global_rules': 'old', 'rechunk': False}}
    source = {'templates': [page, graph]}
    result = title_only_group(source)
    assert [t['kind'] for t in result['templates']] == ['page_index', 'knowledge_graph']
    assert [f['type'] for f in result['templates'][0]['config']['entity']['fields']] == ['title']
    assert [f['type'] for f in result['templates'][0]['config']['relation']['fields']] == ['include']
    assert result['templates'][0]['config']['rechunk'] is False
    assert result['templates'][1]['config'] == graph['config']
    assert source['templates'][0]['config']['entity']['fields'][1]['type'] == 'fact'
