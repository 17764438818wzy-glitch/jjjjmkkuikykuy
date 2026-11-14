from agent_client import CONTRACTS, contract_for


def test_simple_repair_contract_is_short_and_has_no_specialized_rules():
    prompt = contract_for('repair', {'page': 1, 'known_blocks': []})
    # Keep scoping useful without meeting a character cap by dropping required rules.
    assert len(prompt) < len(CONTRACTS['repair']) / 2
    assert 'table_candidate' not in prompt
    assert 'native_data_available' not in prompt
    assert 'graphic_object' not in prompt
    assert 'page_masks' not in prompt


def test_repair_contract_adds_only_rules_for_present_object_types():
    prompt = contract_for('repair', {
        'pages': [2, 3],
        'issues': [{'code': 'merged_cell_content'}],
        'known_blocks': [{
            'table_candidate': True,
            'native_data_available': True,
            'graphic_object': True,
            'graphic_members': [{'id': 'm1'}],
            'required_pages': [2],
            'member_page_options': [[2, 3]],
        }],
    })
    assert CONTRACTS['repair'] in prompt
    assert len(prompt) < len(CONTRACTS['repair']) + 200
    for marker in ('table_candidate', 'structured_table', 'native_data_available',
                   'graphic_object', 'graphic_members', 'page_masks',
                   'replace_table'):
        assert marker in prompt


def test_graphic_repair_contract_does_not_repeat_connectors_or_same_region():
    prompt = contract_for('repair', {
        'page': 1,
        'issues': [{'code': 'embedded_object_review'}],
        'known_blocks': [{'id': 'g1', 'graphic_object': True, 'graphic_region': True}],
    })
    assert '连接箭头' in prompt and '只返回一项 image' in prompt


def test_nonvisual_repair_does_not_receive_visual_knowledge_rules():
    prompt = contract_for('repair', {
        'page': 1,
        'issues': [{'code': 'formula_review'}],
        'known_blocks': [{'id': 'f1', 'type': 'text'}],
    })
    assert len(prompt) < len(CONTRACTS['repair']) / 2
    assert 'chart_spec' not in prompt
    assert 'structured_table' not in prompt
