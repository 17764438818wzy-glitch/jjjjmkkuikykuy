"""Required output fields and historical rules must survive prompt scoping."""
import pytest

from agent_client import CONTRACTS, contract_for


def all_context():
    return {'pages': [1, 2], 'issues': [{'code': 'merged_cell_content'}],
            'known_blocks': [{'type': 'image', 'table_candidate': True,
                              'native_data_available': True, 'graphic_object': True,
                              'required_pages': [1, 2], 'member_page_options': [[1, 2]]}]}


@pytest.mark.parametrize('payload', [{}, {'page': 1, 'known_blocks': [{'type': 'paragraph'}]}, all_context()])
def test_every_repair_scope_teaches_fields_required_by_consumer(payload):
    prompt = contract_for('repair', payload)
    for required in ('text或table或image', 'description', 'bbox', '不能', 'sensitive_boxes'):
        assert required in prompt
    assert '二进制' in prompt and '本地' in prompt


def test_maximal_scope_preserves_every_sentence_of_historical_contract():
    prompt = contract_for('repair', all_context())
    # Sentence-level comparison catches missing safety/format rules, not just keywords.
    for sentence in CONTRACTS['repair'].split('。'):
        if sentence.strip():
            assert sentence.strip() in prompt


@pytest.mark.parametrize('flag', ['table_candidate', 'native_data_available', 'graphic_object'])
def test_visual_scopes_receive_full_optional_structure_schema(flag):
    prompt = contract_for('repair', {'page': 1, 'known_blocks': [{flag: True}]})
    for marker in ('"chart_type"', '"x_axis"', '"y_axes"', '"series"',
                   '"rows"', '"header_rows"', '"units"', '"notes"'):
        assert marker in prompt


def test_multiple_pages_receive_explicit_mask_structure():
    assert '"boxes":[]' in contract_for('repair', {'pages': [1, 2]})


@pytest.mark.parametrize('block', [{'type': 'image'}, {'type': 'table'}, {'graphic_region': [0, 0, 1, 1]}])
def test_unflagged_visual_blocks_still_get_visual_rules(block):
    assert 'chart_spec' in contract_for('repair', {'page': 1, 'known_blocks': [block]})
