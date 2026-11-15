"""Terminal punctuation on a paired cover signature must not expose identities."""
import copy

import pytest

from agent_client import AgentResultError
from frontmatter_privacy import _name, redact_frontmatter_blocks
from response_contracts import validate_privacy_spans


def paragraph(text, **extra):
    return {'id': 'p', 'type': 'paragraph', 'text': text,
            'position': {'part': 'word/document.xml', 'body_index': 3}, **extra}


@pytest.mark.parametrize('space', [' ', '\u00a0'])
@pytest.mark.parametrize('newline', ['\n', '\r\n'])
def test_frontmatter_paired_roles_with_terminal_period_mask_only_signature_line(space, newline):
    title = 'Effects of Reading Instruction: 28% remains 28%'
    context = 'Department of Education; study years 2019–2023'
    signature = f'        Author:{space}Ming Wu   Instructor:{space}Qian{space}Zhao.'
    text = newline.join([title, signature, context])
    meta = {'title': title, 'blocks': [paragraph(text, locator_texts=[text])],
            'resources': [], 'formulas': []}
    assert redact_frontmatter_blocks(meta) == 1
    expected = newline.join([title, '[已脱敏]', context])
    assert meta['blocks'][0]['text'] == expected
    assert meta['blocks'][0]['locator_texts'] == [expected]
    assert meta['title'] == title


@pytest.mark.parametrize('heading', ['Introduction', 'References', '文献综述'])
def test_identical_roles_in_body_or_references_are_not_cover_fields(heading):
    text = 'Author:\u00a0Ming Wu   Instructor:\u00a0Qian\u00a0Zhao.'
    meta = {'blocks': [paragraph(heading), paragraph(text)], 'resources': [], 'formulas': []}
    original = copy.deepcopy(meta)
    assert redact_frontmatter_blocks(meta) == 0
    assert meta == original


@pytest.mark.parametrize('text', [
    'Instructor:\u00a0Qian\u00a0Zhao.',
    'Author:\u00a0Ming Wu.',
    'Author:\u00a0Ming Wu   Instructor:.',
    'The title discusses Author: Ming Wu Instructor: Qian Zhao.',
    '“Author: Ming Wu Instructor: Qian Zhao.”',
    'Author: Ming Wu Instructor: Qian Zhao reports.',
    'Author: 2019 Instructor: 28.50.',
])
def test_ambiguous_prose_quotes_and_single_fields_stay_for_exact_review(text):
    meta = {'blocks': [paragraph(text)], 'resources': [], 'formulas': []}
    assert redact_frontmatter_blocks(meta) == 0
    assert meta['blocks'][0]['text'] == text


def test_mask_does_not_remove_same_name_from_public_research_text():
    signature = 'Author:\u00a0Ming Wu   Instructor:\u00a0Qian\u00a0Zhao.'
    citation = 'Qian Zhao developed the cited method; measured accuracy was 28%.'
    meta = {'blocks': [paragraph(signature), paragraph('Introduction'), paragraph(citation)]}
    assert redact_frontmatter_blocks(meta) == 1
    assert meta['blocks'][-1]['text'] == citation


def test_general_name_policy_and_model_exact_whitespace_contract_stay_strict():
    assert _name('Qian Zhao.') is False
    payload = {'text': 'Author:\u00a0Ming Wu'}
    result = {'decision': 'pass', 'sensitive_strings': ['Author: Ming Wu']}
    with pytest.raises(AgentResultError, match='脱敏位置不匹配'):
        validate_privacy_spans(result, payload)
    validate_privacy_spans({**result, 'sensitive_strings': ['Author:\u00a0Ming Wu']}, payload)
