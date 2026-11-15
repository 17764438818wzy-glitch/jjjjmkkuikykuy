"""Preserve model values when closing incomplete JSON containers."""
import pytest

from preextract_contract import parse_candidate


def test_missing_tail_closer_preserves_both_duplicate_source_references():
    raw = ('{"source_refs":[{"block_id":"b1"}],'
           '"source_refs":[{"block_id":"b2"}]')
    repairs = []
    result = parse_candidate(raw, repairs=repairs)
    assert result['source_refs'] == [{'block_id': 'b1'}, {'block_id': 'b2'}]
    assert any(row['policy'] == 'merge_duplicate_source_refs' for row in repairs)
    assert any(row['policy'] == 'close_unclosed_containers' and
               row['scalar_sequence_preserved'] is True for row in repairs)


def test_multiple_missing_tail_closers_ignore_brackets_inside_escaped_strings():
    raw = ('{"items":[{"text":"literal } [ and \\\"quote\\\"",'
           '"source_refs":[{"block_id":"b1"}],"source_refs":[{"block_id":"b2"}]')
    result = parse_candidate(raw)
    assert result['items'][0]['text'] == 'literal } [ and "quote"'
    assert result['items'][0]['source_refs'] == [{'block_id': 'b1'}, {'block_id': 'b2'}]


def test_closing_suffix_does_not_also_edit_an_internal_trailing_comma():
    raw = ('{"source_refs":[{"block_id":"b1"},],'
           '"source_refs":[{"block_id":"b2"}]')
    with pytest.raises(ValueError):
        parse_candidate(raw)


@pytest.mark.parametrize('raw', [
    '{"title":"A","title":"B"',
    '{"source_refs":[{"block_id":"b1"}],"source_refs":[{"block_id":"b2"}],'
    '"target_pointer":"/source_refs/0/block_id"',
    '{"source_refs":[{"block_id":"b1"}],"source_refs":[{"block_id":"b2"}],"value":',
    '{"source_refs":[{"block_id":"b1"}],"source_refs":[{"block_id":"b2"}],"text":"unfinished',
    '{"source_refs":[{"block_id":"b1"}],"source_refs":[{"block_id":"b2"}],"value":[1}',
])
def test_invalid_values_conflicts_or_ambiguous_references_stay_rejected(raw):
    with pytest.raises(ValueError):
        parse_candidate(raw)
