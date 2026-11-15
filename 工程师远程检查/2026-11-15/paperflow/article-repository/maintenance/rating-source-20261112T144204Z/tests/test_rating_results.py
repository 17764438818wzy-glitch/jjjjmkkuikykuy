import copy,json
import pytest
from test_ratings import body


def rating():return body({'revision':1})['rating']


def test_program_derives_scores_from_unchanged_item_levels():
    from rating_results import normalize_result
    value=rating();value.pop('score');value['dimension_scores'][1]['score']=99
    raw=json.dumps(value,ensure_ascii=False,separators=(',',':'));before=copy.deepcopy(value)
    result,audit=normalize_result(raw,'engineering_system')
    assert result['score']==80 and result['dimension_scores'][1]['score']==24
    assert result['tier']==before['tier'] and result['evidence_refs']==before['evidence_refs']
    assert [[s['level'] for s in d['subitems']] for d in result['dimension_scores']]==[[s['level'] for s in d['subitems']] for d in before['dimension_scores']]
    assert audit['arithmetic_changes'] and audit['semantic_values_changed'] is False


def test_keyed_dimensions_are_losslessly_ordered_by_approved_ids():
    from rating_results import normalize_result
    value=rating();value['dimension_scores']={d['id']:d for d in value['dimension_scores']}
    result,audit=normalize_result(json.dumps(value),'engineering_system')
    assert [d['id'] for d in result['dimension_scores']]==list('ABCDE')
    assert result['score']==80 and audit['dimension_container_normalized']


def test_existing_parser_repairs_only_missing_closing_quotes_for_known_ids():
    from rating_results import normalize_result
    raw=json.dumps(rating(),ensure_ascii=False,separators=(',',':'))
    for sid in ('A4','B4','C3','C4','D3'):raw=raw.replace('"id":"'+sid+'"','"id":"'+sid)
    result,audit=normalize_result(raw,'engineering_system')
    assert result['score']==80 and len(audit['syntax_edits'])==5


def test_parser_cannot_invent_an_item_level():
    from rating_results import normalize_result
    raw=json.dumps(rating(),ensure_ascii=False,separators=(',',':')).replace('"level":4','"level":four',1)
    with pytest.raises(ValueError):normalize_result(raw,'engineering_system')


def test_duplicate_dimension_or_scalar_key_is_not_silently_dropped():
    from rating_results import normalize_result
    raw=json.dumps(rating(),separators=(',',':')).replace('"level":4','"level":4,"level":5',1)
    with pytest.raises(ValueError):normalize_result(raw,'engineering_system')
