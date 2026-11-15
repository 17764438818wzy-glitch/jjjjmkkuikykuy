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


def nested_cap():
    value=rating();value.pop('cap_applied');value.pop('cap_reason')
    for dim in value['dimension_scores']:dim.update(cap_applied=False,cap_reason='')
    return value


def test_explicit_unanimous_false_caps_are_lifted_without_changing_original_fields():
    from rating_results import normalize_result
    value=nested_cap();before=copy.deepcopy(value)
    result,audit=normalize_result(json.dumps(value,ensure_ascii=False),'engineering_system')
    assert result['cap_applied'] is False and result['cap_reason']==''
    assert result['dimension_scores']==before['dimension_scores'] or all(
        a['cap_applied']==b['cap_applied'] and a['cap_reason']==b['cap_reason'] for a,b in zip(result['dimension_scores'],before['dimension_scores']))
    assert result['tier']==value['tier'] and result['brief_reason']==value['brief_reason']
    change=audit['structural_projections'][0]
    assert change['target']=='/cap_applied' and change['value'] is False
    assert change['sources']==['/dimension_scores/'+str(i)+'/cap_applied' for i in range(5)]
    assert audit['semantic_values_changed'] is False


@pytest.mark.parametrize('mutator',[
    lambda v:v['dimension_scores'].pop(),
    lambda v:v['dimension_scores'][0].pop('cap_applied'),
    lambda v:v['dimension_scores'][0].update(cap_applied=True),
    lambda v:v['dimension_scores'][0].update(cap_applied=0),
    lambda v:v['dimension_scores'][0].update(cap_applied='false'),
    lambda v:v['dimension_scores'][0].update(cap_reason='此维有缺失'),
    lambda v:v['dimension_scores'][0].pop('cap_reason'),
    lambda v:v.update(cap_applied=True),
    lambda v:v.update(cap_reason='顶层另有封顶理由'),
    lambda v:v.update(cap_applied=None),
])
def test_cap_projection_rejects_missing_conflicting_or_non_boolean_evidence(mutator):
    from rating_results import normalize_result
    value=nested_cap();mutator(value)
    with pytest.raises(ValueError):normalize_result(json.dumps(value),'engineering_system')


def test_absent_cap_decision_is_not_invented_from_item_levels_or_prose():
    from rating_results import normalize_result
    value=rating();value.pop('cap_applied');value['brief_reason']='不封顶'
    with pytest.raises(ValueError):normalize_result(json.dumps(value),'engineering_system')


def test_missing_dimension_reason_is_projected_from_exact_ordered_subitem_reasons():
    from rating_results import normalize_result
    value=rating();value['dimension_scores'][0].pop('reason')
    value['dimension_scores'][0]['subitems'].reverse()
    before=copy.deepcopy(value)
    result,audit=normalize_result(json.dumps(value,ensure_ascii=False),'engineering_system')
    originals={s['id']:s['reason'] for s in before['dimension_scores'][0]['subitems']}
    assert result['dimension_scores'][0]['reason']=='\n'.join(k+': '+originals[k] for k in ['A1','A2','A3','A4'])
    assert result['dimension_scores'][1]['reason']==before['dimension_scores'][1]['reason']
    change=next(p for p in audit['structural_projections'] if p['target']=='/dimension_scores/0/reason')
    assert change['sources']==['/dimension_scores/0/subitems/'+str(i)+'/reason' for i in [3,2,1,0]]
    assert [s['reason'] for s in result['dimension_scores'][0]['subitems']]==[s['reason'] for s in before['dimension_scores'][0]['subitems']]


@pytest.mark.parametrize('mutator',[
    lambda v:v['dimension_scores'][0].update(reason=''),
    lambda v:v['dimension_scores'][0].update(reason=None),
    lambda v:v['dimension_scores'][0].update(reason=1),
    lambda v:v['dimension_scores'][0]['subitems'][0].pop('reason'),
    lambda v:v['dimension_scores'][0]['subitems'][0].update(reason=''),
    lambda v:v['dimension_scores'][0]['subitems'][0].update(reason=None),
    lambda v:v['dimension_scores'][0]['subitems'][0].update(reason=1),
])
def test_reason_projection_never_overwrites_explicit_empty_or_missing_source(mutator):
    from rating_results import normalize_result
    value=rating();value['dimension_scores'][0].pop('reason');mutator(value)
    with pytest.raises(ValueError):normalize_result(json.dumps(value),'engineering_system')


def test_projection_source_pointers_resolve_in_original_keyed_response():
    from rating_results import normalize_result
    value=nested_cap()
    for dim in value['dimension_scores']:dim.pop('reason')
    value['dimension_scores']={dim['id']:dim for dim in value['dimension_scores']}
    result,audit=normalize_result(json.dumps(value),'engineering_system')
    for change in audit['structural_projections']:
        for pointer in change['sources']:
            current=value
            for part in pointer.lstrip('/').split('/'):current=current[int(part)] if isinstance(current,list) else current[part]
            assert current is False or current=='' or isinstance(current,str)
