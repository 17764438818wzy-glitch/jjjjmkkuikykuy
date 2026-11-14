"""Keep saved answers reusable while fixing local scope and intake decisions."""
import copy
import json
from pathlib import Path

import pytest

from preextract_contract import validate_candidate
from test_preextract_contract import fixture
from test_preextract_workbench import runner


def _partition():
    candidate, subset, catalog = fixture()
    whole = copy.deepcopy(subset)
    whole['blocks'].append(dict(id='b2', text='另一章的真实材料', section_id='s2'))
    whole['sections'].append(dict(id='s2', title='下一章', order=2, parent_id=None))
    whole['assets'] = [dict(id='page_0001', kind='source_page'), dict(id='page_0002', kind='source_page')]
    return candidate, subset, whole, catalog


def _validate(candidate, subset, whole, catalog):
    return validate_candidate(candidate, subset, catalog, redaction_policy='best_effort',
                              admission_policy='learning', partition_material=whole)


def test_other_part_unread_ids_remain_notes_without_becoming_read_evidence():
    c, subset, whole, catalog = _partition()
    c['source']['reading_scope']['unread_refs'] = ['s2', 'page_0001..page_0002']
    original = copy.deepcopy((c, subset, whole))
    result = _validate(c, subset, whole, catalog)
    assert not [e for e in result['errors'] if e['code'] == 'source_reference']
    scope = result['candidate']['source']['reading_scope']
    assert not {'s2','page_0001','page_0002'} & set(scope['unread_refs'] + scope['read_refs'])
    assert 's2' in scope['notes'] and 'page_0001..page_0002' in scope['notes']
    assert (c, subset, whole) == original


@pytest.mark.parametrize('injected', ['read', 'learning', 'unknown_unread'])
def test_partition_context_does_not_authorize_unprovided_research(injected):
    c, subset, whole, catalog = _partition()
    if injected == 'read':
        c['source']['reading_scope']['read_refs'].append('s2')
    elif injected == 'learning':
        c['profile']['learning_units'][0]['source_refs'][0].update(block_id='b2', section_id='s2')
    else:
        c['source']['reading_scope']['unread_refs'] = ['page_9999']
    result = _validate(c, subset, whole, catalog)
    scope = result['candidate']['source']['reading_scope']
    if injected == 'learning':
        assert not result['valid']
        assert any(e['code'] == 'learning_unusable' for e in result['errors'])
    else:
        assert ('s2' if injected == 'read' else 'page_9999') not in scope['read_refs'] + scope['unread_refs']
        assert any(e['code'] == 'learning_limit_source_reference' for e in result['warnings'])


def test_supplied_context_block_keeps_its_real_chapter_location():
    c, subset, whole, catalog = _partition()
    subset['blocks'].append(dict(whole['blocks'][-1], reading_role='context'))
    ref = dict(c['profile']['article']['source_refs'][0], block_id='b2', section_id='s2', start=0, end=3)
    c['gaps'] = [dict(id='G1', type='not_read', description='下一章仅有背景标题，详细材料在后段',
                     impact='本段不处理后续研究', use_limit='不能据标题补充结果', source_refs=[ref])]
    result = _validate(c, subset, whole, catalog)
    assert not [e for e in result['errors'] if e['code'] == 'source_reference']
    assert result['candidate']['gaps'][0]['source_refs'][0]['section_id'] == 's2'


def test_partition_reference_context_requires_same_original():
    c, subset, whole, catalog = _partition()
    whole['source']['source_sha256'] = 'b' * 64
    with pytest.raises(ValueError, match='来源|identity'):
        _validate(c, subset, whole, catalog)


def _artifact(path, *, marked=True):
    candidate, _, _ = fixture()
    payload = json.dumps(candidate, ensure_ascii=False)
    text = '# 论文预提取候选\n\n<!-- paper-preextract:data:start -->\n```json\n' + payload + '\n```\n<!-- paper-preextract:data:end -->' if marked else payload
    path.write_text(text, encoding='utf8')
    return path


@pytest.mark.parametrize('marked', [False, True])
def test_scan_skips_structured_artifact_but_keeps_ordinary_markdown(tmp_path, marked):
    from engine import scan_sources
    _artifact(tmp_path / '以前的提取结果.md', marked=marked)
    ordinary = tmp_path / '论文预提取.md'
    ordinary.write_text('# 研究论文\n这里是原始研究正文，不是程序生成的候选。', encoding='utf8')
    (tmp_path / 'paper.txt').write_text('原始研究正文', encoding='utf8')
    assert {Path(r['path']).name for r in scan_sources(tmp_path)} == {'论文预提取.md', 'paper.txt'}


def test_selected_old_artifact_never_enters_model_pipeline(runner, tmp_path, monkeypatch):
    import preextract_workbench
    from processing_outcomes import ai_outcome
    proc, _ = runner
    source = _artifact(tmp_path / 'result.md')
    monkeypatch.setattr(preextract_workbench, 'run_single', lambda *a, **k: pytest.fail('intermediate output must not be submitted'))
    result = proc.run([dict(path=str(source), name=source.name)])
    item = result['items'][0]
    assert ai_outcome(item, result['status'])[0] == '未进入 AI'
    assert '中间' in item['reason'] and not item['automatic_approval']


def test_old_artifact_is_not_offered_as_an_ai_retry(tmp_path):
    from ai_resume import retry_records
    source = _artifact(tmp_path / 'result.md')
    record = dict(source_path=str(source), ai_result=dict(status='call_failed', reason='分段结构或来源校验失败'))
    assert retry_records([record]) == []


def test_saved_answer_validation_has_its_own_label_and_retry_eligibility(runner, monkeypatch):
    import preextract_workbench
    from ai_resume import needs_ai_resume
    from processing_outcomes import ai_outcome
    proc, source = runner
    def fail(*a, **kw):
        work = Path(kw['work_dir']); work.mkdir(parents=True)
        (work / 'material.json').write_text('{}', encoding='utf8')
        kw['progress']('material_prepared'); kw['progress']('extracting_parts')
        raise ValueError('分段结构或来源校验失败，原响应及其他分段保留')
    monkeypatch.setattr(preextract_workbench, 'run_single', fail)
    result = proc.run([dict(path=str(source), name=source.name)])
    item = result['items'][0]
    assert item['ai_result']['status'] == 'validation_failed'
    assert ai_outcome(item, result['status'])[0] == '结果校验失败'
    assert needs_ai_resume(item)


def test_historical_validation_label_changes_without_rewriting_its_record():
    from processing_outcomes import ai_outcome
    item = dict(content_format='paper_preextract', ai_result=dict(status='call_failed',
        reason='分段结构或来源校验失败，原响应及其他分段保留', stage='完整 MD 提取'))
    before = copy.deepcopy(item)
    assert ai_outcome(item, 'completed')[0] == '结果校验失败'
    assert item == before


def test_http_400_keeps_request_failure_label_and_saved_request_barrier(tmp_path):
    from preextract_partition_run import _saved_reply
    from processing_outcomes import ai_outcome
    (tmp_path / 'request-state.json').write_text(json.dumps(dict(status='request_failed_or_uncertain', http_status=400)), encoding='utf8')
    with pytest.raises(RuntimeError, match='不自动重发'):
        _saved_reply(tmp_path, 'unused')
    item = dict(content_format='paper_preextract', ai_result=dict(status='call_failed', reason='模型请求返回 HTTP 400'))
    assert ai_outcome(item, 'completed')[0] == '调用失败'
