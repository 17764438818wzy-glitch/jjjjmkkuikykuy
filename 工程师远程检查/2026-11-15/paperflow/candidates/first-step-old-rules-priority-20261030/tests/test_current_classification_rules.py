from unittest.mock import patch

import pytest

from paperflow_prefect import group_classification as grouping
from paperflow_prefect.nimble_group import NimbleJudge


class FixtureJudge:
    def choose_body(self, records):
        return {'choice': records[0]['file_id']}

    def choose_other(self, record, body):
        return {'choice': 'attachment'}


def make_group(tmp_path, filename='论文.docx'):
    library = tmp_path / 'library'
    group = library / 'person'
    group.mkdir(parents=True)
    (group / filename).write_bytes(b'synthetic body')
    return library, group


def test_duplicate_group_is_processed_once(tmp_path):
    library, group = make_group(tmp_path)
    results = grouping.classify_batch([group, group], library, tmp_path / 'output', FixtureJudge,
        lambda _: {'text': '本地正文证据', 'limitations': []}, verified_new_groups=[group])
    assert len(results) == 1 and 'handoff' in results[0]


def test_complete_cover_title_does_not_require_a_label(tmp_path):
    library, group = make_group(tmp_path)
    title = '企业绩效管理研究'
    evidence = lambda _: {'text': title + '\n摘要', 'limitations': []}
    title_reader = lambda *_: (title, '正文 Word 开头文本', title, False)
    handoff = grouping.classify_group(group, library, FixtureJudge(), evidence, title_reader=title_reader)
    assert handoff['paper_title'] == title and handoff['title_status'] == '已提取'
    assert grouping.verify_handoff(handoff, evidence_reader=evidence)


def test_filename_mismatch_does_not_override_confirmed_cover(tmp_path):
    library, group = make_group(tmp_path, '001_1908034124_姚聪聪_错误文件标题.docx')
    judge = NimbleJudge()
    judge.choose_body = lambda rows: {'choice': rows[0]['file_id']}
    evidence = lambda _: {'format': 'docx', 'text': '题目：正确的论文题目\n学院：测试学院'}
    title_reader = lambda *_: ('正确的论文题目', '正文 Word 开头文本', '正确的论文题目', True)
    response = {'choice': 'title_01', 'model': judge.model, 'confidence': .9,
                'probabilities': {'title_01': .9, 'none': .1}}
    with patch.object(judge, '_decide', return_value=response) as call:
        handoff = grouping.classify_group(group, library, judge, evidence, title_reader=title_reader)
    assert handoff['paper_title'] == '正确的论文题目' and call.call_count == 1
    assert handoff['filename_title_observation']['excerpt'] == '错误文件标题'
    assert grouping.verify_handoff(handoff, evidence_reader=evidence)


def test_filename_model_response_timeout_is_ten_seconds():
    judge = NimbleJudge(timeout=120)
    observed = []
    def timeout(*_):
        observed.append(judge.timeout)
        raise TimeoutError('synthetic')
    with patch.object(judge, '_decide', side_effect=timeout), pytest.raises(TimeoutError):
        judge.choose_filename_title('001_1908034124_姚聪聪_体育经济与管理_短视频营销现状研究.doc')
    assert observed == [10] and judge.timeout == 120
