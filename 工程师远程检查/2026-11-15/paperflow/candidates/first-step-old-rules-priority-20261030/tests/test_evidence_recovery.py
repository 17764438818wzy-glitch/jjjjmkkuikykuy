import pytest

from paperflow_prefect import attachment_evidence_handoff as evidence_module
from paperflow_prefect.group_classification import process_one_group


def test_unknown_filename_title_does_not_discard_confirmed_cover_title(tmp_path):
    from paperflow_prefect.first_step_control import UnknownRequest
    from paperflow_prefect.group_classification import classify_group
    library = tmp_path / 'library'
    group = library / 'person'
    group.mkdir(parents=True)
    (group / '123_合成论文研究题目.txt').write_text('题目：合成论文研究题目\n摘要：示例', encoding='utf8')
    class Judge:
        def choose_body(self, records): return {'choice': records[0]['file_id']}
        def choose_filename_title(self, filename): raise UnknownRequest('synthetic timeout')
    def reader(path):
        return {'format': 'text', 'text': path.read_text(encoding='utf8'), 'truncated': False}
    result = classify_group(group, library, Judge(), reader,
        title_reader=lambda row, evidence: ('合成论文研究题目', '正文文本开头', '合成论文研究题目', True))
    assert result['paper_title'] == '合成论文研究题目'
    assert result['ready_for_receipt'] is True
    assert '未确认' in result['filename_title_note']


def test_completed_handoff_recovers_missing_evidence_without_repeating_model(tmp_path, monkeypatch):
    library = tmp_path / 'library'
    group = library / 'person'
    group.mkdir(parents=True)
    (group / 'paper.txt').write_text('题目：合成论文\n摘要：示例', encoding='utf8')
    (group / 'data.csv').write_text('date,value\n1,2', encoding='utf8')
    output = tmp_path / 'output'
    saved = {}

    class Judge:
        def choose_body(self, records):
            return {'choice': next(row['file_id'] for row in records if row['relative'] == 'paper.txt')}
        def choose_other(self, record, body_id):
            return {'choice': 'attachment', 'model': 'synthetic', 'confidence': 1,
                    'probabilities': {'attachment': 1, 'trash': 0, 'uncertain': 0}}

    def reader(path):
        return {'format': 'text', 'text': path.read_text(encoding='utf8'), 'truncated': False}

    def save_capture(snapshot, version, capture):
        saved[(snapshot, version)] = capture

    def load_capture(snapshot, version):
        return saved.get((snapshot, version))

    original = evidence_module.append_evidence_handoff
    def crash(*args, **kwargs):
        raise SystemExit('synthetic process exit after handoff')
    monkeypatch.setattr(evidence_module, 'append_evidence_handoff', crash)
    with pytest.raises(SystemExit):
        process_one_group(group, library, output, lambda *_: Judge(), reader, durable_journal=True,
                          capture_saver=save_capture, capture_loader=load_capture)
    assert saved
    monkeypatch.setattr(evidence_module, 'append_evidence_handoff', original)
    def no_model(*args):
        pytest.fail('recovery must not issue another model request')
    result = process_one_group(group, library, output, no_model, reader, durable_journal=True,
                               capture_saver=save_capture, capture_loader=load_capture)
    assert result['reused'] is True
    assert result['evidence_errors'] == []
    assert result['delivery_complete'] is True
