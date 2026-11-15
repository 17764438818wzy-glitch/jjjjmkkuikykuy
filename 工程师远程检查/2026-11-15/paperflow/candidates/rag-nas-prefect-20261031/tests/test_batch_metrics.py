import json

from batch_metrics import summarize


def test_missing_observations_do_not_become_zero_requests(tmp_path):
    result = summarize(tmp_path)
    assert result['groups_with_observations'] == 0
    assert result['native_http_attempts'] is None and result['native_http_peak'] is None


def test_overlapping_http_attempts_are_counted_across_groups(tmp_path):
    for group, rows in {
        'one': [{'document_id': 'a', 'started_at_ns': 1, 'ended_at_ns': 5, 'outcome': 'returned'}],
        'two': [{'document_id': 'b', 'started_at_ns': 3, 'ended_at_ns': 7, 'outcome': 'error'},
                {'document_id': 'c', 'started_at_ns': 7, 'ended_at_ns': 9, 'outcome': 'returned'}],
    }.items():
        directory = tmp_path / group
        directory.mkdir()
        (directory / 'native-observations.json').write_text(json.dumps({'requests': rows, 'documents': []}),
                                                              encoding='utf8')
    result = summarize(tmp_path)
    assert (result['groups_with_observations'], result['native_http_attempts'], result['native_http_peak']) == (2, 3, 2)
    assert (result['native_http_returned'], result['native_http_errors']) == (2, 1)
