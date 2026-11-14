from billing import summarize


def rows():
    return [{'id': str(n), 'usage': {'input_tokens': 10, 'output_tokens': 20,
             'total_tokens': 30, 'cached_tokens': 5}, 'seconds': seconds,
             'cost_cny': None, 'channel': 'primary'} for n, seconds in enumerate([1.5, 2.0, 4.0])]


def test_single_use_iterator_keeps_all_request_counts_tokens_and_time():
    summary = summarize(r for r in rows())
    assert summary['requests'] == 3
    assert summary['input_tokens'] == 30 and summary['output_tokens'] == 60
    assert summary['total_tokens'] == 90 and summary['cached_tokens'] == 15
    assert summary['seconds'] == 7.5 and summary['unpriced'] == 3


def test_filtering_single_paper_does_not_lose_its_later_requests():
    entries = rows()
    for n, row in enumerate(entries): row['item_id'] = 'paper-a' if n != 1 else 'paper-b'
    summary = summarize(r for r in entries if r['item_id'] == 'paper-a')
    assert summary['requests'] == 2 and summary['total_tokens'] == 60
    assert summary['seconds'] == 5.5


def test_events_do_not_consume_remaining_iterator_or_count_as_requests():
    entries = [{'event_type': 'circuit_opened', 'seconds': 999}, *rows()]
    summary = summarize(iter(entries))
    assert summary['requests'] == 3 and summary['circuit_breaker_trips'] == 1
    assert summary['seconds'] == 7.5
