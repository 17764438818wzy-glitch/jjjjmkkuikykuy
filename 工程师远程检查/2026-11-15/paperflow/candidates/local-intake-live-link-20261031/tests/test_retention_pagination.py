import copy
from threading import Event

import pytest


def reference(number):
    return {'task_id': 'task-one', 'group_id': f'group-{number}', 'paper_id': f'paper-{number}',
            'receipt_id': f'receipt-{number}', 'consumer_id': 'local-intake',
            'request_id': f'request-{number}', 'retention_id': f'retention-{number}', 'state': 'accepted'}


def fake_page(rows, page, size, *, revision=1):
    return {'task_id': 'task-one', 'items': copy.deepcopy(rows[(page - 1) * size:page * size]),
            'page': page, 'page_size': size, 'list_total': len(rows),
            'has_next': page * size < len(rows), 'revision': revision}


def test_reference_pages_are_verified_twice_and_joined_in_order():
    from upstream_discovery import FirstStepHTTPSource

    source = FirstStepHTTPSource(page_size=2)
    rows, calls = [reference(i) for i in range(5)], []

    def get(path, query):
        assert path.endswith('/task-one/local-retentions')
        calls.append(query['page'])
        return fake_page(rows, query['page'], query['page_size'])

    source._get = get
    assert source.list_retentions('task-one')['items'] == rows
    assert calls == [1, 2, 3, 1, 2, 3]


@pytest.mark.parametrize('kind', ['changed_revision', 'changed_count', 'wrong_page', 'short_page',
                                  'duplicate_id', 'changed_second_pass', 'old_unpaginated'])
def test_incomplete_or_changing_reference_pages_never_look_complete(kind):
    from upstream_discovery import FirstStepHTTPSource

    source = FirstStepHTTPSource(page_size=2)
    rows, calls = [reference(i) for i in range(3)], []

    def get(path, query):
        page = query['page']
        calls.append(page)
        data = fake_page(rows, page, query['page_size'])
        if kind == 'changed_revision' and page == 2:
            data['revision'] = 2
        elif kind == 'changed_count' and page == 2:
            data['list_total'] = 4
        elif kind == 'wrong_page' and page == 2:
            data['page'] = 1
        elif kind == 'short_page' and page == 1:
            data['items'].pop()
        elif kind == 'duplicate_id' and page == 2:
            data['items'][0]['retention_id'] = rows[0]['retention_id']
        elif kind == 'changed_second_pass' and len(calls) > 2 and page == 1:
            data['items'][0]['state'] = 'reserved'
        elif kind == 'old_unpaginated':
            data = {'task_id': 'task-one', 'items': rows}
        return data

    source._get = get
    with pytest.raises(ValueError):
        source.list_retentions('task-one')


def test_reference_pagination_honors_stop_between_requests():
    from upstream_discovery import FirstStepHTTPSource, _ScanStopped

    source = FirstStepHTTPSource(page_size=1)
    rows, stop, calls = [reference(i) for i in range(3)], Event(), []

    def get(path, query):
        calls.append(query['page'])
        stop.set()
        return fake_page(rows, query['page'], query['page_size'])

    source._get = get
    with pytest.raises(_ScanStopped):
        source.list_retentions('task-one', stop_event=stop)
    assert calls == [1]
