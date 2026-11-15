"""Summarize recorded native HTTP timings without starting or changing work."""
import json
from pathlib import Path
import sys


def summarize(work_root):
    requests, observations, groups = [], [], 0
    for path in Path(work_root).glob('*/native-observations.json'):
        data = json.loads(path.read_text(encoding='utf8'))
        groups += 1
        requests.extend(data['requests'])
        observations.extend(data['documents'])
    if not groups:
        return {'groups_with_observations': 0, 'native_http_attempts': None,
                'native_http_peak': None, 'sampled_running_document_peak': None}
    events = [(row['started_at_ns'], 1) for row in requests]
    events += [(row['ended_at_ns'], -1) for row in requests]
    active = peak = 0
    for _, change in sorted(events):
        active += change
        peak = max(peak, active)
    running, sampled_peak = set(), 0
    for row in sorted(observations, key=lambda item: item['at_ns']):
        if row['run'] in ('1', 'RUNNING'):
            running.add(row['document_id'])
        else:
            running.discard(row['document_id'])
        sampled_peak = max(sampled_peak, len(running))
    return {'groups_with_observations': groups, 'native_http_attempts': len(requests),
            'native_http_returned': sum(row.get('outcome') == 'returned' for row in requests),
            'native_http_errors': sum(row.get('outcome') == 'error' for row in requests),
            'native_http_outcome_unrecorded': sum('outcome' not in row for row in requests),
            'native_http_peak': peak, 'sampled_running_document_peak': sampled_peak,
            'first_request_at_ns': min((row['started_at_ns'] for row in requests), default=None),
            'last_request_ended_at_ns': max((row['ended_at_ns'] for row in requests), default=None)}


if __name__ == '__main__':
    print(json.dumps(summarize(sys.argv[1]), ensure_ascii=False, indent=2))
