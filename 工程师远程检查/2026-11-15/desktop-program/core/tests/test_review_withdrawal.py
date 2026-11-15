"""Read-only review's isolated fault test; no candidate or real RAG writes."""
import json
from pathlib import Path

import pytest

from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag, advance, flow, release


def test_invalid_candidate_is_disabled_when_previous_cannot_be_restored(delivery_case,tmp_path):
    client=IndexedRag()
    first=flow(delivery_case,tmp_path,client)
    assert first.run()['state']=='active'
    previous=set(client.docs)
    release(first)
    candidate=flow(advance(delivery_case),tmp_path,client)
    def crash(point):
        if point=='old_disabled':raise RuntimeError('interrupted before active commit')
    candidate.fault=crash
    with pytest.raises(RuntimeError,match='interrupted'):
        candidate.run()
    current=set(client.docs)-previous
    assert current and all(client.docs[key]['status']=='1' for key in current)
    for _,did in previous:
        client.chunks[did].clear()
    for _,did in current:
        client.chunks[did][0]['content']+=' damaged candidate'
    candidate.fault=lambda point:None
    with pytest.raises(ConnectionError):
        candidate.run()
    hits=client.request('POST','/retrieval',{'dataset_ids':sorted({ds for ds,_ in current})})['chunks']
    summary=dict(previous=[dict(dataset_id=ds,document_id=did,status=client.docs[(ds,did)]['status']) for ds,did in previous],
                 candidate=[dict(dataset_id=ds,document_id=did,status=client.docs[(ds,did)]['status']) for ds,did in current],
                 candidate_still_retrievable=any(ch['document_id'] in {did for _,did in current} for ch in hits),
                 switch_phase=candidate.ledger.switching(candidate.identity['source_id'],candidate.code)['phase'])
    Path('evidence/review-withdrawal-result.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    assert not summary['candidate_still_retrievable'], summary
    assert all(client.docs[key]['status']=='0' for key in current)
