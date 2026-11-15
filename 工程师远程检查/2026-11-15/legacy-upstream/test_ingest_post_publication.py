import json
import time
from pathlib import Path

import pytest

from ingest_education import EducationArtifacts
from ingest_gateway import RagflowGateway
from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_artifact_provenance import admitted_source
from test_ingest_contracts import spec


@pytest.mark.parametrize('final_read', ['FAIL', 'missing', 'timeout', 'cleanup_failure'])
def test_failed_final_verification_withdraws_published_artifacts(tmp_path, final_read):
    task, remote, _, _, client = admitted_source(tmp_path)
    queue = QueueStore(tmp_path / 'queue.db')
    key = queue.enqueue(spec(source_id=task['source_id'], dataset_id='edu',
                             pipeline_id='pipe', remote_name='paper.md',
                             path=Path(task['path']), content_sha256=task['content_sha256']),
                        {'status': 'accepted', 'remote_id': 'doc', 'post_status': 'pending'})
    queue.transition(key, 'accepted', 'accepted', execution_begin_at=remote['process_begin_at'])
    client.base_url = queue.get(key)['service_key']
    original = client.documents
    reads = []

    def documents(*args, **kwargs):
        reads.append(1)
        rows = original(*args, **kwargs)
        # The first read opens postprocessing and the second is the artifact
        # writer's final check. Only the scheduler's post-publication check fails.
        if len(reads) >= 3:
            if final_read == 'missing': return []
            if final_read == 'timeout': raise TimeoutError('Final status unavailable')
            rows[0]['run'] = 'FAIL'
        return rows

    client.documents = documents
    output = tmp_path / 'artifacts'
    artifacts = EducationArtifacts(lambda: client, output)

    def invalidate(task):
        artifacts.invalidate(task)
        if final_read == 'cleanup_failure':
            raise OSError('Cleanup failure must not mask the remote failure')

    pool = WorkPool(queue, lambda: RagflowGateway(lambda: client), lambda *_: 'accepted',
                    on_accepted_for=artifacts, on_invalidated_for=invalidate)
    try:
        for _ in range(40):
            pool.tick()
            row = queue.get(key)
            if row['status'] != 'accepted' or row['post_status'] == 'needs_review': break
            time.sleep(.01)
        latest = json.loads((output / 'latest' / (task['source_id'] + '.json')).read_text())
        assert latest['valid'] is False
        assert row['post_status'] != 'done'
        assert not (output / 'direct_trees' / (task['source_id'] + '.json')).exists()
        assert not (output / 'method_graphs' / (task['source_id'] + '.json')).exists()
        # Withdrawal keeps the proven version available for investigation.
        assert list((output / 'attempts' / task['source_id']).glob('*/verified-v2/tree.json'))
        if final_read == 'cleanup_failure':
            assert row['status'] == 'outcome_unresolved'
            assert '产物撤回失败：OSError' in row['error']
    finally:
        pool.close()
