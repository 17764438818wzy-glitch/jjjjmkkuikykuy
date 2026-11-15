from pathlib import Path

from ingest_queue import QueueStore
from test_ingest_contracts import spec


def test_repeated_intake_does_not_invalidate_an_inflight_poll(tmp_path):
    queue = QueueStore(tmp_path / 'queue.db')
    artifact = spec(path=Path('paper.md'))
    task_id = queue.enqueue(artifact, {'status': 'submitted', 'remote_id': 'doc'})
    captured = queue.get(task_id)
    queue.enqueue(artifact)
    assert queue.transition(task_id, 'submitted', 'accepted',
                            expected_updated_at=captured['updated_at'])
