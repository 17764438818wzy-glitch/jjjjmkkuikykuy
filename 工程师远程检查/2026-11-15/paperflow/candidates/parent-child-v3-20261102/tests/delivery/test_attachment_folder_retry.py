from urllib.parse import parse_qs, urlsplit

import pytest

from paperflow_v3.delivery.attachment_delivery import ensure_original_folder
from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient
from ragflow_client import RagflowHTTPError
from test_transport_journal import IDENTITY, event_store


def test_definitely_rejected_folder_create_is_retried_only_after_user_retry(tmp_path):
    class Files:
        base_url = 'http://127.0.0.1:9'

        def __init__(self):
            self.rows = []
            self.attempts = 0

        def request(self, method, endpoint, body=None):
            if method == 'GET':
                query = parse_qs(urlsplit(endpoint).query)
                rows = [row for row in self.rows if row['parent_id'] == query.get('parent_id', [''])[0]]
                return {'files': rows, 'total': len(rows)}
            self.attempts += 1
            if self.attempts == 1:
                raise RagflowHTTPError(403)
            self.rows.append({**body, 'id': 'folder-id'})
            return {'id': 'folder-id'}

    raw, (events, save) = Files(), event_store()
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(IDENTITY, events, save))
    with pytest.raises(RagflowHTTPError):
        ensure_original_folder(client, 'root', '081301', '【081301】专业｜附件原件', tmp_path)
    assert raw.attempts == 1 and raw.rows == []
    client = TaskCheckedClient(raw, lambda: True, ServerEffects(IDENTITY, events, save, retry_failed=True))
    assert ensure_original_folder(client, 'root', '081301', '【081301】专业｜附件原件', tmp_path) == 'folder-id'
    assert raw.attempts == 2 and len(raw.rows) == 1
