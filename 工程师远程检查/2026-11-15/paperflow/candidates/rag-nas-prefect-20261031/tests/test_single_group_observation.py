from types import SimpleNamespace

from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings


def test_observer_preserves_real_multipart_upload_arguments():
    bind(Settings.load())
    from ragflow_client import RagflowClient
    from folder_native_client import observe_native_request
    client = RagflowClient('http://127.0.0.1:1', 'synthetic')
    captured = []
    def request_once(request, timeout):
        captured.append(request)
        return [{'id': 'document-1'}]
    client._request_once = request_once
    client.request = observe_native_request(client.request, SimpleNamespace(native_pipeline_id='pipeline'), [], [])
    assert client.upload_bytes('dataset-1', 'body.md', b'test body') == 'document-1'
    assert captured[0].get_header('Content-type').startswith('multipart/form-data; boundary=')
    assert b'test body' in captured[0].data
