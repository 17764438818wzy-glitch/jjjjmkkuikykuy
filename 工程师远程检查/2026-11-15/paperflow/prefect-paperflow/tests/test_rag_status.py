from pathlib import Path
import tempfile
import unittest


class StatusTests(unittest.TestCase):
    def test_native_stub_and_image_downloads_are_observed_through_both_transports(self):
        import io
        from types import SimpleNamespace
        from paperflow_prefect.legacy import bind
        from paperflow_prefect.settings import Settings
        from paperflow_prefect.rag_status import RagProgress, observe_client
        bind(Settings.load())
        from paperflow.rag import RagPublisher
        with tempfile.TemporaryDirectory() as directory:
            progress = RagProgress(Path(directory) / 'status.json', 20)
            observation = progress.observation({'paper_id':'p', 'revision':'r'}, 'recover_body')
            seen = []
            class Response(io.BytesIO):
                headers = {'Content-Type':'image/png'}
                def read(self, *args):
                    seen.append(progress.snapshot()['counts'])
                    return super().read(*args)
            class Opener:
                def open(self, *args, **kwargs): return Response(b'fixture')
            class DirectDownloads:
                def download_bytes(self, *args):
                    seen.append(progress.snapshot()['counts']); return b'fixture'
                def download_image(self, *args):
                    seen.append(progress.snapshot()['counts']); return b'fixture'
            with observation.bind():
                observation.update('ready', occupied=True)
                for service in (DirectDownloads(), SimpleNamespace(
                        opener=Opener(), base_url='http://127.0.0.1:9', api_key='fixture-only')):
                    publisher = RagPublisher(observe_client(service))
                    self.assertEqual(publisher._download('ds', 'doc'), b'fixture')
                    self.assertEqual(publisher.download_image('image'), b'fixture')
                    self.assertEqual(progress.snapshot()['counts']['waiting_verification'], 1)
            self.assertEqual(len(seen), 4)
            self.assertTrue(all(row['verifying'] == 1 and row['uploading'] == 0 for row in seen))

    def test_actual_upload_readback_and_waiting_are_distinct(self):
        from paperflow_prefect.rag_status import RagProgress, observe_client
        with tempfile.TemporaryDirectory() as directory:
            progress = RagProgress(Path(directory) / 'status.json', 20)
            observation = progress.observation({'paper_id': 'p', 'revision': 'r'}, 'publish_body')
            observation.update('waiting_local', occupied=False)
            self.assertEqual(progress.snapshot()['counts']['waiting'], 1)
            self.assertEqual(progress.snapshot()['counts']['uploading'], 0)
            seen = []
            class Service:
                def upload_bytes(self, *args):
                    seen.append(progress.snapshot()['counts']); return 'doc'
                def request(self, *args, **kwargs):
                    seen.append(progress.snapshot()['counts']); return {}
            with observation.bind():
                observation.update('ready', occupied=True)
                client = observe_client(Service())
                client.upload_bytes('ds', 'paper.md', b'body')
                self.assertEqual(progress.snapshot()['counts']['waiting_verification'], 1)
                client.request('GET', '/datasets/ds/documents?id=doc')
                client.request('POST', '/retrieval', {})
            self.assertEqual(seen[0]['occupied'], 1)
            self.assertEqual(seen[0]['uploading'], 1)
            self.assertEqual(seen[1]['uploading'], 0)
            self.assertEqual(seen[1]['verifying'], 1)
            self.assertEqual(seen[2]['uploading'], 0)
            observation.update('body_verified', occupied=False)
            self.assertEqual(progress.snapshot()['counts']['verified_papers'], 0)
            observation.update('verified', paper_verified=True)
            self.assertEqual(progress.snapshot()['counts']['verified_papers'], 1)
            self.assertEqual(progress.snapshot()['counts']['occupied'], 0)

    def test_network_error_preserves_unknown_without_leaking_request_activity(self):
        from paperflow_prefect.rag_status import RagProgress, observe_client
        with tempfile.TemporaryDirectory() as directory:
            progress = RagProgress(Path(directory) / 'status.json', 20)
            observation = progress.observation({'paper_id': 'p', 'revision': 'r'}, 'publish_body')
            class Service:
                def upload_bytes(self, *args): raise TimeoutError('sensitive response must not be saved')
            with observation.bind():
                observation.update('ready', occupied=True)
                with self.assertRaises(TimeoutError): observe_client(Service()).upload_bytes('ds', 'p', b'data')
            observation.update('unknown', occupied=False, error_type='TimeoutError')
            snapshot = progress.snapshot()
            self.assertEqual(snapshot['counts']['uploading'], 0)
            self.assertEqual(snapshot['counts']['unknown'], 1)
            self.assertNotIn('sensitive response', str(snapshot))


if __name__ == '__main__': unittest.main()
