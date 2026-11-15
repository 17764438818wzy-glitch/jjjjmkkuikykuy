"""Offline compatibility exercise of existing primitives, not a new entrypoint.

The old attachment publisher splits its projection; it is not approved as the
new postprocess handoff and this test does not select or connect it.
"""
from contextlib import contextmanager
from pathlib import Path
import sys

from effect_journal import EffectJournal
from guarded_rag_client import GuardedRagClient


WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow' / 'tests'))


class Permit:
    def check_current(self): pass
    def start_transport(self, record): record()
    def start_observation(self): pass


@contextmanager
def admit(*args):
    yield Permit()


def test_existing_attachment_primitive_with_fake_remote_and_guard(tmp_path):
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    from paperflow_prefect.attachment_publish import ingest_attachment
    from preextract_publish import _Budget
    from test_attachment_publish import FakeStore, Counter, handoff, route

    class FakeWithoutBytesInPatchResult(FakeStore):
        def request(self, method, endpoint, body=None):
            result = super().request(method, endpoint, body)
            if method == 'PATCH' and isinstance(result, dict):
                return {key: value for key, value in result.items() if key != '_bytes'}
            return result

    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    remote = FakeWithoutBytesInPatchResult()
    guarded = GuardedRagClient(remote, journal, 'c' * 64, admit, admit, lambda *_: True, lambda *_: True)
    result = ingest_attachment(handoff(tmp_path), route(), guarded, tmp_path / 'receipts', _Budget(Counter()))
    assert result['status'] == 'verified'
    assert sum(row[0] == 'file' for row in remote.sent) == 1
    assert sum(row[0] == 'link' for row in remote.sent) == 1
    before = list(remote.sent)
    again = ingest_attachment(handoff(tmp_path), route(), guarded, tmp_path / 'receipts', _Budget(Counter()))
    assert again['status'] == 'verified'
    assert remote.sent == before
