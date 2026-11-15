from contextlib import contextmanager
from pathlib import Path
import sys
from urllib.parse import parse_qs, urlsplit

import pytest

from effect_journal import EffectJournal, EffectUnknown

WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())


class Files:
    base_url = 'http://synthetic'
    def __init__(self):
        self.folders = []
        self.creates = 0
        self.lose_response = False
    def request(self, method, endpoint, body=None):
        if method == 'POST':
            self.creates += 1
            self.folders.append({'id': f'folder-{self.creates}', **body})
            if self.lose_response:
                raise TimeoutError('creation response lost')
            return {'id': self.folders[-1]['id']}
        query = parse_qs(urlsplit(endpoint).query)
        if not query.get('parent_id'):
            return {'parent_folder': {'id': 'root', 'parent_id': 'root', 'type': 'folder', 'name': '/'},
                    'files': []}
        return {'files': [row for row in self.folders if row['parent_id'] == query['parent_id'][0]]}


@contextmanager
def admit(running):
    class Permit:
        def check_current(self):
            if not running:
                raise InterruptedError('paused')
        def start_transport(self, record):
            record()
    yield Permit()


@pytest.fixture
def case(tmp_path):
    from isolated_files_folder import ensure_test_files_folder
    raw = Files()
    journal = EffectJournal(tmp_path / 'journal.sqlite3')
    journal.initialize()
    receipt = tmp_path / 'folder.json'
    def run(running=True):
        return ensure_test_files_folder(raw, journal, lambda *_: admit(running), 'a' * 32, receipt)
    return locals()


def test_creates_one_dedicated_folder_and_reuses_its_receipt(case):
    assert case['run']() == 'folder-1'
    assert case['run']() == 'folder-1'
    assert case['raw'].creates == 1
    assert case['raw'].folders[0]['name'] == 'PaperFlow SELFTEST ' + 'a' * 32 + ' attachments'


def test_pause_before_creation_can_resume(case):
    with pytest.raises(InterruptedError):
        case['run'](False)
    assert case['raw'].creates == 0
    assert case['run']() == 'folder-1'


def test_unknown_creation_is_not_sent_again_or_assumed_complete(case):
    case['raw'].lose_response = True
    with pytest.raises(TimeoutError):
        case['run']()
    with pytest.raises(EffectUnknown):
        case['run']()
    assert case['raw'].creates == 1


def test_existing_unrecorded_folder_is_not_adopted(case):
    case['raw'].folders.append({'id': 'unrelated', 'parent_id': 'root', 'type': 'folder',
                                'name': 'PaperFlow SELFTEST ' + 'a' * 32 + ' attachments'})
    with pytest.raises(ValueError, match='unrecorded'):
        case['run']()
    assert case['raw'].creates == 0
