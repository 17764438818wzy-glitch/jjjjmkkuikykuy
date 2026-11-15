"""Bind the audited business release before any synthetic test imports it."""
from pathlib import Path
import sys
import tempfile

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings

_temporary = tempfile.TemporaryDirectory(prefix='paperflow-v3-stages-')
bind(Settings(dict(Settings.load().values, data_root=_temporary.name)))
# Some reused fixtures prepend the old desktop directory during collection.
# Import the selected overlay now so their later imports resolve the same parser.
import preextract_material  # noqa: F401, E402


@pytest.fixture
def managed_counter_factory():
    from test_body_adapter import FixtureCounter

    class ManagedFixtureCounter(FixtureCounter):
        def close(self):
            pass

    return ManagedFixtureCounter
