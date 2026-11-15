import sys
from pathlib import Path

import pytest


CANDIDATE = Path(__file__).resolve().parents[2]
NATIVE_CORE = Path(r'F:\论文预处理系统\程序\updates\unified-prefect-20261029-material-r02\core')
NATIVE_DESKTOP = NATIVE_CORE.parent / 'desktop'
BASE_DESKTOP = Path(r'F:\论文预处理系统\程序\desktop')
EXISTING_TESTS = CANDIDATE.parents[1] / 'prefect-paperflow' / 'tests'
PREFECT_PROJECT = EXISTING_TESTS.parent
sys.path[:0] = [str(CANDIDATE), str(NATIVE_DESKTOP), str(BASE_DESKTOP), str(NATIVE_CORE),
                str(PREFECT_PROJECT), str(EXISTING_TESTS)]


@pytest.fixture
def managed_counter_factory():
    from test_body_adapter import FixtureCounter

    class ManagedFixtureCounter(FixtureCounter):
        def close(self):
            pass

    return ManagedFixtureCounter
