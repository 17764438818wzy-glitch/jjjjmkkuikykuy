from pathlib import Path
import importlib.util, sys
import pytest
here = Path(__file__).resolve().parent
root = here.parents[1]
rag = root / 'candidates/rag-nas-prefect-20261031'
post = root / 'candidates/postprocess-rag-unblock-20261101'
sys.path[:0] = [str(here), str(rag/'tests'), str(rag), str(post), str(root/'prefect-paperflow')]
import group_prefect
assert Path(group_prefect.__file__).resolve() == (here/'group_prefect.py').resolve()
import test_rag_nas_positions
spec = importlib.util.spec_from_file_location('hierarchy_positions_for_tests', here/'ticket_positions.py')
positions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(positions)
test_rag_nas_positions.positions = positions
raise SystemExit(pytest.main([str(here/'test_hierarchy.py'), str(rag/'tests/test_group_prefect.py'), str(post/'test_rag_nas_positions.py'), '-q']))
