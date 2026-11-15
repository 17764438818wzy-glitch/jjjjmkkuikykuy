"""Run the candidate's integration tests against the frozen package imports."""
import importlib
import os
from pathlib import Path
import sys


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
STAGE = ROOT / 'candidates' / os.environ.get('PAPERFLOW_TEST_STAGE', 'llm-flow-staged-20261031')
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path[:0] = [str(STAGE), str(STAGE / 'intake'), str(ROOT / 'prefect-paperflow'),
                str(ROOT / 'prefect-paperflow/paperflow_prefect/clean_overlay'), str(DESKTOP),
                'F:/论文预处理系统/程序/desktop', str(DESKTOP.parent / 'core'), str(HERE / 'tests')]
for name in ('attachment_raw_input', 'folder_completion', 'folder_ledger', 'frozen_body_plan',
             'handoff_contract', 'image_evidence', 'model_limits', 'pause_admission', 'raw_request',
             'raw_partition', 'segment_plan_contract', 'upstream_flow_binding', 'flow_executor',
             'flow_model_operations', 'flow_runtime'):
    sys.modules[name] = importlib.import_module('llm_queue.' + name)
sys.modules['flow_queue_summary'] = importlib.import_module('llm_queue_summary')
sys.modules['flow_folder_ledger'] = sys.modules['folder_ledger']
sys.modules['flow_raw_request'] = sys.modules['raw_request']
sys.modules['flow_pause_admission'] = sys.modules['pause_admission']
import pytest

raise SystemExit(pytest.main(['-q', *(str(HERE / 'tests' / name) for name in (
    'test_flow_model_operations.py', 'test_flow_executor.py', 'test_flow_runtime.py',
    'test_flow_reconcile.py', 'test_flow_queue_summary.py', 'test_upstream_flow_binding.py',
    'test_flow_ledger_gate.py', 'test_flow_pause_boundary.py', 'test_flow_control.py'))]))
