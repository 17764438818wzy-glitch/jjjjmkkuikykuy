"""The LLM count follows the same Prefect folder for new and historical tasks."""
import importlib.util
from pathlib import Path
import sqlite3
from uuid import UUID, uuid4

import pytest


here = Path(__file__).resolve().parent
root = here.parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('parent', ['folder', 'legacy', 'other'])
def test_llm_count_recognizes_only_the_original_folder(tmp_path, parent):
    tests = load('prior_llm_ticket_tests', root / 'candidates/three-card-prefect-20261101/test_llm_ticket_positions.py')
    tests.module = load('candidate_llm_queue_summary', here / 'llm_queue_summary.py')
    args = tests.scenario(tmp_path, job_state='executing', task_state='running')
    _, database, observer, _ = args
    with sqlite3.connect(database) as db:
        folder = UUID(db.execute('SELECT task_run_id FROM llm_folder_queue').fetchone()[0])
    stage = observer.read_task_runs()[0]
    stage.flow_run_id = {'folder': folder, 'legacy': None, 'other': uuid4()}[parent]
    result = tests.module.summarize_llm(*args)
    assert result['llm_processing_folders'] == (1 if parent != 'other' else None)
