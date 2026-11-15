import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT.parents[1]


@pytest.fixture
def cli(monkeypatch):
    monkeypatch.syspath_prepend(str(PROJECT / 'candidates' / 'local-intake-live-link-20261031'))
    path = ROOT / 'projection_next' / 'waiting_cli.py'
    spec = importlib.util.spec_from_file_location('isolated_waiting_cli_projection', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_missing_projection_defaults_to_unknown_without_execution(cli, tmp_path):
    assert cli.configured_postprocessing(None, None, projection_path=tmp_path / 'missing.py') == (None, None)
    assert cli.configured_postprocessing(object(), object(), projection_path=tmp_path / 'missing.py') == (None, None)


def test_cli_loads_projection_factory_without_executing_callbacks(cli, tmp_path):
    path = tmp_path / 'synthetic_projection.py'
    path.write_text("def build_projections(queue, observer):\n"
                    "    def stage(*args): raise AssertionError('must not run during setup')\n"
                    "    def results(*args): raise AssertionError('must not run during setup')\n"
                    "    return stage, results\n", encoding='utf8')
    stage, results = cli.configured_postprocessing(object(), object(), projection_path=path)
    assert callable(stage) and callable(results)


def test_serve_passes_projected_readers_without_starting_them(cli, monkeypatch):
    stage = lambda *_: (_ for _ in ()).throw(AssertionError('not during setup'))
    results = lambda *_: (_ for _ in ()).throw(AssertionError('not during setup'))
    queue, orders, app = object(), object(), object()
    observed = {}
    monkeypatch.setattr(cli, 'LocalQueue', lambda *_, **__: queue)
    monkeypatch.setattr(cli, 'configured_orders', lambda value: orders if value is queue else None)
    monkeypatch.setattr(cli, 'configured_postprocessing', lambda value, observer: (stage, results))
    def create_app(actual, **options):
        observed.update(queue=actual, options=options)
        return app
    def run(actual, **options):
        observed.update(app=actual, server=options)
    monkeypatch.setitem(sys.modules, 'waiting_api', SimpleNamespace(create_app=create_app))
    monkeypatch.setitem(sys.modules, 'uvicorn', SimpleNamespace(run=run))
    assert cli.main(['serve', '--db', 'synthetic.sqlite3', '--port', '12345']) == 0
    assert observed['queue'] is queue and observed['app'] is app
    assert observed['options']['ticket_observer'] is orders
    assert observed['options']['stage_projection'] is stage
    assert observed['options']['results_projection'] is results
    assert observed['server']['host'] == '127.0.0.1'
