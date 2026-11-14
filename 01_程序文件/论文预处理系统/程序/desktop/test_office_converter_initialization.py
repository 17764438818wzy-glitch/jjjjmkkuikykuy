import importlib.util
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_input_targeted_repair_143 import docx_bytes


@pytest.fixture(autouse=True)
def preserve_loaded_converter(monkeypatch):
    import office_wps
    monkeypatch.setattr(office_wps, 'available', lambda: False)
    name = 'uv_upstream_libreoffice'
    absent = object()
    previous = sys.modules.get(name, absent)
    yield
    if previous is absent:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = previous


def fake_external_converter(monkeypatch, execute):
    """Replace only the dynamically loaded external command builder and process."""
    import engine
    module_name = 'uv_upstream_libreoffice'
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    original = importlib.util.spec_from_file_location

    class Loader:
        def create_module(self, spec):
            return None

        def exec_module(self, module):
            execute()
            module.resolve_libreoffice_path = lambda env: SimpleNamespace(status='found', path=Path('soffice'))
            module.build_libreoffice_conversion_command = lambda executable, source, output, **kwargs: [str(executable), str(source), str(output)]

    def spec(name, location, *args, **kwargs):
        return ModuleSpec(name, Loader()) if name == module_name else original(name, location, *args, **kwargs)

    def convert(command, **kwargs):
        source, output = Path(command[-2]), Path(command[-1])
        (output/(source.stem+'.docx')).write_bytes(docx_bytes())
        return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')

    monkeypatch.setattr(importlib.util, 'spec_from_file_location', spec)
    monkeypatch.setattr(engine.subprocess, 'run', convert)


def test_concurrent_first_conversions_wait_for_complete_module(tmp_path, monkeypatch):
    import engine
    loading, release, second_started = threading.Event(), threading.Event(), threading.Event()
    calls = []

    def execute():
        calls.append('load')
        loading.set()
        assert release.wait(5)

    fake_external_converter(monkeypatch, execute)
    source = tmp_path/'paper.doc'
    source.write_bytes(b'private original')

    def second():
        second_started.set()
        return engine.convert_office(source, tmp_path/'two')

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(engine.convert_office, source, tmp_path/'one')
        assert loading.wait(5)
        another = pool.submit(second)
        assert second_started.wait(5)
        try:
            # A correct second caller waits while the first loader is suspended.
            # The buggy code immediately accesses the half-initialized module.
            try:
                another.result(timeout=0.1)
            except TimeoutError:
                pass
        finally:
            release.set()
        assert first.result(timeout=5).read_bytes() == docx_bytes()
        assert another.result(timeout=5).read_bytes() == docx_bytes()
    assert calls == ['load']


def test_failed_loader_does_not_poison_later_conversion(tmp_path, monkeypatch):
    import engine
    calls = []

    def execute():
        calls.append('load')
        if len(calls) == 1:
            raise RuntimeError('loader failed')

    fake_external_converter(monkeypatch, execute)
    source = tmp_path/'paper.doc'
    source.write_bytes(b'private original')
    with pytest.raises(RuntimeError, match='loader failed'):
        engine.convert_office(source, tmp_path/'one')
    assert 'uv_upstream_libreoffice' not in sys.modules
    assert engine.convert_office(source, tmp_path/'two').read_bytes() == docx_bytes()
