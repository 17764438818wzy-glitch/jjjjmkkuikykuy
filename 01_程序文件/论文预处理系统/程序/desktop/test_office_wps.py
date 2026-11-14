from pathlib import Path
from types import SimpleNamespace
import sys
import subprocess
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture(autouse=True)
def isolated_wps_test_lock(tmp_path,monkeypatch):
    import office_wps
    monkeypatch.setattr(office_wps,'_lock_path',lambda:tmp_path/'wps-test.lock')


@pytest.mark.parametrize('conversion_error', [False, True])
def test_existing_office_entry_prefers_wps_without_retrying_libreoffice(tmp_path, monkeypatch, conversion_error):
    import engine
    source = tmp_path / 'original.doc'
    source.write_bytes(b'preserve original')
    output = tmp_path / 'converted'
    expected = output / 'original.docx'
    calls = []

    def convert(actual_source, actual_output):
        calls.append((actual_source, actual_output))
        if conversion_error:
            raise RuntimeError('WPS conversion failed')
        return expected

    def forbidden(*args, **kwargs):
        pytest.fail('A registered WPS conversion must not launch LibreOffice')

    monkeypatch.setitem(sys.modules, 'office_wps', SimpleNamespace(available=lambda: True, convert=convert))
    monkeypatch.setattr(engine.subprocess, 'run', forbidden)
    if conversion_error:
        with pytest.raises(RuntimeError, match='WPS conversion failed'):
            engine.convert_office(source, output)
    else:
        assert engine.convert_office(source, output) == expected
    assert calls == [(source, output)]
    assert source.read_bytes() == b'preserve original'


def write_docx(path):
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('[Content_Types].xml', '<Types/>')
        archive.writestr('word/document.xml', '<document/>')


@pytest.mark.parametrize('failure', ['process', 'invalid_docx', 'timeout'])
def test_failed_wps_conversion_keeps_source_and_previous_output(tmp_path, monkeypatch, failure):
    import office_wps
    source, output = tmp_path / '论文.doc', tmp_path / 'out'
    source.write_bytes(b'private original')
    output.mkdir()
    previous = output / '论文.docx'
    previous.write_bytes(b'previous artifact must not be accepted or truncated')

    def run(command, **kwargs):
        staged, converted = Path(command[-3]), Path(command[-2])
        assert staged != source and staged.read_bytes() == source.read_bytes()
        assert len(str(staged)) < 220
        if failure == 'timeout':
            raise subprocess.TimeoutExpired(command, kwargs['timeout'])
        if failure == 'invalid_docx':
            with zipfile.ZipFile(converted, 'w') as archive:
                archive.writestr('not-a-word-document', 'invalid')
        return SimpleNamespace(returncode=int(failure == 'process'), stdout=b'', stderr=b'')

    monkeypatch.setattr(office_wps.subprocess, 'run', run)
    with pytest.raises(RuntimeError, match='WPS'):
        office_wps.convert(source, output)
    assert source.read_bytes() == b'private original'
    assert previous.read_bytes() == b'previous artifact must not be accepted or truncated'
    assert 'engine=WPS' in (tmp_path / 'out_conversion.log').read_text()


def test_wps_helpers_serialize_without_sharing_scratch_or_outputs(tmp_path, monkeypatch):
    import office_wps
    first_entered, release_first, second_started = threading.Event(), threading.Event(), threading.Event()
    active = 0
    peak = 0
    paths = []
    lock = threading.Lock()

    def run(command, **kwargs):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            paths.append(Path(command[-3]))
        first_entered.set()
        assert release_first.wait(5)
        write_docx(Path(command[-2]))
        with lock:
            active -= 1
        return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')

    source = tmp_path / 'original.doc'
    source.write_bytes(b'private original')
    monkeypatch.setattr(office_wps.subprocess, 'run', run)

    def second():
        second_started.set()
        return office_wps.convert(source, tmp_path / 'two')

    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(office_wps.convert, source, tmp_path / 'one')
        assert first_entered.wait(5)
        two = pool.submit(second)
        assert second_started.wait(5)
        try:
            with pytest.raises(TimeoutError):
                two.result(timeout=0.15)
        finally:
            release_first.set()
        assert office_wps._valid_docx(one.result(timeout=5))
        assert office_wps._valid_docx(two.result(timeout=5))
    assert peak == 1
    assert len(set(paths)) == 2
    assert all(not path.exists() for path in paths)


def test_page_table_and_vector_renderers_use_wps_without_starting_libreoffice(tmp_path, monkeypatch):
    import auto_evidence
    import evidence_render
    import evidence_tasks
    import fitz
    import office_wps
    import verified_pdf
    calls = []

    def convert(source, output, **kwargs):
        assert kwargs['target_format'] == 'pdf'
        calls.append(Path(source).suffix)
        output = Path(output)
        output.mkdir(parents=True, exist_ok=True)
        path = output / (Path(source).stem + '.pdf')
        with fitz.open() as pdf:
            pdf.new_page().insert_text((72, 72), 'Keep source page and vector content')
            pdf.save(path)
        return path

    def forbidden(*args, **kwargs):
        pytest.fail('A page or vector renderer still launches LibreOffice')

    monkeypatch.setattr(office_wps, 'available', lambda: True)
    monkeypatch.setattr(office_wps, 'convert', convert)
    monkeypatch.setattr(subprocess, 'run', forbidden)
    monkeypatch.setattr(verified_pdf, 'copy_verified_pdf', lambda *args: None)
    source = tmp_path / 'paper.docx'
    source.write_bytes(b'private test source')
    vector = tmp_path / 'chart.emf'
    vector.write_bytes(b'private vector')
    assert len(auto_evidence.source_pages(source, tmp_path / 'pages')) == 1
    with evidence_tasks._pdf(source, tmp_path / 'evidence') as pdf:
        assert 'Keep source page' in pdf[0].get_text()
    evidence_render.capture_docx_tables(source, [], lambda *args: None, lambda *args: None)
    assert evidence_render.load_visual_image(vector).width > 0
    assert calls == ['.docx', '.docx', '.docx', '.emf']


def test_lock_wait_uses_the_same_deadline_as_conversion(tmp_path, monkeypatch):
    import filelock
    import office_wps
    original = filelock.FileLock
    budgets = []

    class DelayedLock(original):
        def __enter__(self):
            result = super().__enter__()
            time.sleep(0.1)
            return result

    def run(command, **kwargs):
        budgets.append(kwargs['timeout'])
        write_docx(Path(command[-2]))
        return SimpleNamespace(returncode=0, stdout=b'', stderr=b'')

    source = tmp_path / 'original.doc'
    source.write_bytes(b'original')
    monkeypatch.setattr(filelock, 'FileLock', DelayedLock)
    monkeypatch.setattr(office_wps.subprocess, 'run', run)
    office_wps.convert(source, tmp_path / 'out', timeout=2)
    assert 0 < budgets[0] < 1.95


def test_wps_lock_does_not_change_with_per_paper_temp_directories(tmp_path):
    import os
    import office_wps
    script = '''
from pathlib import Path
from types import SimpleNamespace
import json,sys,time,zipfile
import office_wps
office_wps._lock_path=lambda:Path(sys.argv[3])
def run(command, **kwargs):
    started=time.monotonic()
    time.sleep(0.3)
    with zipfile.ZipFile(command[-2],'w') as archive:
        archive.writestr('[Content_Types].xml','<Types/>')
        archive.writestr('word/document.xml','<document/>')
    print(json.dumps([started,time.monotonic()]),flush=True)
    return SimpleNamespace(returncode=0,stdout=b'',stderr=b'')
office_wps.subprocess.run=run
office_wps.convert(Path(sys.argv[1]),Path(sys.argv[2]))
'''
    source = tmp_path / 'original.doc'
    source.write_bytes(b'original')
    processes = []
    try:
        for i in range(2):
            temp = tmp_path / f'temp-{i}'
            temp.mkdir()
            env = dict(os.environ, TEMP=str(temp), TMP=str(temp))
            processes.append(subprocess.Popen([sys.executable, '-B', '-c', script, str(source), str(tmp_path / f'out-{i}'),str(tmp_path/'shared-wps.lock')],
                cwd=Path(office_wps.__file__).parent, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)))
        intervals = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=10)
            assert process.returncode == 0, stderr.decode(errors='replace')
            intervals.append(__import__('json').loads(stdout))
        intervals.sort()
        assert intervals[0][1] <= intervals[1][0]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
