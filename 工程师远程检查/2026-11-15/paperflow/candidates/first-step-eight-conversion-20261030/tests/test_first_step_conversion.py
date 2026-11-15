"""First-step conversion behavior using synthetic files and fake engines."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import time
import zipfile

import pytest


@pytest.fixture
def native():
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    bind(Settings.load())
    from paperflow_prefect.first_step_conversion import FirstStepConverter
    return FirstStepConverter


def valid_docx(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('[Content_Types].xml', '<Types/>')
        archive.writestr('word/document.xml', '<document/>')
    return path


def test_eight_real_conversion_slots_each_hold_one_file(native, tmp_path):
    lock, release, reached = threading.Lock(), threading.Event(), threading.Event()
    active = 0
    peak = 0
    starts = []

    def libre(source, output, *, target_format, timeout):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            starts.append((source.name, timeout))
            if len(starts) == 8:
                reached.set()
        try:
            assert release.wait(5)
            return valid_docx(output / (source.stem + '.docx'))
        finally:
            with lock:
                active -= 1

    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre)
    sources = []
    for i in range(9):
        source = tmp_path / f'source-{i}.doc'
        source.write_bytes(f'different synthetic document {i}'.encode())
        sources.append(source)
    with ThreadPoolExecutor(max_workers=9) as executor:
        futures = [executor.submit(converter.convert, source, tmp_path / f'out-{i}')
                   for i, source in enumerate(sources)]
        try:
            assert reached.wait(5)
            time.sleep(.15)
            assert len(starts) == 8 and peak == 8
        finally:
            release.set()
        assert all(future.result(timeout=5).is_file() for future in futures)
    assert len(starts) == 9 and all(timeout == 30 for _, timeout in starts)


def test_fallback_is_sequential_with_one_wps_and_fresh_120_second_budget(native, tmp_path):
    from paperflow_prefect.first_step_conversion import ConverterAttemptFailed
    lock, entered, release = threading.Lock(), threading.Event(), threading.Event()
    events, active_wps, peak_wps = [], 0, 0

    def libre(source, output, *, target_format, timeout):
        with lock:
            events.append((source.name, 'libre', timeout))
        raise ConverterAttemptFailed('LibreOffice 转换超时')

    def wps(source, output, *, target_format, timeout):
        nonlocal active_wps, peak_wps
        with lock:
            active_wps += 1
            peak_wps = max(peak_wps, active_wps)
            events.append((source.name, 'wps', timeout))
            entered.set()
        try:
            assert release.wait(5)
            return valid_docx(output / (source.stem + '.docx'))
        finally:
            with lock:
                active_wps -= 1

    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, wps=wps)
    sources = [tmp_path / 'a.doc', tmp_path / 'b.doc']
    for i, source in enumerate(sources):
        source.write_bytes(f'document {i}'.encode())
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(converter.convert, source, tmp_path / f'out-{i}')
                   for i, source in enumerate(sources)]
        try:
            assert entered.wait(5)
            time.sleep(.2)
            assert sum(event[1] == 'wps' for event in events) == 1
        finally:
            release.set()
        assert all(future.result(timeout=5).is_file() for future in futures)
    assert peak_wps == 1
    assert sum(event[1] == 'wps' and event[2] == 120 for event in events) == 2
    for source in sources:
        assert [event[1] for event in events if event[0] == source.name] == ['libre', 'wps']


def test_wps_source_uses_wps_directly(native, tmp_path):
    seen = []
    def libre(*args, **kwargs):
        raise AssertionError('WPS files must not go to LibreOffice')
    def wps(source, output, *, target_format, timeout):
        seen.append(timeout)
        return valid_docx(output / (source.stem + '.docx'))
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, wps=wps)
    source = tmp_path / 'source.wps'
    source.write_bytes(b'synthetic wps')
    assert converter.convert(source, tmp_path / 'out').is_file()
    assert seen == [120]


def test_both_failures_are_preserved_for_unrecognized_status(native, tmp_path):
    from paperflow_prefect.first_step_conversion import ConverterAttemptFailed, ConvertersExhausted
    def libre(*args, **kwargs):
        raise ConverterAttemptFailed('LibreOffice 转换超时')
    def wps(*args, **kwargs):
        raise ConverterAttemptFailed('WPS 转换失败')
    source = tmp_path / 'source.doc'
    source.write_bytes(b'synthetic document')
    before = source.read_bytes()
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, wps=wps)
    with pytest.raises(ConvertersExhausted) as error:
        converter.convert(source, tmp_path / 'out')
    assert 'LibreOffice' in str(error.value) and 'WPS' in str(error.value)
    assert source.read_bytes() == before


def test_duplicate_wait_does_not_consume_other_conversion_channels(native, tmp_path):
    gate, entered = threading.Event(), threading.Event()
    calls = []
    def libre(source, output, *, target_format, timeout):
        calls.append(source.name)
        entered.set()
        assert gate.wait(5)
        return valid_docx(output / (source.stem + '.docx'))
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre)
    sources = [tmp_path / 'a.doc', tmp_path / 'b.doc']
    for path in sources:
        path.write_bytes(b'exact same synthetic source')
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(converter.convert, source, tmp_path / f'out-{i}')
                   for i, source in enumerate(sources)]
        try:
            assert entered.wait(5)
            time.sleep(.2)
            assert len(calls) == 1
        finally:
            gate.set()
        assert all(future.result(timeout=5).is_file() for future in futures)
    assert len(calls) == 1


def test_both_failed_body_conversions_are_reported_unrecognized(tmp_path):
    import json
    from paperflow_prefect.first_step_conversion import ConvertersExhausted
    from paperflow_prefect.group_classification import classify_group
    from paperflow_prefect.group_task_summary import make_group_summary_row
    library, group = tmp_path / 'library', tmp_path / 'library' / 'person'
    group.mkdir(parents=True)
    (group / 'paper.doc').write_bytes(b'synthetic doc')

    class Judge:
        def choose_body(self, rows):
            raise AssertionError('unreadable body is not a model candidate')

    def reader(path):
        raise ConvertersExhausted('LibreOffice 转换超时；WPS 转换失败')

    handoff = classify_group(group, library, Judge(), reader)
    path = tmp_path / 'handoff.json'
    path.write_text(json.dumps(handoff, ensure_ascii=False), encoding='utf8')
    row, status, issues = make_group_summary_row({'group_root': str(group), 'unreceived': str(path)}, library)
    assert status == 'review' and row['status'] == 'unrecognized'
    assert 'paper.doc' in row['reason'] and 'LibreOffice' in row['reason'] and 'WPS' in row['reason']


def test_uncertain_process_never_triggers_wps_fallback(native, tmp_path):
    from paperflow_prefect.first_step_conversion import ConverterProcessUncertain
    calls = []
    def libre(*args, **kwargs):
        raise ConverterProcessUncertain('LibreOffice 转换进程尚未退出')
    def wps(*args, **kwargs):
        calls.append('wps')
        raise AssertionError('must not retry an uncertain process')
    source = tmp_path / 'paper.doc'
    source.write_bytes(b'synthetic')
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, wps=wps)
    with pytest.raises(ConverterProcessUncertain):
        converter.convert(source, tmp_path / 'out')
    assert not calls
    assert list((tmp_path / 'slots').glob('lo-*.uncertain'))


def test_uncertain_slot_records_which_task_owns_it(native, tmp_path):
    import json
    from paperflow_prefect.first_step_conversion import ConverterProcessUncertain
    task_id = '92a9e958-3713-47f1-9c77-1f0ca7b6920f'
    def libre(*args, **kwargs):
        marker = next((tmp_path / 'slots').glob('lo-*.uncertain'))
        data = json.loads(marker.read_text(encoding='utf8'))
        assert data['task_id'] == task_id and data['state'] == 'active'
        raise ConverterProcessUncertain('LibreOffice 进程退出未确认')
    source = tmp_path / 'paper.doc'; source.write_bytes(b'synthetic')
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, task_id=task_id)
    with pytest.raises(ConverterProcessUncertain):
        converter.convert(source, tmp_path / 'out')
    marker = next((tmp_path / 'slots').glob('lo-*.uncertain'))
    data = json.loads(marker.read_text(encoding='utf8'))
    assert data['task_id'] == task_id and data['state'] == 'uncertain'


def test_uncertain_conversion_does_not_turn_a_cover_into_passed(tmp_path):
    from paperflow_prefect.first_step_control import UnknownRequest
    from paperflow_prefect.first_step_conversion import ConverterProcessUncertain
    from paperflow_prefect.group_classification import classify_group
    from paperflow_prefect.group_evidence import read_evidence
    from test_rule_precedence import Judge, make_group
    library, group = make_group(tmp_path)
    cover = group / '毕业论文封面.doc'
    cover.write_bytes(b'synthetic cover')
    def reader(path):
        if path == cover:
            raise ConverterProcessUncertain('WPS 进程归属尚未确认')
        return read_evidence(path)
    with pytest.raises(UnknownRequest, match='WPS 进程归属尚未确认'):
        classify_group(group, library, Judge(), reader)


def test_paused_conversion_propagates_as_pause_not_file_failure(tmp_path):
    from paperflow_prefect.first_step_control import PauseRequested
    from paperflow_prefect.group_classification import classify_group
    from test_rule_precedence import Judge, make_group
    library, group = make_group(tmp_path)
    def reader(path):
        raise PauseRequested()
    with pytest.raises(PauseRequested):
        classify_group(group, library, Judge(), reader)


@pytest.mark.parametrize('separate_cover', [False, True])
@pytest.mark.parametrize('uncertain', [False, True])
def test_title_recheck_propagates_pause_and_uncertain_process(tmp_path, separate_cover, uncertain):
    from paperflow_prefect.first_step_control import PauseRequested, UnknownRequest
    from paperflow_prefect.first_step_conversion import ConverterProcessUncertain
    from paperflow_prefect.group_classification import classify_group, verify_handoff
    library = tmp_path / 'library'
    group = library / '张三_201913137111'
    group.mkdir(parents=True)
    (group / '正文.docx').write_bytes(b'body')
    if separate_cover:
        (group / '毕业论文封面.docx').write_bytes(b'cover')
    title = '基于信息系统的毕业设计研究'
    class Judge:
        def choose_body(self, rows):
            return {'choice': next(row['file_id'] for row in rows if row['relative'] == '正文.docx')}
    def reader(path):
        if separate_cover and '封面' not in path.name:
            return {'format': 'docx', 'text': '摘要\n正文未列出题目。'}
        return {'format': 'docx', 'text': f'题目：{title}\n学号：201913137111\n学生姓名：张三\n摘要'}
    def title_reader(row, evidence):
        if separate_cover and '封面' not in row['relative']:
            return ('', '正文 Word 开头文本', '', False)
        return (title, '正文 Word 开头文本', title, True)
    handoff = classify_group(group, library, Judge(), reader, title_reader=title_reader)
    assert handoff['paper_title'] == title
    if separate_cover:
        assert handoff['title_source']['reference'] == '同组独立封面'
    def interrupted(path):
        if uncertain:
            raise ConverterProcessUncertain('进程归属未确认')
        raise PauseRequested()
    with pytest.raises(UnknownRequest if uncertain else PauseRequested):
        verify_handoff(handoff, evidence_reader=interrupted)


def test_wps_global_lock_wait_is_not_charged_to_its_conversion_timeout(native, tmp_path, monkeypatch):
    import subprocess
    import office_wps
    import processing_stop
    from filelock import FileLock
    from paperflow_prefect.first_step_office import convert_wps
    lock_path = tmp_path / 'global-wps.lock'
    monkeypatch.setattr(office_wps, '_lock_path', lambda: lock_path)
    monkeypatch.setattr(office_wps, '_wait_for_owned_exit', lambda state: None)
    observed = []

    def runner(command, **kwargs):
        observed.append(kwargs['timeout'])
        valid_docx(Path(command[-2]))
        Path(command[-1]).write_text('{"processes": []}', encoding='utf8')
        return subprocess.CompletedProcess(command, 0, b'', b'')

    monkeypatch.setattr(processing_stop, 'run_process', runner)
    source = tmp_path / 'source.doc'
    source.write_bytes(b'synthetic')
    lock = FileLock(str(lock_path))
    lock.acquire()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(convert_wps, source, tmp_path / 'out', target_format='docx', timeout=.1)
        try:
            time.sleep(.25)
            assert not future.done() and not observed
        finally:
            lock.release()
        assert future.result(timeout=3).is_file()
    assert observed == [.1]


def test_lost_wps_ownership_keeps_the_shared_channel_unavailable(native, tmp_path, monkeypatch):
    import subprocess
    import office_wps
    import processing_stop
    from paperflow_prefect.first_step_conversion import ConverterAttemptFailed, ConverterProcessUncertain
    monkeypatch.setattr(office_wps, '_lock_path', lambda: tmp_path / 'global-wps.lock')
    calls = []
    def runner(command, **kwargs):
        calls.append('wps')
        return subprocess.CompletedProcess(command, 1, b'', b'synthetic missing ownership')
    def libre(*args, **kwargs):
        raise ConverterAttemptFailed('LibreOffice 转换失败')
    monkeypatch.setattr(processing_stop, 'run_process', runner)
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre)
    source = tmp_path / 'paper.doc'
    source.write_bytes(b'synthetic')
    for i in range(2):
        with pytest.raises(ConverterProcessUncertain):
            converter.convert(source, tmp_path / f'out-{i}')
    assert calls == ['wps']
    assert (tmp_path / 'slots' / 'wps.uncertain').is_file()


def test_wps_uncertain_marker_keeps_owned_process_identity(native, tmp_path, monkeypatch):
    import json
    import subprocess
    import office_wps
    import processing_stop
    from paperflow_prefect.first_step_conversion import ConverterAttemptFailed, ConverterProcessUncertain
    monkeypatch.setattr(office_wps, '_lock_path', lambda: tmp_path / 'global-wps.lock')
    def runner(command, **kwargs):
        Path(command[-1]).write_text(json.dumps({'processes': [{'pid': 12345, 'created_at': 123.4}]}), encoding='utf8')
        return subprocess.CompletedProcess(command, 1, b'', b'')
    def still_running(state):
        raise RuntimeError('owned process has not exited')
    def libre(*args, **kwargs):
        raise ConverterAttemptFailed('LibreOffice 转换失败')
    monkeypatch.setattr(processing_stop, 'run_process', runner)
    monkeypatch.setattr(office_wps, '_wait_for_owned_exit', still_running)
    source = tmp_path / 'paper.doc'; source.write_bytes(b'synthetic')
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre,
                       task_id='92a9e958-3713-47f1-9c77-1f0ca7b6920f')
    with pytest.raises(ConverterProcessUncertain):
        converter.convert(source, tmp_path / 'out')
    marker = json.loads((tmp_path / 'slots' / 'wps.uncertain').read_text(encoding='utf8'))
    assert marker['processes'] == [{'pid': 12345, 'created_at': 123.4}]


def test_libre_timeout_stops_the_owned_child_before_releasing_slot(native, tmp_path):
    import subprocess
    import sys
    import psutil
    from paperflow_prefect.first_step_office import _run_libre
    child_pid = tmp_path / 'child.pid'
    code = ("import pathlib,subprocess,sys,time; "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
            "pathlib.Path(sys.argv[1]).write_text(str(child.pid)); time.sleep(60)")
    with pytest.raises(subprocess.TimeoutExpired):
        _run_libre([sys.executable, '-c', code, str(child_pid)], timeout=1.0, stop=None)
    assert child_pid.is_file()
    assert not psutil.pid_exists(int(child_pid.read_text()))


def test_conversion_channels_are_shared_across_separate_pool_instances(tmp_path):
    from paperflow_prefect.first_step_conversion import _Channels
    first, second = _Channels(tmp_path / 'channels'), _Channels(tmp_path / 'channels')
    entered = threading.Event()

    def wait_for_wps():
        with second.claim('wps'):
            entered.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with first.claim('wps'):
            future = executor.submit(wait_for_wps)
            time.sleep(.2)
            assert not entered.is_set()
        future.result(timeout=3)
    assert entered.is_set()


def test_paused_group_leaves_wps_queue_without_starting_another_conversion(native, tmp_path):
    from paperflow_prefect.first_step_control import PauseRequested
    from paperflow_prefect.first_step_conversion import ConverterAttemptFailed
    in_wps, release, cancel_second = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def libre(source, output, *, target_format, timeout):
        raise ConverterAttemptFailed('LibreOffice 转换超时')
    def wps(source, output, *, target_format, timeout):
        calls.append(source.name)
        in_wps.set()
        assert release.wait(5)
        return valid_docx(output / (source.stem + '.docx'))
    first = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, wps=wps)
    second = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre, wps=wps,
                    stop_requested=cancel_second.is_set)
    a, b = tmp_path / 'a.doc', tmp_path / 'b.doc'
    a.write_bytes(b'a'); b.write_bytes(b'b')
    with ThreadPoolExecutor(max_workers=2) as executor:
        one = executor.submit(first.convert, a, tmp_path / 'out-a')
        try:
            assert in_wps.wait(5)
            two = executor.submit(second.convert, b, tmp_path / 'out-b')
            time.sleep(.2)
            cancel_second.set()
            with pytest.raises(PauseRequested):
                two.result(timeout=3)
        finally:
            release.set()
        assert one.result(timeout=5).is_file()
    assert calls == ['a.doc']


def test_wps_worker_unexpected_exception_does_not_release_unconfirmed_slot(native, tmp_path, monkeypatch):
    import office_wps
    import processing_stop
    from paperflow_prefect.first_step_conversion import ConverterAttemptFailed, ConverterProcessUncertain
    monkeypatch.setattr(office_wps, '_lock_path', lambda: tmp_path / 'global-wps.lock')
    def runner(*args, **kwargs):
        raise OSError('synthetic worker failure after start')
    def libre(*args, **kwargs):
        raise ConverterAttemptFailed('LibreOffice 转换失败')
    monkeypatch.setattr(processing_stop, 'run_process', runner)
    source = tmp_path / 'paper.doc'; source.write_bytes(b'synthetic')
    converter = native(tmp_path / 'cache', tmp_path / 'slots', libre=libre)
    with pytest.raises(ConverterProcessUncertain):
        converter.convert(source, tmp_path / 'out')
    assert (tmp_path / 'slots' / 'wps.uncertain').exists()


def test_missing_engines_are_system_failure_not_unrecognized(native, tmp_path, monkeypatch):
    import office_libreoffice
    import office_wps
    from paperflow_prefect.first_step_conversion import ConverterUnavailable, ConvertersExhausted
    monkeypatch.setattr(office_libreoffice, 'executable', lambda: None)
    monkeypatch.setattr(office_wps, 'available', lambda: False)
    source = tmp_path / 'paper.doc'; source.write_bytes(b'synthetic')
    converter = native(tmp_path / 'cache', tmp_path / 'slots')
    with pytest.raises(ConverterUnavailable) as exc:
        converter.convert(source, tmp_path / 'out')
    assert not isinstance(exc.value, ConvertersExhausted)
    assert 'LibreOffice' in str(exc.value) and 'WPS' in str(exc.value)


def test_pause_during_global_wps_queue_does_not_launch_worker(native, tmp_path, monkeypatch):
    import office_wps
    import processing_stop
    from filelock import FileLock
    from paperflow_prefect.first_step_control import PauseRequested
    from paperflow_prefect.first_step_office import convert_wps
    lock_path = tmp_path / 'global-wps.lock'
    monkeypatch.setattr(office_wps, '_lock_path', lambda: lock_path)
    calls = []
    monkeypatch.setattr(processing_stop, 'run_process', lambda *args, **kwargs: calls.append('started'))
    source = tmp_path / 'source.doc'; source.write_bytes(b'synthetic')
    paused = threading.Event()
    lock = FileLock(str(lock_path)); lock.acquire()
    with ThreadPoolExecutor(max_workers=1) as executor:
        try:
            future = executor.submit(convert_wps, source, tmp_path / 'out', target_format='docx',
                                     timeout=120, stop_requested=paused.is_set)
            time.sleep(.2)
            paused.set()
            with pytest.raises(PauseRequested):
                future.result(timeout=3)
        finally:
            lock.release()
    assert not calls
