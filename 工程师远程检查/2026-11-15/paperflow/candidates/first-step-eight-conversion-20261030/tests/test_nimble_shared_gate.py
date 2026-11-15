"""The offline and controlled first-step entry points share one Nimble gate."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import time

from filelock import FileLock


def test_offline_judge_waits_for_frontend_nimble_gate(tmp_path, monkeypatch):
    from paperflow_prefect import nimble_group, settings as settings_module
    from paperflow_prefect.settings import Settings, first_step_shared_data

    program, release = tmp_path / 'program', tmp_path / 'release'
    runtime = tmp_path / 'runtime'
    default_data, workspace_data = runtime / 'data', runtime / 'workspace' / 'data'
    runtime.mkdir()
    values = {'program_root': str(program), 'release_root': str(release), 'data_root': str(default_data)}
    workspace = dict(values, data_root=str(workspace_data))
    (runtime / 'workspace-config.json').write_text(json.dumps(workspace), encoding='utf8')
    setting = Settings(values)
    monkeypatch.setattr(settings_module, 'PROJECT', tmp_path / 'candidate')
    monkeypatch.setattr(Settings, 'load', classmethod(lambda cls, path=None: setting))
    assert first_step_shared_data(setting) == workspace_data.resolve()
    gate_path = workspace_data / 'control-locks' / 'nimble-requests.lock'
    gate_path.parent.mkdir(parents=True)
    calls = []

    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self, _): return b'{}'
    def fake_urlopen(request, timeout):
        calls.append(timeout)
        return Response()
    monkeypatch.setattr(nimble_group, 'urlopen', fake_urlopen)
    judge = nimble_group.NimbleJudge(timeout=10)
    gate = FileLock(str(gate_path)); gate.acquire()
    with ThreadPoolExecutor(max_workers=1) as executor:
        try:
            result = executor.submit(judge._transmit, object(), 10)
            time.sleep(.2)
            assert not result.done() and calls == []
        finally:
            gate.release()
        assert result.result(timeout=3) == b'{}'
    assert calls == [10]
    assert judge._remaining_after_queue(time.monotonic() - 20) == 10


def test_another_explicit_workspace_keeps_its_own_resource_gates(tmp_path, monkeypatch):
    from paperflow_prefect import settings as settings_module
    from paperflow_prefect.settings import Settings, first_step_shared_data
    program, release = tmp_path / 'program', tmp_path / 'release'
    project = tmp_path / 'workspace-a'
    (project / 'runtime').mkdir(parents=True)
    cfg_a = {'program_root': str(program), 'release_root': str(release),
             'data_root': str(project / 'runtime' / 'workspace' / 'data')}
    (project / 'runtime' / 'workspace-config.json').write_text(json.dumps(cfg_a), encoding='utf8')
    data_b = tmp_path / 'workspace-b' / 'data'
    setting_b = Settings(dict(cfg_a, data_root=str(data_b)))
    monkeypatch.setattr(settings_module, 'PROJECT', project)
    assert first_step_shared_data(setting_b) == data_b.resolve()
