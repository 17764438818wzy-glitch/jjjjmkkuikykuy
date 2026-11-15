"""Install the reviewed control files while the new lanes remain closed.

This is an explicit operator command. It refuses active/scheduled Prefect work,
backs up each file, and restarts only the two named first-step services.
It never submits work or changes either queue barrier.
"""
from datetime import datetime
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
import psutil
from filelock import FileLock


ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
LIVE = WORKSPACE / 'candidates' / 'first-step-control-20261029' / 'paperflow_prefect_v2'
CANDIDATE = ROOT / 'control_next'
FILES = {
    'first_step_control.py': ('c0436deeb4e169bc57a3e2ef6ae09d74ef963601351f800c4fa0aec7fcdde0cb', '1dc55476d1991a75eda3cd40a40c13e95f1621c55c1263bee27c657d20ae0fc6'),
    'first_step_delete.py': ('fd42a7931645cc3e2daf8e9aab567dd003ace1eb4e488cefb849496ff5b2ea01', 'b650b4755d8bf03bf7464653ec60f888a1435c6a703551988bd84a95682d07a0'),
    'first_step_scheduler.py': ('25228f7bf45946c637fc48d8a56807ded55356cd96663e69223c0e147eb5bceb', 'c031d9b62124aa26985da6041edb344369b234ff60168ad6f44e9e2080f2da3b'),
    'group_summary_store.py': ('5216000302d77ded49605482c742bf49d605171a159acc4944375df1ad5b7471', '84740ba0131e75d85192abe524ffa6f2080032fdad136072ad1c67eb78fdf61a'),
    'llm_admission_state.py': ('57b1e43230f2b274d60f77c14c730c4c845734e9c5d9652bb1b062759f4d48f4', 'c6b2db69d1f8b99bba827c6a0e8a24890c32a9217f6f6160aa736cd6144b7bca'),
}
TASKS = ('Paperflow-FirstStep-API-14203', 'Paperflow-Unified-FirstStep-Runner')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def powershell(action, task):
    # Both strings are fixed program constants, never derived from task input.
    script = "$ErrorActionPreference='Stop'; " + action + "-ScheduledTask -TaskName '" + task + "'"
    subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', script],
                   check=True, capture_output=True, text=True)


def assert_prefect_idle(api):
    from prefect.client.orchestration import SyncPrefectClient
    from prefect.client.schemas.filters import (FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType,
                                                TaskRunFilter, TaskRunFilterState, TaskRunFilterStateType)
    from prefect.client.schemas.objects import StateType
    states = [StateType.RUNNING, StateType.PENDING, StateType.SCHEDULED, StateType.CANCELLING]
    with SyncPrefectClient(api, httpx_settings={'timeout': 5, 'trust_env': False}) as client:
        flows = client.read_flow_runs(flow_run_filter=FlowRunFilter(state=FlowRunFilterState(
            type=FlowRunFilterStateType(any_=states))), limit=1)
        active = client.read_task_runs(task_run_filter=TaskRunFilter(state=TaskRunFilterState(
            type=TaskRunFilterStateType(any_=states))), limit=1)
        if flows or active:
            raise RuntimeError('Prefect 存在运行、等待或已排程工作，未重启服务')


def assert_deployments(api):
    from prefect.client.orchestration import SyncPrefectClient
    with SyncPrefectClient(api, httpx_settings={'timeout': 5, 'trust_env': False}) as client:
        deployments = [client.read_deployment_by_name(name) for name in
                       ('论文第一步-任务工单/手动运行', '资料文件夹工单/前处理')]
        if any(d.paused or 'paperflow-order-v2-live-20261031' not in (d.tags or []) for d in deployments):
            raise RuntimeError('现有工单部署状态不可核验')
        return tuple(str(d.id) for d in deployments)


def task_states():
    script = ("$ErrorActionPreference='Stop'; "
              "@('Paperflow-FirstStep-API-14203','Paperflow-Unified-FirstStep-Runner') | "
              "ForEach-Object { $t=Get-ScheduledTask -TaskName $_; "
              "[pscustomobject]@{name=$_.ToString();state=$t.State.ToString()} } | "
              "ConvertTo-Json -Compress")
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', script],
                            check=True, capture_output=True, text=True)
    return {item['name']: item['state'] for item in json.loads(result.stdout)}


def service_processes(require_both=False):
    """Capture only these services and their descendants; never terminate processes."""
    directory = WORKSPACE / 'candidates' / 'first-step-control-20261029'
    wanted = {os.path.normcase(str((directory / name).resolve())) for name in
              ('managed_api_only.py', 'run_unified_first_step.py')}
    found = set()
    snapshot = {}
    for process in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        try:
            info = process.info
            if not (info['name'] or '').lower().startswith('python'):
                continue
            if info['cmdline'] is None:
                raise RuntimeError('无法检查 Python 进程命令行')
            arguments = {os.path.normcase(os.path.abspath(arg)) for arg in info['cmdline'] if arg}
            matched = wanted & arguments
            if not matched:
                continue
            found.update(matched)
            for member in [process, *process.children(recursive=True)]:
                snapshot[member.pid] = member.create_time()
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied as exc:
            raise RuntimeError('无法检查第一步服务的进程树') from exc
    if require_both and found != wanted:
        raise RuntimeError('无法确认两个第一步服务进程')
    return snapshot


def wait_stopped(previous, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = []
        for pid, created in previous.items():
            try:
                process = psutil.Process(pid)
                if abs(process.create_time() - created) < .01 and process.is_running():
                    remaining.append(pid)
            except psutil.NoSuchProcess:
                pass
            except psutil.AccessDenied as exc:
                raise RuntimeError('无法核验原服务进程是否退出') from exc
        states = task_states()
        if not remaining and not service_processes() and all(states.get(t) == 'Ready' for t in TASKS):
            try:
                with socket.create_connection(('127.0.0.1', 14203), timeout=.25):
                    pass
            except OSError:
                return
        time.sleep(.5)
    raise RuntimeError('第一步服务或其原进程尚未完全停止')


def assert_control_idle(database):
    """Do not count uncertain historical requests as completed or retry them."""
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=3) as db:
        gates = dict(db.execute('SELECT name,blocked FROM paperflow_queue_barriers'))
        if gates != {'rag_upload': 1, 'postprocessing': 1}:
            raise RuntimeError('队列门禁未保持关闭')
        if any(db.execute('SELECT COUNT(*) FROM ' + name).fetchone()[0] for name in
               ('paperflow_postprocess_stage_jobs', 'postprocessing_claims', 'postprocessing_results', 'postprocessing_receipts')):
            raise RuntimeError('后处理已有工作，未替换控制代码')
        columns = {row[1] for row in db.execute('PRAGMA table_info(paperflow_postprocess_stage_jobs)')}
        if 'source_llm_generation' not in columns:
            raise RuntimeError('后处理源代次字段不可用')
        checks = (
            "SELECT COUNT(*) FROM tasks WHERE start_pending<>0",
            # Task creation persists an unbound row before ensure() can send to
            # Prefect. Refuse shutdown while such a request may still be live.
            "SELECT COUNT(*) FROM tasks WHERE prefect_order_model='v2' AND prefect_run_id IS NULL",
            "SELECT COUNT(*) FROM uploads WHERE state='uploading'",
            "SELECT COUNT(*) FROM control_groups WHERE state='running'",
            "SELECT COUNT(*) FROM model_decisions WHERE state='inflight'",
            "SELECT COUNT(*) FROM llm_request_admissions WHERE state IN ('admitted','returned_unverified')",
            "SELECT COUNT(*) FROM paperflow_llm_stage_jobs WHERE state IN ('reserved','submitting','submitted','executing')",
        )
        if any(db.execute(query).fetchone()[0] for query in checks):
            raise RuntimeError('本机控制库存在待启动或正在处理的工作')
        return db.execute("SELECT COUNT(*) FROM llm_request_admissions WHERE state='unknown'").fetchone()[0]


def assert_closed_gates(database):
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=3) as db:
        if dict(db.execute('SELECT name,blocked FROM paperflow_queue_barriers')) != {
                'rag_upload': 1, 'postprocessing': 1}:
            raise RuntimeError('服务重启后队列门禁发生变化')


@contextmanager
def control_write_fence(database):
    """Temporarily block new control-database writes while services are stopped.

    This is not a Prefect-wide or server-wide lock. Callers still inspect the
    actual Prefect state before and after stopping the local service processes.
    """
    with sqlite3.connect(database.as_uri() + '?mode=rw', uri=True, timeout=3) as db:
        db.execute('BEGIN IMMEDIATE')
        try:
            yield
        finally:
            db.rollback()


def stop_services():
    previous = service_processes()
    for task in reversed(TASKS):
        if task_states().get(task) == 'Running':
            powershell('Stop', task)
    wait_stopped(previous)


def start_services():
    errors = []
    for task in reversed(TASKS):
        try:
            if task_states().get(task) != 'Running':
                powershell('Start', task)
        except Exception as exc:
            errors.append(task + ': ' + type(exc).__name__)
    if errors:
        raise RuntimeError('未能恢复服务：' + ', '.join(errors))


def wait_ready(api, original_health, expected_deployments, database, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            current = health()
            if not isinstance(current.get('instance_id'), str) or not current['instance_id']:
                raise RuntimeError('第一步 API 新实例身份不可核验')
            if current['instance_id'] == original_health.get('instance_id'):
                raise RuntimeError('旧 API 实例仍在运行')
            identity_fields = ('prefect_api_url', 'summary_index', 'local_intake_queue_db', 'receipt_root', 'scope')
            if any(not original_health.get(key) or current.get(key) != original_health[key] for key in identity_fields):
                raise RuntimeError('第一步 API 服务配置发生变化')
            if Path(current['summary_index']).resolve() != database.resolve():
                raise RuntimeError('第一步 API 控制数据库身份不一致')
            if task_states() != {t: 'Running' for t in TASKS}:
                raise RuntimeError('第一步计划任务尚未恢复')
            service_processes(require_both=True)
            if assert_deployments(api) != expected_deployments:
                raise RuntimeError('工单部署身份发生变化')
            return current
        except (OSError, ValueError, RuntimeError, psutil.Error) as exc:
            last = exc
            time.sleep(.5)
    raise RuntimeError('第一步服务重启后未就绪：' + type(last).__name__) from last


def health():
    with urllib.request.urlopen('http://127.0.0.1:14203/health', timeout=2) as response:
        value = json.load(response)
    if value.get('status') != 'ok' or value.get('first_step_order_model') != 'v2':
        raise RuntimeError('第一步 API 健康状态不可核验')
    return value


def main():
    # Covers baseline, backup, shutdown, installation, restart and any recovery.
    with FileLock(str(ROOT / 'deployment-backups' / 'control-install.lock'), timeout=0):
        return _main_locked()


def write_install_state(path, state):
    """Flush intent before replacement; incomplete state requires operator review."""
    temporary = path.with_suffix('.tmp')
    with temporary.open('w', encoding='utf8') as stream:
        json.dump(state, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _main_locked():
    state_path = ROOT / 'deployment-backups' / 'control-install-state.json'
    if state_path.exists():
        previous_state = json.loads(state_path.read_text(encoding='utf8'))
        if previous_state.get('status') not in {'installed', 'rolled_back'}:
            raise RuntimeError('发现未完成的控制安装记录，须先核对现场')
    sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
    from paperflow_prefect.local_preprocessing import _control_paths
    database, _ = _control_paths()
    unknown_before = assert_control_idle(database)
    for name, (old, new) in FILES.items():
        if sha(LIVE / name) != old or sha(CANDIDATE / name) != new:
            raise RuntimeError('控制文件基线已变化：' + name)
    original_health = health()
    api = original_health['prefect_api_url']
    if (not isinstance(original_health.get('instance_id'), str) or not original_health['instance_id']
            or not api or not original_health.get('summary_index')
            or Path(original_health['summary_index']).resolve() != database.resolve()):
        raise RuntimeError('第一步 API 与控制数据库身份不一致')
    if any(not original_health.get(key) for key in ('local_intake_queue_db', 'receipt_root', 'scope')):
        raise RuntimeError('第一步 API 来源配置不可核验')
    assert_prefect_idle(api)
    expected_deployments = assert_deployments(api)
    if task_states() != {t: 'Running' for t in TASKS}:
        raise RuntimeError('第一步计划任务原本并非全部运行，未重启')
    service_processes(require_both=True)
    backup = ROOT / 'deployment-backups' / ('control-files-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
    backup.mkdir(parents=True)
    for name in FILES:
        shutil.copy2(LIVE / name, backup / name)
        if sha(backup / name) != FILES[name][0]:
            raise RuntimeError('控制文件备份哈希不一致：' + name)
    installed = []
    attempted = []
    touched_services = False
    install_state = {'status': 'active', 'phase': 'prepared', 'backup': str(backup.resolve()), 'attempted': []}
    write_install_state(state_path, install_state)
    try:
        with control_write_fence(database):
            assert_control_idle(database)
            assert_prefect_idle(api)
            if assert_deployments(api) != expected_deployments:
                raise RuntimeError('工单部署身份发生变化')
            previous = service_processes(require_both=True)
            install_state['phase'] = 'stopping'
            write_install_state(state_path, install_state)
            touched_services = True
            for task in reversed(TASKS):
                powershell('Stop', task)
            wait_stopped(previous)
            # Database admissions were fenced; direct Prefect activity is
            # separately rechecked after the old processes have exited.
            unknown_stopped = assert_control_idle(database)
            assert_prefect_idle(api)
            if unknown_stopped != unknown_before:
                raise RuntimeError('停止服务前后的未知请求数量发生变化')
            for name, (old, new) in FILES.items():
                if task_states() != {t: 'Ready' for t in TASKS} or service_processes():
                    raise RuntimeError('替换控制文件期间服务意外启动')
                if sha(LIVE / name) != old or sha(CANDIDATE / name) != new or sha(backup / name) != old:
                    raise RuntimeError('停机后控制文件或备份发生变化：' + name)
                temporary = LIVE / ('.postprocessing-update-' + name + '.tmp')
                shutil.copyfile(CANDIDATE / name, temporary)
                # Flush intent before replace. A process crash requires manual
                # reconciliation; startup must not silently repeat installation.
                attempted.append(name)
                install_state.update(phase='installing', attempted=list(attempted))
                write_install_state(state_path, install_state)
                os.replace(temporary, LIVE / name)
                installed.append(name)
                if sha(LIVE / name) != new:
                    raise RuntimeError('控制文件写入后不一致：' + name)
            if task_states() != {t: 'Ready' for t in TASKS} or service_processes():
                raise RuntimeError('替换完成前服务意外启动')
            assert_control_idle(database)
            assert_prefect_idle(api)
            install_state['phase'] = 'starting'
            write_install_state(state_path, install_state)
        # The services themselves need write access during startup.
        start_services()
        wait_ready(api, original_health, expected_deployments, database)
        assert_closed_gates(database)
        for name, (_, new) in FILES.items():
            if sha(LIVE / name) != new:
                raise RuntimeError('加载后的控制文件发生变化：' + name)
    except BaseException as primary:
        recovery = []
        if touched_services:
            try:
                with control_write_fence(database):
                    # Never interrupt newly active work just to roll files back.
                    assert_control_idle(database)
                    assert_prefect_idle(api)
                    # Only our own attempted writes may be restored. Any other
                    # change belongs to another actor and stops recovery.
                    for name, (old, new) in FILES.items():
                        allowed = {old, new} if name in attempted else {old}
                        if sha(LIVE / name) not in allowed or sha(backup / name) != old:
                            raise RuntimeError('控制文件或备份已变化，拒绝覆盖：' + name)
                    stop_services()
                    assert_control_idle(database)
                    assert_prefect_idle(api)
                    for name, (old, new) in FILES.items():
                        current = sha(LIVE / name)
                        allowed = {old, new} if name in attempted else {old}
                        if current not in allowed or sha(backup / name) != old:
                            raise RuntimeError('停机后控制文件或备份已变化：' + name)
                    for name in attempted:
                        old, new = FILES[name]
                        if sha(LIVE / name) == new:
                            temporary = LIVE / ('.postprocessing-restore-' + name + '.tmp')
                            shutil.copyfile(backup / name, temporary)
                            os.replace(temporary, LIVE / name)
                start_services()
                wait_ready(api, original_health, expected_deployments, database)
            except BaseException as exc:
                recovery.append(type(exc).__name__ + ': ' + str(exc))
        try:
            install_state.update(status='recovery_required' if recovery else 'rolled_back', phase='finished')
            write_install_state(state_path, install_state)
        except BaseException as exc:
            recovery.append('安装记录未写入：' + type(exc).__name__)
        message = '控制更新未完成：' + type(primary).__name__ + ': ' + str(primary)
        if recovery:
            message += '；自动恢复未确认：' + '; '.join(recovery)
        else:
            message += '；已恢复原控制文件及服务'
        raise RuntimeError(message) from primary
    record = {'installed': installed, 'backup': str(backup.resolve()),
              'api_restarted': True, 'gates_unchanged': True,
              'unknown_llm_requests_before_restart': unknown_before}
    try:
        install_state.update(status='installed', phase='finished')
        write_install_state(state_path, install_state)
        (backup / 'install.json').write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf8')
    except OSError as exc:
        raise RuntimeError('控制代码和服务已更新，但安装记录写入失败；不要重复安装') from exc
    print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
