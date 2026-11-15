"""Single desktop entry. Refuse to recover data owned by an existing live process."""
import sys
sys.dont_write_bytecode = True
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'core'))
sys.path.insert(0, str(ROOT / 'desktop'))
from project_paths import DATA_DIR, PATHS


def running_owners():
    """Read existing identity metadata; never instantiate Store or record an owner."""
    import psutil
    from model_activity import process_is_same
    owners = []
    for path in (DATA_DIR / 'private/model_activity/owners').glob('*.json'):
        try:
            owner = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            raise RuntimeError('运行身份记录暂不可读；为保护原任务，本次没有启动新实例。') from exc
        if (owner.get('schema') != 'model-activity-owner/1' or owner.get('run_id') != path.stem
                or type(owner.get('pid')) is not int or not isinstance(owner.get('create_time'), (int, float))):
            raise RuntimeError('运行身份记录无效；为保护原任务，本次没有启动新实例。')
        if owner['pid'] != os.getpid() and process_is_same(owner['pid'], owner['create_time']):
            owners.append(dict(pid=owner['pid'], create_time=owner['create_time'], run_id=owner['run_id']))
    # The original normal entry also has a native Windows mutex. This process
    # check covers idle legacy windows and the explicit 379-body Workbench entry.
    roots = [Path(PATHS['production_app']).resolve(), ROOT.resolve()]
    entry_names = {'launch.pyw', 'app.py', 'unified_launcher.py', 'continue_379_native.py',
                   'start_remaining_once.py'}
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        if proc.pid == os.getpid() or (proc.info.get('name') or '').lower() not in {'python.exe', 'pythonw.exe'}:
            continue
        for arg in (proc.info.get('cmdline') or [])[1:]:
            try:
                candidate = Path(arg).resolve()
            except (OSError, ValueError):
                continue
            if candidate.name.lower() not in entry_names:
                continue
            if (any(candidate.is_relative_to(root) for root in roots)
                    or candidate.name.lower() == 'continue_379_native.py'):
                if not any(x['pid'] == proc.pid for x in owners):
                    owners.append(dict(pid=proc.pid, create_time=proc.info['create_time'], entry=candidate.name))
                break
    return owners


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv not in ([], ['--check'], ['--monitor']):
        raise ValueError('该入口支持直接启动、--monitor 或 --check；离线验收请使用 scripts/verify_bundle.py。')
    if argv != ['--check']:
        # Select the immutable release before identity checks import a module
        # that is part of that release; never mix base and selected modules.
        from source_update import activate_source_update
        activate_source_update(ROOT)
    owners = running_owners()
    if argv == ['--check']:
        print(json.dumps(dict(start_allowed=not owners, running_owners=owners,
                              data_dir=str(DATA_DIR), files_changed=False), ensure_ascii=False))
        return 2 if owners else 0
    if owners:
        import ctypes
        ctypes.windll.user32.MessageBoxW(None,
            '现有程序或处理任务仍在运行，请继续使用当前窗口。\n\n本次没有启动新实例，也没有恢复、停止或改写任何批次。',
            '论文预处理系统', 0x40)
        return 0
    # The 379-body entry imports this module only for running_owners(). Its
    # existing sys.path and source files are never changed by the next release.
    os.environ['PAPERFLOW_LEGACY_APP'] = str(ROOT / 'desktop')
    # Native main holds the original data-dir Windows mutex for the GUI lifetime.
    from app import main as native_main
    native_main(['--monitor', '--data-dir', str(DATA_DIR)])
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        if '--check' in sys.argv:
            raise
        import ctypes
        ctypes.windll.user32.MessageBoxW(None, str(exc), '论文预处理系统启动未完成', 0x10)
