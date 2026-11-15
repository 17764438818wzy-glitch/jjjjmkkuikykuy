"""Install the Prefect observer without importing the execution engine.

The running workbench keeps validation in a bounded, isolated subprocess so it
never shares the execution engine's paperflow modules.
"""
import importlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

from .prefect_evidence import EvidenceReader
from .prefect_reader import HttpTransport, PrefectReader, install_prefect_routes, safe_text

DEFAULT_API = 'http://192.168.1.120:4200/api'
DEFAULT_DEPLOYMENT = '3c968368-58fc-40a0-9966-0ba2fde8c92c'


class _SubprocessValidator:
    def __init__(self, project, release, legacy, data_root):
        self.project, self.release, self.legacy, self.data_root = project, release, legacy, data_root

    def __call__(self, paper, receipt, prepared, scope, _refs):
        if scope != 'production':
            raise ValueError('核验范围无效')
        worker = Path(__file__).with_name('prefect_validator_worker.py')
        payload = json.dumps({'paper': paper, 'receipt': receipt, 'prepared': prepared}, ensure_ascii=True)
        allowed_env = {'SYSTEMROOT', 'WINDIR', 'PATH', 'TEMP', 'TMP'}
        environment = {k: v for k, v in os.environ.items() if k.upper() in allowed_env}
        process = subprocess.run([sys.executable, '-I', '-B', '-X', 'utf8', str(worker), str(self.project),
                                  str(self.release), str(self.legacy), str(self.data_root)], input=payload, text=True,
                                 encoding='utf8', capture_output=True, timeout=3, check=False,
                                 creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0), env=environment)
        if process.returncode != 0 or len(process.stdout) > 16 * 1024 * 1024:
            raise ValueError('隔离核验暂不可用')
        value = json.loads(process.stdout)
        return value['result'], value['errors']


def _receipt_reader(project):
    package = project / 'paperflow_prefect'
    name = '_paperflow_readonly_evidence'
    existing = sys.modules.get(name)
    if existing is not None:
        if Path(existing.__file__).resolve() != (package / '__init__.py').resolve():
            raise ValueError('证据模块路径冲突')
    else:
        spec = importlib.util.spec_from_file_location(name, package / '__init__.py',
            submodule_search_locations=[str(package)])
        if spec is None or spec.loader is None:
            raise ValueError('证据模块不可用')
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(name, None)
            raise
    return importlib.import_module(name + '.stage3_handoff').read_receipt


def _api_url(value):
    value = value if isinstance(value, str) else ''
    parsed = urlsplit(value)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password
        or parsed.query or parsed.fragment):
        return DEFAULT_API
    return value.rstrip('/')


def make_reader():
    project = Path(os.environ.get('PAPERFLOW_PREFECT_PROJECT',
        str(Path(__file__).resolve().parents[2] / '预处理系统2' / 'prefect-paperflow'))).resolve()
    try:
        config_path = project / 'config.json'
        if config_path.stat().st_size > 1024 * 1024:
            raise ValueError('配置过大')
        config = json.loads(config_path.read_text(encoding='utf8'))
        if not isinstance(config, dict):
            raise ValueError('配置无效')
    except (OSError, ValueError, TypeError):
        config = {}
    evidence = None
    try:
        validator = None; expected_core = None
        release_value = config.get('release_root')
        legacy = Path(__file__).resolve().parents[1] / 'legacy_bundle' / 'desktop'
        if isinstance(release_value, str) and not release_value.startswith(('\\\\', '//')):
            release = Path(release_value).resolve()
            if ((release / 'core' / 'paperflow' / 'package.py').is_file()
                and (release / 'desktop' / 'preextract_publish.py').is_file()
                and (legacy / 'project_paths.py').is_file()):
                validator = _SubprocessValidator(project, release, legacy, Path(config['data_root']).resolve())
                expected_core = release / 'core'
        evidence = EvidenceReader(config['receipt_root'], config['classification_root'], config['data_root'],
                                  _receipt_reader(project), validator=validator, expected_core=expected_core,
                                  library_root=config['library_root'])
    except (OSError, ValueError, TypeError, KeyError, ImportError):
        pass
    url = _api_url(os.environ.get('PAPERFLOW_PREFECT_API_URL') or config.get('prefect_api_url') or DEFAULT_API)
    deployment = os.environ.get('PAPERFLOW_PREFECT_DEPLOYMENT_ID') or DEFAULT_DEPLOYMENT
    try:
        return PrefectReader(HttpTransport(url), deployment, evidence)
    except ValueError:
        return PrefectReader(HttpTransport(url), DEFAULT_DEPLOYMENT, evidence)


def install_prefect_observer(app, resources):
    reader = make_reader()
    install_prefect_routes(app, reader, host_provider=resources.sample)
    try:
        project = Path(os.environ.get('PAPERFLOW_PREFECT_PROJECT',
            str(Path(__file__).resolve().parents[2] / '预处理系统2' / 'prefect-paperflow'))).resolve()
        _receipt_reader(project)
        summary = importlib.import_module('_paperflow_readonly_evidence.group_task_summary')
        configured_root = os.environ.get('PAPERFLOW_FIRST_STEP_OUTPUT_ROOT')
        if configured_root:
            summary_root = Path(configured_root).resolve()
        elif reader.evidence is not None:
            summary_root = reader.evidence.handoff_root
        else:
            config = json.loads((project / 'config.json').read_text(encoding='utf8'))
            summary_root = Path(config['classification_root']).resolve()
        summary.install_group_summary_routes(app, summary_root, sanitize=safe_text)
    except (OSError, ValueError, TypeError, KeyError, ImportError, AttributeError):
        from fastapi import HTTPException

        @app.get('/api/v1/first-step/tasks/{task_id}')
        def first_step_unavailable(task_id: str):
            raise HTTPException(status_code=503, detail='第一步统计暂不可用')
