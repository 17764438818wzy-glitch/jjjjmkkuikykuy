"""Explicit, pinned access to business functions; never start old controllers."""
import importlib
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
from .records import read_json, sha_file

_binding_lock = threading.Lock()
_bound = None


def bind(settings):
    global _bound
    release, base = settings.path('release_root'), settings.path('program_root')
    with _binding_lock:
        if _bound is not None:
            if _bound != (release, base): raise RuntimeError('Cannot mix native releases')
            return
        manifest = read_json(settings.path('source_manifest'))
        for path, digest in manifest['files'].items():
            if sha_file(path) != digest: raise RuntimeError('Native source changed; re-review required: ' + path)
        if any(name in sys.modules for name in ('auto_pipeline', 'preextract_pipeline', 'paperflow')):
            raise RuntimeError('Use a fresh process for this isolated adapter')
        os.environ['PAPERFLOW_SELECTED_CORE'] = str(release / 'core')
        os.environ['PAPERFLOW_BASE_ROOT'] = str(base)
        os.environ['PAPER_UPSTREAM_DATA_DIR'] = str(settings.path('data_root'))
        sys.path[:0] = [str(release / 'desktop'), str(base / 'desktop'), str(release / 'core')]
        paths = read_json(base / 'config/paths.json')
        dependencies = Path(paths['original_core']) / '.venv/Lib/site-packages'
        if dependencies.is_dir(): sys.path.append(str(dependencies))
        _bound = (release, base)


def connection(settings):
    # Read the existing Windows credential only after a real run is authorized.
    settings.require_execution(); bind(settings)
    from auto_config import load_key
    from preextract_workbench import model_connection
    config = read_json(settings.path('model_config'))
    key = load_key(settings.path('settings_root'))
    if not key: raise ValueError('模型凭据不可读')
    agent = SimpleNamespace(connection=lambda: (config['base_url'], key))
    processor = SimpleNamespace(config=config, agent=agent,
        store=SimpleNamespace(root=settings.path('data_root')), _resume_root=settings.path('data_root'))
    return model_connection(processor)


def rag_client(settings):
    settings.require_execution(); bind(settings)
    from auto_config import load_key
    from ragflow_client import RagflowClient
    return RagflowClient(settings['rag_url'], load_key(settings.path('settings_root'), 'ragflow'))


def preflight(settings):
    bind(settings)
    names = ['preextract_material', 'preextract_contract', 'preextract_request',
             'preextract_images', 'preextract_partition_run', 'preextract_publish',
             'paperflow.embedding_counter', 'paperflow.rag',
             'paperflow.original_archive.archive', 'paperflow.original_archive.lifecycle']
    loaded = {name: importlib.import_module(name).__file__ for name in names}
    runtime = read_json(settings.path('runtime_config'))
    counter = runtime['embedding_admission']['counter']
    paths = {key: Path(counter[key]).exists() for key in
             ('model_path', 'library_path', 'cache_dir', 'python_path', 'runtime_manifest')}
    return {'mode': 'connection-check', 'execution_enabled': settings['execution_enabled'],
        'modules': loaded, 'counter_paths': paths,
        'template_exists': settings.path('template_path').is_file(),
        'catalog_exists': settings.path('catalog_path').is_file(),
        'configured_limits': settings['limits'],
        'credentials_loaded': False, 'real_papers_read': 0, 'business_external_calls': 0,
        'nas_probed': False, 'production_acceptance': False,
        'local_retention': True}
