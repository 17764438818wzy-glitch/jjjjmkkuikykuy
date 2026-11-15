"""Snapshot non-secret existing settings for this isolated integration."""
from pathlib import Path
from paperflow_prefect.records import read_json, write_json, sha_file

root = Path(__file__).resolve().parent
base = Path(r'F:\论文预处理系统\程序')
release = base / 'updates/unified-AB-20261026-r05-rag600-nas-20261027-c03-normal59-llm140-r01'
settings_root = Path(r'F:\论文预处理系统\看板新工作数据-20261027')
runtime_file = Path(r'F:\论文预处理系统\预处理核心与工作台\deployments\workbench-8089-20261018\runtime\config.json')

if (root / 'config.json').exists():
    raise SystemExit('Configuration already exists; review rather than overwrite it')
old = read_json(runtime_file)
model = read_json(settings_root / 'ai_settings.json')
settings = {
    'schema': 'prefect-paperflow-config/1', 'execution_enabled': False,
    'prefect_api_url': 'http://192.168.1.120:4200/api',
    'library_root': r'F:\总论文_整理后_20261028', 'data_root': str(root / 'runtime/data'),
    'program_root': str(base), 'release_root': str(release), 'settings_root': str(settings_root),
    'template_path': str(base / 'resources/论文预提取_空白模板_v1.md'),
    'catalog_path': str(root / 'catalog.json'), 'runtime_config': str(root / 'runtime-config.json'),
    'model_config': str(root / 'model-config.json'), 'source_manifest': str(root / 'source-manifest.json'),
    'nas_destination': r'\\zwjy-nas\paper_ai\论文原件库\Prefect五步',
    'rag_url': read_json(settings_root / 'settings.json')['ragflow_url'],
    'max_pages': 160, 'max_live_papers': 160,
    'limits': {'body_llm': 100, 'attachment_llm': 20, 'total_llm': 120,
               'local': 4, 'package': 2, 'rag': 20, 'nas': 2},
    'nas_reserve_bytes': 1024**3,
}
write_json(root / 'config.json', settings)
write_json(root / 'catalog.json', read_json(old['catalog_path']))
write_json(root / 'model-config.json', {key: model[key] for key in
    ('base_url', 'text_model', 'timeout', 'max_tokens', 'preextract_limits')})
write_json(root / 'runtime-config.json', {key: old[key] for key in
    ('embedding_admission', 'embedding_model_budget')})
files = {}
for folder in (base / 'desktop', release / 'desktop', release / 'core', base / 'vendor/paper_pipeline'):
    for path in folder.rglob('*.py'):
        if path.name.startswith('test_') or '__pycache__' in path.parts: continue
        files[str(path.resolve())] = sha_file(path)
files[str((base / 'desktop/project_paths.py').resolve())] = sha_file(base / 'desktop/project_paths.py')
write_json(root / 'source-manifest.json', {'release': str(release), 'files': files})
print({'configured': str(root), 'execution_enabled': False, 'source_files_pinned': len(files)})
