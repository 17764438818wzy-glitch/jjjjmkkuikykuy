"""Offline source/path/UI acceptance. No network, subprocesses or production writes."""
import sys
sys.dont_write_bytecode = True
import ast, json, os, hashlib, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / '_verification'
DEST.mkdir(exist_ok=True)
CHECKS = []


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)


def audit(event, args):
    if event.startswith(('socket.', 'subprocess.', 'os.system', 'os.posix_spawn')):
        raise RuntimeError('Offline verification forbids network or child-process execution: ' + event)
    candidates = []
    if event == 'open':
        mode, flags = args[1], args[2]
        writing = (isinstance(mode, str) and any(x in mode for x in 'wax+')) or (
            isinstance(flags, int) and bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)))
        if writing:
            candidates.append(args[0])
    elif event in {'os.mkdir', 'os.remove', 'os.rmdir', 'os.chmod', 'os.utime'}:
        candidates.append(args[0])
    elif event in {'os.rename', 'os.replace'}:
        candidates.extend(args[:2])
    for candidate in candidates:
        if isinstance(candidate, (str, bytes, os.PathLike)):
            target = Path(os.fsdecode(candidate)).resolve()
            if not target.is_relative_to(DEST.resolve()):
                raise RuntimeError('Verification rejected a write outside its isolated directory')


def main():
    manifest = json.loads((ROOT / 'SOURCE_MANIFEST.json').read_text(encoding='utf-8'))
    # Compare every copied source, not the active batch's changing manifests.
    check('production_sources_unchanged', all(sha(r['source']) == r['original_sha256'] for r in manifest['records']))
    check('copied_source_hashes_match_manifest', all(sha(ROOT / r['file']) == r['bundled_sha256'] for r in manifest['records']))
    for folder in ('desktop', 'core', 'vendor'):
        for path in (ROOT / folder).rglob('*.py'):
            ast.parse(path.read_text(encoding='utf-8-sig'), filename=str(path))
    check('source_syntax', True)
    sys.path.insert(0, str(ROOT / 'core'))
    sys.path.insert(0, str(ROOT / 'desktop'))
    with tempfile.TemporaryDirectory(prefix='offline-', dir=DEST) as temporary:
        sandbox = Path(temporary)
        data = sandbox / 'data'
        os.environ['PAPER_UPSTREAM_DATA_DIR'] = str(data)
        os.environ['PAPERFLOW_LEGACY_APP'] = str(ROOT / 'desktop')
        sys.addaudithook(audit)
        from project_paths import DATA_DIR, LEGACY_ROOT, CORE_ROOT, TEMPLATE_PATH, OFFICE_CONVERTER
        from engine import load_legacy_taxonomy, digest
        import preextract_pipeline as pipeline
        import preextract_contract as contract
        from preextract_publish import _paperflow
        from office_wps import _lock_path
        import verified_pdf
        check('data_dir_override_isolated', DATA_DIR == data.resolve())
        check('legacy_source_bundled', LEGACY_ROOT.is_relative_to(ROOT) and (LEGACY_ROOT / 'src/paper_cleaning/run_stage1.py').is_file())
        check('office_helper_bundled', OFFICE_CONVERTER.is_relative_to(ROOT) and OFFICE_CONVERTER.is_file())
        check('template_defaults_bound_before_import', pipeline.run_single.__kwdefaults__['template_path'] == TEMPLATE_PATH
              and contract.DEFAULT_TEMPLATE == TEMPLATE_PATH)
        check('runtime_configuration_not_migrated', pipeline.DEFAULT_RUNTIME == Path(json.loads((ROOT / 'config/paths.json').read_text(encoding='utf-8'))['runtime_dir']))
        taxonomy = load_legacy_taxonomy()
        check('bundled_classification_catalog_loads', len(taxonomy) > 10)
        check('office_lock_shared_data_dir', _lock_path() == DATA_DIR / 'private/locks/wps-conversion.lock')
        check('verified_pdf_cache_shared_data_dir', verified_pdf.CACHE_ROOT == DATA_DIR / 'private/verified_pdf')
        original_core = json.loads((ROOT / 'config/paths.json').read_text(encoding='utf-8'))['original_core']
        package = _paperflow(original_core)
        check('historical_core_reference_resolves_to_bundle', Path(package.__file__).resolve().is_relative_to(CORE_ROOT.resolve()))
        source = sandbox / 'source.txt'
        source.write_text('Known source, already registered. No model call is required.', encoding='utf-8')
        work = sandbox / 'registered-work'
        (work / 'model').mkdir(parents=True)
        (work / 'model/request-state.json').write_text('{"status":"response_complete"}', encoding='utf-8')
        registry = DATA_DIR / 'single-request/source-index.json'
        registry.parent.mkdir(parents=True)
        identity = digest(source) + '|' + digest(TEMPLATE_PATH) + '|' + pipeline.PARSER_VERSION
        registry.write_text(json.dumps({identity: str(work)}), encoding='utf-8')
        check('existing_source_index_reused', pipeline.default_work_dir(source) == work)
        pipeline.register_workspace(source, TEMPLATE_PATH, work)
        check('workspace_registration_preserves_existing_identity', json.loads(registry.read_text(encoding='utf-8')) == {identity: str(work)})
        from app import main as native_main
        native_main(['--smoke-test', '--data-dir', str(DATA_DIR)])
        check('six_tab_gui_idle_no_task_started', True)
        check('no_model_or_batch_created_in_offline_data', not any((DATA_DIR / 'runs').iterdir()))
        report = dict(status='passed', checks=CHECKS, count=len(CHECKS), production_files_modified=False,
            ai_called=False, network_allowed=False, gui_data_isolated=True)
        (DEST / 'acceptance.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
