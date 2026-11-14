"""Source layout only; production rules, configuration and saved identities stay intact."""
from pathlib import Path
import json
import os

ROOT = Path(__file__).resolve().parent.parent
PATHS = json.loads((ROOT / 'config/paths.json').read_text(encoding='utf-8'))
DATA_DIR = Path(os.environ.get('PAPER_UPSTREAM_DATA_DIR', PATHS['data_dir'])).resolve()
RUNTIME_DIR = Path(PATHS['runtime_dir']).resolve()
LEGACY_ROOT = ROOT / 'vendor/cleaning-pipeline'
CORE_ROOT = ROOT / 'core'
TEMPLATE_PATH = ROOT / 'resources/论文预提取_空白模板_v1.md'
ROUTE_CONFIG = ROOT / 'resources/provider-route.yml'
OFFICE_CONVERTER = ROOT / 'vendor/paper_pipeline/pipeline/libreoffice.py'


def resolve_core(path):
    """Resolve old source locations in saved records, without rewriting those records."""
    path = Path(path).resolve()
    selected=Path(os.environ.get('PAPERFLOW_SELECTED_CORE',str(CORE_ROOT))).resolve()
    if path in (Path(PATHS['original_core']).resolve(),CORE_ROOT.resolve()) or (
            path.name=='core' and path.is_relative_to((ROOT/'updates').resolve())):
        return selected
    return path
