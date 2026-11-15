"""Create a frozen, importable candidate; never touch the running services."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
stage_name = sys.argv[1] if len(sys.argv) > 1 else 'llm-flow-staged-20261031'
if not re.fullmatch(r'[A-Za-z0-9_-]+', stage_name):
    raise ValueError('invalid staging name')
DESTINATION = ROOT / 'candidates' / stage_name


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    DESTINATION.mkdir(parents=True, exist_ok=False)
    sources = {}
    package_names = ('attachment_raw_input', 'folder_completion', 'folder_ledger', 'frozen_body_plan',
        'handoff_contract', 'image_evidence', 'model_limits', 'pause_admission', 'raw_partition',
        'raw_request', 'segment_plan_contract', 'upstream_flow_binding', 'flow_executor',
        'flow_model_operations', 'flow_runtime')
    alternatives = {'folder_ledger':'flow_folder_ledger', 'raw_request':'flow_raw_request',
                    'pause_admission':'flow_pause_admission'}
    mappings = [(HERE / (alternatives.get(name, name) + '.py'), DESTINATION / 'llm_queue' / (name + '.py'))
                for name in package_names]
    mappings += [
        (HERE / 'flow_local_preprocessing.py', DESTINATION / 'local_preprocessing.py'),
        (HERE / 'run_unified_folder_worker.py', DESTINATION / 'run_unified_folder_worker.py'),
        (HERE / 'flow_prefect_orders.py', DESTINATION / 'intake/prefect_orders.py'),
        (HERE / 'flow_queue_summary.py', DESTINATION / 'intake/llm_queue_summary.py'),
        (HERE / 'flow_waiting_api.py', DESTINATION / 'intake/waiting_api.py'),
        (HERE / 'flow_app.js', DESTINATION / 'ui/app.js'),
        (HERE / 'flow_index.html', DESTINATION / 'ui/index.html'),
        (HERE / 'flow_prefect_ticket_counts.js', DESTINATION / 'ui/prefect-ticket-counts.js'),
    ]
    for name in ('first_step_control', 'first_step_delete', 'first_step_scheduler',
                 'group_summary_store', 'llm_admission_state'):
        mappings.append((HERE / 'flow_control' / (name + '.py'),
                         DESTINATION / 'control' / (name + '.py')))
    for source, target in mappings:
        target.parent.mkdir(parents=True, exist_ok=True)
        before = sha(source)
        shutil.copyfile(source, target)
        if sha(source) != before or sha(target) != before:
            raise RuntimeError('candidate changed while staging: ' + str(source))
        sources[str(source.relative_to(ROOT))] = before

    (DESTINATION / 'llm_queue/__init__.py').write_text('"""Folder LLM stage."""\n', encoding='utf8')
    relative = {
        'folder_ledger.py': ('folder_completion',),
        'frozen_body_plan.py': ('segment_plan_contract',),
        'raw_partition.py': ('raw_request', 'segment_plan_contract'),
        'upstream_flow_binding.py': ('handoff_contract', 'folder_ledger'),
        'flow_model_operations.py': ('attachment_raw_input', 'folder_completion', 'frozen_body_plan',
            'image_evidence', 'model_limits', 'raw_partition', 'raw_request', 'segment_plan_contract'),
        'flow_runtime.py': ('flow_executor', 'flow_model_operations', 'folder_ledger',
                            'pause_admission', 'upstream_flow_binding'),
    }
    for filename, names in relative.items():
        target = DESTINATION / 'llm_queue' / filename
        text = target.read_text(encoding='utf8')
        for name in names:
            needle = 'from ' + name + ' import '
            if needle not in text:
                raise RuntimeError(f'missing relative import target {filename}: {name}')
            text = text.replace(needle, 'from .' + name + ' import ')
        if filename == 'upstream_flow_binding.py':
            needle = 'from native_order import MARKER'
            if needle not in text:
                raise RuntimeError('missing Prefect marker')
            text = text.replace(needle, "MARKER = 'paperflow-order-v2-live-20261031'")
        target.write_text(text, encoding='utf8', newline='\n')

    target = DESTINATION / 'local_preprocessing.py'
    text = target.read_text(encoding='utf8')
    needle = 'from .segment_plan_contract import describe_plan'
    if needle not in text:
        raise RuntimeError('upstream plan binding not present')
    target.write_text(text.replace(needle, 'from .llm_queue.segment_plan_contract import describe_plan'), encoding='utf8', newline='\n')
    target = DESTINATION / 'intake/waiting_api.py'
    text = target.read_text(encoding='utf8')
    if 'from flow_queue_summary import summarize_llm' not in text:
        raise RuntimeError('LLM queue summary not present')
    target.write_text(text.replace('from flow_queue_summary import summarize_llm',
                                   'from llm_queue_summary import summarize_llm'), encoding='utf8', newline='\n')
    manifest = {'sources': sources, 'staged': {str(p.relative_to(DESTINATION)): sha(p)
        for p in DESTINATION.rglob('*') if p.is_file()}}
    (DESTINATION / 'stage-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf8')
    print('staged files:', len(manifest['staged']))
    print('path:', DESTINATION)


if __name__ == '__main__':
    main()
