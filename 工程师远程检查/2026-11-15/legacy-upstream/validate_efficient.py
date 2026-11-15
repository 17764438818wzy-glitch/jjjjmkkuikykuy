"""Run the configured provider on the previous slow sample; never print credentials."""
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from auto_config import load_config, load_key
from auto_pipeline import AutoProcessor
from engine import Store, digest, write_json


def main():
    installed = Path(r'E:\uv\ragflow_upstream\data')
    previous = installed / 'runs/AUTO_20260915_173804_64af1a'
    old = json.loads((previous / 'manifest.json').read_text(encoding='utf-8'))
    source = Path(old['items'][0]['source_path'])
    before = digest(source)
    root = Path(__file__).parent / 'acceptance-evidence' / ('efficient-' + datetime.now().strftime('%Y%m%d-%H%M%S'))
    config = load_config(installed); config['auto_upload'] = False
    processor = AutoProcessor(Store(root), config, load_key(installed),
                              load_key(installed, 'ai_vision_primary') or load_key(installed, 'ai_vision'),
                              load_key(installed, 'ai_vision_fallback'))
    started = time.monotonic(); last = None
    def progress(event):
        nonlocal last
        phase = event.get('stage') or event.get('progress', {}).get('stage')
        if phase and phase != last:
            print(json.dumps({'seconds': round(time.monotonic()-started, 1), 'stage': phase}, ensure_ascii=False), flush=True)
            last = phase
    result = processor.run([{'path': str(source), 'name': source.name,
                             'format': source.suffix, 'size': source.stat().st_size}], progress)
    item = result['items'][0]
    report = {'run_id': result['run_id'], 'baseline_seconds': 599, 'elapsed_seconds': round(time.monotonic()-started, 2),
              'status': item['status'], 'reason_kind': item.get('reason_kind'), 'reason': item['reason'],
              'source_unchanged': digest(source) == before, 'source_hash': before,
              'classification': item.get('classification'), 'calls': item.get('processing_metrics'),
              'billing': result['billing'], 'output_root': str(root), 'ragflow_uploaded': False}
    if item['status'] == 'pass':
        package = Path(item['package_path'])
        meta = json.loads((package / 'metadata.json').read_text(encoding='utf-8'))
        original = next((previous / 'private').glob('*/package/metadata.json'))
        oldmeta = json.loads(original.read_text(encoding='utf-8'))
        tables = [b for b in meta['blocks'] if b['type'] == 'table']
        oldtables = [b for b in oldmeta['blocks'] if b['type'] == 'table']
        values = lambda rows: [[(c['row'],c['column'],c.get('rowspan',1),c.get('colspan',1),c['text']) for c in b['cells']] for b in rows]
        ingest = (package / 'ingest.md').read_text(encoding='utf-8')
        raw_para = {json.dumps(b.get('position',{}),sort_keys=True):b['text'] for b in oldmeta['blocks'] if b['type']=='paragraph'}
        new_para = {json.dumps(b.get('position',{}),sort_keys=True):b['text'] for b in meta['blocks'] if b['type']=='paragraph'}
        report['checks'] = {'block_counts': dict(Counter(b['type'] for b in meta['blocks'])),
            'original_table_cells_exact': values(tables) == values(oldtables),
            'table_cells': sum(len(b['cells']) for b in tables),
            'native_paragraph_positions_preserved': set(raw_para) == set(new_para),
            'paragraphs_text_exact': sum(raw_para.get(k)==v for k,v in new_para.items()),
            'all_assets_exist': all((package/r['local_path']).is_file() for r in meta['resources']),
            'ocr_in_ingest': all(r.get('ocr',{}).get('text','').strip() in ingest for r in meta['resources']),
            'private_evidence_not_published': not (package/'_review').exists(),
            'issue_states': dict(Counter(i.get('status') for i in meta['issues']))}
        report['package_path'] = str(package)
    write_json(root/'result.json',report)
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)


if __name__ == '__main__': main()
