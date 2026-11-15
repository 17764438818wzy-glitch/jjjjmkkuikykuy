"""Stage named copies of the twelve authorized bodies; never change originals."""
from pathlib import Path
import json

from title_llm_supplement import scoped_targets
from paperflow_prefect.named_delivery import materialize_named_input, resolved_delivery_row
from paperflow_prefect.records import sha_file
from paperflow_prefect import legacy
from paperflow_prefect.settings import Settings


candidate = Path(__file__).resolve().parent
legacy.bind(Settings.load())
project = candidate.parent.parent / 'prefect-paperflow'
data = project / 'runtime/data'
targets = {row['paper_id']: row for row in scoped_targets(project, candidate)}
invs = {}
for path in data.glob('unified-papers/*/*/*/received-inventory.json'):
    row = json.loads(path.read_text(encoding='utf8'))
    if row.get('paper_id') in targets:
        if row['paper_id'] in invs:
            raise ValueError('接收记录不唯一')
        invs[row['paper_id']] = row
if set(invs) != set(targets):
    raise ValueError('接收记录不完整')

root = candidate / 'staged-generated-title-inputs'
root.mkdir(parents=True, exist_ok=True)
manifest_path = root / 'manifest.json'
records = []
for paper_id in sorted(targets, key=lambda x: targets[x]['group']):
    target, inventory = targets[paper_id], invs[paper_id]
    original = Path(target['original_path'])
    body = dict(inventory['body'], path=str(original))
    paper = dict(inventory, body=body)
    named_row = resolved_delivery_row(paper, body, data)
    if named_row.get('title_origin') != 'model_generated':
        raise ValueError('生成题目无法与原稿核对：' + target['group'])
    named = materialize_named_input(paper, named_row, root / 'papers' / paper_id)
    if named is None or sha_file(named) != target['source_sha256'] or sha_file(original) != target['source_sha256']:
        raise ValueError('命名副本或原稿核对失败：' + target['group'])
    records.append({'group': target['group'], 'paper_id': paper_id, 'source_sha256': target['source_sha256'],
                    'original': str(original), 'named_copy': str(named), 'title_origin': 'model_generated'})
manifest = {'schema': 'generated-title-staged-inputs/1', 'scope': '12 authorized bodies',
            'installed_in_running_program': False, 'records': records}
if manifest_path.exists():
    if json.loads(manifest_path.read_text(encoding='utf8')) != manifest:
        raise ValueError('已有清单与当前身份不符')
else:
    with manifest_path.open('x', encoding='utf8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
print(json.dumps({'staged': len(records), 'manifest': str(manifest_path)}, ensure_ascii=False))
