"""Read-only regression of original extraction; no AI and no approved publication."""
import json
import time
from collections import Counter
from pathlib import Path
from evidence import build_package,sha
from evidence_tasks import check_structure


def main():
    previous=Path(r'E:\uv\ragflow_upstream\data\runs\AUTO_20260915_173804_64af1a\private')
    source=next(previous.glob('*/converted/source.docx'))
    original=next(previous.glob('*/package/metadata.json'))
    old=json.loads(original.read_text(encoding='utf-8'))
    output=Path(__file__).parent/'acceptance-evidence/native-regression/private/package'
    before=sha(source);started=time.monotonic()
    meta=build_package(source,output,{'id':'local_native_regression','title':old['title'],'major':'会计学'},redact_content=False)
    check_structure(meta)
    def cells(m):
        return [[(c['row'],c['column'],c.get('rowspan',1),c.get('colspan',1),c['text']) for c in b['cells']] for b in m['blocks'] if b['type']=='table']
    paragraphs=lambda m:[b['text'] for b in m['blocks'] if b['type']=='paragraph']
    result={'mode':'native_extraction_only_no_ai_approval','elapsed_seconds':round(time.monotonic()-started,2),
        'source_unchanged':sha(source)==before,'block_counts':dict(Counter(b['type'] for b in meta['blocks'])),
        'paragraphs_exact':paragraphs(meta)==paragraphs(old),'table_cells_exact':cells(meta)==cells(old),
        'table_cells':sum(len(c) for c in cells(meta)),
        'resource_bytes_exact':sorted(r['original_sha256'] for r in meta['resources'])==sorted(r['original_sha256'] for r in old['resources']),
        'issues_before':dict(Counter(i['code'] for i in old['issues'])),
        'issues_after':dict(Counter(i['code'] for i in meta['issues'])),
        'output_is_private':True}
    report=output.parents[1]/'result.json'
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))
    assert result['source_unchanged'] and result['paragraphs_exact'] and result['table_cells_exact'] and result['resource_bytes_exact']


if __name__=='__main__': main()
