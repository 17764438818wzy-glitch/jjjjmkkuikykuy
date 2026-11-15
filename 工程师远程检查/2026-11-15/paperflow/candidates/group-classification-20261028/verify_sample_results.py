"""Compare a saved isolated sample run with its earlier reference and source hashes."""
import argparse
import csv
import json
import os
from collections import Counter
from pathlib import Path

from paperflow_prefect.group_classification import verify_handoff
from paperflow_prefect.group_sort_cli import _read_for_sort


EVALUATION = Path(r'F:\论文预处理系统\预处理系统2\evaluations\nimble-50-20261028')


def read_lines(path):
    return [json.loads(line) for line in path.read_text(encoding='utf8').splitlines() if line.strip()]


def normalized(path):
    return os.path.normcase(os.path.abspath(path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('results')
    parser.add_argument('summary')
    parser.add_argument('table')
    args = parser.parse_args()
    samples = json.loads((EVALUATION / 'final-selection.json').read_text(encoding='utf8'))['samples']
    numbers = {normalized(sample['destination_dir']): sample['number'] for sample in samples}
    evidence = read_lines(EVALUATION / 'classification-v1-20261028/evidence.jsonl')
    labels = {row['id']: row['reference'] for row in
              read_lines(EVALUATION / 'classification-group-v1-20261028/reference-labels.jsonl')}
    previous = {normalized(row['path']): dict(row, reference=labels[row['id']]) for row in evidence}
    by_group = {}
    for row in previous.values():
        by_group.setdefault(row['group'], []).append(row)
    results = json.loads(Path(args.results).read_text(encoding='utf-8-sig'))
    counts, table, mismatches, errors = Counter(), [], [], []
    for item in results:
        number = numbers.get(normalized(item['group_root']))
        if number is None or 'error' in item:
            errors.append({'group': number, 'error': item.get('error', '未找到此前资料组')})
            continue
        file = item.get('handoff') or item.get('unreceived')
        if not file:
            errors.append({'group': number, 'error': '没有保存交接或未接收记录'})
            continue
        h = json.loads(Path(file).read_text(encoding='utf-8-sig'))
        try:
            verified = verify_handoff(h, allow_incomplete='unreceived' in item,
                                      evidence_reader=_read_for_sort)
        except Exception as exc:
            errors.append({'group': number, 'error': type(exc).__name__ + ': ' + str(exc)})
            continue
        counts['groups'] += 1
        counts['verified_complete'] += bool(verified)
        counts['unreceived'] += 'unreceived' in item
        actual = []
        if h['body']:
            actual.append(('body', h['body']))
        for kind, key in [('attachment', 'attachments'), ('trash', 'trash'), ('unresolved', 'unresolved')]:
            actual.extend((kind, row) for row in h[key])
        actual_paths = {normalized(row['path']) for _, row in actual}
        expected_paths = {normalized(row['path']) for row in by_group[number]}
        if actual_paths != expected_paths:
            errors.append({'group': number, 'error': '交接文件与原始样本清单不一致'})
        expected_body = next(row for row in by_group[number] if row['reference'] == 'body')
        chosen = h['body']['path'] if h['body'] else None
        body_match = bool(chosen and normalized(chosen) == normalized(expected_body['path']))
        counts['bodies_selected'] += bool(chosen)
        counts['bodies_match_reference'] += body_match
        counts['unresolved_files'] += len(h['unresolved'])
        counts['explicit_title'] += bool(h['paper_title'])
        counts['candidate_title'] += bool(h['paper_title_candidate'])
        counts['missing_title'] += not (h['paper_title'] or h['paper_title_candidate'])
        for kind, row in actual:
            old = previous.get(normalized(row['path']))
            counts['files'] += 1
            if not old or old['group'] != number:
                errors.append({'group': number, 'error': '文件没有属于该组的历史来源'})
                continue
            if row['sha256'] != old['sha256']:
                errors.append({'group': number, 'error': '当前记录指纹与历史样本不同', 'file': row['relative']})
            else:
                counts['files_match_original_hash'] += 1
            if kind != 'unresolved':
                counts['files_classified'] += 1
                if kind == old['reference']:
                    counts['files_match_reference'] += 1
                else:
                    mismatches.append({'group': number, 'file': row['relative'],
                                       'reference': old['reference'], 'current': kind})
        table.append({'组号': number, '资料组': Path(h['group_root']).name,
                      '历史正文': Path(expected_body['path']).name,
                      '本次正文': Path(chosen).name if chosen else '',
                      '正文是否一致': '是' if body_match else '待核对' if not chosen else '否',
                      '题目': h['paper_title'] or '', '候选题目需核对': h['paper_title_candidate'] or '',
                      '题目备注': h['title_note'] or '', '附件数': len(h['attachments']),
                      '垃圾数': len(h['trash']), '未判清数': len(h['unresolved']),
                      '交接核验': '通过' if verified else '未接收', '清单路径': file})
    summary = {'counts': dict(counts), 'mismatches': mismatches, 'errors': errors,
               'note': '历史参考用于本批对照；题目候选及类别一致不代表人工逐条复核或总体准确率。'}
    with Path(args.summary).open('x', encoding='utf8') as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2)
    if table:
        with Path(args.table).open('x', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(table[0]))
            writer.writeheader()
            writer.writerows(sorted(table, key=lambda row: row['组号']))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
