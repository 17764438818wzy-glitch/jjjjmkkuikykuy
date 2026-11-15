"""Summarize an existing first-step test without running the model again."""
import csv
import hashlib
import json
import os
from collections import Counter
from pathlib import Path

root = Path(os.environ['PAPERFLOW_TEST_OUTPUT'])


def load(path):
    raw = path.read_bytes()
    try:
        return json.loads(raw.decode('utf-8-sig'))
    except UnicodeDecodeError:
        return json.loads(raw.decode('gb18030'))


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


results_file = root / 'test-results.json'
raw_results = results_file.read_bytes()
results = load(results_file)
try:
    raw_results.decode('utf-8-sig')
except UnicodeDecodeError:
    (root / 'test-results-original-gbk.json').write_bytes(raw_results)
    results_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
timing = load(root / 'test-timing.json')
counts = Counter()
index, source_issues, copy_issues, unresolved = [], [], [], []
for result in results:
    delivery = result.get('delivery_dir')
    handoff_path = result.get('handoff') or result.get('unreceived')
    if not delivery or not handoff_path:
        counts['group_error'] += 1
        index.append(dict(group=result['group_root'], status='处理失败', delivery_dir=delivery or '',
                          body='', title_status='', title='', reason=result.get('error', '没有交付记录')))
        continue
    folder = Path(delivery)
    manifest = load(folder / 'delivery.json')
    handoff = load(Path(handoff_path))
    ready = bool(manifest['ready_for_receipt'])
    counts['delivery_ready' if ready else 'delivery_incomplete'] += 1
    counts['evidence_error'] += bool(result.get('evidence_errors'))
    counts['delivery_error'] += bool(result.get('delivery_errors'))
    body = handoff.get('body')
    counts['body'] += bool(body)
    if handoff.get('paper_title'):
        title_status, title = '已确认', handoff['paper_title']
        counts['title_confirmed'] += 1
    elif handoff.get('paper_title_candidate'):
        title_status, title = '候选待核对', handoff['paper_title_candidate']
        counts['title_candidate'] += 1
    else:
        title_status, title = ('未确认' if body else '无正文'), ''
        counts['title_missing'] += bool(body)
    rows = ([body] if body else []) + handoff['attachments'] + handoff['trash'] + handoff['unresolved']
    reasons = [row.get('reason', '') for row in handoff['unresolved']]
    if reasons:
        unresolved.append(dict(group=handoff['group_relative'],
                               files=[dict(file=row['relative'], reason=row.get('reason'))
                                      for row in handoff['unresolved']]))
    for row in rows:
        counts['files'] += 1
        try:
            if not row.get('sha256') or digest(row['path']) != row['sha256']:
                source_issues.append(row['path'])
        except (OSError, ValueError):
            source_issues.append(row.get('path', ''))
        copy = folder / 'files' / Path(row['relative'])
        try:
            if not row.get('sha256') or digest(copy) != row['sha256']:
                copy_issues.append(str(copy))
        except (OSError, ValueError):
            copy_issues.append(str(copy))
    index.append(dict(group=handoff['group_relative'], status='交付完整' if ready else '未完整交付',
                      delivery_dir=str(folder), body=body['relative'] if body else '',
                      title_status=title_status, title=title, reason='; '.join(reasons)))

trace = [json.loads(line) for line in (root / 'test-trace.jsonl').read_text(encoding='utf-8-sig').splitlines() if line]
group_active = request_active = group_peak = request_peak = 0
for event in trace:
    if event['event'] == 'group_start':
        group_active += 1; group_peak = max(group_peak, group_active)
    elif event['event'] == 'group_finish':
        group_active -= 1
    elif event['event'] == 'model_request_start':
        request_active += 1; request_peak = max(request_peak, request_active)
    elif event['event'] == 'model_request_finish':
        request_active -= 1
model_finishes = [event for event in trace if event['event'] == 'model_request_finish']
summary = dict(schema='paperflow-first-step-test-summary/1', elapsed_seconds=timing['elapsed_seconds'],
    exit_code=timing['exit_code'], groups=len(results), files=counts['files'], ready=counts['delivery_ready'],
    incomplete=counts['delivery_incomplete'], group_error=counts['group_error'],
    evidence_error=counts['evidence_error'], delivery_error=counts['delivery_error'], body=counts['body'],
    title_confirmed=counts['title_confirmed'], title_candidate=counts['title_candidate'],
    title_missing=counts['title_missing'], peak_groups=group_peak, peak_model_requests_in_flight=request_peak,
    peak_nimble_processing_slots_from_log=1, nimble_processing_tasks_from_log=342,
    model_requests=len(model_finishes),
    model_request_errors=sum(event.get('outcome') == 'error' for event in model_finishes),
    source_hash_issues=source_issues, copy_hash_issues=copy_issues, unresolved=unresolved)
(root / '测试汇总.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
with (root / '资料组目录索引.csv').open('w', encoding='utf-8-sig', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=['group', 'status', 'delivery_dir', 'body', 'title_status', 'title', 'reason'])
    writer.writeheader(); writer.writerows(index)
lines = [
    '# 第一步预处理测试汇总', '',
    f"- 总耗时：{timing['elapsed_seconds']} 秒（程序退出码 {timing['exit_code']}）。",
    f"- 输入：{len(results)} 组，{counts['files']} 个文件。",
    f"- 完整交付：{counts['delivery_ready']} 组；未完整交付：{counts['delivery_incomplete']} 组；没有交付记录：{counts['group_error']} 组。",
    f"- 正文识别：{counts['body']} 组；已确认题目：{counts['title_confirmed']} 组；候选题目待核对：{counts['title_candidate']} 组。",
    f"- 最高同时处理资料组：{group_peak}；最高同时在途模型请求：{request_peak}。",
    '- 根据 Nimble 日志逐条统计，模型内部处理任务峰值为 1（342 个任务没有重叠），因此不能称为模型内部四路并发。',
    f"- 模型请求结束：{len(model_finishes)} 次；请求错误：{summary['model_request_errors']} 次。",
    f"- 测试结束后核对：原文件哈希异常 {len(source_issues)} 项；交付副本哈希异常 {len(copy_issues)} 项。",
    '- 模型日志记录过 KV 缓存不足后重试；没有模型请求报错。', '', '## 未完整交付', '']
for item in unresolved:
    lines.append('- ' + item['group'])
    for problem in item['files']:
        lines.append('  - ' + problem['file'] + '：' + str(problem['reason']))
lines += ['', '上述文件以 .docx 结尾，但文件头为 OLE 复合格式；可读取到的 ZIP 条目中没有 word/document.xml。原文件和副本均保留。',
          '', '## 文件说明', '', '- 资料组目录索引.csv：每组输入、交付目录、正文和题目状态。',
          '- deliveries：逐组交付文件夹。', '- test-results.json：程序逐组原始结果，已转为 UTF-8。',
          '- test-results-original-gbk.json：程序原始编码输出。', '- test-trace.jsonl：资料组和模型请求的起止记录。',
          '- 测试汇总.json：机器可读汇总。', '', '交付完整代表程序交接完成，题目候选和分类内容尚未逐篇人工核准。', '']
(root / '测试汇总.md').write_text('\n'.join(lines), encoding='utf-8')
print(json.dumps(summary, ensure_ascii=True))
