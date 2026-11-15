"""Write concise human-readable handoff reports from completed evidence."""
import json,collections
from organize_library import WORK,SOURCE,OUTPUT,get_records


if __name__=='__main__':
    summary=json.loads((WORK/'summary.json').read_text('utf-8'))
    verification=json.loads((WORK/'verification.json').read_text('utf-8'))
    if not verification['passed']:raise RuntimeError('Do not report success before integrity verification passes')
    with (WORK/'materialization.jsonl').open(encoding='utf-8') as f:results=[json.loads(l) for l in f if l.strip()]
    reports=OUTPUT/'_整理记录';records=get_records()
    review=[r for r in results if not r['body'] and not r['excluded_only']]
    extra=[r for r in results if r['body'] and r['alternates']]
    uncertain=[r for r in records if r['role']=='uncertain']
    notes=[{'task':r['task'],**n} for r in results for n in r['notes']]
    lines=['尚未选定唯一正文的个人目录','这些资料均已保留在“待核对资料”中。需要从不同候选中确认正文。','']
    for i,r in enumerate(review,1):
        lines.extend([f'{i}. {r["folder_path"]}',f'原因：{r["selection_reason"]}'])
        for a in r['alternates']:lines.append('  候选：'+a['source']+('!/'+a['member'] if a.get('member') else ''))
        lines.append('')
    (reports/'待核对清单.txt').write_text('\n'.join(lines),encoding='utf-8-sig')
    lines=['已选出正文、同时保留其他候选的个人目录','其他候选不一定都是论文版本，可能包含尚未细分的辅助材料；没有自动覆盖所选正文。','']
    for i,r in enumerate(extra,1):
        lines.extend([f'{i}. {r["folder_path"]}',f'选择依据：{r["selection_reason"]}'])
        for a in r['alternates']:lines.append('  其他候选：'+a['source']+('!/'+a['member'] if a.get('member') else ''))
        lines.append('')
    (reports/'其他候选清单.txt').write_text('\n'.join(lines),encoding='utf-8-sig')
    lines=['保留的未确定类型文件','这些文件位于对应个人目录的“待核对文件”，不应自动当作论文正文。','']
    for r in uncertain:lines.extend([r['source']+('!/'+r['member'] if r.get('member') else ''),'原因：'+r['reason'],''])
    (reports/'文件类型待核对清单.txt').write_text('\n'.join(lines),encoding='utf-8-sig')
    exclusions={}
    for r in results:
        for a in r['actions']:
            if a['action']=='excluded':exclusions[(a['source'],a.get('container',''),a.get('member',''))]=a
    lines=['未纳入正常提取流程的材料清单','原始文件仍在源目录中；混合压缩包中的原件也可能在“_原始压缩包”备份中。','']
    for a in exclusions.values():lines.extend([a['source']+('!/'+a['member'] if a.get('member') else ''),'原因：'+a.get('reason',''), ''])
    (reports/'排除材料清单.txt').write_text('\n'.join(lines),encoding='utf-8-sig')
    (reports/'整理异常与保留说明.json').write_text(json.dumps(notes,ensure_ascii=False,indent=2),encoding='utf-8')
    schools=collections.Counter(r['school'] for r in results if r['body'])
    text=f'''总论文整理说明

原始目录：{SOURCE}
整理目录：{OUTPUT}

一、结果
扫描 {summary['personal_groups']:,} 个个人目录。
已选出正文并归入“论文资料”：{summary['body_ready']:,} 个。
多个不同候选尚不能确定唯一正文，归入“待核对资料”：{len(review):,} 个。
仅含排除材料的目录：{summary['excluded_only_groups']:,} 个，不进入正常提取流程。
已选正文的目录中，另保留其他候选供核对：{len(extra):,} 个。
尚未确定类型的文件记录：{len(uncertain):,} 条。
归档拆分或保留说明：{len(notes):,} 条，详见 _整理记录/整理异常与保留说明.json。

二、使用方式
正常提取只读取“论文资料/学校/学号_姓名/正文”中的论文和同一人的“附件”目录。
正文与附件通过同一学校、学号和个人目录关联；原始专业、班级、文件名和来源路径记在“资料清单.json”。
不要把整个整理目录递归送入模型：_原始压缩包、其他稿件待核对、待核对文件、待核对资料、_整理记录均不属于正常提取入口。
正文已统一命名为“论文.原格式”，保留文件内容；扩展名与实际文件类型不一致时只修正副本扩展名。
压缩包中的正文已经提取。纯附件包保留原包；混合包按已识别材料分离，并在 _原始压缩包 保留完整原包。
已识别的查重报告、开题报告、过程管理手册、答辩记录等材料排除在正常正文及附件入口之外。
完全相同的副本在同一人内部按 SHA256 合并；内容不同且无法可靠区分的候选均保留。

三、核验
原始目录 {verification['source_files']:,} 个文件的数量、路径、大小、修改时间及 SHA256 均与本次整理前的清单一致。
全部 {verification['output_data_files']:,} 个输出数据文件完成 SHA256 读回核验。
每个已选正文目录均恰好有一份正文；原始文件去向均有记录。
这证明复制完整性与记录覆盖，不表示已经人工通读或确认全部材料类型。待核对项已单独列明。

四、清单位置
_整理记录/待核对清单.txt：尚未选定正文的 {len(review):,} 人。
_整理记录/其他候选清单.txt：已选正文但仍有其他候选的 {len(extra):,} 人。
_整理记录/文件类型待核对清单.txt：未确定类型的文件。
_整理记录/排除材料清单.txt：被排除材料及原因。
_整理记录/完整性核验.json：全量完整性核验结果。
每人目录内的资料清单.json：该人的来源、正文选择依据、文件分类、输出路径及哈希。

五、本次范围
本次完成资料分流、复制、关联和完整性核验。尚未将正文转换为 Markdown，也未进行 LLM 画像提取、RAG 上传或 NAS 归档。

各校已选正文数量：
'''+''.join(f'{school}：{count:,}\n' for school,count in schools.items())
    (OUTPUT/'整理说明.txt').write_text(text,encoding='utf-8-sig')
    print(json.dumps({'readme':str(OUTPUT/'整理说明.txt'),'body_ready':summary['body_ready'],'review':len(review),'selected_with_alternates':len(extra),'uncertain_records':len(uncertain),'notes':len(notes)},ensure_ascii=False))
