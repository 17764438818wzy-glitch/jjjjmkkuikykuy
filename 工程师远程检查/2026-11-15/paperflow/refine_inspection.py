import json,re,pathlib
from organize_library import WORK,get_records,inspect_one,preview,classify,RULES,dump

if __name__=='__main__':
    records=get_records();changes=[]
    for i,r in enumerate(records):
        old=(r['role'],r['reason'])
        if r['role']=='uncertain' and 'compound document' in r['reason']:
            r=inspect_one(r);records[i]=r
        if r['role'] in {'candidate','uncertain'}:
            text=r.get('text_head','');head=re.sub(r'\s+','',text)
            cover=bool(re.search(r'学士学位论文|毕业论文|本科生毕业|bachelor',head[:1000],re.I))
            if not cover and (head.startswith('附录') or (('调查问卷' in r['name'] or '问卷调查表' in r['name']) and re.search(r'单选题|多选题|本题有效填写|答卷总数|亲爱的|本问卷',text))):
                r.update(role='attachment',reason='抽查内容为独立问卷、调查统计或附录')
            if not cover and re.search(r'[_\-](?:外文原文|外文翻译|外文译文)(?:[_\-.]|$)',r['name']):
                r.update(role='attachment',reason='独立外文原文或译文附件')
        if r['role']=='uncertain' and '开头是过程材料' in r['reason']:
            info=preview(pathlib.Path(r['local_path']))
            if r.get('format')=='.pdf':
                import fitz
                with fitz.open(r['local_path']) as d:
                    full='\n'.join(d[j].get_text() for j in range(min(len(d),80)))
                    info.update(text=full[:200000],complete=len(d)<=80 and len(full)<=200000)
            role,reason=classify(r['name'],info)
            r.update(role=role,reason=reason,preview_complete=info['complete'])
            if role=='candidate':r['primary_evidence']=RULES['_primary'](info['text'],pathlib.Path(r['name']).stem)
        if (r['role'],r['reason'])!=old:changes.append({'source':r['source'],'member':r.get('member'),'old':old,'new':(r['role'],r['reason'])})
    with (WORK/'inspection.jsonl').open('w',encoding='utf-8') as f:
        for r in records:f.write(json.dumps(r,ensure_ascii=False)+'\n')
    dump(WORK/'classification-refinements.json',changes)
    print(json.dumps({'refinements':len(changes),'examples':changes[:12]},ensure_ascii=False,indent=2))
