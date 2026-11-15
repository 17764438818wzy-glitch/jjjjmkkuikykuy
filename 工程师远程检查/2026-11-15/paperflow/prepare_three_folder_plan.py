"""Evidence-based routing into the three folders requested by the user."""
import json,pathlib,re,unicodedata
from collections import Counter,defaultdict
import organize_library as o
import review_pending_library as p

PHASE=o.WORK/'三目录重整20261028'
ATTACHMENT_PREFIXES='''784c53bb61d4 ab78bef39a6e 56419dd3043e 8dd4de1038a1 469b55f97c94 0e2f9b9438df c5be46fd3ced b1f0dc4c136b f3522e7ee590 4ec3fad3f547 df361a113935 55b5fe430fe3 f9634c12fee2 e707b4c7f7fe 1a259865c2e5 540a69d2601d cc721e01844b 0886ead5085f 507f4581972f 360c080d2f70 6c12ee35d7f6 ac91155106c2 62ef4e0116c2 f8480fb1fd5a dbe29f8d3599 e222f0852dbc 7f3f0a12ca7d 1c228855de7f 2e8c1d6ee4aa 1e67b5001318 8c4f9f5e9e51 4ba703604052 1be1647e6255 1ca9cf64a039 d01ec4ea4005 ba97230cc4bb 1f48a8ed3437 b680c474814a 8b166d7fb70a 7d2d3393abf3 bde0316b589c a2094353eb30 f0e0ca351f7b'''.split()
TRASH_PREFIXES='''ed181e4dc123 d072e21e5a22 e573604a1021 34e862f3c4e4 bc49e102e0b2 b8491303e9b0 4a1d4b8bfbe4 9c2bc13ce5fe be509c383779 7dfa3871e64c abbb5b620bd6 d8ddf19b2216 5d075e642709 48fc44973988 6b4d1b8cb743 e11069c5643a d018f7452098 c48c417d04f4 13a2e7fff942 e7a3dcdfc416 d170ff3266c6 be9e4cce1cc9 85e44dec4ab1 3d7cb5715b88 9087c73b04bc 2e1c9fac71cc f910559a51b8 29f1c5700022'''.split()
BODY_OVERRIDE={
0:'9d299e82f42c',1:'258441ae82b1',2:'870fc9161774',3:'c65b42b29fe6',4:'dd95de618835',6:'6ef4de73a6f8',7:'22bd9669e368',13:'54c1b27162ca',18:'2f45942f7155',
34:'b33d424a08f0',67:'1bd89b1d3a9b',70:'ebb7be185902',100:'b6618b1a94b9',
84:'3671a8d43a1d',185:'46988937824b',195:'36d92c215dfc',224:'ed4a051b96a1',234:'0cd9893b92df',
190:'0ee5fa4d8385',191:'c7ca310bb65b',199:'7f120a439d11',200:'70b73ba962a0',213:'7577c4ab8828',215:'727af1781c33',219:'5173b89b57f8',221:'9b8d48f82fe8',226:'c6093d1923a0',231:'087a5374470d',233:'13c9fdeda78c',241:'e2169e325aca',242:'3b25c0a2fe4d',243:'7d6415407dc9',245:'6aa92c71443e',250:'e5cbbe1c634b'
}

def textof(r):
    e=r.get('extraction',{})
    return pathlib.Path(e['text_path']).read_text('utf-8') if e.get('text_path') else r.get('text_head','')

def compact(text):return re.sub(r'\s+','',unicodedata.normalize('NFKC',text))

def obvious(r):
    sha=r['sha256'];name=r['name'];t=textof(r);h=compact(t[:8000]);stem=pathlib.Path(name).stem
    if any(sha.startswith(x) for x in ATTACHMENT_PREFIXES):return '附件','已逐份复核的参考文献、综述、数据或程序附件'
    if any(sha.startswith(x) for x in TRASH_PREFIXES):return '垃圾材料','已逐份复核的查重、过程或答辩发言材料'
    if sha.startswith('6abee51b8b30'):return '附件','同一设计的高清图册，图片分辨率高于正文中的配图'
    if re.search(r'(?:图纸清单|图纸目录|设备表|程序清单|代码清单|^附件[_ _])',stem):return '附件','配套图纸清单、设备表或独立附件'
    if r.get('forced_exclude'):return '垃圾材料',r['forced_exclude']
    if re.search(r'(?:全文标明引文报告|原文标注报告|原文对照报告|片段对照报告|简明报告|文本复制检测报告|格式分析报告)',h[:500]) or (re.search(r'Paper(?:Free|Red|Era)|大雅',h[:1500],re.I) and re.search(r'报告|相似比|查重|相似度',h[:1500])):
        return '垃圾材料','首部明确为检测报告'
    if re.search(r'^(?:各位|尊敬的).{0,25}老师.{0,12}好',h[:180]):return '垃圾材料','答辩发言稿'
    if re.search(r'^(?:(?:本科)?毕业(?:设计|论文)(?:\((?:论文|设计)\))?(?:工作|过程管理)?手册|实习手册)',h[:120]):return '垃圾材料','首部明确为实习或毕业过程手册，另行保留论文正文'
    if len(p.normalize(t))<350 and '封面' in stem:return '附件','独立论文封面，保留作者学号和题目'
    if re.search(r'外文翻译|外文译文',h[:100].split('摘要')[0]):return '附件','独立外文材料'
    if re.search(r'^(?:代码|代码清单|程序代码|程序设计说明书)(?:[_（(\s].*)?$',stem):return '附件','程序源码或程序说明'
    e=r.get('extraction',{});pages=e.get('page_info',[])
    if pages and sum(x['width']>x['height']*1.15 for x in pages)/len(pages)>.7:return '附件','逐页尺寸及内容确认的横版图纸或设计展示'
    if r['role']=='exclude':return '垃圾材料',r['reason']
    if r['role']=='attachment':return '附件',r['reason']
    return None,None

def preferred(cs):
    def rank(r):
        name=r['name'];e=r.get('extraction',{})
        final=bool(re.search(r'终稿|定稿|最终|最后一次',name))
        return (final,r.get('format') in {'.docx','.doc'},r.get('format')=='.docx',e.get('normalized_chars',0),-len(name))
    return max(cs,key=rank)

def pending():
    groups=json.loads((p.PHASE/'groups.json').read_text('utf-8'));result=[];unresolved=[]
    for i,g in enumerate(groups):
        cs=g['candidates'];routes={};selected=None;reason=''
        for r in cs:
            cat,why=obvious(r)
            if cat:routes[r['sha256']]={'category':cat,'reason':why}
            if r['sha256'].startswith(('b243253f8c9c','5e66cfddf529')):routes[r['sha256']]={'category':'附件','reason':'程序说明或独立毕业创作成果'}
        # These PDF sets were reviewed as drawing/design books beside a separate thesis.
        if i in set(range(15,42))|set(range(42,67))|{68,69,73,152}|set(range(121,131)):
            for r in cs:
                if r['format']=='.pdf' and i not in {28,34}:
                    routes[r['sha256']]={'category':'附件','reason':'内容和版面复核：图纸、设计文本或展示作品，另有论文正文'}
        if 155<=i<=184:
            theses=[r for r in cs if '本科毕业论文' in compact(textof(r))[:120] and '外文翻译' not in compact(textof(r))[:180]]
            if len(theses)==1:
                selected=theses[0];reason='论文与毕业设计并存：论文作正文，独立设计作附件'
                for r in cs:
                    if r is not selected:routes[r['sha256']]={'category':'附件','reason':'独立毕业设计说明书或外文材料'}
        if i==28:
            selected=next(r for r in cs if r['sha256'].startswith('dfd97feab493'));reason='长篇论文为正文，施工图集作附件'
            routes[next(r['sha256'] for r in cs if r is not selected)]={'category':'附件','reason':'施工图集'}
        if i in BODY_OVERRIDE:
            selected=next(r for r in cs if r['sha256'].startswith(BODY_OVERRIDE[i]));reason='逐份比对正文内容、版本及完整性后选定'
        remain=[r for r in cs if r['sha256'] not in routes]
        if selected is None and len(remain)==1:selected=remain[0];reason='其他材料已按内容归类，唯一论文正文'
        if selected is None and len(remain)>1:
            hashes={r['sha256'] for r in remain};pairs=[q for q in g['pairs'] if q['a'] in hashes and q['b'] in hashes]
            if pairs and all(min(q['a_in_b'],q['b_in_a'])>=.9 for q in pairs):
                selected=preferred(remain);reason='全文对照属于同一论文；同稿优先Word，版本优先明确终稿'
        if selected is None:
            unresolved.append({'index':i,'folder':g['folder'],'remaining':[{'sha':r['sha256'][:12],'name':r['name']} for r in remain],'routes':routes});continue
        routes[selected['sha256']]={'category':'正文','reason':reason}
        for r in cs:
            if r['sha256'] not in routes:
                routes[r['sha256']]={'category':'垃圾材料','reason':'同一论文未选版本或格式副本；正文已保留一份'}
        result.append({'task':g['task'],'folder':g['folder'],'selected':selected['sha256'],'selection_reason':reason,'routes':routes})
    PHASE.mkdir(exist_ok=True);o.dump(PHASE/'pending-decisions.json',result);o.dump(PHASE/'pending-unresolved.json',unresolved)
    print(json.dumps({'resolved':len(result),'unresolved':unresolved},ensure_ascii=False,indent=2))

def all_plan():
    pending()
    pending_map={d['task']:d for d in json.loads((PHASE/'pending-decisions.json').read_text('utf-8'))}
    extended=json.loads((p.PHASE/'extended-groups.json').read_text('utf-8'))
    extended_map={};review=[];changes=[]
    for g in extended:
        cs=g['candidates'];routes={};selected=next(r for r in cs if r['sha256']==g['body']['sha256'])
        for r in cs:
            category,reason=obvious(r)
            if r['sha256'].startswith(('7723f3eb030a','010d6474c6aa','59b75db38fab')):category,reason='垃圾材料','明确的实习手册或毕业设计过程手册；另有完整正文'
            if category:routes[r['sha256']]={'category':category,'reason':reason}
        if selected['sha256'] in routes:
            review.append({'task':g['task'],'folder':g['folder'],'issue':'原先正文被内容规则识别为其他材料','selected':selected['sha256'],'route':routes[selected['sha256']],'candidates':[{'sha':r['sha256'],'name':r['name'],'head':compact(textof(r))[:180]} for r in cs]})
        candidates_for_word=[]
        if selected.get('format')=='.pdf' and selected['sha256'] not in routes:
            for r in cs:
                if r.get('format') not in {'.doc','.docx'} or r['sha256'] in routes:continue
                pair=next((q for q in g['pairs'] if {q['a'],q['b']}=={selected['sha256'],r['sha256']}),None)
                if pair and min(pair['a_in_b'],pair['b_in_a'])>=.93 and pair['length_ratio']>=.88:candidates_for_word.append(r)
        if candidates_for_word:
            selected=preferred(candidates_for_word);changes.append({'task':g['task'],'old':g['body']['sha256'],'new':selected['sha256'],'reason':'同一论文Word与PDF并存，优先Word'})
        for r in cs:
            sha=r['sha256']
            if sha==selected['sha256']:continue
            if sha in routes:continue
            pair=next((q for q in g['pairs'] if {q['a'],q['b']}=={sha,g['body']['sha256']}),None)
            if sha==g['body']['sha256'] and selected['sha256']!=sha:routes[sha]={'category':'垃圾材料','reason':'同一论文已有Word正文，此PDF为格式副本'}
            elif pair and (pair['exact_text'] or min(pair['a_in_b'],pair['b_in_a'])>=.93):routes[sha]={'category':'垃圾材料','reason':'全文对照为同一论文的格式或修订副本，保留所选正文'}
            else:routes[sha]={'category':'附件','reason':'非精确重复的配套材料，保留内容供后续识别'}
        routes[selected['sha256']]={'category':'正文','reason':'沿用已选正文；同稿Word优先规则已复核'}
        extended_map[g['task']]={'selected':selected['sha256'],'routes':routes,'selection_reason':routes[selected['sha256']]['reason']}
    overrides_path=PHASE/'extended-overrides.json'
    if overrides_path.exists():
        for override in json.loads(overrides_path.read_text('utf-8')):
            d=extended_map[override['task']]
            if override.get('selected'):d['selected']=override['selected'];d['selection_reason']=override['reason']
            d['routes'].update(override.get('routes',{}))
    groups=defaultdict(list)
    for r in o.get_records():groups[r['task']].append(r)
    corrections_path=PHASE/'confirmed-role-corrections.json'
    corrections=json.loads(corrections_path.read_text('utf-8')) if corrections_path.exists() else {}
    ds=json.loads((o.WORK/'decisions.json').read_text('utf-8'));result=[]
    for d in ds:
        rs=groups[d['task']];update=pending_map.get(d['task']) or extended_map.get(d['task'])
        routes={r['sha256']:{'category':'垃圾材料' if r['role']=='exclude' else '附件','reason':r['reason']} for r in rs}
        if update:
            body=next(r for r in rs if r['sha256']==update['selected'])
            d['body']={k:body.get(k) for k in ('source','member','sha256')};d['selection_reason']=update['selection_reason'];routes.update(update['routes'])
        routes.update(corrections.get(d['task'],{}))
        if d['body']:
            if d['body']['sha256'] in corrections.get(d['task'],{}):raise RuntimeError('Correction unexpectedly targets selected body')
            routes[d['body']['sha256']]={'category':'正文','reason':d['selection_reason']}
        d['alternates']=[];d['routes']=routes;result.append(d)
    o.dump(PHASE/'all-decisions.json',result);o.dump(PHASE/'extended-review.json',review);o.dump(PHASE/'word-priority-changes.json',changes)
    print(json.dumps({'all_groups':len(result),'body_ready':sum(bool(d['body']) for d in result),'word_priority_changes':len(changes),'selected_review':review},ensure_ascii=False,indent=2))

if __name__=='__main__':
    import sys
    all_plan() if '--all' in sys.argv else pending()
