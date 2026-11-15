"""One-time, copy-only library organizer. Never writes to SOURCE."""
from __future__ import annotations
import argparse, ast, concurrent.futures as cf, csv, hashlib, io, json, os, pathlib, re
import shutil, subprocess, time, unicodedata, zipfile, zlib, threading
from collections import Counter, defaultdict
from xml.etree import ElementTree as ET

HERE=pathlib.Path(__file__).parent
WORK=HERE/'整理工作记录'
SOURCE=pathlib.Path(r'C:\Users\Administrator\Desktop\总论文')
OUTPUT=pathlib.Path(r'F:\总论文_整理后_20261028')
SCRATCH=WORK/'包内文档'
SEVEN=r'C:\Program Files\NVIDIA Corporation\NVIDIA App\7z.exe'
ARCHIVE_EXT={'.zip','.rar','.7z','.tar','.gz','.bz2','.xz'}
DOC_EXT={'.doc','.docx','.pdf','.rtf','.wps','.docm'}
DEPENDENCIES={'node_modules','.git','.svn','.idea','__macosx','site-packages','__pycache__','venv','.venv','jre','jdk','target'}
W='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'

def dump(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')

def digest(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    return h.hexdigest()

def load_rules():
    scope={'re':re,'Path':pathlib.Path,'unicodedata':unicodedata,'AgentError':ValueError}
    specs=[(r'F:\论文预处理系统\程序\desktop\screening.py',{'DOCUMENT_KIND','similarity_report','nonpaper_heading'}),
           (r'F:\论文预处理系统\程序\desktop\material_roles.py',{'PROCESS_FORM','SUPPORTING','DRAWING','CODE_LIST','BODY_START','CAD_CREATOR','_compact','_supporting_name','_front_matter','_form_heading','_primary'}),
           (r'F:\论文预处理系统\程序\desktop\office_input.py',{'_compound_streams'})]
    for path,names in specs:
        tree=ast.parse(pathlib.Path(path).read_text('utf-8'))
        nodes=[]
        for n in tree.body:
            if isinstance(n,ast.FunctionDef) and n.name in names:nodes.append(n)
            elif isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in names for t in n.targets):nodes.append(n)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),path,'exec'),scope)
    return scope

RULES=load_rules()

def special(path):
    parts=path.replace('\\','/').split('/')
    if any(p in {'_预提取缓存','_paper_pipeline_status'} for p in parts):return '旧处理缓存或状态文件'
    if parts[-1] in {'论文预提取.md','论文预提取.bak.md','判.md'}:return '旧预提取结果'
    if any(p in {'测试学院','测试专业','测试继续教育专业'} or re.match(r'^测试专业[-（(]',p) for p in parts):return '测试目录资料'
    if any(re.search(r'-(?:测试\d+|课题\d+)-stu\d+-',p,re.I) for p in parts):return '测试目录资料'
    if parts[-1].startswith('~$') or parts[-1].lower() in {'thumbs.db','.ds_store'}:return '临时文件'
    return ''


def non_document_exclusion(path):
    """Recognize explicit process artifacts without treating research reports as forms."""
    parts=path.replace('\\','/').split('/')
    if any(p.lower() in DEPENDENCIES for p in parts):return ''
    name=parts[-1];ext=pathlib.Path(name).suffix.lower()
    if ext in {'.html','.htm','.txt','.url'}:
        if re.search(r'格式分析报告|(?:全文|原文|片段)?(?:比对|对照)报告|全文标明引文报告|去除本人已发表文献报告|文本复制检测报告|简洁报告|查重(?:网址|入口|检测地址)|论文检测网址',name):
            return '查重报告网页或检测入口'
        if name=='报告说明.txt':return '查重报告配套说明'
    if ext in {'.ppt','.pptx','.pps','.ppsx'} and re.search(r'答辩|开题',name):
        return '答辩或开题演示材料'
    if ext in {'.jpg','.jpeg','.png','.bmp','.tif','.tiff','.wbk',''} and re.search(r'答辩记录|开题报告|过程管理手册|毕业(?:设计|论文)任务书',name):
        return '过程表单图片或备份'
    return ''

def task_of(rel):
    parts=rel.split('/')
    for i,p in enumerate(parts[:-1]):
        if (re.search(r'_[A-Za-z0-9\x27]{5,}$',p) and re.search(r'\d',p.rsplit('_',1)[-1])) or re.search(r'-(?:\d{6,}|stu\d+)-[^/]+$',p,re.I):
            return '/'.join(parts[:i+1])
    return '/'.join(parts[:-1])

def safe_name(name,maxlen=110):
    s=re.sub(r'[<>:"/\\|?*\x00-\x1f]','_',name).strip(' .') or '未命名'
    if re.match(r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)',s,re.I):s='_'+s
    if len(s)>maxlen:
        ext=pathlib.Path(s).suffix
        s=s[:maxlen-len(ext)-11]+'__'+hashlib.sha256(name.encode()).hexdigest()[:8]+ext
    return s

def folder_name(task):
    leaf=task.split('/')[-1]
    if '_' in leaf:
        name,sid=leaf.rsplit('_',1)
        return safe_name(sid+'_'+name,90)
    m=re.search(r'-(\d{6,}|stu\d+)-([^/]+)$',leaf,re.I)
    return safe_name(m.group(1)+'_'+m.group(2),90) if m else safe_name(leaf,90)

def safe_member(name):
    p=name.replace('\\','/')
    return not (p.startswith('/') or re.match(r'^[A-Za-z]:',p) or any(x in {'..',''} for x in p.split('/')) or ':' in p or '\x00' in p)

def preview(path):
    info={'text':'','format':path.suffix.lower(),'read_error':'','pages':None,'creator':'','complete':True}
    try:
        with path.open('rb') as f:head=f.read(16)
        if head.startswith(b'%PDF-'):
            import fitz
            fitz.TOOLS.mupdf_display_errors(False);fitz.TOOLS.mupdf_display_warnings(False)
            with fitz.open(path) as d:
                if d.needs_pass:raise ValueError('PDF encrypted')
                info['pages']=len(d);info['creator']=str(d.metadata.get('producer',''))+' '+str(d.metadata.get('creator',''))
                info['text']='\n'.join(d[i].get_text() for i in range(min(8,len(d))))[:35000]
                info['complete']=len(d)<=8 and len(info['text'])<35000
            info['format']='.pdf'
        elif head.startswith(b'PK') and zipfile.is_zipfile(path):
            with zipfile.ZipFile(path) as z:
                if 'word/document.xml' not in z.namelist():raise ValueError('ZIP container without Word document')
                if z.getinfo('word/document.xml').file_size>64*1024*1024:raise ValueError('Document XML too large for preview')
                doc=ET.fromstring(z.read('word/document.xml'))
                paras=[''.join(p.itertext()) if False else ''.join(t.text or '' for t in p.iter(W+'t')) for p in doc.iter(W+'p')]
                full='\n'.join(p for p in paras if p.strip());info['text']=full[:45000];info['complete']=len(full)<=45000
            info['format']='.docx'
        elif head.startswith(bytes.fromhex('d0cf11e0a1b11ae1')):
            with path.open('rb') as f:data=f.read(24*1024*1024)
            if 'EncryptedPackage'.encode('utf-16le') in data:raise ValueError('Encrypted Office document')
            if 'WordDocument'.encode('utf-16le') not in data and 'WordDocument' not in (RULES['_compound_streams'](path) or set()):raise ValueError('Non-Word compound document')
            candidates=[]
            for offset in (0,1):
                text=data[offset:len(data)-(len(data)-offset)%2].decode('utf-16le',errors='ignore')
                lines=[x for x in re.split(r'[\x00-\x08\x0b\x0c\x0e-\x1f\uffff]',text) if len(x.strip())>12]
                text='\n'.join(lines).replace('\r','\n')
                score=sum(text.count(w) for w in ('毕业','摘要','研究','参考文献','Abstract','目录'))
                candidates.append((score,text))
            info['text']=max(candidates,key=lambda x:x[0])[1][:45000]
            info['format']='.doc';info['preview_method']='legacy_binary_text_signals'
            info['complete']=False
        elif head.lstrip().startswith(b'{\\rtf'):
            info['format']='.rtf';info['read_error']='RTF kept for normal local preprocessing'
        else:
            info['read_error']='File signature not recognized as Word or PDF'
    except Exception as e:info['read_error']=str(e)[:350]
    return info

def classify(name,info):
    text=info.get('text','');stem=pathlib.Path(name).stem;compact=re.sub(r'\s+','',text)
    report=RULES['similarity_report'](text,name)
    if report:return 'exclude',report
    form=RULES['_form_heading'](text)
    strong_body=bool(re.search(r'(?mi)^\s*(?:(?:内容)?摘\s*要|abstract|目\s*录)\s*(?:[:：].*)?$',text) and re.search(r'本研究|本文|本设计|参考文献|introduction',text,re.I))
    if form and strong_body:return 'candidate','组合文档含过程页及正文，保留正文材料'
    if form and not info.get('complete',False):return 'uncertain','开头是过程材料，尚不能排除后面包含正文'
    if form:return 'exclude','过程材料标题：'+form[:100]
    # Require a terminal material name, not a keyword anywhere in a research title.
    form_name=re.search(r'(?:开题报告|任务书|过程管理手册|答辩手册|毕业(?:论文|设计)(?:工作)?手册|查重(?:检测)?报告|简洁报告|原文对照报告|全文对照报告|答辩(?:记录表|审批表|评分表)|选题审核表|中期(?:进展情况)?检查表|指导记录表|意识形态审查表|承诺书)(?:[（(][^）)]{0,15}[）)])?\s*$',stem)
    research_name=bool(re.search(r'基于|面向|管理系统|识别方法|分类方法',stem))
    if form_name and not research_name and not strong_body:return 'exclude','明确的过程材料或检测报告文件名'
    if re.search(r'^(?:各位|尊敬的).{0,20}老师.{0,10}好',compact[:150]) and not re.search(r'(?:摘\s*要|ABSTRACT)',text[:5000],re.I):return 'exclude','内容为答辩发言稿'
    if RULES['_supporting_name'](stem):return 'attachment','外文材料、附录或文献综述'
    if re.search(r'(?:调查问卷|访谈提纲|访谈记录|原始数据|问卷数据|调查数据|统计数据|附件|附录)(?:[（(]?\d*[）)]?)$',stem):return 'attachment','研究附件或数据材料'
    if (RULES['DRAWING'].search(stem) or RULES['CODE_LIST'].search(stem) or re.search(r'(?:图[册纸]|[平剖立]面图|系统图|采矿法图|探线剖面图|横道图|流程图|投影图|断面图)(?:A[0-6])?$',stem,re.I)):
        if not re.search(r'研究|设计说明书|论文',stem):return 'attachment','图纸或代码清单'
    if info.get('pages') is not None and info['pages']<=4 and RULES['CAD_CREATOR'].search(info.get('creator','')):return 'attachment','CAD 生成的图纸 PDF'
    if not info.get('format') in {'.doc','.docx','.pdf','.rtf'} or (info.get('read_error') and not info.get('format')=='.rtf'):return 'uncertain','文档读取需要后续核对：'+info.get('read_error','')
    return 'candidate','正文候选'+('；有正文结构证据' if RULES['_primary'](text,stem) else '；按个人资料中的文档保留')

def inspect_one(job):
    path=pathlib.Path(job['local_path']); before=path.stat()
    r=dict(job);r['sha256']=digest(path);r['size']=before.st_size
    if job.get('forced_exclude'):
        r.update(role='exclude',reason=job['forced_exclude'])
    elif job.get('kind')=='archive':r.update(role='archive',reason='压缩包，按包内清单整理')
    elif job['name']=='报告说明.txt':
        raw=path.read_bytes()
        texts=[raw.decode(e,errors='ignore') for e in ('utf-8','gb18030')]
        if any(('检测' in t or '查重' in t) and ('报告' in t or '复制比' in t) for t in texts):r.update(role='exclude',reason='查重检测报告说明')
        else:r.update(role='attachment',reason='说明附件，保留')
    elif pathlib.Path(job['name']).suffix.lower() in DOC_EXT:
        info=preview(path);role,reason=classify(job['name'],info)
        r.update(role=role,reason=reason,format=info['format'],read_error=info.get('read_error',''),pages=info.get('pages'),creator=info.get('creator',''),preview_method=info.get('preview_method','document_parser'),preview_complete=info['complete'])
        r['primary_evidence']=RULES['_primary'](info['text'],pathlib.Path(job['name']).stem)
        r['text_head']=info['text'][:2200]
        r['text_chars_observed']=len(info['text'])
    else:r.update(role='attachment',reason='原始附件文件')
    after=path.stat()
    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise RuntimeError('Source changed during read: '+str(path))
    return r

def crc_file(path):
    crc=0
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''):crc=zlib.crc32(b,crc)
    return f'{crc&0xffffffff:08x}'

def extract_checked(source,member,dest):
    if member['encrypted'] or member['link'] or not safe_member(member['name']):raise ValueError('Encrypted/link/unsafe member')
    dest.parent.mkdir(parents=True,exist_ok=True)
    if dest.exists():
        if dest.stat().st_size==member['size'] and member.get('crc') and crc_file(dest)==member['crc'].lower():return dest
        raise ValueError('Unverified existing extraction: '+str(dest))
    partial=dest.with_name(dest.name+'.partial')
    if partial.exists():partial=dest.with_name(dest.name+'.partial.'+str(time.time_ns()))
    with partial.open('xb') as out:
        if zipfile.is_zipfile(source):
            with zipfile.ZipFile(source) as z, z.open(z.infolist()[member['index']]) as f:
                count=0
                for b in iter(lambda:f.read(1024*1024),b''):
                    count+=len(b)
                    if count>member['size']:raise ValueError('Member exceeded declared length')
                    out.write(b)
        else:
            p=subprocess.Popen([SEVEN,'x','-so','-spd','-p-','--',str(source),member['raw_name']],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            timer=threading.Timer(120,p.kill);timer.start()
            try:
                count=0
                for b in iter(lambda:p.stdout.read(1024*1024),b''):
                    count+=len(b)
                    if count>member['size']:p.kill();raise ValueError('Member exceeded declared length')
                    out.write(b)
                error=p.stderr.read();code=p.wait()
                if code:raise RuntimeError(error.decode('utf-8','replace')[:300])
            finally:timer.cancel();p.stdout.close();p.stderr.close()
    if partial.stat().st_size!=member['size']:raise RuntimeError('Member size mismatch')
    if member.get('crc') and crc_file(partial)!=member['crc'].lower():raise RuntimeError('Member CRC mismatch')
    partial.rename(dest)
    return dest

def extract_document(archive,member):
    dest=SCRATCH/(hashlib.sha256((archive+'#'+str(member['index'])).encode()).hexdigest()+pathlib.Path(member['name']).suffix.lower())
    if member['size']>512*1024*1024:raise ValueError('Document member exceeds 512 MiB preview budget')
    SCRATCH.mkdir(exist_ok=True)
    source=SOURCE/archive
    return extract_checked(source,member,dest)

def build_inspection():
    base=json.loads((WORK/'source-inventory.json').read_text('utf-8'))
    archives=json.loads((WORK/'archive-inventory.json').read_text('utf-8'))
    jobs=[]
    for f in base['files']:
        p=pathlib.Path(f['path']);jobs.append({'source':f['path'],'task':task_of(f['path']),'local_path':str(SOURCE/f['path']),'name':p.name,'kind':'archive' if p.suffix.lower() in ARCHIVE_EXT else 'file','forced_exclude':special(f['path']),'source_mtime_ns':f['mtime_ns']})
    archive_doc_jobs=[];errors=[]
    for a in archives:
        if special(a['source']):continue
        for m in a.get('members',[]):
            parts=m['name'].replace('\\','/').split('/')
            if m['directory'] or pathlib.Path(m['name']).suffix.lower() not in DOC_EXT or any(p.lower() in DEPENDENCIES for p in parts):continue
            try:
                local=extract_document(a['source'],m)
                archive_doc_jobs.append({'source':a['source'],'task':task_of(a['source']),'member':m['name'],'member_index':m['index'],'local_path':str(local),'name':parts[-1],'kind':'member','forced_exclude':special(m['name'])})
            except Exception as e:errors.append({'source':a['source'],'member':m['name'],'error':str(e)})
    jobs.extend(archive_doc_jobs)
    print(json.dumps({'inspection_jobs':len(jobs),'archive_document_jobs':len(archive_doc_jobs),'member_errors':len(errors)},ensure_ascii=False),flush=True)
    start=time.time();records=[]
    with (WORK/'inspection.jsonl').open('w',encoding='utf-8') as out, cf.ProcessPoolExecutor(max_workers=6) as ex:
        for i,r in enumerate(ex.map(inspect_one,jobs,chunksize=8),1):
            out.write(json.dumps(r,ensure_ascii=False)+'\n');records.append(r)
            if i%1000==0:print(json.dumps({'inspected':i,'total':len(jobs),'seconds':round(time.time()-start)},ensure_ascii=False),flush=True)
    dump(WORK/'inspection-errors.json',errors)
    print(json.dumps({'roles':dict(Counter(r['role'] for r in records)),'errors':errors[:12]},ensure_ascii=False,indent=2),flush=True)

def get_records():
    with (WORK/'inspection.jsonl').open(encoding='utf-8') as f:return [json.loads(line) for line in f if line.strip()]

def choose(records):
    candidates=[r for r in records if r['role']=='candidate']
    byhash=defaultdict(list)
    for r in candidates:byhash[r['sha256']].append(r)
    representatives=[]
    for sha,rs in byhash.items():
        rs.sort(key=lambda r:(r['kind']=='member',len(r['name'])))
        rep=rs[0];rep['duplicates']=[{'source':r['source'],'member':r.get('member')} for r in rs[1:]]
        representatives.append(rep)
    if not representatives:return None,[], '没有可确认的正文'
    if len(representatives)==1:return representatives[0],[],'唯一正文候选（包内外精确去重后）'
    external=[r for r in representatives if r['kind']=='file']
    if len(external)==1 and (external[0].get('primary_evidence') or not any(r.get('primary_evidence') for r in representatives if r is not external[0])) and all(r['kind'] in {'member','nested_member'} for r in representatives if r is not external[0]):
        return external[0],[r for r in representatives if r is not external[0]],'优先保留个人目录中独立提交的正文；包内其他稿件另存核对'
    final=[r for r in representatives if re.search(r'终稿|最终稿|定稿|最终版',r['name']) and not re.search(r'非终稿|初稿',r['name'])]
    if len(final)==1:return final[0],[r for r in representatives if r is not final[0]],'唯一明确标记终稿/定稿的正文；其他稿件另存核对'
    strong=[r for r in representatives if r.get('primary_evidence')]
    if len(strong)==1:return strong[0],[r for r in representatives if r is not strong[0]],'唯一有正文结构证据的文件；其他候选另存核对'
    return None,representatives,'存在多个不同正文候选，需要确认'

def plan():
    groups=defaultdict(list)
    for r in get_records():groups[r['task']].append(r)
    decisions=[];seen=set()
    for task,rs in sorted(groups.items()):
        body,alternates,reason=choose(rs)
        school=task.split('/')[0];folder=folder_name(task);key=(school,folder.casefold())
        if key in seen:folder+='__'+hashlib.sha256(task.encode()).hexdigest()[:8]
        seen.add((school,folder.casefold()))
        excluded_only=all(r['role']=='exclude' for r in rs)
        decisions.append({'task':task,'school':school,'folder':folder,'body':{'source':body['source'],'member':body.get('member'),'sha256':body['sha256']} if body else None,'alternates':[{'source':r['source'],'member':r.get('member'),'sha256':r['sha256']} for r in alternates],'selection_reason':reason,'excluded_only':excluded_only})
    dump(WORK/'decisions.json',decisions)
    print(json.dumps({'personal_groups':len(groups),'with_body':sum(bool(d['body']) for d in decisions),'excluded_only':sum(d['excluded_only'] for d in decisions),'without_body':sum(not d['body'] and not d['excluded_only'] for d in decisions),'with_other_candidates':sum(bool(d['alternates']) for d in decisions),'unresolved_examples':[d for d in decisions if not d['body'] and not d['excluded_only']][:10]},ensure_ascii=False,indent=2))

def repair_inspection():
    records=get_records();jobs=[];positions=[]
    for i,r in enumerate(records):
        if (r['role'] in {'exclude','uncertain'} and not r.get('forced_exclude') and r['kind']!='archive') or r['name']=='报告说明.txt':
            jobs.append(r);positions.append(i)
    with cf.ProcessPoolExecutor(max_workers=6) as ex:
        for i,r in zip(positions,ex.map(inspect_one,jobs,chunksize=8)):records[i]=r
    archives={a['source']:a for a in json.loads((WORK/'archive-inventory.json').read_text('utf-8'))}
    unresolved=[]
    for e in json.loads((WORK/'inspection-errors.json').read_text('utf-8')):
        m=next(m for m in archives[e['source']]['members'] if m['name']==e['member'])
        try:
            p=extract_document(e['source'],m)
            records.append(inspect_one({'source':e['source'],'task':task_of(e['source']),'member':m['name'],'member_index':m['index'],'local_path':str(p),'name':pathlib.PurePosixPath(m['name'].replace('\\','/')).name,'kind':'member','forced_exclude':special(m['name'])}))
        except Exception as error:unresolved.append(dict(e,repair_error=str(error)))
    with (WORK/'inspection.jsonl').open('w',encoding='utf-8') as out:
        for r in records:out.write(json.dumps(r,ensure_ascii=False)+'\n')
    dump(WORK/'inspection-errors.json',unresolved)
    print(json.dumps({'rechecked':len(jobs),'records':len(records),'roles':dict(Counter(r['role'] for r in records)),'remaining_member_errors':unresolved},ensure_ascii=False,indent=2))

def checked_dest(path):
    resolved=path.resolve()
    if not resolved.is_relative_to(OUTPUT.resolve()):raise ValueError('Output path escapes destination')
    if resolved.is_relative_to(SOURCE.resolve()):raise ValueError('Cannot write source')
    path.parent.mkdir(parents=True,exist_ok=True)
    return path

def copy_verified(source,dest,sha):
    checked_dest(dest)
    if dest.exists():
        if digest(dest)==sha:return {'path':dest.relative_to(OUTPUT).as_posix(),'sha256':sha,'size':dest.stat().st_size}
        raise FileExistsError(str(dest))
    with source.open('rb') as src,dest.open('xb') as out:shutil.copyfileobj(src,out,1024*1024)
    if digest(dest)!=sha:raise RuntimeError('Copy hash mismatch: '+str(dest))
    shutil.copystat(source,dest)
    return {'path':dest.relative_to(OUTPUT).as_posix(),'sha256':sha,'size':dest.stat().st_size}

def unique_path(parent,name,sha):
    name=safe_name(name)
    dest=parent/name
    if dest.exists():
        if digest(dest)==sha:return dest
        p=pathlib.Path(name);dest=parent/(p.stem+'__'+sha[:10]+p.suffix)
    return dest

def filtered_package(source,members,dest,inventory):
    """Create a separate attachment ZIP; original archive remains untouched."""
    checked_dest(dest)
    source_is_zip=zipfile.is_zipfile(source)
    for m in members:
        if (m['link'] and not source_is_zip) or m['encrypted'] or not safe_member(m['name']):raise ValueError('Unsplit unsafe member '+m['name'])
    member_names=[m['name'].replace('\\','/').casefold() for m in members]
    if len(member_names)!=len(set(member_names)):raise ValueError('Archive has duplicate member paths')
    zsrc=zipfile.ZipFile(source) if source_is_zip else None
    bulk=None
    if not source_is_zip and len(members)>150:
        bulk=WORK/'临时解包'/hashlib.sha256(str(source).encode()).hexdigest()[:24]
        all_inventory=inventory['members']
        if any(m['link'] or m['encrypted'] or not safe_member(m['name'].rstrip('/\\')) for m in all_inventory):raise ValueError('Unsafe bulk archive')
        if not bulk.exists():
            bulk.mkdir(parents=True)
            result=subprocess.run([SEVEN,'x','-y','-p-','-bd','-o'+str(bulk),'--',str(source)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=300)
            if result.returncode:raise ValueError('Bulk extraction failed: '+result.stderr.decode('utf-8','replace')[:200])
    details=[]
    partial=dest.with_name(dest.name+'.partial')
    if partial.exists():raise FileExistsError(str(partial))
    try:
        with zipfile.ZipFile(partial,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as zout:
            for m in members:
                name=m['name'].replace('\\','/')
                zi=zipfile.ZipInfo(name);zi.compress_type=zipfile.ZIP_DEFLATED
                if source_is_zip:
                    original_info=zsrc.infolist()[m['index']]
                    zi.date_time=original_info.date_time;zi.external_attr=original_info.external_attr;zi.create_system=original_info.create_system
                    f=zsrc.open(original_info)
                elif bulk is not None:
                    local=bulk/pathlib.PurePosixPath(name)
                    if not local.resolve().is_relative_to(bulk.resolve()):raise ValueError('Bulk member escaped')
                    f=local.open('rb')
                else:
                    local=WORK/'其他包内副本'/(hashlib.sha256((str(source)+'#'+str(m['index'])).encode()).hexdigest()+pathlib.Path(name).suffix)
                    extract_checked(source,m,local);f=local.open('rb')
                h=hashlib.sha256();crc=0;count=0
                with f,zout.open(zi,'w',force_zip64=True) as dst:
                    for b in iter(lambda:f.read(1024*1024),b''):
                        count+=len(b)
                        if count>m['size']:raise ValueError('Packaged member exceeded declared length')
                        h.update(b);crc=zlib.crc32(b,crc);dst.write(b)
                if count!=m['size']:raise ValueError('Packaged member size mismatch')
                crc=f'{crc&0xffffffff:08x}'
                if m.get('crc') and crc!=m['crc'].lower():raise ValueError('Packaged member CRC mismatch')
                details.append({'name':name,'size':count,'sha256':h.hexdigest(),'crc':crc})
        with zipfile.ZipFile(partial) as z:
            if z.testzip() is not None:raise ValueError('Generated attachment archive failed CRC')
        partial.rename(dest)
        return {'path':dest.relative_to(OUTPUT).as_posix(),'sha256':digest(dest),'size':dest.stat().st_size,'generated':True,'members':details}
    except Exception:
        if partial.exists() and partial.resolve().is_relative_to(OUTPUT.resolve()):partial.unlink()
        raise
    finally:
        if zsrc:zsrc.close()

ARCHIVES={}

def materialize_task(item):
    decision,records=item
    task=decision['task'];outputs=[];notes=[];actions=[]
    if decision['excluded_only']:
        return dict(decision,outputs=[],actions=[{'source':r['source'],'action':'excluded','reason':r['reason']} for r in records],notes=[])
    root=OUTPUT/('论文资料' if decision['body'] else '待核对资料')/decision['school']/decision['folder']
    root.mkdir(parents=True,exist_ok=False)
    selected=decision['body']['sha256'] if decision['body'] else None
    body_record=next((r for r in records if selected and r['sha256']==selected and r['role']=='candidate' and r.get('member')==decision['body'].get('member') and r['source']==decision['body']['source']),None)
    if body_record:
        outputs.append(copy_verified(pathlib.Path(body_record['local_path']),root/'正文'/('论文'+body_record.get('format',pathlib.Path(body_record['name']).suffix)),selected))
    copied=set([selected] if selected else [])
    for r in records:
        identity={'source':r['source'],'member':r.get('member'),'sha256':r['sha256']}
        if r['role']=='exclude':actions.append(dict(identity,action='excluded',reason=r['reason']));continue
        if r['role']=='archive':continue
        if r['sha256'] in copied:actions.append(dict(identity,action='exact_duplicate_or_selected_body'));continue
        if r['role']=='candidate':
            category='其他稿件待核对' if selected else '正文候选'
        elif r['role']=='uncertain':category='待核对文件'
        else:category='附件'
        # Individual attachment documents from mixed archives are packed below.
        if r['kind'] in {'member','nested_member'} and r['role']=='attachment':continue
        name=r['name']
        if r.get('format') in {'.doc','.docx','.pdf','.rtf'}:name=pathlib.Path(name).stem+r['format']
        dest=unique_path(root/category,name,r['sha256'])
        out=copy_verified(pathlib.Path(r['local_path']),dest,r['sha256']);out.update(source=r['source'],member=r.get('member'))
        outputs.append(out);copied.add(r['sha256']);actions.append(dict(identity,action=category,output=out['path']))
    for archive_record in [r for r in records if r['role']=='archive']:
        rel=archive_record['source'];container=archive_record.get('container_key',rel);a=ARCHIVES[container];members=[m for m in a.get('members',[]) if not m['directory']]
        source_path=pathlib.Path(archive_record['local_path'])
        doc_records={r['member_index']:r for r in records if r['kind'] in {'member','nested_member'} and r.get('container_key',r['source'])==container}
        children={r['parent_member_index']:r for r in records if r.get('parent_container')==container and r['role']=='archive'}
        nonattachments={idx:r for idx,r in doc_records.items() if r['role']!='attachment'}
        nonattachments.update(children)
        auxiliary={m['index']:non_document_exclusion(m['name']) for m in members if non_document_exclusion(m['name'])}
        nonattachments.update(auxiliary)
        for m in members:
            if m['index'] in auxiliary:
                actions.append({'source':rel,'container':container,'member':m['name'],'member_index':m['index'],'action':'excluded','reason':auxiliary[m['index']],'size':m['size'],'crc':m.get('crc')})
        nested=[m['name'] for m in members if m['index'] not in children and pathlib.Path(m['name']).suffix.lower() in ARCHIVE_EXT and not any(p.lower() in DEPENDENCIES for p in m['name'].replace('\\','/').split('/'))]
        if nested:notes.append({'source':rel,'note':'包含嵌套压缩包，原包保留','members':nested})
        if not nonattachments:
            dest=unique_path(root/'附件',archive_record['name'],archive_record['sha256'])
            out=copy_verified(source_path,dest,archive_record['sha256']);out['source']=rel;outputs.append(out)
            actions.append({'source':rel,'sha256':archive_record['sha256'],'action':'原始附件压缩包','output':out['path'],'member_count':len(members)})
            continue
        keep=[m for m in members if m['index'] not in nonattachments and not special(m['name'])]
        retained=bool(keep or children or any(r['role']!='exclude' for r in doc_records.values()))
        if not retained:
            actions.append({'source':rel,'sha256':archive_record['sha256'],'action':'excluded','reason':'包内均为已识别的排除材料'});continue
        rawdest=unique_path(root/'_原始压缩包',archive_record['name'],archive_record['sha256'])
        out=copy_verified(source_path,rawdest,archive_record['sha256']);out['source']=rel;outputs.append(out)
        actions.append({'source':rel,'sha256':archive_record['sha256'],'action':'混合包原件保留','output':out['path']})
        if keep:
            package_name=safe_name(pathlib.Path(archive_record['name']).stem,80)+'_附件.zip'
            if (root/'附件'/package_name).exists():package_name=pathlib.Path(package_name).stem+'__'+archive_record['sha256'][:8]+'.zip'
            try:
                out=filtered_package(source_path,keep,root/'附件'/package_name,a);out['source']=rel;outputs.append(out)
                actions.append({'source':rel,'action':'已分离的附件','output':out['path'],'member_count':len(keep)})
            except Exception as e:
                notes.append({'source':rel,'note':'附件包拆分未完成；原包完整保留','error':str(e)})
    meta=dict(decision,source_root=str(SOURCE),outputs=outputs,actions=actions,notes=notes,
              file_roles=[{k:v for k,v in r.items() if k not in {'text_head','local_path'}} for r in records])
    dump(root/'资料清单.json',meta)
    return dict(decision,outputs=outputs,actions=actions,notes=notes,folder_path=root.relative_to(OUTPUT).as_posix())

def materialize():
    global ARCHIVES
    if OUTPUT.exists():raise FileExistsError('Refusing an existing output root: '+str(OUTPUT))
    if shutil.disk_usage(OUTPUT.parent).free<180*2**30:raise RuntimeError('Insufficient space reserve')
    OUTPUT.mkdir()
    ARCHIVES={a['source']:a for a in json.loads((WORK/'archive-inventory.json').read_text('utf-8'))}
    decisions=json.loads((WORK/'decisions.json').read_text('utf-8'));groups=defaultdict(list)
    for r in get_records():groups[r['task']].append(r)
    results=[];start=time.time()
    with (WORK/'materialization.jsonl').open('w',encoding='utf-8') as log,cf.ThreadPoolExecutor(max_workers=6) as ex:
        for i,r in enumerate(ex.map(materialize_task,[(d,groups[d['task']]) for d in decisions]),1):
            results.append(r);log.write(json.dumps(r,ensure_ascii=False)+'\n');log.flush()
            if i%500==0:print(json.dumps({'organized':i,'total':len(decisions),'seconds':round(time.time()-start)},ensure_ascii=False),flush=True)
    outrecords=OUTPUT/'_整理记录';outrecords.mkdir()
    shutil.copyfile(WORK/'source-inventory.json',outrecords/'原始目录清单.json')
    shutil.copyfile(WORK/'archive-inventory.json',outrecords/'压缩包内部清单.json')
    shutil.copyfile(WORK/'materialization.jsonl',outrecords/'整理映射.jsonl')
    summary={'source':str(SOURCE),'output':str(OUTPUT),'personal_groups':len(results),'body_ready':sum(bool(r['body']) for r in results),'review_groups':sum(not r['body'] and not r['excluded_only'] for r in results),'excluded_only_groups':sum(r['excluded_only'] for r in results),'groups_with_alternate_manuscripts':sum(bool(r['alternates']) for r in results),'outputs':sum(len(r['outputs']) for r in results),'output_bytes':sum(o['size'] for r in results for o in r['outputs']),'notes':sum(len(r['notes']) for r in results),'verification':'pending'}
    dump(WORK/'summary.json',summary);dump(outrecords/'整理统计.json',summary)
    print(json.dumps(summary,ensure_ascii=False,indent=2),flush=True)

def inspect_nested():
    from inventory_archives import listing
    records=get_records();archives=json.loads((WORK/'archive-inventory.json').read_text('utf-8'))
    queue=[(a,0) for a in archives if not special(a['source'])];added=[];errors=[]
    for a,depth in queue:
        if depth>=3:continue
        source=pathlib.Path(a.get('local_path') or SOURCE/a['source'])
        original=a.get('root_source',a['source'])
        for m in a.get('members',[]):
            if m['directory'] or pathlib.Path(m['name']).suffix.lower() not in ARCHIVE_EXT:continue
            if any(p.lower() in DEPENDENCIES for p in m['name'].replace('\\','/').split('/')):continue
            key=a['source']+'!/'+m['name']
            local=WORK/'嵌套压缩包'/(hashlib.sha256(key.encode()).hexdigest()+pathlib.Path(m['name']).suffix.lower())
            try:
                if m['size']>512*1024*1024:raise ValueError('Nested archive exceeds 512 MiB local inspection budget')
                extract_checked(source,m,local)
                inventory={'source':key,'root_source':original,'local_path':str(local),'members':listing(local)}
                if sum(x['size'] for x in inventory['members'])>4*2**30:raise ValueError('Nested expanded size exceeds 4 GiB')
                archives.append(inventory);queue.append((inventory,depth+1))
                record=inspect_one({'source':original,'task':task_of(original),'local_path':str(local),'name':pathlib.PurePosixPath(m['name'].replace('\\','/')).name,'kind':'archive','nested':True,'container_key':key,'parent_container':a['source'],'parent_member_index':m['index'],'forced_exclude':''})
                added.append(record)
                for child in inventory['members']:
                    if child['directory'] or pathlib.Path(child['name']).suffix.lower() not in DOC_EXT:continue
                    if any(p.lower() in DEPENDENCIES for p in child['name'].replace('\\','/').split('/')):continue
                    childkey=key+'!/'+child['name'];dest=SCRATCH/(hashlib.sha256(childkey.encode()).hexdigest()+pathlib.Path(child['name']).suffix.lower())
                    if child['size']>512*1024*1024:errors.append({'source':original,'member':childkey,'error':'Nested document too large'});continue
                    extract_checked(local,child,dest)
                    added.append(inspect_one({'source':original,'task':task_of(original),'member':childkey,'member_index':child['index'],'container_key':key,'local_path':str(dest),'name':pathlib.PurePosixPath(child['name'].replace('\\','/')).name,'kind':'nested_member','forced_exclude':special(child['name'])}))
            except Exception as e:errors.append({'source':original,'member':key,'error':str(e)})
    with (WORK/'inspection.jsonl').open('a',encoding='utf-8') as out:
        for r in added:out.write(json.dumps(r,ensure_ascii=False)+'\n')
    dump(WORK/'archive-inventory.json',archives);dump(WORK/'nested-errors.json',errors)
    print(json.dumps({'nested_records':len(added),'nested_archives':sum(r['kind']=='archive' for r in added),'errors':errors[:20],'error_count':len(errors)},ensure_ascii=False,indent=2),flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['inspect','repair','nested','plan','copy'])
    args=parser.parse_args()
    if args.action=='inspect':build_inspection()
    elif args.action=='repair':repair_inspection()
    elif args.action=='nested':inspect_nested()
    elif args.action=='plan':plan()
    elif args.action=='copy':materialize()
