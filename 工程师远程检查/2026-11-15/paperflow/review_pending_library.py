"""Read-only source analysis for the specifically authorized 251 pending groups."""
import concurrent.futures as cf
import hashlib,json,pathlib,re,shutil,subprocess,time,unicodedata,zipfile
from collections import Counter,defaultdict
from xml.etree import ElementTree as ET
import organize_library as o

PHASE=o.WORK/'待核对复核20261028'
SOFFICE=r'C:\Program Files\LibreOffice\program\soffice.com'


def normalize(text):
    return ''.join(re.findall(r'[a-z0-9\u4e00-\u9fff]+',unicodedata.normalize('NFKC',text).lower()))


def convert_shard(item):
    index,paths=item
    if not paths:return []
    profile=PHASE/f'office-profile-{index}';profile.mkdir(parents=True,exist_ok=True)
    command=[SOFFICE,'-env:UserInstallation='+profile.resolve().as_uri(),'--headless','--nologo','--nodefault','--norestore','--convert-to','txt:Text (encoded):UTF8','--outdir',str(PHASE/'office-text'),*[str(p) for p in paths]]
    result=subprocess.run(command,capture_output=True,timeout=240,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    missing=[str(p) for p in paths if not (PHASE/'office-text'/(p.stem+'.txt')).exists()]
    if result.returncode or missing:raise RuntimeError(json.dumps({'conversion_code':result.returncode,'missing':missing,'stderr':result.stderr.decode('utf-8','replace')[:200]}))
    return paths


def extract(record):
    path=pathlib.Path(record['local_path']);sha=record['sha256'];fmt=record['format']
    if o.digest(path)!=sha:raise RuntimeError('Input changed: '+str(path))
    data={'sha256':sha,'format':fmt,'error':'','pages':None,'page_info':[],'metadata':{},'images':None,'equations':None}
    if fmt in {'.doc','.rtf'}:
        text=(PHASE/'office-text'/(sha+'.txt')).read_text('utf-8-sig');data['method']='LibreOffice TXT from verified copy'
    elif fmt=='.docx':
        with zipfile.ZipFile(path) as z:
            root=ET.fromstring(z.read('word/document.xml'))
            w='{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
            math='{http://schemas.openxmlformats.org/officeDocument/2006/math}'
            paras=[''.join(t.text or '' for t in p.iter() if t.tag in {w+'t',math+'t'}) for p in root.iter(w+'p')]
            text='\n'.join(paras)
            data['images']=sum(n.startswith('word/media/') and not n.endswith('/') for n in z.namelist())
            data['equations']=sum(1 for x in root.iter(math+'oMath'))
            data['embedded_objects']=sum(n.startswith('word/embeddings/') for n in z.namelist())
            data['method']='Full document XML paragraphs and math text'
    elif fmt=='.pdf':
        import fitz
        fitz.TOOLS.mupdf_display_errors(False);fitz.TOOLS.mupdf_display_warnings(False)
        with fitz.open(path) as pdf:
            data['pages']=len(pdf);data['metadata']=pdf.metadata;texts=[]
            for i,page in enumerate(pdf):
                t=page.get_text();texts.append(t)
                data['page_info'].append({'page':i+1,'width':round(page.rect.width,1),'height':round(page.rect.height,1),'chars':len(t),'images':len(page.get_images())})
            data['images']=sum(p['images'] for p in data['page_info']);text='\n\f\n'.join(texts)
            data['method']='Full PDF text, all pages'
    else:raise ValueError(fmt)
    normalized=normalize(text)
    dest=PHASE/'text'/(sha+'.txt');dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(text,encoding='utf-8')
    data.update(text_path=str(dest),chars=len(text),normalized_chars=len(normalized),normalized_sha256=hashlib.sha256(normalized.encode()).hexdigest(),head=text[:3200],tail=text[-1200:])
    return data


def shingles(text,k=7):return {text[i:i+k] for i in range(0,max(0,len(text)-k+1),2)}


def compare(a,b):
    ta=normalize(pathlib.Path(a['text_path']).read_text('utf-8'));tb=normalize(pathlib.Path(b['text_path']).read_text('utf-8'))
    # Offset-independent inclusion, because PDF whitespace/headers alter positions.
    sa={ta[i:i+7] for i in range(max(0,len(ta)-6))};sb={tb[i:i+7] for i in range(max(0,len(tb)-6))}
    common=len(sa&sb);ca=common/max(1,len(sa));cb=common/max(1,len(sb))
    return {'a':a['sha256'],'b':b['sha256'],'exact_text':ta==tb,'a_in_b':round(ca,5),'b_in_a':round(cb,5),'length_ratio':round(min(len(ta),len(tb))/max(1,len(ta),len(tb)),5)}


def main():
    PHASE.mkdir(parents=True,exist_ok=True)
    decisions=json.loads((o.WORK/'decisions.json').read_text('utf-8'))
    selected=[d for d in decisions if not d['body'] and not d['excluded_only']]
    if len(selected)!=251:raise ValueError('Expected original 251-group scope')
    o.dump(PHASE/'scope.json',selected);tasks={d['task'] for d in selected}
    records=[r for r in o.get_records() if r['task'] in tasks];candidates=[r for r in records if r['role']=='candidate']
    unique={r['sha256']:r for r in candidates}
    inp=PHASE/'office-input';inp.mkdir(exist_ok=True);(PHASE/'office-text').mkdir(exist_ok=True)
    jobs=[]
    for r in unique.values():
        if r['format']!='.doc':continue
        copy=inp/(r['sha256']+'.doc')
        if not copy.exists():shutil.copyfile(r['local_path'],copy)
        if o.digest(copy)!=r['sha256']:raise RuntimeError('DOC snapshot changed')
        if not (PHASE/'office-text'/(r['sha256']+'.txt')).exists():jobs.append(copy)
    print(json.dumps({'pending_doc_conversions':len(jobs),'unique_documents':len(unique)}),flush=True)
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        for done in ex.map(convert_shard,[(i,jobs[i::3]) for i in range(3)]):
            print(json.dumps({'converted_documents':len(done)}),flush=True)
    extracted=[]
    with cf.ProcessPoolExecutor(max_workers=4) as ex:
        for i,item in enumerate(ex.map(extract,unique.values(),chunksize=4),1):
            extracted.append(item)
            if i%100==0:print(json.dumps({'fulltext_read':i,'total':len(unique)}),flush=True)
    o.dump(PHASE/'extracted.json',extracted);bysha={e['sha256']:e for e in extracted}
    groups=[]
    for d in selected:
        rs=[r for r in candidates if r['task']==d['task']];cs=list({r['sha256']:r for r in rs}.values());pairs=[]
        for i,a in enumerate(cs):
            for b in cs[i+1:]:pairs.append(compare(bysha[a['sha256']],bysha[b['sha256']]))
        groups.append({'task':d['task'],'folder':d['folder'],'candidates':[dict(r,extraction=bysha[r['sha256']]) for r in cs],'pairs':pairs})
    o.dump(PHASE/'groups.json',groups)
    print(json.dumps({'groups':len(groups),'unique_documents':len(unique),'extraction_errors':sum(bool(e['error']) for e in extracted),'complete':True}),flush=True)


if __name__=='__main__':main()
