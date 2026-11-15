"""Read-only verification of the repository's missing links and existing NAS objects."""
import hashlib,json,ntpath,sqlite3,os,sys
from pathlib import Path
ROOT=Path(__file__).parent
sys.path.insert(0,str(ROOT.parent/'article-repository'))
from article_repository.files import Files

articles=json.loads((ROOT/'repository-missing-files.json').read_text())['articles']
db=Path(r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite')
with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
    assets={aid:(json.loads(payload),json.loads(detail)) for aid,payload,detail in conn.execute('SELECT asset_id,payload,detail FROM assets')}
files=Files(r'\\192.168.1.67\paper_ai\论文原件库',username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
patches=[];unknown=[]
for article in articles:
    base=article['base'];candidates=list(base.get('files',[]));original_path=''
    if not candidates:
        source=next((assets[aid] for aid in base.get('legacy_asset_ids',[]) if aid in assets),None)
        if source:
            payload,detail=source;sha=detail.get('source_hash') or (detail.get('task') or {}).get('source_sha256')
            original_path=payload.get('source_path') or detail.get('input_source_path') or ''
            if sha:candidates=[{'id':'legacy-body-'+sha[:20],'name':ntpath.basename(detail.get('archive_member_path') or original_path),
              'kind':'原文','sha256':sha,'bytes':None,'relative':'objects/'+sha[:2]+'/'+sha,'availability':'unknown'}]
    verified=[]
    for candidate in candidates:
        sha=candidate.get('sha256')
        if not sha:continue
        row={**candidate,'relative':'objects/'+sha[:2]+'/'+sha}
        try:
            path=files.path(row);info=files.stat(path)
            with files.open(path,'rb') as stream:
                if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('NAS hash mismatch')
            row.update(bytes=info.st_size,availability='available');verified.append(row)
        except (OSError,ValueError):
            unknown.append({'article_id':article['id'],'source_key':article['source_key'],'file':row,
                            'original_path':original_path,'local_original_exists':bool(original_path and Path(original_path).is_file())})
    if verified:patches.append({'article_id':article['id'],'source_key':article['source_key'],'files':verified})
(ROOT/'file-link-plan.json').write_text(json.dumps({'patches':patches,'unresolved':unknown},ensure_ascii=False),encoding='utf-8')
print(json.dumps({'verified_articles':len(patches),'verified_files':sum(len(p['files']) for p in patches),
                  'unresolved_files':len(unknown),'local_candidates':sum(r['local_original_exists'] for r in unknown)},ensure_ascii=False))
