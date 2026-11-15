"""Freeze existing effective API profiles; never downloads originals or scores."""
import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path
import json
from urllib.request import Request,ProxyHandler,build_opener
from filelock import FileLock
from article_repository.ratings import RUBRICS,profile_sha256,profile_evidence
from rating_executor import read_json,write_json


def freeze(category,ids,output,cache_root,api_url='http://192.168.1.120:8098'):
    output=Path(output).resolve();cache_root=Path(cache_root).resolve();cache_root.mkdir(parents=True,exist_ok=True)
    output.parent.mkdir(parents=True,exist_ok=True);rubric=RUBRICS[category]
    with FileLock(str(output)+'.lock',timeout=0):
        document=read_json(output) if output.exists() else {'schema':'article-rating-manifest/1.1','research_type':category,
            'rubric_version':rubric['version'],'rubric_sha256':rubric['sha256'],'evidence_basis':'profile','items':[]}
        if document['rubric_version']!=rubric['version'] or document['rubric_sha256']!=rubric['sha256']:raise ValueError('已有清单评分规则版本不同，不能覆盖')
        existing={item['article_id'] for item in document['items']};failures=[]
        def one(aid):
            opener=build_opener(ProxyHandler({}))
            with opener.open(Request(api_url.rstrip('/')+'/api/library/articles/'+aid+'/rating-input'),timeout=150) as response:packet=json.load(response)
            if packet['article_id']!=aid or packet['research_type']!=category:raise ValueError('实际文章身份或原分类已变')
            snapshot=packet['profile_snapshot'];digest=profile_sha256(snapshot)
            if digest!=packet['profile_sha256'] or profile_evidence(snapshot)!=packet['profile_evidence']:raise ValueError('服务端与本地有效画像协议不一致')
            path=cache_root/(aid+'-'+digest+'.json')
            if not path.exists():write_json(path,snapshot)
            elif profile_sha256(read_json(path))!=digest:raise ValueError('已有画像文件内容漂移，保留待核对')
            return {'article_id':aid,'revision':packet['revision'],'research_type':category,'evidence_basis':'profile',
                'profile_snapshot_path':str(path),'profile_sha256':digest,'source_files':[],
                'preparation_status':'ready','preparation_note':'Existing effective profile with manual corrections; no default original reading or page images.'}
        remaining=[aid for aid in dict.fromkeys(ids) if aid not in existing]
        with ThreadPoolExecutor(max_workers=4) as pool:
            pending={pool.submit(one,aid):aid for aid in remaining}
            for future in as_completed(pending):
                aid=pending[future]
                try:document['items'].append(future.result())
                except Exception as exc:failures.append({'article_id':aid,'reason':type(exc).__name__+': '+str(exc)[:500]})
                if (len(document['items'])+len(failures))%25==0:write_json(output,document)
        document['items'].sort(key=lambda item:item['article_id']);write_json(output,document)
        report={'research_type':category,'frozen':len(document['items']),'requested':len(set(ids)),'failures':failures,
            'model_requests_sent':0,'rating_writes':0,'original_downloads':0,'manifest_path':str(output)}
        write_json(output.with_suffix('.preparation.json'),report);return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--category',required=True,choices=sorted(RUBRICS))
    parser.add_argument('--ids-file',required=True,help='Frozen JSON array of article IDs; no reclassification.')
    parser.add_argument('--output',required=True);parser.add_argument('--cache-root',required=True)
    parser.add_argument('--api-url',default='http://192.168.1.120:8098');args=parser.parse_args()
    ids=read_json(args.ids_file)
    if not isinstance(ids,list) or any(not isinstance(aid,str) or not aid for aid in ids):raise ValueError('IDs必须是非空字符串数组')
    print(json.dumps(freeze(args.category,ids,args.output,args.cache_root,args.api_url),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
