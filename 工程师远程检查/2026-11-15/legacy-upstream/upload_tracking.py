"""Read durable upload receipts across batches without using titles as identity."""
from collections import defaultdict
import json
from pathlib import Path
from urllib.parse import urlsplit

VERSION='upload-dedup-v2'


def queued_upload_status(task_ids, queue):
    """Project durable queue state for one paper and its independent assets."""
    snapshot=queue if isinstance(queue,dict) else queue.get_many(task_ids)
    tasks=[snapshot.get(int(task_id)) for task_id in dict.fromkeys(task_ids)]
    tasks=[task for task in tasks if task]
    primary=next((task for task in tasks if task['role']=='paper_primary'),None)
    if primary is None:
        return {'upload':'记录异常','process':'尚未提交','reason':'主文档队列记录不存在',
                'assets_done':0,'assets_total':0}
    value=primary['status']
    if value=='upload_waiting': upload='排队待上传'
    elif value=='upload_intent': upload='上传中'
    elif value in ('upload_unknown','parse_unknown'): upload='上传待确认' if not primary['remote_id'] else '已上传'
    elif primary['remote_id']: upload='已上传'
    else: upload='记录异常'
    process={
        'parse_waiting':'排队中','parse_intent':'已提交，待确认',
        'submitted':'已提交，待确认','running':'处理中',
        'accepted':'已完成','quality_hold':'暂缓',
        'quality_reject':'剔除','technical_review':'技术失败，待复核',
        'outcome_unresolved':'处理状态待确认',
        'parse_unknown':'处理状态待确认',
    }.get(value,'尚未提交')
    if value=='accepted' and primary.get('policy_key')=='education':
        if primary.get('post_status')=='needs_review':process='入库，产物待复核'
        elif primary.get('post_status')!='done':process='入库，产物生成中'
    assets=[task for task in tasks if task['role']!='paper_primary']
    done=sum(bool(task['remote_id']) and task['status'] in
             ('uploaded','accepted','quality_hold','quality_reject') for task in assets)
    if any(task['status'] in ('upload_unknown','technical_review') for task in assets):
        upload='部分上传待确认' if upload=='已上传' else upload
    reason=primary.get('error') or primary.get('failure_kind') or ''
    if value=='upload_unknown': reason=reason or '上传回执待核查'
    metadata=primary.get('source_metadata_status')
    if metadata in ('pending','running','blocked') and primary.get('remote_id'):
        process='处理状态待确认'
        if metadata=='blocked':
            reason='来源标注暂停：'+(primary.get('source_metadata_reason') or 'metadata_blocked')+'；修复接收器后点击“重试选中项”，仅重试来源标注'
        else:
            reason='来源标注正在保存或等待重试，交接尚未完成'
    return {'upload':upload,'process':process,'reason':reason,
            'assets_done':done,'assets_total':len(assets),
            'remote_id':primary['remote_id'],'dataset_id':primary['dataset_id']}


def paper_id_for_item(item):
    value=item.get('paper_id')
    if isinstance(value,str) and value.strip():
        return value.strip()
    package=item.get('package_path')
    if not package:
        return ''
    package=Path(package)
    for name in ('manifest.json','metadata.json'):
        path=package/name
        if not path.is_file():
            continue
        try:
            data=json.loads(path.read_text(encoding='utf-8'))
        except (OSError,ValueError):
            continue
        value=data.get('paper_id')
        if not value and isinstance(data.get('knowledge_package'),dict):
            value=data['knowledge_package'].get('paper_id')
        if isinstance(value,str) and value.strip():
            return value.strip()
    return ''


def service_key(url):
    parsed=urlsplit(url.strip().rstrip('/'))
    host=(parsed.hostname or '').lower()
    if host in {'localhost','127.0.0.1','::1'}:host='localhost'
    if ':' in host:host='['+host+']'
    port=parsed.port
    suffix='' if port is None or (parsed.scheme,port) in {('http',80),('https',443)} else ':'+str(port)
    return parsed.scheme.lower()+'://'+host+suffix+parsed.path.rstrip('/')


class UploadIndex:
    def __init__(self,histories,*,identity_resolver=None):
        self.identity_resolver=identity_resolver or paper_id_for_item
        self.by_source=defaultdict(list);self.by_content=defaultdict(list)
        self.by_paper=defaultdict(list)
        self.manifests={}
        for manifest in histories:
            self.manifests[manifest.get('run_id')]=manifest
            for item in manifest.get('items',[]):
                row=(manifest,item)
                paper_id=self.identity_resolver(item)
                if paper_id:self.by_paper[paper_id].append(row)
                if item.get('source_hash'):self.by_source[item['source_hash']].append(row)
                if item.get('reviewed_hash'):self.by_content[item['reviewed_hash']].append(row)
                for record in item.get('uploads',{}).values():
                    if isinstance(record,dict) and record.get('content_hash') and record['content_hash']!=item.get('reviewed_hash'):
                        self.by_content[record['content_hash']].append(row)

    def matches(self,item,base_url=None,dataset_id=None):
        paper_id=self.identity_resolver(item)
        rows=self.by_paper.get(paper_id,[]) + self.by_source.get(item.get('source_hash'),[])+self.by_content.get(item.get('reviewed_hash'),[])
        # Display a receipt even for a legacy item with no retained identity hash.
        rows=rows+[({'run_id':''},item)]
        found=[];seen=set()
        for manifest,prior in rows:
            for destination,record in prior.get('uploads',{}).items():
                if not isinstance(record,dict) or '|' not in destination:continue
                same_source=bool(item.get('source_hash') and item['source_hash']==prior.get('source_hash'))
                remote_hash=record.get('content_hash') or (prior.get('reviewed_hash') if not record.get('reused_from') else None)
                same_content=bool(remote_hash and remote_hash==item.get('reviewed_hash'))
                same_paper=bool(paper_id and self.identity_resolver(prior)==paper_id)
                legacy_self=prior is item and not item.get('source_hash') and not item.get('reviewed_hash')
                if not (same_paper or same_source or same_content or legacy_self):continue
                base,ds=destination.rsplit('|',1)
                try:
                    if base_url is not None and service_key(base)!=service_key(base_url):continue
                except ValueError:continue
                if dataset_id is not None and ds!=dataset_id:continue
                identity=(id(prior),destination)
                if identity in seen:continue
                seen.add(identity)
                found.append({'manifest':manifest,'item':prior,'key':destination,'record':record,'dataset_id':ds})
        return found


def upload_mark(item,index,base_url,dataset_id=None):
    matches=index.matches(item,base_url,dataset_id)
    if any(r['record'].get('status')=='uploaded' and r['record'].get('document_id') for r in matches):return '已上传'
    if any(r['record'].get('status') in {'sending','unknown','uploaded'} for r in matches):return '上传待确认'
    if dataset_id is not None and any(r['record'].get('status')=='uploaded' and r['record'].get('document_id')
            for r in index.matches(item,base_url)):return '已上传其他库'
    if any(r['record'].get('status')=='failed' for r in matches):return '上传失败，可重试'
    return '未尝试'


def mark_sources(rows,index,base_url):
    from pathlib import Path
    from engine import digest
    result=[]
    for row in rows:
        label='未尝试';row=dict(row)
        if index.by_source:
            try:
                path=Path(row['path']);stat=path.stat();stamp=[stat.st_size,stat.st_mtime_ns]
                fingerprint=row.get('upload_source_hash') if row.get('upload_source_stamp')==stamp else None
                if not fingerprint:fingerprint=digest(path)
                if [path.stat().st_size,path.stat().st_mtime_ns]!=stamp:raise OSError('扫描时文件变化')
                row.update(upload_source_hash=fingerprint,upload_source_stamp=stamp)
                label=upload_mark({'source_hash':fingerprint},index,base_url) or label
            except OSError:label='无记录异常'
        result.append({**row,'upload_mark':label})
    return result
