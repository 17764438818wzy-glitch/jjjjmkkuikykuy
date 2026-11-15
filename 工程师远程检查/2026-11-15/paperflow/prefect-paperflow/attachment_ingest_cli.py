"""Explicit one-attachment entry; importing this module performs no business work."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import quote, urlencode

from paperflow_prefect.attachment_ingest import read_frozen_handoff
from paperflow_prefect.records import identity, read_json, sha_file, write_json


class AuditedClient:
    """Observe native client calls without replacing its transport or retry policy."""
    def __init__(self, native, path):
        self.native, self.path, self.events = native, Path(path), []
        self.base_url = native.base_url

    def _call(self, method, endpoint, call, **details):
        event = {'method':method, 'endpoint':endpoint, 'started_at':time.time(),
                 'mutation':method != 'GET' and endpoint != '/retrieval', **details}
        self.events.append(event); write_json(self.path, self.events)
        try:
            value = call(); event['status'] = 'returned'; return value
        except BaseException as exc:
            event.update(status='unknown' if event['mutation'] else 'failed_query',error_type=type(exc).__name__); raise
        finally:
            event['finished_at'] = time.time(); write_json(self.path, self.events)

    def request(self, method, endpoint, body=None):
        return self._call(method,endpoint,lambda:self.native.request(method,endpoint,body),
                          request_sha256=identity(body))

    def upload_file(self,parent,name,data):
        return self._call('POST','/files',lambda:self.native.upload_file(parent,name,data),
                          parent_id=parent,name=name,bytes=len(data),sha256=hashlib.sha256(data).hexdigest())

    def upload_bytes(self,ds,name,data):
        return self._call('POST',f'/datasets/{ds}/documents',lambda:self.native.upload_bytes(ds,name,data),
                          name=name,bytes=len(data),sha256=hashlib.sha256(data).hexdigest())

    def download_file(self,fid):
        return self._call('GET','/files/'+quote(fid,safe=''),lambda:self.native.download_file(fid))

    def download_bytes(self,ds,did):
        from paperflow.rag import RagPublisher
        return self._call('GET',f'/datasets/{ds}/documents/{did}',lambda:RagPublisher(self.native)._download(ds,did))

    def summary(self):
        return {'calls':len(self.events),'mutations':sum(e['mutation'] for e in self.events),
                'queries':sum(not e['mutation'] for e in self.events),
                'unknown':sum(e.get('status')=='unknown' for e in self.events),
                'failed_queries':sum(e.get('status')=='failed_query' for e in self.events), 'audit_path':str(self.path)}


def validate_targets(frozen, targets, service):
    rows = targets.get('libraries',[])
    if (targets.get('service','').rstrip('/') != service.rstrip('/')
            or [r.get('code') for r in rows] != frozen['routing_codes']
            or len({r.get('dataset_id') for r in rows}) != len(rows)):
        raise ValueError('目标服务或专业库顺序不符合冻结画像')
    for row, candidate in zip(rows,frozen['routing_candidates']):
        if (row.get('name') != candidate['name'] or not row.get('dataset_id') or not row.get('embedding_model')
                or row.get('dataset_name') != '【'+row['code']+'】'+row['name']+'｜资料库'):
            raise ValueError('附件必须使用明确对应的专业资料库')
    if not targets.get('root_id') or not targets.get('folder_name','').startswith('【'+rows[0]['code']+'】'):
        raise ValueError('最高专业原件目录未冻结')


@contextmanager
def rag_slot(settings, home, report):
    """Join the existing shared limit without creating or changing any limit."""
    Path(home).mkdir(parents=True,exist_ok=True)
    os.environ['PREFECT_API_URL'] = settings['prefect_api_url']
    os.environ['PREFECT_HOME'] = str(Path(home).resolve())
    os.environ['PREFECT_SERVER_ALLOW_EPHEMERAL_MODE'] = 'false'
    os.environ.pop('PREFECT_API_KEY',None)
    from prefect.client.orchestration import get_client
    from prefect.concurrency.sync import concurrency
    name = 'paperflow-connected-rag'; configured = settings['limits']['rag']
    with get_client(sync_client=True) as api:
        row = api.read_global_concurrency_limit_by_name(name)
        if row.limit != configured or not row.active or row.slot_decay_per_second:
            raise ValueError('当前 RAG 全局额度与本机配置不一致；未更改服务额度')
    report['rag_limit'] = {'name':name,'configured':configured,'global':row.limit,'occupy':1,
                           'waiting_at':time.time(),'changed':False}
    with concurrency(name,occupy=1,strict=True,timeout_seconds=30,raise_on_lease_renewal_failure=True):
        report['rag_limit']['acquired_at'] = time.time()
        try: yield
        finally: report['rag_limit']['released_at'] = time.time()


def ingest(args, report, client, settings):
    from paperflow_prefect.attachment_publish import ensure_original_folder, ingest_attachment
    from preextract_pipeline import offline_counter
    from preextract_publish import _Budget
    frozen = read_frozen_handoff(args.handoff,args.handoff_sha256)
    if sha_file(args.targets) != args.targets_sha256: raise ValueError('目标映射冻结哈希不符')
    targets = read_json(args.targets); validate_targets(frozen,targets,client.base_url)
    report.update(handoff_sha256=frozen['handoff_sha256'],targets_sha256=args.targets_sha256,
                  verified_references=len(frozen['verified_references']))
    counter = offline_counter(read_json(settings.path('runtime_config')),settings.path('release_root')/'core')
    try:
        budget = _Budget(counter)
        # Verify that the actual MD is splittable before any external mutation.
        budget.split(frozen['card']['md_text'],lambda i,s:'<!-- PFA '+'x'*64+' '+str(i).zfill(6)+' -->\n'+s)
        with rag_slot(settings,Path(args.receipt_root)/'prefect-home',report):
            for target in targets['libraries']:
                result = client.request('GET','/datasets?'+urlencode({'id':target['dataset_id'],'page':1,'page_size':100}))
                rows = result if isinstance(result,list) else result.get('datasets',[])
                matches = [r for r in rows if r.get('id') == target['dataset_id']]
                if len(matches)!=1 or any(matches[0].get(k)!=target[k2] for k,k2 in [('name','dataset_name'),('embedding_model','embedding_model')]):
                    raise ValueError('远端专业资料库或嵌入模型不符')
            folder = ensure_original_folder(client,targets['root_id'],frozen['routing_codes'][0],targets['folder_name'],
                                             args.receipt_root,read_only=args.command=='reconcile')
            primary = dict(targets['libraries'][0],parent_id=folder)
            route = {'paper_id':frozen['card']['metadata']['paper_id'],'body_source_sha256':frozen['body_source_sha256'],
                     'profile_sha256':frozen['profile_sha256'],'routing_codes':frozen['routing_codes'],
                     'primary':primary,'references':targets['libraries'][1:],'history_reviewed':True,'parent_targets':[]}
            result = ingest_attachment(frozen['manifest']['delivery'],route,client,args.receipt_root,budget,
                                       read_only=args.command=='reconcile')
            report.update(status=result['status'],receipt_path=result['receipt_path'],raw=result['raw'],
                          publications=result['publications'],download=result['download'],
                          parent_link_status=result['parent_link_status'])
    finally: counter.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command',required=True)
    for mode in ('check','ingest','reconcile','download'):
        p = subs.add_parser(mode); p.add_argument('--report',required=True)
        if mode != 'download':
            p.add_argument('--handoff',required=True); p.add_argument('--handoff-sha256',required=True)
        if mode != 'check': p.add_argument('--config',required=True)
        if mode in ('ingest','reconcile'):
            p.add_argument('--targets',required=True); p.add_argument('--targets-sha256',required=True)
            p.add_argument('--receipt-root',required=True)
        if mode == 'download':
            p.add_argument('--receipt',required=True); p.add_argument('--output',required=True)
    args = parser.parse_args(argv); report_path = Path(args.report).resolve()
    if report_path.exists() or report_path.with_suffix('.audit.json').exists():
        raise FileExistsError('每次执行使用新的审计报告路径')
    report = {'schema':'attachment-entry-report/1','mode':args.command,'started_at':time.time(),
              'model_requests_added':0,'nas_operations':0,'body_publications':0}
    client = None; code = 0
    try:
        if args.command == 'check':
            frozen = read_frozen_handoff(args.handoff,args.handoff_sha256)
            report.update(status='checked',handoff_sha256=frozen['handoff_sha256'],
                          verified_references=len(frozen['verified_references']),
                          metadata=frozen['card']['metadata'],listing=frozen['card']['listing'],
                          routing_codes=frozen['routing_codes'],external_calls=0)
        else:
            from paperflow_prefect.settings import Settings
            from paperflow_prefect.legacy import rag_client
            settings = Settings.load(args.config); settings.require_execution()
            report['config_sha256'] = sha_file(args.config)
            client = AuditedClient(rag_client(settings),report_path.with_suffix('.audit.json'))
            if args.command == 'download':
                from paperflow_prefect.attachment_download import download_attachment
                report.update(download=download_attachment(args.receipt,client,args.output),status='verified')
            else: ingest(args,report,client,settings)
    except Exception as exc:
        report.update(status='pending_verification',error_type=type(exc).__name__)
        if isinstance(exc,(ValueError,FileExistsError)): report['reason'] = str(exc)
        code = 2
    finally:
        if client is not None: report['remote_calls'] = client.summary()
        report['finished_at'] = time.time(); write_json(report_path,report)
    print(json.dumps({'status':report['status'],'report':str(report_path),
                      **({'error_type':report['error_type']} if code else {})},ensure_ascii=False))
    return code


if __name__=='__main__': raise SystemExit(main())
