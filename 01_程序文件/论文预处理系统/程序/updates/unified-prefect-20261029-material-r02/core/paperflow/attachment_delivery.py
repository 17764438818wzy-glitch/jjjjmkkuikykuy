"""Narrow attachment adapter on the existing delivery Job, IPC gate and HTTP client."""
import hashlib
import json
import re
from pathlib import Path
import uuid
from urllib.parse import quote,urlsplit,parse_qs,urlencode

from .attachments import AttachmentLedger,attachment_target
from .delivery_control import ControlledRagClient
from .handoff_store import canonical
from .package import load,sha


def digest(value):
    return hashlib.sha256(value if isinstance(value,bytes) else canonical(value).encode()).hexdigest()


def names(row):
    original=json.loads(row['data'])['original']
    return row['attachment_id']+Path(original).suffix,row['attachment_id']+'_note.md'


def note_text(row,data):
    note=data['note'];title=note.get('related_paper_title') or '待关联'
    return ('# 附件说明\n'+note['description']+'\n材料类型：'+note['kind']+'\n关联论文线索：'+title+
        '\n关联状态：待关联（未仅凭论文名认定归属）\n原件编号：'+data['remote']['file_id']+
        '\n来源标识：'+row['source_id']+'\n原件SHA256：'+row['source_sha256']+'\n')


def storage_acceptance(config,category):
    if not config.delivery_acceptance_report:raise ValueError('deployment acceptance required before launch')
    report=load(config.delivery_acceptance_report)
    if report.get('passed') is not True or report.get('endpoint')!=config.rag_url:
        raise ValueError('deployment acceptance endpoint mismatch')
    accepted_kind,target=attachment_target(config.attachment_targets,category)
    accepted=report.get('attachment_storage',{})
    if (accepted.get('passed') is not True or accepted.get('targets',{}).get(accepted_kind)!=target
        or not target.get('parent_id') or target.get('dataset_id') not in report.get('identity',{}).get('dataset_ids',[])):
        raise ValueError('explicit attachment storage acceptance required')
    return report


class AttachmentControl:
    def __init__(self,parent):
        self.parent=parent;self.manager=parent.manager;self.config=parent.config
        self.ledger=AttachmentLedger(self.manager.store)
        self.ledger._write(lambda c:c.execute('''CREATE TABLE IF NOT EXISTS attachment_intents (
            attachment_id TEXT NOT NULL,step TEXT NOT NULL,request_hash TEXT NOT NULL,
            PRIMARY KEY(attachment_id,step))'''))

    @staticmethod
    def context(row):return dict(row=dict(row),data=json.loads(row['data']))

    def row(self,c,execution_id,resource,*,allow_unconfigured=False):
        ex=self.manager.executions[execution_id]
        aid=ex.key.removeprefix('attachment:')
        row=c.execute('SELECT * FROM attachment_tasks WHERE attachment_id=?',(aid,)).fetchone()
        if ex.kind!='delivery' or ex.key!='attachment:'+aid or not row:raise ValueError('registered attachment owner required')
        if resource.get('identity')!={'attachment_id':aid,'generation':0}:raise ValueError('attachment identity mismatch')
        if not c.execute('SELECT 1 FROM sources WHERE task_id=? AND source_id=?',(row['task_id'],row['source_id'])).fetchone():
            raise ValueError('attachment source superseded')
        data=json.loads(row['data'])
        unconfigured=(allow_unconfigured and row['state']=='configuration_pending' and not data['remote']
            and not c.execute('SELECT 1 FROM attachment_intents WHERE attachment_id=?',(aid,)).fetchone())
        if not unconfigured:
            if data['target']!=attachment_target(self.config.attachment_targets,row['category'])[1]:raise ValueError('attachment target configuration changed')
            if data['endpoint']!=self.config.rag_url.rstrip('/'):raise ValueError('attachment endpoint changed')
        return row,data

    @staticmethod
    def sent(c,aid,step):
        return bool(c.execute("SELECT 1 FROM control_actions WHERE kind='rag_write' AND state IN ('possible','complete','unresolved','reconciled','diagnostic','unknown') "
            "AND json_extract(request,'$.resource.identity.attachment_id')=? AND json_extract(request,'$.resource.step')=? LIMIT 1",(aid,step)).fetchone())

    @staticmethod
    def request_hash(row,data,step):
        if step=='file_upload':return row['source_sha256']
        text=note_text(row,data)
        if step=='note_upload':return digest(text.encode('utf-8'))
        if step=='note_chunk':return digest(dict(content=text))
        raise ValueError('unknown attachment write step')

    def check_action(self,c,execution_id,kind,resource):
        op=resource.get('operation')
        row,data=self.row(c,execution_id,resource,allow_unconfigured=kind=='local' and op=='attachment_load')
        if kind=='local':
            if op not in ('attachment_load','attachment_verify','attachment_intent','attachment_observe','attachment_bind','attachment_complete'):
                raise ValueError('unknown attachment local operation')
            return True
        if op!='attachment_http':raise ValueError('attachment HTTP operation required')
        target=data['target'];remote=data['remote'];step=resource.get('step');method=resource.get('method')
        if not target.get('parent_id') or not target.get('dataset_id'):raise ValueError('attachment targets not configured')
        if any(not isinstance(target[k],str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',target[k]) for k in ('parent_id','dataset_id')):
            raise ValueError('invalid attachment target identifier')
        endpoint=resource.get('endpoint','');u=urlsplit(endpoint);q=parse_qs(u.query);path=u.path
        if u.scheme or u.netloc or u.fragment:raise ValueError('attachment endpoint must be relative')
        raw_name,note_name=names(row)
        ds=quote(target['dataset_id'],safe='');base=f'/datasets/{ds}/documents'
        if kind=='rag_read' and method=='GET':
            valid=(step=='file_list' and path=='/files' and q.get('parent_id')==[target['parent_id']] and q.get('keywords')==[raw_name])
            identity_endpoint='/paperflow/deployment' if self.config.production_pilot else '/pf-stage4/identity'
            valid|=step=='deployment_identity' and endpoint==identity_endpoint
            fid=remote.get('candidate_file_id',remote.get('file_id'))
            valid|=bool(fid and step=='file_bytes' and path=='/files/'+quote(fid,safe=''))
            valid|=step=='note_list' and path==base and q.get('name')==[note_name]
            did=remote.get('candidate_document_id',remote.get('document_id'))
            valid|=bool(did and step=='note_bytes' and path==base+'/'+quote(did,safe=''))
            valid|=bool(remote.get('document_id') and step=='note_chunks' and path==base+'/'+quote(remote['document_id'],safe='')+'/chunks')
            if not valid:raise ValueError('attachment read outside owned object')
            return True
        if kind!='rag_write' or method!='POST':raise ValueError('attachment only permits owned POST writes')
        expected=self.request_hash(row,data,step)
        intent=c.execute('SELECT * FROM attachment_intents WHERE attachment_id=? AND step=?',(row['attachment_id'],step)).fetchone()
        if not intent or intent['request_hash']!=expected or resource.get('content_sha256')!=expected:
            raise ValueError('stable attachment intent required')
        if self.sent(c,row['attachment_id'],step):raise ValueError('attachment write possibly submitted; reconcile without replay')
        if step=='file_upload':
            valid=path=='/files' and resource.get('parent_id')==target['parent_id'] and resource.get('name')==raw_name and not remote.get('file_id')
        elif step=='note_upload':
            valid=path==base and resource.get('name')==note_name and remote.get('file_id') and not remote.get('document_id')
        else:
            valid=bool(remote.get('document_id') and path==base+'/'+quote(remote['document_id'],safe='')+'/chunks' and not remote.get('chunk_id'))
        if not valid:raise ValueError('attachment write outside exact intent target')
        return False

    @staticmethod
    def proof(c,execution_id,response,steps):
        expected=digest(response)
        for row in c.execute("SELECT request,result FROM control_actions WHERE execution_id=? AND state='complete' AND kind IN ('rag_read','rag_write')",(execution_id,)):
            request=json.loads(row['request'])['resource'];result=json.loads(row['result'] or '{}')
            if request.get('step') in steps and result.get('response_sha256')==expected:return
        raise ValueError('attachment evidence has no completed managed response')

    @staticmethod
    def reconcile(c,aid,step):
        c.execute("UPDATE control_actions SET state='reconciled' WHERE state='unresolved' AND kind='rag_write' "
            "AND json_extract(request,'$.resource.identity.attachment_id')=? AND json_extract(request,'$.resource.step')=?",(aid,step))

    def commit_sql(self,c,execution_id,operation,payload):
        row,data=self.row(c,execution_id,dict(identity=payload['identity']),allow_unconfigured=operation=='attachment_load')
        aid=row['attachment_id'];remote=data['remote'];state=row['state']
        if operation=='attachment_load':
            if state=='configuration_pending':
                _,target=attachment_target(self.config.attachment_targets,row['category'])
                if not target.get('parent_id') or not target.get('dataset_id'):raise ValueError('attachment targets not configured')
                data.update(target=target,endpoint=self.config.rag_url.rstrip('/'))
                c.execute("UPDATE attachment_tasks SET state='pending',data=? WHERE attachment_id=?",(canonical(data),aid))
                row=c.execute('SELECT * FROM attachment_tasks WHERE attachment_id=?',(aid,)).fetchone()
            return self.context(row)
        if operation=='attachment_intent':
            step=payload['step'];expected=self.request_hash(row,data,step)
            previous=c.execute('SELECT request_hash FROM attachment_intents WHERE attachment_id=? AND step=?',(aid,step)).fetchone()
            if previous and previous[0]!=expected:raise ValueError('attachment intent changed')
            c.execute('INSERT OR IGNORE INTO attachment_intents VALUES(?,?,?)',(aid,step,expected))
            return dict(can_post=not self.sent(c,aid,step),content_sha256=expected)
        if operation=='attachment_observe':
            phase=payload['phase'];response=payload['response'];raw_name,note_name=names(row)
            if phase not in ('file','note'):raise ValueError('unknown attachment phase')
            self.proof(c,execution_id,response,('file_upload','file_list') if phase=='file' else ('note_upload','note_list'))
            rows=response.get('files' if phase=='file' else 'docs',[]) if isinstance(response,dict) else response
            if not isinstance(rows,list):raise ValueError('invalid attachment object list')
            if isinstance(response,dict) and response.get('total')!=len(rows):
                raise ValueError('attachment object list incomplete; remains pending reconciliation')
            if phase=='file':
                matches=[r for r in rows if r.get('name')==raw_name and r.get('parent_id')==data['target']['parent_id']
                         and r.get('size')==data['bytes']]
            else:
                matches=[r for r in rows if r.get('name')==note_name and (r.get('dataset_id') or r.get('kb_id'))==data['target']['dataset_id']]
            if len(matches)!=1 or not isinstance(matches[0].get('id'),str) or not matches[0]['id']:
                raise ValueError('attachment remains pending reconciliation: no unique owned object')
            remote['candidate_file_id' if phase=='file' else 'candidate_document_id']=matches[0]['id']
        elif operation=='attachment_bind':
            phase=payload['phase'];value=payload['sha256']
            if phase not in ('file','note'):raise ValueError('unknown attachment phase')
            # Download responses are hashed directly, so qualify that exact digest.
            step='file_bytes' if phase=='file' else 'note_bytes'
            facts=c.execute("SELECT request,result FROM control_actions WHERE execution_id=? AND state='complete' AND kind='rag_read'",(execution_id,)).fetchall()
            candidate_key='candidate_file_id' if phase=='file' else 'candidate_document_id'
            expected_endpoint=('/files/'+quote(remote[candidate_key],safe='') if phase=='file' else
                '/datasets/'+quote(data['target']['dataset_id'],safe='')+'/documents/'+quote(remote[candidate_key],safe=''))
            if not any(json.loads(r['request'])['resource'].get('step')==step and
                       json.loads(r['request'])['resource'].get('endpoint')==expected_endpoint and
                       json.loads(r['result'] or '{}').get('response_sha256')==value for r in facts):
                raise ValueError('attachment download evidence missing')
            expected=row['source_sha256'] if phase=='file' else digest(note_text(row,data).encode('utf-8'))
            if value!=expected:raise ValueError('attachment download hash mismatch')
            key='file_id' if phase=='file' else 'document_id'
            remote[key]=remote['candidate_'+key];remote[key+'_sha256']=value
            state='raw_saved' if phase=='file' else 'note_saved'
            self.reconcile(c,aid,'file_upload' if phase=='file' else 'note_upload')
        elif operation=='attachment_complete':
            response=payload['response'];self.proof(c,execution_id,response,('note_chunks',))
            chunks=response.get('chunks',[]) if isinstance(response,dict) else []
            expected=note_text(row,data)
            if response.get('total')!=len(chunks) or len(chunks)!=1:raise ValueError('attachment note slice set incomplete or unexpected')
            matches=[r for r in chunks if r.get('content')==expected and r.get('document_id')==remote.get('document_id')
                     and r.get('available') in (True,1,'1')]
            if len(matches)!=1 or not matches[0].get('id'):raise ValueError('attachment note not completely readable')
            remote['chunk_id']=matches[0]['id'];state='complete';self.reconcile(c,aid,'note_chunk')
        else:raise ValueError('unknown attachment commit')
        c.execute('UPDATE attachment_tasks SET state=?,data=? WHERE attachment_id=?',(state,canonical(data),aid))
        return self.context(c.execute('SELECT * FROM attachment_tasks WHERE attachment_id=?',(aid,)).fetchone())


class AttachmentClient(ControlledRagClient):
    def __init__(self,native,actions):self.native,self.actions,self.base_url=native,actions,native.base_url

    def http(self,method,endpoint,body=None,*,step,content_sha256=None,name=None,parent_id=None,content_type='application/json'):
        resource=dict(operation='attachment_http',method=method,endpoint=endpoint,step=step,
            content_sha256=content_sha256,name=name,parent_id=parent_id,
            write_operation='attachment_'+step if method=='POST' else 'read')
        kind='rag_read' if method=='GET' else 'rag_write'
        return self.actions.run(kind,resource,lambda cancel:self.invoke_native(lambda:self._once(method,endpoint,body,content_type)))

    def upload(self,endpoint,name,blob,*,step,content_sha256,parent_id=None):
        # Same multipart layout and once-only native transport as the existing uploader.
        if any(ch in name for ch in ('\r','\n','/','\\','"')):raise ValueError('invalid attachment upload name')
        def send(cancel):
            # Read large originals only after this individual RAG slot is granted.
            content=blob.read_bytes() if isinstance(blob,Path) else blob
            if digest(content)!=content_sha256:raise ValueError('attachment upload bytes changed')
            boundary='----PaperUpstream'+uuid.uuid4().hex
            prefix=(f'--{boundary}\r\nContent-Disposition: form-data; name="parent_id"\r\n\r\n{parent_id}\r\n' if parent_id else '')
            payload=(prefix+f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()+content+f'\r\n--{boundary}--\r\n'.encode()
            return self.invoke_native(lambda:self._once('POST',endpoint,payload,'multipart/form-data; boundary='+boundary))
        return self.actions.run('rag_write',dict(operation='attachment_http',method='POST',endpoint=endpoint,step=step,
            content_sha256=content_sha256,name=name,parent_id=parent_id,write_operation='attachment_'+step),send)

    def download(self,endpoint,step):
        return self.actions.run('rag_read',dict(operation='attachment_http',method='GET',endpoint=endpoint,step=step,write_operation='read'),
            lambda cancel:self.invoke_native(lambda:self._download('/api/v1'+endpoint)))

    def deployment_identity(self):
        # Reuse the existing body-delivery identity routes, under one read permit.
        endpoint='/paperflow/deployment' if self.actions.config.production_pilot else '/pf-stage4/identity'
        return self.actions.run('rag_read',dict(operation='attachment_http',method='GET',endpoint=endpoint,
            step='deployment_identity',write_operation='read'),lambda cancel:self.invoke_native(
                lambda:self._once('GET',endpoint) if self.actions.config.production_pilot else json.loads(self._download(endpoint))))


class AttachmentWorkflow:
    def __init__(self,config,actions,client):self.config,self.actions,self.client=config,actions,client

    def run(self,attachment_id):
        commit=self.actions.commit
        ctx=commit('attachment_load',{})
        row,data=ctx['row'],ctx['data']
        if row['state']=='complete':return ctx
        report=storage_acceptance(self.config,row['category'])
        if self.client.deployment_identity()!=report['identity']:raise ValueError('deployment configuration/fingerprint changed')
        manifest=load(Path(row['package_dir'])/'manifest.json')
        original=Path(row['package_dir'])/manifest['original']
        def verify(cancel):
            if sha(Path(row['package_dir'])/'manifest.json')!=row['manifest_sha256'] or sha(original)!=row['source_sha256']:
                raise ValueError('retained attachment changed')
            return True
        self.actions.run('local',dict(operation='attachment_verify'),verify,total_timeout=120)
        target=data['target']
        if not target.get('parent_id') or not target.get('dataset_id'):raise ValueError('attachment targets not configured')
        raw_name,note_name=names(row);ds=quote(target['dataset_id'],safe='');base=f'/datasets/{ds}/documents'
        for phase,step,key,name in (('file','file_upload','file_id',raw_name),('note','note_upload','document_id',note_name)):
            if data['remote'].get(key):continue
            intent=commit('attachment_intent',dict(step=step))
            if intent['can_post']:
                blob=original if phase=='file' else note_text(row,data).encode('utf-8')
                response=self.client.upload('/files' if phase=='file' else base,name,blob,step=step,
                    content_sha256=intent['content_sha256'],parent_id=target['parent_id'] if phase=='file' else None)
            else:
                endpoint=('/files?'+urlencode(dict(parent_id=target['parent_id'],keywords=name,page=1,page_size=100)) if phase=='file'
                          else base+'?'+urlencode(dict(name=name,page=1,page_size=100)))
                response=self.client.http('GET',endpoint,step='file_list' if phase=='file' else 'note_list')
            ctx=commit('attachment_observe',dict(phase=phase,response=response));row,data=ctx['row'],ctx['data']
            candidate=data['remote']['candidate_'+key]
            endpoint='/files/'+quote(candidate,safe='') if phase=='file' else base+'/'+quote(candidate,safe='')
            blob=self.client.download(endpoint,'file_bytes' if phase=='file' else 'note_bytes')
            ctx=commit('attachment_bind',dict(phase=phase,sha256=digest(blob)));row,data=ctx['row'],ctx['data']
        intent=commit('attachment_intent',dict(step='note_chunk'))
        endpoint=base+'/'+quote(data['remote']['document_id'],safe='')+'/chunks'
        if intent['can_post']:
            self.client.http('POST',endpoint,dict(content=note_text(row,data)),step='note_chunk',content_sha256=intent['content_sha256'])
        response=self.client.http('GET',endpoint+'?page=1&page_size=100',step='note_chunks')
        return commit('attachment_complete',dict(response=response))
