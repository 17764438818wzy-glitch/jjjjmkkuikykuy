"""RAGFlow document handoff; never invokes the legacy SQL or rating pipeline."""
from __future__ import annotations

import json
import uuid
import copy
import ssl
import mimetypes
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime,timezone
from http.client import HTTPException
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from engine import digest


DEFAULT_RAGFLOW_URL = "http://192.168.1.120:8088"
LAN_HTTP_ORIGIN = ("http", "192.168.1.120", 8088)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class RagflowHTTPError(RuntimeError):
    def __init__(self,code):
        self.status_code=code
        if code in {401, 403}:
            message = '认证失败，请检查 API Key 是否有效及是否有知识库访问权限。'
        elif 300 <= code < 400:
            message = '服务返回重定向，已阻止跟随；请填写最终服务地址。'
        else:
            message = '服务请求失败，请检查服务状态、地址和接口版本。'
        super().__init__(f'RAGFlow HTTP {code}：{message}')


class RagflowConnectionError(RuntimeError):
    pass


class RagflowTimeoutError(RagflowConnectionError):
    pass


class RagflowClient:
    # Used by the upload reconciler to distinguish the real API client from
    # deliberately small injected test doubles and integrations.
    supports_receipt_validation = True

    def __init__(self, base_url, api_key):
        try:
            parsed = urlsplit(base_url.strip())
            origin = (parsed.scheme, parsed.hostname, parsed.port)
        except ValueError:
            raise ValueError("RAGFlow 地址无效，请检查主机和端口（0–65535）。") from None
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            raise ValueError("RAGFlow 地址不允许：须为 http/https 服务地址，不包含账号、查询参数或片段。")
        if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"} and origin != LAN_HTTP_ORIGIN:
            raise ValueError("RAGFlow 地址不允许：HTTP 仅支持本机 localhost、127.0.0.1、::1 和 http://192.168.1.120:8088；其他地址须使用 HTTPS。")
        if not api_key.strip():
            raise ValueError("请先填写 RAGFlow API Key。界面通过 Windows 加密保存密钥。")
        path = parsed.path.rstrip("/")
        while path.endswith("/api/v1"):
            path = path[:-len("/api/v1")].rstrip("/")
        self.base_url = urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
        self.api_key = api_key.strip()
        # Only this LAN server bypasses environment/Windows proxy discovery.
        # Keep the default HTTPS handler and its certificate verification.
        handlers = [NoRedirect()]
        if (parsed.hostname, parsed.port) == LAN_HTTP_ORIGIN[1:]:
            handlers.append(ProxyHandler({}))
        self.opener = build_opener(*handlers)

    def request(self, method, endpoint, body=None, content_type="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else None
        request = Request(self.base_url + "/api/v1" + endpoint, data=data, method=method,
                          headers={"Authorization": "Bearer " + self.api_key, "Content-Type": content_type})
        attempts = 3 if method == "GET" else 1
        for attempt in range(attempts):
            try:
                return self._request_once(request, timeout=15 if method == "GET" else 120)
            except (RagflowConnectionError, RagflowHTTPError) as exc:
                retryable = isinstance(exc, RagflowConnectionError) or exc.status_code in {502, 503, 504}
                if not retryable or attempt == attempts - 1:
                    if method != "GET" and (isinstance(exc, RagflowConnectionError) or exc.status_code >= 500):
                        exc.args = (str(exc) + " 写入未自动重试；结果可能尚未确认，请先在 RAGFlow 核对，避免重复提交。",)
                    raise
                sleep(0.5 * (attempt + 1))

    def _request_once(self, request, timeout):
        try:
            with self.opener.open(request, timeout=timeout) as response:
                payload = json.load(response)
        except HTTPError as exc:
            exc.close()
            raise RagflowHTTPError(exc.code) from None
        except (URLError, OSError, HTTPException) as exc:
            reason = exc.reason if isinstance(exc, URLError) else exc
            if isinstance(reason, ssl.SSLError):
                raise RuntimeError("RAGFlow HTTPS 证书校验或 TLS 连接失败，请检查服务端证书和 HTTPS 配置。") from None
            if isinstance(reason, TimeoutError):
                raise RagflowTimeoutError("RAGFlow 请求超时，请检查网络和服务器响应速度。") from None
            raise RagflowConnectionError("RAGFlow 连接失败，请检查服务地址、端口、局域网连通性及服务是否启动。") from None
        except ValueError:
            raise RuntimeError("RAGFlow 响应不是有效 JSON，请检查服务地址和接口版本。") from None
        if not isinstance(payload, dict) or payload.get("code") != 0:
            if isinstance(payload, dict) and payload.get("code") in (109, 401, 403):
                raise RuntimeError("RAGFlow 认证失败，请检查 API Key 是否有效及是否有知识库访问权限。")
            message = str(payload.get("message", "响应格式不正确")) if isinstance(payload, dict) else "响应格式不正确"
            raise RuntimeError("RAGFlow：" + message.replace(self.api_key, "[密钥]")[:300])
        return payload.get("data")

    def datasets(self):
        results = []
        for page in range(1, 101):
            rows = self.request("GET", f"/datasets?page={page}&page_size=100")
            if not isinstance(rows, list):
                raise RuntimeError("知识库列表响应不符合预期。")
            results.extend(rows)
            if len(rows) < 100:
                return results
        raise RuntimeError("知识库超过一万条，请缩小 API Key 的可见范围。")

    def upload(self, dataset_id, path, remote_name=None):
        path = Path(path)
        filename=remote_name or path.name
        if any(token in filename for token in ('\r','\n','/','\\')):
            raise ValueError('RAGFlow 上传文件名无效。')
        if remote_name is None and path.name=='ingest.md' and path.with_name('metadata.json').exists():
            from engine import safe_component
            metadata=json.loads(path.with_name('metadata.json').read_text(encoding='utf-8'))
            filename=upload_name(path)
        return self.upload_bytes(dataset_id,filename,path.read_bytes())

    def upload_bytes(self,dataset_id,filename,content):
        """Upload the exact bytes already checked by the calling work pool."""
        data=self._multipart_upload(f"/datasets/{quote(dataset_id, safe='')}/documents",filename,content)
        if not isinstance(data, list) or len(data) != 1 or not data[0].get("id"):
            raise RuntimeError("上传响应未包含唯一文档 ID，请到 RAGFlow 核对。")
        return data[0]["id"]

    def upload_file(self,parent_id,filename,content):
        """RAGFlow's native Files store accepts originals such as DWG and ZIP."""
        data=self._multipart_upload('/files',filename,content,fields={'parent_id':parent_id})
        if not isinstance(data,list) or len(data)!=1 or not data[0].get('id'):
            raise RuntimeError('附件上传响应未包含唯一文件 ID，请核对原回执。')
        return data[0]['id']

    def _multipart_upload(self,endpoint,filename,content,*,fields=None):
        if not isinstance(content,bytes):
            raise ValueError('Upload content must be bytes')
        if any(token in filename for token in ('\r','\n','/','\\')):
            raise ValueError('RAGFlow 上传文件名无效。')
        boundary = "----PaperUpstream" + uuid.uuid4().hex
        content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        if filename.lower().endswith(('.md','.markdown')):
            content_type = 'text/markdown; charset=utf-8'
        prefix=''.join(f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'
                       for key,value in (fields or {}).items()).encode()
        payload = prefix+(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{filename}"\r\n'
                   f'Content-Type: {content_type}\r\n\r\n').encode() + content + f"\r\n--{boundary}--\r\n".encode()
        return self.request("POST",endpoint,payload,"multipart/form-data; boundary="+boundary)

    def download_file(self,file_id):
        request=Request(self.base_url+'/api/v1/files/'+quote(file_id,safe=''),
                        headers={'Authorization':'Bearer '+self.api_key})
        try:
            with self.opener.open(request,timeout=30) as response:return response.read()
        except HTTPError as exc:
            exc.close();raise RagflowHTTPError(exc.code) from None
        except (URLError,OSError,HTTPException):
            raise RagflowConnectionError('RAGFlow 附件下载失败，保留原回执等待核对。') from None

    def documents(self,dataset_id,name=None,document_id=None):
        from urllib.parse import urlencode
        query=urlencode({'page':1,'page_size':100,**({'name':name} if name else {}),**({'id':document_id} if document_id else {})})
        data=self.request('GET',f'/datasets/{quote(dataset_id,safe="")}/documents?'+query)
        if not isinstance(data,dict) or not isinstance(data.get('docs'),list): raise RuntimeError('RAGFlow 文档列表响应无效')
        return data['docs']

    def parse(self,dataset_id,document_id):
        body={'document_ids':[document_id]}
        try: self.request('POST',f'/datasets/{quote(dataset_id,safe="")}/documents/parse',body)
        except RagflowHTTPError as exc:
            if exc.status_code not in {404,405}: raise
            self.request('POST',f'/datasets/{quote(dataset_id,safe="")}/chunks',body)


def upload_name(path):
    from engine import safe_component
    metadata=json.loads(path.with_name('metadata.json').read_text(encoding='utf-8'))
    suffix='_'+digest(path)[:16] if metadata.get('automatic_approval') else ''
    return safe_component(metadata.get('title') or '待确认题目')+'_'+safe_component(metadata['id'])+suffix+'.md'


def _save_item(store,run_id,item):
    """Merge one worker's receipt into the latest manifest without clobbering peers."""
    with store.lock:
        manifest=store.load(run_id)
        current=next((row for row in manifest.get('items',[]) if row.get('id')==item.get('id')),None)
        if current is None: return
        uploads=item.get('uploads')
        for key,value in item.items():
            if key!='uploads': current[key]=copy.deepcopy(value)
        if isinstance(uploads,dict):
            current.setdefault('uploads',{}).update(copy.deepcopy(uploads))
        store.save(manifest)


def upload_items(store, run_id, item_ids, client, dataset, callback=lambda value: None, include_assets=False, upload_config=None):
    # Receipt lookup, reservation and acknowledgement form one transaction per Store.
    # Persist the reservation before network I/O, including when an acknowledgement is lost.
    with store.lock:
        return _upload_items(store,run_id,item_ids,client,dataset,callback,include_assets,upload_config)


def _upload_items(store, run_id, item_ids, client, dataset, callback, include_assets=False, upload_config=None):
    from upload_tracking import UploadIndex
    manifest = store.load(run_id)
    selected = [item for item in manifest["items"] if item["id"] in item_ids]
    if not selected:
        raise ValueError("请先选择导入文件。")
    if manifest["status"] == "running":
        raise ValueError("本批仍在处理，请等待完成。")
    if len({item["major"] for item in selected}) != 1:
        raise ValueError("每次只导入一个专业，请先按专业筛选。")
    for item in selected:
        if item.get('package_path'):
            from evidence import package_fingerprint
            from knowledge_package import knowledge_files
            package=Path(item['package_path'])
            metadata=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
            if metadata.get('category')=='教育教学类' and (package/'manifest.json').is_file():
                knowledge_files(package,require_stable_paper_id=True)
            if not item.get('reviewed_package_hash') or package_fingerprint(Path(item['package_path']))!=item['reviewed_package_hash']:
                raise ValueError('证据包未检查或检查后发生变化，请重新核对正文、表格与图片。')
        if item["status"] not in {"pass", "review"} or not item.get("reviewed_hash") or digest(Path(item["path"])) != item["reviewed_hash"]:
            raise ValueError("所选文件包含未检查、内容已变更或已筛除的文件，请先检查确认。")
    destination_key = client.base_url + "|" + dataset["id"]
    from upload_tracking import paper_id_for_item
    histories=[h for h in store.history() if h.get('run_id')!=run_id]+[manifest]
    index=UploadIndex(histories)
    count = 0
    for item in selected:
        path = Path(item["path"])
        fingerprint = digest(path)
        existing=index.matches(item,client.base_url,dataset['id'])
        confirmed=next((hit for hit in existing if hit['record'].get('status')=='uploaded' and hit['record'].get('document_id')),None)
        uncertain=[hit for hit in existing if hit['record'].get('status') in {'sending','unknown'} or
                   (hit['record'].get('status')=='uploaded' and not hit['record'].get('document_id'))]
        # A local "uploaded" receipt is not proof that the remote document
        # still exists: an operator may have emptied the knowledge base, or a
        # previous response may have returned a stale/non-owned ID.  Recheck
        # confirmed receipts before skipping the network upload.  Authentication
        # and transport failures remain hard errors; only a clear missing or
        # non-owned document is treated as stale and eligible for re-upload.
        if confirmed is not None and getattr(client,'supports_receipt_validation',False) and hasattr(client,'documents'):
            prior=confirmed['record']; remote_id=prior.get('document_id')
            try:
                remote_docs=client.documents(dataset['id'],document_id=remote_id)
            except Exception as exc:
                message=str(exc).lower()
                stale=('you don\'t own the document' in message or 'document not found' in message or
                       'does not exist' in message or 'not found' in message)
                if not stale:
                    raise
                remote_docs=[]
            if len(remote_docs)!=1:
                confirmed=None
        if confirmed is None and uncertain:
            hit=uncertain[0];prior=hit['record']
            adopt_receipt(item,destination_key,hit,fingerprint)
            item['uploads'][destination_key].update(status='unknown',dedupe_status='reconcile',version=2);_save_item(store,run_id,item)
            name=prior.get('remote_name')
            if not name:
                try:name=remote_name(Path(hit['item']['path']))
                except (OSError,ValueError,KeyError):name=None
            if not name or not hasattr(client,'documents'):
                raise RuntimeError('上次上传结果不确定，已阻止重复提交；重试时将尝试自动确认。')
            docs=[d for d in client.documents(dataset['id'],name=name) if d.get('name')==name and d.get('id')]
            if len(docs)!=1:raise RuntimeError('上次上传结果尚未唯一确认，未重复提交；可稍后重试自动确认。')
            prior.update(status='uploaded',document_id=docs[0]['id'],remote_name=name,confirmed_at=now())
            if hit['manifest'].get('run_id'):store.save(hit['manifest'])
            confirmed=hit
        if confirmed is not None:
            adopt_receipt(item,destination_key,confirmed,fingerprint)
            _save_item(store,run_id,item)
            if include_assets:
                _upload_package_assets(store,manifest,item,destination_key,dataset,client,callback=callback,
                                       upload_config=upload_config)
            callback(f"已上传，跳过重复上传：{item.get('title') or item['id']}（{dataset['name']}）")
            continue
        prior_record=item.setdefault('uploads',{}).get(destination_key,{})
        item['uploads'][destination_key] = {"status": "sending", "dataset_name": dataset["name"],
            'dataset_id':dataset['id'],'paper_id':paper_id_for_item(item),'source_hash':item.get('source_hash',''),"content_hash": fingerprint,
            'remote_name':remote_name(path),'attempted_at':now(),'version':2,'route':'paper',
            'retry_count':int(prior_record.get('retry_count',0) or 0)+1,'dedupe_status':'new'}
        _save_item(store,run_id,item)
        try:
            document_id = client.upload(dataset["id"], path)
        except Exception as exc:
            certain_rejection=isinstance(exc,RagflowHTTPError) and exc.status_code in {400,401,403,404,405,413,415,422,429}
            item["uploads"][destination_key]["status"] = "failed" if certain_rejection else "unknown"
            _save_item(store,run_id,item)
            raise
        if not isinstance(document_id,str) or not document_id.strip():
            item['uploads'][destination_key]['status']='unknown';_save_item(store,run_id,item)
            raise RuntimeError('上传未返回有效文档编号，结果待确认，未标记已上传。')
        item["uploads"][destination_key].update(status="uploaded", document_id=document_id,uploaded_at=now(),
                                                dedupe_status='uploaded')
        _save_item(store,run_id,item)
        count += 1
        if include_assets:
            _upload_package_assets(store,manifest,item,destination_key,dataset,client,callback=callback,
                                   upload_config=upload_config)
        callback(f"已上传：{item.get('title') or item['id']}（{dataset['name']}）")
    return count


def now():return datetime.now(timezone.utc).isoformat(timespec='seconds')


def remote_name(path):
    return upload_name(path) if path.name=='ingest.md' and path.with_name('metadata.json').exists() else path.name


_IMAGE_SUFFIXES={'.png','.jpg','.jpeg','.webp','.gif','.bmp','.tif','.tiff'}
# Keep the legacy discovery set above unchanged; routing has its own explicit
# allow-list so a future discovery-only extension cannot accidentally enter a
# dataset without an intentional route decision.
_ROUTE_IMAGE_SUFFIXES={'.png','.jpg','.jpeg','.webp','.gif','.bmp','.tif','.tiff'}

_TEXT_SUFFIXES={'.md','.markdown','.txt','.csv','.tsv','.json','.yaml','.yml','.xml','.html','.htm'}

_ROLE_ROUTES={
    'paper_primary':'paper',
    'paper_assets':'paper',
    'image':'image',
    'image_projection':'image',
}

def is_derived_text_role(role):
    """Return whether a role is a paper-derived text role."""
    return isinstance(role,str) and role.startswith('paper_') and role != 'paper_primary'


def _normalized_nontrivial_lines(text):
    for line in str(text or '').splitlines():
        normalized=unicodedata.normalize('NFKC',line.strip())
        if len(normalized)>=12:
            yield normalized


def _redundancy_stats(derived_text,primary_text):
    primary_lines=set(_normalized_nontrivial_lines(primary_text))
    derived_lines=list(_normalized_nontrivial_lines(derived_text))
    hits=sum(line in primary_lines for line in derived_lines)
    total=len(derived_lines)
    return (hits/total if total else 1.0),hits,total


def redundancy_ratio(derived_text,primary_text):
    """Return the line-level subset ratio after NFKC normalization."""
    return _redundancy_stats(derived_text,primary_text)[0]


def _is_text_path(path):
    return Path(path).suffix.lower() in _TEXT_SUFFIXES


def _threshold_label(threshold):
    percentage=threshold*100
    return f'{int(percentage)}%' if percentage.is_integer() else f'{percentage:.1f}%'


def _derived_upload_policy(config=None):
    from auto_config import normalize_derived_upload_policy
    return normalize_derived_upload_policy(config)


def _increment_upload_stat(stats,key):
    if stats is None:
        return
    lock=stats.get('_lock')
    if lock is None:
        stats[key]=stats.get(key,0)+1
        return
    with lock:
        stats[key]=stats.get(key,0)+1


def _emit_upload_summary(callback,primary_count,stats):
    skipped=stats.get('derived_skipped_redundant',0)+stats.get('derived_skipped_unreadable',0)
    if stats.get('derived_skipped_unreadable',0):
        message=f'本批跳过 {skipped} 个派生文件，上传 {primary_count} 个正文文件'
    else:
        message=f'本批跳过 {skipped} 个派生文件（全部冗余），上传 {primary_count} 个正文文件'
    if stats.get('derived_uploaded',0):
        message+=f'，另上传 {stats["derived_uploaded"]} 个非冗余派生文件'
    callback(message)


_ROUTE_LOCKS={}
_ROUTE_LOCKS_GUARD=threading.Lock()


def _route_lock(client,dataset_id,paper_id):
    key=(client.base_url,dataset_id,paper_id or '')
    with _ROUTE_LOCKS_GUARD:
        return _ROUTE_LOCKS.setdefault(key,threading.RLock())


def artifact_route(path,role=None):
    if role in _ROLE_ROUTES:
        return _ROLE_ROUTES[role]
    suffix=Path(path).suffix.lower()
    if role is not None:
        if is_derived_text_role(role):
            if suffix not in _TEXT_SUFFIXES:
                raise ValueError(f'未知文件类型，拒绝上传：{path}')
            return 'paper'
        raise ValueError(f'未知角色，拒绝上传：{role}')
    if suffix in {'.md','.markdown'}:
        return 'paper'
    if suffix in _ROUTE_IMAGE_SUFFIXES:
        return 'image'
    raise ValueError(f'未知文件类型，拒绝上传：{path}')


def _upload_call(client,dataset_id,path,remote_file_name):
    """Call real clients and small test doubles with one compatible shape."""
    if remote_file_name is None:
        return client.upload(dataset_id,path)
    try:
        return client.upload(dataset_id,path,remote_name=remote_file_name)
    except TypeError as exc:
        # Older injected clients only accepted (dataset_id, path).  Keep that
        # adapter working without hiding unrelated TypeErrors from the client.
        if 'remote_name' not in str(exc):
            raise
        return client.upload(dataset_id,path)


def _package_asset_files(package):
    """Return checked upload artifacts as (relative, path, route, fallback)."""
    package=Path(package).resolve()
    manifest_path=package/'manifest.json'
    if not manifest_path.is_file():
        return []
    from knowledge_package import knowledge_files
    metadata=json.loads((package/'metadata.json').read_text(encoding='utf-8'))
    knowledge_files(package,require_stable_paper_id=metadata.get('category')=='教育教学类')
    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
    # A checked package can contain figure JSON with a per-image privacy
    # decision.  Keep an unapproved figure out of both the image projection
    # and the binary upload even if a legacy metadata row was stale.
    blocked_image_paths=set()
    for figure_row in manifest.get('figures',[]):
        figure_path=figure_row.get('path') if isinstance(figure_row,dict) else None
        if not figure_path:
            continue
        figure_file=package/figure_path
        if not figure_file.is_file():
            continue
        try:
            figure=json.loads(figure_file.read_text(encoding='utf-8'))
        except (OSError,ValueError):
            continue
        image=figure.get('image') or {}
        if image.get('privacy_status')!='approved' and image.get('path'):
            blocked_image_paths.add(image['path'])
    approved_image_paths=set()
    for resource in metadata.get('resources',[]):
        local=resource.get('local_path','')
        path=(package/local).resolve()
        if (resource.get('privacy_status')=='approved' and local.startswith('assets/') and
                path.is_file() and path.is_relative_to(package)):
            if path.suffix.lower() not in _IMAGE_SUFFIXES:
                artifact_route(local,None)
            approved_image_paths.add(local)
    rows=[]
    has_roles=any(isinstance(row,dict) and 'role' in row for row in manifest.get('files',[]))
    for row in manifest.get('files',[]):
        if not isinstance(row,dict):
            continue
        relative=row.get('path')
        if not relative:
            continue
        path=(package/relative).resolve()
        if not path.is_file() or not path.is_relative_to(package):
            continue
        role_present='role' in row
        role=row.get('role')
        if role_present:
            if role=='paper_primary':
                continue
            route=artifact_route(relative,role)
            if role=='image' and (relative not in approved_image_paths or relative in blocked_image_paths):
                continue
            rows.append((relative,path,route,False))
            continue
        if has_roles:
            continue
        if relative in {'ingest.md','document.md','metadata.json','manifest.json'}:
            continue
        if relative.startswith('metadata/') or relative.startswith('writing_units/'):
            continue
        if Path(relative).suffix.lower() in {'.md','.markdown'}:
            rows.append((relative,path,artifact_route(relative,None),'legacy'))
        elif relative in approved_image_paths and relative not in blocked_image_paths:
            rows.append((relative,path,artifact_route(relative,None),'legacy'))
    seen=set();result=[]
    for relative,path,route,fallback in rows:
        if relative not in seen:
            seen.add(relative)
            result.append((relative,path,route,fallback))
    return result


def _asset_remote_name(primary_path,relative):
    stem=Path(remote_name(primary_path)).stem
    if relative == 'ragflow_assets.md':
        return f'{stem}__ragflow_assets.md'
    return f'{stem}__asset_{Path(relative).name}'


def _upload_package_assets(store,manifest,item,destination_key,dataset,client,target_route=None,
                           callback=lambda message:None,upload_config=None,upload_stats=None):
    package=item.get('package_path')
    if not package:
        return
    files=_package_asset_files(package)
    if target_route is not None:
        files=[row for row in files if row[2]==target_route]
    if not files:
        return
    policy=_derived_upload_policy(upload_config)
    package_manifest=json.loads((Path(package)/'manifest.json').read_text(encoding='utf-8'))
    primary_rel=next((row.get('path') for row in package_manifest.get('files',[])
                      if isinstance(row,dict) and row.get('role')=='paper_primary'),None)
    if not primary_rel:
        primary_rel='ingest.md'
    paper_files=[row for row in files if row[2]=='paper']
    primary_text=None
    if paper_files:
        primary_file=Path(item.get('path','')).resolve()
        try:
            primary_text=primary_file.read_text(encoding='utf-8')
        except (OSError,UnicodeError) as exc:
            callback(f'警告：主文件 {primary_file} 不存在或无法读取，已跳过派生文件（{exc}）')
    from upload_tracking import paper_id_for_item
    from upload_tracking import UploadIndex
    record=item.setdefault('uploads',{}).setdefault(destination_key,{
        'status':'pending','dataset_name':dataset['name'],'dataset_id':dataset['id'],
        'paper_id':paper_id_for_item(item),'route':target_route or 'legacy','version':2,
        'retry_count':0,'dedupe_status':'pending',
    })
    receipts=record.setdefault('asset_uploads',[])
    by_path={row.get('path'):row for row in receipts if isinstance(row,dict) and row.get('path')}
    histories=[h for h in store.history() if h.get('run_id')!=manifest.get('run_id')]+[manifest]
    history_index=UploadIndex(histories)
    primary_path=Path(item['path'])
    for relative,path,route,fallback in files:
        if fallback:
            callback(f'旧知识包 {relative} 没有 role，按扩展名兼容路由到 {route} 库')
        if route=='paper' and relative!=primary_rel:
            if primary_text is None:
                _increment_upload_stat(upload_stats,'derived_skipped_unreadable')
                continue
            if policy['skip_derived_upload']:
                try:
                    derived_text=path.read_text(encoding='utf-8')
                except (OSError,UnicodeError) as exc:
                    callback(f'警告：派生文件 {relative} 无法读取，已跳过（{exc}）')
                    _increment_upload_stat(upload_stats,'derived_skipped_unreadable')
                    continue
                ratio,hits,total=_redundancy_stats(derived_text,primary_text)
                if ratio>=policy['derived_redundancy_threshold']:
                    callback(f'[跳过] 派生文件冗余  {_asset_remote_name(primary_path,relative)}\n'
                             f'       子集率 {ratio*100:.1f}% ({hits}/{total} 行)  阈值 {_threshold_label(policy["derived_redundancy_threshold"])}  → 未上传')
                    _increment_upload_stat(upload_stats,'derived_skipped_redundant')
                    continue
        fingerprint=digest(path);name=_asset_remote_name(primary_path,relative)
        prior=by_path.get(relative)
        if prior is None:
            for hit in history_index.matches(item,client.base_url,dataset['id']):
                if hit['item'].get('id')==item.get('id'):
                    continue
                same_paper = bool(paper_id_for_item(item) and
                                  paper_id_for_item(hit['item']) == paper_id_for_item(item))
                old=next((row for row in hit['record'].get('asset_uploads',[])
                          if row.get('path')==relative and row.get('status')=='uploaded'
                          and row.get('document_id') and
                          (row.get('content_hash')==fingerprint or same_paper)),None)
                if old is None:
                    continue
                if getattr(client,'supports_receipt_validation',False) and hasattr(client,'documents'):
                    if len(client.documents(dataset['id'],document_id=old['document_id']))!=1:
                        continue
                prior=copy.deepcopy(old)
                prior['reused_from']={'run_id':hit['manifest'].get('run_id',''),'item_id':hit['item'].get('id','')}
                prior['local_content_hash']=fingerprint
                prior['dedupe_status']='reused';prior['version']=2
                receipts.append(prior);by_path[relative]=prior
                _save_item(store,manifest['run_id'],item)
                break
        if (prior and prior.get('status')=='uploaded' and prior.get('document_id') and
                (prior.get('content_hash')==fingerprint or prior.get('reused_from') or
                 record.get('reused_from'))):
            if record.get('reused_from') and prior.get('content_hash')!=fingerprint:
                prior['local_content_hash']=fingerprint
                prior['dedupe_status']='reused';prior['version']=2
            continue
        if prior and prior.get('status') in {'sending','unknown'}:
            name=prior.get('remote_name') or name
            docs=[]
            if hasattr(client,'documents'):
                docs=[d for d in client.documents(dataset['id'],name=name)
                      if d.get('id') and (not name or d.get('name')==name)]
            if len(docs)==1:
                prior.update(status='uploaded',document_id=docs[0]['id'],remote_name=name,confirmed_at=now())
                continue
            record['asset_status']='unknown';_save_item(store,manifest['run_id'],item)
            raise RuntimeError(f'资产 {relative} 上次上传结果不确定，已阻止重复提交；请先在 RAGFlow 核对。')
        receipt={'path':relative,'remote_name':name,'content_hash':fingerprint,'status':'sending','attempted_at':now(),
                 'version':2,'retry_count':int(prior.get('retry_count',0) or 0)+1 if prior else 1,
                 'dedupe_status':'new'}
        if prior:
            prior.clear();prior.update(receipt)
        else:
            receipts.append(receipt);by_path[relative]=receipt
        record['asset_status']='sending';_save_item(store,manifest['run_id'],item)
        try:
            document_id=_upload_call(client,dataset['id'],path,name)
        except Exception as exc:
            certain=isinstance(exc,RagflowHTTPError) and exc.status_code in {400,401,403,404,405,413,415,422,429}
            receipt['status']='failed' if certain else 'unknown';_save_item(store,manifest['run_id'],item);raise
        if not isinstance(document_id,str) or not document_id.strip():
            receipt['status']='unknown';_save_item(store,manifest['run_id'],item)
            raise RuntimeError(f'资产 {relative} 上传未返回有效文档编号，结果待确认。')
        receipt.update(status='uploaded',document_id=document_id,uploaded_at=now(),dedupe_status='uploaded')
        record['asset_status']='uploaded';_save_item(store,manifest['run_id'],item)
        if route=='paper' and relative!=primary_rel:
            _increment_upload_stat(upload_stats,'derived_uploaded')
    complete=bool(receipts) and all(r.get('status')=='uploaded' and r.get('document_id') for r in receipts)
    record['asset_status']='uploaded' if complete else ('skipped' if not receipts else 'pending')
    record['asset_count']=len(receipts)
    # An image destination has no separate primary document: its projection
    # and approved images are the complete upload.  Previously every child
    # receipt was acknowledged while the parent record remained ``pending``,
    # so a successful image handoff looked like a batch failure forever.
    if target_route=='image' and complete:
        primary=next((r for r in receipts if r.get('path')=='ragflow_images.md'),receipts[0])
        record.update(status='uploaded',document_id=primary['document_id'],
                      uploaded_at=primary.get('uploaded_at') or primary.get('confirmed_at') or now(),
                      dedupe_status='uploaded')
    _save_item(store,manifest['run_id'],item)


def upload_routed_items(store,run_id,item_ids,client,datasets,callback=lambda message:None,auto_parse=False,
                        concurrency=None,upload_config=None):
    """Upload a package's paper and image artifacts to independently routed datasets."""
    paper=datasets.get('paper') if isinstance(datasets,dict) else None
    image=datasets.get('image') if isinstance(datasets,dict) else None
    if not isinstance(paper,dict) or not paper.get('id'):
        raise ValueError('md 知识库缺少 dataset_id，请在当前专业的 md 知识库下拉框中选择并保存')
    options=concurrency if isinstance(concurrency,dict) else {}
    enabled=bool(options.get('enabled'))
    workers=max(1,int(options.get('papers',1))) if enabled else 1
    upload_stats={'derived_skipped_redundant':0,'derived_skipped_unreadable':0,'derived_uploaded':0,
                  '_lock':threading.Lock()}

    def upload_one_unlocked(item_id):
        count=_upload_items(store,run_id,[item_id],client,paper,callback,include_assets=False,
                             upload_config=upload_config)
        manifest=store.load(run_id)
        item=next(item for item in manifest.get('items',[]) if item.get('id')==item_id)
        key=client.base_url+'|'+paper['id']
        _upload_package_assets(store,manifest,item,key,paper,client,target_route='paper',callback=callback,
                               upload_config=upload_config,upload_stats=upload_stats)
        if isinstance(image,dict) and image.get('id'):
            key=client.base_url+'|'+image['id']
            _upload_package_assets(store,manifest,item,key,image,client,target_route='image',callback=callback,
                                   upload_config=upload_config,upload_stats=upload_stats)
        else:
            callback(f'专业“{item.get("major","")}”未选择图片知识库，已跳过图片上传')
        return count

    def upload_one(item_id):
        manifest=store.load(run_id)
        item=next(item for item in manifest.get('items',[]) if item.get('id')==item_id)
        from upload_tracking import paper_id_for_item
        with _route_lock(client,paper['id'],paper_id_for_item(item)):
            return upload_one_unlocked(item_id)

    if workers<=1:
        with store.lock:
            total=sum(upload_one(item_id) for item_id in item_ids)
            _emit_upload_summary(callback,total,upload_stats)
            return total
    total=0;failures=[]
    with ThreadPoolExecutor(max_workers=workers,thread_name_prefix='upload') as pool:
        futures=[pool.submit(upload_one,item_id) for item_id in item_ids]
        for future in as_completed(futures):
            try: total+=future.result()
            except Exception as exc:
                failures.append(exc);callback('上传失败：'+str(exc))
    if failures: raise failures[0]
    _emit_upload_summary(callback,total,upload_stats)
    return total


def adopt_receipt(item,key,hit,fingerprint):
    receipt=copy.deepcopy(hit['record'])
    receipt['local_content_hash']=fingerprint
    receipt['version']=2
    receipt.setdefault('route','paper')
    receipt['dedupe_status']='reused' if hit['item']['id']!=item['id'] else receipt.get('dedupe_status','confirmed')
    if hit['item']['id']!=item['id']:
        receipt['reused_from']={'run_id':hit['manifest'].get('run_id',''),'item_id':hit['item']['id']}
    item.setdefault('uploads',{})[key]=receipt
