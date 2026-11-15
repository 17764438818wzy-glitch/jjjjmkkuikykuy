"""Read bound RAGFlow execution records; expose only a small diagnostic summary."""
from __future__ import annotations

import hashlib
import json
import re
import time
from collections import Counter, OrderedDict
from threading import RLock
from pathlib import Path
from urllib.parse import urlencode
from ingest_execution import ArtifactEvidencePending, current_execution
from education_stream_ingest import write_json
from ingest_source_text import matches_parsed_source


def _output(dsl, node, field):
    value=(dsl.get('components',{}).get(node,{}).get('obj',{}).get('params',{})
           .get('outputs',{}).get(field))
    return value.get('value') if isinstance(value,dict) else None


def _nonblank_lines(text):
    # The observed Markdown parser removes blank lines, but retains every other
    # character. Do not normalize punctuation, words or intra-line whitespace.
    return '\n'.join(line for line in text.splitlines() if line.strip())


def _safe_reason(reason):
    if not isinstance(reason,str): return '节点执行失败'
    reason=re.sub(r'sk-[A-Za-z0-9_-]+|Bearer\s+\S+', '[credential]', reason)
    return reason.replace('\x00','')[:500]


def execution_diagnostic(task, remote, log):
    """Reject stale/cross-document logs before interpreting executed-node errors."""
    if (not current_execution(task, remote) or remote.get('run')!='FAIL' or not isinstance(log,dict)
            or log.get('document_id')!=task.get('remote_id')
            or remote.get('id')!=task.get('remote_id')
            or log.get('kb_id')!=task.get('dataset_id')
            or log.get('document_name')!=task.get('remote_name')
            or log.get('pipeline_id')!=(task.get('execution_pipeline_id') or task.get('pipeline_id'))
            or log.get('task_type')!='Parse' or str(log.get('operation_status')) not in ('4','FAIL')
            or not remote.get('process_begin_at')
            or log.get('process_begin_at')!=remote['process_begin_at']):
        return None
    dsl=log.get('dsl')
    if isinstance(dsl,str):
        try: dsl=json.loads(dsl)
        except ValueError:return None
    if not isinstance(dsl,dict):return None
    original=_output(dsl,'TokenChunker:WholeEducation','chunks')
    if not isinstance(original,list) or len(original)!=1 or not isinstance(original[0],dict):return None
    text=original[0].get('text')
    if not isinstance(text,str):return None
    try: source=Path(task['path']).read_bytes()
    except (OSError,KeyError):return None
    if hashlib.sha256(source).hexdigest()!=task.get('content_sha256'):return None
    if not matches_parsed_source(source.decode('utf-8-sig'), text):return None
    parsed_hash=hashlib.sha256(text.encode('utf-8')).hexdigest()
    for node in dsl.get('path') or []:
        error=_output(dsl,node,'_ERROR')
        if not isinstance(error,str) or not error.strip():continue
        if node not in {'CodeExec:PrepareEducation','CodeExec:EducationGate','CodeExec:ArtifactBody',
                        'CodeExec:ArtifactSources','Extractor:EducationAnnotation','Tokenizer:EducationIndex'}:
            return None
        result={'source':'execution_node','component_id':node,'dependency':'pipeline',
                'kind':'node_error','outcome':'technical_review','reason':'节点执行失败，需检查节点诊断',
                'log_id':log.get('id'),'execution_id':dsl.get('task_id'),
                'remote_id':task['remote_id'],'pipeline_id':log['pipeline_id'],
                'attempt':task.get('execution_attempt',0),'process_begin_at':log['process_begin_at']}
        # Only the final exception line is authoritative. Tracebacks can contain
        # entire quoted papers and even text resembling an exception marker.
        line=error.rstrip().splitlines()[-1]
        marker=re.fullmatch(r'RuntimeError: (EDUCATION_HOLD|EDUCATION_REJECTED)\|(.*)',line)
        if marker and node in {'CodeExec:PrepareEducation','CodeExec:EducationGate'}:
            try: event=json.loads(marker.group(2))
            except ValueError:return result
            if not isinstance(event,dict) or not isinstance(event.get('metadata'),dict):return result
            meta=event['metadata'];issue=meta.get('education_processing_issue')
            if node=='CodeExec:EducationGate' and (
                    meta.get('education_paper_id')!=task.get('source_id')
                    or meta.get('education_source_sha256')!=parsed_hash):return None
            result.update(dependency='validation',reason=_safe_reason(event.get('reason')))
            if (issue=='quality_decision' and meta.get('education_annotation_status')=='已标注'
                    and event.get('decision')==meta.get('education_admission')
                    and event.get('decision') in ('暂缓','剔除')):
                result.update(kind='quality_decision',outcome='quality_hold' if event['decision']=='暂缓' else 'quality_reject')
            elif issue in {'annotation_invalid','model_error','source_mismatch','input_invalid','major_not_allowed'}:
                result['kind']=issue
                if node=='CodeExec:PrepareEducation' and result['reason'].startswith('预处理专业未能识别为教育专业族：'):
                    result['kind']='major_not_allowed'
            return result
        if 'CodeExec contract mismatch at value:' in line:
            result.update(kind='output_contract',reason='节点输出类型不匹配，完整列表未被正确返回')
        elif node=='Extractor:EducationAnnotation':
            # Do not infer HTTP status from arbitrary provider/body text.
            result.update(dependency='llm',reason='生成模型节点执行失败；未确认具体 HTTP 故障类型')
            marker=re.fullmatch(r'RuntimeError: EDUCATION_LLM_FAILURE\|(.*)',line)
            bindings={'72ff5454b8f711f1a8846d29005ec004':'afb95246b4d311f1b74c89b9f52f3fbe',
                      '2df63160b98911f199ca358d29021dae':'e7e4a8e0b01511f199856dc9cfded896'}
            expected_model=bindings.get(log['pipeline_id'])
            model=dsl.get('components',{}).get(node,{}).get('obj',{}).get('params',{}).get('llm_id')
            if (marker and task.get('dataset_id') in (
                    'ecf88dc4b82b11f1a8846d29005ec004',
                    '3e8ef88cbc4e11f1a8e165c0e34ea629')
                    and expected_model and model==expected_model
                    and task.get('execution_model_id')==expected_model):
                try: event=json.loads(marker.group(1))
                except ValueError: event=None
                if (isinstance(event,dict) and set(event)=={'version','kind','status_code','retry_after'}
                        and type(event['version']) is int and event['version']==1
                        and event['kind'] in {'rate_limit','authentication','arrearage','unavailable','timeout','request_invalid'}
                        and (event['status_code'] is None or type(event['status_code']) is int
                             and 400<=event['status_code']<=599)
                        and type(event['retry_after']) in (int,float) and 1<=event['retry_after']<=3600):
                    result.update(kind=event['kind'],status_code=event['status_code'],
                        retry_after=event['retry_after'],reason='模型请求失败：'+event['kind'])
        elif node=='Tokenizer:EducationIndex':
            result.update(reason='分词或向量索引节点执行失败')
        return result
    return None


PROOF_METADATA = ('education_paper_id', 'education_admission', 'education_source_sha256',
                  'education_method_details', 'education_research_steps')


def _matches_final_source_log(task, remote, log):
    if not isinstance(log, dict): return False
    pipeline = task.get('execution_pipeline_id') or task.get('pipeline_id')
    if (not current_execution(task, remote) or remote.get('run') != 'DONE'
            or remote.get('name') != task.get('remote_name')
            or remote.get('pipeline_id') != pipeline
            or log.get('document_id') != task.get('remote_id')
            or log.get('kb_id') != task.get('dataset_id')
            or log.get('document_name') != task.get('remote_name')
            or log.get('pipeline_id') != pipeline or log.get('task_type') != 'Parse'
            or str(log.get('operation_status')) != '3'
            or log.get('process_begin_at') != remote.get('process_begin_at')):
        return False
    return True


def execution_source_proof(task, remote, log, chunks):
    """Bind source bytes and retrieved chunks to one completed pipeline execution."""
    if not _matches_final_source_log(task, remote, log): return None
    pipeline = task.get('execution_pipeline_id') or task.get('pipeline_id')
    dsl = log.get('dsl')
    if isinstance(dsl, str):
        try: dsl = json.loads(dsl)
        except ValueError: return None
    if not isinstance(dsl, dict) or not log.get('id') or not dsl.get('task_id'): return None
    executed = dsl.get('path') or []
    required = {'TokenChunker:WholeEducation', 'CodeExec:EducationGate', 'Tokenizer:EducationIndex'}
    if not required.issubset(executed) or any(_output(dsl, n, '_ERROR') for n in executed): return None
    original = _output(dsl, 'TokenChunker:WholeEducation', 'chunks')
    if not isinstance(original, list) or len(original) != 1 or not isinstance(original[0], dict): return None
    parsed = original[0].get('text')
    if not isinstance(parsed, str): return None
    try:
        source = Path(task['path']).read_bytes()
        if hashlib.sha256(source).hexdigest() != task.get('content_sha256'): return None
        if not matches_parsed_source(source.decode('utf-8-sig'), parsed): return None
    except (OSError, KeyError, UnicodeError): return None
    parsed_hash = hashlib.sha256(parsed.encode('utf-8')).hexdigest()
    metadata = remote.get('meta_fields') or {}
    if (metadata.get('education_paper_id') != task.get('source_id')
            or metadata.get('education_admission') != '入库'
            or metadata.get('education_source_sha256') != parsed_hash
            or any(metadata.get(k) is None for k in PROOF_METADATA)):
        return None
    gate = _output(dsl, 'CodeExec:EducationGate', 'json')
    tokenized = _output(dsl, 'Tokenizer:EducationIndex', 'chunks')
    if not isinstance(gate, list) or not gate or not isinstance(tokenized, list) or not tokenized: return None
    for rows in (gate, tokenized):
        complete_metadata = False
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get('metadata'), dict): return None
            row_metadata = row['metadata']
            if any(row_metadata.get(k) != metadata[k] for k in PROOF_METADATA[:3]): return None
            if any(k in row_metadata and row_metadata[k] != metadata[k] for k in PROOF_METADATA[3:]): return None
            complete_metadata |= all(k in row_metadata for k in PROOF_METADATA)
        if not complete_metadata: return None
    expected = Counter()
    asset_variants = {}
    for row in tokenized:
        words = row.get('important_kwd')
        if not isinstance(words, list) or not all(isinstance(k, str) for k in words): return None
        if not isinstance(row.get('text'), str) or row.get('available_int') not in (0, 1): return None
        roles = [k for k in words if k.startswith('role=')]
        if len(roles) != 1 or roles[0] not in {'role=body', 'role=index', 'role=assets'}: return None
        if roles[0] == 'role=assets':
            if row['available_int']: return None
            # RAGFlow assigns IDs from text + document ID, so identical material
            # text is stored once. Only accept a keyword variant this run emitted.
            asset_variants.setdefault(row['text'], set()).add(tuple(sorted(words)))
        else:
            expected[(row['text'], tuple(sorted(words)), bool(row['available_int']))] += 1
    if any(text in asset_variants for text, _, _ in expected): return None
    actual = Counter()
    observed_assets = set()
    ids = set()
    for chunk in chunks:
        if not isinstance(chunk, dict): return None
        chunk_id = chunk.get('id')
        if (not isinstance(chunk_id, str) or not chunk_id or chunk_id in ids
                or chunk.get('document_id') != task.get('remote_id')
                or chunk.get('dataset_id', task['dataset_id']) != task['dataset_id']): return None
        words = chunk.get('important_keywords')
        if (not isinstance(words, list) or not all(isinstance(k, str) for k in words)
                or not isinstance(chunk.get('content'), str)
                or not isinstance(chunk.get('available'), bool)): return None
        ids.add(chunk_id)
        if 'role=assets' in words:
            text = chunk['content']
            if (chunk['available'] or text in observed_assets
                    or tuple(sorted(words)) not in asset_variants.get(text, set())): return None
            observed_assets.add(text)
        else:
            actual[(chunk['content'], tuple(sorted(words)), chunk['available'])] += 1
    if actual != expected or observed_assets != set(asset_variants): return None
    return {'log_id': log['id'], 'execution_id': dsl['task_id'], 'pipeline_id': pipeline,
            'process_begin_at': log['process_begin_at'], 'source_sha256': task['content_sha256'],
            'parsed_sha256': parsed_hash, 'chunk_ids': sorted(ids)}


class IngestionLogReader:
    def __init__(self, client_factory, clock=time.monotonic, cache_dir=None):
        self.client_factory=client_factory
        self.clock=clock
        self.cache_dir = Path(cache_dir) if cache_dir is not None else None
        self._lock=RLock()
        self._cache={}
        self._finals=OrderedDict()

    @staticmethod
    def _key(dataset, document, begin):
        return hashlib.sha256(json.dumps([dataset, document, begin]).encode()).hexdigest()

    def remember(self, log):
        """Prime a finalized record once; never scan history during normal polls."""
        if (not isinstance(log, dict) or str(log.get('operation_status')) not in ('3', '4')
                or not all(log.get(k) for k in ('id', 'kb_id', 'document_id', 'process_begin_at'))):
            return False
        try: compact = self._compact(log)
        except (TypeError, ValueError, AttributeError): return False
        if not compact['dsl'].get('task_id'): return False
        key = self._key(log['kb_id'], log['document_id'], log['process_begin_at'])
        with self._lock:
            self._finals[key] = compact
            self._finals.move_to_end(key)
            while len(self._finals) > 64: self._finals.popitem(last=False)
            if self.cache_dir is not None:
                write_json(self.cache_dir / (key + '.json'), compact)
        return True

    def _stored(self, task, remote):
        key = self._key(task['dataset_id'], task['remote_id'], remote.get('process_begin_at'))
        with self._lock:
            if key in self._finals: return self._finals[key]
            if self.cache_dir is not None:
                try: return json.loads((self.cache_dir / (key + '.json')).read_text(encoding='utf-8'))
                except (OSError, ValueError): pass
        return None

    @staticmethod
    def _compact(log):
        result={key:log.get(key) for key in ('id','document_id','document_name','kb_id',
                'pipeline_id','task_type','operation_status','process_begin_at')}
        dsl=log.get('dsl') or {}
        if isinstance(dsl,str):dsl=json.loads(dsl)
        components={}
        for node in dsl.get('path') or []:
            error=_output(dsl,node,'_ERROR')
            if isinstance(error,str) and error.strip():
                components[node]={'obj':{'params':{'outputs':{'_ERROR':{
                    'value':error.rstrip().splitlines()[-1]}}}}}
        original=_output(dsl,'TokenChunker:WholeEducation','chunks')
        components['TokenChunker:WholeEducation']={'obj':{'params':{'outputs':{'chunks':{'value':original}}}}}
        for node, field, keys in (
                ('CodeExec:EducationGate', 'json', ('metadata',)),
                ('Tokenizer:EducationIndex', 'chunks', ('text', 'important_kwd', 'available_int', 'metadata'))):
            rows = _output(dsl, node, field)
            if isinstance(rows, list):
                rows = [{key: row.get(key) for key in keys} if isinstance(row, dict) else row for row in rows]
                for row in rows:
                    if isinstance(row, dict) and isinstance(row.get('metadata'), dict):
                        row['metadata'] = {k: row['metadata'][k] for k in PROOF_METADATA if k in row['metadata']}
            outputs = components.setdefault(node, {'obj': {'params': {'outputs': {}}}})['obj']['params']['outputs']
            outputs[field] = {'value': rows}
        result['dsl']={'path':dsl.get('path'),'task_id':dsl.get('task_id'),'components':components}
        return result

    def _page(self,dataset,page):
        # Filtered and individual-log endpoints time out on this deployment.
        # Share bounded, unfiltered recent pages across concurrent poll workers.
        key=(dataset,page)
        with self._lock:
            cached=self._cache.get(key)
            if cached and self.clock()-cached[0]<5:return cached[1]
            query=urlencode({'log_type':'file','page':page,'page_size':25,
                             'orderby':'create_time','desc':'true'})
            result=self.client_factory().request('GET',f'/datasets/{dataset}/ingestions?{query}')
            rows=result.get('logs')
            if not isinstance(rows,list):raise ValueError('Invalid ingestion-log response')
            compact = []
            for log in rows:
                try: row = self._compact(log)
                except (TypeError, ValueError, AttributeError): continue
                compact.append(row)
                if self.cache_dir is not None: self.remember(row)
            rows = compact
            self._cache[key]=(self.clock(),rows)
            return rows

    def __call__(self,task,remote):
        if task.get('policy_key') in ('research_profile','research_material') and remote.get('run')=='FAIL':
            from ingest_profile_diagnostics import diagnostic
            for page in range(1,4):
                rows=self._page(task['dataset_id'],page)
                for log in rows:
                    result=diagnostic(task,remote,log)
                    if result:return result
                if len(rows)<25:break
            return None
        if task.get('policy_key')!='education' or remote.get('run')!='FAIL':return None
        # A failed final database write can prevent the execution log itself from
        # being stored. Recognize only this exact server exception format, after
        # binding the remote state; never infer a paper decision from free text.
        lines=(remote.get('progress_msg') or '').rstrip().splitlines()
        database_errors={
            "(1213, 'Deadlock found when trying to get lock; try restarting transaction')":
                ('database_deadlock','远端报告数据库事务死锁（MySQL 1213）；需核查服务端写入'),
            'Education execution-log writer is busy':
                ('database_log_busy','教育执行日志写入等待超时；需要核查日志事务'),
            'Education execution-log connection changed before transaction':
                ('database_log_connection_changed','教育执行日志的数据库连接发生变化；已阻止无锁写入'),
        }
        last_error=(re.fullmatch(r'\d{2}:\d{2}:\d{2} \[ERROR\]\[Exception\]: (.+)',lines[-1])
                    if lines else None)
        database_error=database_errors.get(last_error.group(1)) if last_error else None
        if (current_execution(task,remote) and remote.get('name')==task.get('remote_name')
                and database_error):
            return {'source':'remote_execution_status','dependency':'database',
                'kind':database_error[0],'outcome':'technical_review',
                'reason':database_error[1],
                'remote_id':task['remote_id'],'pipeline_id':remote['pipeline_id'],
                'attempt':task.get('execution_attempt',0),'process_begin_at':remote['process_begin_at']}
        if self.cache_dir is not None:
            stored = self._stored(task, remote)
            if stored:
                diagnostic = execution_diagnostic(task, remote, stored)
                if diagnostic: return diagnostic
        for page in range(1,4):
            rows=self._page(task['dataset_id'],page)
            for log in rows:
                diagnostic=execution_diagnostic(task,remote,log)
                if diagnostic:return diagnostic
            if len(rows)<25:break
        return None

    def source_proof(self, task, remote, chunks):
        if (not current_execution(task, remote) or remote.get('run') != 'DONE'
                or remote.get('name') != task.get('remote_name')):
            return None
        stored = self._stored(task, remote)
        found_final = _matches_final_source_log(task, remote, stored)
        if stored:
            proof = execution_source_proof(task, remote, stored, chunks)
            if proof: return proof
        for page in range(1, 4):
            rows = self._page(task['dataset_id'], page)
            for log in rows:
                found_final |= _matches_final_source_log(task, remote, log)
                proof = execution_source_proof(task, remote, log, chunks)
                if proof: return proof
            if len(rows) < 25: break
        if not found_final:
            error = ArtifactEvidencePending('Education artifact source proof is awaiting the current final execution log [no_matching_final]')
            error.artifact_evidence_reason = 'no_matching_final'
            raise error
        return None
