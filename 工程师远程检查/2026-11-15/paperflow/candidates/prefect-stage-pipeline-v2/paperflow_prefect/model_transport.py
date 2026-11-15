"""Owned cancellable HTTP, bounded off-loop evidence writes, native journals."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import time
import httpx
from .pipeline_contracts import ArtifactRef
from .pipeline_runtime import current_context, artifact, verify_ref
from .pipeline_ledger import EvidenceChanged, OutcomeUnknown
from .records import write_json, read_json, sha_file


async def disk(function,*args):
    """Drain a bounded local write on cancellation before releasing file ownership."""
    task=asyncio.create_task(asyncio.to_thread(function,*args))
    try: return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try: await asyncio.shield(task)
            except asyncio.CancelledError: continue
        task.result()
        raise


def canonical(body): return json.dumps(body,ensure_ascii=False,separators=(',',':')).encode()


def _save_request(body,directory,envelope,connection):
    directory=Path(directory).resolve(); directory.mkdir(parents=True,exist_ok=True)
    planned=directory/'pipeline-planned-request.json'
    if planned.exists():
        if read_json(planned)!=body: raise EvidenceChanged('planned request changed')
    else: write_json(planned,body)
    descriptor=directory/'pipeline-request-ref.json'
    value=dict(schema='pipeline-request/1',operation_key=envelope.operation_key,
               request=artifact(planned).to_dict(),request_sha256=hashlib.sha256(canonical(body)).hexdigest(),
               directory=str(directory),endpoint=connection['base_url'],model=connection['model'])
    if descriptor.exists() and read_json(descriptor)!=value: raise EvidenceChanged('request binding changed')
    if not descriptor.exists(): write_json(descriptor,value)
    return artifact(descriptor)


async def save_request(body,directory,envelope):
    ctx=current_context(envelope)
    return await disk(_save_request,body,directory,envelope,ctx.connection)


class StreamFailure(RuntimeError): pass


class Stream:
    def __init__(self):
        self.pending=bytearray(); self.finish=None; self.usage=None; self.done=False
        self.output=bytearray(); self.error=None
    def feed(self,chunk,*,eof=False):
        self.pending.extend(chunk)
        if len(self.pending)>8*1024*1024: raise StreamFailure('SSE event exceeds bounded parser buffer')
        while b'\n' in self.pending or (eof and self.pending):
            if b'\n' in self.pending:
                line,_,remaining=self.pending.partition(b'\n'); self.pending=bytearray(remaining)
            else: line=bytes(self.pending); self.pending.clear()
            if not line.startswith(b'data:'): continue
            raw=line[5:].strip()
            if raw==b'[DONE]': self.done=True; continue
            if not raw: continue
            value=json.loads(raw)
            if value.get('error'):
                self.error=value['error']; raise StreamFailure('explicit stream error')
            if value.get('usage'): self.usage=value['usage']
            for choice in value.get('choices',[]):
                content=(choice.get('delta') or choice.get('message') or {}).get('content')
                if content: self.output.extend(content.encode('utf8'))
                self.finish=choice.get('finish_reason') or self.finish


class EvidenceWriter:
    def __init__(self,directory):
        self.directory=directory
        self.raw=(directory/'response-events.jsonl').open('xb')
        self.text=None
    def headers(self,status):
        if status>=400:
            self.raw.close()
            self.raw=(self.directory/'http-error.txt').open('xb')
        else: self.text=(self.directory/'response.txt').open('xb')
    def append(self,raw,text):
        self.raw.write(raw); self.raw.flush()
        if self.text is not None:
            self.text.write(text); self.text.flush()
    def close(self):
        for stream in (self.raw,self.text):
            if stream is not None and not stream.closed:
                stream.flush(); os.fsync(stream.fileno()); stream.close()


async def send_model(request_ref: ArtifactRef,envelope) -> ArtifactRef:
    ctx=current_context(envelope)
    claim=ctx.ledger._claims().get(envelope.operation_key)
    if not claim or claim[0]!=envelope: raise EvidenceChanged('transport requires the owning ledger claim')
    if envelope.dispatch_kind.value=='recovery':
        if ctx.recovery_authorizer is None: raise OutcomeUnknown('recovery policy must authorize original bytes')
        await ctx.recovery_authorizer(request_ref,envelope)
    await disk(verify_ref,request_ref)
    descriptor=await disk(read_json,request_ref.path)
    if descriptor['operation_key']!=envelope.operation_key: raise EvidenceChanged('request operation mismatch')
    body_ref=ArtifactRef.from_dict(descriptor['request']); await disk(verify_ref,body_ref)
    body=await disk(read_json,body_ref.path); encoded=canonical(body)
    if hashlib.sha256(encoded).hexdigest()!=descriptor['request_sha256']: raise EvidenceChanged('request bytes mismatch')
    connection=ctx.connection
    if descriptor['endpoint']!=connection['base_url'] or descriptor['model']!=connection['model']:
        raise EvidenceChanged('model configuration changed')
    directory=Path(descriptor['directory']); state_path=directory/'request-state.json'
    if state_path.exists() or any((directory/n).exists() for n in ('request.json','response.txt','response-events.jsonl','http-error.txt')):
        raise OutcomeUnknown('saved model evidence must be parsed or reconciled, never overwritten')
    state=None; writer=None; raw_buffer=bytearray(); stream=Stream(); status=None
    async def flush():
        if writer and (raw_buffer or stream.output):
            raw,text=bytes(raw_buffer),bytes(stream.output); raw_buffer.clear(); stream.output.clear()
            await disk(writer.append,raw,text)
    async with ctx.scope('body-http' if envelope.stage.value=='profile' else 'attachment-http'):
        async with ctx.scope('model_start'):
            await disk(ctx.fence)
            # This synchronous short ledger transaction must remain in the owning ContextVar context.
            reservation=ctx.ledger.reserve_send(envelope.paper_id,envelope.item_id,envelope.part_id,
                descriptor['request_sha256'],envelope.dispatch_kind.value=='recovery')
            if reservation['operation_key']!=envelope.operation_key or reservation['owner']!=claim[1]:
                raise EvidenceChanged('reservation is not bound to this task')
            await disk(write_json,directory/'request.json',body)
            state=dict(status='request_in_flight',request_sha256=descriptor['request_sha256'],
                stored_request_sha256=await disk(sha_file,directory/'request.json'),model=body['model'],
                started_at=time.time(),http_invoked=None,client_http_requests=0,request_phase='dispatch_pending',
                reservation_id=reservation['reservation_id'],operation_key=envelope.operation_key)
            await disk(write_json,state_path,state)
        try:
            await disk(ctx.fence)
            def open_writer():
                nonlocal writer
                writer=EvidenceWriter(directory)
            await disk(open_writer)
            endpoint=connection['base_url'].rstrip('/')
            if not endpoint.endswith('/chat/completions'): endpoint+='/chat/completions'
            state.update(http_invoked=True,client_http_requests=1,http_started_at=time.time(),request_phase='transport_called')
            await disk(write_json,state_path,state)
            # AsyncClient and stream are owned directly by this task. No HTTP is run in a thread.
            async with asyncio.timeout(3600):
                # TLS context construction may read the OS certificate store.
                client=await disk(lambda: httpx.AsyncClient(timeout=httpx.Timeout(3600,connect=30),follow_redirects=False,trust_env=False))
                async with client:
                    async with client.stream('POST',endpoint,content=encoded,headers={
                            'Content-Type':'application/json','Accept-Encoding':'identity',
                            'Authorization':'Bearer '+connection['api_key']}) as response:
                        status=response.status_code
                        await disk(writer.headers,status)
                        state.update(http_status=status,response_headers_at=time.time(),request_phase='streaming')
                        await disk(write_json,state_path,state)
                        sse='text/event-stream' in response.headers.get('content-type','')
                        plain=bytearray(); last_flush=time.monotonic()
                        iterator=response.aiter_raw().__aiter__()
                        pending=asyncio.create_task(anext(iterator,None))
                        try:
                            while True:
                                done,_=await asyncio.wait({pending},timeout=.25)
                                if not done:
                                    await flush(); last_flush=time.monotonic(); continue
                                chunk=pending.result()
                                if chunk is None: break
                                raw_buffer.extend(chunk)
                                if sse and status<400: stream.feed(chunk)
                                else:
                                    plain.extend(chunk)
                                    if len(plain)>64*1024*1024: raise StreamFailure('nonstream body exceeds limit')
                                if len(raw_buffer)>=65536 or len(stream.output)>=65536 or time.monotonic()-last_flush>=.25:
                                    await flush(); last_flush=time.monotonic()
                                pending=asyncio.create_task(anext(iterator,None))
                            if status>=400:
                                state['error_response_sha256']=hashlib.sha256(plain).hexdigest()
                                raise StreamFailure('HTTP failure')
                            if sse: stream.feed(b'',eof=True)
                            else:
                                value=json.loads(plain)
                                if value.get('error'): stream.error=value['error']; raise StreamFailure('explicit model error')
                                choice=value['choices'][0]; stream.output.extend((choice['message'].get('content') or '').encode())
                                stream.finish=choice.get('finish_reason'); stream.usage=value.get('usage'); stream.done=True
                        finally:
                            if not pending.done(): pending.cancel()
                            await asyncio.gather(pending,return_exceptions=True)
            await flush(); await disk(writer.close); writer=None
            state.update(finish_reason=stream.finish,usage=stream.usage,done_received=stream.done,
                response_sha256=await disk(sha_file,directory/'response.txt'),
                events_sha256=await disk(sha_file,directory/'response-events.jsonl'),finished_at=time.time())
            complete=stream.finish=='stop' and stream.done
            state.update(status='response_complete' if complete else 'partial_response',
                         request_phase='response_complete' if complete else 'partial_response')
            await disk(write_json,state_path,state)
            if not complete: raise StreamFailure('truncated or incomplete response')
            return await disk(artifact,state_path)
        except BaseException as exc:
            await flush()
            if writer: await disk(writer.close); writer=None
            state.update(status='partial_response' if state.get('status')=='partial_response' else 'request_failed_or_uncertain',
                request_phase='failed_after_http' if state.get('http_invoked') else 'failed_before_http',
                error_type=type(exc).__name__,finished_at=time.time(),finish_reason=stream.finish,
                usage=stream.usage,done_received=stream.done)
            if stream.error: state.update(error_kind='explicit_stream_error',model_error_code=stream.error.get('code') if isinstance(stream.error,dict) else None)
            if (directory/'response.txt').exists(): state['response_sha256']=await disk(sha_file,directory/'response.txt')
            await disk(write_json,state_path,state)
            raise


def _write_bytes(path,data):
    with Path(path).open('xb') as handle:
        handle.write(data); handle.flush(); os.fsync(handle.fileno())
