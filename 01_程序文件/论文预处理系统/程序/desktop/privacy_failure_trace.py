"""Private failure diagnostics with only the latest actual visual input retained.

This agent proxy does not alter calls, responses, cache acceptance or retries.
Free text and arbitrary request/response fields are deliberately excluded: the
existing failure trace holds the rejection reason, while this trace explains
the exact input geometry and returned coordinates.
"""
import hashlib
import io
import json
import math
import re

from PIL import Image


_PHASES={'identify','locate_privacy_regions','verify_masked','verify_overbroad_page_mask'}
_COORDINATE_SPACES={'normalized','pixel','percent','0_1000'}
_NUMBER=re.compile(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z')


def _coordinates(value):
    """Keep original finite coordinate values, never arbitrary strings."""
    if not isinstance(value,(list,tuple)) or len(value)!=4:return None
    for number in value:
        if type(number) not in (int,float,str):return None
        if isinstance(number,str) and (len(number)>64 or not _NUMBER.fullmatch(number)):return None
        if not math.isfinite(float(number)):return None
    return list(value)


def _payload_fields(payload):
    out={}
    if not isinstance(payload,dict):return out
    if payload.get('phase') in _PHASES:out['phase']=payload['phase']
    region=_coordinates(payload.get('review_region'))
    if region is not None:out['review_region']=region
    context=payload.get('context_image_indices')
    if isinstance(context,list) and all(type(n) is int and n>0 for n in context):
        out['context_image_indices']=list(context)
    if payload.get('coordinate_space') in _COORDINATE_SPACES:
        out['coordinate_space']=payload['coordinate_space']
    return out


def _response_fields(result):
    out={}
    if not isinstance(result,dict):return out
    for key,allowed in (('decision',{'pass','reject'}),('failure_kind',{'content','privacy'}),
                        ('coordinate_space',_COORDINATE_SPACES)):
        value=result.get(key)
        if isinstance(value,str) and value in allowed:out[key]=value
    if type(result.get('research')) is bool:out['research']=result['research']
    boxes=result.get('sensitive_boxes')
    if isinstance(boxes,list):
        out['sensitive_boxes']=[]
        for box in boxes:
            if isinstance(box,dict):
                clean={}
                coords=_coordinates(box.get('bbox'))
                if coords is not None:clean['bbox']=coords
                elif all(key in box for key in ('x0','y0','x1','y1')):
                    coords=_coordinates([box[key] for key in ('x0','y0','x1','y1')])
                    if coords is not None:clean.update(zip(('x0','y0','x1','y1'),coords))
                if not clean:continue
                if box.get('coordinate_space') in _COORDINATE_SPACES:
                    clean['coordinate_space']=box['coordinate_space']
                out['sensitive_boxes'].append(clean)
            else:
                coords=_coordinates(box)
                if coords is not None:out['sensitive_boxes'].append(coords)
    return out


class PrivacyFailureTrace:
    """Forward an agent and retain a whitelist of per-call visual evidence."""
    def __init__(self,agent):
        object.__setattr__(self,'_agent',agent)
        object.__setattr__(self,'_events',[])
        object.__setattr__(self,'_last_inputs',())

    def __getattr__(self,name):return getattr(self._agent,name)

    def __setattr__(self,name,value):
        # ask() writes processing_metrics to the real agent; cache and billing
        # state must also remain on the original worker.
        if name in {'_agent','_events','_last_inputs'}:object.__setattr__(self,name,value)
        else:setattr(self._agent,name,value)

    def _begin(self,payload,images):
        # Release prior input bytes even if the new diagnostic record fails.
        self._last_inputs=()
        event={'payload':_payload_fields(payload),'inputs':[],'response':{}}
        self._events.append(event)
        self._last_inputs=tuple(images)
        for index,data in enumerate(self._last_inputs,1):
            with Image.open(io.BytesIO(data)) as image:size=list(image.size)
            event['inputs'].append({'index':index,'size':size,'sha256':hashlib.sha256(data).hexdigest()})
        return event

    def call(self,task,payload,images=(),**kwargs):
        event=None
        if task=='privacy_image':
            try:event=self._begin(payload,images)
            except Exception:pass  # Diagnostics cannot prevent the actual call.
        result=self._agent.call(task,payload,images=images,**kwargs)
        if event is not None:
            try:
                event['response']=_response_fields(result)
                event['cache_hit']=bool(getattr(self._agent,'last_call_cache_hit',False))
            except Exception:pass
        return result

    def write_failure(self,directory):
        """Caller owns private-path validation and best-effort I/O handling."""
        if not self._events or not self._last_inputs:return
        last=self._events[-1]
        inputs=[]
        for index,data in enumerate(self._last_inputs,1):
            filename=f'last-input-{index}.png'
            (directory/filename).write_bytes(data)
            inputs.append({**last['inputs'][index-1],'file':filename})
        request={'payload':last['payload'],'inputs':inputs,
                 'coordinate_target_image_index':1,'input_scope':'exact_last_privacy_image_call'}
        records={'last-request.json':request,'last-response.json':last['response'],
                 'calls.json':{'image_retention':'latest_call_only','calls':self._events}}
        for name,record in records.items():
            (directory/name).write_text(json.dumps(record,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
        # A later failure may use fewer inputs than an earlier context review.
        # Remove only this recorder's obsolete numbered input files.
        for previous in directory.glob('last-input-*.png'):
            match=re.fullmatch(r'last-input-(\d+)\.png',previous.name)
            if match and int(match.group(1))>len(self._last_inputs):previous.unlink()
