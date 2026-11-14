"""Pure split-plan construction/binding and read-only sealed-plan loading.

A plan fixes a new revision's record mapping. Its provisional counts are not
qualification: actual final manifest prefixes must pass the admission gate.
"""
import copy
import hashlib
import json
from pathlib import Path
import re

from .embedding_admission import AdmissionPolicy,digest
from .structure_split import compact,sha,split_projection,reassemble,SplitHold,profile_context

PLAN_FILE='split-plan.json'
SCHEMA='paperflow-structure-plan/1'


def _require(ok,reason):
    if not ok:raise ValueError('structure plan '+reason)


def _revision(value):
    if isinstance(value,str) and value.startswith('r'):value=value[1:]
    try:return int(value)
    except (TypeError,ValueError):raise ValueError('structure plan revision missing')


def _binding(task,packet):
    manifest=packet.get('manifest',{})
    result={key:task.get('delivery_task_id',task.get(key)) if key=='task_id' else task.get(key)
            for key in ('task_id','source_id','source_sha256')}
    for key in result:_require(isinstance(result[key],str) and bool(result[key]),'identity missing')
    result['revision']=_revision(task.get('revision',manifest.get('revision')))
    if manifest:
        for key in ('task_id','source_id','source_sha256'):
            _require(manifest.get(key)==result[key],'identity mismatch')
        _require(_revision(manifest.get('revision'))==result['revision'],'revision mismatch')
    return result


def _task(packet):
    manifest=packet.get('manifest',{})
    return {key:manifest.get(key) for key in ('task_id','source_id','source_sha256','revision')}


def _article_title(profile):
    """Two explicit source fields; no title inference or profile mutation."""
    for path in (('article','title'),('title_block','article','title')):
        value=profile
        for key in path:
            value=value.get(key) if isinstance(value,dict) else None
        if isinstance(value,str) and value.strip():return value,list(path)
    return None,None


def _projections(packet):
    from .rag import RagPublisher
    return {rid:json.loads(body) for rid,body,_ in RagPublisher(None).raw_records(packet)['body']
            if rid in ('__profile','__tree')}


def _source_value(projection,path):
    _require(isinstance(path,list),'context field path invalid')
    value=projection
    for key in path:
        if isinstance(value,dict):
            _require(isinstance(key,str) and key in value,'context field path unknown')
        elif isinstance(value,list):
            _require(type(key) is int and 0<=key<len(value),'context field index invalid')
        else:raise ValueError('structure plan context field crosses scalar')
        value=value[key]
    return value


class _SourceTree:
    def __init__(self,nodes):
        self.nodes=nodes;self.ids={};self.chains={}
        for index,node in enumerate(nodes):self.ids.setdefault(compact(node.get('id')),[]).append(index)

    def lookup(self,reference):
        matches=self.ids.get(compact(reference),[])
        _require(len(matches)<=1,'context tree identity ambiguous')
        return matches[0] if matches else None

    def chain(self,index):
        if index in self.chains:return self.chains[index]
        seen=[];current=index
        while current is not None:
            _require(current not in seen,'context tree parent cycle')
            seen.append(current);node=self.nodes[current]
            _require(isinstance(node.get('title'),str),'context tree title invalid')
            parent=node.get('parent_id')
            current=self.lookup(parent) if parent is not None else None
            _require(parent is None or current is not None,'context tree parent unknown')
        result=list(reversed(seen));self.chains[index]=result
        return result

    def titles(self,indices):return [self.nodes[index]['title'] for index in indices]


def _validate_context(packet,projection,logical,envelope,tree):
    context=envelope.get('context')
    _require(isinstance(context,dict) and set(context)<=
             {'article_title','field_paths','ancestor_paths','unresolved_section_ids'},'context schema invalid')
    _require(context.get('article_title')==_article_title(packet['profile'])[0],
             'article context mismatch')
    segments=envelope.get('segments')
    _require(isinstance(segments,list) and bool(segments),'context segments missing')
    paths=[]
    for segment in segments:
        _require(isinstance(segment,dict),'context segment invalid')
        path=segment.get('path');_source_value(projection,path)
        _require(bool(path),'context segment path missing');paths.append(path)
    fields=context.get('field_paths');ancestors=context.get('ancestor_paths')
    _require(isinstance(fields,list) and bool(fields) and isinstance(ancestors,list),'context paths missing')
    for field in fields:
        _source_value(projection,field)
        _require(any(len(field)<len(path) and path[:len(field)]==field for path in paths),
                 'context field outside represented segments')
    _require(all(isinstance(path,list) and all(isinstance(title,str) for title in path)
                 for path in ancestors),'context ancestor path invalid')
    unknown=context.get('unresolved_section_ids',[])
    _require(isinstance(unknown,list),'context unresolved references invalid')
    if logical=='__tree':
        _require(not unknown,'context tree has unresolved profile references')
        represented=set()
        for path in paths:
            _require(len(path)>=2 and path[0]=='nodes' and type(path[1]) is int,'context tree node path invalid')
            represented.add(path[1])
        chains={index:tree.chain(index) for index in represented}
        parent_paths=[tree.titles(chain[:-1]) for chain in chains.values()]
        _require(all(any(parent[:len(path)]==path for parent in parent_paths) for path in ancestors),
                 'context ancestor outside represented tree branches')
        # Whole subtrees already contain internal node titles. Their external
        # ancestors still need the complete chain for each represented root.
        for chain in chains.values():
            if not represented.intersection(chain[:-1]):
                _require(tree.titles(chain[:-1]) in ancestors,'context external ancestors missing')
    else:
        def resolve_section(reference):
            index=tree.lookup(reference)
            return None if index is None else tree.titles(tree.chain(index))
        expected=profile_context(projection,paths,article_title=context['article_title'],resolve_section=resolve_section)
        for key in ('field_paths','ancestor_paths','unresolved_section_ids'):
            _require({compact(value) for value in context.get(key,[])}==
                     {compact(value) for value in expected.get(key,[])},'context source references incomplete or invented')


def build_plan(task,packet,policy,counter,*,render=None):
    """Build in memory with an injected counter; caller owns sealing/retries.

    packet.manifest_sha256 may be an explicit prepacking placeholder. This
    function never grants admission, including when passed a real manifest.
    """
    from .rag import RagPublisher
    _require(isinstance(policy,AdmissionPolicy),'verified policy required')
    expected={k:getattr(policy,k) for k in ('model_sha256','libllama_sha256','cl100k_sha256')}
    _require(counter is not None and getattr(counter,'identity',None)==expected,'counter identity mismatch')
    binding=_binding(task,packet);publisher=RagPublisher(None)
    render=render or (lambda rid,text:publisher.render(task,packet,(rid,text,None)))
    projections=_projections(packet);structures={}
    article_title,title_path=_article_title(packet['profile'])
    clean=lambda text:re.sub(r'</?(table|td|caption|tr|th)( [^<>]{0,12})?>',' ',text).strip()
    for logical,projection in projections.items():
        structures[logical]=split_projection(logical[2:],projection,
            article_title=article_title,tree_nodes=packet['tree']['nodes'],
            render=render,count_many=counter.count_many,prepare_text=clean,
            model_budget=policy.model_budget,cl_limit=policy.pipeline_limit,
            policy_id=policy.fingerprint,binding=binding)
    plan=dict(schema=SCHEMA,binding=binding,policy_id=policy.fingerprint,
        renderer_fingerprint=policy.renderer_fingerprint,planning_manifest_sha256=packet.get('manifest_sha256'),
        qualification=False,structures=structures,article_title_source_path=title_path)
    for structure in structures.values():
        structure['records']=[list(record) for record in structure['records']]
        structure['record_hashes']={rid:sha(body) for rid,body in structure['records']}
    plan['plan_sha256']=digest(plan)
    return validate_plan(task,packet,plan,policy=policy)


def validate_plan(task,packet,plan,*,policy=None):
    """Validate mapping integrity without tokenization, IO, or qualification."""
    _require(isinstance(plan,dict) and plan.get('schema')==SCHEMA,'schema invalid')
    _require(plan.get('qualification') is False,'cannot carry qualification')
    payload={k:v for k,v in plan.items() if k!='plan_sha256'}
    _require(plan.get('plan_sha256')==digest(payload),'hash mismatch')
    _require(plan.get('binding')==_binding(task,packet),'binding mismatch')
    if 'article_title_source_path' in plan:
        _require(plan['article_title_source_path']==_article_title(packet['profile'])[1],
                 'article context source changed')
    _require(isinstance(plan.get('policy_id'),str) and bool(plan['policy_id']),'policy missing')
    if policy is not None:
        _require(isinstance(policy,AdmissionPolicy) and plan['policy_id']==policy.fingerprint
                 and plan.get('renderer_fingerprint')==policy.renderer_fingerprint,'policy mismatch')
    projections=_projections(packet);structures=plan.get('structures')
    _require(isinstance(structures,dict) and set(structures)==set(projections),'logical coverage mismatch')
    all_ids=set();original_ids={r['id'] for r in packet['chunks']}|{'asset_'+a['id'] for a in packet['assets']}
    _require(not any(rid.startswith(('__profile.p','__tree.p')) for rid in original_ids),'reserved child namespace')
    tree=_SourceTree(packet['tree']['nodes'])
    for logical,projection in projections.items():
        structure=structures[logical];records=structure.get('records',[])
        _require(structure.get('logical_record')==logical and structure.get('policy_id')==plan['policy_id']
                 and structure.get('binding')==plan['binding'],'logical binding mismatch')
        _require(structure.get('projection_sha256')==sha(compact(projection)),'source projection changed')
        _require(isinstance(records,list) and bool(records),'records missing')
        hashes={}
        for index,record in enumerate(records):
            _require(isinstance(record,(list,tuple)) and len(record)==2,'record invalid')
            rid,body=record
            expected_id=logical+'.p'+str(index).zfill(6) if structure.get('split') is True else logical
            _require(rid==expected_id and isinstance(body,str) and rid not in all_ids|original_ids,'record identity collision')
            all_ids.add(rid);hashes[rid]=sha(body)
            if structure.get('split') is True:
                envelope=json.loads(body)
                _require(envelope.get('schema')=='pf-structure-parts/1'
                    and envelope.get('logical_record')==logical,'envelope identity mismatch')
                _validate_context(packet,projection,logical,envelope,tree)
        _require(hashes==structure.get('record_hashes'),'record hash mismatch')
        if structure.get('split') is True:
            _require(compact(reassemble(records))==compact(projection),'reassembly mismatch')
        else:_require(len(records)==1 and records[0][1]==compact(projection),'unsplit mapping mismatch')
    return plan


def attach_plan(task,packet,plan,*,policy=None):
    validate_plan(task,packet,plan,policy=policy)
    result=copy.deepcopy(packet);result['structure_plan']=copy.deepcopy(plan)
    return result


def plan_records(packet):
    plan=packet.get('structure_plan')
    if plan is None:return None
    validate_plan(_task(packet),packet,plan)
    return {logical:[(rid,body,None) for rid,body in structure['records']]
            for logical,structure in plan['structures'].items()}


def load_sealed_plan(task,packet):
    """Only a manifest-listed, byte-verified plan can alter a sealed mapping."""
    root=Path(packet['root']);path=root/PLAN_FILE;entries=packet['manifest'].get('files',[])
    entries=[e for e in entries if isinstance(e,dict) and e.get('path')==PLAN_FILE]
    if not path.exists() and not entries:return packet
    _require(len(entries)==1 and path.is_file() and not path.is_symlink(),'file not sealed')
    data=path.read_bytes()
    _require(hashlib.sha256(data).hexdigest()==entries[0].get('sha256'),'sealed file hash mismatch')
    if entries[0].get('bytes') is not None:_require(len(data)==entries[0]['bytes'],'sealed file size mismatch')
    return attach_plan(task,packet,json.loads(data.decode('utf-8-sig')))


def clone_new_revision(oldpacket,new_revision,*,expected_targets=None):
    """Describe an explicit new draft; no files, ledger or permits are changed."""
    from .handoff import _expectations
    manifest=oldpacket['manifest'];old_task=_task(oldpacket)
    identity=_binding(old_task,oldpacket);previous=identity.pop('revision')
    _require(type(new_revision) is int and new_revision>previous,'later integer revision required')
    _require(expected_targets is not None,'explicit expected targets required')
    codes={route['code'] for route in oldpacket.get('routes',[])}
    codes.update(route['code'] for route in oldpacket.get('pending_routes',[]) if route.get('code'))
    expectations=_expectations(dict(targets=[dict(code=code) for code in sorted(codes)]),expected_targets)
    # New authority/identity must come from the old ledger's normal handoff.
    # Do not copy old seals, remote receipts, count evidence or source paths as
    # the draft destination. The caller chooses and verifies a NEW local root.
    packet={key:copy.deepcopy(oldpacket[key]) for key in
            ('profile','tree','assets','chunks','routes','pending_routes','compatibility_notes') if key in oldpacket}
    packet.update(manifest=dict(identity,schema='paperflow/1.0',revision=f'r{new_revision:03d}'),
                  manifest_sha256='0'*64)
    publication=set(manifest.get('publication_files',[]))
    files=[copy.deepcopy(entry) for entry in manifest.get('files',[]) if isinstance(entry,dict)
           and entry.get('path') in publication and entry['path'] not in (PLAN_FILE,'manifest.json','READY.json')]
    _require(bool(files),'copy source files missing')
    return dict(schema='paperflow-structure-revision-draft/1',source_root=oldpacket['root'],
        source_manifest_sha256=oldpacket['manifest_sha256'],source_revision=previous,
        task=dict(identity,revision=new_revision),packet=packet,copy_files=files,
        expected_targets=copy.deepcopy(expectations),requires_materialization=True)
