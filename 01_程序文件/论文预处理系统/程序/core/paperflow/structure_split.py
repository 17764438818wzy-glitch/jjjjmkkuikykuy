"""Pure structure split-plan logic. No app imports, files, network, or counter init.

The caller supplies exact count_many and rendering/cleaning. Planning is outside
the publication path; records() must only consume an already verified plan.
"""
import copy
import hashlib
import json


def compact(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':'))


def sha(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def profile_context(projection,paths,*,article_title,resolve_section):
    """Context for these exact source paths, without sibling inheritance.

    A direct section_ids on a containing object applies to its child fields.
    Descendant references apply only when their values are carried by this
    record, not when a larger parent was considered before being split.
    """
    def source(path):
        value=projection
        for key in path:
            if isinstance(value,dict):valid=isinstance(key,str) and key in value
            elif isinstance(value,list):valid=type(key) is int and 0<=key<len(value)
            else:valid=False
            if not valid:raise SplitHold('profile_context_source_path')
            value=value[key]
        return value
    references={};fields=[]
    def direct(value):
        if isinstance(value,dict) and isinstance(value.get('section_ids'),list):
            for ref in value['section_ids']:references[compact(ref)]=ref
    def descend(value):
        direct(value)
        if isinstance(value,dict):
            for child in value.values():descend(child)
        elif isinstance(value,list):
            for child in value:descend(child)
    for path in paths:
        if not isinstance(path,list) or not path:raise SplitHold('profile_context_source_path')
        if path[:-1] not in fields:fields.append(path[:-1])
        for end in range(len(path)):direct(source(path[:end]))
        descend(source(path))
    ancestors=[];unknown=[]
    for reference in references.values():
        path=resolve_section(reference)
        if path is None:unknown.append(copy.deepcopy(reference))
        elif path not in ancestors:ancestors.append(path)
    result=dict(article_title=article_title,field_paths=fields,ancestor_paths=ancestors)
    if unknown:result['unresolved_section_ids']=unknown
    return result


class SplitHold(ValueError):
    def __init__(self,reason,details=None):
        self.reason=reason;self.details=details or {};super().__init__(reason)


def reassemble(parts):
    envelopes=[json.loads(body) for _,body in parts]
    if not envelopes:raise SplitHold('missing_parts')
    expected=envelopes[0]['projection_sha256'];total=envelopes[0]['part_count']
    if len(parts)!=total or sorted(e['part_index'] for e in envelopes)!=list(range(total)):
        raise SplitHold('part_coverage')
    if any(e['projection_sha256']!=expected or e['part_count']!=total for e in envelopes):
        raise SplitHold('part_identity')
    segments=[s for e in envelopes for s in e['segments']]
    grouped={};ordinary=[]
    for segment in segments:
        if 'fragment' in segment:grouped.setdefault(tuple(segment['path']),[]).append(segment)
        else:ordinary.append(segment)
    for path,pieces in grouped.items():
        pieces.sort(key=lambda s:s['fragment']['utf8_start']);cursor=0;raw=b''
        first=pieces[0]['fragment']
        for piece in pieces:
            f=piece['fragment'];data=piece['value'].encode('utf-8')
            if (f['utf8_start']!=cursor or f['utf8_end']!=cursor+len(data)
                    or f['value_sha256']!=first['value_sha256'] or f['utf8_length']!=first['utf8_length']):
                raise SplitHold('string_coverage')
            raw+=data;cursor+=len(data)
        if cursor!=first['utf8_length'] or hashlib.sha256(raw).hexdigest()!=first['value_sha256']:
            raise SplitHold('string_hash')
        ordinary.append(dict(path=list(path),order=pieces[0]['order'],value=raw.decode('utf-8')))
    root={};seen=set()
    for segment in sorted(ordinary,key=lambda s:s['order']):
        path=segment['path'];key=tuple(path)
        if key in seen:raise SplitHold('duplicate_field')
        seen.add(key);target=root
        for i,component in enumerate(path):
            last=i==len(path)-1
            if isinstance(target,list):
                if type(component) is not int:raise SplitHold('path_type')
                while len(target)<=component:target.append(None)
                if last:target[component]=copy.deepcopy(segment['value'])
                else:
                    if target[component] is None:target[component]=[] if type(path[i+1]) is int else {}
                    target=target[component]
            else:
                if last:target[component]=copy.deepcopy(segment['value'])
                else:
                    if component not in target:target[component]=[] if type(path[i+1]) is int else {}
                    target=target[component]
    if sha(compact(root))!=expected:raise SplitHold('projection_reconstruction_hash')
    return root


def split_projection(kind,projection,*,article_title,tree_nodes,render,count_many,
                     prepare_text=lambda text:text,model_budget=2000,cl_limit=2038,policy_id,binding):
    logical='__'+kind;original=compact(projection);projection_sha=sha(original)
    calls=0
    def measure(records):
        nonlocal calls
        calls+=len(records)
        texts=[prepare_text(render(rid,text)) for rid,text in records]
        values=count_many(texts)
        if len(values)!=len(records) or any(type(v.get(k)) is not int or v[k]<0 for v in values
                for k in ('model_tokens','cl100k_tokens')):raise SplitHold('counter_unverified')
        return values
    def fits(records):
        return all(c['model_tokens']<=model_budget and c['cl100k_tokens']<=cl_limit for c in measure(records))
    before=measure([(logical,original)])[0]
    base=dict(schema='pf-structure-split-plan/1',logical_record=logical,projection_sha256=projection_sha,
              policy_id=policy_id,binding=binding,before=before,model_budget=model_budget,cl_limit=cl_limit)
    if before['model_tokens']<=model_budget and before['cl100k_tokens']<=cl_limit:
        return dict(base,split=False,records=[(logical,original)],counts=[before],reassembly_verified=True,
                    string_fields_fragmented=0,string_fragments=0,complete_subtree_units=0,count_queries=calls)
    # An admitted projection can explicitly lack a title. Keep that absence
    # in every context; validate_plan still binds it to the source projection.
    if article_title is not None and (not isinstance(article_title,str) or not article_title.strip()):
        raise SplitHold('article_title_unverified')
    order={};serial=0
    def index(value,path):
        nonlocal serial
        order[tuple(path)]=serial;serial+=1
        if isinstance(value,dict):
            for k,v in value.items():index(v,[*path,k])
        elif isinstance(value,list):
            for i,v in enumerate(value):index(v,[*path,i])
    index(projection,[])
    # Tree payload values must come from the exact existing index projection.
    # Full source nodes may contain diagnostic/non-indexed fields.
    nodes=projection['nodes'] if kind=='tree' else tree_nodes
    byid={};graph_problem=None
    for i,node in enumerate(nodes):
        node_id=node.get('id')
        if node_id in byid:graph_problem='tree_duplicate_identity'
        byid[node_id]=i
    def ancestors(node_id,include=False):
        path=[];seen=set();current=node_id if include else nodes[byid[node_id]].get('parent_id')
        while current is not None:
            if current in seen:raise SplitHold('tree_parent_cycle')
            if current not in byid:raise SplitHold('tree_parent_unknown',dict(node_id=current))
            seen.add(current);node=nodes[byid[current]]
            if not isinstance(node.get('title'),str):raise SplitHold('tree_title_unverified')
            path.append(node['title']);current=node.get('parent_id')
        return list(reversed(path))
    def context(path,section_ids=(),node_id=None):
        paths=[];unknown=[]
        if node_id is not None:paths.append(ancestors(node_id))
        for sid in section_ids if isinstance(section_ids,list) else []:
            if sid in byid:paths.append(ancestors(sid,include=True))
            else:unknown.append(sid)
        result=dict(article_title=article_title,field_paths=[path[:-1]],ancestor_paths=paths)
        if unknown:result['unresolved_section_ids']=unknown
        return result
    def segment(path,value):return dict(order=order[tuple(path)],path=path,value=value)
    def unit(segments,ctx,subtree=False):return dict(segments=segments,context=ctx,subtree=subtree)
    def merged_context(units):
        result=dict(article_title=article_title,field_paths=[],ancestor_paths=[])
        for u in units:
            for key in ('field_paths','ancestor_paths','unresolved_section_ids'):
                for value in u['context'].get(key,[]):
                    if value not in result.setdefault(key,[]):result[key].append(value)
        return result
    def record(units,i=0,total=1):
        segments=[s for u in units for s in u['segments']]
        def resolve_section(reference):
            try:known=reference in byid
            except TypeError:known=False
            if not known:return None
            if graph_problem:raise SplitHold(graph_problem)
            return ancestors(reference,include=True)
        ctx=(profile_context(projection,[s['path'] for s in segments],article_title=article_title,
                             resolve_section=resolve_section) if kind=='profile' else merged_context(units))
        envelope=dict(schema='pf-structure-parts/1',logical_record=logical,projection_sha256=projection_sha,
            part_index=i,part_count=total,context=ctx,segments=segments)
        return logical+'.p'+str(i).zfill(6),compact(envelope)
    def fit_units(units):return fits([record(units)])
    def expand_value(path,value,ctx):
        candidate=unit([segment(path,value)],ctx)
        if fit_units([candidate]):return [candidate]
        def child_context(child_path,child_value):
            child=copy.deepcopy(ctx);child['field_paths']=[child_path[:-1]]
            if isinstance(child_value,dict) and isinstance(child_value.get('section_ids'),list):
                for sid in child_value['section_ids']:
                    if sid in byid:
                        value=ancestors(sid,include=True)
                        if value not in child['ancestor_paths']:child['ancestor_paths'].append(value)
                    elif sid not in child.setdefault('unresolved_section_ids',[]):child['unresolved_section_ids'].append(sid)
            return child
        if isinstance(value,dict) and value:
            return [u for k,v in value.items() for u in expand_value([*path,k],v,child_context([*path,k],v))]
        if isinstance(value,list) and value:
            return [u for i,v in enumerate(value) for u in expand_value([*path,i],v,child_context([*path,i],v))]
        if not isinstance(value,str) or not value:raise SplitHold('context_or_scalar_exceeds_budget',dict(path=path))
        full=value.encode('utf-8');full_sha=hashlib.sha256(full).hexdigest();pieces=[];start=0;byte_start=0
        while start<len(value):
            low=1;high=len(value)-start;best=0;best_unit=None
            while low<=high:
                take=(low+high)//2;text=value[start:start+take];end=byte_start+len(text.encode('utf-8'))
                s=segment(path,text);s['fragment']=dict(utf8_start=byte_start,utf8_end=end,
                    utf8_length=len(full),value_sha256=full_sha)
                proposed=unit([s],ctx)
                if fit_units([proposed]):best=take;best_unit=proposed;low=take+1
                else:high=take-1
            if not best:raise SplitHold('context_exceeds_budget',dict(path=path,utf8_start=byte_start))
            pieces.append(best_unit);byte_start=best_unit['segments'][0]['fragment']['utf8_end'];start+=best
        return pieces
    subtree_units=0
    if kind=='tree':
        if graph_problem:raise SplitHold(graph_problem)
        children={i:[] for i in range(len(nodes))};roots=[]
        for i,node in enumerate(nodes):
            parent=node.get('parent_id')
            if parent is None:roots.append(i)
            elif parent in byid:children[byid[parent]].append(i)
            else:raise SplitHold('tree_parent_unknown',dict(node_id=node.get('id')))
        visited=set()
        def subtree(i,stack=()):
            if i in stack:raise SplitHold('tree_parent_cycle')
            return [i]+[j for child in children[i] for j in subtree(child,(*stack,i))]
        def emit(i):
            nonlocal subtree_units
            indices=subtree(i);visited.update(indices);ctx=context(['nodes',i],node_id=nodes[i]['id'])
            candidate=unit([segment(['nodes',j],nodes[j]) for j in indices],ctx,True)
            if fit_units([candidate]):subtree_units+=1;return [candidate]
            result=expand_value(['nodes',i],nodes[i],ctx)
            for child in children[i]:result.extend(emit(child))
            return result
        units=[u for i in roots for u in emit(i)]
        if len(visited)!=len(nodes):raise SplitHold('tree_parent_cycle_or_unrooted')
    else:
        units=[u for key,value in projection.items() for u in expand_value([key],value,context([key]))]
    groups=[];current=[]
    for u in units:
        if current and not fit_units([*current,u]):groups.append(current);current=[]
        current.append(u)
    if current:groups.append(current)
    # Resolve final part-index/count overhead with real counting. A changed
    # header is never accepted merely because its provisional form fitted.
    for _ in range(10000):
        records=[record(group,i,len(groups)) for i,group in enumerate(groups)]
        counts=measure(records);bad=next((i for i,c in enumerate(counts)
            if c['model_tokens']>model_budget or c['cl100k_tokens']>cl_limit),None)
        if bad is None:break
        group=groups[bad]
        if len(group)>1:
            middle=len(group)//2;groups[bad:bad+1]=[group[:middle],group[middle:]];continue
        u=group[0]
        if len(u['segments'])>1:
            middle=len(u['segments'])//2
            groups[bad:bad+1]=[[unit(u['segments'][:middle],u['context'])],[unit(u['segments'][middle:],u['context'])]];continue
        s=u['segments'][0];value=s['value'];path=s['path']
        if isinstance(value,str) and len(value)>1:
            if 'fragment' not in s:s['fragment']=dict(utf8_start=0,utf8_end=len(value.encode()),
                utf8_length=len(value.encode()),value_sha256=sha(value))
            middle=len(value)//2;left=copy.deepcopy(s);right=copy.deepcopy(s)
            left['value']=value[:middle];right['value']=value[middle:]
            cut=s['fragment']['utf8_start']+len(left['value'].encode())
            left['fragment']['utf8_end']=cut;right['fragment']['utf8_start']=cut
            groups[bad:bad+1]=[[unit([left],u['context'])],[unit([right],u['context'])]];continue
        if isinstance(value,(dict,list)) and value:
            # This boundary case had fitted the provisional part header.
            children_values=value.items() if isinstance(value,dict) else enumerate(value)
            expanded=[unit([segment([*path,k],v)],u['context']) for k,v in children_values]
            groups[bad:bad+1]=[[child] for child in expanded];continue
        raise SplitHold('final_envelope_exceeds_budget',dict(path=path))
    else:raise SplitHold('part_count_not_stable')
    restored=reassemble(records)
    if compact(restored)!=original:raise SplitHold('projection_bytes_changed')
    fragments=[s for g in groups for u in g for s in u['segments'] if 'fragment' in s]
    return dict(base,split=True,records=records,counts=counts,reassembly_verified=True,
        string_fields_fragmented=len({tuple(s['path']) for s in fragments}),string_fragments=len(fragments),
        complete_subtree_units=subtree_units,count_queries=calls)
