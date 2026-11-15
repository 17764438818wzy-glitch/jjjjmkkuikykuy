"""Native learning tree-reference adapter; no I/O except read-only seal checks.

Only the two marked native function bodies are lifted. No model, request,
business transport, counter initialization or alternate RAG writer exists here.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import copy
import hashlib
import importlib
import json

POLICY = 'learning-unresolved-parent/1'
_EXPECTED_MODULE_SHA = {'structure_split': '4776a5fe95aab75e579a8377b94657eb514b22d2de2dcf50e00384320d2843f1', 'structure_plan': '99aabb8a3a2484b1edfed14111604fe7ebae45077b7bd31da097ec2c1693fe98', 'package': '158b537307af8e93ec9072ad209efbe743360d5a3a83776ded22ab0848f9486b', 'rag': 'e2fca17dad122917c7fa74cdbcecfc0b9c8d4e0dd2049f90be0063107aee6d0e'}
_current = ContextVar('paperflow_tree_reference_scope', default=None)
_public_depth = ContextVar('paperflow_tree_public_depth', default=0)
_installed_root = None


def _index_tree(nodes):
    packet = dict(profile={}, tree=dict(nodes=nodes), chunks=[], assets=[])
    return json.loads(next(text for rid, text, _ in _rag.RagPublisher(None).raw_records(packet)['body'] if rid == '__tree'))


class _Relations:
    def __init__(self, nodes):
        self.nodes = nodes
        identities = {compact(node.get('id')) for node in nodes}
        self.links = [dict(node_index=i, node_id=node.get('id'), parent_id=node['parent_id'],
                           target_pointer='/tree/nodes/' + str(i) + '/parent_id')
                      for i, node in enumerate(nodes)
                      if isinstance(node.get('parent_id'), str) and compact(node['parent_id']) not in identities]
        self.tree = _plan._SourceTree(nodes)
        self.paths = {}; self.missing = {}
        if not self.links:
            return
        # Native lookup/chain still decides duplicate IDs, cycles and title
        # types. Only its exact absent-parent result can become unresolved.
        for node in nodes:
            self.tree.lookup(node.get('id'))
        for index in range(len(nodes)):
            try:
                self.paths[index] = self.tree.chain(index)
                self.missing[index] = []
            except ValueError as exc:
                if str(exc) != 'structure plan context tree parent unknown':
                    raise
                current = index
                while True:
                    parent = nodes[current].get('parent_id')
                    target = self.tree.lookup(parent) if parent is not None else None
                    if target is None:
                        if not isinstance(parent, str):
                            raise exc
                        self.paths[index] = None
                        self.missing[index] = [parent]
                        break
                    current = target

    def index(self, reference):
        return self.tree.lookup(reference)

    def ancestors(self, reference, include=False):
        index = self.index(reference)
        if index is None:
            return None
        chain = self.paths.get(index)
        if chain is None:
            return None
        return self.tree.titles(chain if include else chain[:-1])

    def unknown(self, reference):
        index = self.index(reference)
        return [reference] if index is None else self.missing[index]

    def expected_tree_context(self, represented):
        ancestors = []; unknown = []
        for index in sorted(represented):
            chain = self.paths[index]
            if chain is None:
                for reference in self.missing[index]:
                    if reference not in unknown:
                        unknown.append(reference)
            elif not represented.intersection(chain[:-1]):
                path = self.tree.titles(chain[:-1])
                if path not in ancestors:
                    ancestors.append(path)
        return ancestors, unknown

    def policy(self):
        return dict(schema=POLICY, admission_policy='learning',
                    tree_projection_sha256=sha(compact(_index_tree(self.nodes))),
                    unresolved_parents=copy.deepcopy(self.links))


@contextmanager
def publication_scope(candidate, bundle, validation, admission_policy):
    if admission_policy != 'learning':
        token = _current.set(None)
        try:
            yield
        finally:
            _current.reset(token)
        return
    if (not isinstance(validation, dict) or validation.get('valid') is not True
            or validation.get('candidate') != candidate
            or validation.get('admission', {}).get('policy') != 'learning'
            or validation.get('admission', {}).get('redaction_policy') != 'best_effort'
            or any(candidate.get('source', {}).get(key) != bundle.get('source', {}).get(key)
                   for key in ('source_id', 'source_sha256'))):
        raise ValueError('tree reference scope requires native learning validation and source binding')
    relations = _Relations(candidate['tree']['nodes'])
    token = _current.set(dict(origin='publication', relations=relations))
    try:
        yield
    finally:
        _current.reset(token)


def _learning_split(kind,projection,*,article_title,tree_nodes,render,count_many,
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
    relations=_Relations(nodes)
    def ancestors(node_id,include=False):
        return relations.ancestors(node_id,include)
    def context(path,section_ids=(),node_id=None):
        paths=[];unknown=[]
        if node_id is not None:
            value=ancestors(node_id)
            if value is None:unknown.extend(relations.unknown(node_id))
            else:paths.append(value)
        for sid in section_ids if isinstance(section_ids,list) else []:
            if sid in byid:
                value=ancestors(sid,include=True)
                if value is None:unknown.append(sid)
                else:paths.append(value)
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
    referenced_contexts=set();context_values={}
    def resolved_context(units):
        segments=[s for u in units for s in u['segments']]
        def resolve_section(reference):
            try:known=reference in byid
            except TypeError:known=False
            if not known:return None
            if graph_problem:raise SplitHold(graph_problem)
            return ancestors(reference,include=True)
        return (profile_context(projection,[s['path'] for s in segments],article_title=article_title,
                             resolve_section=resolve_section) if kind=='profile' else merged_context(units))
    def reference_context(units):
        ctx=resolved_context(units);key=sha(compact(ctx))
        if key in referenced_contexts:return False
        referenced_contexts.add(key);context_values[key]=ctx
        return True
    def record(units,i=0,total=1):
        segments=[s for u in units for s in u['segments']]
        ctx=resolved_context(units);key=sha(compact(ctx))
        if key in referenced_contexts:ctx=dict(source_context_sha256=key)
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
                        if value is None:
                            if sid not in child.setdefault('unresolved_section_ids',[]):child['unresolved_section_ids'].append(sid)
                        elif value not in child['ancestor_paths']:child['ancestor_paths'].append(value)
                    elif sid not in child.setdefault('unresolved_section_ids',[]):child['unresolved_section_ids'].append(sid)
            return child
        if isinstance(value,dict) and value:
            return [u for k,v in value.items() for u in expand_value([*path,k],v,child_context([*path,k],v))]
        if isinstance(value,list) and value:
            return [u for i,v in enumerate(value) for u in expand_value([*path,i],v,child_context([*path,i],v))]
        if not isinstance(value,str) or not value:
            if reference_context([candidate]) and fit_units([candidate]):return [candidate]
            raise SplitHold('context_or_scalar_exceeds_budget',dict(path=path))
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
            if not best:
                # Preserve full source context in the sealed plan. Only its
                # content-addressed reference repeats on an otherwise
                # impossible fragment; validators resolve and recheck it.
                if reference_context([proposed]):continue
                raise SplitHold('context_exceeds_budget',dict(path=path,utf8_start=byte_start))
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
            else:roots.append(i)  # Emission seed with unresolved external parent, never a semantic root.
        visited=set()
        def subtree(i,stack=()):
            if i in stack:raise SplitHold('tree_parent_cycle')
            return [i]+[j for child in children[i] for j in subtree(child,(*stack,i))]
        def emit(i):
            nonlocal subtree_units
            indices=subtree(i);visited.update(indices);ctx=context(['nodes',i],node_id=nodes[i]['id'])
            candidate=unit([segment(['nodes',j],nodes[j]) for j in indices],ctx,not relations.unknown(nodes[i]['id']))
            if fit_units([candidate]):subtree_units+=int(candidate['subtree']);return [candidate]
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
        if reference_context(group):continue
        raise SplitHold('final_envelope_exceeds_budget',dict(path=path))
    else:raise SplitHold('part_count_not_stable')
    restored=reassemble(records)
    if compact(restored)!=original:raise SplitHold('projection_bytes_changed')
    fragments=[s for g in groups for u in g for s in u['segments'] if 'fragment' in s]
    used={json.loads(body)['context']['source_context_sha256'] for _,body in records
          if 'source_context_sha256' in json.loads(body)['context']}
    return dict(base,split=True,records=records,counts=counts,reassembly_verified=True,
        contexts={key:context_values[key] for key in sorted(used)},
        string_fields_fragmented=len({tuple(s['path']) for s in fragments}),string_fragments=len(fragments),
        complete_subtree_units=subtree_units,count_queries=calls)


def _learning_validate_context(packet,projection,logical,envelope,tree,contexts=None):
    context=envelope.get('context')
    if isinstance(context,dict) and set(context)=={'source_context_sha256'}:
        key=context['source_context_sha256']
        _require(isinstance(key,str) and isinstance(contexts,dict) and key in contexts,
                 'source context reference missing')
        context=contexts[key]
        _require(isinstance(context,dict) and sha(compact(context))==key,'source context reference hash mismatch')
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
        represented=set()
        for path in paths:
            _require(len(path)>=2 and path[0]=='nodes' and type(path[1]) is int,'context tree node path invalid')
            represented.add(path[1])
        relations=_current.get()['relations']
        chains={index:relations.paths[index] for index in represented if relations.paths[index] is not None}
        parent_paths=[tree.titles(chain[:-1]) for chain in chains.values()]
        _require(all(any(parent[:len(path)]==path for parent in parent_paths) for path in ancestors),
                 'context ancestor outside represented tree branches')
        for chain in chains.values():
            if not represented.intersection(chain[:-1]):
                _require(tree.titles(chain[:-1]) in ancestors,'context external ancestors missing')
        expected_unknown=relations.expected_tree_context(represented)[1]
        _require({compact(v) for v in unknown}=={compact(v) for v in expected_unknown},
                 'context source references incomplete or invented')
    else:
        def resolve_section(reference):
            index=tree.lookup(reference)
            return None if index is None else _current.get()['relations'].ancestors(reference,include=True)
        expected=profile_context(projection,paths,article_title=context['article_title'],resolve_section=resolve_section)
        for key in ('field_paths','ancestor_paths','unresolved_section_ids'):
            _require({compact(value) for value in context.get(key,[])}==
                     {compact(value) for value in expected.get(key,[])},'context source references incomplete or invented')


def _split_projection(kind, projection, **kwargs):
    scope = _current.get()
    if not scope:
        return _original_split(kind, projection, **kwargs)
    nodes = projection['nodes'] if kind == 'tree' else kwargs['tree_nodes']
    relations = _Relations(nodes)
    if not relations.links:
        return _original_split(kind, projection, **kwargs)
    result = _learning_split(kind, projection, **kwargs)
    if kind == 'tree':
        result['reference_policy'] = relations.policy()
    return result


def _validated_relations(packet, plan):
    structures = plan.get('structures') if isinstance(plan, dict) else None
    tree_structure = structures.get('__tree') if isinstance(structures, dict) else None
    flag = tree_structure.get('reference_policy') if isinstance(tree_structure, dict) else None
    if flag is None:
        return None
    if not isinstance(flag, dict) or flag.get('schema') != POLICY or flag.get('admission_policy') != 'learning':
        raise ValueError('tree reference policy unknown')
    relations = _Relations(packet['tree']['nodes'])
    if not relations.links or flag != relations.policy():
        raise ValueError('tree reference policy does not match source projection')
    return relations


def _validate_plan(task, packet, plan, *, policy=None):
    relations = _validated_relations(packet, plan)
    if relations is None:
        return _original_validate_plan(task, packet, plan, policy=policy)
    token = _current.set(dict(origin='plan', relations=relations))
    try:
        return _original_validate_plan(task, packet, plan, policy=policy)
    finally:
        _current.reset(token)


def _validate_context(packet, projection, logical, envelope, tree, contexts=None):
    scope = _current.get()
    if scope and scope['relations'].links:
        return _learning_validate_context(packet, projection, logical, envelope, tree, contexts)
    return _original_validate_context(packet, projection, logical, envelope, tree, contexts)


def _compatible_tree(tree):
    scope = _current.get()
    if not scope or not scope['relations'].links:
        return _original_compatible_tree(tree)
    relations = _Relations(tree.get('nodes', []))
    if relations.policy() != scope['relations'].policy():
        raise ValueError('tree reference scope projection changed')
    return copy.deepcopy(tree)


def _public_gap(relations, link):
    diagnostic = dict(schema='paperflow-tree-relation/1', status='unresolved_parent',
        node_id=link['node_id'], parent_id=link['parent_id'], reference_policy=POLICY,
        tree_projection_sha256=relations.policy()['tree_projection_sha256'])
    return dict(id='program_tree_relation_' + sha(compact(diagnostic))[:20],
        target_pointer=link['target_pointer'], type='uncertain',
        description='程序保留的未解析树关系', impact=None,
        use_limit='父关系未解析；保留原值，不作为已核实章节层级或根节点。',
        needed_information=[], source_refs=[], claim_status='uncertain', program_diagnostic=diagnostic)


def _validate_package(root, task, catalog, *, strict_seal=False):
    # All original READY/manifest/file/binding checks run before reading policy.
    manifest, _, listed, _ = _package.verify_delivery_identity(root, task, strict_seal=strict_seal)
    if 'split-plan.json' not in listed:
        return _original_validate_package(root, task, catalog, strict_seal=strict_seal)
    plan = _package.load(Path(root) / 'split-plan.json')
    packet = dict(tree=_package.load(Path(root) / 'tree.json'))
    relations = _validated_relations(packet, plan)
    if relations is None:
        return _original_validate_package(root, task, catalog, strict_seal=strict_seal)
    token = _current.set(dict(origin='sealed', relations=relations))
    try:
        packet = _original_validate_package(root, task, catalog, strict_seal=strict_seal)
        # The sealed plan is internal. Its preservation policy must also have
        # a source-derived visible limitation in the public profile on reload.
        gaps = packet['profile'].get('gaps', [])
        for link in relations.links:
            expected = _public_gap(relations, link)
            _package._require(isinstance(gaps, list) and any(
                isinstance(row, dict) and isinstance(row.get('id'), str)
                and row['id'].rstrip('_') == expected['id']
                and all(row.get(key) == value for key, value in expected.items() if key != 'id')
                for row in gaps), 'tree reference public limitation missing or changed')
        return packet
    finally:
        _current.reset(token)


def install(core_root):
    global _installed_root, _split, _plan, _package, _rag
    global compact, sha, profile_context, reassemble, SplitHold, _require, _source_value, _article_title
    global _original_split, _original_validate_plan, _original_validate_context
    global _original_compatible_tree, _original_validate_package
    core_root = Path(core_root).resolve()
    if _installed_root is not None:
        if _installed_root != core_root:
            raise RuntimeError('Cannot mix native tree reference releases')
        return
    modules = {name:importlib.import_module('paperflow.' + name) for name in _EXPECTED_MODULE_SHA}
    for name, module in modules.items():
        path = Path(module.__file__).resolve()
        if path != core_root / 'paperflow' / (name + '.py') or hashlib.sha256(path.read_bytes()).hexdigest() != _EXPECTED_MODULE_SHA[name]:
            raise RuntimeError('Native tree reference source changed: ' + name)
    _split, _plan, _package, _rag = [modules[name] for name in ('structure_split', 'structure_plan', 'package', 'rag')]
    compact, sha, profile_context, reassemble, SplitHold = [_split.__dict__[name] for name in ('compact', 'sha', 'profile_context', 'reassemble', 'SplitHold')]
    _require, _source_value, _article_title = [_plan.__dict__[name] for name in ('_require', '_source_value', '_article_title')]
    _original_split = _split.split_projection
    _original_validate_plan, _original_validate_context = _plan.validate_plan, _plan._validate_context
    _original_compatible_tree, _original_validate_package = _package._compatible_tree, _package.validate_package
    _split.split_projection = _split_projection
    _plan.split_projection = _split_projection
    _plan.validate_plan, _plan._validate_context = _validate_plan, _validate_context
    _package._compatible_tree, _package.validate_package = _compatible_tree, _validate_package
    _installed_root = core_root


def install_publication(native_module):
    if hasattr(native_module, '_paperflow_tree_reference_original_prepare'):
        if native_module._paperflow_tree_reference_policy != POLICY:
            raise RuntimeError('Publication tree reference policy conflict')
        return
    original_prepare = native_module.prepare_publication
    original_refs = native_module._public_refs

    def public_refs(value, source, file_refs):
        depth = _public_depth.get()
        token = _public_depth.set(depth + 1)
        try:
            result = original_refs(value, source, file_refs)
        finally:
            _public_depth.reset(token)
        scope = _current.get()
        if depth == 0 and scope and scope['origin'] == 'publication' and isinstance(result, dict) and 'tree' in result and 'profile' in result:
            relations = _Relations(result['tree']['nodes'])
            scope['relations'] = relations
            if relations.links:
                gaps = result.setdefault('gaps', [])
                for link in relations.links:
                    gap = _public_gap(relations, link)
                    while any(row.get('id') == gap['id'] for row in gaps):
                        gap['id'] += '_'
                    gaps.append(gap)
        return result

    def prepare(candidate, bundle, out_dir, **kwargs):
        with publication_scope(candidate, bundle, kwargs.get('candidate_validation'), kwargs.get('admission_policy', 'strict')):
            return original_prepare(candidate, bundle, out_dir, **kwargs)

    native_module._paperflow_tree_reference_original_prepare = original_prepare
    native_module._paperflow_tree_reference_original_refs = original_refs
    native_module._paperflow_tree_reference_policy = POLICY
    native_module.prepare_publication = prepare
    native_module._public_refs = public_refs
