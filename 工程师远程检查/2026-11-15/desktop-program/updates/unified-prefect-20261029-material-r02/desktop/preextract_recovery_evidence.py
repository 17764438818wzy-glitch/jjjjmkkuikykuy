"""Reuse native proof readers for derived candidates, without a fake model journal."""
import json
from pathlib import Path
from preextract_material import file_sha


def _read(path):return json.loads(Path(path).read_text(encoding='utf8'))


def candidate_evidence(source,work,*,template_path):
    from preextract_pipeline import _revision,_import_candidate
    work=Path(work).resolve();bundle=_read(work/'material.json')
    if bundle['source']['source_sha256']!=file_sha(source):raise ValueError('candidate source identity changed')
    paths={work/'material.json'}
    if (work/'candidate-import.json').is_file():
        revision=_revision(source,work)
        if not revision or revision['mode']!='candidate_import':raise ValueError('candidate import revision missing')
        _import_candidate(work,revision)
        candidate=work/'candidate-import.json';kind='candidate_import'
        paths.update((work/'revision.json',candidate))
    elif (work/'model/partition-state.json').is_file():
        from preextract_partition_run import run_partition
        from preextract_contract import load_template
        state=_read(work/'model/partition-state.json')
        if state.get('status')!='assembled':raise ValueError('partition assembly is not complete')
        def never(*a,**k):raise ValueError('saved partition evidence requires another request')
        run_partition(bundle,work,load_template(template_path)[0],{},never,never,before_model=never)
        candidate=work/'model/assembled-candidate.json';kind='partition_assembly'
        # Include the exact native evidence validated above, including prior
        # attempts retained by the partition recovery journal.
        paths.update(p for p in (work/'model').rglob('*') if p.is_file() and
            p.suffix in ('.json','.txt','.jsonl') and p.name!='validation.json')
    else:
        from preextract_scheduler_adapter import request_observation
        phase,step,_=request_observation(work/'model')
        if phase!='response_complete' or step!='postprocess':raise ValueError('complete adopted response proof required')
        from preextract_contract import load_template
        from preextract_pipeline import validate_saved_request
        validate_saved_request(_read(work/'model/request.json'),bundle,load_template(template_path)[0])
        candidate=work/'model/response.txt';kind='model_response'
        paths.update(work/'model'/name for name in ('request.json','request-state.json','response.txt',
            'response-events.jsonl','adopted-response.json') if (work/'model'/name).is_file())
    return dict(kind=kind,candidate_path=str(candidate),template_path=str(Path(template_path).resolve()),
        template_sha256=file_sha(template_path),files={str(p.relative_to(work)):file_sha(p) for p in sorted(paths)})


def verify_parent(source,work,meta):
    parent=Path(meta['parent']);proof=meta['parent_evidence']
    current=candidate_evidence(source,parent,template_path=proof['template_path'])
    if current!=proof:raise ValueError('revision parent evidence changed')
    if file_sha(Path(work)/'material.json')!=proof['files']['material.json']:
        raise ValueError('revision must reuse the complete registered material')
    return meta


def required_candidate_path(meta):
    return Path(meta['parent_evidence']['candidate_path']) if meta.get('parent_evidence') else Path(meta['parent'])/'model/response.txt'


def inherited_bundle(source,parent,bundle,*,alias_context=None,template_path=None):
    """The model's image/attachment material is reused from the verified ancestor."""
    from preextract_pipeline import _revision
    from preextract_images import restore_request_images
    current=Path(parent).resolve()
    for _ in range(64):
        if (current/'model/partition-material.json').is_file():
            if alias_context is not None and template_path is not None:
                from preextract_partition_run import run_partition
                from preextract_contract import load_template
                if _read(current/'model/partition-state.json').get('status')!='assembled':
                    raise ValueError('inherited image proof requires complete partition assembly')
                def never(*a,**k):raise ValueError('inherited image proof cannot create a request')
                _,_,restored=run_partition(bundle,current,load_template(template_path)[0],{},never,never,
                                           alias_context=alias_context)
                return restored
            return _read(current/'model/partition-material.json')
        if (current/'model/request.json').is_file():
            return restore_request_images(bundle,current,_read(current/'model/request.json'),
                                          alias_context=alias_context,original_bundle=bundle)
        revision=_revision(source,current)
        if not revision:break
        current=Path(revision['parent'])
    from preextract_cleanup_scope import verify_reuse
    verify_reuse(current,source=source)
    return bundle
