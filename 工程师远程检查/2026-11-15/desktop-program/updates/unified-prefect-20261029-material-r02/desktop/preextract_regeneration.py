"""Explicit, source-bound regeneration grants; native request_once still owns HTTP."""
from pathlib import Path
import json
import re

from preextract_material import file_sha

SCHEMA = 'preextract-regeneration-authorization/1'
MARKER = 'preextract-regeneration-work/1'


def _root(root):
    if root is None:
        from preextract_pipeline import DATA_DIR
        root = DATA_DIR
    return Path(root).resolve()


def _read(path):
    return json.loads(Path(path).read_text(encoding='utf8'))


def _grant(root, digest, campaign=None):
    directory = root/'private/preextract_regeneration'
    index = directory/'active.json'
    if not index.is_file():
        if campaign: raise ValueError('regeneration campaign has no active authorization')
        return None
    active = _read(index).get(digest)
    if active is None:
        if campaign: raise ValueError('source is outside regeneration authorization')
        return None
    name = active.get('campaign','')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,79}',name) or campaign and campaign!=name:
        raise ValueError('regeneration campaign identity changed')
    path = directory/name/'authorization.json'
    if file_sha(path)!=active.get('authorization_sha256'):
        raise ValueError('regeneration authorization changed')
    grant = _read(path)
    if (grant.get('schema')!=SCHEMA or grant.get('campaign')!=name
            or grant.get('mode')!='new_llm_profile' or digest not in grant.get('sources',{})):
        raise ValueError('explicit new-model authorization required')
    work = root/'single-request/regenerations'/name/digest
    for part in (work,*work.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError('regeneration links forbidden')
        if part==root: break
    return grant,path,work,active


def authorization(source,template,work,*,root=None,source_sha256=None):
    """A marker can never authorize itself or replace an unrelated request record."""
    root = _root(root)
    marker = Path(work)/'regeneration.json'
    if not marker.is_file(): return None
    meta = _read(marker)
    digest = source_sha256 or file_sha(source)
    found = _grant(root,digest,meta.get('campaign'))
    if found is None: raise ValueError('regeneration authorization is missing')
    grant,path,expected,active = found
    required = dict(schema=MARKER,campaign=grant['campaign'],source_sha256=digest,
        authorization_sha256=active['authorization_sha256'],template_sha256=grant['template_sha256'],
        catalog_sha256=grant['catalog_sha256'])
    if (Path(work).resolve()!=expected or meta!=required
            or file_sha(template)!=grant['template_sha256']
            or file_sha(grant['catalog_path'])!=grant['catalog_sha256']):
        raise ValueError('regeneration source, template or catalog changed')
    return required


def active_workspace(source,template,*,root=None,campaign=None,source_sha256=None):
    root = _root(root)
    digest = source_sha256 or file_sha(source)
    found = _grant(root,digest,campaign)
    if found is None: return None
    work = found[2]
    if authorization(source,template,work,root=root,source_sha256=digest) is None:
        raise ValueError('regeneration workspace marker is missing')
    return work
