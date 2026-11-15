"""Authenticated, read-only identity for the bounded Paperflow deployment."""
import hashlib
import json
from pathlib import Path
import uuid

from api.apps import login_required
from api.utils.api_utils import add_tenant_id_to_kwargs, get_result

SCOPE=Path('/ragflow/logs/paperflow-deployment/identity.json')
FILES=(Path(__file__),SCOPE,Path('/ragflow/VERSION'),Path('/ragflow/conf/service_conf.yaml'),
       Path('/ragflow/api/apps/__init__.py'),Path('/ragflow/api/ragflow_server.py'),
       Path('/ragflow/api/apps/restful_apis/chunk_api.py'),Path('/ragflow/api/apps/restful_apis/document_api.py'),
       Path('/etc/nginx/conf.d/ragflow.conf'))


def fingerprints():
    return {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in FILES}


STARTED=fingerprints()
NONCE=uuid.uuid4().hex


@manager.route('/paperflow/deployment',methods=['GET'])  # noqa: F821
@login_required
@add_tenant_id_to_kwargs
async def deployment(tenant_id):
    if fingerprints()!=STARTED:
        return get_result(code=409,message='Deployment files changed; re-acceptance required'),409
    scope=json.loads(SCOPE.read_text())
    if tenant_id!=scope['tenant_id']:
        return get_result(code=403,message='Deployment scope unavailable'),403
    # This file is provisioned after checking each library belongs to this tenant.
    # Scope is a publication allowlist; it does not alter other tenants or routes.
    return get_result(data=dict(deployment=scope['deployment'],endpoint=scope['endpoint'],
        dataset_ids=scope['dataset_ids'],process_nonce=NONCE,files=STARTED,
        chunk_api_sha256=STARTED['/ragflow/api/apps/restful_apis/chunk_api.py']))
