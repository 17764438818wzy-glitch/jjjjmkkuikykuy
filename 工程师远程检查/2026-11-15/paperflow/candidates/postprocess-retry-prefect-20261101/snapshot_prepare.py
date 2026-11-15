"""Freeze a supported input closure under the existing folder claim."""
import copy
import json
from pathlib import PureWindowsPath
import re
from uuid import uuid5

from remote_jobs import NAMESPACE, _validated, JobConflict
from snapshot_collect import collect_inputs
from snapshot_wire import snapshot_sha
from windows_snapshot import _windows_name


_COUNTER_HASHES = ('model_sha256', 'libllama_sha256', 'cl100k_sha256')
_COUNTER_KEYS = set(_COUNTER_HASHES) | {'embedding_model_budget', 'cl_limit'}


def _counter_contract(value):
    if (not isinstance(value, dict) or set(value) != _COUNTER_KEYS
            or any(not isinstance(value[key], str) or not re.fullmatch('[a-f0-9]{64}', value[key])
                   for key in _COUNTER_HASHES)
            or any(type(value[key]) is not int or value[key] <= 0
                   for key in ('embedding_model_budget', 'cl_limit'))):
        raise ValueError('original tokenizer identity and budgets are required')
    return copy.deepcopy(value)


def counter_contract_from_runtime(runtime):
    """Freeze the same identity and budgets the original offline counter uses."""
    try:
        policy = runtime['embedding_admission']['policy']
        limits = (policy['pipeline_limit'], policy['adapter_limit'])
        if any(type(value) is not int or value <= 0 for value in limits):
            raise ValueError('invalid original tokenizer pipeline limits')
        return _counter_contract(dict(**{key: policy[key] for key in _COUNTER_HASHES},
            embedding_model_budget=runtime.get('embedding_model_budget', 2000), cl_limit=min(limits)))
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError('original tokenizer runtime is unavailable') from exc


def prepare_manifest(claim, inputs, *, catalog, code_sha256, paperflow_root_windows,
                     business_workspace_windows, output_root_windows, runtime_template_sha256,
                     counter_contract, allowed_roots, defer_binary_hash=False,
                     max_json_file_bytes=None, max_total_json_bytes=None,
                     trusted_catalog_path=None, job_id=None):
    """Collect only; the returned snapshot is not a server execution grant.

    trusted_catalog_path is caller authority from backend settings, not an
    input declaration. Returned trusted_files is separate local transport
    policy and still requires an independent transport-configuration match.
    """
    try:
        # Detach caller-owned input before the collector starts, refusing NaN or
        # objects whose meaning could change while the manifest is serialized.
        original = json.loads(json.dumps(inputs, ensure_ascii=False, sort_keys=True,
                                         separators=(',', ':'), allow_nan=False))
        frozen_catalog = json.loads(json.dumps(catalog, ensure_ascii=False, sort_keys=True,
                                              separators=(',', ':'), allow_nan=False))
        frozen_counter = _counter_contract(counter_contract)
        root, _, _ = _windows_name(paperflow_root_windows)
        workspace, _, _ = _windows_name(business_workspace_windows)
        output, _, _ = _windows_name(output_root_windows)
        if (not isinstance(frozen_catalog, dict) or not frozen_catalog or root.drive != 'F:'
                or root.name != 'core' or str(PureWindowsPath(paperflow_root_windows)) != paperflow_root_windows
                or workspace.drive != 'F:' or str(workspace) != business_workspace_windows
                or output.drive != 'F:' or len(str(output)) > 64 or len(output.parts) < 2
                or str(output) != output_root_windows or output.is_relative_to(workspace)
                or workspace.is_relative_to(output) or output.is_relative_to(root)
                or root.is_relative_to(output)):
            raise ValueError('a canonical F: business core path is required')
        if not isinstance(runtime_template_sha256, str) or not re.fullmatch('[a-f0-9]{64}', runtime_template_sha256):
            raise ValueError('a pinned server template identity is required')
        if (not isinstance(claim, dict) or claim.get('state') != 'processing'
                or claim.get('identity') != original.get('identity')
                or claim.get('manifest_sha256') != original.get('manifest_sha256')):
            raise ValueError('folder claim does not own these inputs')
        closure = collect_inputs(original, allowed_roots=allowed_roots, defer_binary_hash=defer_binary_hash,
                                 max_json_file_bytes=max_json_file_bytes, max_total_json_bytes=max_total_json_bytes,
                                 trusted_catalog_path=trusted_catalog_path)
        if trusted_catalog_path is not None and (closure.get('catalog_evidence') or {}).get('value') != frozen_catalog:
            raise ValueError('snapshot catalog differs from the trusted material-bound catalog')
        scope = ('supported_dependency_graph_binary_hash_deferred' if defer_binary_hash
                 else 'supported_dependency_closure_only')
        if (closure.get('status') != 'complete'
                or closure.get('evidence_scope') != scope
                or closure['identity'] != claim['identity']
                or closure['manifest_sha256'] != claim['manifest_sha256']
                or closure['inputs'] != original):
            raise ValueError('supported input closure could not be frozen')
        if any(PureWindowsPath(row['path']).is_relative_to(output) for row in closure['files']):
            raise ValueError('output root overlaps original input evidence')
        manifest = dict(schema='paperflow-remote-snapshot/1',
            job_id=job_id or str(uuid5(NAMESPACE, original['identity']['task_run_id'])),
            claim_id=claim['claim_id'], identity=copy.deepcopy(original['identity']),
            generation=claim['generation'], manifest_sha256=claim['manifest_sha256'],
            code_sha256=code_sha256, inputs=closure['inputs'], files=closure['files'],
            catalog=frozen_catalog, paperflow_root_windows=paperflow_root_windows,
            business_workspace_windows=business_workspace_windows, output_root_windows=output_root_windows,
            runtime_template_sha256=runtime_template_sha256, counter_contract=frozen_counter,
            binary_hash_scope='server' if defer_binary_hash else 'source_and_server')
        sha = snapshot_sha(manifest)
        job = {key: manifest[key] for key in ('job_id', 'claim_id', 'identity', 'generation',
                                               'manifest_sha256', 'code_sha256')}
        job['snapshot_sha256'] = sha
        _validated(job)
        catalog_evidence = closure.get('catalog_evidence')
        # Separate local transport policy, never a self-authorizing manifest
        # field. The transport must match this against its own pinned config.
        trusted_files = ([] if catalog_evidence is None else
                         [{key: catalog_evidence[key] for key in ('path', 'sha256')}])
        return dict(manifest=manifest, snapshot_sha256=sha, dependency_scope=scope,
                    trusted_files=trusted_files)
    except (KeyError, TypeError, JobConflict) as exc:
        raise ValueError('folder snapshot cannot be prepared') from exc
