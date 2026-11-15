"""Derive one server transport scope from one verified original folder.

This is a pure configuration transform: it neither discovers, reserves nor
dispatches work. The caller must independently pin and validate the template,
obtain the record through the registered read-only folder verifier, and recheck
the original task gates before starting the same folder.
"""
import copy
import hashlib
import json
from pathlib import Path


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def derive_folder_config(template, record, *, identity, generation, expected_counter):
    try:
        if (not isinstance(template, dict) or template.get('schema') != 'paperflow-remote-live/1'
                or not isinstance(record, dict) or record.get('schema') != 'paperflow-selected-ticket-evidence/1'
                or not isinstance(identity, dict) or set(identity) != set(FIELDS)
                or record.get('identity') != identity or type(generation) is not int or generation <= 0
                or record.get('generation') != generation or record.get('claim_created') is not False
                or record.get('server_execution_started') is not False
                or record.get('counter_contract') != expected_counter
                or record['configuration_sources']['settings']['sha256'] != template['settings_sha256']
                or record['configuration_sources']['runtime']['sha256'] != template['tokenizer_runtime_sha256']):
            raise ValueError('the original folder or independently pinned configuration changed')
        policy = record['configuration']
        roots, trusted = policy['allowed_roots'], policy['trusted_files']
        if (not isinstance(roots, list) or not roots or any(not isinstance(root, str) or not root
                or not Path(root).is_absolute() or '..' in Path(root).parts for root in roots)
                or len(set(roots)) != len(roots)
                or trusted != [dict(path=template['expected_catalog_path'], sha256=template['expected_catalog_sha256'])]):
            raise ValueError('the folder source roots or pinned catalog differ')
        result = copy.deepcopy(template)
        result['transport_config']['allowed_roots'] = copy.deepcopy(roots)
        result['transport_config']['trusted_files'] = copy.deepcopy(trusted)
        result['snapshot_options']['allowed_roots'] = copy.deepcopy(roots)
        canonical = json.dumps(result['transport_config'], ensure_ascii=False, sort_keys=True,
                               separators=(',', ':'), allow_nan=False).encode('utf-8')
        result['transport_config_sha256'] = hashlib.sha256(canonical).hexdigest()
        return result
    except (KeyError, TypeError, OSError) as exc:
        raise ValueError('the original folder configuration is incomplete') from exc
