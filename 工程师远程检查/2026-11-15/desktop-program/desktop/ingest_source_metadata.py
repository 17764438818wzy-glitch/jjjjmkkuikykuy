"""Host-only boundary for the shared source receiver; no source adjudication here."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import threading
from pathlib import Path

from evidence import package_fingerprint
from source_education import source_education_revision


class SourceMetadataBlocked(ValueError):
    """Safe local reason code, never receiver output or paper content."""


def source_payload(spec):
    """Enroll at explicit enqueue only, without scanning old tasks or originals."""
    if spec.policy_key in ('research_profile','research_material'):
        # Its dual-hash source packet is published by the profile postprocessor.
        # The older degree receiver accepts only its explicitly enrolled old datasets.
        return None
    if spec.role != 'paper_primary' or not spec.package_path:
        return None
    if not (spec.package_path/'metadata.json').is_file():
        return None
    metadata = json.loads((spec.package_path/'metadata.json').read_text(encoding='utf8'))
    if 'source_education' not in metadata:
        return None
    text = spec.path.read_text(encoding='utf8')
    match = re.search(r'\[PAPER_PIPELINE_METADATA_V1\]\s*(.*?)\s*\[/PAPER_PIPELINE_METADATA_V1\]', text, re.S)
    if not match:
        raise SourceMetadataBlocked('handoff_missing')
    handoff = json.loads(match.group(1))
    payload = {key: metadata.get(key) for key in ('source_education', 'source_education_revision')}
    if (handoff.get('paper_id') != spec.handoff_id
            or any(handoff.get(key) != value for key, value in payload.items())
            or source_education_revision(payload['source_education']) != payload['source_education_revision']):
        raise SourceMetadataBlocked('handoff_mismatch')
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def verify_local(task):
    try:
        content = Path(task['path']).read_bytes()
        if hashlib.sha256(content).hexdigest() != task['content_sha256']:
            raise SourceMetadataBlocked('content_changed')
        if package_fingerprint(Path(task['package_path'])) != task['package_sha256']:
            raise SourceMetadataBlocked('package_changed')
    except (OSError, KeyError, ValueError) as exc:
        if isinstance(exc, SourceMetadataBlocked):
            raise
        raise SourceMetadataBlocked('local_source_unavailable') from None


def validate_receipt(task, receipt):
    revision = json.loads(task['source_metadata_payload'])['source_education_revision']
    if (not isinstance(receipt, dict) or receipt.get('status') not in
            ('confirmed', 'retryable_error', 'blocked')):
        raise SourceMetadataBlocked('invalid_receiver_receipt')
    if (receipt.get('document_id') != task['remote_id']
            or receipt.get('source_education_revision') != revision):
        raise SourceMetadataBlocked('receiver_identity_mismatch')
    if (receipt.get('verification') not in ('pending', 'verified', 'conflict', 'unsupported')
            or not isinstance(receipt.get('receipt_path'), str)
            or len(receipt['receipt_path']) > 4096
            or not re.fullmatch(r'[a-zA-Z0-9_.-]{1,80}', receipt.get('reason_code') or '')):
        raise SourceMetadataBlocked('invalid_receiver_receipt')
    return {key: receipt[key] for key in ('status', 'document_id', 'source_education_revision',
            'verification', 'receipt_path', 'reason_code')}


class SourceReceiverCLI:
    def __init__(self, command, timeout=60, max_output=65536):
        if (not isinstance(command, list) or not command
                or any(not isinstance(arg, str) or not arg or '\x00' in arg for arg in command)
                or not Path(command[0]).is_absolute()
                or Path(command[0]).suffix.lower() in ('.bat', '.cmd')):
            raise ValueError('source_receiver_command must be a trusted absolute executable argv')
        if not 0 < timeout <= 120 or not 1 <= max_output <= 65536:
            raise ValueError('Invalid source receiver resource limits')
        self.command, self.timeout, self.max_output = list(command), timeout, max_output

    def __call__(self, task, remote):
        verify_local(task)
        if remote.get('id') != task.get('remote_id') or remote.get('name') != task['remote_name']:
            raise SourceMetadataBlocked('remote_identity_mismatch')
        request = {key: task[key] for key in ('service_key', 'dataset_id', 'package_path',
            'content_sha256', 'package_sha256', 'source_id', 'handoff_id', 'batch_id')}
        request.update(request_id=task['source_metadata_request_id'], document_id=task['remote_id'])
        data = (json.dumps(request, ensure_ascii=False)+'\n').encode('utf8')
        process = subprocess.Popen(self.command, shell=False, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        output = []
        def read():
            block = bytearray()
            try:
                while len(block) <= self.max_output:
                    chunk = os.read(process.stdout.fileno(), min(4096,self.max_output+1-len(block)))
                    if not chunk:
                        break
                    block.extend(chunk)
                output.append(bytes(block))
                if len(block) > self.max_output:
                    try: process.kill()
                    except OSError: pass
            finally:
                process.stdout.close()
        def write():
            try:
                process.stdin.write(data)
                process.stdin.close()
            except (OSError, ValueError):
                pass
            finally:
                process.stdin.close()
        reader = threading.Thread(target=read, daemon=True)
        writer = threading.Thread(target=write, daemon=True)
        reader.start(); writer.start()
        try:
            process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise TimeoutError('source_receiver_timeout') from None
        finally:
            reader.join(1); writer.join(1)
        if not output or len(output[0]) > self.max_output:
            raise SourceMetadataBlocked('receiver_output_limit')
        if process.returncode:
            raise RuntimeError('source_receiver_exit')
        try:
            lines = output[0].decode('utf8').splitlines()
            if len(lines) != 1:
                raise ValueError()
            receipt = json.loads(lines[0])
        except (ValueError, UnicodeError):
            raise SourceMetadataBlocked('invalid_receiver_output') from None
        return validate_receipt(task, receipt)
