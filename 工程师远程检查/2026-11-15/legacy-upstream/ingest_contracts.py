"""Identity and outcome contract shared by RAGFlow ingest adapters."""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from engine import safe_component
from upload_tracking import service_key


class Outcome(str, Enum):
    accepted = 'accepted'
    quality_hold = 'quality_hold'
    quality_reject = 'quality_reject'
    technical_review = 'technical_review'
    outcome_unresolved = 'outcome_unresolved'


@dataclass(frozen=True)
class ArtifactSpec:
    service_url: str
    dataset_id: str
    source_id: str
    role: str
    lane: Literal['paper', 'image']
    content_sha256: str
    path: Path
    remote_name: str
    pipeline_id: str | None
    parse_required: bool
    priority: int
    batch_id: str
    policy_key: str
    channel_key: str
    handoff_id: str | None = None
    package_path: Path | None = None
    package_sha256: str | None = None

    def __post_init__(self) -> None:
        parts = urlsplit(self.service_url.strip())
        if parts.scheme not in ('http', 'https') or not parts.hostname:
            raise ValueError('Invalid RAGFlow service URL')
        for field in ('dataset_id', 'source_id', 'role', 'remote_name',
                      'batch_id', 'policy_key', 'channel_key'):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f'{field} is required')
        if self.lane not in ('paper', 'image'):
            raise ValueError('Invalid artifact lane')
        if not re.fullmatch(r'[0-9a-fA-F]{64}', self.content_sha256):
            raise ValueError('content_sha256 must be a SHA-256 hex digest')
        if not isinstance(self.path, Path):
            raise ValueError('path must be a Path')
        if bool(self.package_path) != bool(self.package_sha256):
            raise ValueError('Package path and hash must be supplied together')
        if self.package_path and (not isinstance(self.package_path,Path) or
                not re.fullmatch(r'[0-9a-fA-F]{64}',self.package_sha256 or '')):
            raise ValueError('Invalid package provenance')
        if (len(self.remote_name.encode('utf-8')) > 255
                or any(c in self.remote_name for c in '\r\n/\\')):
            raise ValueError('Invalid remote filename')
        if self.policy_key == 'native_naive' and self.pipeline_id:
            raise ValueError('Native parser tasks cannot select a Pipeline')
        if self.parse_required and not self.pipeline_id and self.policy_key != 'native_naive':
            raise ValueError('A parse task requires a Pipeline ID')
        if not isinstance(self.priority, int) or self.priority < 0:
            raise ValueError('priority must be a nonnegative integer')


def task_identity(spec: ArtifactSpec) -> tuple[str, str, str, str]:
    return (service_key(spec.service_url), spec.dataset_id,
            spec.source_id, spec.role)


def canonical_remote_name(title: str, source_id: str, content_sha256: str,
                          suffix: str, legacy_id: str | None = None) -> str:
    if not re.fullmatch(r'[0-9a-fA-F]{64}', content_sha256):
        raise ValueError('Invalid content hash')
    if not suffix.startswith('.') or any(c in suffix for c in '\r\n/\\'):
        raise ValueError('Invalid filename suffix')
    identity = safe_component(source_id)
    if identity != source_id or not source_id.strip():
        raise ValueError('source_id is not filename-safe')
    legacy = ''
    if legacy_id is not None:
        if safe_component(legacy_id) != legacy_id:
            raise ValueError('legacy_id is not filename-safe')
        legacy = '_' + legacy_id
    tail = f'_{identity}_{content_sha256[:16]}{legacy}{suffix}'
    available = 255 - len(tail.encode('utf-8'))
    if available < 1:
        raise ValueError('Identity fields exceed remote filename limit')
    head = safe_component(title or 'paper')
    while len(head.encode('utf-8')) > available:
        head = head[:-1]
    head = head.rstrip(' ._') or 'p'
    return head + tail
