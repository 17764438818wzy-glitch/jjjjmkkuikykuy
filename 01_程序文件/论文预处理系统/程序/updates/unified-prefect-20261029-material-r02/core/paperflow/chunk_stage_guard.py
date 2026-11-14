"""Exact stage-only preflight through the existing final projection/admission core.

The temporary seal is real, but it is not the future delivery manifest. This
module never creates final qualification, registers a package, or changes text.
"""
from pathlib import Path
import hashlib

from .embedding_admission import (ConfiguredAdmission, AdmissionRejected, digest,
                                  evaluate, final_inputs, renderer_fingerprint)


CONTRACT = 'paperflow-chunk-stage-preflight/1'
INHERITED_FILES = ('article.redacted.md', 'profile.json', 'tree.json', 'assets.json')


def contract_fingerprint(config):
    settings = getattr(config, 'embedding_admission', None) or {}
    return digest(dict(contract=CONTRACT,
        implementation_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        policy=settings.get('policy'), path_evidence=settings.get('path_evidence'),
        model_budget=getattr(config, 'embedding_model_budget', None),
        native=bool(config.native_pipeline_id), renderer=renderer_fingerprint()))


def check_chunk_stage(task, packet, admission):
    """Count current chunk-owned projections; caller holds no resource permit.

Record ownership comes from packet.chunks, not role/body or id prefixes:
learning and chart are chunk outputs, while profile, Tree and asset descriptions
also share role=body and must not consume a chunk correction.
"""
    owned = {row['id']: row.get('kind') for row in packet['chunks']
             if row.get('kind') in ('body', 'learning', 'chart')}
    inputs = [row for row in final_inputs(task, packet, native=admission.native)
              if row.role == 'body' and row.record_id in owned]
    if isinstance(admission, ConfiguredAdmission):
        admission.observed_renderer = renderer_fingerprint()
    evidence = evaluate(inputs, admission.policy, admission.counter,
        observed_path=admission.observed_path, observed_renderer=admission.observed_renderer)
    evidence.update(schema=CONTRACT, scope='chunk_stage_provisional_projection',
        stage_preflight_passed=evidence['admission_verified'], admission_verified=False,
        final_identity_verified=False, provisional_manifest_sha256=packet['manifest_sha256'],
        final_checks_required=['sealed_package_full_input', 'first_remote_write_full_input'])
    for row in evidence['records']:
        row['chunk_kind'] = owned[row['record_id']]
    # Missing/drifting policy and invalid counters are infrastructure/configuration
    # failures. Only verified numeric overlength may request a content correction.
    if evidence['issues'] or not evidence['counter_verified'] or not evidence['path_verified']:
        raise AdmissionRejected(evidence)
    return evidence
