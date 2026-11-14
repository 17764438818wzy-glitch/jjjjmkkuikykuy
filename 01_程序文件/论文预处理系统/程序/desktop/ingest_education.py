"""Education routing, admission and source-linked Tree/Graph artifacts."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import quote

from engine import digest
from evidence import package_fingerprint
from education_direct_tree import bind_tree_chunks, extract_tree
from education_method_graph import build_method_graph
from education_stream_ingest import write_json
from ingest_contracts import ArtifactSpec, Outcome, canonical_remote_name
from ingest_execution import RemoteExecutionChanged
from ingest_execution_diagnostics import IngestionLogReader, PROOF_METADATA
from knowledge_package import knowledge_files


PAPER_ID = re.compile(r'paper_[0-9a-f]{24}\Z')
IMAGE_ROLES = {'image', 'image_projection', 'paper_assets'}


def education_specs(item: dict, deployment: dict, canonical_package: Path,
                    service_url: str, batch_id: str) -> list[ArtifactSpec]:
    """Construct upload tasks only after the canonical package passes identity checks."""
    package = Path(canonical_package)
    if (item.get('status') != 'pass' or item.get('category') != '教育教学类'
            or not PAPER_ID.fullmatch(item.get('paper_id') or '')):
        raise ValueError('Education item is outside approved scope')
    knowledge_files(package, require_stable_paper_id=True)
    manifest = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    reviewed_source = Path(item['package_path']) if item.get('package_path') else package
    if (manifest.get('paper_id') != item['paper_id']
            or manifest.get('document_id') != item['id']
            or digest(reviewed_source / 'ingest.md') != item.get('reviewed_hash')):
        raise ValueError('Education package identity or content hash mismatch')
    pipeline = deployment.get('full_ingest_pipeline_id') or deployment['pipeline_id']
    main = package / 'ingest.md'
    main_hash = digest(main)
    common = dict(service_url=service_url, source_id=item['paper_id'],
                  priority=0, batch_id=batch_id, policy_key='education',
                  channel_key=deployment.get('model_channel_key') or 'education-default',
                  package_path=package,package_sha256=package_fingerprint(package))
    tasks = [ArtifactSpec(dataset_id=deployment['paper_dataset_id'],
                          role='paper_primary', lane='paper',
                          content_sha256=main_hash, path=main,
                          remote_name=canonical_remote_name(
                              manifest.get('title') or item.get('title') or 'education-paper',
                              item['paper_id'], main_hash, '.md', legacy_id=item['id']),
                          pipeline_id=pipeline, parse_required=True,handoff_id=item['paper_id'], **common)]
    resources = {row.get('local_path'): row for row in
                 json.loads((package / 'metadata.json').read_text(encoding='utf-8')).get('resources') or []}
    for row in manifest['files']:
        role = row.get('role')
        if role not in IMAGE_ROLES or not deployment.get('image_dataset_id'):
            continue
        relative = row['path']
        path = package / relative
        if role == 'image':
            resource = resources.get(relative)
            if (not resource or resource.get('privacy_status') != 'approved'
                    or resource.get('sha256') != row['sha256']):
                raise ValueError('Original image privacy/hash check failed')
        path_hash = digest(path)
        path_tag = hashlib.sha256(relative.encode('utf-8')).hexdigest()[:8]
        tasks.append(ArtifactSpec(dataset_id=deployment['image_dataset_id'],
            role=f'{role}:{relative}', lane='image', content_sha256=path_hash,
            path=path, remote_name=canonical_remote_name(
                f'{Path(relative).stem}_{path_tag}', item['paper_id'], path_hash,
                Path(relative).suffix or '.bin', legacy_id=item['id']),
            pipeline_id=None, parse_required=False, **common))
    return tasks


def classify_education(task: dict, remote: dict) -> Outcome:
    """Distinguish explicit quality decisions from ambiguous technical failures."""
    metadata = remote.get('meta_fields') or {}
    if metadata.get('education_paper_id') == task.get('source_id'):
        admission = metadata.get('education_admission')
        if remote.get('run') == 'DONE' and admission == '入库':
            return Outcome.accepted
        if admission == '暂缓':
            return Outcome.quality_hold
        if admission == '剔除':
            return Outcome.quality_reject
    progress = remote.get('progress_msg') or ''
    if re.fullmatch(r'\d+ tasks? are ahead in the queue\.\.\.', progress.strip()):
        return Outcome.outcome_unresolved
    return Outcome.technical_review


class EducationArtifacts:
    """Persist per-paper deterministic artifacts from admitted remote sources."""
    def __init__(self, client_factory, output_dir: Path, source_reader=None):
        self.client_factory = client_factory
        self.output_dir = Path(output_dir)
        self.source_reader = source_reader or IngestionLogReader(client_factory)

    def invalidate(self, task: dict) -> None:
        """Withdraw current pointers before a new attempt, retaining all versions."""
        paper_id=task['source_id']
        if not PAPER_ID.fullmatch(paper_id): raise ValueError('Invalid artifact identity')
        latest=self.output_dir/'latest'/(paper_id+'.json')
        previous=json.loads(latest.read_text(encoding='utf-8')) if latest.exists() else {}
        write_json(latest,dict(previous,valid=False,paper_id=paper_id,attempt=task.get('execution_attempt') or 0))
        for folder,filename in (('direct_trees','tree.json'),('method_graphs','method_graph.json')):
            path=self.output_dir/folder/(paper_id+'.json')
            if path.exists():
                versions=self.output_dir/'attempts'/paper_id
                version=Path(previous['version_path']) if previous.get('version_path') else versions/str(previous.get('attempt',0))
                if not version.resolve().is_relative_to(versions.resolve()):
                    raise ValueError('Prior artifact version is outside the paper archive')
                archive=version/filename
                old=json.loads(path.read_text(encoding='utf-8'))
                if archive.exists() and json.loads(archive.read_text(encoding='utf-8'))!=old:
                    old_hash=hashlib.sha256(json.dumps(old,sort_keys=True).encode()).hexdigest()[:16]
                    archive=version/('legacy-'+old_hash)/filename
                if not archive.exists(): write_json(archive,old)
                path.unlink()

    def __call__(self, task: dict, remote: dict) -> None:
        paper_id = task['source_id']
        remote_id = task['remote_id']
        if not PAPER_ID.fullmatch(paper_id): raise ValueError('Invalid artifact identity')
        latest = self.output_dir / 'latest' / (paper_id + '.json')
        if latest.exists():
            previous = json.loads(latest.read_text(encoding='utf-8'))
            write_json(latest, dict(previous, valid=False))
        if remote.get('run') != 'DONE':
            raise RemoteExecutionChanged(remote)
        if (task['role'] != 'paper_primary' or remote.get('id') != remote_id
                or remote.get('name') != task['remote_name']
                or classify_education(task, remote) != Outcome.accepted):
            raise ValueError('Education artifact source is not an admitted paper')
        path = Path(task['path'])
        source_hash = digest(path)
        if source_hash != task['content_sha256']:
            raise ValueError('Education artifact source changed after upload')
        client = self.client_factory()
        chunks = []
        dataset = quote(task['dataset_id'], safe='')
        document = quote(remote_id, safe='')
        for page in range(1, 101):
            payload = client.request('GET',
                f'/datasets/{dataset}/documents/{document}/chunks?page={page}&page_size=100')
            batch = payload.get('chunks') or []
            chunks.extend(batch)
            if len(chunks) >= payload.get('total', 0):
                break
            if not batch:
                raise ValueError('Education artifact chunk pagination stopped early')
        else:
            raise ValueError('Education artifact chunk page limit exceeded')
        proof = self.source_reader.source_proof(task, remote, chunks)
        if not proof:
            error = ValueError('Education artifact source proof is missing or inconsistent')
            error.artifact_evidence_reason = 'proof_conflict'
            raise error
        tree = bind_tree_chunks(extract_tree(path.read_text(encoding='utf-8'),
                                             paper_id, remote_id), chunks)
        graph = build_method_graph(paper_id, remote_id, remote.get('meta_fields') or {}, chunks)
        graph['ingest_sha256'] = source_hash
        attempt=int(task.get('execution_attempt') or 0)
        # Method matching changed in v3. Keep the previous verified projection
        # immutable while permitting a source-verified rebuild of this attempt.
        version=self.output_dir/'attempts'/paper_id/str(attempt)/'verified-v3'
        artifacts=(('direct_trees','tree.json',tree),('method_graphs','method_graph.json',graph))
        for folder,filename,value in artifacts:
            archive=version/filename
            if archive.exists() and json.loads(archive.read_text(encoding='utf-8'))!=value:
                raise ValueError('Existing education attempt artifact differs')
            if not archive.exists(): write_json(archive,value)
            destination=self.output_dir/folder/(paper_id+'.json')
            if destination.exists():
                old=json.loads(destination.read_text(encoding='utf-8'))
                if old!=value:
                    legacy=version.parent/filename
                    if not legacy.exists(): write_json(legacy,old)
                    elif json.loads(legacy.read_text(encoding='utf-8'))!=old:
                        old_hash=hashlib.sha256(json.dumps(old,sort_keys=True).encode()).hexdigest()[:16]
                        write_json(version.parent/('legacy-'+old_hash)/filename,old)
        current = client.documents(task['dataset_id'], document_id=remote_id)
        fresh = current[0] if len(current) == 1 else {}
        if (any(fresh.get(k) != remote.get(k) for k in
                ('id', 'name', 'run', 'pipeline_id', 'process_begin_at'))
                or any((fresh.get('meta_fields') or {}).get(k) !=
                       (remote.get('meta_fields') or {}).get(k) for k in PROOF_METADATA)):
            raise RemoteExecutionChanged(fresh)
        if digest(path) != source_hash:
            raise ValueError('Education artifact source changed during verification')
        for folder, _, value in artifacts:
            write_json(self.output_dir/folder/(paper_id+'.json'),value)
        write_json(latest,
                   {'valid':True,'paper_id':paper_id,'remote_id':remote_id,'attempt':attempt,
                    'pipeline_id':proof['pipeline_id'],'source_sha256':source_hash,'source_proof':proof,
                    'version_path':str(version)})
