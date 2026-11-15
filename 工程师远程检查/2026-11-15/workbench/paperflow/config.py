from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import hashlib
from pathlib import Path


def rules_fingerprint(config):
    root=Path(__file__).parent
    files=[root/n for n in ('producer.py','package.py','reading.py','rag.py')]
    files+=sorted((root/'prompts').glob('*'))
    h=hashlib.sha256((config.contract_version+config.provider+config.model).encode())
    for path in files:
        if path.is_file():h.update(path.read_bytes())
    return h.hexdigest()


@dataclass
class Config:
    home: str = r'E:\rag mid\langgraph_workflow\runtime'
    handoff_root: str = r'E:\rag mid'
    legacy_app: str = r'E:\uv\ragflow_upstream'
    source_roots: list[str] = field(default_factory=lambda: [
        r'C:\Users\Administrator\Desktop\管理学', r'C:\Users\Administrator\Desktop\法学'])
    catalog_path: str = r'E:\rag mid\reference\specialty_catalog.json'
    rag_url: str = 'http://192.168.1.120:8088'
    provider: str = 'free'
    model: str = 'deepseek-v4.1-flash'
    model_access_fingerprint: str = ''  # Non-secret revision of upstream access configuration.
    reader_python: str | None = None
    office_temp_root: str = r'E:\rag mid\lo_convert'
    llm_workers: int = 2
    article_workers: int | None = None  # Explicit graph/repair pool; legacy profiles keep their existing limits.
    rag_workers: int = 1
    local_workers: int = 2
    max_ingest_backlog: int = 30
    max_live_units: int = 8
    max_pending_package_bytes: int = 2*1024**3
    min_free_gib: float = 5.0
    max_memory_percent: float = 90.0
    scan_seconds: float = 45.0
    stable_seconds: float = 15.0
    retry_seconds: float = 20.0
    max_stagnant_repairs: int = 3
    max_infrastructure_retries: int = 20
    max_infrastructure_seconds: float = 86400.0
    short_retry_interval: float = 0.5
    model_timeout: float = 2400.0
    lease_seconds: float = 3000.0
    contract_version: str = 'paperflow/1.0'
    cleanup: bool = True
    old_version_retention_seconds: float = 7*86400  # Legacy retirements without replacement acceptance proof.
    preserve_sealed_packages: bool = True
    orphan_package_retention_seconds: float | None = None
    maintenance_seconds: float = 300
    maintenance_documents_per_pass: int = 4
    acceptance_report: str | None = None
    native_pipeline_id: str | None = None  # Explicit opt-in; legacy publishing remains the default.
    native_ingest_timeout: float = 600.0
    delivery_acceptance_report: str | None = None
    production_pilot: str | None = None  # Explicit expiring selection, never a bulk grant.
    attachment_targets: dict = field(default_factory=dict)  # drawing/software -> explicit parent_id + description dataset_id.
    remote_harness: dict | None = None  # Explicit remote execution; all other services remain local.
    remote_harness_groups: int = 0  # Opt-in: eight native article-stage contexts per SDK.

    @property
    def root(self) -> Path:
        return Path(self.home)

    @property
    def catalog(self) -> dict:
        obj = json.loads(Path(self.catalog_path).read_text(encoding='utf-8-sig'))
        codes = [r['code'] for r in obj['majors']]
        if len(codes) != len(set(codes)):
            raise ValueError('duplicate specialty code')
        return obj

    def prepare(self) -> None:
        for part in ('work', 'private', 'receipts', 'tmp', 'checkpoints', 'logs'):
            (self.root / part).mkdir(parents=True, exist_ok=True)

    @classmethod
    def load(cls, path: Path | str | None = None) -> 'Config':
        data = json.loads(Path(path).read_text(encoding='utf-8-sig')) if path else {}
        result = cls(**data)
        if min(result.llm_workers, result.rag_workers, result.local_workers) < 1:
            raise ValueError('worker counts must be positive')
        return result

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding='utf-8')
