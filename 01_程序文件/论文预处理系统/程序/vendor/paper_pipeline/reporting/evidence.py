"""Run evidence bundle collection.

The bundle records paths and small summaries only. It does not copy source
documents, final Markdown bodies, database credentials, or SQL connection data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from paper_pipeline.reporting.report_models import ReportGenerationContext, utc_now_iso


@dataclass(slots=True)
class RunEvidenceBundle:
    run_id: str
    generated_at: str
    report_paths: list[str] = field(default_factory=list)
    audit_log_path: str = ""
    run_state_path: str = ""
    processing_plan_path: str = ""
    hash_ledger_path: str = ""
    stage0_rejected_audit_path: str = ""
    output_cleaned_paths: list[str] = field(default_factory=list)
    library_paths: list[str] = field(default_factory=list)
    index_manifest_path: str = ""
    sql_preview_paths: list[str] = field(default_factory=list)
    sql_upload_batch_paths: list[str] = field(default_factory=list)
    failed_items: list[dict[str, Any]] = field(default_factory=list)
    pending_items: list[dict[str, Any]] = field(default_factory=list)
    blocked_items: list[dict[str, Any]] = field(default_factory=list)
    checksum: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceCollector:
    context: ReportGenerationContext

    def collect(
        self,
        *,
        report_paths: list[str | Path] | None = None,
        audit_log_path: str | Path | None = None,
    ) -> RunEvidenceBundle:
        run_dir = self.context.run_dir
        run_state_path = run_dir / "run_state.json"
        processing_plan_path = run_dir / "processing_plan.json"
        hash_ledger_path = run_dir / "hash_ledger.json"
        stage0_path = (
            self.context.library_path
            / "runs"
            / "stage0_rejected"
            / "stage0_rejected.jsonl"
        )
        index_manifest_path = self.context.library_path / "indexes" / "manifest.json"
        sql_preview_paths = sorted(run_dir.glob("sql_previews/*.json"))
        sql_upload_batch_paths = sorted(run_dir.glob("sql_upload_batches/*.json"))
        sql_upload_batch_paths.extend(sorted(run_dir.glob("sql_uploads/*.json")))
        output_cleaned_paths = _collect_existing_paths(
            Path(self.context.output_cleaned_dir) / "runs" / self.context.run_id,
        )
        library_paths = _collect_library_paths(self.context.library_path)

        run_state = _read_json(run_state_path)
        failed_items, pending_items = _runtime_items(run_state)
        blocked_items = _blocked_items_from_previews(sql_preview_paths)
        blocked_items.extend(_blocked_items_from_index_manifest(index_manifest_path))

        bundle = RunEvidenceBundle(
            run_id=self.context.run_id,
            generated_at=utc_now_iso(),
            report_paths=[str(path) for path in report_paths or []],
            audit_log_path=str(audit_log_path or run_dir / "audit" / "audit.jsonl"),
            run_state_path=str(run_state_path) if run_state_path.exists() else "",
            processing_plan_path=(
                str(processing_plan_path) if processing_plan_path.exists() else ""
            ),
            hash_ledger_path=str(hash_ledger_path) if hash_ledger_path.exists() else "",
            stage0_rejected_audit_path=str(stage0_path) if stage0_path.exists() else "",
            output_cleaned_paths=output_cleaned_paths,
            library_paths=library_paths,
            index_manifest_path=(
                str(index_manifest_path) if index_manifest_path.exists() else ""
            ),
            sql_preview_paths=[str(path) for path in sql_preview_paths],
            sql_upload_batch_paths=[str(path) for path in sql_upload_batch_paths],
            failed_items=failed_items,
            pending_items=pending_items,
            blocked_items=blocked_items,
        )
        bundle.checksum = _checksum(bundle)
        return bundle

    def write(
        self,
        *,
        report_paths: list[str | Path] | None = None,
        audit_log_path: str | Path | None = None,
    ) -> RunEvidenceBundle:
        bundle = self.collect(
            report_paths=report_paths,
            audit_log_path=audit_log_path,
        )
        destination = self.context.reports_dir / "evidence_bundle.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(bundle.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return bundle


def _collect_existing_paths(root: Path) -> list[str]:
    if not root.exists():
        return []
    return [
        str(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name in {"final.md", "metadata.json"}
    ]


def _collect_library_paths(library_dir: Path) -> list[str]:
    if not library_dir.exists():
        return []
    paths: list[str] = []
    for path in sorted(library_dir.rglob("*")):
        if not path.is_file():
            continue
        if _is_under(path, library_dir, "runs") or _is_under(path, library_dir, "indexes"):
            continue
        if path.suffix == ".md" or path.name.endswith(".metadata.json"):
            paths.append(str(path))
    return paths


def _runtime_items(run_state: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    failed: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for item in run_state.get("file_states", []):
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", ""))
        compact = {
            "source_path": str(item.get("source_path", "")),
            "status": status,
            "reason": str(item.get("reason", "")),
        }
        if status == "failed":
            failed.append(compact)
        elif status == "pending":
            pending.append(compact)
    return failed, pending


def _blocked_items_from_previews(paths: list[Path]) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for path in paths:
        preview = _read_json(path)
        for item in preview.get("blocked_items", []):
            if isinstance(item, dict):
                blocked.append(
                    {
                        "paper_id": str(item.get("paper_id", "")),
                        "reasons": [str(reason) for reason in item.get("reasons", [])],
                        "blocking_fields": [
                            str(field) for field in item.get("blocking_fields", [])
                        ],
                        "source": str(path),
                    },
                )
    return blocked


def _blocked_items_from_index_manifest(path: Path) -> list[dict[str, Any]]:
    manifest = _read_json(path)
    blocked: list[dict[str, Any]] = []
    for run in manifest.get("runs", []):
        if not isinstance(run, dict):
            continue
        for failure in run.get("failures", []):
            if isinstance(failure, dict):
                blocked.append(
                    {
                        "paper_id": str(failure.get("paper_id", "")),
                        "reasons": [str(failure.get("error", "index failed"))],
                        "blocking_fields": ["index_status"],
                        "source": str(path),
                    },
                )
    return blocked


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _checksum(bundle: RunEvidenceBundle) -> str:
    payload = bundle.to_dict()
    payload["checksum"] = ""
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _is_under(path: Path, root: Path, directory_name: str) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == directory_name
