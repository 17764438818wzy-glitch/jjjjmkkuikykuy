"""Generate human and machine readable run reports."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.reporting.evidence import EvidenceCollector
from paper_pipeline.reporting.report_models import (
    ReportGenerationContext,
    ReportGenerationResult,
    utc_now_iso,
)
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller

RUN_REPORT_FILENAME = "run_report.json"
LEGACY_MACHINE_REPORT_FILENAME = "machine_readable_report.json"
_PRODUCTION_LOG_FILENAMES = {
    "pipeline.json",
    "run.json",
    "run_log.json",
    "error.json",
    "error_log.json",
    "sql.json",
    "sql_log.json",
}


@dataclass(slots=True)
class _ReportArtifacts:
    run_state: dict[str, Any]
    processing_plan: dict[str, Any]
    hash_ledger: dict[str, Any]
    stage0_records: list[dict[str, Any]]
    metadata_records: list[dict[str, Any]]
    pipeline_logs: list[dict[str, Any]]
    index_manifest: dict[str, Any]
    sql_previews: list[dict[str, Any]]
    sql_upload_batches: list[dict[str, Any]]


def generate_run_report(context: ReportGenerationContext) -> ReportGenerationResult:
    ensure_real_flow_50_caller("generate_run_report", allow_test_callers=True)
    context.reports_dir.mkdir(parents=True, exist_ok=True)
    artifacts = _load_artifacts(context)
    report = _build_machine_report(context, artifacts)

    human_report_path = context.reports_dir / "human_readable_report.md"
    machine_report_path = context.reports_dir / RUN_REPORT_FILENAME
    legacy_machine_report_path = context.reports_dir / LEGACY_MACHINE_REPORT_FILENAME
    report_manifest_path = context.reports_dir / "report_manifest.json"
    evidence_bundle_path = context.reports_dir / "evidence_bundle.json"

    specialty_paths = _write_specialty_reports(context.reports_dir, report)
    all_report_paths: list[str | Path] = [
        human_report_path,
        machine_report_path,
        legacy_machine_report_path,
        report_manifest_path,
        *specialty_paths,
    ]
    machine_report_json = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    machine_report_path.write_text(machine_report_json, encoding="utf-8")
    legacy_machine_report_path.write_text(machine_report_json, encoding="utf-8")
    human_report_path.write_text(_render_human_report(report), encoding="utf-8")

    evidence_bundle = EvidenceCollector(context).write(
        report_paths=all_report_paths,
        audit_log_path=context.run_dir / "audit" / "audit.jsonl",
    )
    manifest = {
        "run_id": context.run_id,
        "created_at": report["created_at"],
        "generated_by": context.generated_by,
        "report_paths": [str(path) for path in all_report_paths],
        "human_readable_report_path": str(human_report_path),
        "run_report_path": str(machine_report_path),
        "machine_readable_report_path": str(legacy_machine_report_path),
        "evidence_bundle_path": str(evidence_bundle_path),
        "evidence_checksum": evidence_bundle.checksum,
    }
    report_manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    return ReportGenerationResult(
        run_id=context.run_id,
        reports_dir=context.reports_dir,
        human_report_path=human_report_path,
        machine_report_path=machine_report_path,
        report_manifest_path=report_manifest_path,
        evidence_bundle_path=evidence_bundle_path,
        specialty_report_paths=specialty_paths,
    )


def _load_artifacts(context: ReportGenerationContext) -> _ReportArtifacts:
    run_dir = context.run_dir
    library_dir = context.library_path
    return _ReportArtifacts(
        run_state=_read_json(run_dir / "run_state.json"),
        processing_plan=_read_json(run_dir / "processing_plan.json"),
        hash_ledger=_read_json(run_dir / "hash_ledger.json"),
        stage0_records=_read_jsonl(
            library_dir / "runs" / "stage0_rejected" / "stage0_rejected.jsonl",
            run_id=context.run_id,
        ),
        metadata_records=_load_metadata_records(library_dir, context.run_id),
        pipeline_logs=_load_pipeline_logs(Path(context.output_cleaned_dir), context.run_id),
        index_manifest=_read_json(library_dir / "indexes" / "manifest.json"),
        sql_previews=_read_json_files(run_dir / "sql_previews"),
        sql_upload_batches=(
            _read_json_files(run_dir / "sql_upload_batches")
            + _read_json_files(run_dir / "sql_uploads")
        ),
    )


def _build_machine_report(
    context: ReportGenerationContext,
    artifacts: _ReportArtifacts,
) -> dict[str, Any]:
    plan_summary = _plan_summary(context, artifacts.processing_plan)
    scan_summary = _scan_summary(artifacts.hash_ledger, artifacts.processing_plan)
    batch_summary = _batch_summary(artifacts.run_state, artifacts.processing_plan)
    stage0_summary = _stage0_summary(artifacts.stage0_records)
    pipeline_summary = _pipeline_summary(artifacts.pipeline_logs)
    classification_summary = _classification_summary(artifacts.metadata_records)
    duplicate_summary = _duplicate_summary(artifacts.hash_ledger, artifacts.metadata_records)
    index_summary = _index_summary(artifacts.index_manifest, context.run_id)
    sql_preview_summary = _sql_preview_summary(artifacts.sql_previews)
    sql_upload_summary = _sql_upload_summary(
        artifacts.sql_upload_batches,
        artifacts.metadata_records,
    )
    error_summary = _error_summary(batch_summary, index_summary, sql_upload_summary)
    blocking_summary = _blocking_reason_summary(sql_preview_summary, index_summary)
    risk_summary = _risk_flag_summary(artifacts.metadata_records)

    return {
        "run_id": context.run_id,
        "created_at": utc_now_iso(),
        "generated_by": context.generated_by,
        "overall_status": _overall_status(batch_summary, index_summary),
        "input_dir": str(context.input_dir),
        "library_dir": str(context.library_dir),
        "output_cleaned_dir": str(context.output_cleaned_dir),
        "plan_summary": plan_summary,
        "scan_summary": scan_summary,
        "batch_summary": batch_summary,
        "stage0_summary": stage0_summary,
        "archive_summary": _archive_summary(stage0_summary),
        "pipeline_summary": pipeline_summary,
        "classification_summary": classification_summary,
        "duplicate_summary": duplicate_summary,
        "pending_review_summary": _pending_review_summary(artifacts.metadata_records),
        "index_summary": index_summary,
        "sql_preview_summary": sql_preview_summary,
        "sql_upload_summary": sql_upload_summary,
        "error_summary": error_summary,
        "blocking_reason_summary": blocking_summary,
        "risk_flag_summary": risk_summary,
        "evidence_paths": _evidence_paths(context),
        "next_suggested_actions": _next_actions(
            batch_summary,
            index_summary,
            sql_preview_summary,
            duplicate_summary,
            classification_summary,
        ),
    }


def _plan_summary(
    context: ReportGenerationContext,
    plan: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": str(plan.get("run_id", context.run_id)),
        "status": str(plan.get("status", "")),
        "batch_count": int(plan.get("batch_count", len(plan.get("batches", [])))),
        "batch_file_counts": [
            int(batch.get("file_count", 0))
            for batch in plan.get("batches", [])
            if isinstance(batch, dict)
        ],
        "confirmed": str(plan.get("status", "")) == "confirmed",
        "cancelled": str(plan.get("status", "")) == "cancelled",
        "can_execute": str(plan.get("status", "")) == "confirmed",
        "will_write_sql": bool(plan.get("will_write_sql", False)),
        "requires_user_sql_confirmation": bool(
            plan.get("requires_user_sql_confirmation", True),
        ),
        "input_dir": str(plan.get("input_dir", context.input_dir)),
        "library_dir": str(plan.get("library_dir", context.library_dir)),
        "output_cleaned_dir": str(
            plan.get("output_cleaned_dir", context.output_cleaned_dir),
        ),
    }


def _scan_summary(
    ledger: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any]:
    summary = ledger.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    records = [item for item in ledger.get("records", []) if isinstance(item, dict)]
    exact = sum(
        1
        for item in records
        if str(item.get("duplicate_status", "")) == "exact_duplicate"
        or str(item.get("skip_reason", "")) == "duplicate_skip"
    )
    suspected = int(summary.get("suspected_duplicate_files", 0))
    return {
        "total_files": int(
            summary.get("total_files", plan.get("total_scanned_count", len(records))),
        ),
        "new_file_count": int(summary.get("unique_files", plan.get("new_file_count", 0))),
        "exact_duplicate_count": int(summary.get("duplicate_files", exact)),
        "suspected_duplicate_count": suspected,
        "skipped_count": int(summary.get("skipped_files", exact)),
        "hash_ledger_record_count": len(records),
        "source_hashes": [str(item.get("source_hash", "")) for item in records],
        "source_paths": [str(item.get("source_path", "")) for item in records],
    }


def _batch_summary(
    run_state: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any]:
    batch_states = [
        item for item in run_state.get("batch_states", []) if isinstance(item, dict)
    ]
    file_states = [
        item for item in run_state.get("file_states", []) if isinstance(item, dict)
    ]
    return {
        "run_status": str(run_state.get("status", "")),
        "batch_statuses": [str(item.get("status", "")) for item in batch_states],
        "processed_count": int(
            run_state.get(
                "completed_items",
                0,
            )
        )
        + int(run_state.get("pending_items", 0))
        + int(run_state.get("failed_items", 0)),
        "completed_count": int(run_state.get("completed_items", 0)),
        "pending_count": int(run_state.get("pending_items", 0)),
        "failed_count": int(run_state.get("failed_items", 0)),
        "paused": str(run_state.get("status", "")) == "paused",
        "stopped": str(run_state.get("status", "")) == "stopped",
        "interrupted": str(run_state.get("status", "")) == "interrupted",
        "abandoned": str(run_state.get("status", "")) == "abandoned",
        "temporary_artifact_count": len(run_state.get("temporary_artifacts", [])),
        "completed_artifact_count": len(run_state.get("completed_artifacts", [])),
        "planned_batch_count": int(plan.get("batch_count", len(plan.get("batches", [])))),
        "pending_items": _items_by_status(file_states, "pending"),
        "failed_items": _items_by_status(file_states, "failed"),
    }


def _stage0_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    decisions = Counter(str(item.get("decision", "")) for item in records)
    failure_reasons = Counter(
        str(item.get("reason", ""))
        for item in records
        if "failed" in str(item.get("reason", "")).lower()
    )
    return {
        "archive_extraction_success_count": decisions.get("accept", 0),
        "archive_extraction_failed_count": sum(failure_reasons.values()),
        "zip_supported": True,
        "rar_supported": True,
        "seven_z_supported": True,
        "stage0_accept_count": decisions.get("accept", 0),
        "stage0_reject_count": decisions.get("stage0_reject", 0),
        "stage0_pending_count": decisions.get("pending", 0),
        "stage0_rejected_items": records,
        "extraction_failure_reasons": dict(failure_reasons),
    }


def _archive_summary(stage0_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "success_count": stage0_summary["archive_extraction_success_count"],
        "failed_count": stage0_summary["archive_extraction_failed_count"],
        "zip_supported": stage0_summary["zip_supported"],
        "rar_supported": stage0_summary["rar_supported"],
        "seven_z_supported": stage0_summary["seven_z_supported"],
        "failure_reasons": stage0_summary["extraction_failure_reasons"],
    }


def _pipeline_summary(logs: list[dict[str, Any]]) -> dict[str, Any]:
    by_stage: dict[str, Counter[str]] = {}
    failed_stage_by_file: dict[str, str] = {}
    pending_reason_by_file: dict[str, str] = {}
    paths: dict[str, list[str]] = {
        "output_cleaned_paths": [],
        "final_md_paths": [],
        "metadata_json_paths": [],
        "log_paths": [],
    }
    for log in logs:
        path = str(log.get("_path", ""))
        if path:
            paths["log_paths"].append(path)
        for stage in log.get("stages", []):
            if not isinstance(stage, dict):
                continue
            name = str(stage.get("stage", "unknown"))
            status = str(stage.get("status", "unknown"))
            by_stage.setdefault(name, Counter())[status] += 1
            source = str(log.get("source_path", log.get("paper_id", path)))
            if status in {"failed", "timeout"}:
                failed_stage_by_file[source] = name
            if status == "pending":
                pending_reason_by_file[source] = str(stage.get("reason", "pending"))
    return {
        "stage_counts": {stage: dict(counter) for stage, counter in by_stage.items()},
        "failed_by_stage": {
            stage: counter.get("failed", 0) + counter.get("timeout", 0)
            for stage, counter in by_stage.items()
        },
        "failed_stage_by_file": failed_stage_by_file,
        "pending_reason_by_file": pending_reason_by_file,
        **paths,
    }


def _classification_summary(metadata_records: list[dict[str, Any]]) -> dict[str, Any]:
    tags: Counter[str] = Counter()
    for item in metadata_records:
        tags.update(str(tag) for tag in item.get("research_method_tags", []))
    return {
        "category_distribution": _counter(metadata_records, "category"),
        "direction_distribution": _counter(metadata_records, "direction"),
        "research_method_primary_distribution": _counter(
            metadata_records,
            "research_method_primary",
        ),
        "research_method_tags_distribution": dict(tags),
        "tier_distribution": _counter(metadata_records, "tier"),
        "uncertain_classification_count": sum(
            1
            for item in metadata_records
            if not item.get("category") or not item.get("direction")
        ),
        "needs_review_count": _needs_review_count(metadata_records),
        "risk_flags_distribution": _risk_flag_summary(metadata_records),
    }


def _duplicate_summary(
    ledger: dict[str, Any],
    metadata_records: list[dict[str, Any]],
) -> dict[str, Any]:
    ledger_records = [item for item in ledger.get("records", []) if isinstance(item, dict)]
    exact = sum(
        1
        for item in ledger_records
        if str(item.get("duplicate_status", "")) == "exact_duplicate"
        or str(item.get("skip_reason", "")) == "duplicate_skip"
    )
    statuses = Counter(str(item.get("duplicate_status", "")) for item in metadata_records)
    suspected = statuses.get("suspected_duplicate", 0)
    return {
        "exact_duplicate_count": exact + statuses.get("exact_duplicate", 0),
        "suspected_duplicate_count": suspected,
        "unique_count": statuses.get("unique", 0),
        "pending_review_count": sum(
            1
            for item in metadata_records
            if str(item.get("tier", "")) == "pending"
            or str(item.get("duplicate_status", "")) == "suspected_duplicate"
        ),
        "pending_review_applied_count": _review_action_count(
            metadata_records,
            "pending_review_applied",
        ),
        "review_history_event_count": sum(
            len(item.get("review_history", [])) for item in metadata_records
        ),
        "index_update_required_count": sum(
            1 for item in metadata_records if item.get("index_update_required")
        ),
        "needs_rebuild_count": sum(
            1
            for item in metadata_records
            if str(item.get("index_status", "")) == "needs_rebuild"
        ),
    }


def _pending_review_summary(metadata_records: list[dict[str, Any]]) -> dict[str, Any]:
    pending = [
        item
        for item in metadata_records
        if str(item.get("tier", "")) == "pending"
        or str(item.get("duplicate_status", "")) == "suspected_duplicate"
    ]
    return {
        "pending_review_count": len(pending),
        "pending_review_applied_count": _review_action_count(
            metadata_records,
            "pending_review_applied",
        ),
        "items": [
            {
                "paper_id": f"{item.get('id', '')}_v{item.get('version', 1)}",
                "duplicate_status": str(item.get("duplicate_status", "")),
            }
            for item in pending
        ],
    }


def _index_summary(manifest: dict[str, Any], run_id: str) -> dict[str, Any]:
    runs = [item for item in manifest.get("runs", []) if isinstance(item, dict)]
    matching = [item for item in runs if str(item.get("run_id", "")) == run_id]
    selected = matching[-1] if matching else (runs[-1] if runs else {})
    skipped = [item for item in selected.get("skipped", []) if isinstance(item, dict)]
    failures = [item for item in selected.get("failures", []) if isinstance(item, dict)]
    return {
        "manifest_present": bool(manifest),
        "indexed_count": int(selected.get("success_count", 0)),
        "index_failed_count": int(selected.get("failure_count", len(failures))),
        "needs_rebuild_count": _reason_count(skipped, "needs_rebuild"),
        "skipped_index_count": int(selected.get("skipped_count", len(skipped))),
        "skipped_reasons": _skip_reasons(skipped),
        "failures": failures,
        "sql_eligible_count": int(selected.get("sql_eligible_count", 0)),
        "sql_blocked_count": int(selected.get("sql_blocked_count", 0)),
        "ready_for_sql_preview_count": int(selected.get("ready_for_sql_preview_count", 0)),
    }


def _sql_preview_summary(previews: list[dict[str, Any]]) -> dict[str, Any]:
    if not previews:
        return {
            "preview_id": "",
            "preview_checksum": "",
            "candidate_count": 0,
            "eligible_count": 0,
            "blocked_count": 0,
            "confirmed_by": "",
            "confirmation_status": "not_confirmed",
            "blocking_fields": {},
            "blocked_items": [],
        }
    latest = previews[-1]
    blocking_fields: Counter[str] = Counter()
    for item in latest.get("blocked_items", []):
        if isinstance(item, dict):
            blocking_fields.update(str(field) for field in item.get("blocking_fields", []))
    return {
        "preview_id": str(latest.get("preview_id", "")),
        "preview_checksum": str(latest.get("preview_checksum", "")),
        "candidate_count": int(latest.get("candidate_count", 0)),
        "eligible_count": int(latest.get("eligible_count", 0)),
        "blocked_count": int(latest.get("blocked_count", 0)),
        "confirmed_by": str(latest.get("confirmed_by", "")),
        "confirmation_status": str(latest.get("confirmation_status", "not_confirmed")),
        "blocking_fields": dict(blocking_fields),
        "blocked_items": latest.get("blocked_items", []),
    }


def _sql_upload_summary(
    batches: list[dict[str, Any]],
    metadata_records: list[dict[str, Any]],
) -> dict[str, Any]:
    uploaded_status = sum(
        1 for item in metadata_records if str(item.get("sql_status", "")) == "uploaded"
    )
    failed_status = sum(
        1 for item in metadata_records if str(item.get("sql_status", "")) == "failed"
    )
    not_uploaded_status = sum(
        1
        for item in metadata_records
        if str(item.get("sql_status", "")) == "not_uploaded"
    )
    uploaded_count = sum(int(batch.get("uploaded_count", 0)) for batch in batches)
    failed_count = sum(int(batch.get("failed_upload_count", 0)) for batch in batches)
    skipped_count = sum(int(batch.get("skipped_upload_count", 0)) for batch in batches)
    latest = batches[-1] if batches else {}
    errors = []
    for batch in batches:
        for item in batch.get("failed_items", []):
            if isinstance(item, dict):
                errors.append(str(item.get("error", "")))
    return {
        "upload_batch_id": str(latest.get("upload_batch_id", latest.get("sql_batch_id", ""))),
        "confirmed_by": str(latest.get("confirmed_by", "")),
        "confirmation_status": str(latest.get("confirmation_status", "")),
        "uploaded_count": uploaded_count or uploaded_status,
        "failed_upload_count": failed_count or failed_status,
        "skipped_upload_count": skipped_count,
        "sql_status_distribution": {
            "uploaded": uploaded_status,
            "failed": failed_status,
            "not_uploaded": not_uploaded_status,
        },
        "upload_error_reasons": errors
        or [
            str(item.get("sql_upload_error", ""))
            for item in metadata_records
            if item.get("sql_upload_error")
        ],
        "sql_audit_history_count": sum(
            len(item.get("sql_audit_history", [])) for item in metadata_records
        ),
    }


def _error_summary(
    batch_summary: dict[str, Any],
    index_summary: dict[str, Any],
    sql_upload_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "file_failed_count": batch_summary["failed_count"],
        "file_failed_items": batch_summary["failed_items"],
        "index_failed_count": index_summary["index_failed_count"],
        "index_failures": index_summary["failures"],
        "sql_upload_failed_count": sql_upload_summary["failed_upload_count"],
        "sql_upload_errors": sql_upload_summary["upload_error_reasons"],
    }


def _blocking_reason_summary(
    sql_preview_summary: dict[str, Any],
    index_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "sql_blocking_fields": sql_preview_summary["blocking_fields"],
        "index_skipped_reasons": index_summary["skipped_reasons"],
    }


def _risk_flag_summary(metadata_records: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in metadata_records:
        for flag in item.get("risk_flags", []):
            if isinstance(flag, dict):
                counter[str(flag.get("code", ""))] += 1
            else:
                counter[str(flag)] += 1
    return dict(counter)


def _evidence_paths(context: ReportGenerationContext) -> dict[str, str]:
    run_dir = context.run_dir
    paths = {
        "run_state_path": run_dir / "run_state.json",
        "processing_plan_path": run_dir / "processing_plan.json",
        "hash_ledger_path": run_dir / "hash_ledger.json",
        "stage0_rejected_audit_path": (
            context.library_path / "runs" / "stage0_rejected" / "stage0_rejected.jsonl"
        ),
        "index_manifest_path": context.library_path / "indexes" / "manifest.json",
    }
    return {key: str(path) for key, path in paths.items() if path.exists()}


def _next_actions(
    batch_summary: dict[str, Any],
    index_summary: dict[str, Any],
    sql_preview_summary: dict[str, Any],
    duplicate_summary: dict[str, Any],
    classification_summary: dict[str, Any],
) -> list[str]:
    actions: list[str] = []
    if batch_summary["pending_count"] or duplicate_summary["pending_review_count"]:
        actions.append("Resolve pending review items before downstream upload.")
    if duplicate_summary["suspected_duplicate_count"]:
        actions.append("Review suspected duplicate items.")
    if index_summary["index_failed_count"]:
        actions.append("Inspect failed index items and rebuild indexes.")
    if sql_preview_summary["blocked_count"]:
        actions.append("Resolve SQL blocking fields and regenerate SQL preview.")
    if classification_summary["needs_review_count"]:
        actions.append("Review uncertain classification or risk flags.")
    if not actions:
        actions.append("No blocking follow-up detected.")
    return actions


def _overall_status(
    batch_summary: dict[str, Any],
    index_summary: dict[str, Any],
) -> str:
    if batch_summary["failed_count"] or index_summary["index_failed_count"]:
        return "completed_with_issues"
    if batch_summary["pending_count"]:
        return "completed_with_pending"
    return batch_summary["run_status"] or "unknown"


def _write_specialty_reports(reports_dir: Path, report: dict[str, Any]) -> list[Path]:
    specs = {
        "stage0_rejected_report.md": report["stage0_summary"],
        "duplicate_skip_report.md": report["duplicate_summary"],
        "pending_review_report.md": report["pending_review_summary"],
        "index_report.md": report["index_summary"],
        "sql_upload_report.md": report["sql_upload_summary"],
        "classification_change_report.md": report["classification_summary"],
        "abandoned_tasks_report.md": {"abandoned": report["batch_summary"]["abandoned"]},
        "sql_withdrawal_report.md": _sql_withdrawal_summary(report),
    }
    paths: list[Path] = []
    for filename, payload in specs.items():
        path = reports_dir / filename
        title = filename.removesuffix(".md").replace("_", " ").title()
        path.write_text(
            f"# {title}\n\n```json\n"
            f"{json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)}\n"
            "```\n",
            encoding="utf-8",
        )
        paths.append(path)
    return paths


def _sql_withdrawal_summary(report: dict[str, Any]) -> dict[str, Any]:
    distribution = report["sql_upload_summary"]["sql_status_distribution"]
    return {"withdrawn_count": int(distribution.get("withdrawn", 0))}


def _render_human_report(report: dict[str, Any]) -> str:
    pending = report["batch_summary"]["pending_count"]
    failed = report["batch_summary"]["failed_count"]
    sql_blocked = report["sql_preview_summary"]["blocked_count"]
    index_failed = report["index_summary"]["index_failed_count"]
    suspected = report["duplicate_summary"]["suspected_duplicate_count"]
    needs_review = report["classification_summary"]["needs_review_count"]
    sql_uploaded = report["sql_upload_summary"]["uploaded_count"]
    lines = [
        "# Run Report",
        "",
        f"- run_id: {report['run_id']}",
        f"- overall_status: {report['overall_status']}",
        f"- pending: {pending}",
        f"- failed: {failed}",
        f"- SQL blocked: {sql_blocked}",
        f"- index failed: {index_failed}",
        f"- suspected duplicate: {suspected}",
        f"- needs review: {needs_review}",
        f"- SQL uploaded: {sql_uploaded}",
        "",
        "## Next Suggested Actions",
        "",
    ]
    lines.extend(f"- {action}" for action in report["next_suggested_actions"])
    lines.append("")
    return "\n".join(lines)


def _load_metadata_records(library_dir: Path, run_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not library_dir.exists():
        return records
    metadata_paths = {
        *library_dir.rglob("*.metadata.json"),
        *library_dir.rglob("metadata.json"),
    }
    for path in sorted(metadata_paths):
        if _is_under(path, library_dir, "runs") or _is_under(path, library_dir, "indexes"):
            continue
        try:
            metadata = metadata_from_json(path.read_text(encoding="utf-8"))
            record = json.loads(
                json.dumps(
                    _metadata_to_dict(metadata),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
            record = _read_json(path)
        if run_id and record.get("run_id") not in {"", run_id}:
            continue
        record["_metadata_path"] = str(path)
        records.append(record)
    return records


def _metadata_to_dict(metadata: object) -> dict[str, Any]:
    from paper_pipeline.domain.serialization import metadata_to_dict

    return metadata_to_dict(metadata)  # type: ignore[arg-type]


def _load_pipeline_logs(output_cleaned_dir: Path, run_id: str) -> list[dict[str, Any]]:
    run_dir = output_cleaned_dir / "runs" / run_id
    logs: list[dict[str, Any]] = []
    if not run_dir.exists():
        return logs
    for path in sorted(run_dir.rglob("*.json")):
        if not _is_production_log_path(path, run_dir):
            continue
        payload = _read_json(path)
        if "stages" in payload:
            payload["_path"] = str(path)
            logs.append(payload)
    return logs


def _is_production_log_path(path: Path, run_dir: Path) -> bool:
    if path.name not in _PRODUCTION_LOG_FILENAMES:
        return False
    if path.parent == run_dir:
        return True
    return path.parent.name == "logs"


def _read_json_files(directory: Path) -> list[dict[str, Any]]:
    if not directory.exists():
        return []
    return [_read_json(path) for path in sorted(directory.glob("*.json"))]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _read_jsonl(path: Path, *, run_id: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            decoded = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(decoded, dict):
            continue
        if run_id and decoded.get("run_id") not in {"", run_id}:
            continue
        records.append(decoded)
    return records


def _items_by_status(file_states: list[dict[str, Any]], status: str) -> list[dict[str, str]]:
    return [
        {
            "source_path": str(item.get("source_path", "")),
            "reason": str(item.get("reason", "")),
        }
        for item in file_states
        if str(item.get("status", "")) == status
    ]


def _counter(records: list[dict[str, Any]], field_name: str) -> dict[str, int]:
    return dict(Counter(str(item.get(field_name, "")) for item in records if item.get(field_name)))


def _needs_review_count(records: list[dict[str, Any]]) -> int:
    count = 0
    for item in records:
        if str(item.get("tier", "")) == "pending":
            count += 1
            continue
        if item.get("risk_flags"):
            count += 1
            continue
        history = item.get("review_history", [])
        if any("review" in str(entry.get("action", "")) for entry in history if isinstance(entry, dict)):
            count += 1
    return count


def _review_action_count(records: list[dict[str, Any]], action: str) -> int:
    count = 0
    for item in records:
        for entry in item.get("review_history", []):
            if isinstance(entry, dict) and entry.get("action") == action:
                count += 1
    return count


def _reason_count(skipped: list[dict[str, Any]], needle: str) -> int:
    return sum(
        1
        for item in skipped
        for reason in item.get("reasons", [])
        if needle in str(reason)
    )


def _skip_reasons(skipped: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in skipped:
        counter.update(str(reason) for reason in item.get("reasons", []))
    return dict(counter)


def _is_under(path: Path, root: Path, directory_name: str) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == directory_name
