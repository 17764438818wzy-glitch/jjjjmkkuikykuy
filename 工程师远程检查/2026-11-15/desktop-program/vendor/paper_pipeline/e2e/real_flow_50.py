"""Task 16 real-flow 50-file evaluation runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.agents import (
    CategoryAgent,
    DirectionAgent,
    QualityTierAgent,
    ResearchMethodAgent,
)
from paper_pipeline.audit.audit_log import AuditEvent, AuditEventType, AuditLogWriter
from paper_pipeline.domain import (
    AgentResult,
    DuplicateStatus,
    IndexStatus,
    PaperMetadata,
    ReviewHistoryItem,
    RiskFlag,
    RunState,
    Stage0Decision,
    Tier,
)
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.duplicate.detector import (
    DuplicateCandidate,
    apply_duplicate_detection_to_metadata,
    detect_duplicate,
)
from paper_pipeline.e2e.checklist import evaluate_issue_lists, overall_conclusion
from paper_pipeline.e2e.evaluation import default_metrics, write_evaluation_outputs
from paper_pipeline.indexing.index_builder import build_formal_library_indexes
from paper_pipeline.ingestion.archive_extractor import extract_archive, is_supported_archive
from paper_pipeline.pipeline import LocalDityPipeline, PipelineStageName, PipelineStageStatus
from paper_pipeline.planning.processing_plan import processing_plan_to_json
from paper_pipeline.planning.service import (
    confirm_processing_plan,
    ensure_runs_dir_outside_input_dir,
    generate_processing_plan,
)
from paper_pipeline.reporting.report_generator import (
    ReportGenerationContext,
    generate_run_report,
)
from paper_pipeline.runtime import BatchRunner, FileProcessingResult
from paper_pipeline.runtime.batch_runner import create_real_flow_50_batch_authority
from paper_pipeline.runtime.recovery import detect_interrupted_run
from paper_pipeline.runtime.run_state import (
    authorize_run_state_write,
    run_state_to_dict,
    save_run_state,
    validate_run_state_consistency,
)
from paper_pipeline.sql.preview import SqlUploadPreviewService
from paper_pipeline.sql.postgres_adapter import PostgresSqlUploadAdapter
from paper_pipeline.sql.sql_models import (
    SqlUploadConfirmation,
    calculate_preview_checksum,
)
from paper_pipeline.sql.upload_adapter import FakeSqlUploadAdapter
from paper_pipeline.sql.upload_service import SqlUploadConfirmationResult, SqlUploadService
from paper_pipeline.sql.write_authorization import authorize_sql_write_from_real_flow_50
from paper_pipeline.stage0.admission import (
    Stage0Result,
    admit_extracted_members,
    admit_file,
)
from paper_pipeline.storage.hash_ledger import save_hash_ledger
from paper_pipeline.storage.id_allocator import FormalPaperIdAllocator
from paper_pipeline.storage.library_repository import LibraryArchiveResult, LibraryRepository
from paper_pipeline.storage.scanner import (
    ScanResult,
    ScanSummary,
    calculate_file_hash,
    scan_input_raw,
)


@dataclass(slots=True)
class RealFlow50Config:
    input_dir: str | Path | None = None
    library_dir: str | Path | None = None
    output_cleaned_dir: str | Path | None = None
    source_dir: str | Path | None = None
    run_id: str = ""
    mode: str = "dry-run-sql"
    copy_sample: bool = False
    generate_sample: bool = False
    expected_file_count: int = 50
    max_file_count: int = 0
    allow_less_than_50: bool = False
    confirm_upload: bool = True
    created_by: str = "codex-task-16"
    sql_fail_paper_ids: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class RealFlow50Result:
    run_id: str
    input_dir: Path
    library_dir: Path
    output_cleaned_dir: Path
    metrics: dict[str, Any]
    evaluation_report_md_path: Path
    evaluation_report_json_path: Path
    e2e_run_manifest_path: Path
    general_report_manifest_path: Path
    evidence_bundle_path: Path


@dataclass(slots=True)
class _ProcessedPaper:
    metadata: PaperMetadata
    body: str
    archive_result: LibraryArchiveResult
    pipeline_status: PipelineStageStatus


class _RealFlowProcessor:
    def __init__(
        self,
        *,
        run_id: str,
        input_dir: Path,
        library_dir: Path,
        output_cleaned_dir: Path,
        run_dir: Path,
        created_by: str,
    ) -> None:
        self.run_id = run_id
        self.input_dir = input_dir
        self.library_dir = library_dir
        self.output_cleaned_dir = output_cleaned_dir
        self.run_dir = run_dir
        self.created_by = created_by
        self.work_dir = (run_dir / "work").resolve()
        self.stage0_audit_dir = library_dir / "runs" / "stage0_rejected"
        self.stage0_decisions_path = run_dir / "stage0_decisions.jsonl"
        self.pipeline = LocalDityPipeline(output_cleaned_dir=output_cleaned_dir)
        self.id_allocator = FormalPaperIdAllocator(library_dir)
        self.library_repository = LibraryRepository(library_dir)
        self.audit_writer = AuditLogWriter(run_dir / "audit" / "audit.jsonl")
        self.processed_papers: list[_ProcessedPaper] = []
        self.stage0_results: list[Stage0Result] = []
        self.pipeline_results: list[Any] = []
        self.agent_results: list[AgentResult] = []
        self.metadata_validation_failed_count = 0

    def process_file(self, source_path: str) -> FileProcessingResult:
        path = Path(source_path)
        if is_supported_archive(path):
            return self._process_archive(path)
        return self._process_single_path(path, source_path_for_runtime=source_path)

    def _process_archive(self, archive_path: Path) -> FileProcessingResult:
        extraction = extract_archive(
            archive_path,
            work_dir=self.work_dir,
            run_id=self.run_id,
        )
        if not extraction.success:
            source_hash = calculate_file_hash(archive_path) if archive_path.exists() else ""
            result = Stage0Result(
                source_path=str(archive_path),
                source_hash=source_hash,
                decision=Stage0Decision.PENDING,
                reason=extraction.error_reason,
                confidence=0.0,
                evidence=["archive_extraction_failed"],
                run_id=self.run_id,
            )
            self._record_stage0_result(result)
            self._audit(
                AuditEventType.FILE_PENDING,
                target_type="source_file",
                target_id=str(archive_path),
                status="pending",
                message=extraction.error_reason,
            )
            return FileProcessingResult.pending(
                str(archive_path),
                reason=extraction.error_reason,
            )

        stage0_results = admit_extracted_members(
            extraction,
            run_id=self.run_id,
            audit_dir=self.stage0_audit_dir,
        )
        accepted_artifacts: list[str] = []
        pending_reasons: list[str] = []
        for stage0_result in stage0_results:
            self._record_stage0_result(stage0_result)
            if stage0_result.decision is Stage0Decision.PENDING:
                pending_reasons.append(stage0_result.reason)
                continue
            if stage0_result.decision is Stage0Decision.STAGE0_REJECT:
                self._audit(
                    AuditEventType.STAGE0_REJECTED,
                    target_type="source_file",
                    target_id=stage0_result.source_path,
                    status="stage0_reject",
                    message=stage0_result.reason,
                )
                continue
            result = self._run_pipeline_for_stage0(
                stage0_result,
                runtime_source_path=str(archive_path),
            )
            if result.status is not PipelineStageStatus.SUCCESS:
                pending_reasons.append(result.pending_reason or result.error)
            elif result.artifacts is not None:
                accepted_artifacts.append(str(result.artifacts.final_markdown))

        if pending_reasons:
            reason = "archive member pending: " + "; ".join(pending_reasons)
            return FileProcessingResult.pending(str(archive_path), reason=reason)
        return FileProcessingResult.completed(
            str(archive_path),
            artifact_uri=accepted_artifacts[0] if accepted_artifacts else "",
        )

    def _process_single_path(
        self,
        path: Path,
        *,
        source_path_for_runtime: str,
    ) -> FileProcessingResult:
        stage0_result = admit_file(
            path,
            run_id=self.run_id,
            audit_dir=self.stage0_audit_dir,
        )
        self._record_stage0_result(stage0_result)
        if stage0_result.decision is Stage0Decision.PENDING:
            self._audit(
                AuditEventType.FILE_PENDING,
                target_type="source_file",
                target_id=str(path),
                status="pending",
                message=stage0_result.reason,
            )
            return FileProcessingResult.pending(
                source_path_for_runtime,
                reason=f"stage0 pending: {stage0_result.reason}",
            )
        if stage0_result.decision is Stage0Decision.STAGE0_REJECT:
            self._audit(
                AuditEventType.STAGE0_REJECTED,
                target_type="source_file",
                target_id=str(path),
                status="stage0_reject",
                message=stage0_result.reason,
            )
            return FileProcessingResult.completed(source_path_for_runtime)

        result = self._run_pipeline_for_stage0(
            stage0_result,
            runtime_source_path=source_path_for_runtime,
        )
        if result.status is not PipelineStageStatus.SUCCESS:
            return FileProcessingResult.pending(
                source_path_for_runtime,
                reason=result.pending_reason or result.error,
                elapsed_seconds=result.duration_seconds,
            )
        artifact_uri = ""
        if result.artifacts is not None:
            artifact_uri = str(result.artifacts.final_markdown)
        return FileProcessingResult.completed(
            source_path_for_runtime,
            elapsed_seconds=result.duration_seconds,
            artifact_uri=artifact_uri,
        )

    def _run_pipeline_for_stage0(
        self,
        stage0_result: Stage0Result,
        *,
        runtime_source_path: str,
    ):
        pipeline_result = self.pipeline.run_stage0_result(
            stage0_result,
            run_id=self.run_id,
        )
        self.pipeline_results.append(pipeline_result)
        self._write_pipeline_log(pipeline_result, runtime_source_path)
        if pipeline_result.status is not PipelineStageStatus.SUCCESS:
            return pipeline_result

        if pipeline_result.artifacts is None:
            return pipeline_result

        try:
            metadata = self._build_metadata(
                stage0_result=stage0_result,
                final_markdown=pipeline_result.artifacts.final_markdown,
            )
            archive_result = self.library_repository.archive_markdown(
                pipeline_result.artifacts.final_markdown,
                metadata,
            )
            self.processed_papers.append(
                _ProcessedPaper(
                    metadata=metadata,
                    body=pipeline_result.artifacts.final_markdown.read_text(
                        encoding="utf-8",
                    ),
                    archive_result=archive_result,
                    pipeline_status=pipeline_result.status,
                ),
            )
            self._audit(
                AuditEventType.METADATA_WRITTEN,
                target_type="paper",
                target_id=f"{metadata.id}_v{metadata.version}",
                status="written",
                message="metadata and final.md archived",
                evidence=[str(archive_result.metadata_path), str(archive_result.markdown_path)],
            )
        except Exception as exc:
            self.metadata_validation_failed_count += 1
            pipeline_result.pending_reason = str(exc)
            pipeline_result.status = PipelineStageStatus.PENDING
        return pipeline_result

    def _build_metadata(
        self,
        *,
        stage0_result: Stage0Result,
        final_markdown: Path,
    ) -> PaperMetadata:
        identity = self.id_allocator.allocate(source_hash=stage0_result.source_hash)
        text_profile = _paper_text_profile(stage0_result.source_path)
        keywords = text_profile["keywords"]
        metadata = PaperMetadata(
            id=identity.id,
            version=identity.version,
            title=str(text_profile["title"]),
            year=2026,
            school="Task 16 Local Evaluation",
            major="软件工程",
            abstract=str(text_profile["abstract"]),
            keywords=[str(keyword) for keyword in keywords]
            if isinstance(keywords, list)
            else [],
            chapter_types=[
                "摘要",
                "绪论",
                "文献综述",
                "系统设计",
                "结论",
                "参考文献",
            ],
            sensitive_clean=True,
            source_hash=stage0_result.source_hash,
            source_path=stage0_result.source_path,
            run_id=self.run_id,
        )

        category = CategoryAgent().run("task16-category", metadata)
        self.agent_results.append(category)
        if category.status == "success":
            metadata.category = str(category.decision.get("category", ""))

        direction = DirectionAgent().run("task16-direction", metadata)
        self.agent_results.append(direction)
        if direction.status == "success":
            metadata.direction = str(direction.decision.get("direction", ""))

        method = ResearchMethodAgent().run("task16-method", metadata)
        self.agent_results.append(method)
        if method.decision:
            method_tags = method.decision.get("research_method_tags", [])
            suggested_tags = method.decision.get("suggested_tags", [])
            metadata.research_method_primary = str(
                method.decision.get("research_method_primary", ""),
            )
            metadata.research_method_tags = [
                str(tag) for tag in method_tags
            ] if isinstance(method_tags, list) else []
            metadata.suggested_tags = [
                str(tag) for tag in suggested_tags
            ] if isinstance(suggested_tags, list) else []

        quality = QualityTierAgent().run("task16-quality", metadata)
        self.agent_results.append(quality)
        if quality.decision:
            score = quality.decision.get("score", 0)
            metadata.score = float(score) if isinstance(score, (int, float, str)) else 0.0
            metadata.quality_reason = str(quality.decision.get("quality_reason", ""))
            _apply_automatic_quality_tier_decision(metadata, quality.decision)

        _apply_llm_initial_review_decision(
            metadata,
            _llm_quality_payload_for(final_markdown),
        )

        body = final_markdown.read_text(encoding="utf-8")
        duplicate_result = detect_duplicate(
            DuplicateCandidate(metadata=metadata, body=body),
            [
                DuplicateCandidate(metadata=paper.metadata, body=paper.body)
                for paper in self.processed_papers
            ],
            suspected_threshold=0.999,
        )
        apply_duplicate_detection_to_metadata(metadata, duplicate_result)
        if duplicate_result.requires_pending_review:
            metadata.tier = Tier.PENDING
            metadata.quality_tier_suggestion = Tier.PENDING.value
            metadata.gold_candidate = False
            metadata.needs_codex_review = True
            metadata.tier_decision_source = "duplicate_risk_pending"
            metadata.quality_reason = f"deepseek_uncertain: {duplicate_result.reason}"
            metadata.risk_flags.append(
                RiskFlag(
                    code="suspected_duplicate",
                    message=duplicate_result.reason,
                ),
            )
            metadata.add_review_history(
                ReviewHistoryItem(
                    reviewer="task16-e2e",
                    action="pending_review_required",
                    comment=duplicate_result.reason,
                    duplicate_status=metadata.duplicate_status.value,
                    similar_to=list(metadata.similar_to),
                ),
            )
        return metadata

    def _record_stage0_result(self, result: Stage0Result) -> None:
        self.stage0_results.append(result)
        self.stage0_decisions_path.parent.mkdir(parents=True, exist_ok=True)
        with self.stage0_decisions_path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "run_id": result.run_id,
                        "source_path": result.source_path,
                        "source_hash": result.source_hash,
                        "decision": result.decision.value,
                        "reason": result.reason,
                        "confidence": result.confidence,
                        "evidence": list(result.evidence),
                        "created_at": result.created_at.isoformat(),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            handle.write("\n")

    def _write_pipeline_log(self, pipeline_result: Any, runtime_source_path: str) -> None:
        if pipeline_result.artifacts is None:
            return
        path = pipeline_result.artifacts.logs_dir / "pipeline.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "run_id": self.run_id,
                    "source_path": runtime_source_path,
                    "paper_id": pipeline_result.accession_id,
                    "stage0_decision": pipeline_result.stage0_decision.value,
                    "status": pipeline_result.status.value,
                    "pending_reason": pipeline_result.pending_reason,
                    "stages": [
                        {
                            "stage": stage.stage_name.value,
                            "status": stage.status.value,
                            "input_path": stage.input_path,
                            "output_path": stage.output_path,
                            "reason": stage.error,
                            "logs": list(stage.logs),
                            "risk_flags": list(stage.risk_flags),
                        }
                        for stage in pipeline_result.stages
                    ],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def _audit(
        self,
        event_type: AuditEventType,
        *,
        target_type: str,
        target_id: str,
        status: str,
        message: str = "",
        evidence: list[str] | None = None,
    ) -> None:
        self.audit_writer.append(
            AuditEvent(
                run_id=self.run_id,
                event_type=event_type,
                actor=self.created_by,
                action=event_type.value,
                target_type=target_type,
                target_id=target_id,
                status=status,
                message=message,
                evidence=evidence or [],
            ),
        )


def run_real_flow_50(config: RealFlow50Config) -> RealFlow50Result:
    run_id = config.run_id or _default_run_id()
    library_dir = Path(config.library_dir or Path("test_runs") / run_id / "library")
    output_cleaned_dir = Path(
        config.output_cleaned_dir or Path("test_runs") / run_id / "output_cleaned",
    )
    run_dir = library_dir / "runs" / run_id
    input_dir, input_mode = _resolve_input_dir(config, run_id, run_dir)
    ensure_runs_dir_outside_input_dir(
        input_dir=input_dir,
        runs_dir=library_dir / "runs",
    )

    library_dir.mkdir(parents=True, exist_ok=True)
    output_cleaned_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    before_input = _fingerprint_tree(input_dir)
    full_scan_result = scan_input_raw(input_dir, run_id=run_id)
    scan_result = _limit_scan_result(full_scan_result, config.max_file_count)
    ledger_path = save_hash_ledger(
        records=scan_result.records,
        runs_dir=library_dir / "runs",
        summary=scan_result.summary,
    )
    plan = generate_processing_plan(
        library_dir=library_dir,
        output_cleaned_dir=output_cleaned_dir,
        scan_result=scan_result,
        run_id=run_id,
        input_dir=input_dir,
        runs_dir=library_dir / "runs",
    )
    confirm_processing_plan(plan)
    processing_plan_path = run_dir / "processing_plan.json"
    processing_plan_path.write_text(processing_plan_to_json(plan), encoding="utf-8")

    processor = _RealFlowProcessor(
        run_id=run_id,
        input_dir=input_dir,
        library_dir=library_dir,
        output_cleaned_dir=output_cleaned_dir,
        run_dir=run_dir,
        created_by=config.created_by,
    )
    with authorize_run_state_write():
        run_state = BatchRunner(
            runs_dir=library_dir / "runs",
            file_processor=processor.process_file,
            authority=create_real_flow_50_batch_authority(),
        ).run(plan)
    validate_run_state_consistency(run_state)
    processor._audit(  # noqa: SLF001 - this is the owning orchestration boundary.
        AuditEventType.RUN_COMPLETED,
        target_type="run",
        target_id=run_id,
        status=run_state.status.value,
        message="Task 16 e2e batch runner completed",
    )

    index_result = build_formal_library_indexes(library_dir)
    preview = SqlUploadPreviewService(library_dir).generate_preview(
        run_id=run_id,
        created_by=config.created_by,
    )
    unconfirmed_result, unconfirmed_adapter_calls = _attempt_unconfirmed_upload(
        library_dir=library_dir,
        preview_id=preview.preview_id,
        preview_checksum=preview.preview_checksum,
        created_by=config.created_by,
    )
    upload_result = _confirm_upload_if_requested(
        config=config,
        library_dir=library_dir,
        preview_id=preview.preview_id,
        preview_checksum=preview.preview_checksum,
    )

    general_report = generate_run_report(
        ReportGenerationContext(
            run_id=run_id,
            input_dir=input_dir,
            library_dir=library_dir,
            output_cleaned_dir=output_cleaned_dir,
            generated_by="paper_pipeline.e2e.real_flow_50",
        ),
    )
    after_input = _fingerprint_tree(input_dir)
    metadata_records = _load_metadata_records(library_dir, run_id)
    metrics = _build_metrics(
        config=config,
        scan_result=scan_result,
        plan_batch_sizes=[batch.file_count for batch in plan.batches],
        run_state=run_state,
        processor=processor,
        index_result=index_result,
        preview=preview,
        upload_result=upload_result,
        unconfirmed_result=unconfirmed_result,
        unconfirmed_adapter_calls=unconfirmed_adapter_calls,
        metadata_records=metadata_records,
        before_input=before_input,
        after_input=after_input,
        output_cleaned_dir=output_cleaned_dir,
        library_dir=library_dir,
    )
    manifest = _build_manifest(
        run_id=run_id,
        input_dir=input_dir,
        input_mode=input_mode,
        library_dir=library_dir,
        output_cleaned_dir=output_cleaned_dir,
        ledger_path=ledger_path,
        processing_plan_path=processing_plan_path,
        run_state=run_state,
        general_report_manifest=general_report.report_manifest_path,
        evidence_bundle_path=general_report.evidence_bundle_path,
        metrics=metrics,
    )
    placeholder_paths = [
        library_dir / "runs" / run_id / "evaluation" / "evaluation_report.md",
        library_dir / "runs" / run_id / "evaluation" / "evaluation_report.json",
        library_dir / "runs" / run_id / "evaluation" / "e2e_run_manifest.json",
    ]
    p0_issues, p1_issues, p2_issues = evaluate_issue_lists(
        metrics=metrics,
        expected_file_count=config.expected_file_count,
        required_report_paths=[],
    )
    conclusion = overall_conclusion(p0_issues, p1_issues)
    evaluation_paths = write_evaluation_outputs(
        run_id=run_id,
        library_dir=library_dir,
        input_dir=input_dir,
        output_cleaned_dir=output_cleaned_dir,
        metrics=metrics,
        p0_issues=p0_issues,
        p1_issues=p1_issues,
        p2_issues=p2_issues,
        overall_conclusion=conclusion,
        manifest=manifest,
    )
    p0_issues, p1_issues, p2_issues = evaluate_issue_lists(
        metrics=metrics,
        expected_file_count=config.expected_file_count,
        required_report_paths=placeholder_paths,
    )
    if p0_issues:
        conclusion = overall_conclusion(p0_issues, p1_issues)
        write_evaluation_outputs(
            run_id=run_id,
            library_dir=library_dir,
            input_dir=input_dir,
            output_cleaned_dir=output_cleaned_dir,
            metrics=metrics,
            p0_issues=p0_issues,
            p1_issues=p1_issues,
            p2_issues=p2_issues,
            overall_conclusion=conclusion,
            manifest=manifest,
        )

    return RealFlow50Result(
        run_id=run_id,
        input_dir=input_dir,
        library_dir=library_dir,
        output_cleaned_dir=output_cleaned_dir,
        metrics=metrics,
        evaluation_report_md_path=evaluation_paths.evaluation_report_md_path,
        evaluation_report_json_path=evaluation_paths.evaluation_report_json_path,
        e2e_run_manifest_path=evaluation_paths.e2e_run_manifest_path,
        general_report_manifest_path=general_report.report_manifest_path,
        evidence_bundle_path=general_report.evidence_bundle_path,
    )


def _persist_interrupted_run_state(state_path: str | Path) -> RunState | None:
    interrupted = detect_interrupted_run(state_path)
    if interrupted is None:
        return None
    with authorize_run_state_write():
        save_run_state(interrupted, state_path)
    return interrupted


def _persist_abandoned_run_state(state: RunState, state_path: str | Path) -> RunState:
    with authorize_run_state_write():
        save_run_state(state, state_path)
    return state


def _build_metrics(
    *,
    config: RealFlow50Config,
    scan_result: ScanResult,
    plan_batch_sizes: list[int],
    run_state: Any,
    processor: _RealFlowProcessor,
    index_result: Any,
    preview: Any,
    upload_result: SqlUploadConfirmationResult | None,
    unconfirmed_result: SqlUploadConfirmationResult,
    unconfirmed_adapter_calls: int,
    metadata_records: list[PaperMetadata],
    before_input: dict[str, tuple[str, int, int]],
    after_input: dict[str, tuple[str, int, int]],
    output_cleaned_dir: Path,
    library_dir: Path,
) -> dict[str, Any]:
    metrics = default_metrics()
    stage0_counts = _count_stage0(processor.stage0_results)
    pipeline_success_count = sum(
        result.status is PipelineStageStatus.SUCCESS for result in processor.pipeline_results
    )
    pipeline_pending_count = sum(
        result.status is PipelineStageStatus.PENDING for result in processor.pipeline_results
    )
    pipeline_failed_count = sum(
        result.status in {PipelineStageStatus.FAILED, PipelineStageStatus.TIMEOUT}
        for result in processor.pipeline_results
    )
    upload = upload_result.upload_result if upload_result and upload_result.upload_result else None
    real_database_requested = _uses_postgres(config)
    wrote_real_sql = bool(real_database_requested and upload and upload.success_count > 0)
    bm25_entries = _index_entry_count(library_dir / "indexes" / "bm25" / "index.json")
    vector_entries = _index_entry_count(library_dir / "indexes" / "vector" / "index.json")
    tier_counts = _count_metadata_values(metadata_records, "tier")
    sql_counts = _count_metadata_values(metadata_records, "sql_status")
    duplicate_counts = _count_metadata_values(metadata_records, "duplicate_status")
    input_changed = before_input != after_input

    metrics.update(
        {
            "total_input_files": scan_result.summary.total_files,
            "scanned_files": len(scan_result.records),
            "exact_duplicate_files": scan_result.summary.duplicate_files,
            "suspected_duplicate_files": duplicate_counts.get(
                DuplicateStatus.SUSPECTED_DUPLICATE.value,
                0,
            ),
            "unique_files": scan_result.summary.unique_files,
            "plan_generated": True,
            "plan_confirmed": True,
            "batch_count": len(plan_batch_sizes),
            "batch_sizes": plan_batch_sizes,
            "run_status": run_state.status.value,
            "completed_files": run_state.completed_items,
            "pending_files": run_state.pending_items,
            "failed_files": run_state.failed_items,
            "interrupted_files": 0,
            "stage0_accept_count": stage0_counts.get(Stage0Decision.ACCEPT.value, 0),
            "stage0_reject_count": stage0_counts.get(
                Stage0Decision.STAGE0_REJECT.value,
                0,
            ),
            "stage0_pending_count": stage0_counts.get(Stage0Decision.PENDING.value, 0),
            "stage0_rejected_audit_count": stage0_counts.get(
                Stage0Decision.STAGE0_REJECT.value,
                0,
            ),
            "pipeline_success_count": pipeline_success_count,
            "pipeline_pending_count": pipeline_pending_count,
            "pipeline_failed_count": pipeline_failed_count,
            "stage2_document_parse_success_count": _stage_count(
                processor.pipeline_results,
                PipelineStageName.DOCUMENT_PARSE.value,
                PipelineStageStatus.SUCCESS.value,
            ),
            "stage2_document_parse_failed_count": _stage_failed_count(
                processor.pipeline_results,
                PipelineStageName.DOCUMENT_PARSE.value,
            ),
            "markdown_generated_count": sum(
                paper.archive_result.markdown_path.exists()
                for paper in processor.processed_papers
            ),
            "final_md_missing_count": max(
                pipeline_success_count - len(processor.processed_papers),
                0,
            ),
            "category_success_count": _agent_success_count(
                processor.agent_results,
                "CategoryAgent",
            ),
            "direction_success_count": _agent_success_count(
                processor.agent_results,
                "DirectionAgent",
            ),
            "research_method_success_count": _agent_success_count(
                processor.agent_results,
                "ResearchMethodAgent",
            ),
            "quality_tier_suggested_count": _agent_success_count(
                processor.agent_results,
                "QualityTierAgent",
            ),
            "needs_review_count": sum(
                item.tier is Tier.PENDING or bool(item.risk_flags)
                for item in metadata_records
            ),
            "risk_flags_count": sum(len(item.risk_flags) for item in metadata_records),
            "library_gold_count": tier_counts.get(Tier.GOLD.value, 0),
            "library_silver_count": tier_counts.get(Tier.SILVER.value, 0),
            "library_pending_count": tier_counts.get(Tier.PENDING.value, 0),
            "library_rejected_count": tier_counts.get(Tier.REJECTED.value, 0),
            "output_cleaned_bundle_count": _output_cleaned_bundle_count(
                output_cleaned_dir,
                processor.run_id,
            ),
            "metadata_written_count": len(metadata_records),
            "metadata_validation_failed_count": processor.metadata_validation_failed_count,
            "exact_duplicate_skipped_count": scan_result.summary.duplicate_files,
            "suspected_duplicate_pending_count": sum(
                item.duplicate_status is DuplicateStatus.SUSPECTED_DUPLICATE
                and item.tier is Tier.PENDING
                for item in metadata_records
            ),
            "pending_review_suggested_count": sum(
                item.duplicate_status is DuplicateStatus.SUSPECTED_DUPLICATE
                for item in metadata_records
            ),
            "review_history_count": sum(len(item.review_history) for item in metadata_records),
            "indexed_count": index_result.success_count,
            "index_failed_count": index_result.failure_count,
            "needs_rebuild_count": sum(
                item.index_status is IndexStatus.NEEDS_REBUILD for item in metadata_records
            ),
            "index_skipped_count": index_result.skipped_count,
            "bm25_index_entries": bm25_entries,
            "vector_stub_entries": vector_entries,
            "sql_eligible_count": preview.eligible_count,
            "sql_blocked_count": preview.blocked_count,
            "preview_generated": True,
            "preview_checksum_valid": (
                bool(preview.preview_checksum)
                and calculate_preview_checksum(preview) == preview.preview_checksum
            ),
            "confirmation_required": True,
            "second_confirmation_performed": bool(
                config.confirm_upload and preview.ready_for_confirmation,
            ),
            "unconfirmed_upload_blocked": (
                unconfirmed_result.accepted is False and unconfirmed_adapter_calls == 0
            ),
            "real_database_upload_requested": real_database_requested,
            "sql_dry_run_covered": not real_database_requested,
            "sql_upload_mode": "postgres" if real_database_requested else "fake/dry_run",
            "sql_upload_success_count": upload.success_count if upload else 0,
            "sql_upload_failed_count": upload.failed_count if upload else 0,
            "sql_status_uploaded_count": sql_counts.get("uploaded", 0),
            "sql_status_failed_count": sql_counts.get("failed", 0),
            "sql_status_not_uploaded_count": sql_counts.get("not_uploaded", 0),
            "touched_input_raw": input_changed,
            "deleted_original_files": _deleted_files(before_input, after_input),
            "moved_original_files": _moved_files(before_input, after_input),
            "connected_real_database": wrote_real_sql,
            "wrote_real_sql": wrote_real_sql,
            "sql_remote_response_detail_count": (
                upload.success_count if upload is not None and wrote_real_sql else 0
            ),
            "output_cleaned_deleted": False,
            "old_versions_deleted": False,
            "unconfirmed_upload_triggered": unconfirmed_adapter_calls > 0,
            "failed_items_written_uploaded": _failed_items_written_uploaded(
                metadata_records,
            ),
            "blocked_items_uploaded": _blocked_items_uploaded(metadata_records),
            "pending_reason_count": sum(
                1 for item in run_state.file_states if item.status.value == "pending" and item.reason
            ),
        },
    )
    return metrics


def _attempt_unconfirmed_upload(
    *,
    library_dir: Path,
    preview_id: str,
    preview_checksum: str,
    created_by: str,
) -> tuple[SqlUploadConfirmationResult, int]:
    adapter = FakeSqlUploadAdapter()
    service = SqlUploadService(library_dir, adapter=adapter)
    result = service.confirm_sql_upload_preview(
        SqlUploadConfirmation(
            preview_id=preview_id,
            preview_checksum=preview_checksum,
            confirmed_by=created_by,
            confirmation_text="未确认上传",
        ),
    )
    return result, adapter.upload_call_count


def _confirm_upload_if_requested(
    *,
    config: RealFlow50Config,
    library_dir: Path,
    preview_id: str,
    preview_checksum: str,
) -> SqlUploadConfirmationResult | None:
    if not config.confirm_upload:
        return None
    adapter = _upload_adapter_for_mode(config)
    service = SqlUploadService(library_dir, adapter=adapter)
    preview = SqlUploadPreviewService(library_dir).load_preview(preview_id)
    context = service.confirmation_context_for_preview(preview)
    with authorize_sql_write_from_real_flow_50(
        run_id=preview.run_id,
        operation="real_flow_50.sql_upload",
    ):
        return service.confirm_sql_upload_preview(
            SqlUploadConfirmation(
                preview_id=preview_id,
                preview_checksum=preview_checksum,
                confirmed_by=config.created_by,
                confirmation_text=context.confirmation_text,
                preview_was_shown=True,
                operator_confirmed_upload=True,
                eligible_count=context.eligible_count,
                upload_mode=context.upload_mode,
                target_summary=context.target_summary,
            ),
        )


def _apply_automatic_quality_tier_decision(
    metadata: PaperMetadata,
    decision: dict[str, Any],
) -> None:
    suggested_tier = Tier(str(decision.get("tier", Tier.PENDING.value)))
    if _has_initial_review_risk(metadata, decision):
        metadata.tier = Tier.PENDING
        metadata.quality_tier_suggestion = Tier.PENDING.value
        metadata.gold_candidate = False
        metadata.needs_codex_review = True
        metadata.tier_decision_source = "auto_quality_suggestion"
        reason = str(
            decision.get("quality_reason")
            or decision.get("reason")
            or "risk or uncertainty requires pending review",
        )
        metadata.quality_reason = f"deepseek_uncertain: {reason}"
        return

    if suggested_tier is Tier.GOLD:
        metadata.tier = Tier.PENDING
        metadata.quality_tier_suggestion = Tier.GOLD.value
        metadata.gold_candidate = True
        metadata.needs_codex_review = True
        metadata.tier_decision_source = "auto_quality_suggestion"
        return

    if suggested_tier is Tier.SILVER:
        metadata.tier = Tier.PENDING
        metadata.quality_tier_suggestion = Tier.SILVER.value
        metadata.gold_candidate = False
        metadata.needs_codex_review = True
        metadata.tier_decision_source = "auto_quality_suggestion"
        return

    metadata.tier = suggested_tier if suggested_tier is Tier.REJECTED else Tier.PENDING
    metadata.quality_tier_suggestion = metadata.tier.value
    metadata.gold_candidate = False
    metadata.needs_codex_review = suggested_tier is not Tier.REJECTED
    metadata.tier_decision_source = "auto_quality_suggestion"


def _has_initial_review_risk(metadata: PaperMetadata, decision: dict[str, Any]) -> bool:
    risk_flags = decision.get("risk_flags", [])
    return (
        bool(metadata.risk_flags)
        or (isinstance(risk_flags, list) and bool(risk_flags))
        or bool(decision.get("needs_review", False))
        or str(decision.get("tier", Tier.PENDING.value)) == Tier.PENDING.value
    )


def _apply_llm_initial_review_decision(
    metadata: PaperMetadata,
    decision: dict[str, Any],
) -> None:
    if not decision:
        return
    mode = str(decision.get("llm_mode", ""))
    decision_text = str(decision.get("decision", "")).lower()
    reason = str(decision.get("reason") or decision.get("llm_error") or "")
    risk_flags = decision.get("risk_flags", [])
    confidence = _optional_float(decision.get("confidence"))

    if decision_text in {"approve_gold", "gold"}:
        metadata.tier = Tier.PENDING
        metadata.quality_tier_suggestion = Tier.GOLD.value
        metadata.gold_candidate = True
        metadata.needs_codex_review = True
        metadata.tier_decision_source = "llm_initial_review"
        metadata.quality_reason = reason or "llm_initial_review: approve_gold"
        return
    if decision_text in {"approve_silver", "silver"}:
        metadata.tier = Tier.PENDING
        metadata.quality_tier_suggestion = Tier.SILVER.value
        metadata.gold_candidate = False
        metadata.needs_codex_review = True
        metadata.tier_decision_source = "llm_initial_review"
        metadata.quality_reason = reason or "llm_initial_review: approve_silver"
        return
    if decision_text in {"reject", "rejected"}:
        metadata.tier = Tier.REJECTED
        metadata.quality_tier_suggestion = Tier.REJECTED.value
        metadata.gold_candidate = False
        metadata.needs_codex_review = False
        metadata.tier_decision_source = "llm_initial_review"
        metadata.quality_reason = reason or "llm_initial_review: rejected"
        return
    if (
        mode in {"failed", "config-error", "fake"}
        or decision_text in {"", "pending", "uncertain"}
        or (confidence is not None and confidence < 0.7)
        or (isinstance(risk_flags, list) and bool(risk_flags))
    ):
        metadata.tier = Tier.PENDING
        metadata.quality_tier_suggestion = Tier.PENDING.value
        metadata.gold_candidate = False
        metadata.needs_codex_review = True
        metadata.tier_decision_source = "llm_initial_review"
        if mode == "failed":
            prefix = "llm_failed"
        elif mode == "config-error":
            prefix = "llm_failed"
        elif mode == "fake":
            prefix = "llm_not_configured"
        else:
            prefix = "deepseek_uncertain"
        metadata.quality_reason = f"{prefix}: {reason or 'initial review requires pending'}"


def _llm_quality_payload_for(final_markdown: Path) -> dict[str, Any]:
    path = final_markdown.parent / "logs" / "llm_quality_filter.json"
    if not path.exists():
        return {}
    decoded = json.loads(path.read_text(encoding="utf-8"))
    return decoded if isinstance(decoded, dict) else {}


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _upload_adapter_for_mode(config: RealFlow50Config):
    if _uses_postgres(config):
        return PostgresSqlUploadAdapter.from_environ()
    return FakeSqlUploadAdapter(fail_paper_ids=set(config.sql_fail_paper_ids))


def _uses_postgres(config: RealFlow50Config) -> bool:
    return config.mode.lower() in {"postgres", "postgresql", "real-postgres"}


def _resolve_input_dir(
    config: RealFlow50Config,
    run_id: str,
    run_dir: Path,
) -> tuple[Path, str]:
    if config.generate_sample or (
        config.input_dir is None and config.source_dir is None
    ):
        target = run_dir / "input_raw_sample"
        generate_safe_sample_input(target, file_count=config.expected_file_count)
        return target, "generated_safe_sample"

    if config.copy_sample:
        if config.source_dir is None:
            raise ValueError("source_dir is required when copy_sample=True")
        target = run_dir / "input_raw_sample"
        _copy_sample_files(Path(config.source_dir), target, config.expected_file_count)
        return target, "真实样本副本"

    if config.input_dir is None:
        raise ValueError("input_dir is required unless generate_sample or copy_sample is used")
    input_dir = Path(config.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(input_dir)
    total_files = sum(1 for path in input_dir.rglob("*") if path.is_file())
    if total_files < config.expected_file_count and not config.allow_less_than_50:
        raise ValueError(
            f"input_dir contains {total_files} files; expected {config.expected_file_count}",
        )
    return input_dir, "已准备测试目录"


def _limit_scan_result(scan_result: ScanResult, max_file_count: int) -> ScanResult:
    if max_file_count <= 0 or len(scan_result.records) <= max_file_count:
        return scan_result

    records = scan_result.records[:max_file_count]
    duplicate_hashes = {
        record.source_hash
        for record in records
        if sum(item.source_hash == record.source_hash for item in records) > 1
    }
    summary = ScanSummary(
        run_id=scan_result.summary.run_id,
        input_root=scan_result.summary.input_root,
        total_files=len(records),
        unique_files=sum(1 for record in records if not record.is_duplicate),
        duplicate_files=sum(1 for record in records if record.is_duplicate),
        duplicate_hashes=len(duplicate_hashes),
        total_bytes=sum(record.source_size for record in records),
        started_at=scan_result.summary.started_at,
        completed_at=scan_result.summary.completed_at,
    )
    return ScanResult(records=records, summary=summary)


def generate_safe_sample_input(target: str | Path, *, file_count: int = 50) -> Path:
    root = Path(target)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    if file_count <= 0:
        return root

    regular_count = max(file_count - 10, 0)
    for index in range(1, regular_count + 1):
        suffix = ".pdf" if index <= 20 else ".docx" if index <= 35 else ".doc"
        (root / f"paper_{index:03d}{suffix}").write_text(
            _full_paper_text(index),
            encoding="utf-8",
        )

    additions = [
        ("paper_041_literature_review.txt", _literature_review_text()),
        ("paper_042_opening_report.txt", "开题报告\n论文题目：软件系统开发研究\n填写导师意见"),
        ("paper_043_uncertain.txt", "零散资料，缺少论文结构。"),
        ("paper_044_template.txt", "毕业论文模板\n请在此处填写论文题目、学院、专业。"),
        ("paper_046_similar_duplicate.txt", _full_paper_text(46, force_topic=2)),
        ("paper_049_doc_sample.doc", _full_paper_text(49)),
        ("paper_050_docx_sample.docx", _full_paper_text(50)),
    ]
    for name, content in additions[: max(file_count - regular_count, 0)]:
        (root / name).write_text(content, encoding="utf-8")

    if file_count >= 45:
        duplicate_source = root / "paper_001.pdf"
        duplicate_target = root / "paper_045_duplicate_of_001.txt"
        if duplicate_source.exists():
            duplicate_target.write_bytes(duplicate_source.read_bytes())

    if file_count >= 47:
        with zipfile.ZipFile(root / "paper_047_zip_valid.zip", "w") as archive:
            archive.writestr("paper_zip.txt", _full_paper_text(47))

    if file_count >= 48:
        (root / "paper_048_corrupt.zip").write_bytes(b"not a real zip archive")

    _trim_or_pad_sample(root, file_count)
    return root


def _copy_sample_files(source: Path, target: Path, expected_file_count: int) -> None:
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    files = sorted(path for path in source.rglob("*") if path.is_file())
    for path in files[:expected_file_count]:
        relative = path.relative_to(source)
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)


def _build_manifest(
    *,
    run_id: str,
    input_dir: Path,
    input_mode: str,
    library_dir: Path,
    output_cleaned_dir: Path,
    ledger_path: Path,
    processing_plan_path: Path,
    run_state: Any,
    general_report_manifest: Path,
    evidence_bundle_path: Path,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "input_mode": input_mode,
        "input_dir": str(input_dir),
        "library_dir": str(library_dir),
        "output_cleaned_dir": str(output_cleaned_dir),
        "hash_ledger_path": str(ledger_path),
        "processing_plan_path": str(processing_plan_path),
        "run_state": run_state_to_dict(run_state),
        "general_report_manifest_path": str(general_report_manifest),
        "evidence_bundle_path": str(evidence_bundle_path),
        "metrics": metrics,
        "sql_mode": "fake/dry-run",
        "connected_real_database": False,
        "wrote_real_input_raw": False,
    }


def _load_metadata_records(library_dir: Path, run_id: str) -> list[PaperMetadata]:
    records: list[PaperMetadata] = []
    metadata_paths = {
        *library_dir.rglob("*.metadata.json"),
        *library_dir.rglob("metadata.json"),
    }
    for path in sorted(metadata_paths):
        if _is_under(path, library_dir, "runs") or _is_under(path, library_dir, "indexes"):
            continue
        try:
            metadata = metadata_from_json(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
        if metadata.run_id in {"", run_id}:
            records.append(metadata)
    return records


def _paper_text_profile(source_path: str) -> dict[str, str | list[str]]:
    source_name = Path(source_path).name.lower()
    profile_index = 2 if "similar" in source_name else _first_int(source_name)
    topic = _topic(profile_index)
    return {
        "title": f"{topic}软件平台系统设计与实现样本 {profile_index}",
        "abstract": (
            f"本文围绕{topic}业务场景，使用 Spring Boot、Vue、数据库和前后端分离"
            f"技术完成需求分析、系统设计、数据库设计、功能实现和测试。样本编号：{profile_index}。"
        ),
        "keywords": ["软件", "系统", "数据库", "Vue", "Spring Boot", topic, f"样本{profile_index}"],
    }


def _full_paper_text(index: int, *, force_topic: int | None = None) -> str:
    topic = _topic(force_topic or index)
    return (
        "本科毕业论文\n"
        f"题目：{topic}软件平台系统设计与实现\n"
        f"摘要：本文围绕{topic}业务场景，使用 Spring Boot、Vue、数据库和前后端分离"
        f"技术完成需求分析、系统设计、数据库设计、功能实现和测试。样本编号：{index}。\n"
        "关键词：软件；系统；数据库；Vue；Spring Boot\n"
        "第一章 绪论\n"
        "第二章 文献综述\n"
        "第三章 系统需求分析\n"
        "第四章 系统设计与实现\n"
        "结论\n"
        "参考文献\n"
    )


def _literature_review_text() -> str:
    return "文献综述\n本文仅为独立文献综述文件，不包含完整毕业论文结构。"


def _topic(index: int) -> str:
    topics = [
        "图书借阅",
        "校园报修",
        "在线考试",
        "实验室预约",
        "仓储库存",
        "社团管理",
        "课程评价",
        "招聘信息",
        "志愿服务",
        "资产管理",
    ]
    return topics[(max(index, 1) - 1) % len(topics)]


def _trim_or_pad_sample(root: Path, file_count: int) -> None:
    files = sorted(path for path in root.rglob("*") if path.is_file())
    for path in files[file_count:]:
        path.unlink()
    current = len([path for path in root.rglob("*") if path.is_file()])
    for index in range(current + 1, file_count + 1):
        (root / f"paper_pad_{index:03d}.txt").write_text(
            _full_paper_text(index),
            encoding="utf-8",
        )


def _first_int(value: str) -> int:
    match = re.search(r"(\d+)", value)
    return int(match.group(1)) if match else 1


def _fingerprint_tree(root: Path) -> dict[str, tuple[str, int, int]]:
    result: dict[str, tuple[str, int, int]] = {}
    if not root.exists():
        return result
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        result[path.relative_to(root).as_posix()] = (
            digest,
            path.stat().st_size,
            path.stat().st_mtime_ns,
        )
    return result


def _count_stage0(results: list[Stage0Result]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.decision.value] = counts.get(result.decision.value, 0) + 1
    return counts


def _stage_count(results: list[Any], stage_name: str, status: str) -> int:
    count = 0
    for result in results:
        for stage in result.stages:
            if stage.stage_name.value == stage_name and stage.status.value == status:
                count += 1
    return count


def _stage_failed_count(results: list[Any], stage_name: str) -> int:
    return sum(
        1
        for result in results
        for stage in result.stages
        if stage.stage_name.value == stage_name
        and stage.status in {PipelineStageStatus.FAILED, PipelineStageStatus.TIMEOUT}
    )


def _agent_success_count(results: list[AgentResult], agent_name: str) -> int:
    return sum(result.agent_name == agent_name and result.status == "success" for result in results)


def _count_metadata_values(records: list[PaperMetadata], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        value = getattr(record, field_name)
        if hasattr(value, "value"):
            value = value.value
        value_str = str(value)
        counts[value_str] = counts.get(value_str, 0) + 1
    return counts


def _output_cleaned_bundle_count(output_cleaned_dir: Path, run_id: str) -> int:
    run_dir = output_cleaned_dir / "runs" / run_id
    if not run_dir.exists():
        return 0
    return sum(1 for path in run_dir.glob("*") if path.is_dir() and (path / "final.md").exists())


def _index_entry_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    entries = decoded.get("entries", []) if isinstance(decoded, dict) else []
    return len(entries) if isinstance(entries, list) else 0


def _deleted_files(
    before: dict[str, tuple[str, int, int]],
    after: dict[str, tuple[str, int, int]],
) -> bool:
    return any(path not in after for path in before)


def _moved_files(
    before: dict[str, tuple[str, int, int]],
    after: dict[str, tuple[str, int, int]],
) -> bool:
    if set(before) == set(after):
        return False
    before_hashes = {value[0] for value in before.values()}
    after_hashes = {value[0] for value in after.values()}
    return before_hashes == after_hashes and set(before) != set(after)


def _failed_items_written_uploaded(records: list[PaperMetadata]) -> bool:
    return any(record.sql_upload_error and record.sql_status.value == "uploaded" for record in records)


def _blocked_items_uploaded(records: list[PaperMetadata]) -> bool:
    return any(
        record.sql_status.value == "uploaded"
        and (
            record.tier not in {Tier.GOLD, Tier.SILVER}
            or record.duplicate_status is not DuplicateStatus.UNIQUE
            or record.index_status is not IndexStatus.INDEXED
        )
        for record in records
    )


def _is_under(path: Path, root: Path, directory_name: str) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == directory_name


def _default_run_id() -> str:
    return f"task16-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run task 16 real-flow evaluation")
    parser.add_argument("--input", dest="input_dir")
    parser.add_argument("--source", dest="source_dir")
    parser.add_argument("--library", dest="library_dir")
    parser.add_argument("--output-cleaned", dest="output_cleaned_dir")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--mode", default="dry-run-sql", choices=["dry-run-sql", "postgresql"])
    parser.add_argument("--copy-sample", action="store_true")
    parser.add_argument("--generate-sample", action="store_true")
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--allow-less-than-50", action="store_true")
    parser.add_argument("--no-confirm-upload", action="store_true")
    args = parser.parse_args(argv)

    result = run_real_flow_50(
        RealFlow50Config(
            input_dir=args.input_dir,
            source_dir=args.source_dir,
            library_dir=args.library_dir,
            output_cleaned_dir=args.output_cleaned_dir,
            run_id=args.run_id,
            mode=args.mode,
            copy_sample=args.copy_sample,
            generate_sample=args.generate_sample,
            max_file_count=args.max_files,
            allow_less_than_50=args.allow_less_than_50,
            confirm_upload=not args.no_confirm_upload,
        ),
    )
    print(json.dumps({"run_id": result.run_id, "evaluation_report": str(result.evaluation_report_json_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
