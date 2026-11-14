"""Confirmed SQL upload orchestration and metadata status writeback."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from paper_pipeline.domain import PaperMetadata, ReviewHistoryItem, SqlStatus
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller
from paper_pipeline.source_status_markers import (
    resolve_workbench_original_source,
    write_source_status_marker,
)
from paper_pipeline.sql.audit import build_sql_audit_record
from paper_pipeline.sql.adapter_factory import default_sql_upload_adapter
from paper_pipeline.sql.eligibility import SqlEligibilityCheck, check_sql_eligibility
from paper_pipeline.sql.preview import SqlUploadPreviewService
from paper_pipeline.sql.sql_models import (
    SqlUploadBlockedItem,
    SqlUploadConfirmation,
    SqlUploadConfirmationContext,
    SqlUploadPreview,
    SqlUploadPreviewItem,
    build_sql_upload_confirmation_context,
    calculate_preview_checksum,
    file_fingerprint,
    is_expired,
    utc_now_iso,
)
from paper_pipeline.sql.upload_adapter import (
    SqlUploadAdapter,
    SqlUploadFailedItem,
    SqlUploadResult,
    SqlUploadedItem,
)
from paper_pipeline.sql.write_authorization import ensure_sql_write_authorized
from paper_pipeline.storage.metadata_writer import MetadataWriter


@dataclass(frozen=True, slots=True)
class SqlUploadConfirmationResult:
    accepted: bool
    preview_id: str
    errors: list[str] = field(default_factory=list)
    rechecked_blocked_items: list[SqlUploadBlockedItem] = field(default_factory=list)
    upload_result: SqlUploadResult | None = None
    sql_status: str = "not_uploaded"
    upload_mode: str = "not-configured"
    target_summary: str = ""


@dataclass(slots=True)
class SqlUploadService:
    library_dir: str | Path
    adapter: SqlUploadAdapter = field(default_factory=default_sql_upload_adapter)
    metadata_writer: MetadataWriter = field(default_factory=MetadataWriter)

    def confirm_sql_upload_preview(
        self,
        confirmation: SqlUploadConfirmation | None,
    ) -> SqlUploadConfirmationResult:
        ensure_real_flow_50_caller(
            "SqlUploadService.confirm_sql_upload_preview",
            allow_test_callers=True,
            allow_modules=("paper_pipeline.sql.upload_service",),
        )
        context = self._previewless_confirmation_context()
        if confirmation is None:
            return SqlUploadConfirmationResult(
                accepted=False,
                preview_id="",
                errors=["confirmation is required"],
                sql_status="not_uploaded",
                upload_mode=context.upload_mode,
                target_summary=context.target_summary,
            )
        preview_service = SqlUploadPreviewService(self.library_dir)
        try:
            preview = preview_service.load_preview(confirmation.preview_id)
        except (FileNotFoundError, TypeError, KeyError, ValueError) as exc:
            return SqlUploadConfirmationResult(
                accepted=False,
                preview_id=confirmation.preview_id,
                errors=[_sanitize_sql_upload_text(f"preview cannot be loaded: {exc}")],
                sql_status="not_uploaded",
                upload_mode=context.upload_mode,
                target_summary=context.target_summary,
            )

        context = self.confirmation_context_for_preview(preview)
        errors = _validate_confirmation(preview, confirmation, context)
        if context.upload_mode in {"fake", "dry_run"}:
            errors.append(
                f"SQL upload skipped because upload_mode={context.upload_mode}; "
                "metadata remains not_uploaded",
            )
            return SqlUploadConfirmationResult(
                accepted=False,
                preview_id=preview.preview_id,
                errors=errors,
                sql_status=context.upload_mode,
                upload_mode=context.upload_mode,
                target_summary=context.target_summary,
            )
        rechecked_blocked, stale_errors = self._recheck_preview_items(preview)
        errors.extend(stale_errors)
        if rechecked_blocked:
            errors.append("eligibility changed after preview; regenerate SQL preview")
        if not preview.items:
            errors.append("preview has no eligible SQL items")

        if errors:
            return SqlUploadConfirmationResult(
                accepted=False,
                preview_id=preview.preview_id,
                errors=[_sanitize_sql_upload_text(error) for error in errors],
                rechecked_blocked_items=rechecked_blocked,
                sql_status="not_uploaded",
                upload_mode=context.upload_mode,
                target_summary=context.target_summary,
            )

        ensure_sql_write_authorized("SqlUploadService.confirm_sql_upload_preview")
        sql_batch_id = f"sqlbatch-{preview.preview_id}"
        upload_result = self.adapter.upload(
            preview=preview,
            items=list(preview.items),
            sql_batch_id=sql_batch_id,
        )
        upload_result = _sanitize_upload_result(upload_result)
        self._write_upload_results(
            preview=preview,
            confirmation=confirmation,
            upload_result=upload_result,
        )
        return SqlUploadConfirmationResult(
            accepted=True,
            preview_id=preview.preview_id,
            upload_result=upload_result,
            sql_status=_sql_status_for_upload_result(upload_result),
            upload_mode=context.upload_mode,
            target_summary=context.target_summary,
        )

    def confirmation_context_for_preview(
        self,
        preview: SqlUploadPreview,
    ) -> SqlUploadConfirmationContext:
        context = self._previewless_confirmation_context()
        return build_sql_upload_confirmation_context(
            preview,
            upload_mode=context.upload_mode,
            target_summary=context.target_summary,
        )

    def _previewless_confirmation_context(self) -> SqlUploadConfirmationContext:
        mode, target_summary = _adapter_upload_context(self.adapter)
        placeholder = SqlUploadPreview(
            preview_id="",
            run_id="",
            created_at="",
            created_by="",
            candidate_count=0,
            eligible_count=0,
            blocked_count=0,
            items=[],
            blocked_items=[],
            preview_checksum="",
            expires_at=utc_now_iso(),
            ready_for_confirmation=False,
            will_upload_sql=False,
        )
        return build_sql_upload_confirmation_context(
            placeholder,
            upload_mode=mode,
            target_summary=target_summary,
        )

    def cancel_sql_upload_preview(
        self,
        preview_id: str,
        *,
        cancelled_by: str,
        reason: str = "",
    ) -> SqlUploadConfirmationResult:
        ensure_real_flow_50_caller(
            "SqlUploadService.cancel_sql_upload_preview",
            allow_test_callers=True,
            allow_modules=("paper_pipeline.sql.upload_service",),
        )
        try:
            preview = SqlUploadPreviewService(self.library_dir).load_preview(preview_id)
        except (FileNotFoundError, TypeError, KeyError, ValueError) as exc:
            return SqlUploadConfirmationResult(
                accepted=False,
                preview_id=preview_id,
                errors=[_sanitize_sql_upload_text(f"preview cannot be loaded for cancellation: {exc}")],
            )
        message = f"SQL upload cancelled by {cancelled_by}"
        if reason:
            message = f"{message}: {reason}"
        return SqlUploadConfirmationResult(
            accepted=False,
            preview_id=preview.preview_id,
            errors=[message],
        )

    def _recheck_preview_items(
        self,
        preview: SqlUploadPreview,
    ) -> tuple[list[SqlUploadBlockedItem], list[str]]:
        blocked: list[SqlUploadBlockedItem] = []
        stale_errors: list[str] = []
        for item in preview.items:
            metadata_fingerprint = file_fingerprint(item.metadata_path)
            md_fingerprint = file_fingerprint(item.md_path)
            if (
                metadata_fingerprint != item.metadata_fingerprint
                or md_fingerprint != item.md_fingerprint
            ):
                stale_errors.append(f"preview is stale for {item.paper_id}")

            metadata = _load_metadata(item.metadata_path)
            result = check_sql_eligibility(
                SqlEligibilityCheck(
                    metadata=metadata,
                    metadata_path=item.metadata_path,
                    md_path=item.md_path,
                ),
            )
            if not result.eligible:
                blocked.append(
                    SqlUploadBlockedItem(
                        paper_id=result.paper_id or item.paper_id,
                        reasons=list(result.reasons),
                        blocking_fields=list(result.blocking_fields),
                        suggested_next_action=result.suggested_next_action,
                        metadata_path=item.metadata_path,
                        md_path=item.md_path,
                    ),
                )
        return blocked, stale_errors

    def _write_upload_results(
        self,
        *,
        preview: SqlUploadPreview,
        confirmation: SqlUploadConfirmation,
        upload_result: SqlUploadResult,
    ) -> None:
        item_by_paper_id = {item.paper_id: item for item in preview.items}
        for uploaded_item in upload_result.uploaded_items:
            preview_item = item_by_paper_id.get(uploaded_item.paper_id)
            if preview_item is None:
                continue
            self._mark_uploaded(
                preview=preview,
                confirmation=confirmation,
                upload_result=upload_result,
                preview_item=preview_item,
                uploaded_item=uploaded_item,
            )

        for failed_item in upload_result.failed_items:
            preview_item = item_by_paper_id.get(failed_item.paper_id)
            if preview_item is None:
                continue
            self._mark_failed(
                preview=preview,
                confirmation=confirmation,
                upload_result=upload_result,
                preview_item=preview_item,
                failed_item=failed_item,
            )

    def _mark_uploaded(
        self,
        *,
        preview: SqlUploadPreview,
        confirmation: SqlUploadConfirmation,
        upload_result: SqlUploadResult,
        preview_item: SqlUploadPreviewItem,
        uploaded_item: SqlUploadedItem,
    ) -> None:
        metadata = _load_metadata(preview_item.metadata_path)
        if metadata is None:
            return
        uploaded_at = utc_now_iso()
        metadata.sql_status = SqlStatus.UPLOADED
        metadata.sql_uploaded_at = uploaded_at
        metadata.sql_batch_id = upload_result.sql_batch_id
        metadata.sql_preview_id = preview.preview_id
        metadata.sql_uploaded_by = confirmation.confirmed_by
        metadata.sql_upload_adapter = upload_result.adapter_name
        metadata.sql_upload_record_id = uploaded_item.record_id
        metadata.sql_upload_error = ""
        metadata.sql_audit_history.append(
            build_sql_audit_record(
                action="sql_upload_succeeded",
                operator=confirmation.confirmed_by,
                preview_id=preview.preview_id,
                batch_id=upload_result.sql_batch_id,
                adapter=upload_result.adapter_name,
                paper_id=preview_item.paper_id,
                status=SqlStatus.UPLOADED.value,
                record_id=uploaded_item.record_id,
            ),
        )
        metadata.add_review_history(
            ReviewHistoryItem(
                reviewer=confirmation.confirmed_by,
                action="sql_upload_succeeded",
                comment=f"SQL upload succeeded via {upload_result.adapter_name}",
            ),
        )
        _write_uploaded_source_marker(
            metadata,
            preview=preview,
            upload_result=upload_result,
            preview_item=preview_item,
            uploaded_item=uploaded_item,
        )
        self.metadata_writer.write_metadata(metadata, preview_item.metadata_path)

    def _mark_failed(
        self,
        *,
        preview: SqlUploadPreview,
        confirmation: SqlUploadConfirmation,
        upload_result: SqlUploadResult,
        preview_item: SqlUploadPreviewItem,
        failed_item: SqlUploadFailedItem,
    ) -> None:
        metadata = _load_metadata(preview_item.metadata_path)
        if metadata is None:
            return
        metadata.sql_status = SqlStatus.FAILED
        metadata.sql_batch_id = upload_result.sql_batch_id
        metadata.sql_preview_id = preview.preview_id
        metadata.sql_uploaded_by = confirmation.confirmed_by
        metadata.sql_upload_adapter = upload_result.adapter_name
        metadata.sql_upload_record_id = ""
        metadata.sql_upload_error = failed_item.error
        metadata.sql_audit_history.append(
            build_sql_audit_record(
                action="sql_upload_failed",
                operator=confirmation.confirmed_by,
                preview_id=preview.preview_id,
                batch_id=upload_result.sql_batch_id,
                adapter=upload_result.adapter_name,
                paper_id=preview_item.paper_id,
                status=SqlStatus.FAILED.value,
                message=failed_item.error,
            ),
        )
        metadata.add_review_history(
            ReviewHistoryItem(
                reviewer=confirmation.confirmed_by,
                action="sql_upload_failed",
                comment=failed_item.error,
            ),
        )
        self.metadata_writer.write_metadata(metadata, preview_item.metadata_path)


def confirm_sql_upload_preview(
    library_dir: str | Path,
    confirmation: SqlUploadConfirmation,
    adapter: SqlUploadAdapter | None = None,
) -> SqlUploadConfirmationResult:
    ensure_real_flow_50_caller(
        "confirm_sql_upload_preview",
        allow_test_callers=True,
        allow_modules=("paper_pipeline.sql.upload_service",),
    )
    service = SqlUploadService(
        library_dir=library_dir,
        adapter=adapter or default_sql_upload_adapter(),
    )
    return service.confirm_sql_upload_preview(confirmation)


def confirm_sql_upload(
    library_dir: str | Path,
    confirmation: SqlUploadConfirmation,
    adapter: SqlUploadAdapter | None = None,
) -> SqlUploadConfirmationResult:
    ensure_real_flow_50_caller("confirm_sql_upload", allow_test_callers=True)
    return confirm_sql_upload_preview(library_dir, confirmation, adapter)


def _write_uploaded_source_marker(
    metadata: PaperMetadata,
    *,
    preview: SqlUploadPreview,
    upload_result: SqlUploadResult,
    preview_item: SqlUploadPreviewItem,
    uploaded_item: SqlUploadedItem,
) -> None:
    source = resolve_workbench_original_source(metadata.source_path)
    if source is None:
        return
    if not source.exists() or not source.is_file():
        return
    write_source_status_marker(
        source,
        status=SqlStatus.UPLOADED.value,
        run_id=preview.run_id,
        paper_id=preview_item.paper_id,
        target_table=upload_result.adapter_name,
        extra={
            "accession_id": uploaded_item.accession_id,
            "version": uploaded_item.version,
            "record_id": uploaded_item.record_id,
            "sql_batch_id": upload_result.sql_batch_id,
            "sql_preview_id": preview.preview_id,
        },
    )


def cancel_sql_upload(
    library_dir: str | Path,
    preview_id: str,
    *,
    cancelled_by: str,
    reason: str = "",
    adapter: SqlUploadAdapter | None = None,
) -> SqlUploadConfirmationResult:
    ensure_real_flow_50_caller("cancel_sql_upload", allow_test_callers=True)
    service = SqlUploadService(
        library_dir=library_dir,
        adapter=adapter or default_sql_upload_adapter(),
    )
    return service.cancel_sql_upload_preview(
        preview_id,
        cancelled_by=cancelled_by,
        reason=reason,
    )


def _validate_confirmation(
    preview: SqlUploadPreview,
    confirmation: SqlUploadConfirmation,
    context: SqlUploadConfirmationContext,
) -> list[str]:
    errors: list[str] = []
    if not preview.preview_checksum:
        errors.append("preview checksum is missing")
    if not confirmation.preview_checksum:
        errors.append("preview_checksum is required")
    elif confirmation.preview_checksum != preview.preview_checksum:
        errors.append("preview_checksum does not match preview")
    if calculate_preview_checksum(preview) != preview.preview_checksum:
        errors.append("persisted preview checksum is invalid")
    if not confirmation.confirmed_by.strip():
        errors.append("confirmed_by is required")
    if not confirmation.preview_was_shown:
        errors.append("preview_was_shown=True is required")
    if not confirmation.operator_confirmed_upload:
        errors.append("operator_confirmed_upload=True is required")
    if confirmation.eligible_count != preview.eligible_count:
        errors.append("eligible_count does not match preview")
    if confirmation.upload_mode != context.upload_mode:
        errors.append("upload_mode does not match current SQL adapter mode")
    if confirmation.target_summary != context.target_summary:
        errors.append("target_summary does not match current SQL target")
    if confirmation.confirmation_text != context.confirmation_text:
        errors.append("confirmation_text does not match preview-bound confirmation text")
    if is_expired(preview.expires_at):
        errors.append("preview has expired")
    if not preview.ready_for_confirmation:
        errors.append("preview is not ready for confirmation")
    if preview.will_upload_sql:
        errors.append("preview must not pre-authorize SQL upload")
    return errors


def _adapter_upload_context(adapter: SqlUploadAdapter) -> tuple[str, str]:
    adapter_name = str(getattr(adapter, "adapter_name", type(adapter).__name__))
    lowered = adapter_name.lower()
    if "dry" in lowered:
        mode = "dry_run"
    elif "fake" in lowered:
        mode = "fake"
    else:
        mode = "real"
    target_summary = _adapter_target_summary(adapter_name, adapter)
    return mode, target_summary


def _adapter_target_summary(adapter_name: str, adapter: SqlUploadAdapter) -> str:
    config = getattr(adapter, "config", None)
    environ = getattr(adapter, "environ", None)
    safe_summary = getattr(config, "safe_summary", None)
    if callable(safe_summary):
        summary = safe_summary(environ)
        parts = [
            f"adapter={adapter_name}",
            f"dsn={summary.get('dsn', 'UNSET')}",
            f"host={summary.get('host') or 'UNSET'}",
            f"port={summary.get('port')}",
            f"database={summary.get('database') or 'UNSET'}",
            f"user={summary.get('user') or 'UNSET'}",
            f"table={summary.get('table_name')}",
            f"sslmode={summary.get('sslmode')}",
        ]
        return "; ".join(parts)
    return f"adapter={adapter_name}"


def _sql_status_for_upload_result(upload_result: SqlUploadResult) -> str:
    if upload_result.failed_count:
        return "failed"
    if upload_result.success_count:
        return "uploaded"
    return "not_uploaded"


def _sanitize_upload_result(upload_result: SqlUploadResult) -> SqlUploadResult:
    return SqlUploadResult(
        sql_batch_id=upload_result.sql_batch_id,
        adapter_name=upload_result.adapter_name,
        success_count=upload_result.success_count,
        failed_count=upload_result.failed_count,
        skipped_count=upload_result.skipped_count,
        uploaded_items=list(upload_result.uploaded_items),
        failed_items=[
            SqlUploadFailedItem(
                paper_id=item.paper_id,
                error=_sanitize_sql_upload_text(item.error),
            )
            for item in upload_result.failed_items
        ],
        skipped_items=[
            _sanitize_sql_upload_text(item) for item in upload_result.skipped_items
        ],
    )


def _sanitize_sql_upload_text(text: str) -> str:
    redacted = text
    redacted = re.sub(r"://([^:/@\s]+):([^/@\s]+)@", r"://\1:<redacted>@", redacted)
    redacted = re.sub(
        r"(?i)\bauthorization\s*:\s*bearer\s+[^;\s]+",
        "<redacted:credential>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)\bbearer\s+[^;\s]+",
        "<redacted:credential>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)\b(password|token|api_key|secret|dsn)=([^;,\s]+)",
        r"\1=<redacted>",
        redacted,
    )
    return redacted


def _load_metadata(path: str | Path) -> PaperMetadata | None:
    candidate = Path(path)
    if not candidate.exists():
        return None
    try:
        return metadata_from_json(candidate.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, KeyError):
        return None
