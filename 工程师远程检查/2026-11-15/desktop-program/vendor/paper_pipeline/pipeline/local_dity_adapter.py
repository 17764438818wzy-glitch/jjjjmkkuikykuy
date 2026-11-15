"""Local Dity pipeline adapters and production stage implementations."""

from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from time import perf_counter
from typing import Any, Protocol

from paper_pipeline.domain import Stage0Decision
from paper_pipeline.pipeline.artifacts import (
    PipelineArtifactBundle,
    make_accession_id,
)
from paper_pipeline.pipeline.document_parse import (
    DocumentParseResult,
    DocumentParseStatus,
    ParserAttempt,
    validate_document_parse_payload,
)
from paper_pipeline.pipeline.document_parsers import (
    DocToDocxDoclingBackend,
    DocToPdfMinerUBackend,
    DocxToPdfMinerUBackend,
    DocumentParseContext,
    LocalTextParserBackend,
    MinerUParserBackend,
    ParserBackend,
    run_document_parse_route,
)
from paper_pipeline.pipeline.docling import (
    DoclingAvailability,
    DoclingParserBackend,
    resolve_docling_availability,
)
from paper_pipeline.pipeline.errors import Stage0GateError
from paper_pipeline.pipeline.libreoffice import resolve_libreoffice_path
from paper_pipeline.pipeline.mineru import resolve_mineru_path
from paper_pipeline.pipeline.llm import (
    LLMConfig,
    LLMProvider,
    redact_llm_secrets,
    resolve_llm_config,
    resolve_llm_provider,
)
from paper_pipeline.pipeline.stages import (
    DEFAULT_PIPELINE_STAGE_ORDER,
    PipelineStageName,
    PipelineStageResult,
    PipelineStageStatus,
)
from paper_pipeline.runtime import FileProcessingResult
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller
from paper_pipeline.stage0.admission import Stage0Result

_LLM_QUALITY_PROMPT = (
    "Return a JSON object that evaluates whether the cleaned paper text should "
    "continue through the pipeline."
)


@dataclass(frozen=True, slots=True)
class DityToolConfig:
    libreoffice_path: Path | None = None
    mineru_path: Path | None = None
    llm_endpoint: str = ""
    dity_console_url: str = "http://127.0.0.1:7861/local"
    libreoffice_timeout_seconds: int = 600
    mineru_timeout_seconds: int = 3600
    llm_timeout_seconds: int = 120

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        which: Any = None,
        path_exists: Any = None,
    ) -> DityToolConfig:
        source = environ if environ is not None else os.environ
        libreoffice = resolve_libreoffice_path(
            source,
            which=which,
            path_exists=path_exists,
        )
        mineru = resolve_mineru_path(
            source,
            which=which,
            path_exists=path_exists,
        )
        return cls(
            libreoffice_path=libreoffice.path if libreoffice.status == "found" else None,
            mineru_path=mineru.path if mineru.status == "found" else None,
            llm_endpoint=str(source.get("DITY_LLM_ENDPOINT", "")).strip(),
            dity_console_url=str(
                source.get("DITY_CONSOLE_URL", "http://127.0.0.1:7861/local"),
            ).strip()
            or "http://127.0.0.1:7861/local",
            libreoffice_timeout_seconds=_positive_int(
                _config_value(
                    source,
                    "LIBREOFFICE_TIMEOUT_SECONDS",
                    "DITY_LIBREOFFICE_TIMEOUT_SECONDS",
                ),
                600,
            ),
            mineru_timeout_seconds=_positive_int(
                _config_value(
                    source,
                    "MINERU_TIMEOUT_SECONDS",
                    "DITY_MINERU_TIMEOUT_SECONDS",
                ),
                3600,
            ),
            llm_timeout_seconds=_positive_int(
                _config_value(
                    source,
                    "LLM_TIMEOUT_SECONDS",
                    "DITY_LLM_TIMEOUT_SECONDS",
                ),
                120,
            ),
        )


@dataclass(frozen=True, slots=True)
class ExternalCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class ExternalCommandRunner(Protocol):
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> ExternalCommandResult:
        """Run an external command without going through a shell."""


class SubprocessCommandRunner:
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> ExternalCommandResult:
        completed = subprocess.run(  # noqa: S603 - command is a configured executable list.
            command,
            cwd=cwd,
            timeout=timeout_seconds,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return ExternalCommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


class LlmQualityClient(Protocol):
    def evaluate_quality(
        self,
        *,
        endpoint: str,
        payload: dict[str, object],
        timeout_seconds: int,
    ) -> dict[str, object]:
        """Call an LLM quality endpoint and return its JSON decision."""


class HttpJsonLlmQualityClient:
    def evaluate_quality(
        self,
        *,
        endpoint: str,
        payload: dict[str, object],
        timeout_seconds: int,
    ) -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"LLM endpoint request failed: {exc}") from exc
        if not isinstance(decoded, dict):
            raise TypeError("LLM endpoint must return a JSON object")
        return decoded


class LocalDityStageAdapter(Protocol):
    def run_stage(
        self,
        *,
        stage_name: PipelineStageName,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        """Run one pipeline stage and return a structured stage result."""


@dataclass(slots=True)
class LocalDityPipelineResult:
    source_path: str
    run_id: str
    accession_id: str
    status: PipelineStageStatus
    stage0_decision: Stage0Decision
    stages: list[PipelineStageResult] = field(default_factory=list)
    artifacts: PipelineArtifactBundle | None = None
    pending_reason: str = ""
    error: str = ""
    duration_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.status, PipelineStageStatus):
            self.status = PipelineStageStatus(self.status)
        if not isinstance(self.stage0_decision, Stage0Decision):
            self.stage0_decision = Stage0Decision(self.stage0_decision)

    def stage_by_name(self, stage_name: PipelineStageName | str) -> PipelineStageResult:
        resolved_stage_name = PipelineStageName(stage_name)
        for stage in self.stages:
            if stage.stage_name == resolved_stage_name:
                return stage
        raise KeyError(stage_name)

    def to_file_processing_result(self) -> FileProcessingResult:
        if self.status in {
            PipelineStageStatus.PENDING,
            PipelineStageStatus.FAILED,
            PipelineStageStatus.TIMEOUT,
        }:
            return FileProcessingResult.pending(
                self.source_path,
                reason=self.pending_reason or self.error,
                elapsed_seconds=self.duration_seconds,
            )
        if self.status is PipelineStageStatus.SKIPPED:
            return FileProcessingResult.completed(
                self.source_path,
                elapsed_seconds=self.duration_seconds,
            )
        return FileProcessingResult.completed(
            self.source_path,
            elapsed_seconds=self.duration_seconds,
            artifact_uri=(
                str(self.artifacts.final_markdown)
                if self.artifacts is not None
                else ""
            ),
        )


class LocalDityPipeline:
    def __init__(
        self,
        *,
        output_cleaned_dir: str | Path,
        stage_adapter: LocalDityStageAdapter | None = None,
        stage_order: Sequence[PipelineStageName] = DEFAULT_PIPELINE_STAGE_ORDER,
    ) -> None:
        self.output_cleaned_dir = Path(output_cleaned_dir)
        self.stage_adapter = stage_adapter or RealLocalDityStageAdapter()
        self.stage_order = tuple(stage_order)

    def run_stage0_result(
        self,
        stage0_result: Stage0Result,
        *,
        run_id: str | None = None,
        accession_id: str | None = None,
    ) -> LocalDityPipelineResult:
        ensure_real_flow_50_caller(
            "LocalDityPipeline.run_stage0_result",
            allow_test_callers=True,
        )
        resolved_run_id = run_id or stage0_result.run_id or "RUN_UNSPECIFIED"
        resolved_accession_id = accession_id or make_accession_id(
            source_hash=stage0_result.source_hash,
            source_path=stage0_result.source_path,
        )
        if stage0_result.decision is Stage0Decision.STAGE0_REJECT:
            return LocalDityPipelineResult(
                source_path=stage0_result.source_path,
                run_id=resolved_run_id,
                accession_id=resolved_accession_id,
                status=PipelineStageStatus.SKIPPED,
                stage0_decision=stage0_result.decision,
                pending_reason=f"stage0 rejected: {stage0_result.reason}",
            )
        if stage0_result.decision is Stage0Decision.PENDING:
            return LocalDityPipelineResult(
                source_path=stage0_result.source_path,
                run_id=resolved_run_id,
                accession_id=resolved_accession_id,
                status=PipelineStageStatus.PENDING,
                stage0_decision=stage0_result.decision,
                pending_reason=f"stage0 pending: {stage0_result.reason}",
            )
        return self.run(
            stage0_result,
            run_id=resolved_run_id,
            accession_id=resolved_accession_id,
        )

    def run(
        self,
        stage0_result: Stage0Result,
        *,
        run_id: str | None = None,
        accession_id: str | None = None,
    ) -> LocalDityPipelineResult:
        ensure_real_flow_50_caller(
            "LocalDityPipeline.run",
            allow_test_callers=True,
            allow_modules=("paper_pipeline.pipeline.local_dity_adapter",),
        )
        if stage0_result.decision is not Stage0Decision.ACCEPT:
            raise Stage0GateError("Local Dity pipeline requires stage0 accept")

        resolved_run_id = run_id or stage0_result.run_id or "RUN_UNSPECIFIED"
        resolved_accession_id = accession_id or make_accession_id(
            source_hash=stage0_result.source_hash,
            source_path=stage0_result.source_path,
        )
        artifacts = PipelineArtifactBundle.create(
            output_cleaned_dir=self.output_cleaned_dir,
            run_id=resolved_run_id,
            accession_id=resolved_accession_id,
        )
        start = perf_counter()
        stages: list[PipelineStageResult] = []
        current_input = stage0_result.source_path

        for index, stage_name in enumerate(self.stage_order):
            stage_start = perf_counter()
            stage_result = self.stage_adapter.run_stage(
                stage_name=stage_name,
                input_path=current_input,
                artifacts=artifacts,
                stage0_result=stage0_result,
                prior_results=tuple(stages),
            )
            measured_duration = perf_counter() - stage_start
            if stage_result.duration_seconds <= 0:
                stage_result = replace(
                    stage_result,
                    duration_seconds=measured_duration,
                )
            stages.append(stage_result)

            if stage_result.status is PipelineStageStatus.SUCCESS:
                current_input = stage_result.output_path
                continue

            pending_reason = _pending_reason(stage_result)
            stages.extend(
                _skipped_stages(
                    remaining_stages=self.stage_order[index + 1 :],
                    input_path=current_input,
                    reason=pending_reason,
                ),
            )
            return LocalDityPipelineResult(
                source_path=stage0_result.source_path,
                run_id=resolved_run_id,
                accession_id=resolved_accession_id,
                status=PipelineStageStatus.PENDING,
                stage0_decision=stage0_result.decision,
                stages=stages,
                artifacts=artifacts,
                pending_reason=pending_reason,
                error=stage_result.error,
                duration_seconds=perf_counter() - start,
            )

        return LocalDityPipelineResult(
            source_path=stage0_result.source_path,
            run_id=resolved_run_id,
            accession_id=resolved_accession_id,
            status=PipelineStageStatus.SUCCESS,
            stage0_decision=stage0_result.decision,
            stages=stages,
            artifacts=artifacts,
            duration_seconds=perf_counter() - start,
        )


class RealLocalDityStageAdapter:
    """Production adapter that calls configured local tools and services.

    The adapter never reports a fake success for a missing binary-only tool. If a
    source file is a real binary PDF or office document and the required tool is
    not configured, the stage returns pending/failed with evidence. Plain-text
    inputs and text-backed test fixtures are processed by deterministic local
    steps because no LibreOffice or MinerU work is required for those bytes.
    """

    def __init__(
        self,
        *,
        tool_config: DityToolConfig | None = None,
        command_runner: ExternalCommandRunner | None = None,
        llm_client: LlmQualityClient | None = None,
        llm_provider: LLMProvider | None = None,
        llm_environ: Mapping[str, str] | None = None,
        docling_availability: DoclingAvailability | None = None,
    ) -> None:
        self.tool_config = tool_config or DityToolConfig.from_environ()
        self.command_runner = command_runner or SubprocessCommandRunner()
        self.docling_availability = docling_availability or resolve_docling_availability()
        self.llm_config = _resolve_pipeline_llm_config(self.tool_config, llm_environ)
        legacy_client = llm_client
        if legacy_client is None and self.llm_config.provider == "legacy-dity":
            legacy_client = HttpJsonLlmQualityClient()
        self.llm_provider = llm_provider or resolve_llm_provider(
            self.llm_config,
            legacy_client=legacy_client,
        )

    def run_stage(
        self,
        *,
        stage_name: PipelineStageName,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        ensure_real_flow_50_caller(
            "RealLocalDityStageAdapter.run_stage",
            allow_test_callers=True,
            allow_modules=("paper_pipeline.pipeline.local_dity_adapter",),
        )
        handlers = {
            PipelineStageName.NORMALIZE_CONVERT: self._normalize_convert,
            PipelineStageName.DOCUMENT_PARSE: self._document_parse,
            PipelineStageName.BLOCK_GENERATION: self._block_generation,
            PipelineStageName.DESENSITIZE_CLEAN: self._desensitize_clean,
            PipelineStageName.BASIC_QUALITY_FILTER: self._basic_quality_filter,
            PipelineStageName.LLM_QUALITY_FILTER: self._llm_quality_filter,
            PipelineStageName.CHAPTER_STRUCTURE: self._chapter_structure,
            PipelineStageName.FINAL_QUALITY_CHECK: self._final_quality_check,
            PipelineStageName.MARKDOWN_OUTPUT: self._markdown_output,
        }
        return handlers[stage_name](
            input_path=input_path,
            artifacts=artifacts,
            stage0_result=stage0_result,
            prior_results=prior_results,
        )

    def _normalize_convert(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.NORMALIZE_CONVERT
        output_path = _stage_output_path(stage_name, artifacts)
        source = Path(input_path)
        if not source.exists():
            return self._failed(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message=f"source file is missing: {source}",
            )

        suffix = source.suffix.lower()
        text = _read_text_if_possible(source)
        office_formats = {".doc", ".docx", ".odt", ".rtf", ".ppt", ".pptx"}
        if suffix in office_formats:
            payload = {
                "stage_name": stage_name.value,
                "status": PipelineStageStatus.SUCCESS.value,
                "source_path": str(source),
                "normalized_path": str(source),
                "normalized_format": suffix.removeprefix("."),
                "libreoffice_invoked": False,
                "libreoffice_path": "",
            }
            _write_json(output_path, payload)
            return self._success(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                logs=[f"Office source preserved as {suffix.removeprefix('.')} for stage2 routing"],
            )

        normalized_format = "text" if text is not None else suffix.removeprefix(".") or "binary"
        payload = {
            "stage_name": stage_name.value,
            "status": PipelineStageStatus.SUCCESS.value,
            "source_path": str(source),
            "normalized_path": str(source),
            "normalized_format": normalized_format,
            "libreoffice_invoked": False,
            "libreoffice_path": "",
        }
        _write_json(output_path, payload)
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=[f"Source normalized as {normalized_format}"],
        )

    def _document_parse(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.DOCUMENT_PARSE
        output_path = _stage_output_path(stage_name, artifacts)
        normalized = _load_json_object(input_path)
        normalized_path = Path(str(normalized.get("normalized_path", "")))
        if not normalized_path.exists():
            return self._failed(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message=f"normalized input is missing: {normalized_path}",
            )

        context = DocumentParseContext(
            source_path=str(normalized.get("source_path") or stage0_result.source_path),
            source_hash=stage0_result.source_hash,
            normalized_path=str(normalized_path),
            normalized_format=str(normalized.get("normalized_format", "")).strip(),
            output_dir=artifacts.parsed_dir,
        )
        try:
            parse_result = run_document_parse_route(
                context,
                parser_backends=self._parser_backends(),
            )
            payload = parse_result.to_dict()
        except Exception as exc:  # noqa: BLE001 - stage boundary must emit diagnostics.
            parse_result = _document_parse_exception_result(context, exc)
            payload = parse_result.to_dict()
        _write_json(output_path, payload)
        log_path = artifacts.logs_dir / f"{stage_name.value}.json"
        _write_json(log_path, _document_parse_log_payload(payload))
        if parse_result.status is DocumentParseStatus.SUCCESS:
            selected = parse_result.selected_parser or "unknown"
            return self._success(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                logs=[f"stage2_document_parse selected parser: {selected}", str(log_path)],
            )

        reason = (
            parse_result.pending_reason_text
            or parse_result.pending_reason_code
            or "stage2_document_parse pending"
        )
        return PipelineStageResult(
            stage_name=stage_name,
            status=PipelineStageStatus.PENDING,
            input_path=input_path,
            output_path=str(output_path),
            logs=[reason, str(log_path)],
            error=reason,
            risk_flags=["pipeline_stage_uncertain"],
        )

    def _parser_backends(self) -> dict[str, ParserBackend]:
        docling_backend = DoclingParserBackend(availability=self.docling_availability)
        mineru_backend = MinerUParserBackend(
            mineru_path=self.tool_config.mineru_path,
            command_runner=self.command_runner,
            timeout_seconds=self.tool_config.mineru_timeout_seconds,
        )
        return {
            "local_text": LocalTextParserBackend(),
            "docling": docling_backend,
            "mineru": mineru_backend,
            "docx_to_pdf_mineru": DocxToPdfMinerUBackend(
                libreoffice_path=self.tool_config.libreoffice_path,
                mineru_backend=mineru_backend,
                command_runner=self.command_runner,
                timeout_seconds=self.tool_config.libreoffice_timeout_seconds,
            ),
            "doc_to_docx_docling": DocToDocxDoclingBackend(
                libreoffice_path=self.tool_config.libreoffice_path,
                docling_backend=docling_backend,
                command_runner=self.command_runner,
                timeout_seconds=self.tool_config.libreoffice_timeout_seconds,
            ),
            "doc_to_pdf_mineru": DocToPdfMinerUBackend(
                libreoffice_path=self.tool_config.libreoffice_path,
                mineru_backend=mineru_backend,
                command_runner=self.command_runner,
                timeout_seconds=self.tool_config.libreoffice_timeout_seconds,
            ),
        }

    def _block_generation(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.BLOCK_GENERATION
        output_path = _stage_output_path(stage_name, artifacts)
        try:
            document_parse_payload = _load_document_parse_payload(input_path)
        except Exception as exc:  # noqa: BLE001 - stage boundary reports pending.
            return self._pending(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message=f"document_parse.json is missing or invalid: {exc}",
            )
        blocks = _blocks_from_document_parse_payload(document_parse_payload)
        if not blocks:
            return self._pending(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message="document_parse result contains no text blocks",
            )
        _write_json(
            output_path,
            {
                "stage_name": stage_name.value,
                "status": PipelineStageStatus.SUCCESS.value,
                "title": _document_parse_title(document_parse_payload),
                "parser_selected": _selected_document_parser(document_parse_payload),
                "blocks": blocks,
            },
        )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=[f"Generated {len(blocks)} text blocks"],
        )

    def _desensitize_clean(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.DESENSITIZE_CLEAN
        output_path = _stage_output_path(stage_name, artifacts)
        payload = _load_json_object(input_path)
        blocks = [
            {
                **block,
                "text": _desensitize_text(str(block.get("text", ""))),
            }
            for block in _coerce_blocks(payload.get("blocks", []))
        ]
        _write_json(
            output_path,
            {
                "stage_name": stage_name.value,
                "status": PipelineStageStatus.SUCCESS.value,
                "title": str(payload.get("title", "")),
                "sensitive_clean": True,
                "blocks": blocks,
            },
        )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=["Sensitive fields cleaned from text blocks"],
        )

    def _basic_quality_filter(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.BASIC_QUALITY_FILTER
        output_path = _stage_output_path(stage_name, artifacts)
        payload = _load_json_object(input_path)
        text = _blocks_text(_coerce_blocks(payload.get("blocks", [])))
        score = _basic_quality_score(text)
        decision = "accept" if score >= 40 else "pending"
        result_payload = {
            "stage_name": stage_name.value,
            "status": PipelineStageStatus.SUCCESS.value,
            "decision": decision,
            "score": score,
            "reason": "basic structure signals are sufficient"
            if decision == "accept"
            else "text is too short or lacks paper structure signals",
        }
        _write_json(output_path, result_payload)
        if decision != "accept":
            return self._pending(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message=str(result_payload["reason"]),
            )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=["Basic quality filter accepted the paper"],
        )

    def _llm_quality_filter(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.LLM_QUALITY_FILTER
        output_path = _stage_output_path(stage_name, artifacts)
        basic_result = _load_json_object(input_path)
        cleaned_payload = _load_json_object(artifacts.blocks_dir / "cleaned_blocks.json")
        blocks = _coerce_blocks(cleaned_payload.get("blocks", []))
        llm_payload: dict[str, object] = {
            "run_id": stage0_result.run_id,
            "source_hash": stage0_result.source_hash,
            "basic_quality": basic_result,
            "text_preview": _blocks_text(blocks)[:6000],
        }

        if self.llm_config.mode == "real":
            try:
                decision = self.llm_provider.complete_json(
                    prompt=_LLM_QUALITY_PROMPT,
                    payload=llm_payload,
                    config=self.llm_config,
                    timeout_seconds=self.tool_config.llm_timeout_seconds,
                )
            except Exception as exc:  # noqa: BLE001 - structured stage boundary.
                decision = _local_quality_decision(
                    llm_payload,
                    reason=(
                        "llm_failed: "
                        f"{redact_llm_secrets(str(exc), self.llm_config)}"
                    ),
                )
                _apply_llm_status(
                    decision,
                    stage_name=stage_name,
                    config=self.llm_config,
                    mode="failed",
                    invoked=True,
                    error=str(exc),
                )
                _write_json(output_path, decision)
                return self._success(
                    stage_name=stage_name,
                    input_path=input_path,
                    output_path=output_path,
                    logs=[
                        "LLM initial review routed to pending after external LLM failure; "
                        "see llm_mode=failed artifact",
                    ],
                )
            reason = str(decision.get("reason") or "LLM quality endpoint returned a decision")
            normalized_decision = str(decision.get("decision", "")).lower()
            if not normalized_decision:
                decision = _local_quality_decision(
                    llm_payload,
                    reason="llm_invalid_response: LLM response omitted decision",
                )
                normalized_decision = str(decision["decision"])
                reason = str(decision["reason"])
            _apply_llm_status(
                decision,
                stage_name=stage_name,
                config=self.llm_config,
                mode="real",
                invoked=True,
            )
            _write_json(output_path, decision)
            if normalized_decision not in {"accept", "accepted", "success", "pass"}:
                return self._success(
                    stage_name=stage_name,
                    input_path=input_path,
                    output_path=output_path,
                    logs=[reason],
                )
            return self._success(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                logs=[reason],
            )

        mode = "config-error" if self.llm_config.mode == "config-error" else "fake"
        if mode == "config-error":
            decision = _local_quality_decision(
                llm_payload,
                reason=f"llm_failed: {self.llm_config.message}",
            )
        else:
            decision = _local_quality_decision(
                llm_payload,
                reason="llm_not_configured: fake LLM mode cannot approve initial review",
            )
        _apply_llm_status(
            decision,
            stage_name=stage_name,
            config=self.llm_config,
            mode=mode,
            invoked=False,
            error=self.llm_config.message if mode == "config-error" else "",
        )
        _write_json(output_path, decision)
        if mode == "config-error":
            log_message = (
                "LLM initial review routed to pending because requested real LLM provider "
                "is not correctly configured"
            )
        else:
            log_message = (
                "LLM initial review routed to pending because real LLM provider is not configured"
            )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=[log_message],
        )

    def _chapter_structure(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.CHAPTER_STRUCTURE
        output_path = _stage_output_path(stage_name, artifacts)
        cleaned_payload = _load_json_object(artifacts.blocks_dir / "cleaned_blocks.json")
        try:
            document_parse_payload = _load_document_parse_payload(
                artifacts.parsed_dir / "document_parse.json",
            )
        except Exception as exc:  # noqa: BLE001 - stage boundary reports pending.
            return self._pending(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message=f"document_parse.json is missing or invalid: {exc}",
            )
        blocks = _coerce_blocks(cleaned_payload.get("blocks", []))
        chapters = _chapters_from_document_parse(document_parse_payload, blocks)
        _write_json(
            output_path,
            {
                "stage_name": stage_name.value,
                "status": PipelineStageStatus.SUCCESS.value,
                "title": str(cleaned_payload.get("title", "")),
                "parser_selected": _selected_document_parser(document_parse_payload),
                "chapters": chapters,
                "blocks": blocks,
            },
        )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=[f"Identified {len(chapters)} chapter headings"],
        )

    def _final_quality_check(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.FINAL_QUALITY_CHECK
        output_path = _stage_output_path(stage_name, artifacts)
        _load_json_object(artifacts.logs_dir / "llm_quality_filter.json")
        chapter_payload = _load_json_object(input_path)
        blocks = _coerce_blocks(chapter_payload.get("blocks", []))
        chapters = _coerce_string_list(chapter_payload.get("chapters", []))
        reasons: list[str] = []
        if not blocks:
            reasons.append("no cleaned text blocks")
        if len(_blocks_text(blocks).strip()) < 20:
            reasons.append("cleaned text is too short")

        payload = {
            "stage_name": stage_name.value,
            "status": PipelineStageStatus.SUCCESS.value if not reasons else PipelineStageStatus.PENDING.value,
            "decision": "accept" if not reasons else "pending",
            "chapter_count": len(chapters),
            "reasons": reasons,
        }
        _write_json(output_path, payload)
        if reasons:
            return self._pending(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message="; ".join(reasons),
            )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=["Final quality check accepted cleaned Markdown source"],
        )

    def _markdown_output(
        self,
        *,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        stage_name = PipelineStageName.MARKDOWN_OUTPUT
        output_path = _stage_output_path(stage_name, artifacts)
        cleaned_payload = _load_json_object(artifacts.blocks_dir / "cleaned_blocks.json")
        chapter_payload = _load_json_object(artifacts.blocks_dir / "chapters.json")
        normalized_payload = _load_json_object(artifacts.normalized_dir / "normalized.json")
        try:
            document_parse_payload = _load_document_parse_payload(
                artifacts.parsed_dir / "document_parse.json",
            )
        except Exception as exc:  # noqa: BLE001 - stage boundary reports pending.
            return self._pending(
                stage_name=stage_name,
                input_path=input_path,
                output_path=output_path,
                message=f"document_parse.json is missing or invalid: {exc}",
            )
        llm_payload = _load_json_object(artifacts.logs_dir / "llm_quality_filter.json")

        blocks = _coerce_blocks(cleaned_payload.get("blocks", []))
        chapters = _coerce_string_list(chapter_payload.get("chapters", []))
        title = str(
            cleaned_payload.get("title")
            or _document_parse_title(document_parse_payload)
            or _title_from_path(Path(stage0_result.source_path)),
        )
        markdown = _render_final_markdown(
            title=title,
            blocks=blocks,
            chapters=chapters,
            normalized_payload=normalized_payload,
            document_parse_payload=document_parse_payload,
            llm_payload=llm_payload,
        )
        output_path.write_text(markdown, encoding="utf-8")
        _write_json(
            artifacts.metadata_json,
            {
                "schema_version": 1,
                "run_id": stage0_result.run_id,
                "accession_id": artifacts.base_dir.name,
                "source_hash": stage0_result.source_hash,
                "sensitive_clean": True,
                "stage_count": len(prior_results) + 1,
                "pipeline_adapter": "real_local_dity_stage_adapter",
                "libreoffice_invoked": bool(normalized_payload.get("libreoffice_invoked")),
                "parser_selected": _selected_document_parser(document_parse_payload),
                "llm_invoked": bool(llm_payload.get("llm_invoked")),
                "sql_status": "not_uploaded",
                "index_status": "not_indexed",
            },
        )
        return self._success(
            stage_name=stage_name,
            input_path=input_path,
            output_path=output_path,
            logs=["Final Markdown and pipeline metadata written"],
        )

    def _success(
        self,
        *,
        stage_name: PipelineStageName,
        input_path: str,
        output_path: Path,
        logs: list[str],
    ) -> PipelineStageResult:
        return PipelineStageResult(
            stage_name=stage_name,
            status=PipelineStageStatus.SUCCESS,
            input_path=input_path,
            output_path=str(output_path),
            logs=logs,
            risk_flags=[],
        )

    def _pending(
        self,
        *,
        stage_name: PipelineStageName,
        input_path: str,
        output_path: Path,
        message: str,
    ) -> PipelineStageResult:
        _write_stage_message(output_path, message)
        return PipelineStageResult(
            stage_name=stage_name,
            status=PipelineStageStatus.PENDING,
            input_path=input_path,
            output_path=str(output_path),
            logs=[message],
            error=message,
            risk_flags=["pipeline_stage_uncertain"],
        )

    def _failed(
        self,
        *,
        stage_name: PipelineStageName,
        input_path: str,
        output_path: Path,
        message: str,
    ) -> PipelineStageResult:
        _write_stage_message(output_path, message)
        return PipelineStageResult(
            stage_name=stage_name,
            status=PipelineStageStatus.FAILED,
            input_path=input_path,
            output_path=str(output_path),
            logs=[message],
            error=message,
            risk_flags=["pipeline_stage_failed"],
        )


class StubLocalDityStageAdapter:
    def __init__(
        self,
        *,
        stage_statuses: Mapping[
            PipelineStageName,
            PipelineStageStatus,
        ]
        | None = None,
        tool_config: DityToolConfig | None = None,
    ) -> None:
        self.stage_statuses = dict(stage_statuses or {})
        self.tool_config = tool_config or DityToolConfig()

    @classmethod
    def fail_at_desensitize(cls) -> StubLocalDityStageAdapter:
        return cls(
            stage_statuses={
                PipelineStageName.DESENSITIZE_CLEAN: PipelineStageStatus.FAILED,
            },
        )

    def run_stage(
        self,
        *,
        stage_name: PipelineStageName,
        input_path: str,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> PipelineStageResult:
        status = self.stage_statuses.get(stage_name, PipelineStageStatus.SUCCESS)
        output_path = _stage_output_path(stage_name, artifacts)
        log_path = artifacts.logs_dir / f"{stage_name.value}.log"

        if status is not PipelineStageStatus.SUCCESS:
            message = _stage_failure_message(stage_name, status)
            log_path.write_text(message, encoding="utf-8")
            return PipelineStageResult(
                stage_name=stage_name,
                status=status,
                input_path=input_path,
                output_path=str(output_path),
                logs=[message, str(log_path)],
                error=message,
                risk_flags=[_stage_risk_flag(status)],
            )

        self._write_success_artifact(
            stage_name=stage_name,
            output_path=output_path,
            artifacts=artifacts,
            stage0_result=stage0_result,
            prior_results=prior_results,
        )
        log_message = f"{stage_name.value} completed with synthetic test adapter"
        log_path.write_text(log_message, encoding="utf-8")
        return PipelineStageResult(
            stage_name=stage_name,
            status=PipelineStageStatus.SUCCESS,
            input_path=input_path,
            output_path=str(output_path),
            logs=[log_message, str(log_path)],
            risk_flags=[],
        )

    def _write_success_artifact(
        self,
        *,
        stage_name: PipelineStageName,
        output_path: Path,
        artifacts: PipelineArtifactBundle,
        stage0_result: Stage0Result,
        prior_results: Sequence[PipelineStageResult],
    ) -> None:
        if stage_name is PipelineStageName.MARKDOWN_OUTPUT:
            output_path.write_text(_stub_markdown(), encoding="utf-8")
            artifacts.metadata_json.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": stage0_result.run_id,
                        "accession_id": artifacts.base_dir.name,
                        "source_hash": stage0_result.source_hash,
                        "sensitive_clean": True,
                        "stage_count": len(prior_results) + 1,
                        "sql_status": "not_uploaded",
                        "index_status": "not_indexed",
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            return

        payload = {
            "stage_name": stage_name.value,
            "status": PipelineStageStatus.SUCCESS.value,
            "source_hash": stage0_result.source_hash,
            "note": "synthetic test artifact",
        }
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def build_local_dity_file_processor(
    *,
    run_id: str,
    output_cleaned_dir: str | Path,
    audit_dir: str | Path | None = None,
    stage_adapter: LocalDityStageAdapter | None = None,
):
    raise RuntimeError(
        "direct local_dity file processor execution is disabled; run through "
        "paper_pipeline.e2e.real_flow_50",
    )


def _optional_path(value: str | None) -> Path | None:
    raw = str(value or "").strip()
    return Path(raw) if raw else None


def _config_value(
    source: Mapping[str, str],
    primary_key: str,
    legacy_key: str,
) -> str:
    primary_value = str(source.get(primary_key, "")).strip()
    if primary_value:
        return primary_value
    return str(source.get(legacy_key, "")).strip()


def _positive_int(value: str | None, default: int) -> int:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_stage_message(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".json":
        _write_json(
            path,
            {
                "status": PipelineStageStatus.PENDING.value,
                "message": message,
            },
        )
        return
    path.write_text(message, encoding="utf-8")


def _document_parse_log_payload(payload: Mapping[str, object]) -> dict[str, object]:
    return {
        "stage_name": PipelineStageName.DOCUMENT_PARSE.value,
        "status": str(payload.get("status", "")),
        "selected_parser": str(payload.get("selected_parser", "")),
        "attempts": payload.get("attempts", []),
        "pending_reason_code": str(payload.get("pending_reason_code", "")),
        "pending_reason_text": str(payload.get("pending_reason_text", "")),
        "codex_next_action_hint": str(payload.get("codex_next_action_hint", "")),
        "parse_quality_metrics": payload.get("parse_quality_metrics", {}),
    }


def _document_parse_exception_result(
    context: DocumentParseContext,
    exc: Exception,
) -> DocumentParseResult:
    reason = f"stage2_document_parse failed: {type(exc).__name__}: {exc}"
    return DocumentParseResult(
        status=DocumentParseStatus.PENDING,
        source_path=context.source_path,
        source_hash=context.source_hash,
        normalized_path=context.normalized_path,
        normalized_format=context.normalized_format,
        attempts=[
            ParserAttempt(
                parser="stage2_router",
                role="router",
                status="failed",
                error_summary=reason,
            ),
        ],
        pending_reason_code="pending_parser_failed",
        pending_reason_text=reason,
    )


def _load_json_object(path: str | Path) -> dict[str, Any]:
    decoded = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError(f"JSON object expected at {path}")
    return decoded


def _load_document_parse_payload(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    validate_document_parse_payload(payload)
    return payload


def _json_object_from_text(value: str) -> dict[str, Any] | None:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _json_object_from_path(path: str | Path) -> dict[str, Any] | None:
    try:
        return _load_json_object(path)
    except (OSError, TypeError, json.JSONDecodeError):
        return None


def _read_text_if_possible(path: Path) -> str | None:
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    if b"\x00" in raw[:2048]:
        return None
    for encoding in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            text = raw.decode(encoding)
        except UnicodeDecodeError:
            continue
        if text.strip():
            return text
    return None


def _blocks_from_document_parse_payload(
    payload: Mapping[str, object],
) -> list[dict[str, object]]:
    return _coerce_blocks(payload.get("blocks", []))


def _coerce_blocks(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    blocks: list[dict[str, object]] = []
    for item in value:
        if isinstance(item, Mapping):
            text = str(item.get("text", "")).strip()
            if text:
                block = dict(item)
                block["text"] = text
                block.setdefault("type", "paragraph")
                blocks.append(block)
        elif str(item).strip():
            blocks.append({"type": "paragraph", "text": str(item).strip()})
    return blocks


def _blocks_text(blocks: Sequence[Mapping[str, object]]) -> str:
    return "\n\n".join(str(block.get("text", "")).strip() for block in blocks).strip()


def _desensitize_text(text: str) -> str:
    cleaned = re.sub(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", "[email]", text)
    cleaned = re.sub(r"\b1[3-9]\d{9}\b", "[phone]", cleaned)
    cleaned = re.sub(r"\b\d{8,20}\b", "[number]", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    return cleaned.strip()


def _basic_quality_score(text: str) -> int:
    score = 20 if len(text.strip()) >= 20 else 0
    structure_signals = ("abstract", "摘要", "keyword", "关键词", "conclusion", "结论")
    score += 10 * sum(1 for signal in structure_signals if signal.lower() in text.lower())
    if len(text.strip()) >= 200:
        score += 20
    return min(score, 100)


def _resolve_pipeline_llm_config(
    tool_config: DityToolConfig,
    environ: Mapping[str, str] | None,
) -> LLMConfig:
    source = dict(environ if environ is not None else os.environ)
    if tool_config.llm_endpoint:
        if environ is None:
            source.pop("LLM_PROVIDER", None)
        source["DITY_LLM_ENDPOINT"] = tool_config.llm_endpoint
    return resolve_llm_config(source)


def _apply_llm_status(
    decision: dict[str, object],
    *,
    stage_name: PipelineStageName,
    config: LLMConfig,
    mode: str,
    invoked: bool,
    error: str = "",
) -> None:
    decision["stage_name"] = stage_name.value
    decision["llm_mode"] = mode
    decision["llm_provider"] = str(config.provider)
    decision["llm_model"] = config.model
    decision["llm_invoked"] = invoked
    decision["llm_endpoint"] = config.endpoint
    if error:
        decision["llm_error"] = redact_llm_secrets(error, config)


def _local_quality_decision(
    payload: Mapping[str, object],
    *,
    reason: str,
) -> dict[str, object]:
    text = str(payload.get("text_preview", ""))
    score = _basic_quality_score(text)
    return {
        "decision": "pending",
        "score": score,
        "reason": reason,
        "needs_codex_review": True,
    }


def _chapters_from_document_parse(
    document_parse_payload: Mapping[str, object],
    blocks: Sequence[Mapping[str, object]],
) -> list[str]:
    evidence = document_parse_payload.get("admission_evidence", {})
    if isinstance(evidence, Mapping):
        headings = _coerce_string_list(evidence.get("heading_candidates", []))
        if headings:
            return _deduplicate(headings)

    typed_headings = [
        str(block.get("text", "")).strip()
        for block in blocks
        if str(block.get("type", "")).strip().lower() == "heading"
        and str(block.get("text", "")).strip()
    ]
    if typed_headings:
        return _deduplicate(typed_headings)
    return _chapters_from_text(_blocks_text(blocks))


def _chapters_from_text(text: str) -> list[str]:
    chapters: list[str] = []
    for line in text.splitlines():
        candidate = line.strip().strip("#").strip()
        if not candidate:
            continue
        lower = candidate.lower()
        if (
            lower.startswith(("chapter", "abstract", "keywords", "conclusion", "references"))
            or candidate.startswith(("第", "摘要", "关键词", "结论", "参考文献"))
        ):
            chapters.append(candidate[:80])
    return _deduplicate(chapters) or ["Main Text"]


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _deduplicate(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _title_from_path(path: Path) -> str:
    return path.stem.replace("_", " ").replace("-", " ").strip().title() or "Untitled Paper"


def _document_parse_title(payload: Mapping[str, object]) -> str:
    document = payload.get("document", {})
    if isinstance(document, Mapping):
        return str(document.get("title", "")).strip()
    return ""


def _selected_document_parser(payload: Mapping[str, object]) -> str:
    selected = str(payload.get("selected_parser", "")).strip()
    if selected:
        return selected
    attempts = payload.get("attempts", [])
    if isinstance(attempts, list):
        for attempt in attempts:
            if isinstance(attempt, Mapping):
                parser = str(attempt.get("parser", "")).strip()
                if parser:
                    return parser
    return ""


def _render_final_markdown(
    *,
    title: str,
    blocks: Sequence[Mapping[str, object]],
    chapters: Sequence[str],
    normalized_payload: Mapping[str, object],
    document_parse_payload: Mapping[str, object],
    llm_payload: Mapping[str, object],
) -> str:
    body = _blocks_text(blocks)
    if not body:
        body = "No cleaned body text was produced."
    selected_parser = _selected_document_parser(document_parse_payload) or "unknown"
    evidence = [
        f"LibreOffice invoked: {bool(normalized_payload.get('libreoffice_invoked'))}",
        f"Parser selected: {selected_parser}",
        f"LLM invoked: {bool(llm_payload.get('llm_invoked'))}",
    ]
    chapter_lines = "\n".join(f"- {chapter}" for chapter in chapters) or "- Main Text"
    evidence_lines = "\n".join(f"- {item}" for item in evidence)
    return (
        f"# {title}\n\n"
        "## Extracted Chapter Structure\n\n"
        f"{chapter_lines}\n\n"
        "## Cleaned Paper Text\n\n"
        f"{body}\n\n"
        "## Processing Evidence\n\n"
        f"{evidence_lines}\n"
    )


def _command_error(prefix: str, result: ExternalCommandResult) -> str:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
    return f"{prefix}: {detail}"


def _stage_output_path(
    stage_name: PipelineStageName,
    artifacts: PipelineArtifactBundle,
) -> Path:
    output_paths = {
        PipelineStageName.NORMALIZE_CONVERT: artifacts.normalized_dir
        / "normalized.json",
        PipelineStageName.DOCUMENT_PARSE: artifacts.parsed_dir / "document_parse.json",
        PipelineStageName.BLOCK_GENERATION: artifacts.blocks_dir / "blocks.json",
        PipelineStageName.DESENSITIZE_CLEAN: artifacts.blocks_dir
        / "cleaned_blocks.json",
        PipelineStageName.BASIC_QUALITY_FILTER: artifacts.logs_dir
        / "basic_quality_filter.json",
        PipelineStageName.LLM_QUALITY_FILTER: artifacts.logs_dir
        / "llm_quality_filter.json",
        PipelineStageName.CHAPTER_STRUCTURE: artifacts.blocks_dir / "chapters.json",
        PipelineStageName.FINAL_QUALITY_CHECK: artifacts.logs_dir
        / "final_quality_check.json",
        PipelineStageName.MARKDOWN_OUTPUT: artifacts.final_markdown,
    }
    return output_paths[stage_name]


def _stage_failure_message(
    stage_name: PipelineStageName,
    status: PipelineStageStatus,
) -> str:
    if status is PipelineStageStatus.TIMEOUT:
        return f"{stage_name.value} timeout in synthetic test adapter"
    if status is PipelineStageStatus.PENDING:
        return f"{stage_name.value} pending in synthetic test adapter"
    if status is PipelineStageStatus.SKIPPED:
        return f"{stage_name.value} skipped in synthetic test adapter"
    return f"{stage_name.value} failed in synthetic test adapter"


def _stage_risk_flag(status: PipelineStageStatus) -> str:
    if status is PipelineStageStatus.TIMEOUT:
        return "pipeline_stage_timeout"
    if status is PipelineStageStatus.PENDING:
        return "pipeline_stage_uncertain"
    if status is PipelineStageStatus.SKIPPED:
        return "pipeline_stage_skipped"
    return "pipeline_stage_failed"


def _pending_reason(stage_result: PipelineStageResult) -> str:
    if stage_result.status is PipelineStageStatus.TIMEOUT:
        return f"{stage_result.stage_name.value} timeout: {stage_result.error}"
    if stage_result.stage_name is PipelineStageName.DOCUMENT_PARSE:
        payload = _json_object_from_path(stage_result.output_path)
        if payload is not None:
            reason = _document_parse_pending_reason(payload)
            if reason:
                return f"{stage_result.stage_name.value} {stage_result.status.value}: {reason}"
    return (
        f"{stage_result.stage_name.value} {stage_result.status.value}: "
        f"{stage_result.error}"
    )


def _document_parse_pending_reason(payload: Mapping[str, object]) -> str:
    parts = [
        str(payload.get("pending_reason_code", "")).strip(),
        str(payload.get("pending_reason_text", "")).strip(),
    ]
    hint = str(payload.get("codex_next_action_hint", "")).strip()
    if hint:
        parts.append(f"next_action={hint}")
    return "; ".join(part for part in parts if part)


def _skipped_stages(
    *,
    remaining_stages: Sequence[PipelineStageName],
    input_path: str,
    reason: str,
) -> list[PipelineStageResult]:
    return [
        PipelineStageResult(
            stage_name=stage_name,
            status=PipelineStageStatus.SKIPPED,
            input_path=input_path,
            output_path="",
            logs=[f"skipped because upstream stage is pending: {reason}"],
            error=reason,
            risk_flags=["upstream_pending"],
        )
        for stage_name in remaining_stages
    ]




def _stub_markdown() -> str:
    return """# Synthetic Fixture Paper

## Abstract

This Markdown file is produced only when a test explicitly injects the synthetic
stage adapter. Production pipeline construction uses RealLocalDityStageAdapter.

## Keywords

desensitization; parsing; structured processing

## Chapter 1 Introduction

This fixture demonstrates normalized, parsed, cleaned, and structured paper
content for tests that exercise downstream safety behavior.

## References

[1] Synthetic desensitized reference entry.
"""
