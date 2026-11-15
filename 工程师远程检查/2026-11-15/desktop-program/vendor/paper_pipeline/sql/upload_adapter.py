"""SQL upload adapter boundary.

The default adapter is a fake in-memory uploader. It never connects to a real
database and is intended for task 12 preview/confirmation tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from paper_pipeline.sql.sql_models import SqlUploadPreview, SqlUploadPreviewItem


@dataclass(frozen=True, slots=True)
class SqlUploadedItem:
    paper_id: str
    accession_id: str
    version: int
    record_id: str


@dataclass(frozen=True, slots=True)
class SqlUploadFailedItem:
    paper_id: str
    error: str


@dataclass(frozen=True, slots=True)
class SqlUploadResult:
    sql_batch_id: str
    adapter_name: str
    success_count: int
    failed_count: int
    skipped_count: int
    uploaded_items: list[SqlUploadedItem] = field(default_factory=list)
    failed_items: list[SqlUploadFailedItem] = field(default_factory=list)
    skipped_items: list[str] = field(default_factory=list)


class SqlUploadAdapter(Protocol):
    adapter_name: str

    def upload(
        self,
        *,
        preview: SqlUploadPreview,
        items: list[SqlUploadPreviewItem],
        sql_batch_id: str,
    ) -> SqlUploadResult:
        """Upload preview items and return a structured result."""


@dataclass(slots=True)
class FakeSqlUploadAdapter:
    fail_paper_ids: set[str] = field(default_factory=set)
    adapter_name: str = "fake_sql_uploader"
    upload_call_count: int = 0

    def upload(
        self,
        *,
        preview: SqlUploadPreview,
        items: list[SqlUploadPreviewItem],
        sql_batch_id: str,
    ) -> SqlUploadResult:
        self.upload_call_count += 1
        uploaded: list[SqlUploadedItem] = []
        failed: list[SqlUploadFailedItem] = []

        for item in items:
            if item.paper_id in self.fail_paper_ids or item.accession_id in self.fail_paper_ids:
                failed.append(
                    SqlUploadFailedItem(
                        paper_id=item.paper_id,
                        error="fake adapter configured failure",
                    ),
                )
                continue
            uploaded.append(
                SqlUploadedItem(
                    paper_id=item.paper_id,
                    accession_id=item.accession_id,
                    version=item.version,
                    record_id=f"fake-{item.accession_id}-v{item.version}",
                ),
            )

        return SqlUploadResult(
            sql_batch_id=sql_batch_id,
            adapter_name=self.adapter_name,
            success_count=len(uploaded),
            failed_count=len(failed),
            skipped_count=0,
            uploaded_items=uploaded,
            failed_items=failed,
            skipped_items=[],
        )
