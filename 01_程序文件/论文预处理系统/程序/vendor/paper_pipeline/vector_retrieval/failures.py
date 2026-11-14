"""Failure and rebuild planning."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RebuildPlan:
    resource_id: str
    rebuild_chunks: bool
    rebuild_embeddings: bool
    reason: str
    dry_run: bool = True


class FailureRebuildService:
    def plan_for_change(self, resource_id: str, change_type: str) -> RebuildPlan:
        if change_type in {"markdown_changed", "chunk_rule_changed", "blocks_changed"}:
            return RebuildPlan(
                resource_id=resource_id,
                rebuild_chunks=True,
                rebuild_embeddings=True,
                reason=change_type,
            )
        if change_type == "embedding_model_changed":
            return RebuildPlan(
                resource_id=resource_id,
                rebuild_chunks=False,
                rebuild_embeddings=True,
                reason=change_type,
            )
        if change_type in {"withdrawn", "replaced"}:
            return RebuildPlan(
                resource_id=resource_id,
                rebuild_chunks=False,
                rebuild_embeddings=False,
                reason="exclude_old_vectors_keep_traceability",
            )
        return RebuildPlan(
            resource_id=resource_id,
            rebuild_chunks=False,
            rebuild_embeddings=False,
            reason="unknown_change",
        )

