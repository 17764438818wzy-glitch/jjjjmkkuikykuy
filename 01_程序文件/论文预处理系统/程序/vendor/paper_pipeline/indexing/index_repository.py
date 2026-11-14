"""JSON-backed repository for local testable indexes."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_pipeline.indexing.index_models import (
    Bm25IndexEntry,
    IndexBuildResult,
    VectorIndexEntry,
    utc_now_iso,
)


@dataclass(slots=True)
class JsonIndexRepository:
    library_dir: str | Path

    @property
    def root(self) -> Path:
        return Path(self.library_dir)

    @property
    def indexes_dir(self) -> Path:
        return self.root / "indexes"

    @property
    def bm25_path(self) -> Path:
        return self.indexes_dir / "bm25" / "index.json"

    @property
    def vector_path(self) -> Path:
        return self.indexes_dir / "vector" / "index.json"

    @property
    def manifest_path(self) -> Path:
        return self.indexes_dir / "manifest.json"

    def write_indexes(
        self,
        *,
        bm25_entries: list[Bm25IndexEntry],
        vector_entries: list[VectorIndexEntry],
    ) -> None:
        if bm25_entries:
            self._write_json(
                self.bm25_path,
                {
                    "schema_version": 1,
                    "updated_at": utc_now_iso(),
                    "entries": [entry.to_dict() for entry in bm25_entries],
                },
            )
        if vector_entries:
            self._write_json(
                self.vector_path,
                {
                    "schema_version": 1,
                    "updated_at": utc_now_iso(),
                    "entries": [entry.to_dict() for entry in vector_entries],
                },
            )

    def append_manifest(self, result: IndexBuildResult) -> Path:
        manifest = self._read_manifest()
        manifest["updated_at"] = utc_now_iso()
        runs = manifest.setdefault("runs", [])
        if not isinstance(runs, list):
            runs = []
            manifest["runs"] = runs
        runs.append(result.manifest_record())
        return self._write_json(self.manifest_path, manifest)

    def _read_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return {"schema_version": 1, "runs": []}
        decoded = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        if not isinstance(decoded, dict):
            return {"schema_version": 1, "runs": []}
        decoded.setdefault("schema_version", 1)
        decoded.setdefault("runs", [])
        return decoded

    def _write_json(self, path: Path, payload: dict[str, Any]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return path
