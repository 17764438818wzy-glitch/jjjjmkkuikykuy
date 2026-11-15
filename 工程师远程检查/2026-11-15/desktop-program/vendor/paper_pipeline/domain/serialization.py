"""Serialization helpers for domain models."""

from __future__ import annotations

import json
from dataclasses import asdict, fields
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping, TypeVar, cast

from paper_pipeline.domain.enums import (
    DuplicateStatus,
    IndexStatus,
    SqlStatus,
    Tier,
    VersionStatus,
)
from paper_pipeline.domain.models import (
    PaperMetadata,
    ReviewHistoryItem,
    RiskFlag,
    SourceFileRecord,
)

_EnumT = TypeVar("_EnumT", bound=StrEnum)


def metadata_to_dict(metadata: PaperMetadata) -> dict[str, Any]:
    return cast(dict[str, Any], _to_jsonable(asdict(metadata)))


def metadata_to_json(metadata: PaperMetadata) -> str:
    return json.dumps(metadata_to_dict(metadata), ensure_ascii=False, sort_keys=True)


def metadata_from_json(payload: str) -> PaperMetadata:
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise TypeError("PaperMetadata JSON must decode to an object")
    return metadata_from_dict(decoded)


def metadata_from_dict(data: Mapping[str, Any]) -> PaperMetadata:
    field_names = {item.name for item in fields(PaperMetadata)}
    kwargs = {key: value for key, value in data.items() if key in field_names}

    if "tier" in kwargs:
        kwargs["tier"] = _enum_from(Tier, kwargs["tier"])
    if "sql_status" in kwargs:
        kwargs["sql_status"] = _enum_from(SqlStatus, kwargs["sql_status"])
    if "version_status" in kwargs:
        kwargs["version_status"] = _enum_from(VersionStatus, kwargs["version_status"])
    if "index_status" in kwargs:
        kwargs["index_status"] = _enum_from(IndexStatus, kwargs["index_status"])
    if "duplicate_status" in kwargs:
        kwargs["duplicate_status"] = _enum_from(
            DuplicateStatus,
            kwargs["duplicate_status"],
        )
    if "risk_flags" in kwargs:
        kwargs["risk_flags"] = [_risk_flag_from(item) for item in kwargs["risk_flags"]]
    if "review_history" in kwargs:
        kwargs["review_history"] = [
            _review_history_item_from(item) for item in kwargs["review_history"]
        ]
    if "created_at" in kwargs:
        kwargs["created_at"] = _datetime_from(kwargs["created_at"])
    if "updated_at" in kwargs:
        kwargs["updated_at"] = _datetime_from(kwargs["updated_at"])

    return PaperMetadata(**kwargs)


def source_file_record_to_dict(record: SourceFileRecord) -> dict[str, Any]:
    return cast(dict[str, Any], _to_jsonable(asdict(record)))


def source_file_record_from_dict(data: Mapping[str, Any]) -> SourceFileRecord:
    field_names = {item.name for item in fields(SourceFileRecord)}
    kwargs = {key: value for key, value in data.items() if key in field_names}

    if "discovered_at" in kwargs:
        kwargs["discovered_at"] = _datetime_from(kwargs["discovered_at"])
    if "first_seen_at" in kwargs and kwargs["first_seen_at"] is not None:
        kwargs["first_seen_at"] = _datetime_from(kwargs["first_seen_at"])
    if "duplicate_status" in kwargs:
        kwargs["duplicate_status"] = _enum_from(
            DuplicateStatus,
            kwargs["duplicate_status"],
        )

    return SourceFileRecord(**kwargs)


def source_file_records_to_json(records: list[SourceFileRecord]) -> str:
    return json.dumps(
        [source_file_record_to_dict(record) for record in records],
        ensure_ascii=False,
        sort_keys=True,
    )


def source_file_records_from_json(payload: str) -> list[SourceFileRecord]:
    decoded = json.loads(payload)
    if not isinstance(decoded, list):
        raise TypeError("SourceFileRecord JSON must decode to a list")
    return [source_file_record_from_dict(item) for item in decoded]


def _to_jsonable(value: object) -> object:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    return value


def _enum_from(enum_type: type[_EnumT], value: _EnumT | str) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    return enum_type(value)


def _datetime_from(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _risk_flag_from(value: RiskFlag | Mapping[str, Any]) -> RiskFlag:
    if isinstance(value, RiskFlag):
        return value
    return RiskFlag(
        code=str(value["code"]),
        message=str(value["message"]),
        severity=str(value.get("severity", "warning")),
    )


def _review_history_item_from(
    value: ReviewHistoryItem | Mapping[str, Any],
) -> ReviewHistoryItem:
    if isinstance(value, ReviewHistoryItem):
        return value
    reviewed_at = value.get("reviewed_at", value.get("created_at"))
    if reviewed_at is None:
        reviewed_at = datetime.now().isoformat()
    return ReviewHistoryItem(
        reviewer=str(value["reviewer"]),
        action=str(value["action"]),
        comment=str(value.get("comment", "")),
        reviewed_at=_datetime_from(reviewed_at),
        operator=str(value.get("operator", "")),
        created_at=(
            _datetime_from(value["created_at"]) if value.get("created_at") else None
        ),
        old_tier=str(value.get("old_tier", "")),
        new_tier=str(value.get("new_tier", "")),
        reason=str(value.get("reason", "")),
        evidence=[str(item) for item in value.get("evidence", [])],
        agent_name=str(value.get("agent_name", "")),
        decision_source=str(value.get("decision_source", "")),
        model=str(value.get("model", "")),
        decision=str(value.get("decision", "")),
        duplicate_status=str(value.get("duplicate_status", "")),
        similar_to=[str(item) for item in value.get("similar_to", [])],
    )
