"""JSON helpers for processing plans."""

from __future__ import annotations

import json
from dataclasses import asdict, fields
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping, TypeVar, cast

from paper_pipeline.domain import BatchPlan, ProcessingPlan, ProcessingPlanStatus

_EnumT = TypeVar("_EnumT", bound=StrEnum)


def processing_plan_to_dict(plan: ProcessingPlan) -> dict[str, Any]:
    return cast(dict[str, Any], _to_jsonable(asdict(plan)))


def processing_plan_to_json(plan: ProcessingPlan) -> str:
    return json.dumps(
        processing_plan_to_dict(plan),
        ensure_ascii=False,
        sort_keys=True,
    )


def processing_plan_from_json(payload: str) -> ProcessingPlan:
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise TypeError("ProcessingPlan JSON must decode to an object")
    return processing_plan_from_dict(decoded)


def processing_plan_from_dict(data: Mapping[str, Any]) -> ProcessingPlan:
    field_names = {item.name for item in fields(ProcessingPlan)}
    kwargs = {key: value for key, value in data.items() if key in field_names}

    if "created_at" in kwargs:
        kwargs["created_at"] = _datetime_from(kwargs["created_at"])
    if "status" in kwargs:
        kwargs["status"] = _enum_from(ProcessingPlanStatus, kwargs["status"])
    if "batches" in kwargs:
        kwargs["batches"] = [_batch_plan_from_dict(item) for item in kwargs["batches"]]

    return ProcessingPlan(**kwargs)


def _batch_plan_from_dict(data: Mapping[str, Any]) -> BatchPlan:
    field_names = {item.name for item in fields(BatchPlan)}
    kwargs = {key: value for key, value in data.items() if key in field_names}

    if "created_at" in kwargs:
        kwargs["created_at"] = _datetime_from(kwargs["created_at"])
    if "plans" in kwargs:
        kwargs["plans"] = [
            processing_plan_from_dict(item)
            if isinstance(item, Mapping)
            else item
            for item in kwargs["plans"]
        ]

    return BatchPlan(**kwargs)


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
