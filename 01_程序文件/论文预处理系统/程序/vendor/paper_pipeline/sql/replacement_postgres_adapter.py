from __future__ import annotations

from collections.abc import Mapping
from typing import Any


OLD_RESOURCE_UPDATE = "replacement_old_resource_update"
OLD_INDEX_STATUS_UPDATE = "replacement_old_index_status_update"
UPLOAD_REFERENCE = "replacement_new_resource_upload_reference"

PREPARED_OPERATIONS = {
    OLD_RESOURCE_UPDATE: "old_resource_update",
    OLD_INDEX_STATUS_UPDATE: "old_index_status_update",
}

TEXT_PAYLOAD_FIELDS = {
    "sql",
    "sql_text",
    "raw" + "_sql",
    "query",
}

NEW = "new"
ACTIVE = "active"
SEALED_SUCCESS = "sealed_success"
SEALED_FAILURE = "sealed_failure"


class AdapterStateError(RuntimeError):
    pass


class PreparedPostgreSQLAdapter:
    def __init__(self, gateway: Any):
        self._gateway = gateway
        self._state = NEW
        self._operation_id: str | None = None

    @property
    def state(self) -> str:
        return self._state

    @property
    def operation_id(self) -> str | None:
        return self._operation_id

    def begin(self, operation_id: str) -> Any:
        if self._state != NEW:
            raise AdapterStateError("already_begun")
        result = self._gateway.begin_unit(operation_id)
        self._operation_id = operation_id
        self._state = ACTIVE
        return result

    def apply_planned_step(
        self, statement: Mapping[str, Any], params: Mapping[str, Any]
    ) -> dict[str, Any]:
        self._require_active("apply")
        statement_map = _as_mapping(statement)
        params_map = _as_mapping(params)
        _reject_text_payload(statement_map)
        _reject_text_payload(params_map)

        statement_kind = statement_map.get("statement_kind")
        operation_kind = PREPARED_OPERATIONS.get(statement_kind)
        if operation_kind is None:
            raise ValueError("unsupported_prepared_kind")

        result = _as_mapping(
            self._gateway.apply_prepared(operation_kind, dict(params_map))
        )
        actual = result.get("actual_rowcount", result.get("rowcount"))
        return {
            "statement_id": statement_map.get("statement_id"),
            "operation_id": self._operation_id,
            "kind": statement_kind,
            "actual_rowcount": actual,
            "status": result.get("status", "unknown"),
        }

    def seal_success(self, summary: Mapping[str, Any]) -> Mapping[str, Any]:
        self._require_active("seal")
        result = self._gateway.seal_success(summary)
        self._state = SEALED_SUCCESS
        return result

    def seal_failure(self, summary: Mapping[str, Any]) -> Mapping[str, Any]:
        self._require_active("seal")
        result = self._gateway.seal_failure(summary)
        self._state = SEALED_FAILURE
        return result

    def _require_active(self, action: str) -> None:
        if self._state == ACTIVE:
            return
        if self._state == NEW:
            if action == "apply":
                raise AdapterStateError("begin_required")
            raise AdapterStateError("seal_before_begin")
        raise AdapterStateError("adapter_closed")


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _reject_text_payload(value: Mapping[str, Any]) -> None:
    for key, nested in value.items():
        if key in TEXT_PAYLOAD_FIELDS:
            raise ValueError("text_payload_forbidden")
        if isinstance(nested, Mapping):
            _reject_text_payload(nested)
