"""Bind a local parse attempt to a new execution on the same remote clock."""
from __future__ import annotations

import json
from datetime import datetime


class ExecutionUnconfirmed(ValueError):
    """The remote state has not yet been proven to belong to this attempt."""


class ArtifactEvidencePending(ValueError):
    """The current final execution record is not visible yet; retry reads only."""


class RemoteExecutionChanged(ValueError):
    """Postprocessing observed a different status or remote execution."""

    def __init__(self, remote):
        super().__init__('Remote execution changed while building artifacts')
        self.remote = remote


def is_database_status_diagnostic(diagnostic) -> bool:
    return (isinstance(diagnostic,dict)
            and diagnostic.get('source')=='remote_execution_status'
            and diagnostic.get('dependency')=='database'
            and diagnostic.get('kind') in {'database_deadlock','database_log_busy',
                                          'database_log_connection_changed'}
            and diagnostic.get('outcome')=='technical_review')


def current_execution(task: dict, remote: dict) -> bool:
    begin = remote.get('process_begin_at')
    expected_pipeline=task.get('execution_pipeline_id') or task.get('pipeline_id')
    if (not begin or remote.get('id') != task.get('remote_id')
            or remote.get('pipeline_id') != expected_pipeline):
        return False
    bound = task.get('execution_begin_at')
    if bound:
        return begin == bound
    baseline = task.get('parse_baseline')
    if not baseline:
        return False
    try:
        baseline = json.loads(baseline) if isinstance(baseline, str) else baseline
        if baseline.get('remote_id') != task.get('remote_id'):
            return False
        previous = baseline.get('process_begin_at')
        # Both timestamps came from the server; local timezone/skew is irrelevant.
        return not previous or datetime.fromisoformat(begin) > datetime.fromisoformat(previous)
    except (TypeError, ValueError, AttributeError):
        return False
