"""Errors raised by the local Dity pipeline adapter."""

from __future__ import annotations


class LocalDityPipelineError(RuntimeError):
    """Base exception for local Dity pipeline failures."""


class Stage0GateError(LocalDityPipelineError):
    """Raised when callers try to run Dity stages before stage0 acceptance."""
