"""Planning helpers for real small-sample vector retrieval trials.

This module builds an executable-looking plan but never performs the trial.
Actual OpenAI calls, SQL migration, and PostgreSQL writes require external
authorization.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Mapping


TRIAL_STEPS = [
    "read_eligible_formal_resources",
    "select_1_to_3_sports_resources",
    "generate_chunk_preview",
    "request_embedding_preview",
    "write_pgvector_small_sample",
    "run_single_topic_retrieval",
    "generate_material_package_preview",
    "save_trial_report",
]

DEFAULT_TRIAL_TOPIC = "花样跳绳训练对 6-7 岁儿童身体素质影响的研究"


@dataclass(frozen=True, slots=True)
class RealTrialConfig:
    openai_api_key_set: bool = False
    postgres_config_set: bool = False
    sample_size: int = 3
    topic: str = DEFAULT_TRIAL_TOPIC

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> "RealTrialConfig":
        source = environ if environ is not None else os.environ
        sample_size = _bounded_sample_size(
            _int_from_env(source, "VECTOR_TRIAL_SAMPLE_SIZE", 3),
        )
        postgres_set = bool(
            str(source.get("POSTGRES_DSN", source.get("DATABASE_URL", ""))).strip()
            or (
                str(source.get("POSTGRES_HOST", "")).strip()
                and str(source.get("POSTGRES_DB", source.get("POSTGRES_DATABASE", ""))).strip()
                and str(source.get("POSTGRES_USER", "")).strip()
                and str(source.get(str(source.get("POSTGRES_PASSWORD_ENV", "POSTGRES_PASSWORD")), "")).strip()
            ),
        )
        return cls(
            openai_api_key_set=bool(
                str(source.get("VECTOR_OPENAI_API_KEY", "")).strip()
                or str(source.get("OPENAI_API_KEY", "")).strip(),
            ),
            postgres_config_set=postgres_set,
            sample_size=sample_size,
            topic=str(source.get("VECTOR_TRIAL_TOPIC", "")).strip()
            or DEFAULT_TRIAL_TOPIC,
        )


@dataclass(frozen=True, slots=True)
class RealTrialPlan:
    ready: bool
    blockers: list[str]
    steps: list[str]
    sample_size: int
    topic: str
    dry_run: bool = True
    requires_explicit_confirmation: bool = True
    will_execute: bool = False
    notes: list[str] = field(default_factory=list)


class RealTrialPlanner:
    def build_plan(self, config: RealTrialConfig) -> RealTrialPlan:
        blockers: list[str] = []
        if not config.openai_api_key_set:
            blockers.append("OPENAI_API_KEY is required")
        if not config.postgres_config_set:
            blockers.append("PostgreSQL connection is required")
        return RealTrialPlan(
            ready=not blockers,
            blockers=blockers,
            steps=list(TRIAL_STEPS),
            sample_size=config.sample_size,
            topic=config.topic,
            notes=[
                "This plan does not execute real OpenAI calls.",
                "This plan does not execute SQL migration or database writes.",
                "Run only after migration review and explicit authorization.",
            ],
        )


def _int_from_env(source: Mapping[str, str], key: str, default: int) -> int:
    raw = str(source.get(key, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _bounded_sample_size(value: int) -> int:
    if value < 1:
        return 1
    if value > 3:
        return 3
    return value
