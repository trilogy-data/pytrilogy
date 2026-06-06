from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from trilogy.core.graph_models import SearchCriteria


class SourceAttempt(Enum):
    FULL = "full"
    PARTIAL_UNSCOPED = "partial_unscoped"
    PARTIAL_SCOPED = "partial_scoped"

    @property
    def accepts_partial(self) -> bool:
        return self is not SourceAttempt.FULL

    @property
    def criteria(self) -> SearchCriteria:
        if self is SourceAttempt.PARTIAL_UNSCOPED:
            return SearchCriteria.PARTIAL_UNSCOPED
        if self is SourceAttempt.PARTIAL_SCOPED:
            return SearchCriteria.PARTIAL_INCLUDING_SCOPED
        return SearchCriteria.FULL_ONLY


@dataclass(frozen=True)
class SourcePolicy:
    attempts: tuple[SourceAttempt, ...]

    @property
    def accepts_partial(self) -> bool:
        return any(attempt.accepts_partial for attempt in self.attempts)

    @property
    def cache_key(self) -> str:
        return "|".join(attempt.value for attempt in self.attempts)


STRICT_SOURCE_POLICY = SourcePolicy((SourceAttempt.FULL,))
FALLBACK_SOURCE_POLICY = SourcePolicy(
    (
        SourceAttempt.FULL,
        SourceAttempt.PARTIAL_UNSCOPED,
        SourceAttempt.PARTIAL_SCOPED,
    )
)
ROWSET_SOURCE_POLICY = FALLBACK_SOURCE_POLICY


def source_policy_from_legacy_accept_partial(
    accept_partial: bool,
) -> SourcePolicy:
    return FALLBACK_SOURCE_POLICY
