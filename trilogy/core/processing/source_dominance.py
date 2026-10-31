"""`formal/Trilogy/Cost.lean`: of two datasources that both answer a request,
one whose `complete where` arm implies the other's and whose grain is coarser
stores no more rows on any data (`Cost.dominated_le`), so it is never the
costlier read. Sources neither dominates have no data-independent order
(`Cost.partial_and_summary_incomparable`)."""

from collections.abc import Iterable

from trilogy.core.models.build import BuildDatasource, BuildUnionDatasource
from trilogy.core.processing.condition_utility import condition_implies

Source = BuildDatasource | BuildUnionDatasource


def arm_within(a: Source, b: Source) -> bool:
    if b.non_partial_for is None:
        return True
    return a.non_partial_for is not None and condition_implies(
        a.non_partial_for.conditional, b.non_partial_for.conditional
    )


def dominates(a: Source, b: Source) -> bool:
    if not (arm_within(a, b) and a.grain.components <= b.grain.components):
        return False
    return not (arm_within(b, a) and a.grain.components == b.grain.components)


def dominators(source: Source, pool: Iterable[Source]) -> int:
    """How many of `pool` dominate `source`. Dominance is transitive, so a
    dominated source always counts more than its dominator: sorting by this
    never puts the costlier one first."""
    return sum(1 for other in pool if dominates(other, source))
