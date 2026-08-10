"""Matching rules shared by the two `then where` delivery sites.

A staged chain declares that stage N's aggregates and windows compute over only
the rows passing stages 1..N-1. Two places act on that: the group graph
delivers earlier stages' atoms onto a later stage's computation hosts
(`condition_placement._staged_precondition_placements`), and ROOT re-applies
them when it re-sources such a computation standalone
(`root._staged_precondition_clauses`). Both have to answer the same question —
which stage computes this cross-row value, and what came before it — so the
rules live here rather than drifting apart in two modules.
"""

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildWhereClause

# The derivations that read across rows, and so are what a stage bound has to
# be delivered INTO rather than merely ANDed alongside.
CROSS_ROW_DERIVATIONS: frozenset[Derivation] = frozenset(
    {Derivation.AGGREGATE, Derivation.GROUP_TO, Derivation.WINDOW}
)


def stage_lineage_addresses(clause: BuildWhereClause) -> set[str]:
    """A stage's row-argument addresses plus one level of lineage.

    A stage can reference its cross-row computation through a scalar wrapper
    (`1.3 * avg(x) by k > 5`), and it is the inner anonymous aggregate — not
    the wrapping concept — that the planner buckets and that ROOT re-sources,
    so matching has to see through the wrapper.
    """
    addresses = {c.address for c in clause.row_arguments}
    for concept in clause.row_arguments:
        addresses.update(s.address for s in concept.sources)
    return addresses


def concept_is_cross_row(concept: BuildConcept) -> bool:
    """Whether a condition argument computes across rows, directly or through
    a scalar wrapper (`1.3 * avg(x) by k`)."""
    return concept.derivation in CROSS_ROW_DERIVATIONS or any(
        s.derivation in CROSS_ROW_DERIVATIONS for s in concept.sources
    )


def cross_row_stage_args(clause: BuildWhereClause) -> list[BuildConcept]:
    """The stage's row arguments that compute across rows — the computations
    whose input population the staged contract bounds."""
    return [c for c in clause.row_arguments if concept_is_cross_row(c)]


def stage_computes_cross_row(clause: BuildWhereClause) -> bool:
    """Whether this stage's predicate computes an aggregate or window itself.

    Only such a stage needs the earlier stages delivered into anything: a
    scalar stage is an ordinary conjunct of the combined row gate, already
    applied at the end.
    """
    return bool(cross_row_stage_args(clause))


def hosting_stage_index(
    staged_conditions: list[BuildWhereClause], args: list[BuildConcept]
) -> int | None:
    """Index of the first stage whose predicate computes one of `args`.

    Match the arg's own address against the stage's lineage expansion, never
    the reverse. Testing whether a stage mentions anything in the arg's
    lineage matches any stage that reads a column merely FEEDING the arg, so
    in `where f = 1 then where sum(z) by x > 5 then where z > 1` the trailing
    scalar stage would answer for `sum(z) by x` and drag that aggregate's own
    gate into the conditions used to re-source it.

    Returns None when no stage computes any of `args` — including when none of
    them is cross-row, which is the case for an ordinary row-column re-source
    that needs no stage bound at all.
    """
    cross_row = {c.address for c in args if c.derivation in CROSS_ROW_DERIVATIONS}
    if not cross_row:
        return None
    for index, clause in enumerate(staged_conditions):
        if cross_row & stage_lineage_addresses(clause):
            return index
    return None
