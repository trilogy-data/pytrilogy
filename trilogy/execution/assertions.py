"""Run a datasource's ``assert`` clauses against a build before it is published.

The check sits between the write and the swap: a file build is read back from
its staged path (see ``staged_write``) and a table build from inside the
still-open write transaction, so a failure leaves the previous asset exactly as
it was. ``prior(...)`` terms are read from the current asset *before* the
write, because a table overwrite destroys it in place.

Each side of an assertion is a trilogy select pinned to the datasource at one
physical address: the datasource is swapped for a repointed copy and every
other datasource is hidden, so the planner has one place to read from. The
fetched values are then substituted into the condition and the result checked
as a constant query, which keeps the comparison's semantics (``NULL``, numeric
promotion, string collation) with the engine rather than reimplementing them.

The asset is usually remote, so reads are what a check costs. Assertions whose
side queries come out at the same grain (``Grain.from_concepts`` over their
group keys) are read together: one read of the previous asset and one of the
build per grain, however many assertions there are. Every group's substituted
condition is then checked in one constant query rather than one per group.

Missing history is never a failure: a first build, an asset that cannot be
read, or one whose schema lacks a column the assertion needs all pass the
``prior``-bearing assertions with a warning. A group that exists in the new
build but not the previous one passes the same way. A group that vanished from
the new build fails, as does a term that evaluates to ``NULL`` there.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from trilogy.constants import MagicConstants, logger
from trilogy.core.assertions import (
    AssertionPlan,
    AssertionTerm,
    plan_assertion,
    substitute,
)
from trilogy.core.enums import DatasourceState, Modifier
from trilogy.core.models.author import ConceptRef, Grain
from trilogy.core.models.datasource import Address, Datasource, DatasourceAssertion
from trilogy.core.models.environment import Environment
from trilogy.execution.state.exceptions import (
    is_missing_source_error,
    is_schema_mismatch_error,
)
from trilogy.execution.state.isolation import hidden_datasources, scope_where
from trilogy.parsing.render import Renderer

if TYPE_CHECKING:
    from trilogy.executor import Executor

GroupKey = tuple[Any, ...]
#: Fetched term values for one side of one assertion, keyed by ``by`` group.
SideValues = dict[GroupKey, dict[str, Any]]
#: Per assertion (by position), the previous asset's values; None when the
#: previous asset could not answer.
PriorValues = dict[int, SideValues | None]
#: Substituted conditions checked per constant query, so a grain with
#: thousands of groups does not render one unbounded statement.
HOLDS_CHUNK = 200


class DatasourceAssertionError(RuntimeError):
    def __init__(self, datasource_id: str, failures: list[str]):
        self.datasource_id = datasource_id
        self.failures = failures
        lines = "\n".join(f"  - {f}" for f in failures)
        super().__init__(
            f"{datasource_id}: {len(failures)} assertion(s) failed; the asset"
            f" was not replaced.\n{lines}"
        )


@contextmanager
def pinned_datasource(
    environment: Environment, datasource: Datasource, address: Address
) -> Iterator[Datasource]:
    """Plan against ``datasource`` at ``address`` and nothing else."""
    pinned = datasource.duplicate()
    pinned.address = address
    pinned.status = DatasourceState.PUBLISHED
    if pinned.non_partial_for is None:
        # A `~key` with no `complete where` holds part of the key's domain but
        # every row of its own; those rows are the population being checked.
        for column in pinned.columns:
            column.modifiers = [m for m in column.modifiers if m != Modifier.PARTIAL]
    key = datasource.identifier
    others = [k for k in environment.datasources if k != key]
    with hidden_datasources(environment, others):
        original = environment.datasources.get(key)
        environment.datasources[key] = pinned
        try:
            yield pinned
        finally:
            if original is not None:
                environment.datasources[key] = original
            else:
                del environment.datasources[key]


def _render(expr: Any) -> str:
    return Renderer().to_string(expr)


#: One assertion's share of a merged read: its group keys and its terms.
SideRequest = tuple[list[ConceptRef], list[AssertionTerm]]


def _fetch_sides(
    executor: Executor,
    datasource: Datasource,
    address: Address,
    requests: dict[int, SideRequest],
) -> dict[int, SideValues]:
    """One read answering the terms of several assertions whose side queries
    share an output grain.

    The select projects every assertion's keys, so each reads its group tuple
    in its own key order; at one grain the extra keys add no rows.
    Placeholders restart at zero per assertion, so each term is aliased with
    its assertion's index for the read and keyed by its own placeholder in the
    result. A filtered aggregate renders as a ``CASE`` inside the shared
    ``GROUP BY``, so it reports 0 for a group it filters empty rather than
    dropping the group."""
    keys: list[ConceptRef] = []
    for group_keys, _ in requests.values():
        keys.extend(k for k in group_keys if k.address not in {x.address for x in keys})
    position = {k.address: i for i, k in enumerate(keys)}
    columns = [
        (index, term, f"__a{index}{term.placeholder}")
        for index, (_, terms) in requests.items()
        for term in terms
    ]
    items = [_render(k) for k in keys] + [
        f"{_render(term.expr)} as {alias}" for _, term, alias in columns
    ]
    text = "select " + ", ".join(items) + scope_where(datasource) + ";"
    with pinned_datasource(executor.environment, datasource, address):
        result = executor.execute_ephemeral(text)
    rows = result.fetchall() if result is not None else []
    width = len(keys)
    sides: dict[int, SideValues] = {index: {} for index in requests}
    for row in rows:
        for offset, (index, term, _) in enumerate(columns):
            group = tuple(row[position[k.address]] for k in requests[index][0])
            sides[index].setdefault(group, {})[term.placeholder] = row[width + offset]
    return sides


def _fetch_side(
    executor: Executor,
    datasource: Datasource,
    address: Address,
    keys: list[ConceptRef],
    terms: list[AssertionTerm],
) -> SideValues:
    return _fetch_sides(executor, datasource, address, {0: (keys, terms)})[0]


def _plans(datasource: Datasource) -> list[tuple[DatasourceAssertion, AssertionPlan]]:
    return [(a, plan_assertion(a.condition)) for a in datasource.assertions]


def _by_grain(
    environment: Environment, plans: list[tuple[int, AssertionPlan]]
) -> list[list[tuple[int, AssertionPlan]]]:
    """Plans whose side queries come out at the same grain, so one read can
    answer them all."""
    grains: dict[frozenset[str], list[tuple[int, AssertionPlan]]] = {}
    for index, plan in plans:
        grain = Grain.from_concepts(plan.group_keys, environment=environment)
        grains.setdefault(frozenset(grain.components), []).append((index, plan))
    return list(grains.values())


def collect_prior_values(executor: Executor, datasource: Datasource) -> PriorValues:
    """Read every ``prior(...)`` term from the asset as it stands now.

    Must run before the write: a table build replaces the asset in place.
    """
    prior: PriorValues = {}
    address = (
        datasource.address
        if isinstance(datasource.address, Address)
        else Address(location=datasource.address)
    )
    assertions = datasource.assertions
    plans = [(i, p) for i, (_, p) in enumerate(_plans(datasource)) if p.uses_prior]
    if plans and address.is_missing_locally:
        logger.warning(
            "%s: no previous asset at %s; `prior` assertions pass this build",
            datasource.identifier,
            address.location,
        )
        return {index: None for index, _ in plans}

    def unanswerable(exc: Exception) -> bool:
        dialect = executor.generator
        return is_missing_source_error(exc, dialect) or is_schema_mismatch_error(
            exc, dialect
        )

    group_keys = {index: plan.group_keys for index, plan in plans}
    for members in _by_grain(executor.environment, plans):
        fetched: dict[int, SideValues | None] = {}
        failed: dict[int, Exception] = {}
        try:
            fetched.update(
                _fetch_sides(
                    executor,
                    datasource,
                    address,
                    {
                        index: (plan.group_keys, plan.prior_terms)
                        for index, plan in members
                    },
                )
            )
        except Exception as exc:
            if not unanswerable(exc):
                raise
            if len(members) == 1:
                failed[members[0][0]] = exc
            else:
                # One assertion naming a column the previous asset lacks must
                # not cost the others their history: read them one at a time.
                for index, plan in members:
                    try:
                        fetched[index] = _fetch_side(
                            executor,
                            datasource,
                            address,
                            plan.group_keys,
                            plan.prior_terms,
                        )
                    except Exception as single:
                        if not unanswerable(single):
                            raise
                        failed[index] = single
        for index, error in failed.items():
            logger.warning(
                "%s: previous asset cannot answer `%s` (%s); the assertion"
                " passes this build",
                datasource.identifier,
                _render(assertions[index].condition),
                error,
            )
            prior[index] = None
        for index, values in fetched.items():
            # A grouped read of an empty asset (a table created just before
            # its first build) has no groups to compare against: that is no
            # history, not a history of vanished groups.
            if not values and group_keys[index]:
                logger.warning(
                    "%s: previous asset has no rows; `%s` passes this build",
                    datasource.identifier,
                    _render(assertions[index].condition),
                )
                prior[index] = None
                continue
            prior[index] = values
    return prior


def _describe_group(keys: list[ConceptRef], group: GroupKey) -> str:
    if not keys:
        return ""
    pairs = ", ".join(f"{_render(k)} = {_shown(v)}" for k, v in zip(keys, group))
    return f" for {pairs}"


def _label(term: AssertionTerm) -> str:
    rendered = _render(term.expr)
    return f"prior({rendered})" if term.prior else rendered


def _shown(value: Any) -> str:
    return format(value, "f") if isinstance(value, Decimal) else repr(value)


def _describe_values(terms: list[AssertionTerm], values: dict[str, Any]) -> str:
    return ", ".join(f"{_label(t)} = {_shown(values[t.placeholder])}" for t in terms)


def _substituted(plan: AssertionPlan, group: GroupKey, values: dict[str, Any]) -> Any:
    """The template as a constant: its terms and its ``by`` keys are this
    group's values. A key left in place would be read from whatever the
    constant query found in scope, not from the group."""
    keys = {
        k.address: MagicConstants.NULL if v is None else v
        for k, v in zip(plan.group_keys, group)
    }
    return substitute(plan.template, {**values, **keys})


def _holds(executor: Executor, conditions: list[Any]) -> list[bool]:
    """Whether each constant condition is true; ``NULL`` is not."""
    held: list[bool] = []
    for start in range(0, len(conditions), HOLDS_CHUNK):
        chunk = conditions[start : start + HOLDS_CHUNK]
        items = ", ".join(
            f"{_render(condition)} as __assert_ok_{i}"
            for i, condition in enumerate(chunk)
        )
        result = executor.execute_ephemeral(f"select {items};")
        row = result.fetchone() if result is not None else None
        # SQLite and MySQL return a boolean as 1/0.
        held.extend(
            row is not None and row[i] is not None and bool(row[i])
            for i in range(len(chunk))
        )
    return held


def _check_one(
    executor: Executor,
    datasource: Datasource,
    assertion: DatasourceAssertion,
    plan: AssertionPlan,
    new_values: SideValues,
    prior_values: SideValues | None,
) -> list[str]:
    # A missing previous asset was already reported once when it was read.
    if plan.uses_prior and prior_values is None:
        return []
    previous_side = prior_values or {}
    failures: list[str] = []
    to_check: list[tuple[str, GroupKey, dict[str, Any]]] = []
    spelled = _render(assertion.condition)
    groups = list(new_values) + [g for g in previous_side if g not in new_values]
    for group in groups:
        where = _describe_group(plan.group_keys, group)
        values = dict(new_values.get(group, {}))
        if plan.uses_prior:
            if group not in previous_side:
                logger.warning(
                    "%s: `%s` has no previous value%s; passes this build",
                    datasource.identifier,
                    spelled,
                    where,
                )
                continue
            values.update(previous_side[group])
        if group not in new_values:
            previous = _describe_values(plan.prior_terms, values)
            failures.append(
                f"`{spelled}`{where}: group has no rows in the new build"
                + (f"; {previous}" if previous else "")
            )
            continue
        if any(values[t.placeholder] is None for t in plan.new_terms):
            failures.append(
                f"`{spelled}`{where}: {_describe_values(plan.terms, values)}"
                " (NULL never satisfies an assertion)"
            )
            continue
        if any(values[t.placeholder] is None for t in plan.prior_terms):
            logger.warning(
                "%s: `%s` has a NULL previous value%s; passes this build",
                datasource.identifier,
                spelled,
                where,
            )
            continue
        to_check.append((where, group, values))
    verdicts = _holds(
        executor, [_substituted(plan, group, values) for _, group, values in to_check]
    )
    for (where, _, values), held in zip(to_check, verdicts):
        if not held:
            failures.append(
                f"`{spelled}`{where}: {_describe_values(plan.terms, values)}"
            )
    return failures


def evaluate_assertion(
    executor: Executor,
    datasource: Datasource,
    assertion: DatasourceAssertion,
    new_address: Address,
    prior_values: SideValues | None,
) -> list[str]:
    """The failures of one assertion against the build at ``new_address``."""
    plan = plan_assertion(assertion.condition)
    new_values = _fetch_side(
        executor, datasource, new_address, plan.group_keys, plan.new_terms
    )
    return _check_one(executor, datasource, assertion, plan, new_values, prior_values)


def check_assertions(
    executor: Executor,
    datasource: Datasource,
    new_address: Address,
    prior: PriorValues | None,
) -> None:
    """Evaluate every assertion of ``datasource`` against the build at
    ``new_address``; raise :class:`DatasourceAssertionError` listing each
    failure. ``prior`` is what :func:`collect_prior_values` read before the
    write (None when nothing was collected)."""
    plans = _plans(datasource)
    new_values: dict[int, SideValues] = {}
    for members in _by_grain(
        executor.environment, list(enumerate(p for _, p in plans))
    ):
        new_values.update(
            _fetch_sides(
                executor,
                datasource,
                new_address,
                {index: (plan.group_keys, plan.new_terms) for index, plan in members},
            )
        )
    failures: list[str] = []
    for index, (assertion, plan) in enumerate(plans):
        failures.extend(
            _check_one(
                executor,
                datasource,
                assertion,
                plan,
                new_values[index],
                (prior or {}).get(index),
            )
        )
    if failures:
        raise DatasourceAssertionError(datasource.identifier, failures)


def render_assertion(assertion: DatasourceAssertion) -> str:
    return _render(assertion.condition)
