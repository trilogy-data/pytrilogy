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

from trilogy.constants import logger
from trilogy.core.assertions import (
    AssertionPlan,
    AssertionTerm,
    plan_assertion,
    substitute,
)
from trilogy.core.enums import DatasourceState
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.datasource import Address, Datasource, DatasourceAssertion
from trilogy.core.models.environment import Environment
from trilogy.execution.state.exceptions import (
    is_missing_source_error,
    is_schema_mismatch_error,
)
from trilogy.execution.state.isolation import hidden_datasources
from trilogy.parsing.render import Renderer

if TYPE_CHECKING:
    from trilogy.executor import Executor

GroupKey = tuple[Any, ...]
#: Fetched term values for one side of one assertion, keyed by ``by`` group.
SideValues = dict[GroupKey, dict[str, Any]]
#: Per assertion (by position), the previous asset's values; None when the
#: previous asset could not answer.
PriorValues = dict[int, SideValues | None]


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


def _side_query(keys: list[ConceptRef], terms: list) -> str:
    items = [_render(k) for k in keys] + [
        f"{_render(t.expr)} as {t.placeholder}" for t in terms
    ]
    return "select " + ", ".join(items) + ";"


def _literal(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


def _fetch_side(
    executor: Executor,
    datasource: Datasource,
    address: Address,
    keys: list[ConceptRef],
    terms: list,
) -> SideValues:
    text = _side_query(keys, terms)
    with pinned_datasource(executor.environment, datasource, address):
        result = executor.execute_ephemeral(text)
    rows = result.fetchall() if result is not None else []
    width = len(keys)
    return {
        tuple(row[:width]): {
            t.placeholder: _literal(row[width + i]) for i, t in enumerate(terms)
        }
        for row in rows
    }


def _plans(datasource: Datasource) -> list[tuple[DatasourceAssertion, AssertionPlan]]:
    return [(a, plan_assertion(a.condition)) for a in datasource.assertions]


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
    plans = [(i, a, p) for i, (a, p) in enumerate(_plans(datasource)) if p.uses_prior]
    if plans and address.is_missing_locally:
        logger.warning(
            "%s: no previous asset at %s; `prior` assertions pass this build",
            datasource.identifier,
            address.location,
        )
        return {index: None for index, _, _ in plans}
    for index, assertion, plan in plans:
        try:
            values = _fetch_side(
                executor, datasource, address, plan.group_keys, plan.prior_terms
            )
        except Exception as exc:
            dialect = executor.generator
            if not (
                is_missing_source_error(exc, dialect)
                or is_schema_mismatch_error(exc, dialect)
            ):
                raise
            logger.warning(
                "%s: previous asset cannot answer `%s` (%s); the assertion"
                " passes this build",
                datasource.identifier,
                _render(assertion.condition),
                exc,
            )
            prior[index] = None
            continue
        # A grouped read of an empty asset (a table created just before its
        # first build) has no groups to compare against: that is no history,
        # not a history of vanished groups.
        if not values and plan.group_keys:
            logger.warning(
                "%s: previous asset has no rows; `%s` passes this build",
                datasource.identifier,
                _render(assertion.condition),
            )
            prior[index] = None
            continue
        prior[index] = values
    return prior


def _describe_group(keys: list[ConceptRef], group: GroupKey) -> str:
    if not keys:
        return ""
    pairs = ", ".join(f"{_render(k)} = {v!r}" for k, v in zip(keys, group))
    return f" for {pairs}"


def _label(term: AssertionTerm) -> str:
    rendered = _render(term.expr)
    return f"prior({rendered})" if term.prior else rendered


def _describe_values(terms: list[AssertionTerm], values: dict[str, Any]) -> str:
    return ", ".join(f"{_label(t)} = {values[t.placeholder]!r}" for t in terms)


def _holds(executor: Executor, condition: Any) -> bool:
    result = executor.execute_ephemeral(
        f"select 1 as __assert_ok where {_render(condition)};"
    )
    return bool(result is not None and result.fetchall())


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
        if not _holds(executor, substitute(plan.template, values)):
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
    failures: list[str] = []
    for index, assertion in enumerate(datasource.assertions):
        failures.extend(
            evaluate_assertion(
                executor, datasource, assertion, new_address, (prior or {}).get(index)
            )
        )
    if failures:
        raise DatasourceAssertionError(datasource.identifier, failures)


def render_assertion(assertion: DatasourceAssertion) -> str:
    return _render(assertion.condition)
