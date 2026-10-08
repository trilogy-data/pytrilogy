"""What a `~` extension region's rows decide: the aggregates evaluated over
them and the WHERE inputs tested over them. Shared by the region-domain
decision (`region_domains`) and condition placement, which must agree."""

from collections.abc import Callable, Iterable
from typing import TypeGuard

from trilogy.core.enums import FunctionType
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
    BuildFunction,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.keyspace import Keyspace, Region

from .extent_ownership import null_on_padding
from .projection import decided_at_output_grain

GROUPING_FLAGS = (FunctionType.GROUPING, FunctionType.GROUPING_ID)


def inline_arguments_taking_a_value(
    concept: BuildConcept | None,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> list[BuildConceptArgs]:
    """The arguments of the aggregate `concept` written inline (`sum(coalesce(
    amount, 0))`) that take a value on a padded row of `region`: they read
    something absent there and are not NULL for it. No concept node stands for
    one, so nothing computes it on the solid rows before they are padded."""
    if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
        return []
    return [
        arg
        for arg in concept.lineage.function.arguments
        if isinstance(arg, BuildConceptArgs)
        and not isinstance(arg, BuildConcept)
        and not null_on_padding(arg, region, keyspace, environment)
        and any(
            not keyspace.defined_on(read.address, region)
            for read in arg.concept_arguments
        )
    ]


def nameable(argument: BuildConceptArgs) -> TypeGuard[BuildFunction]:
    """An inline argument the strategy builder can stand a concept in for and
    project on the solid rows (`_name_inline_arguments`)."""
    return isinstance(argument, BuildFunction)


def arguments_within(
    concept: BuildConcept | None,
    keys_of: Callable[[str], frozenset[str]],
    reach: frozenset[str],
) -> bool:
    """An aggregate is evaluated OVER a region's rows when they hold its every
    argument: `count(customer_id) by status` counts the customer with no order,
    under the NULL status of a row that has none."""
    if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
        return False
    arguments = concept.lineage.function.concept_arguments
    return bool(arguments) and all(
        (keys := keys_of(arg.address)) and keys <= reach for arg in arguments
    )


def aggregates_over_region(
    members: Iterable[str],
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """Every member is an aggregate evaluated over the region's rows."""
    counted = False
    for member in members:
        concept = environment.concepts.get(member)
        if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
            return False
        if concept.lineage.function.operator in GROUPING_FLAGS:
            # a ROLLUP pass's own flag, whatever rows enter the pass
            continue
        counted = True
        if not arguments_within(concept, keyspace.keys_of, region.reach):
            return False
    return counted


def evaluated_over_region(
    members: Iterable[str],
    grain: Iterable[str],
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
    one_pass: bool = False,
) -> bool:
    """Aggregates the region's rows survive: they count what the region holds,
    or group by something it carries (each extension row its own group) with
    no inline argument taking a value on the padding. Grouped by the span key
    alone (`min(amount) by user_id`), the solid rows are the whole input of an
    aggregate that is NULL on the extension row as it is where the FINAL pads
    the group it never had; only one answering a padded row differently from
    no row (`count`: 0, not NULL) takes the region. A property of the span
    (`by state`) reads the region's rows through the lookup it needs anyway.

    `one_pass`: a ROLLUP/CUBE/GROUPING SETS pass, whose subtotal rows nothing
    joins back to. One member counting the region brings its rows under the
    whole pass; the members absent there aggregate their NULLs, and an inline
    argument taking a value there is named and projected on the solid rows
    below the pass."""
    members = tuple(members)
    if aggregates_over_region(members, region, keyspace, environment):
        return True
    if any(
        not (one_pass and nameable(argument))
        for m in members
        for argument in inline_arguments_taking_a_value(
            environment.concepts.get(m), region, keyspace, environment
        )
    ):
        return False
    carried = [g for g in grain if keyspace.carried_on(g, region)]
    if carried:
        return (
            one_pass
            or any(g not in region.spans for g in carried)
            or any(
                (concept := environment.concepts.get(m)) is not None
                and concept.zero_on_empty
                for m in members
            )
        )
    return one_pass and any(
        aggregates_over_region((m,), region, keyspace, environment) for m in members
    )


def keyless(address: str, keyspace: Keyspace) -> bool:
    """One value for every row of the statement (`count(order_id) by *`): it
    filters the region's rows exactly as it filters the solid ones."""
    return not keyspace.keys_by_address.get(address)


def fed_gate(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    """A keyless value the region's rows feed (`avg(bal) by *` over the
    customers, the ones with no order included)."""
    return keyless(address, keyspace) and aggregates_over_region(
        (address,), region, keyspace, environment
    )


def restated_over_region(
    address: str,
    region: Region,
    held: set[str],
    keyspace: Keyspace,
    outputs: list[BuildConcept],
    environment: BuildEnvironment,
) -> bool:
    """Whether a WHERE reading `address` is tested where a region domain's
    rows join back, not on the domain (`held`, the columns it carries) or
    below it.

    Any host below that pairs on solid keys and never sees the rows the
    domain adds back: a customer whose every order the atom rejected would
    return as an extension row, and `status is null` would never test the
    customer with no order. So is a value the region's rows carry but the
    domain does not hold (`activity`, a scalar over an aggregate by the
    span), when the final rows decide it: every output not evaluated over
    the region is grouped at a grain determining it. An aggregate the domain
    feeds unites the region's rows on its input, and the atom is applied
    there, before it, whatever its grain (`count(customer_id) by status where
    activity = 'dormant'`); beside one, an aggregate it does not feed takes
    the atom on its own input too (`_uncovered_grouping_placements`)."""
    if address in held:
        return False
    if not keyspace.defined_on(address, region) or keyless(address, keyspace):
        return True
    if not keyspace.carried_on(address, region):
        return False
    # an aggregate output a region domain feeds unites the region's rows
    unfed = [
        c
        for c in outputs
        if not (
            isinstance(c.lineage, BuildAggregateWrapper)
            and evaluated_over_region(
                (c.address,),
                c.grain.components if c.grain else (),
                region,
                keyspace,
                environment,
                one_pass=c.lineage.grouping.nulls_grouping_keys,
            )
        )
    ]
    if len(unfed) < len(outputs):
        unfed = [c for c in unfed if not isinstance(c.lineage, BuildAggregateWrapper)]
    return decided_at_output_grain(address, unfed, environment)
