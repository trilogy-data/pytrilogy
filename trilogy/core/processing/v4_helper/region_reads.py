"""What a `~` extension region's rows decide: the aggregates evaluated over
them and the WHERE inputs tested over them. Shared by the region-domain
decision (`region_domains`) and condition placement, which must agree."""

from collections.abc import Iterable

from trilogy.core.enums import NULL_COLLECTING_AGGREGATES
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.keyspace import Keyspace, Region

from .extent_ownership import null_on_padding
from .projection import decided_at_output_grain


def argument_takes_a_value_on_padding(
    address: str, region: Region, keyspace: Keyspace, environment: BuildEnvironment
) -> bool:
    """Whether the aggregate at `address` answers a padded row of `region`
    differently from no row at all, so it must be computed on the solid rows.

    An argument written inline (`sum(coalesce(amount, 0))`) is a derivation no
    concept node stands for; it takes a value on the region's rows when it
    reads something absent there and is not NULL for it. A NULL-collecting
    operator (`array_agg(amount)`) takes the padding's NULL itself, `[NULL]`
    where an empty group is NULL, so any argument absent there keeps it
    solid."""
    concept = environment.concepts.get(address)
    if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
        return False
    function = concept.lineage.function
    if function.operator in NULL_COLLECTING_AGGREGATES:
        return any(
            not keyspace.defined_on(read.address, region)
            for read in function.concept_arguments
        )
    return any(
        not null_on_padding(arg, region, keyspace, environment)
        and any(
            not keyspace.defined_on(read.address, region)
            for read in arg.concept_arguments
        )
        for arg in function.arguments
        if isinstance(arg, BuildConceptArgs) and not isinstance(arg, BuildConcept)
    )


def aggregates_over_region(
    members: Iterable[str],
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """An aggregate is evaluated OVER a region's rows when they hold its
    argument: `count(customer_id) by status` counts the customer with no order,
    under the NULL status of a row that has none."""
    members = tuple(members)
    for member in members:
        concept = environment.concepts.get(member)
        if concept is None or not isinstance(concept.lineage, BuildAggregateWrapper):
            return False
        arguments = concept.lineage.function.concept_arguments
        if not arguments or not all(
            keyspace.carried_on(arg.address, region) for arg in arguments
        ):
            return False
    return bool(members)


def evaluated_over_region(
    members: Iterable[str],
    grain: Iterable[str],
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """Aggregates the region's rows survive: they count what the region holds,
    or group by something it carries (each extension row its own group) with
    no inline argument taking a value on the padding."""
    members = tuple(members)
    if aggregates_over_region(members, region, keyspace, environment):
        return True
    return any(keyspace.carried_on(g, region) for g in grain) and not any(
        argument_takes_a_value_on_padding(m, region, keyspace, environment)
        for m in members
    )


def fed_by_region_domain(
    concept: BuildConcept,
    region: Region,
    keyspace: Keyspace,
    environment: BuildEnvironment,
) -> bool:
    """An aggregate output a region domain feeds (`evaluated_over_region`)."""
    return isinstance(concept.lineage, BuildAggregateWrapper) and evaluated_over_region(
        (concept.address,),
        concept.grain.components if concept.grain else (),
        region,
        keyspace,
        environment,
    )


def keyless(address: str, keyspace: Keyspace) -> bool:
    """One value for every row of the statement (`count(order_id) by *`): it
    filters the region's rows exactly as it filters the solid ones."""
    return not keyspace.keys_by_address.get(address)


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
    unfed = [
        c for c in outputs if not fed_by_region_domain(c, region, keyspace, environment)
    ]
    if len(unfed) < len(outputs):
        unfed = [c for c in unfed if not isinstance(c.lineage, BuildAggregateWrapper)]
    return decided_at_output_grain(address, unfed, environment)
