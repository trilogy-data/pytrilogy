"""Arm identity for a concept-level `union(...)`.

A union stacks one row source per argument. Sibling unions over one arm family
(`all_k <- union(k1, k2)` beside `all_amt <- union(amt, pad)`) share those row
sources: `k1` and `amt` are read off the same rows, `k2` and `pad` off the
same rows. Each arm is therefore its own planning scope, labelled in the
concept graph like a rowset's internals so its lineage never buckets with
another arm's, and a UNION group is fed one parent per arm.

An arm is identified by the row population it reads: a property's key, a key
itself. A keyless argument (a constant, a keyless expression) takes an
identity the union's own keyed arguments leave unclaimed within the family,
in family order, so `union(amt, zero)` beside `union(k1, k2)` pairs `zero`
with `k2`. A union with no keyed argument at all is its own family and its
arms are positional.
"""

from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import BuildConcept, BuildFunction
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.utility import unique

ARM_SCOPE_PREFIX = "arm:"


def union_args(concept: BuildConcept) -> list[BuildConcept]:
    if not isinstance(concept.lineage, BuildFunction):
        return []
    return list(concept.lineage.concept_arguments)


def is_union_concept(concept: BuildConcept) -> bool:
    return concept.derivation == Derivation.UNION and bool(union_args(concept))


def _own_identity(arg: BuildConcept) -> str | None:
    if arg.keys:
        return "|".join(sorted(arg.keys))
    if arg.purpose == Purpose.KEY:
        return arg.address
    if arg.grain is not None and arg.grain.components:
        return "|".join(sorted(arg.grain.components))
    return None


def _own_identities(concept: BuildConcept) -> list[str | None]:
    return [_own_identity(arg) for arg in union_args(concept)]


def union_family(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[BuildConcept]:
    """Same-arity unions transitively sharing a keyed arm identity with
    `concept`, itself included, ordered by address."""
    arity = len(union_args(concept))
    pool = unique(
        [
            c
            for c in environment.concepts.values()
            if is_union_concept(c) and len(union_args(c)) == arity
        ],
        "address",
    )
    identities = {
        c.address: {i for i in _own_identities(c) if i is not None} for c in pool
    }
    by_address = {c.address: c for c in pool}
    family = {concept.address}
    frontier = [concept.address]
    while frontier:
        current = frontier.pop()
        for other in pool:
            if other.address in family:
                continue
            if identities[current] & identities[other.address]:
                family.add(other.address)
                frontier.append(other.address)
    return [by_address[a] for a in sorted(family) if a in by_address] or [concept]


def union_arm_identities(
    concept: BuildConcept, environment: BuildEnvironment
) -> list[str]:
    """One identity per argument position of `concept`, aligned across its
    family."""
    own = _own_identities(concept)
    family_order: list[str] = []
    for member in union_family(concept, environment):
        for identity in _own_identities(member):
            if identity is not None and identity not in family_order:
                family_order.append(identity)
    claimed = {i for i in own if i is not None}
    unclaimed = [i for i in family_order if i not in claimed]
    result: list[str] = []
    for index, identity in enumerate(own):
        if identity is not None:
            result.append(identity)
        elif unclaimed:
            result.append(unclaimed.pop(0))
        else:
            result.append(f"#{index}")
    return result


def arm_scope(identity: str) -> str:
    return f"{ARM_SCOPE_PREFIX}{identity}"


def nest_scope(outer: str, inner: str) -> str:
    return f"{outer}/{inner}" if outer else inner


def union_arms(
    union_outputs: list[BuildConcept], environment: BuildEnvironment
) -> list[list[BuildConcept]] | None:
    """Per arm, the contributing argument of every output, in output order.

    None when the outputs do not stack over one arm family (some arm lacks
    an argument for some output): no UNION ALL can align them."""
    by_identity: dict[str, dict[str, BuildConcept]] = {}
    for output in union_outputs:
        identities = union_arm_identities(output, environment)
        for arg, identity in zip(union_args(output), identities):
            by_identity.setdefault(identity, {})[output.address] = arg
    arms: list[list[BuildConcept]] = []
    for args in by_identity.values():
        if len(args) != len(union_outputs):
            return None
        arms.append([args[output.address] for output in union_outputs])
    return arms
