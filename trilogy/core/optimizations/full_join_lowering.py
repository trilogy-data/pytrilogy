"""Lower FULL OUTER JOINs for dialects that don't have them (MySQL, MariaDB).

Join resolution renders row-preserving by default, so a partial binding or a
``union join`` key produces a FULL JOIN. MySQL has no such operator, so the
join is re-expressed over a *key spine*: the UNION of every participant's join
key, LEFT JOINed back to each participant.

    -- before                        -- after
    FROM a                           FROM (SELECT k FROM a
    FULL JOIN b ON a.k = b.k               UNION
                                           SELECT k FROM b) spine
                                     LEFT JOIN a ON a.k <=> spine.k
                                     LEFT JOIN b ON b.k <=> spine.k

The spine form is preferred over the textbook ``LEFT ... UNION ALL ... RIGHT``
rewrite for two reasons. It scales to N participants on one key (an N-arm
``union join`` / multiselect align) with a single extra relation instead of
2^N copies of the select list. And it needs no "did the left side match"
sentinel: these joins exist *because* a key is nullable, and ``a.k IS NULL`` is
not a usable anti-match test on a nullable key, while a null-safe comparison
against the spine is exact.

The consumer's projection is left alone. It already reads a FULL join's key as
``coalesce(a.k, b.k)``, and since every spine row matches at least one side
that coalesce still yields the spine's key value, including when the key
itself is NULL.

Shapes that can't be expressed this way (a keyless FULL between multi-row
relations, FULL joins in one CTE that key off different concepts, an extra ON
predicate) raise rather than emit a wrong or invalid query.
"""

from __future__ import annotations

from trilogy.constants import logger
from trilogy.core.enums import JoinType, Modifier, SourceType
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.execute import (
    CTE,
    CTEConceptPair,
    DatasourceCTE,
    InstantiatedUnnestJoin,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import optimization_log
from trilogy.core.optimizations.null_safe_join import proven_non_null

COMPONENT = "LowerFullJoins"


class UnsupportedFullJoinError(UnresolvableQueryException):
    """A FULL JOIN survived planning for a dialect that has no FULL JOIN and
    whose shape the key-spine rewrite cannot express."""


# Levers an author can actually pull. Each one removes the reason the planner
# chose a preserving join, so the query re-plans as INNER/LEFT and never reaches
# this pass.
NULL_REJECT_LEVER = (
    "add `where {key} is not null`: with no NULL keys the join narrows and the "
    "ambiguity disappears"
)
COMPLETE_BINDING_LEVER = (
    "if a `~partial` binding made this key preserving, bind it complete (drop "
    "the `~`) on any datasource that really carries the key's full domain"
)
SPLIT_LEVER = (
    "align the joins on one key, or move one of them into its own rowset so "
    "each spine covers a single key"
)
NATIVE_LEVER = "run this statement on a dialect with native FULL JOIN support"


def _unsupported(reason: str, *levers: str) -> UnsupportedFullJoinError:
    """One consistent error shape: what happened, then how to fix it."""
    remediation = "\n".join(f"  - {lever}" for lever in (*levers, NATIVE_LEVER))
    return UnsupportedFullJoinError(
        f"{reason}\n\nThis dialect has no FULL OUTER JOIN. To resolve:\n"
        f"{remediation}"
    )


def _log(message: str) -> None:
    logger.info(optimization_log(COMPONENT, message))


def _full_joins(cte: CTE) -> list[Join]:
    return [
        join
        for join in cte.joins
        if isinstance(join, Join) and join.jointype == JoinType.FULL
    ]


def _slots(cte: CTE, join: Join) -> list[BuildConcept]:
    """The spine's key columns, taken from the first FULL join's left concepts.

    One left concept bound to two different right concepts needs two spine
    columns that would carry the same name, and the arms could not be lined up
    positionally. That is a distinct shape, not a key mismatch, so it gets its
    own diagnosis before ``_pairs_by_slot`` sees a duplicated slot list.
    """
    pairs = join.joinkey_pairs or []
    slots = [pair.left for pair in pairs]
    seen = {c.address for c in slots}
    if len(seen) != len(slots):
        repeated = sorted(
            {
                c.address
                for c in slots
                if [x.address for x in slots].count(c.address) > 1
            }
        )
        raise _unsupported(
            f"Cannot lower the FULL JOIN in {cte.name}: {repeated} each bind more "
            "than one key on the same join, so a single key spine cannot "
            "represent them as distinct columns.",
            SPLIT_LEVER,
            COMPLETE_BINDING_LEVER,
        )
    return slots


def _pairs_by_slot(
    join: Join, slots: list[BuildConcept], cte_name: str
) -> list[CTEConceptPair]:
    """Order ``join``'s key pairs to line up with ``slots``, keyed on the left
    concept address. Every FULL join in a CTE must cover exactly the same left
    key addresses for one shared spine to serve them all."""
    pairs = join.joinkey_pairs or []
    by_address = {pair.left.address: pair for pair in pairs}
    if len(by_address) != len(pairs) or set(by_address) != {s.address for s in slots}:
        raise _unsupported(
            f"Cannot lower the FULL JOINs in {cte_name}: they key off different "
            f"concepts ({sorted(by_address)} vs "
            f"{sorted(s.address for s in slots)}), so no single key spine covers "
            "them.",
            SPLIT_LEVER,
            COMPLETE_BINDING_LEVER,
        )
    return [by_address[slot.address] for slot in slots]


def _validate(cte: CTE, joins: list[Join]) -> None:
    for join in joins:
        if not join.joinkey_pairs:
            raise _unsupported(
                f"Cannot lower the keyless FULL JOIN between {cte.base_alias} and "
                f"{join.right_cte.name} in {cte.name}: with no join key there is "
                "no spine to build, and neither side is a single-row aggregate "
                "that would make it a plain cross join.",
                "give the two sides a shared key to join on, or aggregate one of "
                "them to a grand total (no `by`), which makes the cartesian exact",
            )
        if join.condition is not None:
            raise _unsupported(
                f"Cannot lower the FULL JOIN to {join.right_cte.name} in "
                f"{cte.name}: it carries an extra ON predicate, which changes "
                "which rows match and cannot be reproduced by a key spine.",
                "move the extra predicate into a `where` so the join matches on "
                "keys alone",
            )


def render_alias(cte: CTE, node: CTE | UnionCTE) -> str:
    """The alias ``cte`` references ``node``'s columns by.

    Mirrors the renderer: a folded ``DatasourceCTE`` is referenced by its raw
    table alias, not by the CTE name it had before inlining. ``base_alias`` is
    one of these, so participants must be matched against it in alias space.
    """
    if isinstance(node, DatasourceCTE) and cte.renders_inline(node):
        return cte.source_key_for(node)
    return cte.source_key_for(node.name)


def _spine_participants(
    cte: CTE, joins: list[Join], slots: list[BuildConcept]
) -> dict[str, tuple[CTE | UnionCTE, list[BuildConcept]]]:
    """Map participant name -> (node, its concept per slot).

    A participant is any node the FULL joins bind: each join's right side, and
    the node its key pairs read the left side from.
    """
    providers: dict[str, tuple[CTE | UnionCTE, list[BuildConcept]]] = {}

    def record(node: CTE | UnionCTE, concepts: list[BuildConcept]) -> None:
        existing = providers.get(node.name)
        if existing is not None and [c.address for c in existing[1]] != [
            c.address for c in concepts
        ]:
            raise _unsupported(
                f"Cannot lower the FULL JOINs in {cte.name}: {node.name} binds "
                "inconsistent key concepts across the FULL joins it participates "
                "in, so one spine cannot serve them both.",
                SPLIT_LEVER,
                COMPLETE_BINDING_LEVER,
            )
        providers[node.name] = (node, concepts)

    for join in joins:
        ordered = _pairs_by_slot(join, slots, cte.name)
        left_nodes = {pair.cte.name for pair in ordered}
        if len(left_nodes) != 1:
            raise _unsupported(
                f"Cannot lower the FULL JOIN to {join.right_cte.name} in "
                f"{cte.name}: its key pairs read the left side from more than "
                f"one relation ({sorted(left_nodes)}), so there is no single "
                "relation for the spine to replace.",
                SPLIT_LEVER,
                COMPLETE_BINDING_LEVER,
            )
        # Resolve to the consumer's own instance: a folded parent lives on
        # ``inlined_parents``, and only that instance carries the datasource
        # the spine arm has to read from.
        record(Join.authoritative(cte, ordered[0].cte), [pair.left for pair in ordered])
        record(
            Join.authoritative(cte, join.right_cte), [pair.right for pair in ordered]
        )

    if not any(
        render_alias(cte, node) == cte.base_alias for node, _ in providers.values()
    ):
        raise _unsupported(
            f"Cannot lower the FULL JOIN in {cte.name}: its FROM base "
            f"{cte.base_alias} does not participate in the FULL join keys, so "
            "replacing the base with a key spine would strand it.",
            "move the preserving join into its own rowset so it is the only "
            "relation in its scope",
            COMPLETE_BINDING_LEVER,
        )
    return providers


def _nullable_slots(joins: list[Join], slots: list[BuildConcept]) -> list[bool]:
    """Which slots the original FULL joins compared null-safely.

    A null-safe key pairs NULL with NULL, which is what the spine reproduces:
    UNION folds every participant's NULL keys into one spine row, and the
    null-safe LEFT JOINs re-expand it against all of them.
    """
    flags = [False] * len(slots)
    for join in joins:
        for index, pair in enumerate(_pairs_by_slot(join, slots, "")):
            if Modifier.NULLABLE in pair.modifiers or Modifier.NULLABLE in (
                join.modifiers or []
            ):
                flags[index] = True
    return flags


def _check_null_keys(
    cte: CTE,
    slots: list[BuildConcept],
    nullable: list[bool],
    participants: list[tuple[CTE | UnionCTE, list[BuildConcept]]],
) -> None:
    """Refuse a slot compared with plain ``=`` whose key can actually be NULL.

    Under ``=`` a NULL key matches nothing, so a native FULL JOIN preserves
    *every* NULL-key row from *both* sides as its own unmatched output row. The
    spine can't reproduce that count: UNION collapses them to a single NULL key
    that then re-joins to neither side. Null-safe slots are fine (that is the
    pairing the spine implements), and so are slots no participant can null.
    """
    for index, slot in enumerate(slots):
        if nullable[index]:
            continue
        unproven = [
            node.name
            for node, concepts in participants
            if not proven_non_null(concepts[index], node)
        ]
        if unproven:
            raise _unsupported(
                f"Cannot lower the FULL JOIN in {cte.name}: join key "
                f"{slot.address} is compared with plain equality but may be NULL "
                f"in {sorted(unproven)}. A native FULL JOIN keeps every NULL-key "
                "row from both sides as its own unmatched row; a key spine folds "
                "them into one, so the row counts would differ.",
                NULL_REJECT_LEVER.format(key=slot.address),
                COMPLETE_BINDING_LEVER,
            )


def _branch_cte(
    name: str,
    node: CTE | UnionCTE,
    concepts: list[BuildConcept],
    inlined: DatasourceCTE | None,
) -> CTE:
    """A ``SELECT <keys> FROM <node>`` arm of the spine union.

    A participant the consumer folded in (``inline_datasource``) has no WITH
    entry to select from, so the arm reads its raw table directly, the same
    shape a leaf ``DatasourceCTE`` renders.
    """
    grain = BuildGrain.from_concepts(concepts)
    if inlined is not None:
        datasource = inlined.datasource
        return DatasourceCTE(
            name=name,
            source=QueryDatasource(
                input_concepts=list(concepts),
                output_concepts=list(concepts),
                datasources=[datasource],
                source_map={c.address: {datasource} for c in concepts},
                grain=grain,
                joins=[],
                base_datasource=datasource,
            ),
            output_columns=list(concepts),
            source_map={c.address: [datasource.safe_identifier] for c in concepts},
            grain=grain,
            parent_ctes=[],
            datasource=datasource,
        )
    source = QueryDatasource(
        input_concepts=list(concepts),
        output_concepts=list(concepts),
        datasources=[node.source],
        source_map={c.address: {node.source} for c in concepts},
        grain=grain,
        joins=[],
    )
    return CTE(
        name=name,
        source=source,
        output_columns=list(concepts),
        source_map={c.address: [node.name] for c in concepts},
        grain=grain,
        parent_ctes=[node],
    )


def _build_spine(
    name: str,
    slots: list[BuildConcept],
    participants: list[tuple[CTE | UnionCTE, list[BuildConcept]]],
    inlined: dict[str, DatasourceCTE],
) -> UnionCTE:
    branches = [
        _branch_cte(f"{name}_{index}", node, concepts, inlined.get(node.name))
        for index, (node, concepts) in enumerate(participants)
    ]
    grain = BuildGrain.from_concepts(slots)
    source = QueryDatasource(
        input_concepts=list(slots),
        output_concepts=list(slots),
        datasources=[branch.source for branch in branches],
        source_map={c.address: {b.source for b in branches} for c in slots},
        grain=grain,
        joins=[],
        source_type=SourceType.UNION,
    )
    return UnionCTE(
        name=name,
        source=source,
        # The arms render inline; their own sources are what need WITH entries
        # (an inlined participant reads its raw table, so it contributes none).
        parent_ctes=[node for node, _ in participants if node.name not in inlined],
        internal_ctes=list(branches),
        output_columns=list(slots),
        grain=grain,
        # Distinct, not UNION ALL: the spine is a key domain, and SQL's UNION
        # treats NULL keys as equal, which is exactly the pairing we want.
        operator="UNION",
    )


def _lower_cte(cte: CTE, index: int) -> UnionCTE | None:
    joins = _full_joins(cte)
    if not joins:
        return None
    _validate(cte, joins)
    slots = _slots(cte, joins[0])
    providers = _spine_participants(cte, joins, slots)

    # The FROM base leads so the union's first arm names the spine's columns.
    base_name = next(
        name
        for name, (node, _) in providers.items()
        if render_alias(cte, node) == cte.base_alias
    )
    participants = [providers.pop(base_name), *providers.values()]
    nullable = _nullable_slots(joins, slots)
    _check_null_keys(cte, slots, nullable, participants)

    inlined = {
        node.name: folded
        for node, _ in participants
        if (folded := cte.inlined_parent_for_source(node.name)) is not None
    }
    spine = _build_spine(f"_spine_{index}_{cte.name}", slots, participants, inlined)
    spine_joins: list[Join] = [
        Join(
            right_cte=node,
            jointype=JoinType.LEFT_OUTER,
            left_cte=spine,
            joinkey_pairs=[
                CTEConceptPair(
                    left=slot,
                    right=concept,
                    existing_datasource=node.source,
                    cte=spine,
                    modifiers=[Modifier.NULLABLE] if is_nullable else [],
                )
                for slot, concept, is_nullable in zip(slots, concepts, nullable)
            ],
        )
        for node, concepts in participants
    ]

    # Spine joins lead: every other join in the chain reads a participant's
    # alias, which now only exists once that participant has been joined on.
    remaining: list[Join | InstantiatedUnnestJoin] = [
        join for join in cte.joins if join not in joins
    ]
    cte.joins = [*spine_joins, *remaining]
    cte.parent_ctes = [spine, *cte.parent_ctes]
    cte.base_name_override = spine.name
    cte.base_alias_override = spine.name
    _log(
        f"{cte.name}: lowered {len(joins)} FULL JOIN(s) over "
        f"{[s.address for s in slots]} to key spine {spine.name} with "
        f"{len(participants)} LEFT JOIN(s)"
    )
    return spine


def lower_full_joins(
    input: list[CTE | UnionCTE], root_cte: CTE | UnionCTE
) -> list[CTE | UnionCTE]:
    """Rewrite every FULL JOIN in the working set into a key spine.

    Runs after all other optimization, so the join types it sees are final:
    anything the narrowing passes could prove away is already gone.
    """
    output = list(input)
    targets = [root_cte, *input] if root_cte not in input else list(input)
    for index, cte in enumerate(targets):
        if not isinstance(cte, CTE):
            continue
        spine = _lower_cte(cte, index)
        if spine is not None:
            output.append(spine)
    return output
