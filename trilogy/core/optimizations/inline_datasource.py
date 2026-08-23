from collections import defaultdict

from trilogy.constants import CONFIG
from trilogy.core.enums import JoinType
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.execute import CTE, DatasourceCTE, Join, RecursiveCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    append_condition,
    consumed_parent_column,
    is_sole_consumer,
    rebind_rename_to_consumed,
    rename_reference,
    render_cte_used_map,
)


def _can_inline_filtered_parent(
    cte: CTE,
    parent: DatasourceCTE,
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> bool:
    if not parent.condition or not is_sole_consumer(cte, parent, inverse_map):
        return False
    return all(
        isinstance(join, Join) and join.jointype == JoinType.INNER for join in cte.joins
    )


def _rename_fold_plan(
    cte: CTE,
    parent: DatasourceCTE,
    missing: set[str],
    root_outputs: set[str],
) -> list[tuple[int, BuildConcept]] | None:
    """Plan to render consumer outputs that read a parent-scan RENAME — an
    address the raw datasource cannot supply — from lineage after the fold.

    Each qualifying output is a bare reference to a parent column that is
    itself a single-hop rename of a datasource column (`S_STORE_ID as
    "s_store_id1"`): pin the consumer's column to the rename's base object
    (see rebind_rename_to_consumed) and drop its source_map entry, so the
    merged CTE renders `<raw column> as <name>` exactly as the scan did.
    Returns None when any missing address is not such a rename (a derived
    expression like `x is not null` needs re-derivation this fold cannot
    prove), which keeps the historical refusal."""
    by_address: dict[str, tuple[int, BuildConcept]] = {}
    for i, col in enumerate(cte.output_columns):
        by_address.setdefault(col.address, (i, col))
    plan: list[tuple[int, BuildConcept]] = []
    for address in missing:
        # An existence subselect resolves through existence_source_map against
        # a named CTE; a lineage render can't satisfy it.
        if address in cte.existence_source_map:
            return None
        entry = by_address.get(address)
        if entry is None:
            return None
        i, col = entry
        consumed = consumed_parent_column(col, cte, parent)
        if consumed is None:
            return None
        base = rename_reference(consumed)
        if base is None or (
            base.address not in root_outputs and not (base.pseudonyms & root_outputs)
        ):
            return None
        plan.append((i, rebind_rename_to_consumed(col, base)))
    return plan


def _join_key_demand(cte: CTE, parent_name: str) -> set[str]:
    """Addresses the consumer renders from ``parent_name`` as a join key.

    Join legs resolve their column through ``CTEConceptPair.cte`` /
    ``Join.right_cte``, never through ``source_map``, so a key can be demanded
    from a parent the source_map does not attribute it to. The grand-total
    ``__preql_internal.all_rows`` broadcast marker is the common case: it is a
    synthesized constant, so it carries an empty (or other-parent) source list
    while the dim scan is its only producer on the left leg."""
    demand: set[str] = set()
    for join in cte.joins:
        if not isinstance(join, Join):
            continue
        for pair in join.joinkey_pairs or []:
            if pair.cte is not None and pair.cte.name == parent_name:
                demand.add(pair.left.address)
            if join.right_cte.name == parent_name:
                demand.add(pair.right.address)
    return demand


class InlineDatasource(OptimizationRule):
    def __init__(self):
        super().__init__()
        self.candidates = defaultdict(lambda: set())
        self.count = defaultdict(lambda: 0)

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, UnionCTE):
            optimized = any(
                self.optimize(x, inverse_map=inverse_map)[0] for x in cte.internal_ctes
            )
            return optimized, None
        if isinstance(cte, RecursiveCTE):
            return False, None
        parents = cte.dependency_nodes()
        if not parents:
            return False, None

        self.debug(
            f"Checking {cte.name} for consolidating inline tables with {len(parents)} parents"
        )
        to_inline: list[DatasourceCTE] = []
        for parent_cte in parents:
            if isinstance(parent_cte, UnionCTE):
                continue
            if isinstance(parent_cte, RecursiveCTE):
                continue
            if not isinstance(parent_cte, DatasourceCTE):
                self.debug(
                    f"Cannot inline: parent {parent_cte.name} is not a DatasourceCTE"
                )
                continue
            if not parent_cte.is_root_datasource:
                self.debug(f"Cannot inline: parent {parent_cte.name} is not root")
                continue
            if parent_cte.dependency_nodes():
                self.debug(f"Cannot inline: parent {parent_cte.name} has parents")
                continue
            filtered_inline = _can_inline_filtered_parent(cte, parent_cte, inverse_map)
            if parent_cte.condition and not filtered_inline:
                self.debug(
                    f"Cannot inline: parent {parent_cte.name} has condition, cannot be inlined"
                )
                continue
            if parent_cte.group_to_grain:
                self.debug(f"Cannot inline: parent {parent_cte.name} is grouped")
                continue
            raw_root = parent_cte.source.base_datasource
            if not isinstance(raw_root, BuildDatasource):
                self.debug(f"Cannot inline: Parent {parent_cte.name} is not datasource")
                continue
            root: BuildDatasource = raw_root
            if not root.can_be_inlined:
                self.debug(
                    f"Cannot inline: Parent {parent_cte.name} datasource is not inlineable"
                )
                continue
            # A merged key physically present as one datasource column also
            # satisfies its pseudonym addresses: a fact FK `web_sales.date.id`
            # also covers the canonical `date.id` a consumer inherited through a
            # `left join web_sales.date.id = date.id` merge. Without expanding,
            # a bare fact scan that advertises the canonical can't fold into its
            # consumer (q2.1 juicy/quizzical) -- the base datasource only declares
            # the native address. Same physical column, so the inline renders it
            # correctly; the join resolver is pseudonym-aware.
            #
            # Gated to a SINGLE-consumer scan: inlining a scan shared by >1
            # consumer duplicates it into each (a multiselect's arms sharing one
            # `facts` scan -> two scans). A shared scan is cheaper kept as one CTE,
            # so only expand pseudonyms when this scan feeds exactly one consumer
            # (canonical_collision keeps its single unified `facts` scan).
            root_outputs = {x.address for x in root.output_concepts}
            if len(inverse_map.get(parent_cte.name, [])) <= 1:
                for x in root.output_concepts:
                    root_outputs |= x.pseudonyms
            join_demand = _join_key_demand(cte, parent_cte.name) - root_outputs
            if join_demand:
                self.log(
                    f"Cannot inline: join keys {join_demand} read from "
                    f"{parent_cte.name} are not columns of the raw datasource"
                )
                continue
            inherited = {
                x for x, v in cte.source_map.items() if v and parent_cte.name in v
            }
            if not inherited.issubset(root_outputs):
                # A source_map entry the consumer never renders from this parent
                # is metadata, not a requirement: the bridge attaches derived
                # concepts (e.g. gcat's `org.flag` / `vehicle.full_name`) to the
                # scan that COULD compute them, but the consumer computes them
                # from raw columns itself and later hides them as unused —
                # hide runs after this rule, so consult the rendered used-map
                # (what the consumer actually reads per parent) instead.
                consumed = render_cte_used_map(cte).get(parent_cte.name, set())
                cte_missing = (inherited & consumed) - root_outputs
                if (
                    cte_missing
                    and _rename_fold_plan(cte, parent_cte, cte_missing, root_outputs)
                    is None
                ):
                    self.log(
                        f"Cannot inline: Not all required inputs to {parent_cte.name} are found on datasource, missing {cte_missing}"
                    )
                    continue
            if not root.grain.issubset(parent_cte.grain):
                self.log(
                    f"Cannot inline: {parent_cte.name} is at wrong grain to inline ({root.grain} vs {parent_cte.grain})"
                )
                continue
            to_inline.append(parent_cte)

        # Register every candidate before inlining any, so the cutoff count
        # reflects all consumers of a raw source.
        registered = False
        for replaceable in to_inline:
            if replaceable.name not in self.candidates[cte.name]:
                self.candidates[cte.name].add(replaceable.name)
                self.count[replaceable.source.identifier] += 1
                registered = True
        if registered:
            return True, None
        optimized = False
        for replaceable in to_inline:
            if (
                self.count[replaceable.source.identifier]
                > CONFIG.optimizations.constant_inline_cutoff
            ):
                self.log(
                    f"Skipping inlining raw datasource {replaceable.source.identifier} ({replaceable.name}) due to multiple references"
                )
                continue
            replaceable_base = replaceable.source.base_datasource
            assert replaceable_base is not None  # checked above
            # Recompute the rename-fold plan at apply time — candidacy was
            # established on a prior visit and other merges may have shifted
            # this CTE's source_map since.
            root_outputs = {x.address for x in replaceable_base.output_concepts}
            if len(inverse_map.get(replaceable.name, [])) <= 1:
                for x in replaceable_base.output_concepts:
                    root_outputs |= x.pseudonyms
            join_demand = _join_key_demand(cte, replaceable.name) - root_outputs
            if join_demand:
                self.log(
                    f"Failed to inline {replaceable.name}: join keys {join_demand} "
                    "are not columns of the raw datasource"
                )
                continue
            inherited = {
                x for x, v in cte.source_map.items() if v and replaceable.name in v
            }
            missing: set[str] = set()
            if not inherited.issubset(root_outputs):
                consumed = render_cte_used_map(cte).get(replaceable.name, set())
                missing = (inherited & consumed) - root_outputs
            plan = (
                _rename_fold_plan(cte, replaceable, missing, root_outputs)
                if missing
                else []
            )
            if plan is None:
                self.log(
                    f"Failed to inline {replaceable.name}: rename fold no longer provable"
                )
                continue
            result = cte.inline_parent_datasource(replaceable, force_group=False)
            if result:
                for i, new_col in plan:
                    # Render the rename from lineage post-fold: the pinned base
                    # object resolves against the inlined datasource; a stale
                    # source_map entry would win over lineage and point at a
                    # column the raw table does not have.
                    cte.output_columns[i] = new_col
                    cte.source_map.pop(new_col.address, None)
                if replaceable.condition is not None:
                    cte.condition = append_condition(
                        cte.condition, replaceable.condition
                    )
                self.log(
                    f"Inlined parent {replaceable.name} with {replaceable.source.safe_identifier}"
                )
                optimized = True
            else:
                self.log(f"Failed to inline {replaceable.name}")
        return optimized, None
