"""Hoist predicate-only inner joins from a child CTE to its shared parent CTE.

When several siblings read from a shared parent CTE and each applies the same
dim joins purely to filter rows via the same WHERE predicate, the dim joins and
predicate are evaluated once per sibling. Pushing the join and its predicate up
to the shared parent does that work once: ``JOIN dim WHERE dim.col = X`` acts
as an existence predicate on the FK column already on the parent, and the
siblings shrink to thin projections of it.

Safety constraints:

  - INNER join only (a LEFT join a bundled predicate forces INNER counts).
  - Right side at-grain (dim grain within the join keys), so no fan-out.
  - The dim concepts the join brings in are referenced only by the bundled
    predicates, nowhere else in the child.
  - All siblings of the parent already carry the same predicate, so the
    post-hoist row set matches every sibling's existing expectation.

Runs after InlineDatasource so it does not hoist work to a dim CTE that will
immediately disappear. When the child's dim has already been folded, the hoist
preserves that folded binding on the parent.
"""

from typing import cast

from trilogy.core.enums import JoinType, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConditional,
    BuildDatasource,
    BuildWindowItem,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    DatasourceCTE,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import (
    MergedCTEMap,
    OptimizationRule,
)
from trilogy.core.optimizations.utils import (
    add_datasource_sorted,
    append_condition,
    condition_contains_atom,
    is_grouped_cte,
    render_cte_used_map,
    strip_condition_atom,
)
from trilogy.core.processing.condition_utility import (
    gather_non_null_proofs,
    is_scalar_condition,
)

HOISTABLE_JOIN_TYPES = {JoinType.INNER, JoinType.LEFT_OUTER}


def _datasource_matches(
    left: object,
    right: QueryDatasource | BuildDatasource,
) -> bool:
    if not isinstance(left, (QueryDatasource, BuildDatasource)):
        return False
    if left.identifier == right.identifier:
        return True
    left_base = left.base_datasource if isinstance(left, QueryDatasource) else None
    right_base = right.base_datasource if isinstance(right, QueryDatasource) else None
    if left_base is not None and left_base is right:
        return True
    if right_base is not None and right_base is left:
        return True
    return left_base is not None and left_base is right_base


class JoinHoist(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def _find_left_base_cte(
        self, parent_cte: CTE, fk_addresses: set[str]
    ) -> CTE | UnionCTE | None:
        """Find which dependency provides the FK columns for the hoisted join."""
        for p in parent_cte.dependency_nodes(include_inlined=True):
            if isinstance(p, (CTE, UnionCTE)) and fk_addresses.issubset(
                {c.address for c in p.output_columns}
            ):
                return p
        return None

    def _find_left_base_datasource(
        self, parent_cte: CTE, fk_addresses: set[str]
    ) -> QueryDatasource | BuildDatasource | None:
        """Find which datasource on parent_cte.source provides the FK columns
        (used as ConceptPair.existing_datasource for the new BaseJoin)."""
        for ds in parent_cte.source.datasources:
            ds_outputs = {c.address for c in ds.output_concepts}
            if fk_addresses.issubset(ds_outputs):
                return ds
        return None

    def _find_left_base_join_cte(
        self,
        parent_cte: CTE,
        left_base_ds: QueryDatasource | BuildDatasource,
        fk_addresses: set[str],
    ) -> tuple[CTE | UnionCTE | None, bool]:
        left_base_cte = self._find_left_base_cte(parent_cte, fk_addresses)
        if left_base_cte is not None:
            return left_base_cte, False
        if isinstance(left_base_ds, BuildDatasource):
            return CTE.from_datasource(left_base_ds), True
        return None, False

    def _candidates(self, cte: CTE) -> list:
        if not cte.condition:
            return []
        if isinstance(cte.condition, BuildConditional):
            return cte.condition.decompose()
        return [cte.condition]

    def _collect_referenced_addresses_excluding(
        self, cte: CTE, exclude_candidates: list
    ) -> set[str]:
        """Addresses cte still consumes from any parent after the bundled
        candidates are hypothetically removed.

        Renders cte so the check follows alias/lineage chains a shallow scan
        of ``output_columns`` would miss. The bundled candidates are
        temporarily stripped from ``cte.condition`` so their own references
        are not counted as needed elsewhere.
        """
        original_condition = cte.condition
        stripped_condition = original_condition
        for cand in exclude_candidates:
            stripped_condition = strip_condition_atom(stripped_condition, cand)
        cte.condition = stripped_condition
        try:
            used_map = render_cte_used_map(cte)
        finally:
            cte.condition = original_condition
        referenced: set[str] = set()
        for addrs in used_map.values():
            referenced.update(addrs)
        # Outputs do not show up in cte's own used_map but still have to be
        # projected, so they pin source_map entries.
        referenced.update(c.address for c in cte.output_columns)
        return referenced

    def _join_hoist_plan(
        self,
        cte: CTE,
        parent_cte: CTE,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> list[tuple[Join, list, list, JoinType]] | None:
        """Per-join plan: which joins to hoist, and which candidate predicates
        ride along with each.

        A join is included only when the dim concepts it supplies are
        referenced solely by the bundled candidates, all siblings of
        parent_cte already carry every bundled candidate, and the FK keys are
        materialized on parent_cte. Returns None if nothing is hoistable."""
        if parent_cte.condition and not is_scalar_condition(parent_cte.condition):
            return None
        # Hoisting a join+predicate into a row-limited parent filters before
        # the LIMIT, changing which rows fill it.
        if parent_cte.limit is not None:
            return None
        non_materialized = {k for k, v in parent_cte.source_map.items() if v == []}
        for x in parent_cte.output_columns:
            if x.address in non_materialized and isinstance(x.lineage, BuildWindowItem):
                return None
        materialized = {k for k, v in parent_cte.source_map.items() if v != []}
        siblings = inverse_map.get(parent_cte.name, [])
        if not siblings:
            return None
        if not is_grouped_cte(parent_cte):
            return None
        candidates = [
            c
            for c in self._candidates(cte)
            if isinstance(c, BoolExpr) and is_scalar_condition(c)
            # `existence_arguments` may carry literal IN-list values; only
            # reject concept-bearing ones.
            and not any(arg for tup in c.existence_arguments for arg in tup)
        ]
        if not candidates:
            return None

        child_joins = [
            j
            for j in cte.joins
            if isinstance(j, Join)
            and isinstance(j.right_cte, CTE)
            and j.right_cte.name != parent_cte.name
            and j.right_cte.source is not parent_cte.source
        ]
        # A dim the parent already reads from cannot be hoisted: the parent
        # would render an unaliased self-join `FROM dim <join> dim`.
        parent_source_names = {p.name for p in parent_cte.dependency_nodes()}
        plan: list[tuple[Join, list, list, JoinType]] = []
        for j in child_joins:
            if j.jointype not in HOISTABLE_JOIN_TYPES:
                continue
            if (
                j.right_cte.name == parent_cte.name
                or j.right_cte.source is parent_cte.source
            ):
                continue
            if j.right_cte.name in parent_source_names or any(
                _datasource_matches(d, j.right_cte.source)
                for d in parent_cte.source.datasources
            ):
                continue
            if not j.joinkey_pairs:
                continue
            if j.condition is not None:
                continue
            join: Join = j
            join_keys_left = {p.left.address for p in join.joinkey_pairs or []}
            join_keys_right = {p.right.address for p in join.joinkey_pairs or []}
            if not join_keys_left.issubset(materialized):
                continue
            dim_grain = set(join.right_cte.grain.components)
            if dim_grain and not dim_grain.issubset(join_keys_right):
                continue
            join_brings = {c.address for c in join.right_cte.output_columns}
            filter_concepts = join_brings - join_keys_right
            # to_push: not yet on parent.condition, AND-extend the parent.
            # to_strip_only: already on parent.condition (hoisted via a
            # sibling), only strip from cte.
            to_push: list = []
            to_strip_only: list = []
            bail = False
            for cand in candidates:
                cand_args = {x.address for x in cand.row_arguments}
                if not (cand_args & filter_concepts):
                    continue
                if not cand_args.issubset(filter_concepts | materialized):
                    bail = True
                    break
                already_on_parent = condition_contains_atom(cand, parent_cte.condition)
                if already_on_parent:
                    to_strip_only.append(cand)
                    continue
                # The candidate must apply to all siblings, on each sibling's
                # own condition or already pushed up to the parent.
                if not all(
                    condition_contains_atom(cand, s.condition)
                    or condition_contains_atom(cand, parent_cte.condition)
                    for s in siblings
                ):
                    bail = True
                    break
                to_push.append(cand)
            if bail:
                continue
            bundled = to_push + to_strip_only
            if not bundled:
                continue
            join_type = self._join_type_after_hoist(join, bundled)
            if join_type is None:
                continue
            needed_elsewhere = self._collect_referenced_addresses_excluding(
                cte, bundled
            )
            if filter_concepts & needed_elsewhere:
                continue
            plan.append((join, to_push, to_strip_only, join_type))
        if len(siblings) == 1 and len(plan) != len(child_joins):
            return None
        return plan or None

    def _parent_already_joins_dim(
        self, parent_cte: CTE, dim_qds: QueryDatasource | BuildDatasource
    ) -> bool:
        return any(
            isinstance(bj, BaseJoin)
            and _datasource_matches(bj.right_datasource, dim_qds)
            for bj in parent_cte.source.joins
        )

    def _join_type_after_hoist(self, join: Join, bundled: list) -> JoinType | None:
        if join.jointype == JoinType.INNER:
            return JoinType.INNER
        if join.jointype != JoinType.LEFT_OUTER:
            return None
        right_addresses = {c.address for c in join.right_cte.output_columns}
        forced = {addr for cand in bundled for addr in gather_non_null_proofs(cand)}
        if forced & right_addresses:
            return JoinType.INNER
        return None

    def _hoist_join(
        self,
        cte: CTE,
        parent_cte: CTE,
        join: Join,
        join_type: JoinType,
    ) -> bool:
        """Construct fresh BaseJoin + Join state on parent_cte for `join`, and
        strip the original from cte. If parent_cte already joins the same dim
        (a sibling hoisted it earlier), only strip from cte. Returns True on
        success."""
        assert isinstance(join.right_cte, CTE)
        dim_cte = join.right_cte
        dim_qds: QueryDatasource | BuildDatasource = dim_cte.source
        dim_was_inlined = False
        if isinstance(dim_cte, DatasourceCTE) and cte.renders_inline(dim_cte):
            dim_was_inlined = True
            dim_qds = dim_cte.datasource
        # An inlined dim appears in cte.source_map under its folded datasource
        # identifier, not its CTE name; the cleanup below scrubs whichever
        # token occurs so nothing references a table no longer in the FROM.
        dim_render_token = cte.source_key_for(dim_cte)
        dim_tokens = {dim_cte.name, dim_render_token}

        cte_base_join: BaseJoin | None = None
        for bj in cte.source.joins:
            if isinstance(bj, BaseJoin) and _datasource_matches(
                bj.right_datasource, dim_qds
            ):
                cte_base_join = bj
                break

        if not self._parent_already_joins_dim(parent_cte, dim_qds):
            fk_addresses = {p.left.address for p in (join.joinkey_pairs or [])}
            left_base_ds = self._find_left_base_datasource(parent_cte, fk_addresses)
            if left_base_ds is None:
                self.debug(
                    f"Cannot locate left base for FK {fk_addresses} on "
                    f"{parent_cte.name}; "
                    f"parents={[p.name for p in parent_cte.dependency_nodes()]}, "
                    f"datasources={[d.identifier for d in parent_cte.source.datasources]}"
                )
                return False
            left_base_cte, inline_left_base = self._find_left_base_join_cte(
                parent_cte,
                left_base_ds,
                fk_addresses,
            )
            if left_base_cte is None:
                self.debug(
                    f"Cannot locate left CTE for FK {fk_addresses} on "
                    f"{parent_cte.name}; "
                    f"parents={[p.name for p in parent_cte.dependency_nodes()]}, "
                    f"datasources={[d.identifier for d in parent_cte.source.datasources]}"
                )
                return False

            new_concept_pairs = [
                ConceptPair(
                    left=p.left,
                    right=p.right,
                    existing_datasource=left_base_ds,
                    modifiers=p.modifiers,
                )
                for p in (join.joinkey_pairs or [])
            ]
            new_base_join = BaseJoin(
                right_datasource=dim_qds,
                join_type=join_type,
                concept_pairs=new_concept_pairs,
                modifiers=list(join.modifiers),
            )
            parent_cte.source.joins.append(new_base_join)
            add_datasource_sorted(parent_cte, dim_qds)
            existing_input_addrs = {c.address for c in parent_cte.source.input_concepts}
            for c in dim_cte.output_columns:
                if c.address not in existing_input_addrs:
                    parent_cte.source.input_concepts.append(c)
                    existing_input_addrs.add(c.address)
            for c in dim_cte.output_columns:
                parent_cte.source.source_map.setdefault(c.address, set()).add(dim_qds)
            new_joinkey_pairs = [
                CTEConceptPair(
                    left=p.left,
                    right=p.right,
                    existing_datasource=left_base_ds,
                    modifiers=p.modifiers,
                    cte=left_base_cte,
                )
                for p in (join.joinkey_pairs or [])
            ]
            new_join = Join(
                right_cte=dim_cte,
                jointype=join_type,
                left_cte=None,
                joinkey_pairs=new_joinkey_pairs,
                modifiers=list(join.modifiers),
                # The synthetic left base is the parent's own raw datasource
                # (no parent-CTE alias); its FK keys are local columns.
                left_is_local=inline_left_base,
            )
            parent_cte.joins.append(new_join)
            if dim_was_inlined:
                assert isinstance(dim_cte, DatasourceCTE)
                dim_source_key = parent_cte.add_inlined_datasource(dim_cte)
            else:
                parent_cte.add_dependency(dim_cte)
                dim_source_key = parent_cte.source_key_for(dim_cte)
            for c in dim_cte.output_columns:
                parent_cte.source_map.setdefault(c.address, [])
                if dim_source_key not in parent_cte.source_map[c.address]:
                    parent_cte.source_map[c.address].append(dim_source_key)

        cte.joins.remove(join)
        if cte_base_join is not None and cte_base_join in cte.source.joins:
            cte.source.joins.remove(cte_base_join)
        # Filter-only concepts the dim brought in go away entirely; join keys
        # remain because cte may still need them from the FK side.
        join_keys_right = {p.right.address for p in (join.joinkey_pairs or [])}
        dim_filter_addresses = {
            c.address for c in dim_cte.output_columns
        } - join_keys_right
        still_referenced = any(
            isinstance(bj, BaseJoin)
            and _datasource_matches(bj.right_datasource, dim_qds)
            for bj in cte.source.joins
        )
        if not still_referenced and dim_qds in cte.source.datasources:
            cte.source.datasources = [
                d for d in cte.source.datasources if d.identifier != dim_qds.identifier
            ]
        for addr in dim_filter_addresses:
            qds_sources = cte.source.source_map.get(addr)
            if qds_sources is not None:
                qds_sources = {
                    s
                    for s in qds_sources
                    if not hasattr(s, "identifier")
                    or not _datasource_matches(s, dim_qds)
                }
                if not qds_sources:
                    del cte.source.source_map[addr]
                else:
                    cte.source.source_map[addr] = qds_sources
        cte.source.input_concepts = [
            c
            for c in cte.source.input_concepts
            if c.address not in dim_filter_addresses
        ]
        for addr in dim_filter_addresses:
            cte.source_map.pop(addr, None)
        # Join keys in cte.source_map that pointed to the dim redirect to the
        # FK source (the original pair.cte), or cte renders `dim_cte.col` for a
        # dim no longer in its FROM. The FK (left) key always redirects. The
        # dim (right) key only renders identically to the FK after an INNER
        # join; a scoped join onto a rowset carries that key forward as cte's
        # own output, so it redirects too and get_alias resolves it via the FK
        # source's pseudonym.
        for pair in join.joinkey_pairs or []:
            redirect_addrs = [pair.left.address]
            if join_type == JoinType.INNER and pair.right.address != pair.left.address:
                redirect_addrs.append(pair.right.address)
            for addr in redirect_addrs:
                rendering_sources = cte.source_map.get(addr)
                if not rendering_sources or not dim_tokens.intersection(
                    rendering_sources
                ):
                    continue
                new_sources = [s for s in rendering_sources if s not in dim_tokens]
                if pair.cte is not None and pair.cte.name not in new_sources:
                    new_sources.append(pair.cte.name)
                cte.source_map[addr] = new_sources
                qds_set = cte.source.source_map.get(addr)
                if qds_set is not None:
                    qds_set = {
                        s
                        for s in qds_set
                        if not hasattr(s, "identifier")
                        or not _datasource_matches(s, dim_qds)
                    }
                    if not qds_set:
                        if pair.existing_datasource is not None:
                            cte.source.source_map[addr] = {pair.existing_datasource}
                        else:
                            del cte.source.source_map[addr]
                    else:
                        cte.source.source_map[addr] = qds_set
        still_used = any(
            isinstance(jj, Join)
            and isinstance(jj.right_cte, CTE)
            and jj.right_cte.name == dim_cte.name
            for jj in cte.joins
        )
        if not still_used:
            cte.parent_ctes = [
                p for p in cte.dependency_nodes() if p.name != dim_cte.name
            ]
            # A folded dim with no remaining join is gone from the FROM: drop
            # it from inlined_parents and purge its dangling token from any
            # leftover source_map entries.
            if dim_was_inlined:
                cte.inlined_parents = [
                    p for p in cte.inlined_parents if p.name != dim_cte.name
                ]
                for addr, srcs in list(cte.source_map.items()):
                    if isinstance(srcs, list) and dim_render_token in srcs:
                        cte.source_map[addr] = [
                            s for s in srcs if s != dim_render_token
                        ]
        return True

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if isinstance(cte, UnionCTE):
            return False, None
        if self.complete.get(cte.name):
            return False, None
        parents = cte.dependency_nodes()
        if not cte.condition or not cte.joins or not parents:
            self.complete[cte.name] = True
            return False, None
        existence_parent_names = {
            source
            for sources in cte.existence_source_map.values()
            for source in sources
        }
        flattened_existence = {
            x.address for y in cte.condition.existence_arguments for x in y
        }
        candidate_parents = [
            p
            for p in parents
            if isinstance(p, CTE)
            and p.name not in existence_parent_names
            and not (
                flattened_existence
                and all(x.address in flattened_existence for x in p.output_columns)
            )
            and p.source.source_type not in (SourceType.WINDOW, SourceType.UNNEST)
        ]
        if not candidate_parents:
            self.complete[cte.name] = True
            return False, None

        actions = False
        for parent_cte in candidate_parents:
            plan = self._join_hoist_plan(cte, parent_cte, inverse_map)
            if not plan:
                continue
            for join, to_push, to_strip_only, join_type in plan:
                if not self._hoist_join(cte, parent_cte, join, join_type):
                    continue
                for cand in to_push:
                    parent_cte.condition = append_condition(
                        parent_cte.condition,
                        cast(
                            BoolExpr,
                            cand,
                        ),
                    )
                    cte.condition = strip_condition_atom(cte.condition, cand)
                for cand in to_strip_only:
                    cte.condition = strip_condition_atom(cte.condition, cand)
                self.log(
                    f"Hoisted join {join.right_cte.name} from {cte.name} to "
                    f"{parent_cte.name}: pushed {len(to_push)}, "
                    f"stripped {len(to_strip_only)}"
                )
                actions = True
                self.complete[parent_cte.name] = False
        self.complete[cte.name] = True
        return actions, None
