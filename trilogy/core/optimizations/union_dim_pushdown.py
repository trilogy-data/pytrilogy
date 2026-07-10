"""Push dim joins shared by all consumers of a UnionCTE into each branch.

When every consumer of a UnionCTE post-joins the same dim INNER on the same FK
keys, with the same WHERE atoms on dim columns, those joins do redundant work
across consumers. Push the dim into each ``internal_cte`` branch so the dim
filter applies before ``UNION ALL`` — fact rows that don't match are dropped
early.

Safety constraints:
  - INNER only.
  - Every consumer must post-join the same dim with the same FK key pairs.
  - WHERE atoms touching only dim columns must match across consumers.
  - The dim's grain must be a subset of the join keys (no fan-out).
  - All ``internal_ctes`` must be plain CTEs (nested UnionCTE not yet handled).

Two entry points (``optimize`` dispatches on CTE kind):

  - ``_optimize_union``: the case above — push the shared dim into the union's
    branches and strip it from the direct consumers.

  - ``_optimize_plain``: when a dedup / pure-projection pass-through sits between
    the union and the dim-joining consumers (so they aren't *direct* union
    consumers — e.g. q10's distinct-customer dedup), first MOVE the shared dim
    down into that pass-through. It then becomes a direct dim-joining consumer
    of the union, and the union step pushes it the rest of the way. The plain
    step taints its union parent(s) (resets ``complete``) so the union step
    re-fires on the next driver loop. MOVE-only: requires ``strip_safe`` plus a
    real filter, otherwise the dim would stay on the consumers and chain
    nowhere.

Runs after ``PredicatePushdown`` so consumer WHEREs have settled at the
consumer level, and before any rule that flattens / inlines the UnionCTE.
"""

from dataclasses import dataclass
from typing import cast

from trilogy.core.enums import Derivation, JoinType, SetOperator, SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildConceptArgs,
    BuildDatasource,
    BuildGrain,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    DatasourceCTE,
    Join,
    QueryDatasource,
    SourceBinding,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import (
    MergedCTEMap,
    OptimizationRule,
)
from trilogy.core.optimizations.utils import (
    add_datasource_sorted,
    append_condition,
    strip_condition_atom,
)
from trilogy.core.processing.condition_utility import (
    decompose_condition,
    is_scalar_condition,
)
from trilogy.utility import unique


def _base_datasource(
    datasource: BuildDatasource | QueryDatasource,
) -> BuildDatasource | QueryDatasource | None:
    if isinstance(datasource, QueryDatasource):
        return datasource.base_datasource
    return None


def _datasource_wraps(
    datasource: BuildDatasource | QueryDatasource,
    target: BuildDatasource | QueryDatasource,
) -> bool:
    return _base_datasource(datasource) is target


def _datasource_matches_dim(
    datasource: BuildDatasource | QueryDatasource,
    dim_qds: BuildDatasource | QueryDatasource,
) -> bool:
    return datasource.identifier == dim_qds.identifier or _datasource_wraps(
        datasource, dim_qds
    )


def _add_render_dependencies(
    concepts: list[BuildConcept],
    dim_ds: BuildDatasource | QueryDatasource,
) -> list[BuildConcept]:
    available = dim_ds.output_concepts
    available_by_address = {concept.address: concept for concept in available}
    alias_by_address = {}
    if isinstance(dim_ds, BuildDatasource):
        alias_by_address = {
            column.concept.address: column.alias for column in dim_ds.columns
        }
    seen = {concept.address for concept in concepts}
    idx = 0
    while idx < len(concepts):
        concept = concepts[idx]
        idx += 1
        dependency_sources: list[BuildConceptArgs] = []
        if concept.lineage is not None:
            dependency_sources.append(concept.lineage)
        alias = alias_by_address.get(concept.address)
        if isinstance(alias, BuildConceptArgs):
            dependency_sources.append(alias)
        for source in dependency_sources:
            for dependency in source.rendered_concept_arguments:
                if dependency.address in seen:
                    continue
                available_dependency = available_by_address.get(dependency.address)
                if available_dependency is None:
                    continue
                concepts.append(available_dependency)
                seen.add(available_dependency.address)
    return concepts


def _find_dim_cte_for_qds(consumer: CTE, join_qds_id: str) -> CTE | UnionCTE | None:
    """Locate the CTE the consumer's BaseJoin actually targets.

    Prefer exact binding identifiers. After ``InlineDatasource`` a consumer's
    ``BaseJoin`` can render against the raw ``BuildDatasource`` while the graph
    still holds a ``DatasourceCTE`` wrapper over that datasource. In that case,
    fall back through ``base_datasource`` only when the match is unambiguous.
    """
    binding_base_matches: list[CTE | UnionCTE] = []
    join_base_matches: list[CTE | UnionCTE] = []
    for binding in consumer.source_bindings(include_inlined=True):
        if binding.node is None:
            continue
        if _source_binding_matches_exact_id(binding, join_qds_id):
            return binding.node
        if _source_binding_matches_raw_id(binding, join_qds_id):
            binding_base_matches.append(binding.node)
    for j in consumer.joins:
        if isinstance(j, Join) and isinstance(j.right_cte, (CTE, UnionCTE)):
            if _cte_matches_exact_id(j.right_cte, join_qds_id):
                return j.right_cte
            if _cte_matches_raw_id(j.right_cte, join_qds_id):
                join_base_matches.append(j.right_cte)
    unique_matches = unique(join_base_matches, "name")
    if len(unique_matches) == 1:
        return unique_matches[0]
    unique_matches = unique(binding_base_matches, "name")
    if len(unique_matches) == 1:
        return unique_matches[0]
    return None


def _source_binding_matches_exact_id(binding: SourceBinding, source_id: str) -> bool:
    return binding.node is not None and _cte_matches_exact_id(binding.node, source_id)


def _source_binding_matches_raw_id(binding: SourceBinding, source_id: str) -> bool:
    if binding.node is not None and _cte_matches_raw_id(binding.node, source_id):
        return True
    return binding.datasource is not None and _datasource_matches_raw_id(
        binding.datasource, source_id
    )


def _cte_matches_exact_id(cte: CTE | UnionCTE, source_id: str) -> bool:
    return cte.source.identifier == source_id


def _cte_matches_raw_id(cte: CTE | UnionCTE, source_id: str) -> bool:
    if isinstance(cte, DatasourceCTE) and cte.datasource.identifier == source_id:
        return True
    base = cte.source.base_datasource
    return isinstance(base, BuildDatasource) and base.identifier == source_id


def _datasource_matches_raw_id(
    datasource: BuildDatasource | QueryDatasource, source_id: str
) -> bool:
    if isinstance(datasource, BuildDatasource) and datasource.identifier == source_id:
        return True
    base = _base_datasource(datasource)
    return isinstance(base, BuildDatasource) and base.identifier == source_id


def _dim_local_atoms(
    cte: CTE,
    dim_qds: BuildDatasource | QueryDatasource,
) -> list[BoolExpr]:
    """WHERE atoms on ``cte.condition`` whose ``row_arguments`` reference
    *only* ``dim_qds``'s output concepts. Atoms touching other datasources
    can't be pushed verbatim.

    Existence (subselect) atoms — ``D_WEEK_SEQ IN (SELECT … FROM cooperative)``
    — are included as long as their row_arguments are dim-local. The
    subselect's source CTE must be propagated to each branch via
    ``_push_into_branch``; literal IN-lists carry empty existence concepts
    and need no propagation.
    """
    if not cte.condition:
        return []
    dim_outs = {c.address for c in dim_qds.output_concepts}
    found: list[BoolExpr] = []
    seen: set[str] = set()
    for atom in decompose_condition(cte.condition):
        if not is_scalar_condition(atom):
            continue
        atom_addrs = {x.address for x in atom.row_arguments}
        if not atom_addrs or not atom_addrs.issubset(dim_outs):
            continue
        key = str(atom)
        if key in seen:
            continue
        seen.add(key)
        found.append(atom)
    return found


class _DimDescriptor:
    """One pushable dim shared by every consumer."""

    dim_qds: BuildDatasource | QueryDatasource
    join_qds_id: str
    key_pairs: list[ConceptPair]
    dim_concepts: list[BuildConcept]
    where_atoms: list[BoolExpr]
    strip_safe: bool

    def __init__(
        self,
        dim_qds: BuildDatasource | QueryDatasource,
        join_qds_id: str,
        key_pairs: list[ConceptPair],
        dim_concepts: list[BuildConcept],
        where_atoms: list[BoolExpr],
        strip_safe: bool,
    ) -> None:
        self.dim_qds = dim_qds
        # Original join right_datasource identifier — used to disambiguate
        # multiple sibling dim CTEs sharing the same base BD (e.g. an
        # unfiltered + a filtered variant).
        self.join_qds_id = join_qds_id
        self.key_pairs = key_pairs
        self.dim_concepts = dim_concepts
        self.where_atoms = where_atoms
        # ``strip_safe`` is True when every non-FK dim concept the consumer
        # references appears in a ``where_atom`` (no SELECT/CASE projection
        # references). When False we still push the dim+filter into each
        # branch for early filtering, but the consumer keeps its own dim join
        # (no strip, no union output exposure).
        self.strip_safe = strip_safe

    @property
    def fk_left_addrs(self) -> set[str]:
        return {p.left.address for p in self.key_pairs}


_DimKey = tuple[str, frozenset[tuple[str, str]]]


@dataclass
class _ConsumerDimCandidate:
    dim_qds: BuildDatasource | QueryDatasource
    join_qds_id: str
    key_pairs: list[ConceptPair]
    dim_concepts: list[BuildConcept]
    where_atoms: list[BoolExpr]
    strip_safe: bool


@dataclass
class _PushContext:
    dim_cte: CTE | UnionCTE
    source_consumer: CTE | None


class UnionDimPushdown(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def _is_pass_through(self, cte: CTE) -> bool:
        """True if ``cte`` is a single-source projection or dedup of its
        UnionCTE parent — no joins of any kind, no extra datasources. A
        ``condition`` is permitted; predicate pushdown can leave channel /
        FK filters on a dedup CTE (e.g. q66's ``cooperative`` carries
        ``channel in (WEB,CATALOG)`` and ``warehouse_id is not null``
        pushed up from young), and those are orthogonal to a dim being
        pushed into the union branches.
        """
        if cte.joins:
            return False
        if any(isinstance(j, BaseJoin) for j in cte.source.joins):
            return False
        if len(cte.source.datasources) > 1:
            return False
        return True

    def _expand_pass_through_consumers(
        self,
        consumers: list[CTE],
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> list[CTE] | None:
        """Expand pass-through CTE consumers transitively into their
        downstream consumers so the dim-shape check sees the *effective*
        end-consumers. Pass-throughs themselves stay in the original
        ``consumers`` set (we still need to bookkeep them for the push, but
        we won't strip them — they don't carry the dim join). Returns None
        if any non-CTE turns up or a pass-through has no downstream.
        """
        result: list[CTE] = []
        seen: set[str] = set()
        stack: list[CTE | UnionCTE] = list(consumers)
        while stack:
            c = stack.pop()
            if not isinstance(c, CTE):
                return None
            if c.name in seen:
                continue
            seen.add(c.name)
            if self._is_pass_through(c):
                downstream = inverse_map.get(c.name, [])
                if not downstream:
                    return None
                stack.extend(downstream)
            else:
                result.append(c)
        return result

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if self.complete.get(cte.name):
            return False, None
        if isinstance(cte, UnionCTE):
            return self._optimize_union(cte, inverse_map)
        return self._optimize_plain(cte, inverse_map)

    def _optimize_plain(
        self, cte: CTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        """Push a dim join shared by all of ``cte``'s consumers down *into*
        ``cte`` (the shared parent), stripping it from each consumer.

        Scoped to the case that unblocks the union push: ``cte`` is a
        pass-through (dedup / pure projection) of a ``UnionCTE``. After the dim
        lands on ``cte`` it becomes a *direct* dim-joining consumer of that
        union, so the next loop's ``_optimize_union`` can push it the rest of
        the way into the branches. We taint the union parent(s) so that re-fire
        actually happens (``optimize`` short-circuits on ``complete``).
        """
        self.complete[cte.name] = True
        if not self._is_pass_through(cte):
            return False, None
        union_parents = [p for p in cte.dependency_nodes() if isinstance(p, UnionCTE)]
        if not union_parents:
            return False, None
        consumers_raw = inverse_map.get(cte.name, [])
        consumers = [c for c in consumers_raw if isinstance(c, CTE)]
        if not consumers or len(consumers) != len(consumers_raw):
            return False, None
        descriptors = self._find_shared_dims(cte, consumers)
        if not descriptors:
            return False, None
        actions = False
        for d in descriptors:
            if self._apply_plain(cte, consumers, d):
                actions = True
                # Taint the union parent(s): they were (or will be) marked
                # complete with no pushable consumer; now `cte` carries the dim
                # join, so let the union push reconsider them.
                for up in union_parents:
                    self.complete[up.name] = False
                self.log(
                    f"Pushed dim {d.dim_qds.identifier} into shared parent "
                    f"{cte.name}; stripped from {len(consumers)} consumer(s); "
                    f"tainted union parent(s) {[u.name for u in union_parents]}"
                )
        return actions, None

    def _optimize_union(
        self, cte: UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        # EXCEPT/INTERSECT compare whole rows and (for EXCEPT) are
        # order-sensitive; rewriting their arms changes results. Only a
        # UNION ALL stack is safe to restructure.
        if cte.operator != SetOperator.UNION_ALL.value:
            self.complete[cte.name] = True
            return False, None
        if not all(isinstance(b, CTE) for b in cte.internal_ctes):
            self.complete[cte.name] = True
            return False, None
        consumers_raw = inverse_map.get(cte.name, [])
        consumers = [c for c in consumers_raw if isinstance(c, CTE)]
        if not consumers or len(consumers) != len(consumers_raw):
            self.complete[cte.name] = True
            return False, None

        # Look through pass-through CTEs (dedup / pure projection) to the real
        # dim-joining consumers. Direct consumers that ARE pass-throughs stay
        # available, but for shape-matching we use the effective set.
        effective = self._expand_pass_through_consumers(consumers, inverse_map)
        if not effective:
            self.complete[cte.name] = True
            return False, None
        descriptors = self._find_shared_dims(cte, effective)
        if not descriptors:
            self.complete[cte.name] = True
            return False, None

        # Strip is only safe from direct consumers (those that actually own the
        # dim join). Pass-throughs carry no join; non-direct consumers (reached
        # via pass-through) hold their own join + filter and must keep them —
        # the source_map redirect to the UnionCTE can't navigate through the
        # intermediate pass-through CTE.
        direct_with_dim = [c for c in consumers if c in effective]  # not a pass-through
        actions = False
        for d in descriptors:
            if self._apply(cte, direct_with_dim, d):
                actions = True
                self.log(
                    f"Pushed dim {d.dim_qds.identifier} into "
                    f"{len(cte.internal_ctes)} branch(es) of {cte.name}; "
                    f"stripped from {len(direct_with_dim)} direct consumer(s) "
                    f"(effective consumer set: {len(effective)})"
                )
        self.complete[cte.name] = True
        return actions, None

    # ---- detection ----

    def _consumer_dim_map(
        self, consumer: CTE, union_outputs: set[str]
    ) -> dict[_DimKey, _ConsumerDimCandidate]:
        """Map each pushable INNER dim join on this consumer to its descriptor
        bits. Key: (dim_id, frozenset of (left_addr, right_addr) pairs).

        We resolve the dim down to its underlying ``BuildDatasource`` because
        ``source.datasources`` and ``source_map`` consistently reference the
        BD form (whereas ``BaseJoin.right_datasource`` is often a QDS wrapper
        with a different ``safe_identifier``)."""
        # Map BD identifier -> BuildDatasource on this consumer (preferred form
        # for downstream replication onto branches).
        bd_by_id: dict[str, BuildDatasource] = {
            ds.identifier: ds
            for ds in consumer.source.datasources
            if isinstance(ds, BuildDatasource)
        }
        result: dict[_DimKey, _ConsumerDimCandidate] = {}
        for j in consumer.source.joins:
            if not isinstance(j, BaseJoin):
                continue
            if j.join_type != JoinType.INNER:
                continue
            if not j.concept_pairs:
                continue
            join_qds = j.right_datasource
            if join_qds.identifier == consumer.source.identifier:
                continue
            # Skip dims that are themselves UnionCTEs — pushing a union-shaped
            # dim into another union's branches mangles the alias mapping
            # (the long QDS identifier ends up in the WHERE while the join
            # renders with the dim CTE's short alias). q5's
            # return_channel_dim (a UNION of catalog/store/web dim variants)
            # is the canonical example.
            if (
                isinstance(join_qds, QueryDatasource)
                and join_qds.source_type == SourceType.UNION
            ):
                continue
            # Resolve to the underlying BD if the consumer carries one.
            if isinstance(join_qds, BuildDatasource):
                dim_ds: BuildDatasource | QueryDatasource = join_qds
            else:
                base = join_qds.base_datasource
                if isinstance(base, BuildDatasource) and base.identifier in bd_by_id:
                    dim_ds = bd_by_id[base.identifier]
                else:
                    # No matching BD on consumer.datasources — keep the QDS
                    # form. The dim isn't inlineable into a flat table ref so
                    # we'd need a separate dim CTE either way.
                    dim_ds = join_qds
            left_addrs = {p.left.address for p in j.concept_pairs}
            if not left_addrs.issubset(union_outputs):
                continue
            dim_grain_addrs = set(dim_ds.grain.components) if dim_ds.grain else set()
            right_addrs = {p.right.address for p in j.concept_pairs}
            if dim_grain_addrs and not dim_grain_addrs.issubset(right_addrs):
                continue
            key = (
                dim_ds.identifier,
                frozenset((p.left.address, p.right.address) for p in j.concept_pairs),
            )
            where_atoms = _dim_local_atoms(consumer, dim_ds)
            # Only project dim concepts the consumer actually uses (FK keys
            # plus any non-key dim concepts the consumer references).
            consumer_uses = {c.address for c in consumer.source.input_concepts}
            consumer_uses |= {c.address for c in consumer.output_columns}
            if consumer.condition is not None:
                for atom in decompose_condition(consumer.condition):
                    if hasattr(atom, "concept_arguments"):
                        consumer_uses |= {c.address for c in atom.concept_arguments}
            # Strip safety: stripping this dim from the consumer is only safe
            # when every non-FK dim concept it uses is referenced solely in
            # WHERE atoms (which we relocate to the branches). A dim concept
            # used in a SELECT/CASE projection can't be rerouted through the
            # union output_columns by the strip step because the rendering
            # layer still references the original dim alias — q66's young
            # uses ``date.month_of_year`` inside the per-month CASE, so
            # date_dim is not strip-safe for young.
            #
            # When ``strip_safe`` is False we still push the dim + filter
            # into each union branch for early filtering, but the consumer
            # keeps its own dim join (no strip, no union output exposure).
            fk_addrs = {p.left.address for p in j.concept_pairs} | {
                p.right.address for p in j.concept_pairs
            }
            where_atom_concepts: set[str] = set()
            for atom in where_atoms:
                where_atom_concepts |= {c.address for c in atom.row_arguments}
            non_fk_dim_uses = {
                c.address for c in dim_ds.output_concepts if c.address in consumer_uses
            } - fk_addrs
            strip_safe = not (non_fk_dim_uses - where_atom_concepts)
            dim_concepts = [
                c for c in dim_ds.output_concepts if c.address in consumer_uses
            ]
            # Always carry FK keys so the join condition has both sides.
            present = {c.address for c in dim_concepts}
            for p in j.concept_pairs:
                if p.right.address not in present:
                    dim_concepts.append(p.right)
                    present.add(p.right.address)
            dim_concepts = _add_render_dependencies(
                dim_concepts,
                dim_ds,
            )
            result[key] = _ConsumerDimCandidate(
                dim_qds=dim_ds,
                join_qds_id=join_qds.identifier,
                key_pairs=list(j.concept_pairs),
                dim_concepts=dim_concepts,
                where_atoms=where_atoms,
                strip_safe=strip_safe,
            )
        return result

    def _find_shared_dims(
        self, container: CTE | UnionCTE, consumers: list[CTE]
    ) -> list[_DimDescriptor]:
        union_outputs = {x.address for x in container.output_columns}
        per_consumer = [self._consumer_dim_map(c, union_outputs) for c in consumers]
        if not per_consumer or any(not m for m in per_consumer):
            return []
        common_keys = set(per_consumer[0].keys())
        for m in per_consumer[1:]:
            common_keys &= set(m.keys())
        if not common_keys:
            return []
        out: list[_DimDescriptor] = []
        for key in sorted(common_keys, key=lambda k: k[0]):
            atom_strs = [
                frozenset(str(a) for a in m[key].where_atoms) for m in per_consumer
            ]
            if len(set(atom_strs)) != 1:
                continue
            first = per_consumer[0][key]
            # strip_safe is consensual: only strip when every consumer can
            # safely have the dim removed. If any consumer projects a dim
            # concept outside the WHERE atoms we keep the dim on all
            # consumers and just push for early filtering.
            strip_safe = all(m[key].strip_safe for m in per_consumer)
            out.append(
                _DimDescriptor(
                    dim_qds=first.dim_qds,
                    join_qds_id=first.join_qds_id,
                    key_pairs=first.key_pairs,
                    dim_concepts=first.dim_concepts,
                    where_atoms=list(first.where_atoms),
                    strip_safe=strip_safe,
                )
            )
        return out

    # ---- transform ----

    def _dim_cte_exposes(self, dim_cte: CTE | UnionCTE, d: _DimDescriptor) -> bool:
        """The resolved dim CTE must actually expose the dim columns we intend to
        join (FK right key + pushed dim concepts). ``_find_dim_cte_for_qds``'s
        raw-id fallback matches *any* CTE whose base datasource is the dim — which
        wrongly includes a filtered/aggregated derivative of the dim (e.g. q02's
        ``relevent_week_seq`` CTE, a GROUP over ``date_dim`` exposing only that one
        derived column). Joining that as the dim renders columns it doesn't have."""
        out = {c.address for c in dim_cte.output_columns}
        needed = {c.address for c in d.dim_concepts}
        needed |= {p.right.address for p in d.key_pairs}
        return needed.issubset(out)

    def _resolve_push_context(
        self, consumers: list[CTE], d: _DimDescriptor
    ) -> _PushContext | None:
        for c in consumers:
            found = _find_dim_cte_for_qds(c, d.join_qds_id)
            if found is not None and self._dim_cte_exposes(found, d):
                return _PushContext(dim_cte=found, source_consumer=c)
        return None

    def _can_push_into_branch(self, branch: CTE, d: _DimDescriptor) -> bool:
        if any(
            isinstance(j, BaseJoin)
            and j.right_datasource.identifier == d.dim_qds.identifier
            for j in branch.source.joins
        ):
            return True
        branch_out_addrs = {c.address for c in branch.output_columns}
        return (
            d.fk_left_addrs.issubset(branch_out_addrs)
            and self._branch_left_datasource(branch, d.fk_left_addrs) is not None
        )

    def _apply(self, union: UnionCTE, consumers: list[CTE], d: _DimDescriptor) -> bool:
        # Filter-only mode without a filter is pure waste: the dim ends up
        # joined inside each branch *and* on the consumer for no row
        # reduction (INNER on a grain-bounded FK doesn't drop rows). Bail
        # so q75's item-style dims that no consumer filters on aren't
        # duplicated into every union branch.
        if not d.strip_safe and not d.where_atoms:
            return False

        context = self._resolve_push_context(consumers, d)
        if context is None:
            return False

        branches: list[CTE] = []
        for branch in union.internal_ctes:
            if not isinstance(branch, CTE):
                return False
            branches.append(branch)
        if not all(self._can_push_into_branch(branch, d) for branch in branches):
            return False

        for branch in branches:
            if not self._push_into_branch(
                branch,
                context.dim_cte,
                d,
                context.source_consumer,
            ):
                return False

        if d.strip_safe:
            # Expose dim concepts on UnionCTE outputs (consumers will now
            # resolve them through the union after the strip).
            existing = {col.address for col in union.output_columns}
            for concept in d.dim_concepts:
                if concept.address not in existing:
                    union.output_columns.append(concept)
                    existing.add(concept.address)

            for consumer in consumers:
                self._strip_from_consumer(consumer, context.dim_cte, d, union)
        else:
            # Filter-only mode: the consumer keeps its dim join (some dim
            # column is referenced in SELECT/CASE) but the WHERE atoms we
            # pushed into each union branch are now redundant on the
            # consumer — every row coming through the union already passes
            # them, and the consumer's own INNER dim join is grain-bounded
            # to the same dim row. Strip the duplicate filter.
            for consumer in consumers:
                for atom in d.where_atoms:
                    consumer.condition = strip_condition_atom(consumer.condition, atom)

        # If any pushed atom had concept-bearing existence_arguments, the
        # branches may have gained a new parent CTE (the subselect source).
        # Re-derive union.parent_ctes so the CTE reorder pass sees the edge.
        has_existence = any(
            any(arg for tup in a.existence_arguments for arg in tup)
            for a in d.where_atoms
        )
        if has_existence:
            union.parent_ctes = unique(
                [
                    p
                    for branch in union.internal_ctes
                    for p in branch.dependency_nodes()
                ],
                "name",
            )
        return True

    def _apply_plain(
        self, target: CTE, consumers: list[CTE], d: _DimDescriptor
    ) -> bool:
        """Push the shared dim down into ``target`` (the consumers' common
        parent) and strip it from each consumer. MOVE-only: we require
        ``strip_safe`` (no consumer projects a dim column outside the pushed
        WHERE) and an actual filter — a filter-less or non-strippable dim would
        leave the join on the consumers, which neither prunes nor chains into
        the union below ``target``.
        """
        if not d.strip_safe or not d.where_atoms:
            return False
        context = self._resolve_push_context(consumers, d)
        if context is None:
            return False
        if not self._can_push_into_branch(target, d):
            return False
        if not self._push_into_branch(
            target, context.dim_cte, d, context.source_consumer
        ):
            return False
        existing = {col.address for col in target.output_columns}
        for concept in d.dim_concepts:
            if concept.address not in existing:
                target.output_columns.append(concept)
                existing.add(concept.address)
        for consumer in consumers:
            self._strip_from_consumer(consumer, context.dim_cte, d, target)
        self._coarsen_dead_fk_grain(target, d, consumers)
        return True

    @staticmethod
    def _rendered_addrs(cte: CTE) -> set[str]:
        """Addresses ``cte`` actually emits/references: visible outputs, grain
        (GROUP BY) keys, condition concepts, and join keys. Hidden output
        columns that aren't grain keys are *not* rendered."""
        addrs = {
            x.address
            for x in cte.output_columns
            if x.address not in cte.hidden_concepts
        }
        addrs |= set(cte.grain.components)
        if cte.condition is not None:
            addrs |= {x.address for x in cte.condition.concept_arguments}
        for j in cte.joins or []:
            for pair in (j.joinkey_pairs or []) if isinstance(j, Join) else []:
                addrs |= {pair.left.address, pair.right.address}
        return addrs

    def _coarsen_dead_fk_grain(
        self, target: CTE, d: _DimDescriptor, consumers: list[CTE]
    ) -> bool:
        """After moving a dim into a pure-dedup ``target``, drop the FK from its
        grain when the FK is now dead.

        Once the dim join lives on ``target``, its FK (e.g. ``date.id``) was only
        there to reach the dim; the coarser dim attribute we exposed (e.g.
        ``date.year``) is what flows downstream. Deduping at the finer FK grain
        then emits redundant duplicate rows (q04: one per sale-date instead of
        one per year). Drop the FK from ``target``'s grain/output when:

          - ``target`` is a pure dedup (group-to-grain, no aggregate outputs),
          - a coarser dim attr we exposed (FK-functionally-determined) remains
            on ``target``, and
          - every consumer is itself a pure dedup that never renders the FK.

        Those constraints make the coarsening sound: the consumers collapse the
        removed multiplicity, and nothing reads the dropped key. (The FK is
        usually still a *visible* output here — ``HideUnusedConcepts`` runs after
        this rule — so we gate on downstream use, not on the hidden flag.)
        """
        if not target.group_to_grain:
            return False
        if any(c.derivation == Derivation.AGGREGATE for c in target.output_columns):
            return False
        fk_addrs = set(d.fk_left_addrs)
        droppable = fk_addrs & set(target.grain.components)
        if not droppable:
            return False
        target_out = {c.address for c in target.output_columns}
        exposed = ({c.address for c in d.dim_concepts} - fk_addrs) & target_out
        if not exposed:
            return False
        for consumer in consumers:
            if not (
                consumer.group_to_grain
                or consumer.source.source_type == SourceType.GROUP
            ):
                return False
            if any(
                c.derivation == Derivation.AGGREGATE for c in consumer.output_columns
            ):
                return False
            if droppable & self._rendered_addrs(consumer):
                return False

        target.output_columns = [
            x for x in target.output_columns if x.address not in droppable
        ]
        target.grain = BuildGrain(
            components={x.address for x in target.output_columns},
            where_clause=target.grain.where_clause,
        )
        for addr in droppable:
            target.source_map.pop(addr, None)
            target.hidden_concepts.discard(addr)
            for consumer in consumers:
                consumer.source_map.pop(addr, None)
                consumer.existence_source_map.pop(addr, None)
        self.log(
            f"Coarsened dead FK {sorted(droppable)} out of dedup {target.name}'s "
            f"grain after pushing {d.dim_qds.identifier}"
        )
        return True

    def _push_into_branch(
        self,
        branch: CTE,
        dim_cte: CTE | UnionCTE,
        d: _DimDescriptor,
        source_consumer: CTE | None = None,
    ) -> bool:
        # already there? (idempotency)
        if any(
            isinstance(j, BaseJoin)
            and j.right_datasource.identifier == d.dim_qds.identifier
            for j in branch.source.joins
        ):
            return True
        # FK columns must be available on this branch
        branch_out_addrs = {c.address for c in branch.output_columns}
        if not d.fk_left_addrs.issubset(branch_out_addrs):
            return False
        left_ds = self._branch_left_datasource(branch, d.fk_left_addrs)
        if left_ds is None:
            return False
        new_pairs = [
            ConceptPair(
                left=p.left,
                right=p.right,
                existing_datasource=left_ds,
                modifiers=p.modifiers,
            )
            for p in d.key_pairs
        ]
        branch.source.joins.append(
            BaseJoin(
                right_datasource=d.dim_qds,
                join_type=JoinType.INNER,
                concept_pairs=new_pairs,
            )
        )
        add_datasource_sorted(branch, d.dim_qds)
        existing_input = {c.address for c in branch.source.input_concepts}
        existing_qds_out = {c.address for c in branch.source.output_concepts}
        new_outputs: list[BuildConcept] = []
        for c in d.dim_concepts:
            if c.address not in existing_input:
                branch.source.input_concepts.append(c)
                existing_input.add(c.address)
            if c.address not in existing_qds_out:
                new_outputs.append(c)
                existing_qds_out.add(c.address)
        if new_outputs:
            # Reassign (not append) so QueryDatasource.__setattr__ drops the
            # memoized identifier — output_concepts feeds the group-by key.
            branch.source.output_concepts = branch.source.output_concepts + new_outputs
        for c in d.dim_concepts:
            branch.source.source_map.setdefault(c.address, set()).add(d.dim_qds)
        cte_pairs = [
            CTEConceptPair(
                left=p.left,
                right=p.right,
                existing_datasource=left_ds,
                modifiers=p.modifiers,
                cte=branch,
            )
            for p in d.key_pairs
        ]
        new_join = Join(
            right_cte=dim_cte,
            jointype=JoinType.INNER,
            left_cte=None,
            joinkey_pairs=cte_pairs,
            # The branch CTE name isn't a valid alias inside its own SELECT;
            # the LHS keys are the branch's own base columns. Render them as
            # the branch's expressions (mirrors the historical inline_cte).
            left_is_local=isinstance(branch.source.base_datasource, BuildDatasource),
        )
        # If the dim was folded into the consumer (inlined datasource), fold
        # it into the branch too so the branch renders its raw table — the
        # branch CTE wouldn't be emitted to join by name. Otherwise it is a
        # normal emitted CTE the branch joins by name.
        if (
            isinstance(dim_cte, DatasourceCTE)
            and source_consumer is not None
            and source_consumer.renders_inline(dim_cte)
        ):
            dim_source_key = branch.add_inlined_datasource(dim_cte)
        else:
            branch.add_dependency(dim_cte)
            dim_source_key = branch.source_key_for(dim_cte)
        branch.joins.append(new_join)
        existing_out = {c.address for c in branch.output_columns}
        # FK concepts already resolve via the branch's fact ds. Only the
        # *non-key* dim columns need a source_map entry pointing at the dim;
        # adding FK there too would make render_expr coalesce two sources.
        non_key_dim_addrs = {
            c.address for c in d.dim_concepts if c.address not in d.fk_left_addrs
        }
        for c in d.dim_concepts:
            if c.address not in existing_out:
                branch.output_columns.append(c)
                existing_out.add(c.address)
            if c.address not in non_key_dim_addrs:
                continue
            branch.source_map.setdefault(c.address, [])
            if dim_source_key not in branch.source_map[c.address]:
                branch.source_map[c.address].append(dim_source_key)
        for atom in d.where_atoms:
            # append_condition dedups on AND-atoms, so re-adding a predicate the
            # branch already carries is a no-op — no explicit contains-guard.
            branch.condition = append_condition(branch.condition, cast(BoolExpr, atom))
            # For subselect atoms (``D_WEEK_SEQ IN (SELECT … FROM cooperative)``)
            # the inner reference renders against the branch's
            # source_map/existence_source_map; copy the consumer's entry and
            # add the subselect source CTE to branch.parent_ctes.
            if source_consumer is not None:
                self._propagate_existence_sources(branch, source_consumer, atom)
        return True

    def _propagate_existence_sources(
        self,
        branch: CTE,
        source_consumer: CTE,
        atom: BoolExpr,
    ) -> None:
        existence_addrs: set[str] = set()
        for tup in atom.existence_arguments:
            for arg in tup:
                existence_addrs.add(arg.address)
        if not existence_addrs:
            return
        for x in existence_addrs:
            if x in branch.source_map or x in branch.existence_source_map:
                continue
            if x in source_consumer.source_map:
                origin = list(source_consumer.source_map[x])
                branch.source_map[x] = origin
            elif x in source_consumer.existence_source_map:
                origin = list(source_consumer.existence_source_map[x])
                branch.existence_source_map[x] = origin
            else:
                continue
            sources = [
                p for p in source_consumer.dependency_nodes() if p.name in origin
            ]
            for source in sources:
                branch.add_dependency(source)

    def _branch_left_datasource(
        self, branch: CTE, fk_left_addrs: set[str]
    ) -> BuildDatasource | QueryDatasource | None:
        for ds in branch.source.datasources:
            ds_outs = {c.address for c in ds.output_concepts}
            if fk_left_addrs.issubset(ds_outs):
                return ds
        return None

    def _strip_from_consumer(
        self,
        consumer: CTE,
        dim_cte: CTE | UnionCTE,
        d: _DimDescriptor,
        union: CTE | UnionCTE,
    ) -> None:
        # Source-map entries can carry the dim identifier in several forms:
        # the dim CTE name, the dim BD's safe_identifier, the QDS wrapper's
        # safe_identifier (with ``_at_<key>`` suffix). Strip them all.
        dim_aliases = {dim_cte.name, d.dim_qds.safe_identifier}
        for j in consumer.source.joins:
            if isinstance(j, BaseJoin) and _datasource_wraps(
                j.right_datasource, d.dim_qds
            ):
                dim_aliases.add(j.right_datasource.safe_identifier)

        # Remove BaseJoin (match by base_datasource too — j.right_datasource
        # may be a QDS wrapper over the BD form we're tracking).
        def _is_dim_basejoin(j) -> bool:
            if not isinstance(j, BaseJoin):
                return False
            rd = j.right_datasource
            if rd.identifier == d.dim_qds.identifier:
                return True
            return _datasource_wraps(rd, d.dim_qds)

        consumer.source.joins = [
            j for j in consumer.source.joins if not _is_dim_basejoin(j)
        ]
        # Remove Join
        consumer.joins = [
            j
            for j in consumer.joins
            if not (
                isinstance(j, Join)
                and isinstance(j.right_cte, (CTE, UnionCTE))
                and j.right_cte.name == dim_cte.name
            )
        ]
        still_used = any(_is_dim_basejoin(j) for j in consumer.source.joins)
        if not still_used:
            consumer.source.datasources = [
                ds
                for ds in consumer.source.datasources
                if not _datasource_matches_dim(ds, d.dim_qds)
            ]
            consumer.parent_ctes = [
                p for p in consumer.dependency_nodes() if p.name != dim_cte.name
            ]
        # Redirect source_map: dim concepts now resolve via the union CTE.
        union_outputs_now = {c.address for c in union.output_columns}
        for c in d.dim_concepts:
            addr = c.address
            if addr in consumer.source.source_map:
                consumer.source.source_map[addr] = {
                    s
                    for s in consumer.source.source_map[addr]
                    if not (
                        isinstance(s, (BuildDatasource, QueryDatasource))
                        and _datasource_matches_dim(s, d.dim_qds)
                    )
                }
                if not consumer.source.source_map[addr]:
                    consumer.source.source_map[addr] = {union.source}
            if addr in consumer.source_map:
                consumer.source_map[addr] = [
                    s for s in consumer.source_map[addr] if s not in dim_aliases
                ]
                if (
                    union.name not in consumer.source_map[addr]
                    and addr in union_outputs_now
                ):
                    consumer.source_map[addr].append(union.name)
        # Strip pushed WHERE atoms from consumer.condition.
        for atom in d.where_atoms:
            consumer.condition = strip_condition_atom(consumer.condition, atom)
