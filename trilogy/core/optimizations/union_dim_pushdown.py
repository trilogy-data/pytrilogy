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

Runs after ``PredicatePushdown`` so consumer WHEREs have settled at the
consumer level, and before any rule that flattens / inlines the UnionCTE.
"""

from typing import cast

from trilogy.core.enums import BooleanOperator, JoinType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConceptArgs,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import (
    MergedCTEMap,
    OptimizationRule,
)
from trilogy.core.processing.condition_utility import (
    decompose_condition,
    is_scalar_condition,
)
from trilogy.utility import unique

ConditionExpression = BuildComparison | BuildConditional | BuildParenthetical


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
            dependencies = getattr(source, "rendered_concept_arguments", [])
            for dependency in dependencies:
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

    Match strictly on ``cte.source.identifier == join_qds_id`` — never on
    ``base_datasource`` — because filtered/unfiltered variants of the same
    dim share a base. After ``InlineDatasource`` the dim CTE is dropped from
    ``parent_ctes`` but ``Join.right_cte`` retains the reference, so search
    there too.
    """
    for p in consumer.parent_ctes:
        if isinstance(p, (CTE, UnionCTE)) and p.source.identifier == join_qds_id:
            return p
    for j in consumer.joins:
        if (
            isinstance(j, Join)
            and isinstance(j.right_cte, (CTE, UnionCTE))
            and j.right_cte.source.identifier == join_qds_id
        ):
            return j.right_cte
    return None


def _dim_local_atoms(
    cte: CTE,
    dim_qds: BuildDatasource | QueryDatasource,
) -> list[ConditionExpression]:
    """WHERE atoms on ``cte.condition`` that reference *only* ``dim_qds``'s
    output concepts. Atoms touching other datasources can't be pushed verbatim.

    Mirroring ``JoinHoist``: reject atoms whose ``existence_arguments`` carry
    concepts (subselect references that need the source CTE to remain visible).
    Literal IN-lists are fine — those have empty existence_arguments at the
    concept level.
    """
    if not cte.condition:
        return []
    dim_outs = {c.address for c in dim_qds.output_concepts}
    found: list[ConditionExpression] = []
    seen: set[str] = set()
    for atom in decompose_condition(cte.condition):
        if not is_scalar_condition(atom):
            continue
        if any(arg for tup in getattr(atom, "existence_arguments", ()) for arg in tup):
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

    def __init__(
        self,
        dim_qds: BuildDatasource | QueryDatasource,
        join_qds_id: str,
        key_pairs: list[ConceptPair],
        dim_concepts: list[BuildConcept],
        where_atoms: list[ConditionExpression],
    ) -> None:
        self.dim_qds = dim_qds
        # Original join right_datasource identifier — used to disambiguate
        # multiple sibling dim CTEs sharing the same base BD (e.g. an
        # unfiltered + a filtered variant).
        self.join_qds_id = join_qds_id
        self.key_pairs = key_pairs
        self.dim_concepts = dim_concepts
        self.where_atoms = where_atoms

    @property
    def fk_left_addrs(self) -> set[str]:
        return {p.left.address for p in self.key_pairs}


class UnionDimPushdown(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.complete: dict[str, bool] = {}

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if self.complete.get(cte.name):
            return False, None
        if not isinstance(cte, UnionCTE):
            return False, None
        if not all(isinstance(b, CTE) for b in cte.internal_ctes):
            self.complete[cte.name] = True
            return False, None
        consumers_raw = inverse_map.get(cte.name, [])
        consumers = [c for c in consumers_raw if isinstance(c, CTE)]
        if not consumers or len(consumers) != len(consumers_raw):
            self.complete[cte.name] = True
            return False, None

        descriptors = self._find_shared_dims(cte, consumers)
        if not descriptors:
            self.complete[cte.name] = True
            return False, None

        actions = False
        for d in descriptors:
            if self._apply(cte, consumers, d):
                actions = True
                self.log(
                    f"Pushed dim {d.dim_qds.identifier} into {len(cte.internal_ctes)} "
                    f"branch(es) of {cte.name}; stripped from {len(consumers)} consumer(s)"
                )
        self.complete[cte.name] = True
        return actions, None

    # ---- detection ----

    def _consumer_dim_map(
        self, consumer: CTE, union_outputs: set[str]
    ) -> dict[tuple[str, frozenset[tuple[str, str]]], dict]:
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
        result: dict[tuple[str, frozenset[tuple[str, str]]], dict] = {}
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
            # Resolve to the underlying BD if the consumer carries one.
            if isinstance(join_qds, BuildDatasource):
                dim_ds: BuildDatasource | QueryDatasource = join_qds
            else:
                base = getattr(join_qds, "base_datasource", None)
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
            result[key] = {
                "base_join": j,
                "dim_qds": dim_ds,
                "join_qds_id": join_qds.identifier,
                "key_pairs": list(j.concept_pairs),
                "dim_concepts": dim_concepts,
                "where_atoms": where_atoms,
            }
        return result

    def _find_shared_dims(
        self, union: UnionCTE, consumers: list[CTE]
    ) -> list[_DimDescriptor]:
        union_outputs = {x.address for x in union.output_columns}
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
                frozenset(str(a) for a in m[key]["where_atoms"]) for m in per_consumer
            ]
            if len(set(atom_strs)) != 1:
                continue
            first = per_consumer[0][key]
            out.append(
                _DimDescriptor(
                    dim_qds=first["dim_qds"],
                    join_qds_id=first["join_qds_id"],
                    key_pairs=first["key_pairs"],
                    dim_concepts=first["dim_concepts"],
                    where_atoms=list(first["where_atoms"]),
                )
            )
        return out

    # ---- transform ----

    def _apply(self, union: UnionCTE, consumers: list[CTE], d: _DimDescriptor) -> bool:
        # Locate the dim CTE on each consumer so we can re-target into branches.
        dim_cte: CTE | UnionCTE | None = None
        inlined_entry = None
        for c in consumers:
            found = _find_dim_cte_for_qds(c, d.join_qds_id)
            if found is not None:
                dim_cte = found
                # ``CTE.inlined_ctes`` is keyed by ``new_alias``, not by the
                # original CTE name. Scan values to recover the entry.
                for e in c.inlined_ctes.values():
                    if e.original_alias == found.name:
                        inlined_entry = e
                        break
                break
        if dim_cte is None:
            return False

        for branch in union.internal_ctes:
            if not isinstance(branch, CTE):
                return False
            if not self._push_into_branch(branch, dim_cte, d, inlined_entry):
                return False

        # Expose dim concepts on UnionCTE outputs.
        existing = {col.address for col in union.output_columns}
        for concept in d.dim_concepts:
            if concept.address not in existing:
                union.output_columns.append(concept)
                existing.add(concept.address)

        for consumer in consumers:
            self._strip_from_consumer(consumer, dim_cte, d, union)
        return True

    def _push_into_branch(
        self,
        branch: CTE,
        dim_cte: CTE | UnionCTE,
        d: _DimDescriptor,
        inlined_entry=None,
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
        if d.dim_qds not in branch.source.datasources:
            branch.source.datasources = sorted(
                branch.source.datasources + [d.dim_qds],
                key=lambda x: x.identifier,
            )
        existing_input = {c.address for c in branch.source.input_concepts}
        existing_qds_out = {c.address for c in branch.source.output_concepts}
        for c in d.dim_concepts:
            if c.address not in existing_input:
                branch.source.input_concepts.append(c)
                existing_input.add(c.address)
            if c.address not in existing_qds_out:
                branch.source.output_concepts.append(c)
                existing_qds_out.add(c.address)
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
        )
        # The branch CTE name isn't a valid alias inside its own SELECT —
        # the FROM clause aliases the underlying BuildDatasource. Mark the
        # branch as "inlined" on this Join so the LHS renders using
        # ``branch.source.base_datasource.safe_identifier``.
        branch_base = branch.source.base_datasource
        if isinstance(branch_base, BuildDatasource):
            new_join.inline_cte(branch, branch_base)
        if inlined_entry is not None:
            # Dim was inlined on the consumer; replicate on this branch so
            # the RHS renders using the dim's underlying ds alias too.
            dim_base = dim_cte.source.base_datasource
            if isinstance(dim_base, BuildDatasource) and isinstance(dim_cte, CTE):
                new_join.inline_cte(dim_cte, dim_base)
            branch.inlined_ctes[inlined_entry.new_alias] = inlined_entry
        else:
            branch.parent_ctes = unique(branch.parent_ctes + [dim_cte], "name")
        branch.joins.append(new_join)
        existing_out = {c.address for c in branch.output_columns}
        # FK concepts already resolve via the branch's fact ds. Only the
        # *non-key* dim columns need a source_map entry pointing at the dim;
        # adding FK there too would make render_expr coalesce two sources.
        non_key_dim_addrs = {
            c.address for c in d.dim_concepts if c.address not in d.fk_left_addrs
        }
        # source_map values are *datasource* safe_identifiers, not CTE names.
        # When the dim is inlined the safe_identifier equals new_alias; when
        # the dim has its own CTE the dim CTE's safe_identifier is the same
        # as cte.name, but the QDS-level source_map needs the BuildDatasource.
        dim_source_key = (
            inlined_entry.new_alias
            if inlined_entry is not None
            else d.dim_qds.safe_identifier
        )
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
            if branch.condition is None:
                branch.condition = atom
            elif not self._atom_in(atom, branch.condition):
                branch.condition = BuildConditional(
                    left=branch.condition,
                    operator=BooleanOperator.AND,
                    right=cast(ConditionExpression, atom),
                )
        return True

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
        union: UnionCTE,
    ) -> None:
        # Source-map entries can carry the dim identifier in several forms:
        # the dim CTE name, the dim BD's safe_identifier, the QDS wrapper's
        # safe_identifier (with ``_at_<key>`` suffix). Strip them all.
        dim_aliases = {dim_cte.name, d.dim_qds.safe_identifier}
        for j in consumer.source.joins:
            if (
                isinstance(j, BaseJoin)
                and getattr(j.right_datasource, "base_datasource", None) is d.dim_qds
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
            base = getattr(rd, "base_datasource", None)
            return base is d.dim_qds

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
                if ds.identifier != d.dim_qds.identifier
                and getattr(ds, "base_datasource", None) is not d.dim_qds
            ]
            consumer.parent_ctes = [
                p for p in consumer.parent_ctes if p.name != dim_cte.name
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
                        hasattr(s, "identifier")
                        and (
                            s.identifier == d.dim_qds.identifier
                            or getattr(s, "base_datasource", None) is d.dim_qds
                        )
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
            consumer.condition = self._strip_atom(consumer.condition, atom)

    # ---- atom helpers ----

    def _atom_in(self, atom, condition) -> bool:
        if condition is None:
            return False
        if condition == atom:
            return True
        if (
            isinstance(condition, BuildConditional)
            and condition.operator == BooleanOperator.AND
        ):
            return self._atom_in(atom, condition.left) or self._atom_in(
                atom, condition.right
            )
        return False

    def _strip_atom(self, condition, atom):
        if condition is None or condition == atom:
            return None
        if not (
            isinstance(condition, BuildConditional)
            and condition.operator == BooleanOperator.AND
        ):
            return condition
        left = self._strip_atom(condition.left, atom)
        right = self._strip_atom(condition.right, atom)
        if left is None:
            return right
        if right is None:
            return left
        return BuildConditional(left=left, operator=BooleanOperator.AND, right=right)
