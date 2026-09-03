from trilogy.core.enums import SetOperator
from trilogy.core.models.build import (
    BuildConcept,
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import render_cte_used_map


class HideUnusedConcepts(OptimizationRule):
    """Rule instances are phase-local, so the used-map cache below lives for
    exactly one phase and survives its fixpoint loops. The only mutations
    during the phase are this rule's own ``hidden_concepts`` writes, each of
    which evicts the mutated object and any cached union whose render
    included it."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # id -> (the CTE itself, its map). The entry holds the object so the
        # id cannot be recycled under the cache, and the identity re-check on
        # read makes a stale hit impossible rather than merely unlikely.
        self._used_maps: dict[int, tuple[CTE | UnionCTE, dict[str, set[str]]]] = {}
        # branch id -> cached-union ids whose render included that branch; a
        # union's used map depends on its branches' renders, so mutating a
        # branch must evict the union too (transitively for nested unions).
        self._unions_of: dict[int, set[int]] = {}

    def _register_members(self, union_key: int, cte: UnionCTE) -> None:
        for branch in cte.internal_ctes:
            self._unions_of.setdefault(id(branch), set()).add(union_key)
            if isinstance(branch, UnionCTE):
                self._register_members(union_key, branch)

    def _used_map(self, cte: CTE | UnionCTE) -> dict[str, set[str]]:
        key = id(cte)
        entry = self._used_maps.get(key)
        if entry is not None and entry[0] is cte:
            return entry[1]
        used = render_cte_used_map(cte)
        self._used_maps[key] = (cte, used)
        if isinstance(cte, UnionCTE):
            self._register_members(key, cte)
        return used

    def _evict(self, cte: CTE | UnionCTE) -> None:
        key = id(cte)
        self._used_maps.pop(key, None)
        for union_key in self._unions_of.get(key, ()):
            self._used_maps.pop(union_key, None)

    def _hide_branch_only_outputs(self, cte: UnionCTE) -> bool:
        """Hide concepts a branch projects but the union does not expose: no
        consumer can reach them through the union. ``UnionDimPushdown``
        filter-only mode creates these, adding a dim's concepts to each branch
        so the WHERE atom renders while consumers keep their own dim join.
        """
        union_addrs = {c.address for c in cte.output_columns}
        changed = False
        for branch in cte.internal_ctes:
            if not isinstance(branch, CTE):
                continue
            to_hide = {
                c.address
                for c in branch.output_columns
                if c.address not in union_addrs
                and c.address not in branch.hidden_concepts
            }
            if not to_hide:
                continue
            visible = [
                c.address
                for c in branch.output_columns
                if c.address not in branch.hidden_concepts
            ]
            if len(visible) - len(to_hide) < 1:
                # Always leave at least one projected column so the branch
                # SELECT renders.
                continue
            branch.hidden_concepts |= to_hide
            self._evict(branch)
            self.log(
                f"Hiding branch-only outputs {sorted(to_hide)} from {branch.name} "
                f"(union {cte.name} doesn't expose them)"
            )
            changed = True
        return changed

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        children = inverse_map.get(cte.name, [])
        if not children:
            return False, None
        if isinstance(cte, UnionCTE) and cte.operator != SetOperator.UNION_ALL.value:
            # EXCEPT/INTERSECT compare the entire projected row, so every
            # declared output column is row identity and demand-driven pruning
            # would change results. Branch-only extras still hide: arms must
            # project exactly the declared outputs.
            return self._hide_branch_only_outputs(cte), None
        used: set[str] = set()
        for v in children:
            self.debug(f"Analyzing usage of {cte.name} in {v.name}")
            child_used_map = self._used_map(v)
            used.update(child_used_map.get(cte.name, set()))
        # A child's used-map records the canonical address it renders, which may
        # be a pseudonym of the column this CTE physically carries; mark that
        # physical column used too or the child reads a hidden column.
        for concept in cte.output_columns:
            if concept.address not in used and concept.pseudonyms & used:
                used.add(concept.address)
        self.debug(f"Used concepts for {cte.name}: {used}")
        add_to_hidden: list[BuildConcept] = []
        for concept in cte.output_columns:
            if concept.address not in used:
                add_to_hidden.append(concept)
        newly_hidden = [
            x.address for x in add_to_hidden if x.address not in cte.hidden_concepts
        ]
        non_hidden = [
            x for x in cte.output_columns if x.address not in cte.hidden_concepts
        ]
        branch_only_hidden = False
        if isinstance(cte, UnionCTE):
            branch_only_hidden = self._hide_branch_only_outputs(cte)
        if not newly_hidden or len(non_hidden) <= 1:
            return branch_only_hidden, None
        candidates = [x.address for x in cte.output_columns if x.address not in used]
        visible_addresses = {
            x.address
            for x in cte.output_columns
            if x.address not in cte.hidden_concepts
        }
        if visible_addresses.issubset(set(candidates)):
            # Keep one projected address so an anchor CTE cannot render as SELECT FROM.
            keep_address = next(
                x.address
                for x in reversed(cte.output_columns)
                if x.address in visible_addresses
            )
            candidates = [x for x in candidates if x != keep_address]
        if not candidates:
            return branch_only_hidden, None
        new_hidden = set(candidates)
        changed = new_hidden != cte.hidden_concepts
        if changed:
            self.log(
                f"Hiding unused concepts {candidates} from {cte.name} "
                f"(used: {used}, all: {[x.address for x in cte.output_columns]})"
            )
            cte.hidden_concepts = new_hidden
            self._evict(cte)
        # Each union branch's SELECT list is filtered by that branch's own
        # hidden_concepts, so propagate the hide. Re-checked every loop: a
        # branch may only gain the column on a later pass.
        if isinstance(cte, UnionCTE):
            for branch in cte.internal_ctes:
                if not isinstance(branch, CTE):
                    continue
                branch_outputs = {c.address for c in branch.output_columns}
                to_hide = {
                    addr for addr in candidates if addr in branch_outputs
                } - branch.hidden_concepts
                if to_hide:
                    branch.hidden_concepts |= to_hide
                    self._evict(branch)
                    changed = True
        # Report True only on a real change; a no-op True keeps the driver
        # re-running this phase until MAX_OPTIMIZATION_LOOPS.
        return changed or branch_only_hidden, None
