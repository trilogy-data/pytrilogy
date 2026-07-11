import difflib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, ItemsView, Never, ValuesView

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import (
    UndefinedConceptException,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildFunction
from trilogy.core.models.core import DataType


class BuildEnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def values(self) -> ValuesView[BuildConcept]:  # type: ignore
        return super().values()

    def get(self, key: str, default: BuildConcept | None = None) -> BuildConcept | None:  # type: ignore
        try:
            return self.__getitem__(key)
        except UndefinedConceptException:
            return default

    def raise_undefined(
        self, key: str, line_no: int | None = None, file: Path | str | None = None
    ) -> Never:
        # build environment should never check for missing values.
        if line_no is not None:
            message = f"Concept '{key}' not found in environment at line {line_no}."
        else:
            message = f"Concept '{key}' not found in environment."
        raise UndefinedConceptException(message, [])

    def __getitem__(
        self, key: str, line_no: int | None = None, file: Path | None = None
    ) -> BuildConcept:
        try:
            return super(BuildEnvironmentConceptDict, self).__getitem__(key)
        except KeyError:
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1], line_no)
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key, line_no)
        self.raise_undefined(key, line_no, file)

    def _find_similar_concepts(self, concept_name: str):
        def strip_local(input: str):
            if input.startswith(f"{DEFAULT_NAMESPACE}."):
                return input[len(DEFAULT_NAMESPACE) + 1 :]
            return input

        candidates = [strip_local(x) for x in self.keys()]
        # Hide internal names (`_`-prefixed segments) unless the reference itself
        # uses one — mirrors EnvironmentConceptDict._find_similar_concepts.
        if not any(seg.startswith("_") for seg in concept_name.split(".")):
            candidates = [
                c
                for c in candidates
                if not any(seg.startswith("_") for seg in c.split("."))
            ]
        return difflib.get_close_matches(strip_local(concept_name), candidates)

    def items(self) -> ItemsView[str, BuildConcept]:  # type: ignore
        return super().items()


class BuildEnvironmentDatasourceDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)

    def __getitem__(self, key: str) -> BuildDatasource:
        try:
            return super(BuildEnvironmentDatasourceDict, self).__getitem__(key)
        except KeyError:
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key)
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1])
            raise

    def values(self) -> ValuesView[BuildDatasource]:  # type: ignore
        return super().values()

    def items(self) -> ItemsView[str, BuildDatasource]:  # type: ignore
        return super().items()


@dataclass
class BuildEnvironment:
    concepts: BuildEnvironmentConceptDict = field(
        default_factory=BuildEnvironmentConceptDict
    )
    canonical_concepts: BuildEnvironmentConceptDict = field(
        default_factory=BuildEnvironmentConceptDict
    )
    datasources: BuildEnvironmentDatasourceDict = field(
        default_factory=BuildEnvironmentDatasourceDict
    )
    functions: Dict[str, BuildFunction] = field(default_factory=dict)
    data_types: Dict[str, DataType] = field(default_factory=dict)
    namespace: str = DEFAULT_NAMESPACE
    cte_name_map: Dict[str, str] = field(default_factory=dict)
    materialized_concepts: set[str] = field(default_factory=set)
    materialized_canonical_concepts: set[str] = field(default_factory=set)
    non_partial_materialized_canonical_concepts: set[str] = field(default_factory=set)
    alias_origin_lookup: Dict[str, BuildConcept] = field(default_factory=dict)
    # The subset of the graph's declared-subset sources whose key is a *derived*
    # concept (no datasource column binding) and is therefore resolved via the
    # merge mechanism. It survives as a distinct output only on the partial side,
    # so join resolution marks it partial there (a root/rowset partial key
    # collapses away and is handled by the column-partial / rowset machinery
    # instead). Composite of graph facts and author derivations; dissolves once
    # join typing consults per-side origin nodes directly.
    scoped_partial_derived: set[str] = field(default_factory=set)
    # Members of each build-scoped join key equivalence group, keyed by the
    # group's canonical address: exactly the authored relation endpoints (union
    # of the scoped merge map's source->canonical entries), NOT the transitive
    # pseudonym closure — a rowset key's body/parent pseudonyms are not join
    # operands. Consumed via `distinct_scoped_join_group_members`.
    scoped_join_key_groups: Dict[str, set[str]] = field(default_factory=dict)
    # The full concept domain graph for this build: declared edges (global
    # merges + this build's scoped-join overlay) plus structural, binding and
    # FD edges minted from the author model (docs/domain_graph_design.md).
    # The scoped_* registries above are derivable from it; they remain as
    # compat shims until consumers migrate to graph queries.
    domain_graph: DomainGraph = field(default_factory=DomainGraph)
    # Transitive closure of AUTHOR-referenced concept addresses for the select
    # this environment was materialized for (outputs, WHERE/HAVING/ORDER BY,
    # and their lineage) — computed on the author statement, BEFORE scoped-join
    # canonical substitution, and deliberately excluding the scoped-join
    # declarations themselves. None means unknown (conservative consumers
    # treat every member as referenced). Set by `get_query_node`.
    statement_authored_addresses: set[str] | None = None

    def _distinct_scoped_join_groups(self) -> list[tuple[str, list[str]]]:
        """Per scoped-join key group, its canonical plus the members that keep
        their own physical identity (a substituted member resolves to the group
        canonical, not itself), for groups with two or more such members."""
        out: list[tuple[str, list[str]]] = []
        for canonical, members in self.scoped_join_key_groups.items():
            distinct = [
                member
                for member in members
                if (concept := self.concepts.get(member)) is not None
                and concept.address == member
            ]
            if len(distinct) >= 2:
                out.append((canonical, distinct))
        return out

    def distinct_scoped_join_group_members(self) -> set[str]:
        """Addresses of scoped-join key-group members that keep their own
        physical identity, for groups with two or more such members.

        Only these carry an exposure obligation: a root-keyed merge member is
        substituted onto the group canonical (one physical column — nothing to
        expose separately), while rowset and derived-expression keys stay
        distinct columns that each joined side must materialize. A member of
        such a group is never satisfied through a group-mate pseudonym: the
        join between the sides needs each side's own column (TPC-DS q59)."""
        out: set[str] = set()
        for _, distinct in self._distinct_scoped_join_groups():
            out.update(distinct)
        return out

    def distinct_scoped_join_group_mates(self) -> dict[str, set[str]]:
        """Map each distinct-identity group member to its distinct group-mates,
        restricted to COALESCING (INCOMPARABLE — query `full`/`union` join) key
        groups: there both sides keep identity and each must materialize its
        own column. SUBSET (`left`/`subset`) groups are excluded — they resolve
        by substituting the optional side onto the anchor, so satisfying a
        member through its group-mate pseudonym is the mechanism, not a leak.
        EQUAL (global `merge`) groups are likewise excluded: the merged domains
        are declared identical and the canonical key may have no binding of its
        own, so the pseudonym IS how it resolves."""
        coalescing = {
            self.domain_graph.canonical(addr)
            for addr in self.domain_graph.coalescing_relation_members()
        }
        out: dict[str, set[str]] = {}
        for canonical, distinct in self._distinct_scoped_join_groups():
            if canonical not in coalescing:
                continue
            for member in distinct:
                out.setdefault(member, set()).update(m for m in distinct if m != member)
        return out

    def pseudonym_unsatisfiable_group_mates(self) -> dict[str, set[str]]:
        """`distinct_scoped_join_group_mates` widened for STACK VALIDATION:
        additionally, in SUBSET (`left`/`subset`) groups whose distinct
        members are ALL rowset keys, a member the author REFERENCES is never
        satisfied through a group-mate pseudonym. Its values and per-row
        presence exist only in its own scope: counting it as found off a
        mate drops its source from the plan entirely, stranding every
        reference (a presence probe, a projection) as a render sentinel or
        silently swapping in the mate's row population (q35 `subset join`
        between rowsets). Two carve-outs keep the mechanism cases working:
        an UNREFERENCED member stays satisfiable — it enters requests only
        synthetically (an aggregate grain canonicalized onto the group
        root), where the declaration is pure domain metadata and satisfying
        it via the mate is the ruled collapse-to-referenced-side semantics —
        and a group with a non-rowset member is exempt entirely: a bound
        (ROOT) member resolves by binding substitution, and a
        derived-expression key relates the sides BY materializing the key
        and pairing it with the anchor over the pseudonym, so blocking would
        break that machinery. Validation-only: the rowset enrichment
        machinery consumes the narrower map, where a subset mate is a
        satisfiable request, not an exposure obligation."""
        out = self.distinct_scoped_join_group_mates()
        subset_anchored = {
            self.domain_graph.canonical(addr)
            for addr in self.domain_graph.subset_sources()
        }
        authored = self.statement_authored_addresses
        for canonical, distinct in self._distinct_scoped_join_groups():
            if canonical not in subset_anchored:
                continue
            members = [self.concepts.get(m) for m in distinct]
            if any(c is None or c.derivation != Derivation.ROWSET for c in members):
                continue
            unsatisfiable = {m for m in distinct if authored is None or m in authored}
            for member in distinct:
                if unsatisfiable - {member}:
                    out.setdefault(member, set()).update(unsatisfiable - {member})
        return out

    def gen_concept_list_caches(self) -> None:
        concrete_concepts: list[BuildConcept] = []
        non_partial_concrete_concepts: list[BuildConcept] = []
        for datasource in self.datasources.values():
            for column in datasource.columns:
                if column.is_complete:
                    non_partial_concrete_concepts.append(column.concept)
                concrete_concepts.append(column.concept)
        concrete_addresses = set([x.address for x in concrete_concepts])
        canonical_addresses = set([x.canonical_address for x in concrete_concepts])
        non_partial_canonical_addresses = set(
            [x.canonical_address for x in non_partial_concrete_concepts]
        )
        self.materialized_concepts = set()
        self.materialized_canonical_concepts = set()
        self.non_partial_materialized_canonical_concepts = set()

        for c in self.concepts.values():
            if c.address in concrete_addresses:
                self.materialized_concepts.add(c.address)
            if c.canonical_address in canonical_addresses:
                self.materialized_canonical_concepts.add(c.canonical_address)
            if c.canonical_address in non_partial_canonical_addresses:
                self.non_partial_materialized_canonical_concepts.add(
                    c.canonical_address
                )
        for c in self.alias_origin_lookup.values():
            if c.address in concrete_addresses:
                self.materialized_concepts.add(c.address)
            if c.canonical_address in canonical_addresses:
                self.materialized_canonical_concepts.add(c.canonical_address)
            if c.canonical_address in non_partial_canonical_addresses:
                self.non_partial_materialized_canonical_concepts.add(
                    c.canonical_address
                )
