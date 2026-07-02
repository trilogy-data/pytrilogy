import difflib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, ItemsView, Never, ValuesView

from trilogy.constants import DEFAULT_NAMESPACE
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

        matches = difflib.get_close_matches(
            strip_local(concept_name), [strip_local(x) for x in self.keys()]
        )
        return matches

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
    # Source addresses of LEFT (partial) build-scoped joins — the rowset node
    # uses this to mark the advertised target join key partial (drives
    # LEFT-OUTER).
    scoped_partial_sources: set[str] = field(default_factory=set)
    # The subset of scoped_partial_sources whose key is a *derived* concept (no
    # datasource column binding) and is therefore resolved via the merge
    # mechanism. It survives as a distinct output only on the partial side, so
    # join resolution marks it partial there (a root/rowset partial key collapses
    # away and is handled by the column-partial / rowset machinery instead).
    scoped_partial_derived: set[str] = field(default_factory=set)
    # Registry of canonical keys of build-scoped FULL joins. The key is complete
    # (both sides bind it; the FULL JOIN coalesces it). join resolution consults
    # this to emit a FULL JOIN for the key, instead of inferring it from a
    # (falsely-)partial binding — so the gate and rowset enrichment are unaffected.
    scoped_full_join_keys: set[str] = field(default_factory=set)
    # Canonical keys of query-scoped LEFT joins (the preserved-anchor side). Join
    # resolution anchors the join tree on the complete source providing these so
    # multiple optional sources stay directional LEFT_OUTER instead of collapsing
    # to a symmetric FULL (TPC-DS q78). Excludes environment `merge ~` joins.
    scoped_left_anchor_keys: set[str] = field(default_factory=set)

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
