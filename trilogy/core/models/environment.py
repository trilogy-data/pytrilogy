from __future__ import annotations

import copy
import difflib
import os
from collections import UserDict, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    ItemsView,
    Iterator,
    List,
    Mapping,
    Never,
    Optional,
    Self,
    Tuple,
    ValuesView,
)

from pydantic import TypeAdapter as _TypeAdapter

from trilogy.constants import DEFAULT_NAMESPACE, ENV_CACHE_NAME, logger
from trilogy.core.constants import (
    INTERNAL_NAMESPACE,
    WORKING_PATH_CONCEPT,
)
from trilogy.core.enums import (
    ConceptSource,
    Derivation,
    FunctionType,
    Granularity,
    JoinType,
    Modifier,
    Purpose,
)
from trilogy.core.exceptions import (
    FrozenEnvironmentException,
    InvalidSyntaxException,
    UndefinedConceptException,
)
from trilogy.core.models.author import (
    Concept,
    ConceptRef,
    CustomFunctionFactory,
    CustomType,
    Function,
    SelectLineage,
    UndefinedConcept,
    UndefinedConceptFull,
    address_with_namespace,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Datasource, EnvironmentDatasourceDict
from trilogy.utility import safe_open

if TYPE_CHECKING:
    from trilogy.core.models.build import BuildConcept, BuildEnvironment
    from trilogy.parsing.helpers import Meta


@dataclass
class Import:
    alias: str
    path: Path
    input_path: Path | None = (
        None  # filepath where the text came from (path is the import path, but may be resolved from a dictionary for some resolvers)
    )
    # explicit concept filter: only these names are public when imported
    concepts: list[str] | None = None
    # same-line trailing comment on the `import ... as ...;` statement, e.g.
    # `import customer_demographic as customer_demographic; # demographics at POS`.
    # Surfaced under the namespace header in `trilogy explore` to disambiguate
    # otherwise-identical-looking imports (sale-time vs customer-current, etc.).
    description: str | None = None


@dataclass
class BaseImportResolver:
    pass


@dataclass
class FileSystemImportResolver(BaseImportResolver):
    pass


@dataclass
class DictImportResolver(BaseImportResolver):
    content: Dict[str, str] = field(default_factory=dict)
    # Virtual data files (csv, parquet, ...) keyed by the path as written in
    # trilogy source or the resolved absolute form. A datasource that points at
    # a key in this map is treated as published even when there is no real file
    # on disk — useful for server / sandboxed environments.
    data_files: Dict[str, bytes] = field(default_factory=dict)

    def has_data_file(self, *paths: str) -> bool:
        return any(p in self.data_files for p in paths)


@dataclass
class EnvironmentConfig:
    allow_duplicate_declaration: bool = True
    import_resolver: BaseImportResolver = field(
        default_factory=FileSystemImportResolver
    )

    def copy_for_root(self, root: str | None) -> "EnvironmentConfig":
        new = copy.deepcopy(self)
        if isinstance(new.import_resolver, DictImportResolver) and root:
            new.import_resolver = DictImportResolver(
                content={
                    k[len(root) + 1 :]: v
                    for k, v in new.import_resolver.content.items()
                    if k.startswith(f"{root}.")
                },
                data_files=new.import_resolver.data_files,
            )
        return new


def _is_subsequence(needle: list[str], haystack: list[str]) -> bool:
    """True if every element of `needle` appears in `haystack` in order (gaps ok)."""
    it = iter(haystack)
    return all(seg in it for seg in needle)


def _subsequence_gaps(needle: list[str], haystack: list[str]) -> int:
    """Haystack segments skipped between the matched `needle` segments (greedy
    earliest match); 0 == a contiguous run. Assumes `needle` is a subsequence of
    `haystack`. Used to rank a partial-path match's tightness."""
    positions: list[int] = []
    i = 0
    for seg in needle:
        while haystack[i] != seg:
            i += 1
        positions.append(i)
        i += 1
    return positions[-1] - positions[0] - (len(positions) - 1)


class EnvironmentConceptDict(UserDict[str, Concept]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.undefined: dict[str, UndefinedConceptFull] = {}
        self.fail_on_missing: bool = True
        self.hidden: set[str] = set()
        # Leading namespaces of rowset outputs; bounds leaf-shorthand resolution
        # (`rs.col` -> `rs.a.b.col`) to rowset namespaces. Populated by add_rowset.
        self.rowset_namespaces: set[str] = set()
        # Addresses of explicit rowset alias/transform outputs (`... as yr` ->
        # `rs.yr`), as opposed to passed-through source refs (`rs.src.yr`). A
        # leaf-shorthand that IS such a direct output must resolve to itself, not
        # expand into its deeper source path. Populated at COLLECT_SYMBOLS.
        self.rowset_alias_outputs: set[str] = set()
        # Rowset output addresses (`rs.src.col`) that leak ONLY from a scoped-join
        # condition (e.g. the other side of `cur.dow = nxt.dow`) and are never a
        # projected SELECT output. Tracked so leaf-shorthand resolution can drop
        # them as phantom candidates even before the genuine output commits (q02
        # self-relation referenced from inside a `def` body). Populated at
        # COLLECT_SYMBOLS.
        self.rowset_join_key_leaks: set[str] = set()
        self._resolving: set[str] = set()
        self._overlay_stack: list[Mapping[str, Concept]] = []
        self.populate_default_concepts()

    def duplicate(self) -> "EnvironmentConceptDict":
        new = EnvironmentConceptDict()
        # include hidden items via raw iteration
        new.update({k: v.duplicate() for k, v in self.data.items()})
        new.undefined = self.undefined
        new.fail_on_missing = self.fail_on_missing
        new.hidden = set(self.hidden)
        new.rowset_namespaces = set(self.rowset_namespaces)
        new.rowset_alias_outputs = set(self.rowset_alias_outputs)
        new.rowset_join_key_leaks = set(self.rowset_join_key_leaks)
        return new

    def populate_default_concepts(self):
        from trilogy.core.internal import DEFAULT_CONCEPTS

        for concept in DEFAULT_CONCEPTS.values():
            self[concept.address] = concept

    @contextmanager
    def push_overlay(
        self, overlay: Mapping[str, Concept]
    ) -> Iterator[Mapping[str, Concept]]:
        """Install a read-only concept overlay for the duration of the scope.

        While active, reads through ``__getitem__``/``get``/``__contains__``
        consult the overlay before ``self.data``. Mutable dicts are wrapped
        in ``MappingProxyType`` so this API cannot be used as a write path;
        ``self.data`` is never mutated by overlay installation or teardown.
        The wrapper is a *live* view of the caller's dict, so concepts added
        to the underlying dict during the scope become visible immediately.
        """
        if isinstance(overlay, dict):
            view: Mapping[str, Concept] = MappingProxyType(overlay)
        else:
            view = overlay
        self._overlay_stack.append(view)
        try:
            yield view
        finally:
            popped = self._overlay_stack.pop()
            assert popped is view, "overlay stack corrupted"

    @contextmanager
    def without_overlays(self) -> Iterator[None]:
        """Temporarily detach every installed overlay.

        Commit-time write paths (``semantic_state.commit`` running
        ``add_concept``/``merge_concept``) must consult durable
        ``self.data`` rather than the pending overlay view; otherwise
        ``merge_concept``'s equality shortcut reads a staged alias and
        skips rewiring a stale ``alias_origin_lookup`` entry.
        """
        saved, self._overlay_stack = self._overlay_stack, []
        try:
            yield
        finally:
            self._overlay_stack = saved

    def _overlay_lookup(self, key: str) -> Concept | None:
        if not self._overlay_stack:
            return None
        for overlay in reversed(self._overlay_stack):
            hit = overlay.get(key)
            if hit is not None:
                return hit
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                hit = overlay.get(key.split(".", 1)[1])
                if hit is not None:
                    return hit
            elif "." not in key:
                hit = overlay.get(f"{DEFAULT_NAMESPACE}.{key}")
                if hit is not None:
                    return hit
        return None

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str) and self._overlay_lookup(key) is not None:
            return True
        if key in self.data and key not in self.hidden:
            return True
        if isinstance(key, str):
            if DEFAULT_NAMESPACE + "." + key in self.data:
                return True
        return False

    def __iter__(self):
        return (k for k in self.data if k not in self.hidden)

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def keys(self):  # type: ignore
        return [k for k in self.data if k not in self.hidden]

    def values(self) -> ValuesView[Concept]:  # type: ignore
        return [v for k, v in self.data.items() if k not in self.hidden]  # type: ignore

    def items(self) -> ItemsView[str, Concept]:  # type: ignore
        return [(k, v) for k, v in self.data.items() if k not in self.hidden]  # type: ignore

    def all_items(self) -> list[tuple[str, Concept]]:
        """Iterate all concepts including hidden ones (for build-time resolution)."""
        return list(self.data.items())

    def get(self, key: str, default: Concept | None = None) -> Concept | None:  # type: ignore[override]
        try:
            return self.__getitem__(key)
        except UndefinedConceptException:
            return default

    def raise_undefined(
        self, key: str, line_no: int | None = None, file: Path | str | None = None
    ) -> Never:

        matches = self._find_similar_concepts(key)
        message = f"Undefined concept: {key}."
        if matches:
            message += f" Suggestions: {matches}"

        if line_no:
            if file:
                raise UndefinedConceptException(
                    f"{file}: {line_no}: " + message, matches
                )
            raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
        raise UndefinedConceptException(message, matches)

    def __getitem__(
        self, key: str, line_no: int | None = None, file: Path | None = None
    ) -> Concept | UndefinedConceptFull:
        if self._overlay_stack:
            overlay_hit = self._overlay_lookup(key)
            if overlay_hit is not None:
                return overlay_hit
        # fast access path — includes hidden (needed for build resolution)
        if key in self.data:
            return self.data[key]
        if isinstance(key, ConceptRef):
            return self.__getitem__(key.address, line_no=line_no, file=file)  # type: ignore[call-arg]
        try:
            return self.data[key]
        except KeyError:
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1], line_no)
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key, line_no)
            # lazy resolution of derived concepts (e.g. signup_date.year)
            derived = self._try_resolve_derived(key)
            if derived is not None:
                return derived
            # leaf-shorthand on a rowset output (`rs.col` -> `rs.a.b.col`)
            shorthand = self._try_resolve_namespace_suffix(key)
            if shorthand is not None:
                return shorthand
            if not self.fail_on_missing:
                if "." in key:
                    ns, rest = key.rsplit(".", 1)
                else:
                    ns = DEFAULT_NAMESPACE
                    rest = key
                if key in self.undefined:
                    return self.undefined[key]
                undefined = UndefinedConceptFull(
                    line_no=line_no,
                    datatype=DataType.UNKNOWN,
                    name=rest,
                    purpose=Purpose.UNKNOWN,
                    namespace=ns,
                )
                self.undefined[key] = undefined
                return undefined
        self.raise_undefined(key, line_no, file)

    def _try_resolve_namespace_suffix(self, key: str) -> Concept | None:
        """Resolve `rs.col` to a rowset output `rs.<...>.col` when exactly one
        output under namespace `rs` matches the shorthand as an ordered dotted
        subsequence. Two matches -> ambiguity error; zero -> caller's undefined.
        Scoped to rowset namespaces so it never collapses import paths."""
        q_segs = key.split(".")
        if len(q_segs) < 2 or q_segs[0] not in self.rowset_namespaces:
            return None
        prefix = q_segs[0] + "."
        candidates = [
            k
            for k in self.data
            if k != key
            and k.startswith(prefix)
            and _is_subsequence(q_segs, k.split("."))
        ]
        if len(candidates) == 1:
            return self.data[candidates[0]]
        if len(candidates) > 1:
            raise UndefinedConceptException(
                f"Ambiguous reference {key!r}: matches {sorted(candidates)}. "
                "Qualify the full path to disambiguate.",
                sorted(candidates),
            )
        return None

    def _try_resolve_derived(self, key: str) -> Concept | None:
        """Lazily resolve a derived concept like 'signup_date.year' by checking
        if the suffix matches a single-arg function valid for the parent's datatype."""

        if key in self._resolving:
            return None
        if "." not in key:
            return None
        self._resolving.add(key)
        try:
            return self._resolve_derived_inner(key)
        finally:
            self._resolving.discard(key)

    def _resolve_derived_inner(self, key: str) -> Concept | None:
        from trilogy.core.functions import try_create_auto_derived

        parent_addr, suffix = key.rsplit(".", 1)

        parent = self.data.get(parent_addr)
        if parent is None and DEFAULT_NAMESPACE + "." + parent_addr in self.data:
            parent = self.data[DEFAULT_NAMESPACE + "." + parent_addr]
        if parent is None:
            return None

        derived = try_create_auto_derived(
            parent, suffix, environment=Environment(concepts=self)
        )
        if derived is None:
            return None
        self[derived.address] = derived
        return derived

    def _find_similar_concepts(
        self, concept_name: str, extra_keys: list[str] | None = None
    ):
        def strip_local(input: str):
            if input.startswith(f"{DEFAULT_NAMESPACE}."):
                return input[len(DEFAULT_NAMESPACE) + 1 :]
            return input

        # Candidate set = committed concepts plus `extra_keys` — concepts STAGED
        # during the current parse (e.g. a rowset output referenced before the
        # parse commits) which aren't in `self.keys()` yet, so the suggestion
        # can still point at them.
        keys = list(self.keys())
        if extra_keys:
            keys += [k for k in extra_keys if k not in keys]
        # Never suggest the very address being looked up (a staged placeholder for
        # it may be present in the candidate set).
        keys = [k for k in keys if k != concept_name]

        # Partial-path match: a reference that drops an intermediate namespace
        # segment (e.g. `y1999.item_id` for the real `y1999.agg.item_id`, where the
        # rowset column kept its source namespace) shares the looked-up segments as
        # an ordered subsequence of the candidate's. Gated to >=2 segments so a bare
        # leaf doesn't match deep inside an unrelated path; ranked first because a
        # shared namespace prefix is a strong relevance signal.
        q_segs = strip_local(concept_name).split(".")
        path_candidates = (
            [
                strip_local(k)
                for k in keys
                if k != concept_name
                and _is_subsequence(q_segs, strip_local(k).split("."))
            ]
            if len(q_segs) >= 2
            else []
        )
        # Rank by closeness before the cap: fewest extra segments beyond the query,
        # then the tightest (fewest-gap) match. So a bare-alias reference
        # (`ns.alias` -> `ns.alias.id`, extra 1, contiguous) outranks a deep
        # near-miss (`ns.other.alias.deep.id`, extra 3) even when the deep key was
        # inserted first. Without the sort, dict-insertion order lets deep matches
        # consume the 6-cap and bury the obvious shallow child.
        path_matches = sorted(
            path_candidates,
            key=lambda m: (
                len(m.split(".")) - len(q_segs),
                _subsequence_gaps(q_segs, m.split(".")),
            ),
        )

        # Leaf-name match: a bare reference like `first_name` (e.g. in ORDER BY,
        # where the full path is required) has no fuzzy match against the long
        # full-path keys, so difflib returns nothing. Surface every concept whose
        # path ends in `.<leaf>` so the user sees the real path(s) to use.
        leaf = concept_name.rsplit(".", 1)[-1]
        leaf_matches = [
            strip_local(k)
            for k in keys
            if k != concept_name and k.rsplit(".", 1)[-1] == leaf
        ]

        stripped_q = strip_local(concept_name)
        stripped_keys = [strip_local(x) for x in keys]
        fuzzy = difflib.get_close_matches(stripped_q, stripped_keys)

        # Same-namespace fuzzy: when the reference is namespaced (`cs.x`), a fuzzy
        # match sharing that leading segment (`cs.bill_customer.id` for the typo
        # `cs.billing_customer.id`) is a far stronger signal than the generic
        # same-leaf flood or an identical name in a *different* namespace
        # (`ws.billing_customer.id`). Without this, a common leaf like `id` fills
        # every slot with unrelated `*.id` concepts and buries the near-miss.
        ns = stripped_q.split(".", 1)[0] if "." in stripped_q else None
        same_ns_fuzzy = (
            difflib.get_close_matches(
                stripped_q, [k for k in stripped_keys if k.split(".", 1)[0] == ns]
            )
            if ns
            else []
        )

        # Prefer partial-path, then same-namespace near-miss, then general fuzzy,
        # then the same-leaf flood — de-duplicated, capped.
        out: list[str] = []
        for m in path_matches + same_ns_fuzzy + fuzzy + leaf_matches:
            if m not in out:
                out.append(m)
        return out[:6]


_concept_ta: _TypeAdapter | None = None
_custom_type_ta: _TypeAdapter | None = None


def _concept_adapter():
    global _concept_ta
    if _concept_ta is None:
        from trilogy.core.models.author import Concept

        _concept_ta = _TypeAdapter(Concept)
    return _concept_ta


def _custom_type_adapter():
    global _custom_type_ta
    if _custom_type_ta is None:
        from trilogy.core.models.author import CustomType

        _custom_type_ta = _TypeAdapter(CustomType)
    return _custom_type_ta


def validate_concepts(v) -> EnvironmentConceptDict:
    if isinstance(v, EnvironmentConceptDict):
        return v
    elif isinstance(v, dict):
        return EnvironmentConceptDict(
            **{x: _concept_adapter().validate_python(y) for x, y in v.items()}
        )
    raise ValueError


def get_version():
    from trilogy import __version__

    return __version__


@dataclass
class Environment:
    concepts: EnvironmentConceptDict = field(default_factory=EnvironmentConceptDict)
    datasources: EnvironmentDatasourceDict = field(
        default_factory=EnvironmentDatasourceDict
    )
    functions: Dict[str, CustomFunctionFactory] = field(default_factory=dict)
    data_types: Dict[str, CustomType] = field(default_factory=dict)
    named_statements: Dict[str, SelectLineage] = field(default_factory=dict)
    imports: defaultdict[str, list[Import]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # namespace (import alias, including nested dotted forms like
    # `billing_customer.first_sales_date`) -> the source file it was parsed
    # from. `imports` only retains TOP-LEVEL imports because `add_import`
    # flattens a sub-environment's concepts up without its import records;
    # this map preserves the full lineage so consumers (e.g. `explore`'s
    # conformed-dimension dedup) can tell that two role-played namespaces came
    # from the same file. Kept separate from `imports` so renderer logic that
    # keys on `concept.namespace in imports` is unaffected.
    namespace_source: Dict[str, Path] = field(default_factory=dict)
    namespace: str = DEFAULT_NAMESPACE
    working_path: str | Path = field(default_factory=os.getcwd)
    config: EnvironmentConfig = field(default_factory=EnvironmentConfig)
    version: str = field(default_factory=get_version)
    cte_name_map: Dict[str, str] = field(default_factory=dict)
    alias_origin_lookup: Dict[str, Concept] = field(default_factory=dict)
    # Global `merge` statements as build-time join pairs. These are evaluated
    # alongside query-scoped joins by Factory.scoped_merge_map instead of
    # rewriting the author environment during parse.
    merges: list[tuple[str, str, JoinType]] = field(default_factory=list)
    # TODO: support freezing environments to avoid mutation
    frozen: bool = False
    env_file_path: Path | str | None = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    def freeze(self):
        self.frozen = True

    def thaw(self):
        self.frozen = False

    def set_parameters(self, **kwargs) -> Self:

        self.parameters.update(kwargs)
        return self

    def materialize_for_select(
        self,
        local_concepts: dict[str, "BuildConcept"] | None = None,
        build_cache: dict | None = None,
        pseudonym_map: dict[str, set[str]] | None = None,
        grain_build_cache: dict | None = None,
        canonical_build_cache: dict | None = None,
        datasource_build_cache: dict | None = None,
        scoped_joins: list[tuple[str, str, JoinType]] | None = None,
    ) -> "BuildEnvironment":
        """helper method"""
        from trilogy.core.models.build import Factory

        build_scoped_joins = list(scoped_joins or [])
        build_scoped_joins.extend(
            merge for merge in self.merges if merge not in build_scoped_joins
        )
        factory: Factory = Factory(
            self,
            local_concepts=local_concepts,
            build_cache=build_cache,
            pseudonym_map=pseudonym_map,
            grain_build_cache=grain_build_cache,
            canonical_build_cache=canonical_build_cache,
            datasource_build_cache=datasource_build_cache,
            scoped_joins=build_scoped_joins,
        )
        return factory.build(self)

    def add_rowset(self, name: str, lineage: SelectLineage):
        self.named_statements[name] = lineage
        self.concepts.rowset_namespaces.add(name)

    @staticmethod
    def merge_to_join(
        source: Concept,
        target: Concept,
        modifiers: list[Modifier],
    ) -> tuple[str, str, JoinType] | None:
        if source.address == target.address:
            return None
        if Modifier.PARTIAL in modifiers:
            return (target.address, source.address, JoinType.LEFT_OUTER)
        # A non-partial `merge` asserts the two keys are one identity present on
        # BOTH sides -> a FULL join over the coalesced canonical key (the language
        # has no INNER; a filtering condition downstream may still let the optimizer
        # narrow the emitted SQL join to INNER).
        return (source.address, target.address, JoinType.FULL)

    def add_merge_join(
        self,
        source: Concept,
        target: Concept,
        modifiers: list[Modifier],
    ) -> bool:
        if self.frozen:
            raise ValueError("Environment is frozen, cannot merge concepts")
        pair = self.merge_to_join(source, target, modifiers)
        if pair is None or pair in self.merges:
            return False
        self._lint_merge_declaration(pair, source, target)
        self.merges.append(pair)
        return True

    def _lint_merge_declaration(
        self,
        pair: tuple[str, str, JoinType],
        source: Concept,
        target: Concept,
    ) -> None:
        """Author-time contradiction lint: check the new declared domain edge
        against prior merge declarations plus the two endpoints' own
        structural derivation edges. Deliberately shallow — the full graph is
        a build-time artifact; this only needs the facts already in hand."""
        from trilogy.core.domain_graph import (
            DomainGraph,
            EdgeScope,
            declared_edge_from_join,
            structural_domain_edge,
        )

        edge = declared_edge_from_join(*pair, scope=EdgeScope.GLOBAL)
        if edge is None:
            return
        graph = DomainGraph.from_scoped_joins(
            [(merge, EdgeScope.GLOBAL) for merge in self.merges]
        )
        for concept in (source, target):
            structural = structural_domain_edge(concept)
            if structural is not None:
                graph.add_edge(structural)
        reason = graph.contradicts(edge)
        if reason:
            raise InvalidSyntaxException(f"Invalid merge declaration: {reason}")

    def duplicate(self):
        return Environment(
            datasources=self.datasources.duplicate(),
            concepts=self.concepts.duplicate(),
            functions=dict(self.functions),
            data_types=dict(self.data_types),
            imports=defaultdict(list, self.imports),
            namespace_source=dict(self.namespace_source),
            namespace=self.namespace,
            working_path=self.working_path,
            config=copy.deepcopy(self.config),
            version=self.version,
            cte_name_map=dict(self.cte_name_map),
            alias_origin_lookup={
                k: v.duplicate() for k, v in self.alias_origin_lookup.items()
            },
            merges=list(self.merges),
            env_file_path=self.env_file_path,
        )

    def _add_path_concepts(self):
        concept = Concept(
            name=WORKING_PATH_CONCEPT,
            namespace=self.namespace,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                arguments=[str(self.working_path)],
                output_datatype=DataType.STRING,
                output_purpose=Purpose.CONSTANT,
            ),
            datatype=DataType.STRING,
            granularity=Granularity.SINGLE_ROW,
            derivation=Derivation.CONSTANT,
            purpose=Purpose.CONSTANT,
        )
        self.add_concept(concept)

    def __post_init__(self) -> None:
        self._add_path_concepts()

    @classmethod
    def from_file(cls, path: str | Path) -> "Environment":
        if isinstance(path, str):
            path = Path(path)
        with safe_open(path) as f:
            read = f.read()
        return Environment(working_path=path.parent, env_file_path=path).parse(read)[0]

    @classmethod
    def from_string(
        cls, input: str, config: EnvironmentConfig | None = None
    ) -> "Environment":
        config = config or EnvironmentConfig()
        return Environment(config=config).parse(input)[0]

    @classmethod
    def from_cache(cls, path) -> Optional["Environment"]:
        import json

        data = json.loads(Path(path).read_text())
        if data.get("version") != get_version():
            return None
        concepts = EnvironmentConceptDict()
        for k, v in data.get("concepts", {}).items():
            concepts[k] = _concept_adapter().validate_python(v)
        datasources = EnvironmentDatasourceDict()
        for k, v in data.get("datasources", {}).items():
            datasources[k] = Datasource.model_validate(v)
        return cls(
            concepts=concepts,
            datasources=datasources,
            functions={
                k: CustomFunctionFactory.from_dict(v)
                for k, v in data.get("functions", {}).items()
            },
            data_types={
                k: _custom_type_adapter().validate_python(v)
                for k, v in data.get("data_types", {}).items()
            },
            alias_origin_lookup={
                k: _concept_adapter().validate_python(v)
                for k, v in data.get("alias_origin_lookup", {}).items()
            },
            merges=[
                (source, target, JoinType(join_type))
                for source, target, join_type in data.get("merges", [])
            ],
            namespace=data.get("namespace", DEFAULT_NAMESPACE),
            version=data["version"],
            cte_name_map=data.get("cte_name_map", {}),
            env_file_path=data.get("env_file_path"),
        )

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "namespace": self.namespace,
            "cte_name_map": self.cte_name_map,
            "env_file_path": str(self.env_file_path) if self.env_file_path else None,
            "concepts": {
                k: _concept_adapter().dump_python(v, mode="json")
                for k, v in self.concepts.items()
            },
            "datasources": {
                k: v.model_dump(mode="json") for k, v in self.datasources.items()
            },
            "functions": {k: v.to_dict() for k, v in self.functions.items()},
            "data_types": {
                k: _custom_type_adapter().dump_python(v, mode="json")
                for k, v in self.data_types.items()
            },
            "alias_origin_lookup": {
                k: _concept_adapter().dump_python(v, mode="json")
                for k, v in self.alias_origin_lookup.items()
            },
            "merges": [
                (source, target, join_type.value)
                for source, target, join_type in self.merges
            ],
        }

    def to_cache(self, path: Optional[str | Path] = None) -> Path:
        import json

        if not path:
            ppath = Path(self.working_path) / ENV_CACHE_NAME
        else:
            ppath = Path(path)
        ppath.write_text(json.dumps(self.to_dict()))
        return ppath

    def validate_concept(
        self, new_concept: Concept, meta: Meta | None = None
    ) -> Concept | None:
        lookup = new_concept.address
        if lookup not in self.concepts:
            return None
        existing: Concept = self.concepts[lookup]
        if isinstance(existing, UndefinedConcept):
            return None

        def handle_currently_bound_sources():
            if str(existing.lineage) == str(new_concept.lineage):
                return None

            invalidated = False
            for k, datasource in self.datasources.items():
                if existing.address in datasource.output_concepts:
                    logger.warning(
                        f"Removed concept for {existing} assignment from {k}"
                    )
                    clen = len(datasource.columns)
                    datasource.columns = [
                        x
                        for x in datasource.columns
                        if x.concept.address != existing.address
                    ]
                    assert len(datasource.columns) < clen
                    invalidated = len(datasource.columns) < clen
            if invalidated:
                logger.warning(
                    f"Persisted concept {existing.address} lineage {str(existing.lineage)} did not match redeclaration {str(new_concept.lineage)}, invalidated current bound datasource."
                )
            return None

        if existing and self.config.allow_duplicate_declaration:
            if (
                existing.metadata
                and existing.metadata.concept_source == ConceptSource.AUTO_DERIVED
            ):
                # auto derived concepts will not have sources nad do not need to be checked
                return None
            return handle_currently_bound_sources()
        elif (
            existing.metadata
            and existing.metadata.concept_source == ConceptSource.AUTO_DERIVED
        ):
            return None
        elif meta and existing.metadata:
            raise ValueError(
                f"Assignment to concept '{lookup}' on line {meta.line} is a duplicate"
                f" declaration; '{lookup}' was originally defined on line"
                f" {existing.metadata.line_number}"
            )
        elif existing.metadata:
            raise ValueError(
                f"Assignment to concept '{lookup}'  is a duplicate declaration;"
                f" '{lookup}' was originally defined on line"
                f" {existing.metadata.line_number}"
            )
        raise ValueError(
            f"Assignment to concept '{lookup}'  is a duplicate declaration;"
        )

    def add_import(
        self,
        alias: str,
        source: Environment,
        imp_stm: Import | None = None,
        concepts: list[str] | None = None,
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        exists = False
        existing = self.imports[alias]
        if imp_stm:
            if any(
                [x.path == imp_stm.path and x.alias == imp_stm.alias for x in existing]
            ):
                exists = True
            if concepts is None:
                concepts = imp_stm.concepts
        else:
            # A Python-assembled import has no source-level module path; the
            # alias *is* its logical import path. The child's working dir is
            # filesystem provenance, so it rides on ``input_path`` (which
            # ``namespace_source`` already prefers) rather than masquerading as
            # the import path — keeps the value re-parseable and host-portable.
            working_path = Path(source.working_path)
            if any(
                [x.input_path == working_path and x.alias == alias for x in existing]
            ):
                exists = True
            imp_stm = Import(alias=alias, path=Path(alias), input_path=working_path)
        same_namespace = alias == DEFAULT_NAMESPACE

        if not exists:
            self.imports[alias].append(imp_stm)
        # Record namespace -> source file lineage. The direct alias maps to
        # this import's source file; nested aliases (the sub-environment's own
        # imports) are re-prefixed under it so a deep role like
        # `billing_customer.first_sales_date` resolves to `raw/date.preql`.
        # Skip the default-namespace case (the file's own/std `import` lines):
        # those concepts are the importer's own, not a role-played dimension.
        if not same_namespace:
            origin = imp_stm.input_path or imp_stm.path
            if origin is not None:
                self.namespace_source[alias] = Path(origin)
            # list() to tolerate self-import (source is self → we just mutated
            # the dict we'd be iterating).
            for sub_ns, sub_path in list(source.namespace_source.items()):
                self.namespace_source[address_with_namespace(sub_ns, alias)] = sub_path
        # we can't exit early
        # as there may be new concepts
        iteration: list[tuple[str, Concept]] = list(source.concepts.all_items())
        for k, concept in iteration:
            if INTERNAL_NAMESPACE in concept.namespace:
                continue
            # don't overwrite working path
            if concept.name == WORKING_PATH_CONCEPT:
                continue
            namespaced = concept if same_namespace else concept.with_namespace(alias)
            target_k = k if same_namespace else address_with_namespace(k, alias)
            is_hidden = k in source.concepts.hidden
            if (
                concepts is not None
                and k not in concepts
                and concept.name not in concepts
            ):
                # excluded from public view — store in concepts but mark hidden
                new = self.add_concept(namespaced)
                self.concepts.data[target_k] = new
                self.concepts.hidden.add(target_k)
            elif is_hidden:
                # propagate hidden status from source
                new = self.add_concept(namespaced)
                self.concepts.data[target_k] = new
                self.concepts.hidden.add(target_k)
            else:
                new = self.add_concept(namespaced)
                self.concepts[target_k] = new

        # Copy to list to avoid mutation issues during self-import
        for _, datasource in list(source.datasources.items()):
            if same_namespace:
                self.add_datasource(datasource)
            else:
                self.add_datasource(datasource.with_namespace(alias))
        for key, val in list(source.alias_origin_lookup.items()):

            if same_namespace:
                self.alias_origin_lookup[key] = val
            else:
                self.alias_origin_lookup[address_with_namespace(key, alias)] = (
                    val.with_namespace(alias)
                )
        for s_addr, t_addr, jt in source.merges:
            pair = (
                (s_addr, t_addr, jt)
                if same_namespace
                else (
                    address_with_namespace(s_addr, alias),
                    address_with_namespace(t_addr, alias),
                    jt,
                )
            )
            if pair not in self.merges:
                self.merges.append(pair)

        for key, function in list(source.functions.items()):
            if same_namespace:
                self.functions[key] = function
            else:
                self.functions[address_with_namespace(key, alias)] = (
                    function.with_namespace(alias)
                )
        for key, type in list(source.data_types.items()):
            if same_namespace:
                self.data_types[key] = type
            else:
                self.data_types[address_with_namespace(key, alias)] = (
                    type.with_namespace(alias)
                )
        return self

    def add_file_import(
        self, path: str | Path, alias: str, env: "Environment" | None = None
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        from trilogy.parser import parse_text

        if isinstance(path, str):
            if path.endswith(".preql"):
                path = path.rsplit(".", 1)[0]
            if "." not in path:
                target = Path(self.working_path, path)
            else:
                target = Path(self.working_path, *path.split("."))
            target = target.with_suffix(".preql")
        else:
            target = path
        if not env:
            try:
                with safe_open(target) as f:
                    text = f.read()
                nenv = Environment(working_path=target.parent)
                nenv.concepts.fail_on_missing = False
                nenv, _ = parse_text(text, environment=nenv, root=target.parent)
            except Exception as e:
                raise ImportError(
                    f"Unable to import file {target.parent}, parsing error: {e}"
                )
            env = nenv
        imps = Import(alias=alias, path=target)
        self.add_import(alias, source=env, imp_stm=imps)
        return imps

    def parse(
        self, input: str, namespace: str | None = None, persist: bool = False
    ) -> Tuple["Environment", list]:
        from trilogy import parse
        from trilogy.core.query_processor import process_persist
        from trilogy.core.statements.author import (
            MultiSelectStatement,
            PersistStatement,
            SelectStatement,
            ShowStatement,
        )

        if namespace:
            new = Environment()
            _, queries = new.parse(input)
            self.add_import(namespace, new)
            return self, queries
        _, queries = parse(input, self)
        generatable = [
            x
            for x in queries
            if isinstance(
                x,
                (
                    SelectStatement,
                    PersistStatement,
                    MultiSelectStatement,
                    ShowStatement,
                ),
            )
        ]
        while generatable:
            t = generatable.pop(0)
            if isinstance(t, PersistStatement) and persist:
                processed = process_persist(self, t)
                self.add_datasource(processed.datasource)
        return self, queries

    def add_concept(
        self,
        concept: Concept,
        meta: Meta | None = None,
        force: bool = False,
    ):

        if self.frozen:
            raise FrozenEnvironmentException(
                "Environment is frozen, cannot add concepts"
            )
        if not force:
            existing = self.validate_concept(concept, meta=meta)
            if existing:
                concept = existing

        self.concepts[concept.address] = concept
        # `--`-prefixed declarations stay queryable but are omitted from public
        # listings (explore/agent metadata); route into the existing hidden set.
        if concept.metadata and concept.metadata.hidden:
            self.concepts.hidden.add(concept.address)

        from trilogy.core.environment_helpers import generate_related_concepts

        generate_related_concepts(concept, self, meta=meta)

        return concept

    def remove_concept(
        self,
        concept: Concept | str,
    ) -> bool:
        if self.frozen:
            raise FrozenEnvironmentException(
                "Environment is frozen, cannot remove concepts"
            )
        if isinstance(concept, Concept):
            address = concept.address
            c_instance = concept
        else:
            address = concept
            c_instance_check = self.concepts.get(address)
            if not c_instance_check:
                return False
            c_instance = c_instance_check
        from trilogy.core.environment_helpers import remove_related_concepts

        remove_related_concepts(c_instance, self)
        if address in self.concepts:
            del self.concepts[address]
            return True
        if address in self.alias_origin_lookup:
            del self.alias_origin_lookup[address]

        return False

    def add_datasource(
        self,
        datasource: Datasource,
        meta: Meta | None = None,
    ):
        if self.frozen:
            raise FrozenEnvironmentException(
                "Environment is frozen, cannot add datasource"
            )
        if datasource.is_root and (
            datasource.freshness_by or datasource.incremental_by
        ):
            raise SyntaxError(
                f"Root datasource '{datasource.identifier}' should not declare freshness or incremental by."
            )
        self.datasources[datasource.identifier] = datasource
        return datasource

    def delete_datasource(
        self,
        address: str,
        meta: Meta | None = None,
    ) -> bool:
        if self.frozen:
            raise ValueError("Environment is frozen, cannot delete datsources")
        if address in self.datasources:
            del self.datasources[address]
            # self.gen_concept_list_caches()
            return True
        return False

    # LSP/Editor introspection helpers

    def user_concepts(self) -> List[Concept]:
        """Return all user-defined concepts, filtering out internal concepts."""
        return [
            c
            for c in self.concepts.values()
            if not c.namespace.startswith(INTERNAL_NAMESPACE)
            and not c.name.startswith("_")
        ]

    def concepts_at_line(self, line_number: int) -> List[Concept]:
        """Find all concepts defined on a specific line number."""
        return [
            c
            for c in self.concepts.values()
            if c.metadata and c.metadata.line_number == line_number
        ]


@dataclass
class LazyEnvironment(Environment):
    """Variant of environment to defer parsing of a path
    until relevant attributes accessed."""

    load_path: Path | None = None
    setup_queries: list = field(default_factory=list)
    loaded: bool = False

    def __post_init__(self) -> None:
        if self.load_path is None:
            raise ValueError("load_path is required")
        self.working_path = self.load_path.parent
        # skip _add_path_concepts (overridden as no-op below)
        super().__post_init__()

    @property
    def setup_path(self) -> Path:
        assert self.load_path is not None
        return self.load_path.parent / "setup.preql"

    def _add_path_concepts(self):
        pass

    def _load(self):
        if self.loaded:
            return
        from trilogy import parse

        assert self.load_path is not None
        env = Environment(working_path=self.load_path.parent)
        with safe_open(self.load_path) as f:
            env, _ = parse(f.read(), env)
        if self.setup_path.exists():
            with safe_open(self.setup_path) as f2:
                env, q = parse(f2.read(), env)
                for q in q:
                    self.setup_queries.append(q)
        self.loaded = True
        self.datasources = env.datasources
        self.concepts = env.concepts
        self.imports = env.imports
        self.namespace_source = env.namespace_source
        self.alias_origin_lookup = env.alias_origin_lookup
        self.functions = env.functions
        self.data_types = env.data_types
        self.cte_name_map = env.cte_name_map

    def __getattr__(self, name):
        return self.__getattribute__(name)

    def __getattribute__(self, name):
        if name not in (
            "datasources",
            "concepts",
            "imports",
            "functions",
            "datatypes",
            "cte_name_map",
        ) or name.startswith("_"):
            return super().__getattribute__(name)
        if not self.loaded:
            self._load()
        return super().__getattribute__(name)
