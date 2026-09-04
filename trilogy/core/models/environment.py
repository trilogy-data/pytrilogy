from __future__ import annotations

import copy
import difflib
import os
from collections import UserDict, defaultdict
from collections.abc import ItemsView, Iterator, Mapping, ValuesView
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Never,
    Self,
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
from trilogy.core.models.core import DataType, StructType
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
    # Count of leading "." tokens in the source (`..store_sales` -> 2). `path`
    # holds only the dotted module name, so rendering the import back without
    # this resolves it against the wrong directory.
    leading_dots: int = 0


@dataclass
class ImportedSymbols:
    """What ``import`` statements contributed, as opposed to what this file declared.

    A bare ``import x;`` merges into the default namespace, so its symbols are
    otherwise indistinguishable from the importer's own — ``namespace_source``
    only separates *aliased* imports. Rendering an environment back to source
    needs the distinction: an imported symbol is represented by its import line,
    and re-declaring it alongside that line is a duplicate declaration.
    """

    concepts: set[str] = field(default_factory=set)
    datasources: set[str] = field(default_factory=set)
    functions: set[str] = field(default_factory=set)
    data_types: set[str] = field(default_factory=set)
    merges: set[tuple[str, str, JoinType]] = field(default_factory=set)

    def duplicate(self) -> ImportedSymbols:
        return ImportedSymbols(
            concepts=set(self.concepts),
            datasources=set(self.datasources),
            functions=set(self.functions),
            data_types=set(self.data_types),
            merges=set(self.merges),
        )


@dataclass
class NamespaceProjection:
    """Everything `import <source> as <alias>` contributes to an importer.

    A pure function of (source environment, alias): `with_namespace(alias)`
    rewrites the addresses *inside* every concept and datasource, so aliased
    imports cannot share the source's objects the way bare imports do. Copying
    that per import edge is the parse floor — over the TPC corpus, 143 aliased
    edges resolve to 17 distinct (source env, alias) pairs. Callers holding a
    validated-unchanged source env therefore cache this and hand it to
    ``Environment.add_import``; see ``parsing/v2/import_service.py``.

    Concept entries are ``(source_key, source_name, target_key, namespaced,
    hidden)`` — the source key and name are kept because an import's explicit
    concept filter is written against the un-namespaced names.
    """

    concepts: list[tuple[str, str, str, Concept, bool]]
    datasources: list[Datasource]
    alias_origins: list[tuple[str, Concept]]
    merges: list[tuple[str, str, JoinType]]
    functions: list[tuple[str, CustomFunctionFactory]]
    data_types: list[tuple[str, CustomType]]
    namespace_sources: list[tuple[str, Path]]
    # Precomputed form of the per-concept merge loop, for the case where every
    # target key is absent from the importer: exactly the writes
    # ``_merge_imported_concept`` would make, as one dict to ``update`` with and
    # one set to union into ``hidden``. See ``Environment.add_import``.
    bulk_concepts: dict[str, Concept]
    bulk_hidden: set[str]
    # False when the per-concept loop is not reducible to those two writes:
    # a struct concept (``generate_related_concepts`` derives further concepts,
    # order-sensitively) or a key written twice with different objects (the
    # loop's durable-signature dedup decides which survives, a dict does not).
    bulk_safe: bool

    def integrity(self) -> tuple:
        """Cheap stamp over the mutable surface a previous importer could have
        written through. Shared datasources are edited in place by warehouse
        metadata sync, persist status flips, `env` address prefixing, and
        bound-source invalidation; a cached projection whose stamp moved is no
        longer the pure product of the source env and must be rebuilt."""
        return tuple(
            (d.identifier, d.status.value, len(d.columns), str(d.address))
            for d in self.datasources
        )


def build_namespace_projection(source: Environment, alias: str) -> NamespaceProjection:
    concepts: list[tuple[str, str, str, Concept, bool]] = []
    bulk_concepts: dict[str, Concept] = {}
    bulk_hidden: set[str] = set()
    bulk_safe = True
    hidden = source.concepts.hidden
    for k, concept in source.concepts.all_items():
        if INTERNAL_NAMESPACE in concept.namespace:
            continue
        # don't overwrite working path
        if concept.name == WORKING_PATH_CONCEPT:
            continue
        target_k = address_with_namespace(k, alias)
        namespaced = concept.with_namespace(alias)
        is_hidden = k in hidden
        concepts.append((k, concept.name, target_k, namespaced, is_hidden))
        if isinstance(namespaced.datatype, StructType):
            bulk_safe = False
        for target in (namespaced.address, target_k):
            prior = bulk_concepts.get(target)
            if prior is not None and prior is not namespaced:
                bulk_safe = False
            bulk_concepts[target] = namespaced
        if is_hidden:
            bulk_hidden.add(target_k)
        if namespaced.metadata and namespaced.metadata.hidden:
            bulk_hidden.add(namespaced.address)
    return NamespaceProjection(
        concepts=concepts,
        bulk_concepts=bulk_concepts,
        bulk_hidden=bulk_hidden,
        bulk_safe=bulk_safe,
        # list() on the source dicts tolerates self-import: `source is self`
        # means these iterate a dict the merge is about to write.
        datasources=[
            d.with_namespace(alias) for _, d in list(source.datasources.items())
        ],
        alias_origins=[
            (address_with_namespace(k, alias), v.with_namespace(alias))
            for k, v in list(source.alias_origin_lookup.items())
        ],
        merges=[
            (
                address_with_namespace(s_addr, alias),
                address_with_namespace(t_addr, alias),
                jt,
            )
            for s_addr, t_addr, jt in list(source.merges)
        ],
        functions=[
            (address_with_namespace(k, alias), f.with_namespace(alias))
            for k, f in list(source.functions.items())
        ],
        data_types=[
            (address_with_namespace(k, alias), t.with_namespace(alias))
            for k, t in list(source.data_types.items())
        ],
        namespace_sources=[
            (address_with_namespace(ns, alias), path)
            for ns, path in list(source.namespace_source.items())
        ],
    )


@dataclass
class BaseImportResolver:
    pass


@dataclass
class FileSystemImportResolver(BaseImportResolver):
    pass


@dataclass
class DictImportResolver(BaseImportResolver):
    content: dict[str, str] = field(default_factory=dict)
    # Virtual data files (csv, parquet, ...) keyed by the path as written in
    # trilogy source or the resolved absolute form. A datasource that points at
    # a key in this map is treated as published even when there is no real file
    # on disk — useful for server / sandboxed environments.
    data_files: dict[str, bytes] = field(default_factory=dict)
    # Dotted directory of the file whose imports this resolver serves: "" at
    # the top level, "nest." inside `nest.child`. An import resolves under it
    # first and then as an absolute address, the way a filesystem import tries
    # the importing file's directory before `import_paths`.
    prefix: str = ""

    def resolve(self, address: str) -> str | None:
        """Canonical `content` key for an import address, or None."""
        relative = self.prefix + address
        if relative in self.content:
            return relative
        if address in self.content:
            return address
        return None

    def has_data_file(self, *paths: str) -> bool:
        return any(p in self.data_files for p in paths)


@dataclass
class EnvironmentConfig:
    allow_duplicate_declaration: bool = True
    import_resolver: BaseImportResolver = field(
        default_factory=FileSystemImportResolver
    )

    def copy_for_root(self, root: str | None) -> EnvironmentConfig:
        """Config for parsing an imported file; `root` is the file's canonical
        dotted directory (or None at the top level)."""
        new = copy.deepcopy(self)
        if isinstance(new.import_resolver, DictImportResolver):
            new.import_resolver.prefix = f"{root}." if root else ""
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
        # Write counter for content-addressed caches over the AUTHOR
        # environment (e.g. domain_graph's minted-edge cache): unlike a
        # BuildEnvironment this dict mutates between statements, so identity
        # alone cannot prove freshness. Set before super().__init__, which may
        # already route initial data through __setitem__.
        self.mutations: int = 0
        # Effective-content counter: bumps only when a key's VALUE OBJECT
        # actually changes. Unlike `mutations` it ignores overlay push/pop and
        # identical-object rewrites, so caches that only read the durable dict
        # outside any overlay scope (the cross-statement BuildCaches store) can
        # survive a re-parse of identical statements.
        self.content_version: int = 0
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

    def duplicate(self) -> EnvironmentConceptDict:
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
        # Overlays redirect reads without touching self.data, so they count as
        # writes for `mutations`-stamped caches (see the field's comment).
        self.mutations += 1
        self._overlay_stack.append(view)
        try:
            yield view
        finally:
            popped = self._overlay_stack.pop()
            self.mutations += 1
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
        self.mutations += 1
        saved, self._overlay_stack = self._overlay_stack, []
        try:
            yield
        finally:
            self._overlay_stack = saved
            self.mutations += 1

    def __setitem__(self, key: str, item: Concept) -> None:
        self.mutations += 1
        if self.data.get(key) is not item:
            self.content_version += 1
        super().__setitem__(key, item)

    def __delitem__(self, key: str) -> None:
        self.mutations += 1
        self.content_version += 1
        super().__delitem__(key)

    @property
    def has_overlays(self) -> bool:
        return bool(self._overlay_stack)

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
        return bool(isinstance(key, str) and DEFAULT_NAMESPACE + "." + key in self.data)

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
            # `suggest=False`: this miss is answered with `default`, so the
            # difflib pass behind the exception's "Suggestions:" text is built
            # and thrown away. It is O(concepts) per miss and every call on a
            # successful parse lands here.
            return self.__getitem__(key, suggest=False)  # type: ignore[call-arg]
        except UndefinedConceptException:
            return default

    def raise_undefined(
        self,
        key: str,
        line_no: int | None = None,
        file: Path | str | None = None,
        suggest: bool = True,
    ) -> Never:

        matches = self._find_similar_concepts(key) if suggest else []
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
        self,
        key: str,
        line_no: int | None = None,
        file: Path | None = None,
        suggest: bool = True,
    ) -> Concept | UndefinedConceptFull:
        if self._overlay_stack:
            overlay_hit = self._overlay_lookup(key)
            if overlay_hit is not None:
                return overlay_hit
        # fast access path — includes hidden (needed for build resolution)
        if key in self.data:
            return self.data[key]
        if isinstance(key, ConceptRef):
            return self.__getitem__(key.address, line_no=line_no, file=file, suggest=suggest)  # type: ignore[call-arg]
        try:
            return self.data[key]
        except KeyError:
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1], line_no, suggest=suggest)  # type: ignore[call-arg]
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(  # type: ignore[call-arg]
                    DEFAULT_NAMESPACE + "." + key, line_no, suggest=suggest
                )
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
        self.raise_undefined(key, line_no, file, suggest=suggest)

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
        # Ambiguity of a rowset leaf shorthand is judged against every candidate,
        # including the hidden ones filtered out of the suggestion pool below —
        # a shorthand two outputs can claim resolves to neither.
        resolvable = {strip_local(k) for k in keys}
        # Never suggest the very address being looked up (a staged placeholder for
        # it may be present in the candidate set).
        keys = [k for k in keys if k != concept_name]
        # Hide internal names — any path segment starting with `_` (mangled
        # per-rowset aliases like `_rs_alias`, model-private helpers) — unless
        # the user's own reference uses one, in which case they're fair game.
        if not any(seg.startswith("_") for seg in concept_name.split(".")):
            keys = [
                k for k in keys if not any(seg.startswith("_") for seg in k.split("."))
            ]

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
        # An exact leaf match means the user knows the NAME and missed the path —
        # a stronger signal than character-level fuzz (`warehouse_count` must
        # surface `all_orders.warehouse_count` ahead of `warehouse.county`). Rank
        # leaf matches by whole-string similarity so, within the flood of a
        # common leaf like `id`, the candidate closest to the full reference
        # (e.g. the same name in a sibling namespace) still leads.
        stripped_q = strip_local(concept_name)
        leaf_matches.sort(
            key=lambda m: difflib.SequenceMatcher(None, stripped_q, m).ratio(),
            reverse=True,
        )
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

        # Prefer partial-path, then same-namespace near-miss, then exact-leaf
        # matches, then general fuzzy — de-duplicated, capped. A rowset output's
        # leaf shorthand rides immediately behind its own full path, so ranking
        # is unchanged and the shorter spelling is never the missing one.
        out: list[str] = []
        for m in path_matches + same_ns_fuzzy + leaf_matches + fuzzy:
            if m in out:
                continue
            out.append(m)
            shorthand = self._rowset_leaf_shorthand(m, resolvable)
            if shorthand is not None and shorthand not in out:
                out.append(shorthand)
        return out[:6]

    def _rowset_leaf_shorthand(self, address: str, keys: set[str]) -> str | None:
        """`rs.a.b.col` -> `rs.col`, the spelling `_try_resolve_namespace_suffix`
        accepts and the docs teach. None when the address is not a deep rowset
        output, or when a sibling output shares the leaf so the shorthand would
        resolve ambiguously (or not to this address at all)."""
        segs = address.split(".")
        if len(segs) < 3 or segs[0] not in self.rowset_namespaces:
            return None
        shorthand = [segs[0], segs[-1]]
        prefix = segs[0] + "."
        matches = {
            k
            for k in keys
            if k.startswith(prefix) and _is_subsequence(shorthand, k.split("."))
        }
        return ".".join(shorthand) if matches == {address} else None


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


def concept_structural_signature(concept: Concept) -> tuple:
    """Declared identity of a concept: every author-layer field a build or
    downstream cache can observe. Deliberately NOT ``Concept.__eq__``, which
    is a weak name/type/grain comparison that ignores lineage, keys and
    pseudonyms (see BuildConcept's matching contract)."""
    meta = concept.metadata
    return (
        type(concept).__name__,
        concept.name,
        concept.namespace,
        str(concept.datatype),
        concept.purpose,
        concept.derivation,
        concept.granularity,
        str(concept.lineage),
        str(concept.grain),
        frozenset(concept.keys or ()),
        tuple(concept.modifiers),
        frozenset(concept.pseudonyms),
        (
            (meta.line_number, meta.concept_source, meta.description, meta.hidden)
            if meta
            else None
        ),
    )


def datasource_structural_signature(datasource: Datasource) -> tuple:
    """Declared identity of a datasource, excluding runtime state: ``status``
    flips in place at publish/persist time and must survive an identical
    redeclaration rather than reset it."""
    return (
        datasource.identifier,
        str(datasource.address),
        str(datasource.grain),
        tuple(
            (str(c.alias), c.concept.address, tuple(c.modifiers))
            for c in datasource.columns
        ),
        str(datasource.where) if datasource.where else None,
        str(datasource.non_partial_for) if datasource.non_partial_for else None,
        tuple(c.address for c in datasource.incremental_by),
        tuple(c.address for c in datasource.partition_by),
        tuple(c.address for c in datasource.freshness_by),
        datasource.freshness_probe,
        datasource.refresh_script,
        str(datasource.allowed_lag) if datasource.allowed_lag else None,
        datasource.is_root,
        datasource.is_partial,
        frozenset(datasource.column_level_partial_addresses),
    )


@dataclass
class Environment:
    concepts: EnvironmentConceptDict = field(default_factory=EnvironmentConceptDict)
    datasources: EnvironmentDatasourceDict = field(
        default_factory=EnvironmentDatasourceDict
    )
    functions: dict[str, CustomFunctionFactory] = field(default_factory=dict)
    data_types: dict[str, CustomType] = field(default_factory=dict)
    named_statements: dict[str, SelectLineage] = field(default_factory=dict)
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
    namespace_source: dict[str, Path] = field(default_factory=dict)
    # Symbols merged in by `add_import` rather than declared locally. See
    # ImportedSymbols; consumed by the environment renderer.
    imported: ImportedSymbols = field(default_factory=ImportedSymbols)
    namespace: str = DEFAULT_NAMESPACE
    working_path: str | Path = field(default_factory=os.getcwd)
    # Fallback roots for import resolution: an import that does not resolve
    # under working_path is tried against each of these in order. Set from
    # trilogy.toml `import_paths` so a script can import a model that lives
    # outside its own directory.
    import_paths: list[Path] = field(default_factory=list)
    config: EnvironmentConfig = field(default_factory=EnvironmentConfig)
    version: str = field(default_factory=get_version)
    cte_name_map: dict[str, str] = field(default_factory=dict)
    alias_origin_lookup: dict[str, Concept] = field(default_factory=dict)
    # Global `merge` statements as build-time join pairs. These are evaluated
    # alongside query-scoped joins by Factory.scoped_merge_map instead of
    # rewriting the author environment during parse.
    merges: list[tuple[str, str, JoinType]] = field(default_factory=list)
    # TODO: support freezing environments to avoid mutation
    frozen: bool = False
    env_file_path: Path | str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    # (content stamp, map) for fk_derived_keys.
    _fk_derived_keys: tuple[tuple[int, int], dict[str, frozenset[str]]] | None = None

    def freeze(self):
        self.frozen = True

    def thaw(self):
        self.frozen = False

    def set_parameters(self, **kwargs) -> Self:

        self.parameters.update(kwargs)
        return self

    def fk_derived_keys(self) -> dict[str, frozenset[str]]:
        """Keys a datasource declaration implies for the KEY concepts it binds.

        A datasource that binds a ``Purpose.KEY`` concept OUTSIDE its own grain
        is asserting that its grain determines that key — the classic foreign
        key on a fact table. The assertion belongs to the datasource, so it is
        derived here on demand rather than written back onto the concept.
        Two datasources can bind the same key at different grains; the later
        declaration wins, matching the parse-time overwrite this replaced.
        """
        stamp = (self.datasources.content_version, self.concepts.content_version)
        if self._fk_derived_keys is not None and self._fk_derived_keys[0] == stamp:
            return self._fk_derived_keys[1]
        out: dict[str, frozenset[str]] = {}
        for datasource in self.datasources.values():
            grain = datasource.grain
            if not grain or not grain.components:
                continue
            resolved = [self.concepts.get(g) for g in grain.components]
            if any(k is None for k in resolved):
                continue
            components = [k for k in resolved if k is not None]
            new_keys = frozenset(k.address for k in components)
            for column in datasource.columns:
                address = column.concept.address
                if address in grain.components:
                    continue
                target = self.concepts.get(address)
                if target is None or target.purpose != Purpose.KEY:
                    continue
                # A grain component that already binds this key describes the
                # opposite direction; inheriting would invert the relationship.
                if any(address in (k.keys or set()) for k in components):
                    continue
                out[address] = new_keys
        self._fk_derived_keys = (stamp, out)
        return out

    def materialize_for_select(
        self,
        local_concepts: dict[str, BuildConcept] | None = None,
        build_cache: dict | None = None,
        pseudonym_map: dict[str, set[str]] | None = None,
        grain_build_cache: dict | None = None,
        canonical_build_cache: dict | None = None,
        datasource_build_cache: dict | None = None,
        scoped_joins: list[tuple[str, str, JoinType]] | None = None,
    ) -> BuildEnvironment:
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

    def materialize_join_key(
        self, scoped_joins: list[tuple[str, str, JoinType]] | None
    ) -> tuple[tuple[str, str, JoinType], ...]:
        """The scoped-join tuple a materialization actually builds under (env
        merges folded in, as `_materialize_factory` does) — the cache key for
        `EnvBaseline` reuse across the statement and its nested arms."""
        folded = list(scoped_joins or [])
        folded.extend(merge for merge in self.merges if merge not in folded)
        return tuple(folded)

    def _materialize_factory(
        self,
        local_concepts: dict | None,
        build_cache: dict | None,
        pseudonym_map: dict[str, set[str]] | None,
        grain_build_cache: dict | None,
        canonical_build_cache: dict | None,
        datasource_build_cache: dict | None,
        scoped_joins: list[tuple[str, str, JoinType]] | None,
    ):
        """The exact factory `materialize_for_select` builds (env merges folded
        into the scoped joins) — one construction for the full, baseline and
        delta materializations so the three cannot diverge."""
        from trilogy.core.models.build import Factory

        build_scoped_joins = list(scoped_joins or [])
        build_scoped_joins.extend(
            merge for merge in self.merges if merge not in build_scoped_joins
        )
        return Factory(
            self,
            local_concepts=local_concepts,
            build_cache=build_cache,
            pseudonym_map=pseudonym_map,
            grain_build_cache=grain_build_cache,
            canonical_build_cache=canonical_build_cache,
            datasource_build_cache=datasource_build_cache,
            scoped_joins=build_scoped_joins,
        )

    def materialize_baseline(
        self,
        build_cache: dict | None = None,
        pseudonym_map: dict[str, set[str]] | None = None,
        grain_build_cache: dict | None = None,
        canonical_build_cache: dict | None = None,
        datasource_build_cache: dict | None = None,
        scoped_joins: list[tuple[str, str, JoinType]] | None = None,
    ):
        """Materialize with NO select overlay, recording per-unit footprints:
        the reusable half of every nested-select materialization under this
        scoped-join set. Pair with `materialize_delta`."""
        factory = self._materialize_factory(
            {},
            build_cache,
            pseudonym_map,
            grain_build_cache,
            canonical_build_cache,
            datasource_build_cache,
            scoped_joins,
        )
        return factory.build_environment_recorded(self)

    def materialize_delta(
        self,
        baseline,
        local_concepts: dict,
        build_cache: dict | None = None,
        pseudonym_map: dict[str, set[str]] | None = None,
        grain_build_cache: dict | None = None,
        canonical_build_cache: dict | None = None,
        datasource_build_cache: dict | None = None,
        scoped_joins: list[tuple[str, str, JoinType]] | None = None,
    ) -> BuildEnvironment:
        """`materialize_for_select`, computed as baseline + overlay delta.
        Byte-equivalent to the full build (see `build_environment_delta` for
        the soundness argument); the full spelling remains the reference."""
        factory = self._materialize_factory(
            local_concepts,
            build_cache,
            pseudonym_map,
            grain_build_cache,
            canonical_build_cache,
            datasource_build_cache,
            scoped_joins,
        )
        return factory.build_environment_delta(self, baseline)

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
            imported=self.imported.duplicate(),
            namespace=self.namespace,
            working_path=self.working_path,
            import_paths=list(self.import_paths),
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
    def from_file(cls, path: str | Path) -> Environment:
        if isinstance(path, str):
            path = Path(path)
        with safe_open(path) as f:
            read = f.read()
        return Environment(working_path=path.parent, env_file_path=path).parse(read)[0]

    @classmethod
    def from_string(
        cls, input: str, config: EnvironmentConfig | None = None
    ) -> Environment:
        config = config or EnvironmentConfig()
        return Environment(config=config).parse(input)[0]

    @classmethod
    def from_cache(cls, path) -> Environment | None:
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

    def to_cache(self, path: str | Path | None = None) -> Path:
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
                return

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
                    f"Persisted concept {existing.address} lineage {existing.lineage!s} did not match redeclaration {new_concept.lineage!s}, invalidated current bound datasource."
                )
            return

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
        projection: NamespaceProjection | None = None,
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        exists = False
        existing = self.imports[alias]
        if imp_stm:
            if any(
                x.path == imp_stm.path and x.alias == imp_stm.alias for x in existing
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
            if any(x.input_path == working_path and x.alias == alias for x in existing):
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
        # The aliased merge reads entirely off a NamespaceProjection — the
        # `with_namespace(alias)` product of the source env. Callers that can
        # prove the source env is unchanged (the import store) pass a cached
        # one; everyone else builds it here, which is what the loop used to do
        # inline. The bare merge shares source objects outright and needs none,
        # so a projection handed in alongside DEFAULT_NAMESPACE is discarded
        # rather than allowed to namespace concepts that must stay bare.
        if same_namespace:
            projection = None
        elif projection is None:
            projection = build_namespace_projection(source, alias)

        if projection is not None:
            origin = imp_stm.input_path or imp_stm.path
            if origin is not None:
                self.namespace_source[alias] = Path(origin)
            for sub_ns, sub_path in projection.namespace_sources:
                self.namespace_source[sub_ns] = sub_path
        # we can't exit early
        # as there may be new concepts
        if projection is None:
            for k, concept in list(source.concepts.all_items()):
                if INTERNAL_NAMESPACE in concept.namespace:
                    continue
                # don't overwrite working path
                if concept.name == WORKING_PATH_CONCEPT:
                    continue
                self._merge_imported_concept(
                    k, concept.name, k, concept, k in source.concepts.hidden, concepts
                )
        elif not self._bulk_merge_projected_concepts(projection, concepts):
            for k, name, target_k, namespaced, is_hidden in projection.concepts:
                self._merge_imported_concept(
                    k, name, target_k, namespaced, is_hidden, concepts
                )

        # Copy to list to avoid mutation issues during self-import
        if projection is None:
            for _, datasource in list(source.datasources.items()):
                self.add_datasource(datasource)
                self.imported.datasources.add(datasource.identifier)
            for key, val in list(source.alias_origin_lookup.items()):
                self.alias_origin_lookup[key] = val
            for pair in list(source.merges):
                self.imported.merges.add(pair)
                if pair not in self.merges:
                    self.merges.append(pair)
            for key, function in list(source.functions.items()):
                self.functions[key] = function
                self.imported.functions.add(key)
            for key, type in list(source.data_types.items()):
                self.data_types[key] = type
                self.imported.data_types.add(key)
            return self

        for datasource in projection.datasources:
            self.add_datasource(datasource)
            self.imported.datasources.add(datasource.identifier)
        for key, val in projection.alias_origins:
            self.alias_origin_lookup[key] = val
        for pair in projection.merges:
            self.imported.merges.add(pair)
            if pair not in self.merges:
                self.merges.append(pair)
        for key, function in projection.functions:
            self.functions[key] = function
            self.imported.functions.add(key)
        for key, type in projection.data_types:
            self.data_types[key] = type
            self.imported.data_types.add(key)
        return self

    def _bulk_merge_projected_concepts(
        self, projection: NamespaceProjection, concepts: list[str] | None
    ) -> bool:
        """Insert a whole projection with two writes, or decline.

        The per-concept loop is a pure insert for essentially every imported
        concept — `validate_concept` returns None the moment the address is
        absent, and `add_concept`'s durable-signature dedup only has an opinion
        when something is already there. So the collision semantics do not have
        to be re-derived here, only *detected*: if no target key is already in
        the importer, the loop reduces to `data.update` plus a `hidden` union.
        Anything else falls back to the loop, which stays the definition of
        correct behavior.

        Declines when an explicit `concepts` filter is set (the filter is
        per-edge and cannot ride on the shared projection), when an overlay is
        installed (it changes what `validate_concept` considers present), or
        when the projection itself is not reducible (`bulk_safe`).
        """
        if concepts is not None or not projection.bulk_safe:
            return False
        payload = projection.bulk_concepts
        if not payload:
            return True
        data = self.concepts.data
        if self.concepts.has_overlays or not data.keys().isdisjoint(payload.keys()):
            return False
        data.update(payload)
        self.imported.concepts.update(payload.keys())
        self.concepts.hidden |= projection.bulk_hidden
        # One bump for the batch: both counters are compared for change, never
        # for magnitude, and the batch is never empty here.
        self.concepts.mutations += 1
        self.concepts.content_version += 1
        return True

    def _merge_imported_concept(
        self,
        source_key: str,
        source_name: str,
        target_key: str,
        concept: Concept,
        is_hidden: bool,
        concepts: list[str] | None,
    ) -> None:
        excluded = (
            concepts is not None
            and source_key not in concepts
            and source_name not in concepts
        )
        new = self.add_concept(concept)
        self.imported.concepts.add(target_key)
        self.imported.concepts.add(new.address)
        if excluded or is_hidden:
            # excluded from public view (or hidden in the source): still stored,
            # but marked hidden here rather than routed through __setitem__.
            if self.concepts.data.get(target_key) is not new:
                self.concepts.data[target_key] = new
            self.concepts.hidden.add(target_key)
        elif self.concepts.data.get(target_key) is not new:
            self.concepts[target_key] = new

    def add_file_import(
        self, path: str | Path, alias: str, env: Environment | None = None
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        from trilogy.parser import parse_text

        if isinstance(path, str):
            if path.endswith(".preql"):
                path = path.rsplit(".", 1)[0]
            parts = [path] if "." not in path else path.split(".")
            target = Path(self.working_path, *parts).with_suffix(".preql")
            if not target.exists():
                for root in self.import_paths:
                    candidate = Path(root, *parts).with_suffix(".preql")
                    if candidate.exists():
                        target = candidate
                        break
        else:
            target = path
        if not env:
            try:
                with safe_open(target) as f:
                    text = f.read()
                nenv = Environment(
                    working_path=target.parent, import_paths=list(self.import_paths)
                )
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
    ) -> tuple[Environment, list]:
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

        # Identical redeclaration (a script re-parsed against a persistent
        # environment): keep the durable object so no effective write occurs
        # and content_version-stamped caches survive. Signature, not `==` —
        # Concept.__eq__ ignores lineage. Rowset/multiselect concepts are
        # exempt: their lineage embeds the statement's SelectLineage OBJECT,
        # which planning matches by identity against named_statements — a
        # re-parse must replace both together or the rowset can't be wired.
        durable = self.concepts.data.get(concept.address)
        if durable is not None and (
            durable is concept
            or (
                not isinstance(durable, UndefinedConcept)
                and concept.derivation
                not in (Derivation.ROWSET, Derivation.MULTISELECT)
                and concept_structural_signature(durable)
                == concept_structural_signature(concept)
            )
        ):
            concept = durable
        else:
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
        # Identical redeclaration keeps the durable object (and its runtime
        # status) — no effective write, content_version-stamped caches survive.
        durable = dict.get(self.datasources, datasource.identifier)
        if durable is not None and (
            durable is datasource
            or datasource_structural_signature(durable)
            == datasource_structural_signature(datasource)
        ):
            return durable
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

    def user_concepts(self) -> list[Concept]:
        """Return all user-defined concepts, filtering out internal concepts."""
        return [
            c
            for c in self.concepts.values()
            if not c.namespace.startswith(INTERNAL_NAMESPACE)
            and not c.name.startswith("_")
        ]

    def concepts_at_line(self, line_number: int) -> list[Concept]:
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
                env, queries = parse(f2.read(), env)
                for query in queries:
                    self.setup_queries.append(query)
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
