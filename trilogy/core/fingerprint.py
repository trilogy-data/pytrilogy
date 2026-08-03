"""Content fingerprints for parsed environments.

Answers "are two models different, and which objects changed" without
building anything: hashes are computed on author-layer objects in one
post-parse traversal.

Every object gets two hashes:

- ``local``: shallow structure with concept references kept as bare
  addresses. Detects edits to the object's own declaration.
- ``effective``: fully expanded. A concept reference is replaced by the
  hash of the referenced concept's expansion, recursively, and a derived
  concept's expansion is exactly the canonical hash of its lineage — so a
  named intermediate concept and the equivalent inline expression hash
  identically, and derived-concept names never enter downstream hashes.
  Only ROOT concepts (physical bindings) contribute their address as leaf
  identity.

A datasource whose ``effective`` hash changed needs rebuild; a
``local``-only change is a refactor. Pseudonyms are folded in one level
deep as raw addresses (never dereferenced), so merge changes over-invalidate
rather than under-invalidate and alias cycles cannot recurse.

Fingerprints are deployment-env invariant: an address the env transform
rewrote (``Address.env_label`` set) is hashed under its logical,
pre-transform location, so the same model fingerprints identically whether
parsed scoped or unscoped.
"""

from __future__ import annotations

import hashlib
from collections import UserDict, UserList
from collections.abc import Sequence
from dataclasses import fields as dc_fields
from dataclasses import is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from trilogy.core.enums import FunctionType
from trilogy.core.models.author import (
    Concept,
    ConceptRef,
    CustomFunctionFactory,
    Function,
    FunctionCallWrapper,
    Grain,
    Metadata,
    Parenthetical,
)
from trilogy.core.models.datasource import (
    Address,
    ColumnAssignment,
    Datasource,
    DatasourceMetadata,
    RawColumnExpr,
)
from trilogy.core.models.environment import Environment

FINGERPRINT_VERSION = 1

# Fields that are validation metadata or inferred from arguments (output
# types ripple from upstream and would misclassify upstream changes as
# definition edits), not computation identity.
_EXCLUDED_FIELDS: dict[str, set[str]] = {
    "Function": {"valid_inputs", "arg_count", "output_datatype", "output_purpose"},
}

# List fields whose order is not semantic (grouping/partition sets).
_UNORDERED_FIELDS: dict[str, set[str]] = {
    "AggregateWrapper": {"by", "grouping_sets"},
    "NumberingWindowItem": {"over"},
    "NavigationWindowItem": {"over"},
}


class FingerprintError(Exception):
    pass


class ObjectFingerprint(BaseModel):
    local: str
    effective: str


class DatasourceFingerprint(ObjectFingerprint):
    location: str | None = None


class EnvironmentFingerprint(BaseModel):
    fingerprint_version: int = FINGERPRINT_VERSION
    trilogy_version: str = ""
    root: str
    extras: str
    concepts: dict[str, ObjectFingerprint] = Field(default_factory=dict)
    datasources: dict[str, DatasourceFingerprint] = Field(default_factory=dict)


class ChangeKind(str, Enum):
    # the object's own declaration changed
    DEFINITION = "definition"
    # only expansions it depends on changed
    UPSTREAM = "upstream"
    # declaration text changed but expansion is identical; no rebuild needed
    REFACTOR = "refactor"


class SectionDiff(BaseModel):
    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    changed: dict[str, ChangeKind] = Field(default_factory=dict)
    renamed: dict[str, str] = Field(default_factory=dict)

    @property
    def empty(self) -> bool:
        return not (self.added or self.removed or self.changed or self.renamed)


class FingerprintDiff(BaseModel):
    identical: bool
    concepts: SectionDiff
    datasources: SectionDiff
    # datasources whose effective hash changed: contents are stale on rebuild
    invalidated_datasources: list[str] = Field(default_factory=list)
    extras_changed: bool = False


def _h(*parts: str) -> str:
    m = hashlib.blake2b(digest_size=16)
    for p in parts:
        m.update(p.encode("utf-8"))
        m.update(b"\x1f")
    return m.hexdigest()


def _logical_locations(address: Address) -> tuple[str, str, list[str]]:
    """(location, write_location, additional) with any deployment-env prefix
    reversed, so the same model fingerprints identically whether or not the
    env transform has run (``Address.env_label`` records that it did)."""
    if address.env_label is None:
        return (
            address.location,
            address.write_location or "",
            sorted(address.additional_locations),
        )
    from trilogy.execution.envs import strip_env_prefix

    label = address.env_label
    is_file = address.is_file
    return (
        strip_env_prefix(address.location, label, is_file),
        (
            strip_env_prefix(address.write_location, label, is_file)
            if address.write_location
            else ""
        ),
        sorted(
            strip_env_prefix(loc, label, is_file)
            for loc in address.additional_locations
        ),
    )


def _address_token(address: Address | str) -> str:
    if isinstance(address, str):
        return _h("addr", address)
    location, write_location, additional = _logical_locations(address)
    # Address.quoted is source syntax only (see Renderer); it never reaches SQL,
    # so re-quoting an address must not invalidate the datasource's contents.
    return _h(
        "addr",
        location,
        write_location,
        address.type.name,
        # partition column order defines hive layout; keep it
        *address.partition_columns,
        *additional,
    )


class _Canonicalizer:
    """Bottom-up hash-consing over author-layer model objects.

    deep=True replaces concept references with expansion hashes; deep=False
    keeps them as raw addresses (shallow/local mode).
    """

    def __init__(self, environment: Environment, deep: bool):
        self.environment = environment
        self.deep = deep
        self._memo: dict[str, str] = {}
        self._in_progress: set[str] = set()

    def node(self, obj: Any) -> str:
        if obj is None:
            return _h("none")
        if isinstance(obj, Enum):
            return _h("enum", type(obj).__name__, obj.name)
        if isinstance(obj, bool):
            return _h("bool", str(obj))
        if isinstance(obj, (int, float, Decimal)):
            return _h("num", repr(obj))
        if isinstance(obj, str):
            return _h("str", obj)
        if isinstance(obj, bytes):
            return _h("bytes", obj.hex())
        if isinstance(obj, (datetime, date, time)):
            return _h("time", type(obj).__name__, obj.isoformat())
        if isinstance(obj, (Metadata, DatasourceMetadata)):
            return _h("meta")
        if isinstance(obj, Concept):
            return self._concept_node(obj)
        if isinstance(obj, ConceptRef):
            return self._reference(obj.address)
        if isinstance(obj, Grain):
            return self._grain(obj)
        if isinstance(obj, FunctionCallWrapper):
            # deep mode looks through the call so extracting an expression
            # into a custom function is an identity refactor
            if self.deep:
                return self.node(obj.content)
            return _h("fncall", obj.name, self.node(obj.content), self.node(obj.args))
        if isinstance(obj, Parenthetical):
            return self.node(obj.content)
        if isinstance(obj, Function) and obj.operator == FunctionType.PARENTHETICAL:
            return self.node(obj.arguments[0])
        if isinstance(obj, Address):
            return _address_token(obj)
        if isinstance(obj, Datasource):
            return self.datasource_hash(obj)
        if isinstance(obj, UserList):
            return _h("list", *[self.node(v) for v in obj.data])
        if isinstance(obj, UserDict):
            return self._mapping(obj.data)
        if isinstance(obj, (list, tuple)):
            return _h("list", *[self.node(v) for v in obj])
        if isinstance(obj, (set, frozenset)):
            return _h("uset", *sorted(self.node(v) for v in obj))
        if isinstance(obj, dict):
            return self._mapping(obj)
        if is_dataclass(obj):
            return self._dataclass(obj)
        if isinstance(obj, BaseModel):
            return self._pydantic(obj)
        raise FingerprintError(f"Cannot canonicalize {type(obj).__name__}")

    def _mapping(self, obj: dict) -> str:
        items = sorted((self.node(k), self.node(v)) for k, v in obj.items())
        return _h("map", *[p for kv in items for p in kv])

    def _dataclass(self, obj: Any) -> str:
        cls_name = type(obj).__name__
        excluded = _EXCLUDED_FIELDS.get(cls_name, set())
        unordered = _UNORDERED_FIELDS.get(cls_name, set())
        parts = ["dc", cls_name]
        for f in dc_fields(obj):
            if not f.compare or f.name in excluded:
                continue
            value = getattr(obj, f.name)
            parts.append(f.name)
            if f.name in unordered and isinstance(value, list):
                parts.append(self._unordered_list(value))
            else:
                parts.append(self.node(value))
        return _h(*parts)

    def _pydantic(self, obj: BaseModel) -> str:
        parts = ["pm", type(obj).__name__]
        for name in sorted(type(obj).model_fields):
            parts.append(name)
            parts.append(self.node(getattr(obj, name)))
        return _h(*parts)

    def _unordered_list(self, value: list) -> str:
        hashes = sorted(
            self._unordered_list(v) if isinstance(v, list) else self.node(v)
            for v in value
        )
        return _h("uset", *hashes)

    def _grain(self, grain: Grain) -> str:
        if self.deep:
            components = sorted(self._reference(a) for a in grain.components)
        else:
            components = sorted(_h("ref", a) for a in grain.components)
        return _h("grain", *components, self.node(grain.where_clause))

    def _reference(self, address: str) -> str:
        if not self.deep:
            return _h("ref", address)
        cached = self._memo.get(address)
        if cached is not None:
            return cached
        concept = self.environment.concepts.data.get(address)
        if concept is None:
            result = _h("unresolved", address)
        else:
            result = self._concept_node(concept)
        self._memo[address] = result
        return result

    def concept_effective(self, concept: Concept) -> str:
        return self._concept_node(concept)

    def _concept_node(self, concept: Concept) -> str:
        if not self.deep:
            return _h("ref", concept.address)
        if concept.address in self._in_progress:
            return _h("cycle")
        self._in_progress.add(concept.address)
        try:
            return self._expand(concept)
        finally:
            self._in_progress.discard(concept.address)

    def _expand(self, concept: Concept) -> str:
        lineage: Any = concept.lineage
        if isinstance(lineage, FunctionCallWrapper):
            lineage = lineage.content
        if lineage is None:
            parts = [
                "root",
                concept.address,
                self.node(concept.datatype),
                self.node(concept.purpose),
            ]
            for key in sorted(concept.keys or ()):
                parts.append(self._reference(key))
            if concept.pseudonyms:
                # one level, raw, never dereferenced: alias/merge cycles cannot
                # recurse, and merge edits over- rather than under-invalidate
                parts.append("pseudonyms")
                parts.extend(sorted(concept.pseudonyms))
            return _h(*parts)
        # exactly the lineage hash, unwrapped: an inline expression node and a
        # reference to a concept naming that expression must hash identically
        core = self.node(lineage)
        if concept.pseudonyms:
            return _h("pseudo", core, *sorted(concept.pseudonyms))
        return core

    def concept_local(self, concept: Concept) -> str:
        # authored declaration only: for derived concepts the datatype, grain,
        # derivation, and granularity are inferred and ripple from upstream, so
        # including them would misclassify upstream changes as definition edits
        parts = ["concept", self.node(concept.purpose)]
        if concept.lineage is None:
            parts += [
                self.node(concept.datatype),
                self.node(concept.derivation),
                self.node(concept.granularity),
                self._grain(concept.grain),
            ]
        else:
            parts.append(self.node(concept.lineage))
        parts += [
            *sorted(concept.keys or ()),
            self.node(concept.modifiers),
            *sorted(concept.pseudonyms),
        ]
        return _h(*parts)

    def _column_hash(self, column: ColumnAssignment) -> str:
        alias = column.alias
        if isinstance(alias, str):
            alias_part = _h("a", alias)
        elif isinstance(alias, RawColumnExpr):
            alias_part = _h("rawsql", alias.text)
        else:
            alias_part = self.node(alias)
        return _h(
            "col",
            alias_part,
            self.node(column.concept),
            *sorted(self.node(m) for m in column.modifiers),
        )

    def datasource_hash(self, datasource: Datasource) -> str:
        parts = [
            "datasource",
            _address_token(datasource.address),
            # column declaration order is presentational
            *sorted(self._column_hash(c) for c in datasource.columns),
            self._grain(datasource.grain),
            self.node(datasource.where),
            self.node(datasource.non_partial_for),
            "pb",
            *[self.node(r) for r in datasource.partition_by],
            "ib",
            *[self.node(r) for r in datasource.incremental_by],
            "fb",
            *[self.node(r) for r in datasource.freshness_by],
            self.node(datasource.freshness_probe),
            self.node(datasource.refresh_script),
            self.node(datasource.allowed_lag),
            self.node(datasource.is_root),
            "clp",
            *sorted(datasource.column_level_partial_addresses),
        ]
        if not self.deep:
            parts.append(self.node(datasource.status))
        return _h(*parts)


def _function_hash(deep: _Canonicalizer, factory: CustomFunctionFactory) -> str:
    return _h(
        "function",
        factory.name,
        deep.node(factory.function),
        deep.node(factory.function_arguments),
    )


def _extras_hash(environment: Environment, deep: _Canonicalizer) -> str:
    merges = sorted(
        _h("merge", left, right, deep.node(join))
        for left, right, join in environment.merges
    )
    functions = sorted(
        _h("fn", name, _function_hash(deep, factory))
        for name, factory in environment.functions.items()
    )
    data_types = sorted(
        _h("type", name, deep.node(custom))
        for name, custom in environment.data_types.items()
    )
    return _h("extras", *merges, *functions, *data_types)


def datasource_logical_location(datasource: Datasource) -> str | None:
    """The datasource's physical location with any deployment-env prefix
    reversed — the env-invariant identity fingerprint records pair with."""
    if isinstance(datasource.address, str):
        return datasource.address
    location, _, _ = _logical_locations(datasource.address)
    return location


def datasource_effective_hash(environment: Environment, datasource: Datasource) -> str:
    """The effective content hash of one datasource, equal to the value
    ``build_environment_fingerprint`` records for it (memoization aside)."""
    return _Canonicalizer(environment, deep=True).datasource_hash(datasource)


def build_environment_fingerprint(
    environment: Environment,
    extra_datasources: Sequence[tuple[Datasource, Any]] = (),
) -> EnvironmentFingerprint:
    """Fingerprint a parsed environment.

    ``extra_datasources`` carries datasources that are not registered in the
    environment at parse time — persist targets — as (datasource, context)
    pairs, where context is any canonicalizable object folded into both hashes
    (a persist's select lineage: the datasource model alone cannot see the
    select's WHERE, which decides the table's contents).
    """
    deep = _Canonicalizer(environment, deep=True)
    shallow = _Canonicalizer(environment, deep=False)
    concepts: dict[str, ObjectFingerprint] = {}
    for concept in environment.concepts.data.values():
        if concept.is_internal or concept.address in concepts:
            continue
        concepts[concept.address] = ObjectFingerprint(
            local=shallow.concept_local(concept),
            effective=deep.concept_effective(concept),
        )
    datasources: dict[str, DatasourceFingerprint] = {}
    for datasource in environment.datasources.values():
        datasources[datasource.identifier] = DatasourceFingerprint(
            local=shallow.datasource_hash(datasource),
            effective=deep.datasource_hash(datasource),
            location=datasource_logical_location(datasource),
        )
    for datasource, context in extra_datasources:
        if datasource.identifier in datasources:
            continue
        local = shallow.datasource_hash(datasource)
        effective = deep.datasource_hash(datasource)
        if context is not None:
            local = _h("persist", local, shallow.node(context))
            effective = _h("persist", effective, deep.node(context))
        datasources[datasource.identifier] = DatasourceFingerprint(
            local=local,
            effective=effective,
            location=datasource_logical_location(datasource),
        )
    extras = _extras_hash(environment, deep)
    root = _h(
        "root",
        str(FINGERPRINT_VERSION),
        *sorted(f"c:{k}={v.effective}" for k, v in concepts.items()),
        *sorted(f"d:{k}={v.effective}" for k, v in datasources.items()),
        extras,
    )
    return EnvironmentFingerprint(
        trilogy_version=environment.version,
        root=root,
        extras=extras,
        concepts=concepts,
        datasources=datasources,
    )


def _classify(base: ObjectFingerprint, other: ObjectFingerprint) -> ChangeKind | None:
    if base.effective != other.effective:
        if base.local != other.local:
            return ChangeKind.DEFINITION
        return ChangeKind.UPSTREAM
    if base.local != other.local:
        return ChangeKind.REFACTOR
    return None


def _diff_section(
    base: dict[str, ObjectFingerprint] | dict[str, DatasourceFingerprint],
    other: dict[str, ObjectFingerprint] | dict[str, DatasourceFingerprint],
) -> SectionDiff:
    added = sorted(set(other) - set(base))
    removed = sorted(set(base) - set(other))
    changed: dict[str, ChangeKind] = {}
    for key in sorted(set(base) & set(other)):
        kind = _classify(base[key], other[key])
        if kind is not None:
            changed[key] = kind
    # a removed and an added entry with the same unique effective hash is a
    # rename: identical semantics under a new name
    removed_by_effective: dict[str, list[str]] = {}
    for key in removed:
        removed_by_effective.setdefault(base[key].effective, []).append(key)
    added_by_effective: dict[str, list[str]] = {}
    for key in added:
        added_by_effective.setdefault(other[key].effective, []).append(key)
    renamed: dict[str, str] = {}
    for effective, old_keys in removed_by_effective.items():
        new_keys = added_by_effective.get(effective)
        if new_keys and len(old_keys) == 1 and len(new_keys) == 1:
            renamed[old_keys[0]] = new_keys[0]
    added = [k for k in added if k not in set(renamed.values())]
    removed = [k for k in removed if k not in renamed]
    return SectionDiff(added=added, removed=removed, changed=changed, renamed=renamed)


def diff_fingerprints(
    base: EnvironmentFingerprint, other: EnvironmentFingerprint
) -> FingerprintDiff:
    if base.fingerprint_version != other.fingerprint_version:
        raise FingerprintError(
            f"Cannot diff fingerprints with different versions: "
            f"{base.fingerprint_version} vs {other.fingerprint_version}; refingerprint"
        )
    datasource_diff = _diff_section(base.datasources, other.datasources)
    invalidated = sorted(
        key
        for key, kind in datasource_diff.changed.items()
        if kind != ChangeKind.REFACTOR
    )
    return FingerprintDiff(
        identical=base.root == other.root,
        concepts=_diff_section(base.concepts, other.concepts),
        datasources=datasource_diff,
        invalidated_datasources=invalidated,
        extras_changed=base.extras != other.extras,
    )
