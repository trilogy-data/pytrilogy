"""Declaration identity for asset state.

A datasource is declared once, in one file, and then spelled under as many
identifiers as there are import paths to that file: ``stages`` in the file that
declares it, ``stage.stages`` and ``vehicle.stage.stages`` from its importers.
Every spelling reads the same bytes through the same column bindings, so state
belongs to the declaration — ``(physical address, declaring file, declared
name)`` — and each spelling is a view of it. One probe serves them all, and a
snapshot carries one entry for them all, whichever script did the probing.

The address is part of the key because a deployment ``env`` can repoint a
declaration, and two declarations may legitimately share one address (a writer
and a reader that model the same file differently); those stay distinct.
"""

from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path

from trilogy.core.models.datasource import Datasource, UpdateKey
from trilogy.execution.state.partitions import PartitionObservation
from trilogy.execution.state.watermarks import DatasourceWatermark

DeclarationKey = tuple[str, str | None, str]

UPDATE_TIME_KEY = "update_time"


def normalized_file(path: Path | str) -> str:
    """One spelling for a declaring file, so an entrypoint and a
    ``declared_in`` stamp compare equal. Resolved rather than merely
    absolute: on Windows the two can differ in drive-letter case alone."""
    return str(Path(path).resolve())


def build_scope_for(path: Path | str) -> frozenset[str]:
    """The build scope of a single-file run: that file's own declarations.

    What a file *declares* is what a run of it builds. Everything it reaches by
    import belongs to a run of the file that declares it — which is also how a
    directory run assigns ownership, one script per address."""
    return frozenset({normalized_file(path)})


def declared_within(ds: Datasource, build_scope: frozenset[str]) -> bool:
    """Whether a build scope covers this declaration. A datasource with no
    recorded file (parsed from text, not from a path) is always covered: there
    is no other run that would claim it."""
    if not build_scope or ds.declared_in is None:
        return True
    return normalized_file(ds.declared_in) in build_scope


def declaration_key(ds: Datasource) -> DeclarationKey:
    return (ds.safe_address, ds.declared_in, ds.name)


def group_declarations(
    datasources: Iterable[Datasource],
) -> dict[DeclarationKey, list[Datasource]]:
    groups: dict[DeclarationKey, list[Datasource]] = {}
    for ds in datasources:
        groups.setdefault(declaration_key(ds), []).append(ds)
    return groups


def import_depth(ds: Datasource) -> tuple[int, str]:
    """Sort key ranking a spelling by how far it is from the declaration: the
    shortest import path first, ties broken by name, so every caller that has to
    pick one spelling picks the same one as a pure function of the environment."""
    return (ds.identifier.count("."), ds.identifier)


def by_import_depth(group: Iterable[Datasource]) -> list[Datasource]:
    return sorted(group, key=import_depth)


def canonical(group: list[Datasource]) -> Datasource:
    """The spelling that speaks for a declaration."""
    return min(group, key=import_depth)


def _key_map(source: Datasource, target: Datasource) -> dict[str, str]:
    """Watermark key of ``source`` -> the key ``target`` emits for the same
    physical column. The column is the bridge: import copies differ only in the
    namespace of the concepts their columns bind."""
    by_alias = {
        col.alias: col.concept.address
        for col in target.columns
        if isinstance(col.alias, str)
    }
    return {
        col.concept.address: by_alias[col.alias]
        for col in source.columns
        if isinstance(col.alias, str) and col.alias in by_alias
    }


def _rekey(
    keys: dict[str, UpdateKey], mapping: dict[str, str]
) -> dict[str, UpdateKey] | None:
    out = {}
    for key, value in keys.items():
        target = key if key == UPDATE_TIME_KEY else mapping.get(key)
        # A key with no column to bridge through (a watermark reached by
        # lineage) cannot be translated; the caller probes instead.
        if target is None:
            return None
        out[target] = replace(value, concept_name=target)
    return out


def rekey_watermark(
    watermark: DatasourceWatermark, source: Datasource, target: Datasource
) -> DatasourceWatermark | None:
    keys = _rekey(watermark.keys, _key_map(source, target))
    if keys is None:
        return None
    return DatasourceWatermark(keys=keys, unreadable=watermark.unreadable)


def rekey_partitions(
    sides: tuple[list[PartitionObservation], list[PartitionObservation]],
    source: Datasource,
    target: Datasource,
) -> tuple[list[PartitionObservation], list[PartitionObservation]] | None:
    mapping = _key_map(source, target)
    out: list[list[PartitionObservation]] = []
    for side in sides:
        rekeyed = []
        for obs in side:
            keys = _rekey(obs.keys, mapping)
            if keys is None:
                return None
            rekeyed.append(replace(obs, keys=keys))
        out.append(rekeyed)
    return out[0], out[1]
