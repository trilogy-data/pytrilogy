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

from trilogy.core.models.datasource import Datasource
from trilogy.execution.state.partitions import PartitionObservation
from trilogy.execution.state.watermarks import DatasourceWatermark

DeclarationKey = tuple[str, str | None, str]

UPDATE_TIME_KEY = "update_time"


def declaration_key(ds: Datasource) -> DeclarationKey:
    return (ds.safe_address, ds.declared_in, ds.name)


def group_declarations(
    datasources: Iterable[Datasource],
) -> dict[DeclarationKey, list[Datasource]]:
    groups: dict[DeclarationKey, list[Datasource]] = {}
    for ds in datasources:
        groups.setdefault(declaration_key(ds), []).append(ds)
    return groups


def canonical(group: list[Datasource]) -> Datasource:
    """The spelling that speaks for a declaration: the shortest import path,
    ties broken by name, so the choice is a pure function of the environment."""
    return min(group, key=lambda ds: (ds.identifier.count("."), ds.identifier))


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


def _rekey(keys: dict, mapping: dict[str, str]) -> dict | None:
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
