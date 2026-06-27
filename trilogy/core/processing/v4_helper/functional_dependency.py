from __future__ import annotations

from collections.abc import Iterable, Iterator

from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment

from .models import ConceptAttrs


def _attrs_for_address(
    concept_attrs: dict[str, ConceptAttrs], address: str
) -> Iterable[ConceptAttrs]:
    for attrs in concept_attrs.values():
        if attrs.address == address:
            yield attrs


def concept_attr_fd_closure(
    concept_attrs: dict[str, ConceptAttrs],
    determinants: Iterable[str],
    *,
    include_empty_grain: bool = True,
) -> frozenset[str]:
    closure = set(determinants)
    changed = True
    while changed:
        changed = False
        for attrs in concept_attrs.values():
            if attrs.address in closure:
                continue
            if not attrs.grain_components:
                if include_empty_grain:
                    closure.add(attrs.address)
                    changed = True
                continue
            if attrs.grain_components <= closure or (
                bool(attrs.keys) and attrs.keys <= closure
            ):
                closure.add(attrs.address)
                changed = True
    return frozenset(closure)


def concept_attr_fd_determines(
    concept_attrs: dict[str, ConceptAttrs],
    determinants: Iterable[str],
    address: str,
    *,
    include_empty_grain: bool = True,
) -> bool:
    if address in determinants:
        return True
    if not any(_attrs_for_address(concept_attrs, address)):
        return False
    return address in concept_attr_fd_closure(
        concept_attrs,
        determinants,
        include_empty_grain=include_empty_grain,
    )


def _build_fd_concepts(environment: BuildEnvironment) -> Iterator[BuildConcept]:
    yield from environment.concepts.values()
    for datasource in environment.datasources.values():
        yield from datasource.output_concepts


def build_fd_closure(
    environment: BuildEnvironment,
    determinants: Iterable[str],
    *,
    include_empty_grain: bool = True,
) -> frozenset[str]:
    closure = set(determinants)
    changed = True
    while changed:
        changed = False
        for address in list(closure):
            concept = environment.concepts.get(address)
            if concept is None:
                continue
            for equivalent in concept.equivalent_addresses:
                if equivalent not in closure:
                    closure.add(equivalent)
                    changed = True
        for address, concept in environment.concepts.items():
            if address in closure:
                continue
            if concept.address in closure or bool(
                concept.equivalent_addresses & closure
            ):
                closure.add(address)
                changed = True
        for concept in _build_fd_concepts(environment):
            if concept.address in closure:
                continue
            grain = concept.grain.components if concept.grain else frozenset()
            if not grain:
                if include_empty_grain:
                    closure.add(concept.address)
                    changed = True
                continue
            keys = frozenset(concept.keys or set())
            if grain <= closure or (bool(keys) and keys <= closure):
                closure.add(concept.address)
                changed = True
    return frozenset(closure)


def build_fd_determines(
    environment: BuildEnvironment,
    determinants: Iterable[str],
    address: str,
    *,
    include_empty_grain: bool = True,
) -> bool:
    return address in build_fd_closure(
        environment,
        determinants,
        include_empty_grain=include_empty_grain,
    )


def minimize_build_grain(
    environment: BuildEnvironment,
    grain: Iterable[str],
) -> frozenset[str]:
    minimized = set(grain)
    changed = True
    while changed:
        changed = False
        for address in sorted(minimized):
            determinants = minimized - {address}
            if not determinants:
                continue
            if build_fd_determines(
                environment,
                determinants,
                address,
                include_empty_grain=False,
            ):
                minimized.remove(address)
                changed = True
                break
    return frozenset(minimized)
