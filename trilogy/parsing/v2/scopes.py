from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.enums import Purpose
from trilogy.core.models.author import UndefinedConceptFull
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment


@contextmanager
def temporary_function_scope(
    environment: Environment, parameter_names: list[str]
) -> Iterator[None]:
    # Scoped semantic placeholders for function-body hydration; not a collect/commit
    # environment update. CustomFunctionFactory.__call__ later replaces these addresses
    # with real call arguments, so placeholders only need to resolve lookups here.
    namespace = environment.namespace or DEFAULT_NAMESPACE
    added: list[str] = []
    for name in parameter_names:
        address = f"{namespace}.{name}"
        if address in environment.concepts.data:
            continue
        environment.concepts.data[address] = UndefinedConceptFull(
            name=name,
            namespace=namespace,
            datatype=DataType.UNKNOWN,
            purpose=Purpose.UNKNOWN,
        )
        added.append(address)
    try:
        yield
    finally:
        for address in added:
            environment.concepts.data.pop(address, None)


@contextmanager
def temporary_rowset_scope(
    environment: Environment, addresses: list[str]
) -> Iterator[None]:
    # Scoped placeholders for rowset output references used inside the same
    # rowset's multi-select derive clause. rowset_to_concepts replaces these
    # with real concepts via force=True; anything still unresolved is removed on exit.
    added: list[str] = []
    for address in addresses:
        if address in environment.concepts.data:
            continue
        if "." in address:
            namespace, _, name = address.rpartition(".")
        else:
            namespace = environment.namespace or DEFAULT_NAMESPACE
            name = address
        environment.concepts.data[address] = UndefinedConceptFull(
            name=name,
            namespace=namespace,
            datatype=DataType.UNKNOWN,
            purpose=Purpose.UNKNOWN,
        )
        added.append(address)
    try:
        yield
    finally:
        for address in added:
            concept = environment.concepts.data.get(address)
            if isinstance(concept, UndefinedConceptFull):
                environment.concepts.data.pop(address, None)
