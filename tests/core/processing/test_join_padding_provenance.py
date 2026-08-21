"""``_gate_nullable_by_host`` decides whether two padded join keys may pair.

Null-safe equality is right exactly when both sides' NULLs are the SAME rows.
That is a question about provenance, and hosting only proxies it: an aggregate
over a padded scan joined back to that scan reads host/feeder asymmetric while
sharing every padded row, and stripping there lets the guard-upgrade rule flip
the scan's outer join to INNER and delete the padding.
"""

from trilogy.core.enums import Modifier, Purpose, SourceType
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.join_resolution import (
    _gate_nullable_by_host,
    _padding_sources,
)

NULLABLE = [Modifier.NULLABLE]
KEY = "local.padded"


def _concept(address: str) -> BuildConcept:
    namespace, name = address.split(".")
    return BuildConcept(
        name=name,
        canonical_name=name,
        namespace=namespace,
        datatype=DataType.STRING,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        grain=BuildGrain(components=set()),
    )


def _qds(
    outputs: list[str],
    nullable: list[str],
    parents: list[QueryDatasource] | None = None,
) -> QueryDatasource:
    concepts = [_concept(a) for a in outputs]
    return QueryDatasource(
        input_concepts=[],
        output_concepts=concepts,
        datasources=list(parents or []),
        source_map={a: set() for a in outputs},
        grain=BuildGrain(components=set()),
        joins=[],
        source_type=SourceType.SELECT,
        nullable_concepts=[_concept(a) for a in nullable],
    )


def _gate(
    shared: bool,
    host_nodes: set[str] | None = None,
    value_nullables: dict[str, list[str]] | None = None,
    authored: set[str] | None = None,
    modifiers: list[Modifier] | None = None,
) -> list[Modifier]:
    return _gate_nullable_by_host(
        list(NULLABLE if modifiers is None else modifiers),
        "left",
        "right",
        {KEY},
        host_nodes,
        value_nullables or {},
        authored,
        shared,
    )


def _identity(address: str) -> str:
    return address


def test_padding_sources_ignores_leaf_and_unpadded():
    assert _padding_sources(_qds([KEY], []), {KEY}, _identity) == set()


def test_padding_sources_reports_own_identifier():
    padded = _qds([KEY], [KEY])
    assert _padding_sources(padded, {KEY}, _identity) == {padded.identifier}


def test_padding_sources_walks_parents():
    padded = _qds([KEY], [KEY])
    consumer = _qds([KEY], [], parents=[padded])
    assert padded.identifier in _padding_sources(consumer, {KEY}, _identity)


def test_padding_sources_scoped_to_the_requested_key():
    padded = _qds([KEY, "local.other"], ["local.other"])
    assert _padding_sources(padded, {KEY}, _identity) == set()


def test_shared_padding_keeps_null_safe_under_asymmetric_hosting():
    """The q47 shape: hosting reads asymmetric, provenance says one scan."""
    assert _gate(shared=True, host_nodes={"left"}) == NULLABLE


def test_unshared_padding_strips_under_asymmetric_hosting():
    assert _gate(shared=False, host_nodes={"left"}) == []


def test_unshared_padding_strips_with_no_host_basis():
    """Mid-plan merges carry no host basis; independent trees still cannot pair."""
    assert _gate(shared=False, host_nodes=None) == []


def test_symmetric_hosting_keeps_null_safe():
    assert _gate(shared=False, host_nodes={"left", "right"}) == NULLABLE
    assert _gate(shared=False, host_nodes=set()) == NULLABLE


def test_value_nulls_are_exempt_on_either_side():
    assert _gate(shared=False, value_nullables={"left": [KEY]}) == NULLABLE
    assert _gate(shared=False, value_nullables={"right": [KEY]}) == NULLABLE


def test_authored_key_is_exempt():
    assert _gate(shared=False, authored={KEY}) == NULLABLE


def test_gate_leaves_other_modifiers_alone():
    assert _gate(shared=False, modifiers=[Modifier.PARTIAL]) == [Modifier.PARTIAL]
    assert _gate(shared=False, modifiers=[Modifier.NULLABLE, Modifier.PARTIAL]) == [
        Modifier.PARTIAL
    ]
