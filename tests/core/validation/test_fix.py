"""Tests for trilogy.core.validation.fix."""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.authoring import (
    ConceptDeclarationStatement,
    Datasource,
)
from trilogy.core.enums import Purpose
from trilogy.core.models.author import Concept, Grain
from trilogy.core.models.datasource import Address, ColumnAssignment
from trilogy.core.validation import fix as fix_module
from trilogy.core.validation.fix import (
    DatasourceReferenceFix,
    apply_fixes_to_statements,
    rewrite_file_with_reference_merges,
    validate_and_rewrite,
)


def test_validate_and_rewrite_no_errors_str_input():
    test = """
key x int;

datasource example (
    x: x
)
grain (x)
query '''
select 1 as x''';
"""
    assert validate_and_rewrite(test) is None


def test_validate_and_rewrite_path_no_exec(tmp_path: Path):
    src = tmp_path / "clean.preql"
    src.write_text(
        """key x int;

datasource example (
    x: x
)
grain (x)
query '''
select 1 as x''';
""",
        encoding="utf-8",
    )
    # No executor means validation is generate-only; nothing to fix.
    assert validate_and_rewrite(src) is None


def test_validate_and_rewrite_path_roundtrip(tmp_path: Path):
    src = tmp_path / "source.preql"
    src.write_text(
        """key x int; # guessing at type

datasource example (
    x: x
)
grain (x)
query '''
select 'abc' as x''';
""",
        encoding="utf-8",
    )

    engine = Dialects.DUCK_DB.default_executor()
    result = validate_and_rewrite(src, engine)

    assert result is None
    rewritten = src.read_text(encoding="utf-8")
    assert "key x string" in rewritten


def test_validate_and_rewrite_iteration_limit(monkeypatch):
    monkeypatch.setattr(fix_module, "ITERATION_CUTOFF", 0)

    test = """key x int;

datasource example (
    x: x
)
grain (x)
query '''
select 'abc' as x''';
"""
    engine = Dialects.DUCK_DB.default_executor()
    result = validate_and_rewrite(test, engine)

    # Iteration cutoff of 0 means the loop runs one pass, applies a fix,
    # and exits via the for-else before re-validating — output is the fixed text.
    assert result is not None
    assert "key x string" in result


def test_reference_fix_replaces_concept_keys():
    """Replacing a concept via reference fix should update other concepts' keys."""
    customer_sk = Concept(
        name="customer_sk",
        datatype="int",
        purpose=Purpose.KEY,
    )
    # A property whose keys include the concept being replaced.
    customer_email = Concept(
        name="customer_email",
        datatype="string",
        purpose=Purpose.PROPERTY,
        keys={customer_sk.address},
    )
    external_customer_sk = Concept(
        name="customer_sk",
        namespace="customer",
        datatype="int",
        purpose=Purpose.KEY,
    )

    datasource = Datasource(
        name="store_sales",
        columns=[
            ColumnAssignment(
                alias="ss_customer_sk",
                concept=customer_sk.reference,
                modifiers=[],
            ),
            ColumnAssignment(
                alias="ss_customer_email",
                concept=customer_email.reference,
                modifiers=[],
            ),
        ],
        address=Address(location="store_sales", quoted=True),
        grain=Grain(components={customer_sk.address}),
    )

    statements = [
        ConceptDeclarationStatement(concept=customer_sk),
        ConceptDeclarationStatement(concept=customer_email),
        datasource,
    ]

    reference_fixes = [
        DatasourceReferenceFix(
            datasource_identifier=datasource.identifier,
            column_address=customer_sk.address,
            column_alias="ss_customer_sk",
            reference_concept=external_customer_sk.reference,
        )
    ]

    output = apply_fixes_to_statements(statements, [], [], reference_fixes)

    # Concept declaration for the replaced concept is dropped entirely.
    remaining_concepts = [
        s.concept.address for s in output if isinstance(s, ConceptDeclarationStatement)
    ]
    assert customer_sk.address not in remaining_concepts
    assert customer_email.address in remaining_concepts

    # customer_email's keys should now point at the external namespace.
    updated_email = next(
        s.concept for s in output if isinstance(s, ConceptDeclarationStatement)
    )
    assert external_customer_sk.address in updated_email.keys
    assert customer_sk.address not in updated_email.keys

    # Datasource grain should also be updated to the external address.
    updated_ds = next(s for s in output if isinstance(s, Datasource))
    assert external_customer_sk.address in updated_ds.grain.components


def test_rewrite_file_with_reference_merges_renders_output():
    customer_sk = Concept(name="customer_sk", datatype="int", purpose=Purpose.KEY)
    external_customer_sk = Concept(
        name="customer_sk",
        namespace="customer",
        datatype="int",
        purpose=Purpose.KEY,
    )
    datasource = Datasource(
        name="store_sales",
        columns=[
            ColumnAssignment(
                alias="ss_customer_sk",
                concept=customer_sk.reference,
                modifiers=[],
            )
        ],
        address=Address(location="store_sales", quoted=True),
        grain=Grain(components={customer_sk.address}),
    )
    statements = [
        ConceptDeclarationStatement(concept=customer_sk),
        datasource,
    ]
    reference_fixes = [
        DatasourceReferenceFix(
            datasource_identifier=datasource.identifier,
            column_address=customer_sk.address,
            column_alias="ss_customer_sk",
            reference_concept=external_customer_sk.reference,
        )
    ]

    rendered = rewrite_file_with_reference_merges(statements, reference_fixes)

    assert "customer.customer_sk" in rendered
    assert "key local.customer_sk" not in rendered


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
