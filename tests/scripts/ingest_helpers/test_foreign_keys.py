"""Tests for foreign key handling in ingest."""

from trilogy.authoring import Comment, ConceptDeclarationStatement, Datasource
from trilogy.core.enums import Purpose
from trilogy.core.models.author import Concept, Grain
from trilogy.core.models.datasource import Address, ColumnAssignment
from trilogy.scripts.ingest_helpers.foreign_keys import (
    apply_foreign_key_references,
    parse_foreign_keys,
)


def test_parse_foreign_keys_single():
    """Test parsing a single FK specification."""
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {"store_sales": {"ss_customer_sk": "customer.c_customer_sk"}}


def test_parse_foreign_keys_multiple():
    """Test parsing multiple FK specifications."""
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk,store_sales.ss_item_sk:item.i_item_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {
        "store_sales": {
            "ss_customer_sk": "customer.c_customer_sk",
            "ss_item_sk": "item.i_item_sk",
        }
    }


def test_parse_foreign_keys_empty():
    """Test parsing empty string."""
    result = parse_foreign_keys("")
    assert result == {}


def test_parse_foreign_keys_none():
    """Test parsing None."""
    result = parse_foreign_keys(None)
    assert result == {}


def test_apply_foreign_key_references():
    """Test applying FK references to a datasource."""
    # Create test concepts
    customer_sk_concept = Concept(
        name="customer_sk",
        datatype="int",
        purpose=Purpose.PROPERTY,
        keys={"item_sk", "ticket_number"},
    )
    item_sk_concept = Concept(
        name="item_sk",
        datatype="int",
        purpose=Purpose.KEY,
    )
    ticket_number_concept = Concept(
        name="ticket_number",
        datatype="int",
        purpose=Purpose.KEY,
    )


    # Create test datasource
    column_assignments = [
        ColumnAssignment(
            alias="ss_customer_sk",
            concept=customer_sk_concept.reference,
            modifiers=[],
        ),
        ColumnAssignment(
            alias="ss_item_sk",
            concept=item_sk_concept.reference,
            modifiers=[],
        ),
        ColumnAssignment(
            alias="ss_ticket_number",
            concept=ticket_number_concept.reference,
            modifiers=[],
        ),
    ]

    datasource = Datasource(
        name="store_sales",
        columns=column_assignments,
        address=Address(location="store_sales", quoted=True),
        grain=Grain(components={"item_sk", "ticket_number"}),
    )

    customer_datasource = Datasource(
        name="customer",
        columns=[
            ColumnAssignment(
                alias="c_customer_sk",
                concept=customer_sk_concept.reference,
                modifiers=[],
            )
        ],
        address=Address(location="customer", quoted=True),
        grain=Grain(components={"customer_sk"}),
    )

    datasources = {datasource.name: datasource, customer_datasource.name: customer_datasource}

    # Create script content
    script_content = [
        Comment(text="# Test datasource"),
        ConceptDeclarationStatement(concept=customer_sk_concept),
        ConceptDeclarationStatement(concept=item_sk_concept),
        ConceptDeclarationStatement(concept=ticket_number_concept),
        datasource,
    ]

    # Apply FK references
    column_mappings = {"ss_customer_sk": "customer.customer_sk"}

    result = apply_foreign_key_references(
        "store_sales", datasource, datasources, script_content, column_mappings
    )

    # Verify result
    assert "import customer as customer;" in result
    assert "customer.c_customer_sk" in result
    # Customer SK concept declaration should be removed
    assert "customer_sk" not in result or "import customer" in result
    # Other concept declarations should still be there
    assert "key item_sk int" in result
    assert "key ticket_number int" in result


def test_apply_foreign_key_references_multiple_fks():
    """Test applying multiple FK references to a datasource."""
    # Create test concepts
    customer_sk_concept = Concept(
        name="customer_sk",
        datatype="int",
        purpose=Purpose.PROPERTY,
        keys={"item_sk"},
    )
    store_sk_concept = Concept(
        name="store_sk",
        datatype="int",
        purpose=Purpose.PROPERTY,
        keys={"item_sk"},
    )
    item_sk_concept = Concept(
        name="item_sk",
        datatype="int",
        purpose=Purpose.KEY,
    )

    concepts = [customer_sk_concept, store_sk_concept, item_sk_concept]

    # Create test datasource
    column_assignments = [
        ColumnAssignment(
            alias="ss_customer_sk",
            concept=customer_sk_concept.reference,
            modifiers=[],
        ),
        ColumnAssignment(
            alias="ss_store_sk",
            concept=store_sk_concept.reference,
            modifiers=[],
        ),
        ColumnAssignment(
            alias="ss_item_sk",
            concept=item_sk_concept.reference,
            modifiers=[],
        ),
    ]

    datasource = Datasource(
        name="store_sales",
        columns=column_assignments,
        address=Address(location="store_sales", quoted=True),
        grain=Grain(components={"item_sk"}),
    )

    # Create script content
    script_content = [
        Comment(text="# Test datasource"),
        ConceptDeclarationStatement(concept=customer_sk_concept),
        ConceptDeclarationStatement(concept=store_sk_concept),
        ConceptDeclarationStatement(concept=item_sk_concept),
        datasource,
    ]

    # Apply FK references
    column_mappings = {
        "ss_customer_sk": "customer.c_customer_sk",
        "ss_store_sk": "store.s_store_sk",
    }

    result = apply_foreign_key_references(
        "store_sales", datasource, concepts, script_content, column_mappings
    )

    # Verify both imports are present
    assert "import customer as customer;" in result
    assert "import store as store;" in result

    # Verify both references are updated
    assert "customer.c_customer_sk" in result
    assert "store.s_store_sk" in result

    # Verify replaced concept declarations are removed
    assert "customer_sk" not in result or "import customer" in result
    assert "store_sk" not in result or "import store" in result

    # Other concept declarations should still be there
    assert "key item_sk int" in result
