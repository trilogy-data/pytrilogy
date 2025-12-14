"""Tests for foreign key handling in ingest."""

from trilogy import Environment, EnvironmentConfig
from trilogy.authoring import Comment, ConceptDeclarationStatement, Datasource, DictImportResolver, Renderer
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
        grain=Grain(components={"item_sk", "ticket_number", "customer_sk"}),
    )

    # Create a separate customer_sk concept for the customer table
    customer_table_customer_sk_concept = Concept(
        name="customer_sk",
        datatype="int",
        purpose=Purpose.KEY,
    )

    customer_datasource = Datasource(
        name="customer",
        columns=[
            ColumnAssignment(
                alias="c_customer_sk",
                concept=customer_table_customer_sk_concept.reference,
                modifiers=[],
            )
        ],
        address=Address(location="customer", quoted=True),
        grain=Grain(components={"customer_sk"}),
    )

    datasources = {
        datasource.name: datasource,
        customer_datasource.name: customer_datasource,
    }

    # Create script content
    script_content = [
        Comment(text="# Test datasource"),
        ConceptDeclarationStatement(concept=customer_sk_concept),
        ConceptDeclarationStatement(concept=item_sk_concept),
        ConceptDeclarationStatement(concept=ticket_number_concept),
        datasource,
    ]

    # Apply FK references - use the actual column alias from customer datasource
    column_mappings = {"ss_customer_sk": "customer.c_customer_sk"}

    result = apply_foreign_key_references(
        "store_sales", datasource, datasources, script_content, column_mappings
    )

    # Verify result
    assert "import customer as customer;" in result
    # References use concept name, not column alias
    assert "customer.customer_sk" in result
    # Customer SK concept declaration should be removed
    assert "customer_sk" not in result or "import customer" in result
    # Other concept declarations should still be there
    assert "key item_sk int" in result
    assert "key ticket_number int" in result
    customer_file = Renderer().render_statement_string(
        [
            ConceptDeclarationStatement(concept=customer_table_customer_sk_concept),
            customer_datasource,
        ]
    )
    parsed = Environment.from_string(
        result,
        config=EnvironmentConfig(
            import_resolver=DictImportResolver(
                content={
                    "customer": customer_file,
                }
            )
        ),
    )
    assert parsed.datasources["store_sales"].grain.components == {
        "customer.customer_sk",
        "local.item_sk",
        "local.ticket_number",
    }


def test_apply_foreign_key_references_multiple_fks():
    """Test applying multiple FK references to a datasource."""
    # Create test concepts for store_sales table
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

    # Create test datasource for store_sales
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

    # Create customer and store datasources for FK targets
    customer_table_customer_sk = Concept(
        name="customer_sk",
        datatype="int",
        purpose=Purpose.KEY,
    )
    customer_datasource = Datasource(
        name="customer",
        columns=[
            ColumnAssignment(
                alias="c_customer_sk",
                concept=customer_table_customer_sk.reference,
                modifiers=[],
            )
        ],
        address=Address(location="customer", quoted=True),
        grain=Grain(components={"customer_sk"}),
    )

    store_table_store_sk = Concept(
        name="store_sk",
        datatype="int",
        purpose=Purpose.KEY,
    )
    store_datasource = Datasource(
        name="store",
        columns=[
            ColumnAssignment(
                alias="s_store_sk",
                concept=store_table_store_sk.reference,
                modifiers=[],
            )
        ],
        address=Address(location="store", quoted=True),
        grain=Grain(components={"store_sk"}),
    )

    # Create datasources dict with all tables
    datasources = {
        datasource.name: datasource,
        customer_datasource.name: customer_datasource,
        store_datasource.name: store_datasource,
    }

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
        "store_sales", datasource, datasources, script_content, column_mappings
    )

    # Create import resolver with customer and store files
    customer_file = Renderer().render_statement_string(
        [
            ConceptDeclarationStatement(concept=customer_table_customer_sk),
            customer_datasource,
        ]
    )
    store_file = Renderer().render_statement_string(
        [
            ConceptDeclarationStatement(concept=store_table_store_sk),
            store_datasource,
        ]
    )

    # Parse and validate
    parsed = Environment.from_string(
        result,
        config=EnvironmentConfig(
            import_resolver=DictImportResolver(
                content={
                    "customer": customer_file,
                    "store": store_file,
                }
            )
        ),
    )
    rewritten = parsed.datasources["store_sales"]
    assert rewritten.grain.components == {
        "local.item_sk",
    }
    output_addresses = {x.concept.address for x in rewritten.columns}
    assert "customer.customer_sk" in output_addresses
    assert "store.store_sk" in output_addresses
