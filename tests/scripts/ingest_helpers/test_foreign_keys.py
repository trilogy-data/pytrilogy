"""Tests for foreign key handling in ingest."""

from trilogy import Environment, EnvironmentConfig
from trilogy.authoring import (
    Comment,
    ConceptDeclarationStatement,
    Datasource,
    DataType,
    DictImportResolver,
    Renderer,
)
from trilogy.core.enums import Modifier, Purpose
from trilogy.core.models.author import Concept, Grain
from trilogy.core.models.datasource import Address, ColumnAssignment
from trilogy.scripts.ingest_helpers.fk_inference import FKBinding
from trilogy.scripts.ingest_helpers.foreign_keys import (
    apply_foreign_key_references,
    parse_foreign_keys,
)


def test_parse_foreign_keys_single():
    """Test parsing a single FK specification."""
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {
        "store_sales": {
            "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=True)
        }
    }


def test_parse_foreign_keys_multiple():
    """Test parsing multiple FK specifications."""
    fk_str = "store_sales.ss_customer_sk:customer.c_customer_sk,store_sales.ss_item_sk:item.i_item_sk"
    result = parse_foreign_keys(fk_str)
    assert result == {
        "store_sales": {
            "ss_customer_sk": FKBinding(
                "customer.c_customer_sk", partial=True
            ),
            "ss_item_sk": FKBinding("item.i_item_sk", partial=True),
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
    assert customer_sk_concept.datatype == DataType.INTEGER
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
    column_mappings = {
        "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=True)
    }

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
        "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=True),
        "ss_store_sk": FKBinding("store.s_store_sk", partial=True),
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
    # FK columns are subsets of their parent's key — both must be partial.
    partial_aliases = {
        c.alias for c in rewritten.columns if Modifier.PARTIAL in c.modifiers
    }
    assert partial_aliases == {"ss_customer_sk", "ss_store_sk"}
    # And it renders with the ~ marker so re-parse round-trips.
    rendered = Renderer().render_statement_string([rewritten])
    assert "ss_customer_sk: ~customer.customer_sk" in rendered
    assert "ss_store_sk: ~store.store_sk" in rendered


def _store_sales_fixture():
    """Minimal store_sales/customer setup used by the partial-flag tests."""
    customer_sk_concept = Concept(
        name="customer_sk", datatype="int", purpose=Purpose.PROPERTY, keys={"item_sk"}
    )
    item_sk_concept = Concept(name="item_sk", datatype="int", purpose=Purpose.KEY)
    datasource = Datasource(
        name="store_sales",
        columns=[
            ColumnAssignment(
                alias="ss_customer_sk",
                concept=customer_sk_concept.reference,
                modifiers=[],
            ),
            ColumnAssignment(
                alias="ss_item_sk", concept=item_sk_concept.reference, modifiers=[]
            ),
        ],
        address=Address(location="store_sales", quoted=True),
        grain=Grain(components={"item_sk"}),
    )
    customer_pk = Concept(name="customer_sk", datatype="int", purpose=Purpose.KEY)
    customer_datasource = Datasource(
        name="customer",
        columns=[
            ColumnAssignment(
                alias="c_customer_sk", concept=customer_pk.reference, modifiers=[]
            )
        ],
        address=Address(location="customer", quoted=True),
        grain=Grain(components={"customer_sk"}),
    )
    datasources = {
        "store_sales": datasource,
        "customer": customer_datasource,
    }
    script_content = [
        Comment(text="# Test datasource"),
        ConceptDeclarationStatement(concept=customer_sk_concept),
        ConceptDeclarationStatement(concept=item_sk_concept),
        datasource,
    ]
    return datasource, datasources, script_content


def test_apply_foreign_key_complete_binding_omits_partial_modifier():
    """A binding marked complete (partial=False) must NOT add ``~``."""
    datasource, datasources, script_content = _store_sales_fixture()
    column_mappings = {
        "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=False)
    }
    result = apply_foreign_key_references(
        "store_sales", datasource, datasources, script_content, column_mappings
    )
    # No partial modifier on the column.
    fk_col = next(c for c in datasource.columns if c.alias == "ss_customer_sk")
    assert Modifier.PARTIAL not in fk_col.modifiers
    # And the rendered text shows a bare reference, not ~customer.customer_sk.
    assert "ss_customer_sk: customer.customer_sk" in result
    assert "~customer.customer_sk" not in result


def test_apply_foreign_key_partial_binding_adds_modifier():
    """A binding marked partial=True must add ``~`` on the column."""
    datasource, datasources, script_content = _store_sales_fixture()
    column_mappings = {
        "ss_customer_sk": FKBinding("customer.c_customer_sk", partial=True)
    }
    result = apply_foreign_key_references(
        "store_sales", datasource, datasources, script_content, column_mappings
    )
    fk_col = next(c for c in datasource.columns if c.alias == "ss_customer_sk")
    assert Modifier.PARTIAL in fk_col.modifiers
    assert "ss_customer_sk: ~customer.customer_sk" in result
