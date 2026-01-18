"""Tests for join concept injection in query graph generation.

This module tests the `inject_join_concepts` function which identifies concepts
that can serve as join keys between multiple datasources.
"""

from trilogy.core.enums import ConceptSource, Purpose
from trilogy.core.models.core import DataType
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models.author import Metadata
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildGrain
from trilogy.core.processing.node_generators.select_merge_node import (
    inject_join_concepts,
)


def create_test_concept(
    name: str,
    namespace: str = "test",
    purpose: Purpose = Purpose.KEY,
    concept_source: ConceptSource = ConceptSource.MANUAL,
    keys: set[str] | None = None,
) -> BuildConcept:
    """Helper to create a BuildConcept for testing."""
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=purpose,
        build_is_aggregate=False,
        namespace=namespace,
        metadata=Metadata(concept_source=concept_source),
        grain=BuildGrain(),
        keys=keys,
    )


def create_test_datasource(
    name: str,
    concepts: list[BuildConcept],
) -> BuildDatasource:
    """Helper to create a BuildDatasource for testing."""
    from trilogy.core.models.build import BuildColumnAssignment

    columns = [
        BuildColumnAssignment(concept=c, alias=c.name, modifiers=[]) for c in concepts
    ]
    return BuildDatasource(
        name=name,
        columns=columns,
        address=name,
        grain=BuildGrain.from_concepts(
            [c for c in concepts if c.purpose == Purpose.KEY]
        ),
    )


def build_test_graph(
    concepts: list[BuildConcept],
    datasources: list[BuildDatasource],
) -> ReferenceGraph:
    """Build a ReferenceGraph from concepts and datasources for testing."""
    g = ReferenceGraph()

    # Add concepts
    for concept in concepts:
        node = concept_to_node(concept)
        g.concepts[node] = concept
        g.add_node(node)

    # Add datasources and edges
    for ds in datasources:
        ds_node = datasource_to_node(ds)
        g.datasources[ds_node] = ds
        g.add_datasource_node(ds_node, ds)

        for concept in ds.concepts:
            c_node = concept_to_node(concept)
            if c_node not in g.concepts:
                g.concepts[c_node] = concept
                g.add_node(c_node)
            g.add_edge(ds_node, c_node)
            g.add_edge(c_node, ds_node)

    return g


class TestInjectJoinConcepts:
    """Tests for the inject_join_concepts function."""

    def test_no_injection_single_datasource(self):
        """With only one datasource, no join concepts should be injected."""
        # Setup: one datasource with two concepts
        concept_a = create_test_concept("a")
        concept_b = create_test_concept("b", purpose=Purpose.PROPERTY, keys={"test.a"})
        ds1 = create_test_datasource("ds1", [concept_a, concept_b])

        g = build_test_graph([concept_a, concept_b], [ds1])

        # Query only wants concept_b
        relevant_concepts = [concept_to_node(concept_b)]
        relevant_datasets = [datasource_to_node(ds1)]

        result = inject_join_concepts(g, relevant_concepts, relevant_datasets)

        # Should not add anything since there's only one datasource
        assert len(result) == 1
        assert result == relevant_concepts

    def test_injects_shared_concept_between_two_datasources(self):
        """A concept shared between two datasources should be injected as a join key."""
        # Setup: shared key "customer_id" between orders and customers
        customer_id = create_test_concept("customer_id")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_id = create_test_concept("order_id")
        order_total = create_test_concept(
            "order_total", purpose=Purpose.PROPERTY, keys={"test.order_id"}
        )

        ds_customers = create_test_datasource("customers", [customer_id, customer_name])
        ds_orders = create_test_datasource(
            "orders", [order_id, customer_id, order_total]
        )

        g = build_test_graph(
            [customer_id, customer_name, order_id, order_total],
            [ds_customers, ds_orders],
        )

        # Query wants customer_name and order_total (customer_id is the join key)
        relevant_concepts = [
            concept_to_node(customer_name),
            concept_to_node(order_total),
        ]
        relevant_datasets = [
            datasource_to_node(ds_customers),
            datasource_to_node(ds_orders),
        ]

        result = inject_join_concepts(g, relevant_concepts, relevant_datasets)

        # Should inject customer_id as join key
        assert len(result) == 3
        assert concept_to_node(customer_id) in result

    def test_no_injection_when_concept_already_relevant(self):
        """If the shared concept is already in relevant_concepts, don't duplicate it."""
        customer_id = create_test_concept("customer_id")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_id = create_test_concept("order_id")

        ds_customers = create_test_datasource("customers", [customer_id, customer_name])
        ds_orders = create_test_datasource("orders", [order_id, customer_id])

        g = build_test_graph(
            [customer_id, customer_name, order_id],
            [ds_customers, ds_orders],
        )

        # customer_id is already in relevant concepts
        relevant_concepts = [
            concept_to_node(customer_id),
            concept_to_node(customer_name),
        ]
        relevant_datasets = [
            datasource_to_node(ds_customers),
            datasource_to_node(ds_orders),
        ]

        result = inject_join_concepts(g, relevant_concepts, relevant_datasets)

        # Should not duplicate customer_id
        assert len(result) == 2
        assert result == relevant_concepts

    def test_skips_auto_derived_concepts(self):
        """AUTO_DERIVED concepts should not be injected as join keys."""
        # Setup: shared datetime and auto-derived date parts
        created_at = create_test_concept("created_at")
        # Auto-derived concept (e.g., created_at.year)
        created_at_year = create_test_concept(
            "created_at.year",
            purpose=Purpose.PROPERTY,
            concept_source=ConceptSource.AUTO_DERIVED,
            keys={"test.created_at"},
        )
        order_id = create_test_concept("order_id")
        customer_id = create_test_concept("customer_id")

        ds_orders = create_test_datasource(
            "orders", [order_id, created_at, created_at_year, customer_id]
        )
        ds_customers = create_test_datasource(
            "customers", [customer_id, created_at, created_at_year]
        )

        g = build_test_graph(
            [created_at, created_at_year, order_id, customer_id],
            [ds_orders, ds_customers],
        )

        # Query wants order_id and customer_id
        relevant_concepts = [
            concept_to_node(order_id),
            concept_to_node(customer_id),
        ]
        relevant_datasets = [
            datasource_to_node(ds_orders),
            datasource_to_node(ds_customers),
        ]

        result = inject_join_concepts(g, relevant_concepts, relevant_datasets)

        # Should inject created_at but NOT created_at_year (auto-derived)
        assert concept_to_node(created_at) in result
        assert concept_to_node(created_at_year) not in result

    def test_three_datasource_chain(self):
        """Test join injection with three datasources in a chain: A-B-C."""
        # Setup: orders -> line_items -> products
        order_id = create_test_concept("order_id")
        order_date = create_test_concept(
            "order_date", purpose=Purpose.PROPERTY, keys={"test.order_id"}
        )
        line_item_id = create_test_concept("line_item_id")
        product_id = create_test_concept("product_id")
        product_name = create_test_concept(
            "product_name", purpose=Purpose.PROPERTY, keys={"test.product_id"}
        )

        ds_orders = create_test_datasource("orders", [order_id, order_date])
        ds_line_items = create_test_datasource(
            "line_items", [line_item_id, order_id, product_id]
        )
        ds_products = create_test_datasource("products", [product_id, product_name])

        g = build_test_graph(
            [order_id, order_date, line_item_id, product_id, product_name],
            [ds_orders, ds_line_items, ds_products],
        )

        # Query wants order_date and product_name
        relevant_concepts = [
            concept_to_node(order_date),
            concept_to_node(product_name),
        ]
        relevant_datasets = [
            datasource_to_node(ds_orders),
            datasource_to_node(ds_line_items),
            datasource_to_node(ds_products),
        ]

        result = inject_join_concepts(g, relevant_concepts, relevant_datasets)

        # Should inject order_id (between orders and line_items)
        # and product_id (between line_items and products)
        assert concept_to_node(order_id) in result
        assert concept_to_node(product_id) in result

    def test_concept_on_single_datasource_not_injected(self):
        """A concept only on one datasource should not be injected."""
        customer_id = create_test_concept("customer_id")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_id = create_test_concept("order_id")
        order_total = create_test_concept(
            "order_total", purpose=Purpose.PROPERTY, keys={"test.order_id"}
        )
        # internal_code is only on orders, not on customers
        internal_code = create_test_concept(
            "internal_code", purpose=Purpose.PROPERTY, keys={"test.order_id"}
        )

        ds_customers = create_test_datasource("customers", [customer_id, customer_name])
        ds_orders = create_test_datasource(
            "orders", [order_id, customer_id, order_total, internal_code]
        )

        g = build_test_graph(
            [customer_id, customer_name, order_id, order_total, internal_code],
            [ds_customers, ds_orders],
        )

        relevant_concepts = [
            concept_to_node(customer_name),
            concept_to_node(order_total),
        ]
        relevant_datasets = [
            datasource_to_node(ds_customers),
            datasource_to_node(ds_orders),
        ]

        result = inject_join_concepts(g, relevant_concepts, relevant_datasets)

        # Should inject customer_id but NOT internal_code
        assert concept_to_node(customer_id) in result
        assert concept_to_node(internal_code) not in result
