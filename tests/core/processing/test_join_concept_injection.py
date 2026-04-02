"""Tests for reinject_common_join_keys_v2 in common.py.

This module tests the function that identifies concepts shared between
datasources and injects them as join keys into the query graph.
"""

from trilogy.core.enums import Purpose
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildGrain
from trilogy.core.models.core import DataType
from trilogy.core.processing.node_generators.common import reinject_common_join_keys_v2


def create_test_concept(
    name: str,
    namespace: str = "test",
    purpose: Purpose = Purpose.KEY,
    keys: set[str] | None = None,
    pseudonyms: set[str] | None = None,
) -> BuildConcept:
    """Helper to create a BuildConcept for testing."""
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=purpose,
        build_is_aggregate=False,
        namespace=namespace,
        grain=BuildGrain(),
        keys=keys,
        pseudonyms=pseudonyms or set(),
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

    for concept in concepts:
        node = concept_to_node(concept)
        g.concepts[node] = concept
        g.add_node(node)

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


def build_working_graph(
    datasources: list[BuildDatasource],
    relevant_concepts: list[BuildConcept],
) -> ReferenceGraph:
    """Build a working graph that contains only datasources and the relevant concepts.

    Datasources are connected via concept nodes. For prune_and_merge to find
    datasource neighbors, there must be paths between them through concept nodes.
    """
    g = ReferenceGraph()

    for ds in datasources:
        ds_node = datasource_to_node(ds)
        g.datasources[ds_node] = ds
        g.add_node(ds_node)

    for concept in relevant_concepts:
        c_node = concept_to_node(concept)
        g.concepts[c_node] = concept
        g.add_node(c_node)

    # Connect datasources to relevant concepts they provide
    for ds in datasources:
        ds_node = datasource_to_node(ds)
        for concept in ds.concepts:
            c_node = concept_to_node(concept)
            if c_node in g.nodes:
                g.add_edge(ds_node, c_node)
                g.add_edge(c_node, ds_node)

    return g


class TestReinjectCommonJoinKeysV2:
    """Tests for the reinject_common_join_keys_v2 function."""

    def test_no_injection_single_datasource(self):
        """With only one datasource, no join concepts should be injected."""
        concept_a = create_test_concept("a")
        concept_b = create_test_concept("b", purpose=Purpose.PROPERTY, keys={"test.a"})
        ds1 = create_test_datasource("ds1", [concept_a, concept_b])

        orig_g = build_test_graph([concept_a, concept_b], [ds1])
        working_g = build_working_graph([ds1], [concept_b])

        nodelist = [concept_to_node(concept_b)]
        initial_nodes = set(working_g.nodes)

        result = reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        assert result is False
        assert set(working_g.nodes) == initial_nodes

    def test_injects_shared_concept_between_two_datasources(self):
        """A concept shared between two datasources should be injected as a join key.

        For the function to find neighbors, there must be a path between datasources.
        We include a shared concept in both to create that path.
        """
        customer_id = create_test_concept("customer_id")
        # shared_key exists in both datasources and is already in the working graph
        shared_key = create_test_concept("shared_key")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_id = create_test_concept("order_id")
        order_total = create_test_concept(
            "order_total", purpose=Purpose.PROPERTY, keys={"test.order_id"}
        )

        ds_customers = create_test_datasource(
            "customers", [customer_id, customer_name, shared_key]
        )
        ds_orders = create_test_datasource(
            "orders", [order_id, customer_id, order_total, shared_key]
        )

        orig_g = build_test_graph(
            [customer_id, customer_name, order_id, order_total, shared_key],
            [ds_customers, ds_orders],
        )
        # shared_key creates a path between the two datasources
        working_g = build_working_graph(
            [ds_customers, ds_orders], [customer_name, order_total, shared_key]
        )

        nodelist = [
            concept_to_node(customer_name),
            concept_to_node(order_total),
            concept_to_node(shared_key),
        ]

        result = reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        # customer_id should be injected as an additional join key
        # The function uses with_default_grain() when creating nodes
        assert result is True
        assert concept_to_node(customer_id.with_default_grain()) in working_g.nodes

    def test_no_injection_when_concept_already_in_graph(self):
        """If the shared concept is already in the working graph, don't inject.

        The function checks using with_default_grain(), so for concepts to be
        considered 'already present', they need to be added with that grain.
        """
        customer_id = create_test_concept("customer_id")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )

        ds_customers = create_test_datasource("customers", [customer_id, customer_name])
        ds_orders = create_test_datasource("orders", [customer_id])

        orig_g = build_test_graph(
            [customer_id, customer_name],
            [ds_customers, ds_orders],
        )
        # Build working graph with customer_id.with_default_grain() to match
        # how the function checks for existing nodes
        working_g = build_working_graph(
            [ds_customers, ds_orders],
            [customer_id.with_default_grain(), customer_name],
        )

        nodelist = [
            concept_to_node(customer_id.with_default_grain()),
            concept_to_node(customer_name),
        ]
        initial_node_count = len(working_g.nodes)

        reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        # No new nodes should be added since customer_id is already present
        assert len(working_g.nodes) == initial_node_count

    def test_skips_synonyms(self):
        """Concepts in the synonyms set should not be injected.

        We test that customer_id (the only shared KEY besides shared_key) is
        skipped when it's in the synonyms set.
        """
        customer_id = create_test_concept("customer_id")
        shared_key = create_test_concept("shared_key")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_total = create_test_concept(
            "order_total", purpose=Purpose.PROPERTY, keys={"test.shared_key"}
        )

        ds_customers = create_test_datasource(
            "customers", [customer_id, customer_name, shared_key]
        )
        ds_orders = create_test_datasource(
            "orders", [customer_id, order_total, shared_key]
        )

        orig_g = build_test_graph(
            [customer_id, customer_name, order_total, shared_key],
            [ds_customers, ds_orders],
        )
        # shared_key (with default grain) creates the path between datasources
        working_g = build_working_graph(
            [ds_customers, ds_orders],
            [customer_name, order_total, shared_key.with_default_grain()],
        )

        nodelist = [
            concept_to_node(customer_name),
            concept_to_node(order_total),
            concept_to_node(shared_key.with_default_grain()),
        ]
        # Mark customer_id as a synonym to skip - shared_key is already present
        synonyms = {customer_id.address}

        result = reinject_common_join_keys_v2(orig_g, working_g, nodelist, synonyms)

        # customer_id should NOT be injected because it's in synonyms
        # shared_key is already present so won't be re-injected
        assert result is False
        assert concept_to_node(customer_id.with_default_grain()) not in working_g.nodes

    def test_skips_pseudonym_duplicates(self):
        """If a candidate has an existing address in its pseudonyms, skip injection.

        The function checks `any(x in concrete.pseudonyms for x in existing_addresses)`.
        So if customer_id (the candidate) has customer_key in its pseudonyms, and
        customer_key is already in the graph, customer_id won't be injected.
        """
        # customer_id has customer_key as a pseudonym
        customer_id = create_test_concept(
            "customer_id", pseudonyms={"test.customer_key"}
        )
        customer_key = create_test_concept("customer_key")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )

        ds_customers = create_test_datasource(
            "customers", [customer_id, customer_key, customer_name]
        )
        ds_orders = create_test_datasource("orders", [customer_id, customer_key])

        orig_g = build_test_graph(
            [customer_id, customer_key, customer_name],
            [ds_customers, ds_orders],
        )
        # customer_key (with default grain) is already in working graph
        working_g = build_working_graph(
            [ds_customers, ds_orders],
            [customer_key.with_default_grain(), customer_name],
        )

        nodelist = [
            concept_to_node(customer_key.with_default_grain()),
            concept_to_node(customer_name),
        ]

        initial_node_count = len(working_g.nodes)
        reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        # customer_id should not be injected because it has customer_key
        # (already present) in its pseudonyms
        assert concept_to_node(customer_id.with_default_grain()) not in working_g.nodes
        assert len(working_g.nodes) == initial_node_count

    def test_three_datasource_chain(self):
        """Test join injection with three datasources in a chain: A-B-C.

        For this test, we need to already have some shared concepts to create
        paths between datasources. We'll include order_id between orders and
        line_items, and product_id between line_items and products.
        """
        order_id = create_test_concept("order_id")
        order_date = create_test_concept(
            "order_date", purpose=Purpose.PROPERTY, keys={"test.order_id"}
        )
        line_item_id = create_test_concept("line_item_id")
        product_id = create_test_concept("product_id")
        product_name = create_test_concept(
            "product_name", purpose=Purpose.PROPERTY, keys={"test.product_id"}
        )
        # extra_key is shared between orders and line_items
        extra_order_key = create_test_concept("extra_order_key")
        # extra_product_key is shared between line_items and products
        extra_product_key = create_test_concept("extra_product_key")

        ds_orders = create_test_datasource(
            "orders", [order_id, order_date, extra_order_key]
        )
        ds_line_items = create_test_datasource(
            "line_items",
            [line_item_id, order_id, product_id, extra_order_key, extra_product_key],
        )
        ds_products = create_test_datasource(
            "products", [product_id, product_name, extra_product_key]
        )

        orig_g = build_test_graph(
            [
                order_id,
                order_date,
                line_item_id,
                product_id,
                product_name,
                extra_order_key,
                extra_product_key,
            ],
            [ds_orders, ds_line_items, ds_products],
        )
        # Include keys that create paths between datasources
        working_g = build_working_graph(
            [ds_orders, ds_line_items, ds_products],
            [order_date, product_name, extra_order_key, extra_product_key],
        )

        nodelist = [
            concept_to_node(order_date),
            concept_to_node(product_name),
            concept_to_node(extra_order_key),
            concept_to_node(extra_product_key),
        ]

        result = reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        # Should inject order_id (between orders and line_items)
        # and product_id (between line_items and products)
        assert result is True
        assert concept_to_node(order_id.with_default_grain()) in working_g.nodes
        assert concept_to_node(product_id.with_default_grain()) in working_g.nodes

    def test_concept_on_single_datasource_not_injected(self):
        """A concept only on one datasource should not be injected."""
        customer_id = create_test_concept("customer_id")
        shared_key = create_test_concept("shared_key")
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_total = create_test_concept(
            "order_total", purpose=Purpose.PROPERTY, keys={"test.shared_key"}
        )
        # internal_code is only on orders, not on customers
        internal_code = create_test_concept("internal_code")

        ds_customers = create_test_datasource(
            "customers", [customer_id, customer_name, shared_key]
        )
        ds_orders = create_test_datasource(
            "orders", [customer_id, order_total, internal_code, shared_key]
        )

        orig_g = build_test_graph(
            [customer_id, customer_name, order_total, internal_code, shared_key],
            [ds_customers, ds_orders],
        )
        # shared_key creates the path between datasources
        working_g = build_working_graph(
            [ds_customers, ds_orders], [customer_name, order_total, shared_key]
        )

        nodelist = [
            concept_to_node(customer_name),
            concept_to_node(order_total),
            concept_to_node(shared_key),
        ]

        result = reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        # Should inject customer_id but NOT internal_code (only on one ds)
        assert result is True
        assert concept_to_node(customer_id.with_default_grain()) in working_g.nodes
        assert (
            concept_to_node(internal_code.with_default_grain()) not in working_g.nodes
        )

    def test_only_injects_grain_components(self):
        """Only concepts that are part of the grain should be injected."""
        customer_id = create_test_concept("customer_id")
        shared_key = create_test_concept("shared_key")
        # customer_status is shared but is a property, not a key
        customer_status = create_test_concept(
            "customer_status", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        customer_name = create_test_concept(
            "customer_name", purpose=Purpose.PROPERTY, keys={"test.customer_id"}
        )
        order_total = create_test_concept(
            "order_total", purpose=Purpose.PROPERTY, keys={"test.shared_key"}
        )

        ds_customers = create_test_datasource(
            "customers", [customer_id, customer_name, customer_status, shared_key]
        )
        ds_orders = create_test_datasource(
            "orders", [customer_id, order_total, customer_status, shared_key]
        )

        orig_g = build_test_graph(
            [customer_id, customer_name, order_total, customer_status, shared_key],
            [ds_customers, ds_orders],
        )
        # shared_key creates the path between datasources
        working_g = build_working_graph(
            [ds_customers, ds_orders], [customer_name, order_total, shared_key]
        )

        nodelist = [
            concept_to_node(customer_name),
            concept_to_node(order_total),
            concept_to_node(shared_key),
        ]

        reinject_common_join_keys_v2(orig_g, working_g, nodelist, set())

        # customer_id should be injected (it's a key)
        assert concept_to_node(customer_id.with_default_grain()) in working_g.nodes
        # customer_status should NOT be injected (it's a property, not in grain)
        assert (
            concept_to_node(customer_status.with_default_grain()) not in working_g.nodes
        )
