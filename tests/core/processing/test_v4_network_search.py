from trilogy import Environment
from trilogy.core.enums import ComparisonOperator
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import BuildComparison, BuildWhereClause
from trilogy.core.processing.v4_helper import network_obligations as nob
from trilogy.core.processing.v4_helper import network_search as ns
from trilogy.core.processing.v4_helper import network_topology as nt
from trilogy.core.processing.v4_helper.network_build import build_source_network
from trilogy.core.processing.v4_helper.network_model import (
    BindingStrength,
    ConditionFit,
    Obligation,
    ObligationKind,
    SearchLimit,
    SolutionCost,
    SourceNetwork,
)
from trilogy.core.processing.v4_helper.network_search import (
    plan_network_sources,
    search_sources,
)


def _forward_reach(network: SourceNetwork, source: str) -> frozenset[str]:
    """Every candidate a pure lookup CHAIN off `source`'s own keys can reach —
    the transitive closure of `functional_into`. A composition of lookups can
    label or restrict the origin's rows but never multiply them, so a chain is
    exactly as safe as one hop (a snowflake path fact -> dim -> subdim -> dim
    is one labeling, three hops) — provided every INTERMEDIATE is row-complete;
    a row-partial node joins the reach as a terminus only.

    The search asks the REVERSE question (`SourceNetwork.chain_completers`),
    which is one walk back from the full binders rather than one closure per
    candidate. This forward form is the definition the two must agree on, and
    lives here because only these tests read it."""
    seen: set[str] = set()
    frontier = [source]
    while frontier:
        for node in network.functional_successors(frontier.pop()):
            if node in seen or node == source:
                continue
            seen.add(node)
            if network.row_complete(node):
                frontier.append(node)
    return frozenset(seen)


BRIDGE_MODEL = """
key customer_id int;
key item_id int;
key order_number int;
key ticket_number int;
property <order_number, item_id>.catalog_quantity int;

datasource catalog_sales (
    customer_id: customer_id,
    item_id: item_id,
    order_number: order_number,
    catalog_quantity: catalog_quantity,
)
grain (order_number, item_id)
query '''select 1 customer_id, 10 item_id, 100 order_number, 5 catalog_quantity''';

datasource store_sales (
    customer_id: customer_id,
    item_id: item_id,
    ticket_number: ticket_number,
)
grain (ticket_number, item_id)
query '''select 1 customer_id, 10 item_id, 200 ticket_number''';
"""

# q96 shape: the returns fact binds both keys only partially, and the sales fact
# binds both fully — the returns scan is a redundant connector.
TWIN_SCAN_MODEL = """
key ticket_number int;
key item_id int;
key customer_id int;
property <ticket_number, item_id>.quantity int;

datasource store_sales (
    ticket_number: ticket_number,
    item_id: item_id,
    customer_id: customer_id,
    quantity: quantity,
)
grain (ticket_number, item_id)
query '''select 1 ticket_number, 10 item_id, 100 customer_id, 5 quantity''';

datasource store_returns (
    ticket_number: ~ticket_number,
    item_id: ~item_id,
)
grain (ticket_number, item_id)
query '''select 1 ticket_number, 10 item_id''';
"""

PARTIAL_DIMENSION_MODEL = """
key launch_id int;
key vehicle_id int;
property vehicle_id.vehicle_name string;

datasource launch_info (
    launch_id: launch_id,
    vehicle_id: vehicle_id,
    vehicle_name: ~vehicle_name,
)
grain (launch_id)
query '''select 1 launch_id, 2 vehicle_id, 'falcon' vehicle_name''';

datasource vehicle_info (
    vehicle_id: vehicle_id,
    vehicle_name: vehicle_name,
)
grain (vehicle_id)
query '''select 2 vehicle_id, 'falcon' vehicle_name''';
"""

# Two islands sharing no key anywhere in the pool: nothing can ever join them,
# so a request spanning both is decided by the split certificate, not a budget.
SPLIT_POOL_MODEL = """
key order_number int;
property order_number.order_total float;
key session_id int;
property session_id.page_views int;

datasource orders (
    order_number: order_number,
    order_total: order_total,
)
grain (order_number)
query '''select 100 order_number, 5.0 order_total''';

datasource sessions (
    session_id: session_id,
    page_views: page_views,
)
grain (session_id)
query '''select 7 session_id, 3 page_views''';
"""

PARTITION_UNION_MODEL = """
key sales_channel enum<string>['WEB', 'CATALOG'];
key order_id int;
key item_id int;
property <order_id, sales_channel, item_id>.ext_sales_price float?;

partial datasource web_sales (
    raw(''' 'WEB' '''): sales_channel,
    order_id: order_id,
    item_id: item_id,
    ext_sales_price: ext_sales_price,
)
grain (order_id, sales_channel, item_id)
complete where sales_channel = 'WEB'
query '''select 100 order_id, 1 item_id, 20.0 ext_sales_price''';

partial datasource catalog_sales (
    raw(''' 'CATALOG' '''): sales_channel,
    order_id: order_id,
    item_id: item_id,
    ext_sales_price: ext_sales_price,
)
grain (order_id, sales_channel, item_id)
complete where sales_channel = 'CATALOG'
query '''select 200 order_id, 1 item_id, 10.0 ext_sales_price''';
"""

# q29/q84 shape: is_returned is an expression over a column, not a column.
DERIVED_TERMINAL_MODEL = """
key ticket_number int;
key item_id int;
property <ticket_number, item_id>._returned_ticket int?;
auto is_returned <- _returned_ticket is not null;

datasource store_sales (
    ticket_number: ticket_number,
    item_id: item_id,
    _returned_ticket: _returned_ticket,
)
grain (ticket_number, item_id)
query '''select 1 ticket_number, 10 item_id, null _returned_ticket''';
"""

# q05 shape: the dimension's own grain key sits on a candidate the cheaper cover
# never picks, so the dimension attaches to the fact side through the 3-valued
# `channel` discriminator instead.
DISCRIMINATOR_JOIN_MODEL = """
key channel string;
key site_id int;
property site_id.site_name string;
key order_id int;
property <channel, order_id>.return_amount float?;

datasource returns_facts (
    channel: channel,
    order_id: order_id,
    return_amount: return_amount,
)
grain (channel, order_id)
query '''select 'WEB' channel, 1 order_id, 5.0 return_amount''';

datasource return_sites (
    channel: channel,
    order_id: order_id,
    site_id: site_id,
)
grain (channel, order_id)
query '''select 'WEB' channel, 1 order_id, 7 site_id''';

datasource sites (
    channel: channel,
    site_id: site_id,
    site_name: site_name,
)
grain (channel, site_id)
query '''select 'WEB' channel, 7 site_id, 'main' site_name''';
"""

# q17/q25 shape: the authored join relates the two dimensions' shared business
# key, which neither fact binds, and the facts ALSO share `sku` — a cheaper path
# that drops the authored equality.
AUTHORED_HOP_MODEL = """
key sku int;
key a_cust_sk int;
property a_cust_sk.a_cust_id string;
key b_cust_sk int;
property b_cust_sk.b_cust_id string;
key a_order int;
property a_order.a_amount int;
key b_order int;
property b_order.b_amount int;

datasource a_customers (sk: a_cust_sk, cid: a_cust_id) grain (a_cust_sk)
query '''select 1 sk, 'C1' cid''';
datasource b_customers (sk: b_cust_sk, cid: b_cust_id) grain (b_cust_sk)
query '''select 10 sk, 'C1' cid''';
datasource a_facts (o: a_order, cust: a_cust_sk, sku: sku, amt: a_amount)
grain (a_order) query '''select 1 o, 1 cust, 100 sku, 1 amt''';
datasource b_facts (o: b_order, cust: b_cust_sk, sku: sku, amt: b_amount)
grain (b_order) query '''select 1 o, 10 cust, 100 sku, 100 amt''';

merge a_cust_id into ~b_cust_id;
"""

FILTERED_AGGREGATE_MODEL = """
key order_id int;
key customer_id int;
property order_id.status string;
auto order_count <- count(order_id);

datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    status: status,
)
grain (order_id)
query '''select 1 order_id, 10 customer_id, 'open' status''';

datasource customer_summary (
    customer_id: customer_id,
    order_count: order_count,
)
grain (customer_id)
query '''select 10 customer_id, 3 order_count''';
"""


DERIVED_MERGE_MODEL = """
key l_id int;
property l_id.l_key int;
property l_id.l_val int;
datasource lsrc (i: l_id, k: l_key, v: l_val) grain (l_id)
query '''select 1 i, 1 k, 1 v''';

key r_id int;
property r_id.r_key int;
property r_id.r_val int;
datasource rsrc (i: r_id, k: r_key, v: r_val) grain (r_id)
query '''select 1 i, 1 k, 100 v''';

auto ka <- l_key + 1;
auto kb <- r_key + 1;
merge ka into kb;
"""

RECURSIVE_MERGE_MODEL = """
key id int;
property id.parent int;
key pid int;
property pid.plabel string;

datasource edges (id: id, parent: parent) grain (id)
query '''select 1 as id, null as parent union all select 2, 1''';

datasource pnodes (pid: pid, plabel: plabel) grain (pid)
query '''select 1 as pid, 'A' as plabel''';

auto first_parent <- recurse_edge(id, parent);
merge first_parent into pid;
"""


def _build(model: str):
    env = Environment()
    env.parse(model)
    benv = env.materialize_for_select()
    return benv, generate_graph(benv)


def _terminals(benv, *addresses):
    return [benv.concepts[address] for address in addresses]


class TestNetworkLabels:
    def test_bindings_are_labeled_full_or_partial(self):
        benv, graph = _build(TWIN_SCAN_MODEL)
        network = build_source_network(
            _terminals(benv, "local.ticket_number", "local.item_id"), benv, graph
        )

        sales = network.candidates["ds~store_sales"]
        returns = network.candidates["ds~store_returns"]
        assert sales.bindings["local.ticket_number"].strength is BindingStrength.FULL
        assert (
            returns.bindings["local.ticket_number"].strength is BindingStrength.PARTIAL
        )
        assert sales.bindings["local.ticket_number"].stored
        assert sales.binds_fully("local.ticket_number")
        assert not returns.binds_fully("local.ticket_number")

    def test_condition_fit_applies_when_source_binds_every_filter_column(self):
        benv, graph = _build(FILTERED_AGGREGATE_MODEL)
        conditions = BuildWhereClause(
            conditional=BuildComparison(
                left=benv.concepts["local.status"],
                right="open",
                operator=ComparisonOperator.EQ,
            )
        )
        network = build_source_network(
            _terminals(benv, "local.customer_id"), benv, graph, conditions
        )

        assert network.candidates["ds~orders"].condition is ConditionFit.NEUTRAL
        # the summary's aggregate is invalidated by a filter it cannot apply
        assert "ds~customer_summary" not in network.candidates

    def test_terminals_drop_single_row_concepts(self):
        benv, graph = _build(BRIDGE_MODEL)
        network = build_source_network(
            _terminals(benv, "local.customer_id", "local.item_id"), benv, graph
        )

        assert set(network.terminals) == {"local.customer_id", "local.item_id"}


class TestNetworkSearch:
    def test_single_source_cover_wins_over_a_join(self):
        benv, graph = _build(BRIDGE_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.item_id", "local.catalog_quantity"), benv, graph
        )

        assert result.solution is not None
        assert result.solution.sources == ("ds~catalog_sales",)

    def test_cover_spanning_two_sources_records_its_join_keys(self):
        benv, graph = _build(BRIDGE_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.catalog_quantity", "local.ticket_number"),
            benv,
            graph,
        )

        assert result.solution is not None
        assert result.solution.sources == ("ds~catalog_sales", "ds~store_sales")
        keys = result.solution.join_keys[("ds~catalog_sales", "ds~store_sales")]
        assert {"local.customer_id", "local.item_id"} <= keys
        assert result.solution.connectors

    def test_redundant_partial_connector_is_dominated(self):
        benv, graph = _build(TWIN_SCAN_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.ticket_number", "local.item_id", "local.quantity"),
            benv,
            graph,
        )

        assert result.solution is not None
        assert result.solution.sources == ("ds~store_sales",)
        assert not result.solution.partial_terminals

    def test_partial_terminal_reports_the_completion_join_it_implies(self):
        benv, graph = _build(PARTIAL_DIMENSION_MODEL)
        network = build_source_network(
            _terminals(benv, "local.launch_id", "local.vehicle_name"), benv, graph
        )
        launch_only = network.candidates["ds~launch_info"]

        assert launch_only.bindings["local.vehicle_name"].partial
        result = search_sources(network)
        assert result.solution is not None
        # the full binder is preferred outright, so nothing is left to complete
        assert result.solution.sources == ("ds~launch_info", "ds~vehicle_info")
        assert not result.solution.partial_terminals
        assert not result.solution.completions

    def test_whole_population_request_picks_the_union_over_one_arm(self):
        benv, graph = _build(PARTITION_UNION_MODEL)
        result = plan_network_sources(
            _terminals(
                benv, "local.sales_channel", "local.order_id", "local.ext_sales_price"
            ),
            benv,
            graph,
        )

        assert result.solution is not None
        assert result.solution.sources == ("ds~web_sales-catalog_sales",)
        assert not result.solution.partial_terminals

    def test_implying_condition_makes_one_arm_authoritative(self):
        benv, graph = _build(PARTITION_UNION_MODEL)
        conditions = BuildWhereClause(
            conditional=BuildComparison(
                left=benv.concepts["local.sales_channel"],
                right="WEB",
                operator=ComparisonOperator.EQ,
            )
        )
        network = build_source_network(
            _terminals(benv, "local.sales_channel", "local.order_id"),
            benv,
            graph,
            conditions,
        )

        web = network.candidates["ds~web_sales"]
        assert web.condition is ConditionFit.IMPLIED_EXACT
        assert web.binds_fully("local.sales_channel")

    def test_unreachable_terminal_is_reported_not_guessed(self):
        benv, graph = _build(BRIDGE_MODEL)
        network = build_source_network(
            _terminals(benv, "local.customer_id"), benv, graph
        )
        network.candidates.clear()

        result = search_sources(network)

        assert result.solution is None
        assert result.unreachable == frozenset({"local.customer_id"})

    def test_split_pool_declines_with_a_certificate_not_a_budget(self):
        benv, graph = _build(SPLIT_POOL_MODEL)
        network = build_source_network(
            _terminals(benv, "local.order_total", "local.page_views"), benv, graph
        )

        result = search_sources(network)

        assert result.solution is None
        assert result.split == frozenset({"local.page_views"}) or result.split == (
            frozenset({"local.order_total"})
        )
        assert result.limit is None  # a proof, not an exhausted budget

    def test_split_pool_request_within_one_island_still_resolves(self):
        benv, graph = _build(SPLIT_POOL_MODEL)
        network = build_source_network(
            _terminals(benv, "local.order_number", "local.order_total"), benv, graph
        )

        result = search_sources(network)

        assert result.solution is not None
        assert not result.split

    def test_redundant_source_is_dropped_not_merely_costed(self):
        benv, graph = _build(TWIN_SCAN_MODEL)
        network = build_source_network(
            _terminals(benv, "local.ticket_number", "local.item_id"), benv, graph
        )

        result = search_sources(network)

        assert result.solution is not None
        # a second scan binding nothing new is an inner join onto a narrower
        # population — reduction must drop it, not merely cost it
        assert result.solution.sources == ("ds~store_sales",)

    def test_fanout_is_judged_on_what_the_source_provides(self):
        benv, graph = _build(TWIN_SCAN_MODEL)
        network = build_source_network(
            _terminals(benv, "local.customer_id"), benv, graph
        )
        sales = "ds~store_sales"

        # store_sales is (ticket, item) grain, so reading customer_id off it fans
        # out - and joining store_returns on the ticket key must not hide that
        assert network.fans_out(sales, frozenset({"local.customer_id"}))
        assert network.fans_out(
            sales, frozenset({"local.customer_id", "local.ticket_number"})
        )
        result = search_sources(network)
        assert result.solution is not None
        assert result.solution.cost.fanout_sources == 1

    def test_derived_terminal_over_sourced_columns_is_not_a_sourcing_requirement(self):
        benv, graph = _build(DERIVED_TERMINAL_MODEL)
        terminals = _terminals(
            benv, "local.ticket_number", "local.is_returned", "local._returned_ticket"
        )
        network = build_source_network(terminals, benv, graph)

        assert "local.is_returned" not in network.terminals
        result = search_sources(network)
        assert result.unreachable == frozenset()
        assert result.solution is not None
        assert result.solution.sources == ("ds~store_sales",)

    def test_derived_terminal_whose_parent_is_unrequested_stays_a_requirement(self):
        benv, graph = _build(DERIVED_TERMINAL_MODEL)
        network = build_source_network(
            _terminals(benv, "local.is_returned"), benv, graph
        )

        # nothing asks for _returned_ticket, so is_returned is NOT decomposable
        # away — the requirement is retained, spelled as its canonical class
        # (the scan's graph edge emits the `_virt_comp_*` spelling; q84's
        # request spells `.address` — same concept, one class)
        canonical = benv.concepts["local.is_returned"].canonical_address
        assert network.terminals == (canonical,)
        result = search_sources(network)

        # ...and the scan that derives it inline from its own bound column
        # satisfies it (the ladder planned this same single-scan read)
        assert result.solution is not None
        assert result.solution.sources == ("ds~store_sales",)
        assert result.unreachable == frozenset()

    def test_blend_join_is_paid_when_nothing_can_avoid_it(self):
        benv, graph = _build(BRIDGE_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.catalog_quantity", "local.ticket_number"),
            benv,
            graph,
        )

        # the canonical fact-to-fact conformed-dimension blend: shared
        # {customer_id, item_id} covers NEITHER grain, and no candidate can do
        # better, so it prices at 1 and still plans
        assert result.solution is not None
        assert result.solution.sources == ("ds~catalog_sales", "ds~store_sales")
        assert result.solution.cost.blend_joins == 1

    def test_functional_lookup_is_not_a_blend(self):
        benv, graph = _build(PARTIAL_DIMENSION_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.launch_id", "local.vehicle_name"), benv, graph
        )

        assert result.solution is not None
        assert result.solution.cost.blend_joins == 0

    def test_source_carrying_the_key_onto_the_fact_side_survives_reduction(self):
        benv, graph = _build(DISCRIMINATOR_JOIN_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.return_amount", "local.site_id", "local.site_name"),
            benv,
            graph,
        )

        # return_sites provides no VALUE the sites dimension does not, but it is
        # the only source putting site_id on the fact side; without it the
        # dimension attaches through `channel` alone
        assert result.solution is not None
        assert result.solution.sources == (
            "ds~return_sites",
            "ds~returns_facts",
            "ds~sites",
        )
        assert result.solution.cost.blend_joins == 0

    def test_declared_relation_is_materialized_on_both_sides(self):
        benv, graph = _build(AUTHORED_HOP_MODEL)
        network = build_source_network(
            _terminals(
                benv,
                "local.a_amount",
                "local.a_cust_sk",
                "local.b_amount",
                "local.b_cust_sk",
                "local.b_cust_id",
            ),
            benv,
            graph,
        )
        assert network.join_requirements

        result = search_sources(network)

        # one dimension scan binds the merged key and covers it, but leaves the
        # far fact with no way to produce it — both hops are required
        assert result.solution is not None
        assert result.solution.sources == (
            "ds~a_customers",
            "ds~a_facts",
            "ds~b_customers",
            "ds~b_facts",
        )
        assert result.solution.cost.unpaired_join_keys == 0

    def test_derived_merge_key_unifies_through_graph_pseudonyms(self):
        """After `merge ka into kb` both env concepts carry the surviving
        side's lineage; each side's own variant exists only under its canonical
        (`_virt_*`) address, related by the GRAPH's pseudonym edges. Without
        feeding those into the equivalence map the two scans share no key and
        the cover disconnects (the join_matrix derived cells)."""
        benv, graph = _build(DERIVED_MERGE_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.l_id", "local.l_val", "local.r_id", "local.r_val"),
            benv,
            graph,
        )

        assert result.solution is not None
        assert result.solution.sources == ("ds~lsrc", "ds~rsrc")
        keys = result.solution.join_keys[("ds~lsrc", "ds~rsrc")]
        assert keys, "derived merge key did not become a join axis"

    def test_non_basic_merge_origin_becomes_a_connector_candidate(self):
        """A merge key with a RECURSIVE origin is emitted by no scan, so the
        sides it relates share no binding — the one capability the ladder's
        lineage-walking Steiner had. The origin's subplan contract (the key
        plus its grain keys) is offered as a `connector~` candidate instead."""
        benv, graph = _build(RECURSIVE_MERGE_MODEL)
        network = build_source_network(
            _terminals(benv, "local.id", "local.plabel"), benv, graph
        )
        assert "connector~local.first_parent" in network.candidates

        result = search_sources(network)

        assert result.solution is not None
        assert "connector~local.first_parent" in result.solution.sources
        assert any(node.startswith("ds~") for node in result.solution.sources)

    def test_solution_is_deterministic_across_runs(self):
        benv, graph = _build(BRIDGE_MODEL)
        terminals = _terminals(
            benv, "local.catalog_quantity", "local.ticket_number", "local.customer_id"
        )
        first = plan_network_sources(terminals, benv, graph)
        second = plan_network_sources(terminals, benv, graph)

        assert first.solution is not None and second.solution is not None
        assert first.solution.sources == second.solution.sources
        assert first.solution.assignments == second.solution.assignments


# Shared-dimension diamond: week_seq reaches sales through sold_dates and
# inventory through inv_dates. A cover keeping ONE date dimension joins the
# facts on item alone and fans out.
DIAMOND_MODEL = """
key week_seq int;

key sold_date_id int;
property sold_date_id.year int;
datasource sold_dates (sdid: sold_date_id, wsk: week_seq, yr: year)
grain (sold_date_id) query '''select 10 sdid, 100 wsk, 1999 yr''';

key inv_date_id int;
datasource inv_dates (idid: inv_date_id, wsk: week_seq)
grain (inv_date_id) query '''select 30 idid, 100 wsk''';

key item_id int;
key order_id int;
property order_id.qty int;
datasource sales (oid: order_id, iid: item_id, sdid: sold_date_id, q: qty)
grain (order_id) query '''select 1 oid, 1 iid, 10 sdid, 5 q''';

key wh_id int;
property <item_id, inv_date_id, wh_id>.qoh int;
datasource inventory (iid: item_id, idid: inv_date_id, wid: wh_id, q: qoh)
grain (item_id, inv_date_id, wh_id)
query '''select 1 iid, 30 idid, 1 wid, 3 q''';
"""


class TestDiamondLookups:
    def test_shared_terminal_labels_each_fact_through_its_own_lookup(self):
        """A requested terminal both facts can reach through their OWN lookup
        dimension must be materialized on both sides — one date dimension per
        fact — or the dropped side inherits it through the fact-to-fact meet
        (item alone) and fans out."""
        benv, graph = _build(DIAMOND_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.week_seq", "local.qty", "local.qoh"),
            benv,
            graph,
        )

        assert result.solution is not None
        assert {"ds~inv_dates", "ds~sold_dates"} <= set(result.solution.sources)

    def test_unsuppliable_terminal_stays_a_legal_blend(self):
        """BRIDGE_MODEL's fact-to-fact blend: no candidate supplies a lookup
        from the catalog fact to the store fact's ticket key, so the diamond
        rule must add nothing and the blend still plans at cost 1."""
        benv, graph = _build(BRIDGE_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.catalog_quantity", "local.ticket_number"),
            benv,
            graph,
        )

        assert result.solution is not None
        assert result.solution.sources == ("ds~catalog_sales", "ds~store_sales")


# Coalescing (`union join`) axis families are declared per QUERY, so the
# network is captured from a real build rather than constructed directly.
COALESCING_ARMS_MODEL = """
key sid int;
property sid.s_cust int;
datasource store_fact (r: sid, c: s_cust) grain (sid)
query '''select 1 r, 1 c union all select 2 r, 2 c''';

key cid int;
property cid.c_cust int;
datasource catalog_fact (r: cid, c: c_cust) grain (cid)
query '''select 1 r, 1 c union all select 2 r, 3 c''';
"""


def _captured_searches(monkeypatch, model: str, query: str):
    """Run a v4 build of `query` and capture every (network, result) pair the
    planner searched."""
    import trilogy.core.processing.v4_helper.source_planning as sp
    from trilogy.core.processing.v4_helper.network_search import (
        search_sources as real_search,
    )
    from trilogy.dialect.duckdb import DuckDBDialect

    captured = []

    def capturing_search(network):
        result = real_search(network)
        captured.append((network, result))
        return result

    monkeypatch.setattr(sp, "search_sources", capturing_search)
    env = Environment()
    env.parse(model)
    DuckDBDialect().generate_queries(env, env.parse(query)[1])
    return captured


# Two facts sharing no key at all. `bridge` merges the pieces in one hop;
# `hop_one`/`hop_two` only via a mid key, so the path must be built up hop by
# hop through the obligation fixpoint.
DISCONNECTED_FACTS_MODEL = """
key a_id int;
key shared_key int;
property a_id.a_val int;
datasource fact_a (i: a_id, k: shared_key, v: a_val) grain (a_id)
query '''select 1 i, 1 k, 10 v''';

key b_id int;
key other_key int;
property b_id.b_val int;
datasource fact_b (i: b_id, k: other_key, v: b_val) grain (b_id)
query '''select 1 i, 1 k, 20 v''';
"""

ONE_HOP_BRIDGE = """
datasource bridge (sk: shared_key, ok: other_key) grain (shared_key, other_key)
query '''select 1 sk, 1 ok''';
"""

TWO_HOP_BRIDGE = """
key mid_key int;
datasource hop_one (sk: shared_key, m: mid_key) grain (shared_key, mid_key)
query '''select 1 sk, 1 m''';
datasource hop_two (m: mid_key, ok: other_key) grain (mid_key, other_key)
query '''select 1 m, 1 ok''';
"""

# A chain fact -> vehicles -> makers whose MIDDLE node is row-partial: it binds
# its own grain key only partially, so a lookup routed through it drops rows.
ROW_PARTIAL_CHAIN_MODEL = """
key launch_id int;
key vehicle_id int;
key maker_id int;
property maker_id.maker_name string;

datasource launches (l: launch_id, v: vehicle_id) grain (launch_id)
query '''select 1 l, 2 v''';

datasource vehicles (v: ~vehicle_id, m: maker_id) grain (vehicle_id)
query '''select 2 v, 3 m''';

datasource makers (m: maker_id, n: maker_name) grain (maker_id)
query '''select 3 m, 'spacex' n''';
"""


class TestConnectivityObligation:
    def test_disconnected_cover_is_bridged_by_the_candidate_that_merges_it(self):
        """Two facts sharing no key are not an answer. The `connected`
        obligation must add the candidate that joins the pieces rather than
        emitting a cover whose sources cross-join."""
        benv, graph = _build(DISCONNECTED_FACTS_MODEL + ONE_HOP_BRIDGE)
        result = plan_network_sources(
            _terminals(benv, "local.a_val", "local.b_val"), benv, graph
        )

        assert result.solution is not None
        assert set(result.solution.sources) == {"ds~fact_a", "ds~fact_b", "ds~bridge"}

    def test_multi_hop_bridge_is_built_one_hop_at_a_time(self):
        """No single candidate touches both components, so the fixpoint must
        extend the path through a component-adjacent hop and re-ask."""
        benv, graph = _build(DISCONNECTED_FACTS_MODEL + TWO_HOP_BRIDGE)
        result = plan_network_sources(
            _terminals(benv, "local.a_val", "local.b_val"), benv, graph
        )

        assert result.solution is not None
        assert set(result.solution.sources) == {
            "ds~fact_a",
            "ds~fact_b",
            "ds~hop_one",
            "ds~hop_two",
        }

    def test_unbridgeable_split_declines_rather_than_fabricating_a_join(self):
        """With no bridge candidate the obligation is never minted — a truly
        disconnected request is the typed fallbacks' to answer, not this
        search's to paper over."""
        benv, graph = _build(DISCONNECTED_FACTS_MODEL)
        result = plan_network_sources(
            _terminals(benv, "local.a_val", "local.b_val"), benv, graph
        )

        assert result.solution is None
        assert not result.truncated


class TestRowPartialChains:
    def test_row_partial_node_terminates_a_chain_but_does_not_extend_it(self):
        benv, graph = _build(ROW_PARTIAL_CHAIN_MODEL)
        network = build_source_network(
            _terminals(benv, "local.launch_id", "local.maker_name"), benv, graph
        )

        assert not network.row_complete("ds~vehicles")
        reach = _forward_reach(network, "ds~launches")
        assert "ds~vehicles" in reach
        assert "ds~makers" not in reach

    def test_row_complete_middle_extends_the_chain(self):
        benv, graph = _build(
            ROW_PARTIAL_CHAIN_MODEL.replace("v: ~vehicle_id", "v: vehicle_id")
        )
        network = build_source_network(
            _terminals(benv, "local.launch_id", "local.maker_name"), benv, graph
        )

        assert network.row_complete("ds~vehicles")
        assert {"ds~vehicles", "ds~makers"} <= _forward_reach(network, "ds~launches")


class TestSearchBudget:
    def test_cover_limit_truncation_keeps_its_answer_and_names_the_limit(
        self, monkeypatch
    ):
        """Truncation AFTER covers were found: the plan is valid but need not be
        cost-minimal, and the caller must be able to tell."""
        monkeypatch.setattr(ns, "COVER_LIMIT", 1)
        # the partial `vehicle_name` binding is a soft branch, so the request has
        # a second cover for the limit to cut off
        benv, graph = _build(PARTIAL_DIMENSION_MODEL)
        network = build_source_network(
            _terminals(benv, "local.launch_id", "local.vehicle_name"), benv, graph
        )

        result = search_sources(network)

        assert result.limit is SearchLimit.COVERS
        assert result.truncated
        assert result.solution is not None
        assert not result.exhausted

    def test_budget_exhaustion_is_not_the_same_verdict_as_a_decline(self, monkeypatch):
        """Truncation BEFORE the first cover, with no usable seed: `solution is
        None`, but the search made no claim that no solution exists.
        `exhausted` is what keeps the fallbacks from reading a budget failure
        as evidence."""
        monkeypatch.setattr(ns, "STATE_LIMIT", 1)
        monkeypatch.setattr(ns, "_seed_cover", lambda network, targets: None)
        benv, graph = _build(DIAMOND_MODEL)
        network = build_source_network(
            _terminals(benv, "local.week_seq", "local.qty", "local.qoh"), benv, graph
        )

        result = search_sources(network)

        assert result.solution is None
        assert result.limit is SearchLimit.STATES
        assert result.exhausted
        # a decline for lack of a binder is the OTHER verdict, and stays empty
        assert not result.unreachable

    def test_budget_exhaustion_keeps_the_seed_solution(self, monkeypatch):
        """The top-down seed outlives the budget: a truncated walk returns the
        seed as a concrete (possibly non-cost-minimal) solution — reported as
        truncated-but-not-exhausted — instead of exhausting into a guess."""
        monkeypatch.setattr(ns, "STATE_LIMIT", 1)
        benv, graph = _build(DIAMOND_MODEL)
        network = build_source_network(
            _terminals(benv, "local.week_seq", "local.qty", "local.qoh"), benv, graph
        )

        result = search_sources(network)

        assert result.solution is not None
        assert result.limit is SearchLimit.STATES
        assert result.truncated and not result.exhausted

    def test_a_complete_search_reports_no_limit(self):
        benv, graph = _build(BRIDGE_MODEL)
        network = build_source_network(
            _terminals(benv, "local.customer_id", "local.item_id"), benv, graph
        )

        result = search_sources(network)

        assert result.limit is None
        assert not result.truncated
        assert not result.exhausted


class TestSearchMemos:
    def test_signature_is_stable_across_rebuilds_of_the_same_request(self):
        """The per-build search memo keys on this, so an unstable signature
        would silently disable it — and an over-eager one would reuse a
        solution across genuinely different requests."""
        benv, graph = _build(DIAMOND_MODEL)
        terminals = _terminals(benv, "local.week_seq", "local.qty", "local.qoh")
        first = build_source_network(terminals, benv, graph)
        second = build_source_network(terminals, benv, graph)

        assert first.signature() == second.signature()
        assert hash(first.signature())

    def test_signature_separates_requests_with_different_terminals(self):
        benv, graph = _build(DIAMOND_MODEL)
        wide = build_source_network(
            _terminals(benv, "local.week_seq", "local.qty", "local.qoh"), benv, graph
        )
        narrow = build_source_network(_terminals(benv, "local.qty"), benv, graph)

        assert wide.signature() != narrow.signature()

    def test_obligation_memo_does_not_change_the_answer(self):
        benv, graph = _build(DIAMOND_MODEL)
        terminals = _terminals(benv, "local.week_seq", "local.qty", "local.qoh")
        memoized = search_sources(build_source_network(terminals, benv, graph))

        cold = build_source_network(terminals, benv, graph)
        real_compute = nob.compute_pending_obligations
        # every lookup misses, so the search runs fully uncached
        object.__setattr__(cold, "_obligation_cache", _NeverCaching())
        uncached = search_sources(cold)

        assert real_compute is nob.compute_pending_obligations
        assert memoized.solution is not None and uncached.solution is not None
        assert memoized.solution.sources == uncached.solution.sources
        assert memoized.solution.cost == uncached.solution.cost


class _NeverCaching(dict):
    def __setitem__(self, key, value):
        return None


def _captured_network_requests(monkeypatch, model: str, query: str):
    """Every `(concepts, environment, graph, conditions)` the planner labeled a
    network from, so a test can re-label a MODIFIED graph for the same request."""
    import trilogy.core.processing.v4_helper.source_planning as sp
    from trilogy.core.processing.v4_helper.network_build import (
        build_source_network as real_build,
    )
    from trilogy.dialect.duckdb import DuckDBDialect

    captured = []

    def capturing_build(concepts, environment, graph, conditions=None):
        captured.append((concepts, environment, graph, conditions))
        return real_build(concepts, environment, graph, conditions)

    monkeypatch.setattr(sp, "build_source_network", capturing_build)
    env = Environment()
    env.parse(model)
    DuckDBDialect().generate_queries(env, env.parse(query)[1])
    return captured


class TestUnofferedProbePinning:
    def test_probe_no_candidate_offers_is_pinned_to_its_own_carrier(self, monkeypatch):
        """When the graph mints no edge for a requested presence probe, the
        search must bind it to the datasource physically carrying the member's
        column — the same scan `gen_presence_probe_node` pins. Without that the
        probe is unreachable, the search declines, and the probe's filter
        silently drops (reads as "no restriction")."""
        from trilogy.core.processing.node_generators.presence_probe import (
            is_presence_probe,
        )

        requests = _captured_network_requests(
            monkeypatch,
            COALESCING_ARMS_MODEL,
            "where s_cust is null select c_cust union join s_cust = c_cust;",
        )
        for concepts, benv, graph, conditions in requests:
            probe_nodes = [
                node
                for node in graph.nodes
                if node.startswith("c~")
                and is_presence_probe(node.split("~", 1)[1].split("@", 1)[0])
            ]
            if not probe_nodes:
                continue
            stripped = graph.copy()
            stripped.remove_nodes_from(probe_nodes)
            network = build_source_network(concepts, benv, stripped, conditions)
            probes = [a for a in network.terminals if is_presence_probe(a)]
            assert probes, "request lost its probe terminal"
            for probe in probes:
                binders = network.binders(probe)
                assert binders, f"{probe} unreachable once the graph offers it nowhere"
                for node in binders:
                    assert network.candidates[node].bindings[probe].injected
            return
        raise AssertionError("no searched request carried a presence probe")


class TestCoalescingAxisFamilies:
    def test_unpinned_axis_request_assembles_every_member_arm(self, monkeypatch):
        """An axis-population request (`where s_cust is null select c_cust`)
        must read EVERY member's own column: the axis is the union of the
        member domains, and a single-arm read silently drops the other arm's
        rows (the q84-family anti-join shape). The side-pinned probe's row key
        is condition baggage and must not pin the request to the probe's arm."""
        captured = _captured_searches(
            monkeypatch,
            COALESCING_ARMS_MODEL,
            "where s_cust is null select c_cust union join s_cust = c_cust;",
        )
        family_searches = [
            (network, result)
            for network, result in captured
            if network.axis_families.get("local.c_cust")
        ]
        assert family_searches, "no search recorded the axis family requirement"
        network, result = family_searches[-1]
        for node in ("ds~store_fact", "ds~catalog_fact"):
            binding = network.candidates[node].bindings["local.c_cust"]
            assert binding.strength is BindingStrength.PARTIAL
        assert result.solution is not None
        assert set(result.solution.sources) == {"ds~catalog_fact", "ds~store_fact"}
        assert not result.solution.partial_terminals

    def test_arm_pinned_request_reads_the_axis_arm_locally(self, monkeypatch):
        """An arm-scoped aggregate parent (the arm's own row key rides as a
        terminal) reads the axis keys AT that arm by design — the final
        assembly coalesces the arms — so no family requirement may force the
        other arm into its cover (the two-key single-FULL-JOIN shape)."""
        captured = _captured_searches(
            monkeypatch,
            COALESCING_ARMS_MODEL,
            "select c_cust, sum(sid) -> s_rows, sum(cid) -> c_rows"
            " union join s_cust = c_cust;",
        )
        pinned = [
            network
            for network, _ in captured
            if {"local.sid", "local.cid"} & set(network.terminals)
        ]
        assert pinned, "no arm-scoped parent request was searched"
        for network in pinned:
            assert not network.axis_families, (
                "arm-pinned request grew a family requirement: "
                f"{network.terminals} -> {network.axis_families}"
            )


class TestArmUnionBranching:
    def _whole_population(self):
        benv, graph = _build(PARTITION_UNION_MODEL)
        return build_source_network(
            _terminals(
                benv, "local.sales_channel", "local.order_id", "local.ext_sales_price"
            ),
            benv,
            graph,
        )

    def test_arm_is_dropped_when_its_own_union_satisfies_the_obligation(self):
        network = self._whole_population()

        assert network.subsumed_arms == {
            "ds~web_sales": "ds~web_sales-catalog_sales",
            "ds~catalog_sales": "ds~web_sales-catalog_sales",
        }
        obligations = nob.compute_pending_obligations(network, frozenset())
        assert obligations
        for obligation in obligations:
            assert obligation.satisfiers == ("ds~web_sales-catalog_sales",)

    def test_arm_without_its_union_on_offer_survives(self):
        network = self._whole_population()
        obligation = Obligation(
            ObligationKind.COVER, ("local.order_id",), ("ds~web_sales",)
        )

        assert nob.prune_subsumed_arms(network, [obligation]) == [obligation]

    def test_branching_collapses_to_one_cover(self):
        covers, limit = ns._enumerate_covers(self._whole_population())

        # every subset of the arms was a distinct cover before the prune, and
        # `_reduce` collapsed them all back to this one
        assert limit is None
        assert covers == [frozenset({"ds~web_sales-catalog_sales"})]

    def test_condition_pinned_arm_is_not_subsumed(self):
        benv, graph = _build(PARTITION_UNION_MODEL)
        conditions = BuildWhereClause(
            conditional=BuildComparison(
                left=benv.concepts["local.sales_channel"],
                right="WEB",
                operator=ComparisonOperator.EQ,
            )
        )
        network = build_source_network(
            _terminals(benv, "local.sales_channel", "local.order_id"),
            benv,
            graph,
            conditions,
        )

        # the WHERE narrows the request to this arm's own rows, so the union is
        # strictly worse: it re-reads the partitions the filter removes
        assert "ds~web_sales" not in network.subsumed_arms
        assert network.subsumed_arms == {
            "ds~catalog_sales": "ds~web_sales-catalog_sales"
        }
        result = search_sources(network)
        assert result.solution is not None
        assert result.solution.sources == ("ds~web_sales",)


class TestRustWalkParity:
    """`_enumerate_covers` runs in Rust; `_enumerate_covers_py` is the
    executable spec it is held to. Parity is exact — same covers, same order,
    same reported limit — including under truncation, where the push order
    decides which covers survive."""

    CASES = (
        (BRIDGE_MODEL, ("local.item_id", "local.catalog_quantity")),
        (
            BRIDGE_MODEL,
            ("local.catalog_quantity", "local.ticket_number", "local.customer_id"),
        ),
        (TWIN_SCAN_MODEL, ("local.ticket_number", "local.item_id", "local.quantity")),
        (PARTIAL_DIMENSION_MODEL, ("local.launch_id", "local.vehicle_name")),
        (
            PARTITION_UNION_MODEL,
            ("local.sales_channel", "local.order_id", "local.ext_sales_price"),
        ),
        (
            DISCRIMINATOR_JOIN_MODEL,
            ("local.return_amount", "local.site_id", "local.site_name"),
        ),
        (
            AUTHORED_HOP_MODEL,
            (
                "local.a_amount",
                "local.a_cust_sk",
                "local.b_amount",
                "local.b_cust_sk",
                "local.b_cust_id",
            ),
        ),
        (DIAMOND_MODEL, ("local.week_seq", "local.qty", "local.qoh")),
        (DISCONNECTED_FACTS_MODEL + ONE_HOP_BRIDGE, ("local.a_val", "local.b_val")),
        (DISCONNECTED_FACTS_MODEL + TWO_HOP_BRIDGE, ("local.a_val", "local.b_val")),
        (ROW_PARTIAL_CHAIN_MODEL, ("local.launch_id", "local.maker_name")),
        (
            DERIVED_MERGE_MODEL,
            ("local.l_id", "local.l_val", "local.r_id", "local.r_val"),
        ),
        (RECURSIVE_MERGE_MODEL, ("local.id", "local.plabel")),
    )

    def test_rust_walk_matches_the_python_spec(self):
        for model, addresses in self.CASES:
            benv, graph = _build(model)
            network = build_source_network(_terminals(benv, *addresses), benv, graph)
            assert ns._enumerate_covers(network) == ns._enumerate_covers_py(
                network
            ), addresses

    def test_rust_walk_matches_under_truncation(self, monkeypatch):
        for state_limit, cover_limit in ((1, 4096), (3, 4096), (10_000, 1), (7, 2)):
            monkeypatch.setattr(ns, "STATE_LIMIT", state_limit)
            monkeypatch.setattr(ns, "COVER_LIMIT", cover_limit)
            for model, addresses in self.CASES:
                benv, graph = _build(model)
                network = build_source_network(
                    _terminals(benv, *addresses), benv, graph
                )
                assert ns._enumerate_covers(network) == ns._enumerate_covers_py(
                    network
                ), (state_limit, cover_limit, addresses)

    def test_rust_walk_matches_on_captured_axis_family_searches(self, monkeypatch):
        captured = _captured_searches(
            monkeypatch,
            COALESCING_ARMS_MODEL,
            "where s_cust is null select c_cust union join s_cust = c_cust;",
        )
        assert captured
        for network, _result in captured:
            assert ns._enumerate_covers(network) == ns._enumerate_covers_py(network)


class TestCostAndObligationInvariants:
    def test_axes_covers_every_cost_field(self):
        import dataclasses

        cost = SolutionCost(1, 2, 3, 4, 5, 6, 7, 8)

        assert cost.axes() == (1, 2, 3, 4, 5, 6, 7, 8)
        assert len(cost.axes()) == len(dataclasses.fields(SolutionCost))

    def test_obligation_kinds_order_as_their_names_did(self):
        """The enumeration's scarcest-first tiebreak compares `identity`, so the
        kinds must stay orderable and order exactly as the bare strings did."""
        kinds = list(ObligationKind)

        assert sorted(kinds) == sorted(k.value for k in kinds)
        assert min((k, ()) for k in kinds)[0] is ObligationKind.AXIS

    def test_network_survives_hashing(self):
        benv, graph = _build(DIAMOND_MODEL)
        network = build_source_network(_terminals(benv, "local.qty"), benv, graph)

        # a structural __hash__ over the dict fields would raise here
        assert {network, network} == {network}

    def test_paired_cost_and_obligation_agree_on_every_source_set(self):
        """`unpaired_join_keys` charges for exactly the sides the `paired`
        obligation still demands — one predicate (`materializes`), two readers.
        A drift between them would let the search discharge an obligation the
        cost still charges for.

        Swept over every subset. Verified to catch either conjunct being
        dropped from one reader; it does NOT catch every possible rewrite (a
        subset-vs-intersection swap is invisible on this model), so the real
        guarantee is the single definition, and this is the regression net
        under it."""
        from itertools import combinations

        benv, graph = _build(AUTHORED_HOP_MODEL)
        network = build_source_network(
            _terminals(benv, "local.a_amount", "local.b_amount"), benv, graph
        )
        nodes = sorted(network.candidates)
        charged = 0
        for size in range(1, len(nodes) + 1):
            for combo in combinations(nodes, size):
                sources = frozenset(combo)
                pending = [
                    o
                    for o in nob.pending_obligations(network, sources)
                    if o.kind is ObligationKind.PAIRED
                ]
                unpaired = nt.unpaired_join_keys(network, sources)
                assert unpaired == len(pending), sorted(sources)
                charged += unpaired

        # the sweep is not vacuous: some subsets do carry an unmaterialized side
        assert charged > 0

    def test_enumerated_covers_are_already_distinct(self):
        benv, graph = _build(DIAMOND_MODEL)
        network = build_source_network(
            _terminals(benv, "local.week_seq", "local.qty", "local.qoh"), benv, graph
        )

        covers, _limit = ns._enumerate_covers(network)
        assert len(covers) == len(set(covers))

    def test_chain_completers_equals_the_forward_reach_definition(self):
        """`chain_completers` walks BACKWARDS from the full binders; the concept
        it implements is the forward one in `_forward_reach`. Pin them
        together — the backward gate is subtle (an origin is admitted
        unconditionally, but only a row-complete one may be expanded, since
        expanding it makes it an intermediate)."""
        for model, addresses in (
            (PARTIAL_DIMENSION_MODEL, ("local.launch_id", "local.vehicle_name")),
            (DISCRIMINATOR_JOIN_MODEL, ("local.return_amount", "local.site_name")),
            (AUTHORED_HOP_MODEL, ("local.a_amount", "local.b_amount")),
        ):
            benv, graph = _build(model)
            network = build_source_network(_terminals(benv, *addresses), benv, graph)
            for address in network.terminals:
                full = network.full_binders(address)
                forward = frozenset(
                    node
                    for node in network.candidates
                    if node in full or full & _forward_reach(network, node)
                )
                assert network.chain_completers(address) == forward, (model, address)

    def test_functional_adjacency_matches_pairwise_tests(self):
        benv, graph = _build(DISCRIMINATOR_JOIN_MODEL)
        network = build_source_network(
            _terminals(benv, "local.return_amount", "local.site_name"), benv, graph
        )

        for origin in network.candidates:
            for target in network.candidates:
                if origin == target:
                    continue
                edge = target in network.functional_successors(origin)
                assert edge == network.functional_into(origin, target)
                assert edge == (origin in network.functional_predecessors(target))
