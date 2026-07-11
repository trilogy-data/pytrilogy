"""Gate tests for authored-join terminal injection (common.py helpers).

The relevance gate must fire only when a request traverses BOTH sides of a
declared relation from distinct carriers — one-sided requests, denormalized
single-scan coverage, and derived members must all stay lazy (the
reverted-spike canary shapes). ROOT members substitute onto one canonical at
build time, so injection targets the canonical concept.
"""

from trilogy import Dialects
from trilogy.core.enums import JoinType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.node_generators.common import (
    authored_join_pair_candidates,
    inject_authored_join_key_terminals,
    relevant_authored_join_pairs,
)

TWO_FACT_MODEL = """
key a_cust_sk int;
property a_cust_sk.a_cust_id string;
datasource a_customers (
    sk: a_cust_sk,
    cid: a_cust_id,
) grain (a_cust_sk) address customers_a;

key b_cust_sk int;
property b_cust_sk.b_cust_id string;
datasource b_customers (
    sk: b_cust_sk,
    cid: b_cust_id,
) grain (b_cust_sk) address customers_b;

key a_order int;
property a_order.a_amount int;
datasource a_facts (
    o: a_order,
    cust: a_cust_sk,
    amt: a_amount,
) grain (a_order) address a_facts;

key b_order int;
property b_order.b_amount int;
datasource b_facts (
    o: b_order,
    cust: b_cust_sk,
    amt: b_amount,
) grain (b_order) address b_facts;
"""

DENORMALIZED_MODEL = """
key a_cust_sk int;
property a_cust_sk.a_cust_id string;
key b_cust_sk int;
property b_cust_sk.b_cust_id string;
key a_order int;
property a_order.a_amount int;
key b_order int;
property b_order.b_amount int;
datasource wide_facts (
    ao: a_order,
    bo: b_order,
    acust: a_cust_sk,
    bcust: b_cust_sk,
    a_amt: a_amount,
    b_amt: b_amount,
) grain (a_order, b_order) address wide_facts;
"""

CANARY_MODEL = """
key id int;
property id.d1 date;
property id.d2 date;
datasource facts (
    id: id,
    d1: ?d1,
    d2: ?d2,
) grain (id) address facts;
key s1 <- date_spine(date_add(current_date(), day, -10), current_date());
key s2 <- date_spine(date_add(current_date(), day, -10), current_date());
merge d1 into ~s1;
merge d2 into ~s2;
"""

SUBSET_JOIN = [("local.a_cust_id", "local.b_cust_id", JoinType.LEFT_OUTER)]
UNION_JOIN = [("local.a_cust_id", "local.b_cust_id", JoinType.FULL)]
MEMBER_ADDRESSES = {"local.a_cust_id", "local.b_cust_id"}


def _build(model: str, scoped_joins=None):
    env = Environment()
    Dialects.DUCK_DB.default_executor(environment=env).parse_text(model)
    return env.materialize_for_select(scoped_joins=scoped_joins)


def test_candidates_subset_and_union_property_pair():
    for joins in (SUBSET_JOIN, UNION_JOIN):
        build_env = _build(TWO_FACT_MODEL, scoped_joins=joins)
        candidates = authored_join_pair_candidates(build_env)
        assert len(candidates) == 1, candidates
        pair = candidates[0]
        assert {pair.left.address, pair.right.address} == MEMBER_ADDRESSES
        assert pair.canonical.address in MEMBER_ADDRESSES
        assert pair.left.keys and pair.right.keys


def test_candidates_exclude_derived_members():
    build_env = _build(CANARY_MODEL)
    assert authored_join_pair_candidates(build_env) == []


def test_gate_fires_for_cross_fact_fk_carriers():
    for joins in (SUBSET_JOIN, UNION_JOIN):
        build_env = _build(TWO_FACT_MODEL, scoped_joins=joins)
        request = [
            build_env.concepts["local.a_amount"],
            build_env.concepts["local.b_amount"],
        ]
        relevant = relevant_authored_join_pairs(request, build_env)
        assert len(relevant) == 1
        injected = inject_authored_join_key_terminals(list(request), build_env)
        added = {c.address for c in injected} - {c.address for c in request}
        # the merged key plus each side's FK carrier: neither member is bound
        # on a request datasource, so both hops are pinned as mandatory
        assert added == {
            relevant[0].canonical.address,
            "local.a_cust_sk",
            "local.b_cust_sk",
        }


def test_gate_silent_for_one_sided_request():
    build_env = _build(TWO_FACT_MODEL, scoped_joins=SUBSET_JOIN)
    request = [build_env.concepts["local.a_amount"]]
    assert relevant_authored_join_pairs(request, build_env) == []
    assert inject_authored_join_key_terminals(list(request), build_env) == request


def test_gate_silent_for_denormalized_single_scan():
    # wide_facts carries both FK sets, so neither side has a carrier the other
    # lacks and the single-scan plan must stay untouched.
    build_env = _build(DENORMALIZED_MODEL, scoped_joins=SUBSET_JOIN)
    request = [
        build_env.concepts["local.a_amount"],
        build_env.concepts["local.b_amount"],
    ]
    assert relevant_authored_join_pairs(request, build_env) == []


def test_gate_silent_for_canary_request():
    build_env = _build(CANARY_MODEL)
    request = [build_env.concepts["local.s1"]]
    assert relevant_authored_join_pairs(request, build_env) == []


def test_member_projected_is_noop():
    # projecting the merged key pulls both dims into the request's datasource
    # set, so every member is directly bound — the merged concept is already a
    # natural shared join key and injection stays out of the way
    build_env = _build(TWO_FACT_MODEL, scoped_joins=SUBSET_JOIN)
    request = [
        build_env.concepts["local.a_amount"],
        build_env.concepts["local.b_amount"],
        build_env.concepts["local.a_cust_id"],
    ]
    injected = inject_authored_join_key_terminals(list(request), build_env)
    assert {c.address for c in injected} == {c.address for c in request}


def test_gate_silent_for_directly_bound_members():
    # q2 date-spine shape: the fact binds its FK member and the dim binds the
    # canonical — every member is a physical column of a request datasource,
    # enforcement is the natural join, and injection must stay out
    model = """
key date_sk int;
property date_sk.week_seq int;
datasource date_dim (
    sk: date_sk,
    wk: week_seq,
) grain (date_sk) address date_dim;

key cs_date_sk int;
key cs_order int;
property cs_order.cs_amount int;
datasource cat_facts (
    o: cs_order,
    d: cs_date_sk,
    amt: cs_amount,
) grain (cs_order) address cat_facts;
"""
    joins = [("local.date_sk", "local.cs_date_sk", JoinType.LEFT_OUTER)]
    build_env = _build(model, scoped_joins=joins)
    request = [
        build_env.concepts["local.cs_amount"],
        build_env.concepts["local.week_seq"],
    ]
    assert relevant_authored_join_pairs(request, build_env) == []


def test_chained_relations_gate_pairwise():
    model = TWO_FACT_MODEL + """
key c_cust_sk int;
property c_cust_sk.c_cust_id string;
datasource c_customers (
    sk: c_cust_sk,
    cid: c_cust_id,
) grain (c_cust_sk) address customers_c;

key c_order int;
property c_order.c_amount int;
datasource c_facts (
    o: c_order,
    cust: c_cust_sk,
    amt: c_amount,
) grain (c_order) address c_facts;
"""
    joins = [
        ("local.a_cust_id", "local.b_cust_id", JoinType.LEFT_OUTER),
        ("local.b_cust_id", "local.c_cust_id", JoinType.LEFT_OUTER),
    ]
    build_env = _build(model, scoped_joins=joins)
    request = [
        build_env.concepts["local.a_amount"],
        build_env.concepts["local.b_amount"],
    ]
    relevant = relevant_authored_join_pairs(request, build_env)
    assert len(relevant) == 1, [(p.left.address, p.right.address) for p in relevant]
    assert {relevant[0].left.address, relevant[0].right.address} == MEMBER_ADDRESSES
