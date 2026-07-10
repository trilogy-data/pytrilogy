import pytest

from trilogy.core.enums import Purpose
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.author import Concept
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment, EnvironmentConceptDict
from trilogy.parser import parse


def test_undefined_concept_query(test_environment):
    q = "SELECT orid LIMIT 10;"
    try:
        parse(q, test_environment)
    except UndefinedConceptException as e:
        assert e.suggestions == ["order_id"]

    q = "SELECT order_ct LIMIT 10;"
    try:
        parse(q, test_environment)
    except UndefinedConceptException as e:
        assert len(e.suggestions) == 3


def test_undefined_concept_dict():
    env = EnvironmentConceptDict()
    env["order_id"] = Concept(
        name="order_id", datatype=DataType.INTEGER, purpose=Purpose.KEY
    )
    try:
        env["zzz"]
    except UndefinedConceptException as e:
        assert e.suggestions == []
        assert "suggestions" not in e.message.lower()

    try:
        env["orid"]
    except UndefinedConceptException as e:
        assert e.suggestions == ["order_id"]
        assert "suggestions" in e.message.lower()
        assert "order_id" in e.message.lower()


def test_undefined_concept_in_aggregate_raises():
    """An undefined concept used only as a function argument must raise at
    parse time, not slip through to SQL generation as a NoDatasourceException.
    The select transform validates its output concept; the *input* arguments
    were previously unchecked."""
    env = Environment()
    env.parse("key x int;")
    with pytest.raises(UndefinedConceptException):
        env.parse("select sum(totally_made_up_concept) as foo;")


def test_undefined_concept_in_nested_aggregate_raises():
    env = Environment()
    env.parse("key x int;")
    with pytest.raises(UndefinedConceptException):
        env.parse("select sum(totally_made_up_concept) + sum(x) as foo;")


def test_undefined_concept_in_coalesce_raises_undefined_not_type_error():
    """An undefined concept used inside a type-checking function (coalesce) must
    raise the clean UndefinedConceptException (with suggestions) — not a confusing
    `coalesce must be of the same type {STRING, UNKNOWN}` error from the undefined
    arg resolving to a phantom UNKNOWN-typed concept before the undefined check."""
    env = Environment()
    env.parse(
        "key id int;\n"
        "property id.login string;\n"
        "datasource ids (id:id, login:login) grain (id) address ids;"
    )
    with pytest.raises(UndefinedConceptException) as exc:
        env.parse("select coalesce(login, made_up_concept) as x;")
    assert "made_up_concept" in str(exc.value)
    assert "same type" not in str(exc.value)


def test_genuine_coalesce_type_mismatch_still_raises():
    from trilogy.core.functions import InvalidSyntaxException

    env = Environment()
    env.parse(
        "key id int;\n"
        "property id.login string;\n"
        "property id.amt int;\n"
        "datasource ids (id:id, login:login, amt:amt) grain (id) address ids;"
    )
    with pytest.raises(InvalidSyntaxException):
        env.parse("select coalesce(login, amt) as x;")


def _dict_with(*keys: str) -> EnvironmentConceptDict:
    d = EnvironmentConceptDict()
    for k in keys:
        leaf = k.rsplit(".", 1)[-1]
        d[k] = Concept(name=leaf, datatype=DataType.INTEGER, purpose=Purpose.KEY)
    return d


def test_find_similar_leaf_match_surfaces_full_paths():
    """A bare leaf reference (e.g. `first_name` in ORDER BY, where the full path
    is required) has no fuzzy match against long full-path keys, so leaf matching
    must surface every concept whose path ends in `.<leaf>`."""
    d = _dict_with("a.billing.first_name", "a.ship.first_name", "a.last_name")
    sugg = d._find_similar_concepts("first_name")
    # both `.first_name` paths are surfaced, and lead (leaf matches before fuzzy)
    assert "a.billing.first_name" in sugg
    assert "a.ship.first_name" in sugg
    assert sugg.index("a.billing.first_name") < len(sugg)


def test_find_similar_extra_keys_surfaces_staged_concept():
    """`extra_keys` (concepts staged this parse but not yet committed — e.g. a
    rowset output) must be considered for suggestions even though they are absent
    from the dict's own keys."""
    d = _dict_with("ws.order_number")
    sugg = d._find_similar_concepts(
        "qual.order_number", extra_keys=["qual.ws.order_number"]
    )
    assert "qual.ws.order_number" in sugg  # the real (staged) rowset path
    assert "ws.order_number" in sugg


def test_find_similar_never_suggests_the_looked_up_address():
    """A staged placeholder for the missing address itself may be in the
    candidate set; it must never be echoed back as a suggestion."""
    d = _dict_with("qual.ws.order_number")
    sugg = d._find_similar_concepts(
        "qual.order_number",
        extra_keys=["qual.order_number", "qual.ws.order_number"],
    )
    assert "qual.order_number" not in sugg
    assert "qual.ws.order_number" in sugg


def test_find_similar_partial_path_subsequence_ranks_first():
    """Dropping an intermediate namespace segment (`y1999.item_id` for the real
    rowset path `y1999.agg.item_id`, where the column kept its source namespace)
    must surface — and rank above — unrelated same-leaf/fuzzy candidates that don't
    share the namespace prefix."""
    d = _dict_with(
        "y1999.agg.item_id",
        "y1999.agg.product_name",
        "ss.item.id",
        "cs.item.id",
        "cr.item.id",
    )
    sugg = d._find_similar_concepts("y1999.item_id")
    assert "y1999.agg.item_id" in sugg
    # the prefix-sharing path match leads the unrelated `*.item.id` fuzzy matches
    assert sugg[0] == "y1999.agg.item_id"


def test_find_similar_same_namespace_nearmiss_beats_leaf_flood():
    """A namespaced typo on a middle segment (`cs.billing_customer.id` for the real
    `cs.bill_customer.id`) must surface the same-namespace near-miss FIRST — not be
    buried under the flood of unrelated concepts sharing the common leaf `id`, nor
    outranked by an identical name in a different namespace (`ws.billing_customer.id`).
    """
    d = _dict_with(
        "cs.bill_customer.id",
        "ws.billing_customer.id",
        "cs.item.id",
        "cs.date.id",
        "all_sales.item.id",
        "all_sales.date.id",
    )
    sugg = d._find_similar_concepts("cs.billing_customer.id")
    assert sugg[0] == "cs.bill_customer.id"
    # the same-namespace near-miss outranks the different-namespace exact-name match
    assert sugg.index("cs.bill_customer.id") < sugg.index("ws.billing_customer.id")


def test_find_similar_partial_path_via_extra_keys():
    """The real path is often only STAGED (a rowset/CTE output not yet committed);
    the partial-path match must still find it through `extra_keys`."""
    d = _dict_with("ss.item.id", "cs.item.id", "cr.item.id")
    sugg = d._find_similar_concepts(
        "y1999.item_id", extra_keys=["y1999.agg.item_id", "y1999.agg.product_name"]
    )
    assert sugg[0] == "y1999.agg.item_id"


def test_is_subsequence_is_ordered():
    from trilogy.core.models.environment import _is_subsequence

    assert _is_subsequence(["y1999", "item_id"], ["y1999", "agg", "item_id"])
    assert not _is_subsequence(["item_id", "y1999"], ["y1999", "agg", "item_id"])
    assert not _is_subsequence(["x"], ["y1999", "agg", "item_id"])


def test_subsequence_gaps_measures_tightness():
    from trilogy.core.models.environment import _subsequence_gaps

    # contiguous run (query is a prefix) -> no gaps
    assert _subsequence_gaps(["a", "b"], ["a", "b", "c"]) == 0
    # contiguous run in the middle -> still no gaps
    assert _subsequence_gaps(["b", "c"], ["a", "b", "c", "d"]) == 0
    # one intermediate segment skipped
    assert _subsequence_gaps(["a", "c"], ["a", "b", "c"]) == 1
    # two intermediate segments skipped between the matched pair
    assert _subsequence_gaps(["a", "id"], ["a", "x", "y", "id"]) == 2
    # gaps count only segments BETWEEN matches, not trailing depth
    assert _subsequence_gaps(["a", "b"], ["a", "b", "c", "d", "e"]) == 0


def test_find_similar_q30_shallow_child_beats_deep_near_miss():
    """Regression for q30: a bare import-alias reference (`web_returns.billing_customer`)
    must surface — and rank FIRST — the obvious shallow child
    (`web_returns.billing_customer.id`, one extra segment), even though six DEEP
    wrong-namespace near-misses (`web_returns.web_sales.billing_customer.demographics.*`,
    three extra segments) were inserted BEFORE it and would otherwise consume the
    entire 6-slot cap in dict-insertion order."""
    d = _dict_with(
        "web_returns.web_sales.billing_customer.demographics.id",
        "web_returns.web_sales.billing_customer.demographics.gender",
        "web_returns.web_sales.billing_customer.demographics.marital_status",
        "web_returns.web_sales.billing_customer.demographics.education",
        "web_returns.web_sales.billing_customer.demographics.credit_rating",
        "web_returns.web_sales.billing_customer.demographics.purchase_estimate",
        "web_returns.billing_customer.id",
        "web_returns.billing_customer.first_name",
    )
    sugg = d._find_similar_concepts("web_returns.billing_customer")
    assert "web_returns.billing_customer.id" in sugg
    assert sugg[0] == "web_returns.billing_customer.id"
    # the shallow children (extra 1) both outrank every deep near-miss (extra 3)
    deep = "web_returns.web_sales.billing_customer.demographics.id"
    assert sugg.index("web_returns.billing_customer.first_name") < sugg.index(deep)


def test_find_similar_path_matches_rank_by_fewest_extra_segments():
    """Regardless of insertion order, a path match with fewer extra segments
    beyond the query is closer and must rank ahead of a deeper one."""
    d = _dict_with(
        "a.b.deep1.deep2.c",  # extra 3, inserted first
        "a.b.mid.c",  # extra 2
        "a.b.c",  # extra 1, inserted last — the closest
    )
    sugg = d._find_similar_concepts("a.c")
    # query `a.c`: all three contain it as a subsequence; rank by extra segments
    assert sugg[0] == "a.b.c"
    assert (
        sugg.index("a.b.c") < sugg.index("a.b.mid.c") < sugg.index("a.b.deep1.deep2.c")
    )


def test_find_similar_contiguity_breaks_ties_at_equal_depth():
    """When two path matches have the SAME extra depth, the tighter (contiguous)
    match wins: `ns.alias.id` (query as a contiguous prefix) beats `ns.other.id`
    where the query segments are split by an intervening namespace."""
    d = _dict_with(
        "ns.other.id",  # query `ns.id` gapped by `other`, extra 1
        "ns.id.suffix",  # query `ns.id` contiguous prefix, extra 1
    )
    sugg = d._find_similar_concepts("ns.id")
    assert sugg.index("ns.id.suffix") < sugg.index("ns.other.id")


def test_find_similar_shallow_child_survives_flood_of_deep_near_misses():
    """Adversarial: MANY (>6) deep wrong-namespace near-misses all inserted before
    the single correct shallow child. The correct child must still appear within
    the capped top-6 because ranking runs BEFORE the cap."""
    deep = [f"root.other.alias.deep.field{i}" for i in range(20)]
    d = _dict_with(*deep, "root.alias.id")
    sugg = d._find_similar_concepts("root.alias")
    assert "root.alias.id" in sugg
    assert sugg[0] == "root.alias.id"
    assert len(sugg) <= 6


def test_find_similar_equal_rank_preserves_insertion_order():
    """Ties (same extra depth AND same gap count) fall back to stable
    dict-insertion order — sorting must not reshuffle equally-relevant matches."""
    d = _dict_with("ns.alias.a", "ns.alias.b", "ns.alias.c")
    sugg = d._find_similar_concepts("ns.alias")
    assert sugg[:3] == ["ns.alias.a", "ns.alias.b", "ns.alias.c"]


def test_find_similar_exact_leaf_beats_fuzzy():
    """An exact leaf match (user knows the NAME, missed the path) must outrank
    character-level fuzz: bare `warehouse_count` surfaces
    `all_orders.warehouse_count` ahead of `ws.warehouse.county`/`.country`."""
    d = _dict_with(
        "ws.warehouse.county",
        "ws.warehouse.country",
        "all_orders.warehouse_count",
    )
    sugg = d._find_similar_concepts("warehouse_count")
    assert sugg[0] == "all_orders.warehouse_count"


def test_find_similar_hides_internal_names():
    """Mangled per-rowset aliases (`_all_orders_has_return`) and model-private
    helpers (`ws._returned_order_number`) must not leak into suggestions."""
    d = _dict_with(
        "all_orders.has_return",
        "_all_orders_has_return",
        "ws._returned_order_number",
        "ws.order_number",
    )
    sugg = d._find_similar_concepts("has_return")
    assert "all_orders.has_return" in sugg
    assert "_all_orders_has_return" not in sugg
    sugg = d._find_similar_concepts("order_number")
    assert "ws.order_number" in sugg
    assert "ws._returned_order_number" not in sugg


def test_find_similar_underscore_reference_allows_internal_names():
    """A reference that itself uses an `_`-prefixed segment opts back into
    internal candidates."""
    d = _dict_with("_internal_helper_value")
    sugg = d._find_similar_concepts("_internal_helper_val")
    assert "_internal_helper_value" in sugg


def test_undefined_function_arg_reports_item_line():
    """An undefined concept inside a function argument carries no token
    position; the error must fall back to the select ITEM's line — not the
    statement's first line, which for `where ... select ...` points at the
    `where` keyword."""
    env = Environment()
    env.parse("key x int;\nproperty x.y int;\ndatasource ds (x:x, y:y) grain (x) address ds;")
    with pytest.raises(UndefinedConceptException) as exc:
        env.parse("where\n    y > 1\nselect\n    count(missing_thing) as foo\n;")
    assert "line 4" in str(exc.value)
    assert "missing_thing" in str(exc.value)


def test_rowset_field_shorthand_resolves_to_rowset_path(tmp_path):
    """A rowset column from an import namespace, selected WITHOUT `as`, keeps its
    full source path (`qual.ws.order_number`). The bare-leaf shorthand
    `qual.order_number` resolves to that single full path at parse time — even
    though the rowset concept is only STAGED (not yet committed) when the
    referencing statement is finalized."""
    (tmp_path / "ws.preql").write_text(
        "key order_number int;\n"
        "property order_number.cost float;\n"
        "datasource ws (order_number:order_number, cost:cost)\n"
        "grain (order_number) address ws;\n"
    )
    env = Environment(working_path=str(tmp_path))
    env.parse(
        "import ws as ws;\n"
        "rowset qual <- select ws.order_number where ws.cost > 0;\n"
        "select qual.order_number;\n"
    )
    assert env.concepts["qual.order_number"].address == "qual.ws.order_number"


def test_cte_output_shorthand_resolves_staged_path():
    """A reference that drops a CTE column's source namespace (`y1999.item_id`
    for the staged `y1999.agg.item_id`) resolves to the single full path at parse
    time. The named-statement outputs are staged (not yet committed) when the
    third statement is finalized, so resolution must consult the staged
    (pending) candidate set — not just committed concepts."""
    env = Environment()
    env.parse(
        "key id int;\n"
        "property id.color string;\n"
        "property id.name string;\n"
        "datasource items (id:id, color:color, name:name) "
        "grain (id) address items;"
    )
    env.parse(
        "with agg as select id as item_id, name as product_name "
        "where color = 'red';\n"
        "with y1999 as select agg.item_id, agg.product_name "
        "where agg.item_id > 0;\n"
        "select y1999.product_name, y1999.item_id;\n"
    )
    assert env.concepts["y1999.item_id"].address == "y1999.agg.item_id"
