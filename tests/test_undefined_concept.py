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


def test_undefined_rowset_field_suggests_rowset_path(tmp_path):
    """A rowset column from an import namespace, selected WITHOUT `as`, keeps its
    full source path (`qual.ws.order_number`, not `qual.order_number`).
    Referencing the bare leaf in the same parse must suggest the real rowset
    path — even though the rowset concept is only STAGED (not yet committed) when
    the referencing statement fails."""
    (tmp_path / "ws.preql").write_text(
        "key order_number int;\n"
        "property order_number.cost float;\n"
        "datasource ws (order_number:order_number, cost:cost)\n"
        "grain (order_number) address ws;\n"
    )
    env = Environment(working_path=str(tmp_path))
    with pytest.raises(UndefinedConceptException) as exc:
        # rowset column `ws.order_number` (no `as`) is exposed as
        # `qual.ws.order_number`; the bare leaf `qual.order_number` must point
        # at the real path even though `qual` is only staged at this point.
        env.parse(
            "import ws as ws;\n"
            "rowset qual <- select ws.order_number where ws.cost > 0;\n"
            "select qual.order_number;\n"
        )
    assert any(
        "qual.ws.order_number" in s for s in exc.value.suggestions
    ), exc.value.suggestions
    # never echo the looked-up address itself
    assert "qual.order_number" not in exc.value.suggestions


def test_undefined_cte_output_join_key_suggests_staged_path():
    """A join-key reference that drops a CTE column's source namespace
    (`y1999.item_id` for the staged `y1999.agg.item_id`) raises through the
    dict-level lookup in `ConceptLookup.require`. The named-statement outputs are
    staged (not yet committed) when the third statement fails, so the suggestion
    must surface them via the staged candidate set — not just committed concepts."""
    env = Environment()
    env.parse(
        "key id int;\n"
        "property id.color string;\n"
        "property id.name string;\n"
        "datasource items (id:id, color:color, name:name) "
        "grain (id) address items;"
    )
    with pytest.raises(UndefinedConceptException) as exc:
        env.parse(
            "with agg as select id as item_id, name as product_name "
            "where color = 'red';\n"
            "with y1999 as select agg.item_id, agg.product_name "
            "where agg.item_id > 0;\n"
            "select y1999.product_name "
            "inner join y1999.item_id = y1999.item_id;\n"
        )
    assert exc.value.suggestions[0] == "y1999.agg.item_id", exc.value.suggestions
    assert "y1999.item_id" not in exc.value.suggestions
