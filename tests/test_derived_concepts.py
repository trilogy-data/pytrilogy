from trilogy import parse


def test_derivations(test_environment):
    assert (
        test_environment.concepts["order_timestamp"].address == "local.order_timestamp"
    )
    assert (
        test_environment.concepts["order_timestamp.date"].address
        == "local.order_timestamp.date"
    )


def test_hour_derivation(test_environment):
    hour_derived = test_environment.concepts["order_timestamp.hour"]
    assert "hour" in hour_derived.datatype.traits, hour_derived

    year_derived = test_environment.concepts["order_timestamp.year"]
    assert "year" in year_derived.datatype.traits, year_derived


def test_filtering_where_on_derived_aggregate(test_environment):
    exception = False
    try:
        env, _ = parse("""key x int;
    property x.cost float;

    datasource x_source (
        x:x,
        cost:cost)
        grain(x)
        address x_source;


    SELECT
        sum(cost)-> filtered_cst
    where
    x > 10 and filtered_cst >1000;
        """)
    except Exception as e:
        exception = True
        assert (
            "Cannot reference an aggregate derived in the select (local.filtered_cst)"
            in str(e)
        )
    assert exception, "should have an exception"


def test_where_mixed_grain_aggregates_rejected_cleanly():
    """A WHERE clause that mixes inline aggregates at different grains used to
    leak the internal resolver state ("Have {MergeNode<...>}: None and need ...")
    as the user-facing error. It should raise a clean InvalidSyntaxException at
    validation with no internal node names.
    """
    from trilogy import parse
    from trilogy.core.exceptions import InvalidSyntaxException

    # SELECT does NOT itself produce the same aggregate, so the more-specific
    # "also computed in the SELECT" check does not apply. The multi-grain
    # validation handles this case.
    src = """key x int;
property x.cost float;
property x.state string;

datasource x_source (
    x:x,
    cost:cost,
    state:state)
    grain(x)
    address x_source;

where sum(cost) > 1.2 * avg(cost) by state
SELECT
    x;
"""
    raised = None
    try:
        parse(src)
    except Exception as e:
        raised = e
    assert raised is not None, "expected a validation exception"
    assert isinstance(
        raised, InvalidSyntaxException
    ), f"expected InvalidSyntaxException, got {type(raised).__name__}: {raised}"
    msg = str(raised)
    assert "WHERE clause aggregates at multiple grains" in msg
    assert "HAVING" in msg
    # Internal resolver state must never reach the user.
    assert "MergeNode" not in msg
    assert "_virt_agg" not in msg
    assert "@Grain<" not in msg


def test_where_aggregate_also_in_select_rejected_cleanly():
    """The repro from the bug: ``where sum(x) > 1.2 * avg(x) by g`` with
    ``select sum(x) as total`` must not leak the internal MergeNode/_virt_agg
    state to the user.
    """
    from trilogy import parse
    from trilogy.core.exceptions import InvalidSyntaxException

    src = """key x int;
property x.cost float;
property x.state string;

datasource x_source (
    x:x,
    cost:cost,
    state:state)
    grain(x)
    address x_source;

where sum(cost) > 1.2 * avg(cost) by state
SELECT
    x,
    sum(cost) as total_cost;
"""
    raised = None
    try:
        parse(src)
    except Exception as e:
        raised = e
    assert raised is not None, "expected a validation exception"
    assert isinstance(
        raised, InvalidSyntaxException
    ), f"expected InvalidSyntaxException, got {type(raised).__name__}: {raised}"
    msg = str(raised)
    assert "HAVING" in msg
    # Internal resolver state must never reach the user.
    assert "MergeNode" not in msg
    assert "_virt_agg" not in msg
    assert "@Grain<" not in msg


def test_filtering_having_on_unincluded_value(test_environment):
    exception = False
    try:
        env, _ = parse("""key x int;
    property x.cost float;

    datasource x_source (
        x:x,
        cost:cost)
        grain(x)
        address x_source;


    SELECT
        sum(cost)-> filtered_cst
    having
    x > 10 and filtered_cst >1000;
        """)
    except Exception as e:
        exception = True
        assert "HAVING references 'local.x'" in str(e) and "--local.x" in str(e), str(
            e
        )
    assert exception, "should have an exception"


def test_filtering_valid(test_environment):
    env, _ = parse("""key x int;
property x.cost float;

datasource x_source (
    x:x,
    cost:cost)
    grain(x)
    address x_source;

where
    x>-10
SELECT
    sum(cost)-> filtered_cst
having
    filtered_cst >1000;
    """)
