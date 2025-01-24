from trilogy import parse


def test_derivations(test_environment):
    assert (
        test_environment.concepts["order_timestamp"].address == "local.order_timestamp"
    )
    assert (
        test_environment.concepts["order_timestamp.date"].address
        == "local.order_timestamp.date"
    )


def test_filtering_where_on_derived_aggregate(test_environment):
    exception = False
    try:
        env, _ = parse(
            """key x int;
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
        """
        )
    except Exception as e:
        exception = True
        assert (
            "Cannot reference an aggregate derived in the select (local.filtered_cst)"
            in str(e)
        )
    assert exception, "should have an exception"


def test_filtering_having_on_unincluded_value(test_environment):
    exception = False
    try:
        env, _ = parse(
            """key x int;
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
        """
        )
    except Exception as e:
        exception = True
        assert str(e).startswith(
            "Cannot reference a column (local.x) that is not in the select projection in the HAVING clause, move to WHERE"
        ), str(e)
    assert exception, "should have an exception"


def test_filtering_valid(test_environment):
    env, _ = parse(
        """key x int;
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
    """
    )
