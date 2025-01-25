from trilogy import Executor, parse
from trilogy.core.models.environment import Environment


def test_funnel_analysis(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
with funnel_inputs as 
WHERE distinct_id in visits
SELECT
    CASE 
        WHEN event_name = 'Landing Page' then 1
        WHEN event_name = 'Sign Up' then 2
        WHEN event_name = 'New Canvas' then 3
        WHEN event_name = 'Start Subscription' then 4
    END->funnel,
    lag 1 funnel over distinct_id order by funnel asc -> prior_step,
    distinct_id
having 
    funnel = 1 or prior_step = funnel-1
;

# use our existing funnel analysis


SELECT
    funnel_inputs.funnel, 
    count(funnel_inputs.distinct_id) -> customer_count,
    customer_count/ lag 1 customer_count  order by funnel_inputs.funnel  asc -> drop_off
ORDER BY
    funnel_inputs.funnel asc
;

"""

    _, statements = parse(test_select, test_environment)
    query = test_executor.generate_sql(test_select)[0]
    assert "INVALID_REFERENCE_BUG" not in query


def test_funnel_basic(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """
with funnel_inputs as 
WHERE distinct_id in visits
SELECT
    CASE 
        WHEN event_name = 'View Landing Page' then 1
        WHEN event_name = 'Sign Up' then 2
        WHEN event_name = 'New Canvas' then 3
        WHEN event_name = 'Start Subscription' then 4
    END->funnel,
    lag 1 funnel over distinct_id order by funnel asc -> prior_step,
    distinct_id
having 
    funnel = 1 or prior_step = funnel-1
;

# use our existing funnel analysis


SELECT
    funnel_inputs.funnel, 
    count(funnel_inputs.distinct_id) -> customer_count,
    lag 1 customer_count ->lagged_count
ORDER BY
    funnel_inputs.funnel asc
;

"""

    _, statements = parse(test_select, test_environment)
    query = test_executor.generate_sql(test_select)[0]

    assert "INVALID_REFERENCE_BUG" not in query


def test_funnel_with_remap(test_environment: Environment, test_executor: Executor):
    # test keys
    test_select = """

with funnel_inputs as 
WHERE distinct_id in visits
SELECT
    CASE 
        WHEN event_name = 'View Landing Page' then 1
        WHEN event_name = 'Sign Up' then 2
        WHEN event_name = 'New Canvas' then 3
        WHEN event_name = 'Start Subscription' then 4
    END->funnel,
    lag 1 funnel over distinct_id order by funnel asc -> prior_step,
    distinct_id
having 
    funnel = 1 or prior_step = funnel-1
;

# use our existing funnel analysis
merge funnel_inputs.funnel into step;
merge funnel_inputs.distinct_id into customer_id;
SELECT
    --step, 
    name,
    count(customer_id) -> customer_count,
    1.0 - customer_count::float/ lag 1 customer_count  order by step asc -> drop_off
ORDER BY
    step asc
;
"""

    _, statements = parse(test_select, test_environment)
    query = test_executor.generate_sql(test_select)[0]

    assert "INVALID_REFERENCE_BUG" not in query
