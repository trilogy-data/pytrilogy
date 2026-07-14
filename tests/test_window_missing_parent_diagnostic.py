"""A window whose parents resolve incompletely (q51 shape: windows over the
coalesced keys of a union-join rowset) must surface an actionable resolution
error, never a bare ``raise SyntaxError`` (which the CLI rendered as
"Syntax error in <file>: None")."""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    UnresolvableQueryException,
)

QUERY = """
key web_item int;
key web_date date;
property <web_item, web_date>.web_price float?;

key store_item int;
key store_date date;
property <store_item, store_date>.store_price float?;

datasource web_fact (
    i: web_item,
    d: web_date,
    p: web_price,
)
grain (web_item, web_date)
address web_fact;

datasource store_fact (
    i: store_item,
    d: store_date,
    p: store_price,
)
grain (store_item, store_date)
address store_fact;

with wd as
select
    web_item as item_sk,
    web_date as sale_date,
    sum(web_price) as web_daily_total,
    sum(web_price) over (partition by web_item order by web_date) as web_running_total
;

with sd as
select
    store_item as item_sk,
    store_date as sale_date,
    sum(store_price) as store_daily_total,
    sum(store_price) over (partition by store_item order by store_date) as store_running_total
;

with combined as
select
    coalesce(wd.item_sk, sd.item_sk) as item_sk,
    coalesce(wd.sale_date, sd.sale_date) as sale_date,
    wd.web_running_total as web_displayed,
    sd.store_running_total as store_displayed,
    max(wd.web_running_total) over (partition by coalesce(wd.item_sk, sd.item_sk) order by coalesce(wd.sale_date, sd.sale_date)) as web_running_max,
    max(sd.store_running_total) over (partition by coalesce(wd.item_sk, sd.item_sk) order by coalesce(wd.sale_date, sd.sale_date)) as store_running_max
union join wd.item_sk = sd.item_sk
union join wd.sale_date = sd.sale_date
;

where combined.web_running_max > combined.store_running_max
select
    combined.item_sk,
    combined.sale_date,
    combined.web_running_max,
    combined.store_running_max
limit 100;
"""


def test_window_missing_parent_surfaces_actionable_error():
    env = Environment()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        executor.generate_sql(QUERY)
    except SyntaxError as e:
        pytest.fail(
            f"planner dead-end surfaced as a SyntaxError ({e!r}); the CLI "
            "renders this as 'Syntax error: None' with no location or cause"
        )
    except (DisconnectedConceptsException, UnresolvableQueryException) as e:
        # acceptable: a typed resolution error with a real message
        assert str(e).strip(), "resolution error must carry a message"
