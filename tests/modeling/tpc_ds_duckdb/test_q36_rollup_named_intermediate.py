"""q36's rollup + partitioned window where the window's CASE partition key reaches
the grouping output through a *chained* named intermediate (`parent` -> `level` ->
`g_cat`/`g_class`).

The canonical query36.preql conditions its CASE on `g_class` directly (one hop),
which is why the modeling suite never covered the chained spelling. Chained, the
CASE concept is key-transitivity-anchored to item grain, splits from its rollup
siblings, and a redundant item/fact-grain scan survives and INNER JOINs back onto
the ROLLUP output on category/class -- keys that are neither unique nor non-NULL
on subtotal rows -- fanning 133 rows out to 9030 (or 535k for variant D).
"""

from collections import Counter

TAIL = """
where ss.sale_date.year = 2001 and ss.store.state = 'TN'
select
    gm,
    ss.item.category,
    ss.item.class,
    level,
    rnk
by rollup (ss.item.category, ss.item.class)
order by level desc nulls first, ss.item.category asc nulls first, rnk asc nulls first
;
"""

# chained aggregate AND chained grouping intermediates
NAMED_FULL = """
import store_sales as ss;
auto total_profit <- sum(ss.net_profit);
auto total_sales <- sum(ss.ext_sales_price);
auto gm <- total_profit / total_sales;
auto g_cat <- grouping(ss.item.category);
auto g_class <- grouping(ss.item.class);
auto level <- g_cat + g_class;
auto parent <- case when level = 0 then ss.item.category else null end;
auto rnk <- rank(ss.item.category, ss.item.class)
    over (partition by level, parent order by gm asc);
""" + TAIL

# inline aggregates, chained grouping intermediates only
NAMED_GROUPING_ONLY = """
import store_sales as ss;
auto gm <- sum(ss.net_profit) / sum(ss.ext_sales_price);
auto g_cat <- grouping(ss.item.category);
auto g_class <- grouping(ss.item.class);
auto level <- g_cat + g_class;
auto parent <- case when level = 0 then ss.item.category else null end;
auto rnk <- rank(ss.item.category, ss.item.class)
    over (partition by level, parent order by gm asc);
""" + TAIL

# the one-hop spelling the canonical query uses -- already clean
ONE_HOP = """
import store_sales as ss;
auto gm <- sum(ss.net_profit) / sum(ss.ext_sales_price);
auto g_cat <- grouping(ss.item.category);
auto g_class <- grouping(ss.item.class);
auto level <- g_cat + g_class;
auto parent <- case when g_class = 0 then ss.item.category else null end;
auto rnk <- rank(ss.item.category, ss.item.class)
    over (partition by level, parent order by gm asc);
""" + TAIL

TRUTH = """
select
    sum(ss_net_profit) / sum(ss_ext_sales_price) as gm,
    i_category,
    i_class,
    grouping(i_category) + grouping(i_class) as level,
    rank() over (
        partition by grouping(i_category) + grouping(i_class),
        case when grouping(i_category) + grouping(i_class) = 0 then i_category end
        order by sum(ss_net_profit) / sum(ss_ext_sales_price) asc) as rnk
from memory.store_sales, memory.date_dim d1, memory.item, memory.store
where d1.d_year = 2001
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and s_state = 'TN'
group by rollup (i_category, i_class)
"""


def _rows(engine, query: str) -> Counter:
    return Counter(engine.execute_raw_sql(engine.generate_sql(query)[-1]).fetchall())


def test_q36_named_intermediate_chain_matches_control(engine):
    expected = Counter(engine.execute_raw_sql(TRUTH).fetchall())
    assert _rows(engine, ONE_HOP) == expected
    assert _rows(engine, NAMED_GROUPING_ONLY) == expected
    assert _rows(engine, NAMED_FULL) == expected
