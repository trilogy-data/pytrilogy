"""Mirror q64's enrich-later structure on a tiny model:
- item: join key (in both arms, renamed) -> enrich product_name via the chain
- addr: arm-A-only dim (NOT a join key) -> enrich city from its id
Does the arm-only property enrich without a chain (since it doesn't collide)?
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key item_id int;
property item_id.pname string;
key addr_id int;
property addr_id.city string;
key sale_id int;
property sale_id.year int;

datasource sales (
    sale_id: sale_id,
    item_id: item_id,
    addr_id: addr_id,
    yr: year,
)
grain (sale_id)
query '''
select 1 sale_id, 1 item_id, 10 addr_id, 2001 yr
union all select 2 sale_id, 1 item_id, 10 addr_id, 2002 yr
union all select 3 sale_id, 2 item_id, 20 addr_id, 2001 yr
''';

datasource items (
    item_id: item_id,
    pname: pname,
)
grain (item_id)
query '''select 1 item_id, 'Widget' pname union all select 2 item_id, 'Gadget' pname''';

datasource addrs (
    addr_id: addr_id,
    city: city,
)
grain (addr_id)
query '''select 10 addr_id, 'Springfield' city union all select 20 addr_id, 'Shelbyville' city''';

rowset agg_a <- where year = 2001
select item_id as isk_a, addr_id as ad_a, count(sale_id) as cnt_a;
rowset agg_b <- where year = 2002
select item_id as isk_b, count(sale_id) as cnt_b;
"""

VARIANTS = {
    "join-key chained + arm-only addr renamed, enrich both text": MODEL + """
inner join agg_a.isk_a = agg_b.isk_b = item_id
select pname, agg_a.ad_a, count(sale_id) as x, agg_a.cnt_a, agg_b.cnt_b
order by pname asc;
""",
    "enrich pname (chain) + city from arm-only addr id": MODEL + """
inner join agg_a.isk_a = agg_b.isk_b = item_id
select pname, city, agg_a.cnt_a, agg_b.cnt_b
order by pname asc;
""",
    "enrich pname + city, chain addr too": MODEL + """
inner join agg_a.isk_a = agg_b.isk_b = item_id
inner join agg_a.ad_a = addr_id
select pname, city, agg_a.cnt_a, agg_b.cnt_b
order by pname asc;
""",
}

for label, q in VARIANTS.items():
    eng = Dialects.DUCK_DB.default_executor(environment=Environment())
    try:
        rows = [tuple(r) for r in eng.execute_text(q)[-1].fetchall()]
        print(f"{label}:\n   PLANNED -> {rows}")
    except Exception as e:
        print(f"{label}:\n   FAILED {type(e).__name__}: {str(e)[:120]}")
