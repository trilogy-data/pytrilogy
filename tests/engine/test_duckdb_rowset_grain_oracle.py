"""Rowset grain identity, checked against hand-written DuckDB SQL: grain and
hidden-key bodies, renamed outputs, aggregates over members bare and inside
expressions, row derivations over handles, and subset/union joins of rowsets."""

import duckdb
import pytest

from trilogy import Dialects, Environment

ITEMS = "select * from (values (1,'A','x'),(2,'A','y'),(3,'B','x'),(4,'C',null)) as t(item_id,item_name,category)"
STORES = (
    "select * from (values (10,'S1'),(20,'S2'),(30,'S3')) as t(store_id,store_name)"
)
SALES = "select * from (values (100,1,10,2,5.0),(101,1,10,1,5.0),(102,2,20,3,4.0),(103,3,10,1,7.0),(104,3,20,2,7.0),(105,1,20,1,5.0)) as t(sale_id,item_id,store_id,qty,amt)"
RETURNS = "select * from (values (200,1,1.0),(201,1,2.0),(202,4,3.0),(203,null,9.0)) as t(ret_id,item_id,ret_amt)"

MODEL = "\nkey item_id int;\nproperty item_id.item_name string;\nproperty item_id.category string?;\ndatasource items (item_id: item_id, item_name: item_name, category: category) grain (item_id)\nquery '''select * from (values (1,'A','x'),(2,'A','y'),(3,'B','x'),(4,'C',null)) as t(item_id,item_name,category)''';\n\nkey store_id int;\nproperty store_id.store_name string;\ndatasource stores (store_id: store_id, store_name: store_name) grain (store_id)\nquery '''select * from (values (10,'S1'),(20,'S2'),(30,'S3')) as t(store_id,store_name)''';\n\nkey sale_id int;\nproperty sale_id.qty int;\nproperty sale_id.amt float;\ndatasource sales (sale_id: sale_id, item_id: item_id, store_id: store_id, qty: qty, amt: amt) grain (sale_id)\nquery '''select * from (values (100,1,10,2,5.0),(101,1,10,1,5.0),(102,2,20,3,4.0),(103,3,10,1,7.0),(104,3,20,2,7.0),(105,1,20,1,5.0)) as t(sale_id,item_id,store_id,qty,amt)''';\n\nkey ret_id int;\nproperty ret_id.ret_item int?;\nproperty ret_id.ret_amt float;\ndatasource returns (ret_id: ret_id, item_id: ret_item, ret_amt: ret_amt) grain (ret_id)\nquery '''select * from (values (200,1,1.0),(201,1,2.0),(202,4,3.0),(203,null,9.0)) as t(ret_id,item_id,ret_amt)''';\n"

CASES = [
    (
        "r1 body",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.item_id, r1.item_name, r1.rev;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select * from r1",
    ),
    (
        "r1 name only",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.item_name;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select distinct item_name from r1",
    ),
    (
        "r1 sum rev by name",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.item_name, sum(r1.rev) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select item_name, sum(rev) from r1 group by 1",
    ),
    (
        "r1 count key by name",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.item_name, count(r1.item_id) as n;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select item_name, count(distinct item_id) from r1 group by 1",
    ),
    (
        "r1 sum rev total",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect sum(r1.rev) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select sum(rev) from r1",
    ),
    (
        "r1 sum rev*2",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect sum(r1.rev * 2) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select sum(rev*2) from r1",
    ),
    (
        "r1 sum case name",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect sum(case when r1.item_name = 'A' then 1 else 0 end) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select sum(case when item_name='A' then 1 else 0 end) from r1",
    ),
    (
        "r1 sum case name by name",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.item_name, sum(case when r1.rev > 5 then 1 else 0 end) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select item_name, sum(case when rev>5 then 1 else 0 end) from r1 group by 1",
    ),
    (
        "r1 avg rev",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect avg(r1.rev) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select avg(rev) from r1",
    ),
    (
        "r1 count_distinct name",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect count_distinct(r1.item_name) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select count(distinct item_name) from r1",
    ),
    (
        "r1 count name",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect count(r1.item_name) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select count(item_name) from r1",
    ),
    (
        "r1 count grain",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect count(grain(r1.item_id, r1.item_name)) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select count(*) from r1",
    ),
    (
        "r1 basic concat alone",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect concat(r1.item_name, '-') as nm;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select distinct concat(item_name,'-') from r1",
    ),
    (
        "r1 basic plus with agg",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.item_name, sum(r1.rev + 1) as t;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select item_name, sum(rev+1) from r1 group by 1",
    ),
    (
        "r1 basic rev+1 alone",
        "with r1 as select item_id, item_name, sum(amt) as rev;\nselect r1.rev + 1 as x;",
        "with r1 as (select s.item_id, i.item_name, sum(amt) rev from sales s join items i using(item_id) group by 1,2) select distinct rev+1 from r1",
    ),
    (
        "r2 body",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_id, r2.store_id, r2.item_name, r2.q;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select * from r2",
    ),
    (
        "r2 item only",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_id;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select distinct item_id from r2",
    ),
    (
        "r2 store only",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.store_id;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select distinct store_id from r2",
    ),
    (
        "r2 sum q by item",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_id, sum(r2.q) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select item_id, sum(q) from r2 group by 1",
    ),
    (
        "r2 sum q by name",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_name, sum(r2.q) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select item_name, sum(q) from r2 group by 1",
    ),
    (
        "r2 count item",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect count(r2.item_id) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select count(distinct item_id) from r2",
    ),
    (
        "r2 count grain",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect count(grain(r2.item_id, r2.store_id)) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select count(*) from (select distinct item_id, coalesce(store_id,-1) from r2)",
    ),
    (
        "r2 count store by name",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_name, count(r2.store_id) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select item_name, count(distinct store_id) from r2 group by 1",
    ),
    (
        "r2 sum q*2 by store",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.store_id, sum(r2.q*2) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select store_id, sum(q*2) from r2 group by 1",
    ),
    (
        "r2 item+store basic",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_id + coalesce(r2.store_id,0) as x;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select distinct item_id + coalesce(store_id,0) from r2",
    ),
    (
        "r2 basic with sum",
        "with r2 as select item_id, store_id, item_name, sum(qty) as q;\nselect r2.item_id + coalesce(r2.store_id,0) as x, sum(r2.q) as t;",
        "with r2 as (select s.item_id, s.store_id, i.item_name, sum(qty) q from sales s join items i using(item_id) group by 1,2,3) select item_id + coalesce(store_id,0) x, sum(q) from r2 group by 1",
    ),
    (
        "r3 name+cat",
        "with r3 as select item_id, item_name, category;\nselect r3.item_name, r3.category;",
        "select distinct item_name, category from items",
    ),
    (
        "r3 count name",
        "with r3 as select item_id, item_name, category;\nselect count(r3.item_name) as n;",
        "select count(item_name) from items",
    ),
    (
        "r3 cat, count name",
        "with r3 as select item_id, item_name, category;\nselect r3.category, count(r3.item_name) as n;",
        "select category, count(item_name) from items group by 1",
    ),
    (
        "r3 name, count cat",
        "with r3 as select item_id, item_name, category;\nselect r3.item_name, count(r3.category) as n;",
        "select item_name, count(category) from items group by 1",
    ),
    (
        "renamed iid only",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect rr.iid;",
        "select distinct item_id from sales",
    ),
    (
        "renamed sum by iid",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect rr.iid, sum(rr.q) as t;",
        "select item_id, sum(qty) from sales group by 1",
    ),
    (
        "renamed sum total",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect sum(rr.q) as t;",
        "select sum(qty) from sales",
    ),
    (
        "renamed count sid",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect count(rr.sid) as t;",
        "select count(distinct store_id) from sales",
    ),
    (
        "renamed sum by sid",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect rr.sid, sum(rr.q) as t;",
        "select store_id, sum(qty) from sales group by 1",
    ),
    (
        "renamed avg q",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect avg(rr.q) as t;",
        "select avg(q) from (select item_id, store_id, sum(qty) q from sales group by 1,2)",
    ),
    (
        "renamed avg q by iid",
        "with rr as select item_id as iid, store_id as sid, sum(qty) as q;\nselect rr.iid, avg(rr.q) as t;",
        "select item_id, avg(q) from (select item_id, store_id, sum(qty) q from sales group by 1,2) group by 1",
    ),
    (
        "where body sum",
        "with wb as where store_id = 20 select item_id, sum(qty) as q;\nselect sum(wb.q) as t;",
        "select sum(qty) from sales where store_id=20",
    ),
    (
        "where body name",
        "with wb as where store_id = 20 select item_id, item_name, sum(qty) as q;\nselect wb.item_name, sum(wb.q) as t;",
        "select item_name, sum(qty) from sales join items using(item_id) where store_id=20 group by 1",
    ),
    (
        "having body",
        "with hb as select item_id, item_name, sum(qty) as q having q > 3;\nselect hb.item_name, count(hb.item_id) as n, sum(hb.q) as t;",
        "select item_name, count(*), sum(q) from (select item_id, item_name, sum(qty) q from sales join items using(item_id) group by 1,2 having sum(qty)>3) group by 1",
    ),
    (
        "limit body",
        "with lb as select item_id, sum(qty) as q order by q desc limit 2;\nselect sum(lb.q) as t, count(lb.item_id) as n;",
        "select sum(q), count(*) from (select item_id, sum(qty) q from sales group by 1 order by q desc limit 2)",
    ),
    (
        "limit body name only",
        "with lb as select item_id, item_name order by item_id asc limit 2;\nselect lb.item_name, count(lb.item_id) as n;",
        "select item_name, count(*) from (select item_id, item_name from items order by item_id limit 2) group by 1",
    ),
    (
        "reagg avg rev by name",
        "with ra as select item_id, sum(amt) as rev;\nselect ra.item_id, ra.rev;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select * from ra",
    ),
    (
        "reagg max/min",
        "with ra as select item_id, sum(amt) as rev;\nselect max(ra.rev) as mx, min(ra.rev) as mn, avg(ra.rev) as av;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select max(rev),min(rev),avg(rev) from ra",
    ),
    (
        "uj project",
        "with ra as select item_id, sum(amt) as rev;\nwith rb as select ret_item, sum(ret_amt) as r;\nselect ra.item_id, ra.rev, rb.r union join ra.item_id = rb.ret_item;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select coalesce(ra.item_id, rb.ret_item), rev, r from ra full join rb on ra.item_id = rb.ret_item",
    ),
    (
        "uj coalesce",
        "with ra as select item_id, sum(amt) as rev;\nwith rb as select ret_item, sum(ret_amt) as r;\nselect ra.item_id, coalesce(ra.rev,0) - coalesce(rb.r,0) as net union join ra.item_id = rb.ret_item;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select coalesce(ra.item_id, rb.ret_item), coalesce(rev,0)-coalesce(r,0) from ra full join rb on ra.item_id = rb.ret_item",
    ),
    (
        "uj sums",
        "with ra as select item_id, sum(amt) as rev;\nwith rb as select ret_item, sum(ret_amt) as r;\nselect sum(ra.rev) as a, sum(rb.r) as b union join ra.item_id = rb.ret_item;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select sum(rev), sum(r) from ra full join rb on ra.item_id = rb.ret_item",
    ),
    (
        "uj where other side",
        "with ra as select item_id, sum(amt) as rev;\nwith rb as select ret_item, sum(ret_amt) as r;\nwhere rb.r > 2 select ra.item_id, ra.rev union join ra.item_id = rb.ret_item;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select coalesce(ra.item_id, rb.ret_item), rev from ra full join rb on ra.item_id = rb.ret_item where r > 2",
    ),
    (
        "uj key not null",
        "with ra as select item_id, sum(amt) as rev;\nwith rb as select ret_item, sum(ret_amt) as r;\nwhere ra.item_id is not null select ra.item_id, ra.rev, rb.r union join ra.item_id = rb.ret_item;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select ra.item_id, rev, r from ra left join rb on ra.item_id = rb.ret_item",
    ),
    (
        "sj rb into items count",
        "with rb as select ret_item, sum(ret_amt) as r;\nselect category, count(item_id) as n subset join rb.ret_item = item_id;",
        "select category, count(*) from items group by 1",
    ),
    (
        "sj fin into items count",
        "with fin as select item_id, sale_id, store_id;\nselect category, count(item_id) as n subset join fin.item_id = item_id;",
        "select category, count(*) from items group by 1",
    ),
    (
        "sj fin into items where",
        "with fin as select item_id, sale_id, store_id;\nwhere fin.store_id = 20 select category, count(item_id) as n subset join fin.item_id = item_id;",
        "select category, count(distinct item_id) from items join sales using(item_id) where store_id = 20 group by 1",
    ),
    (
        "sj fin into items notnull",
        "with fin as select item_id, sale_id, store_id;\nwhere fin.item_id is not null select item_name, count(item_id) as n subset join fin.item_id = item_id;",
        "select item_name, count(distinct item_id) from items where item_id in (select item_id from sales) group by 1",
    ),
    (
        "sj rowset anchor fin",
        "with ra as select item_id, sum(amt) as rev;\nwith fin as select item_id, sale_id, store_id;\nselect ra.item_id, ra.rev, count(fin.sale_id) as n subset join fin.item_id = ra.item_id;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select ra.item_id, rev, count(sale_id) from ra left join fin using(item_id) group by 1,2",
    ),
    (
        "sj rowset anchor fin sum",
        "with ra as select item_id, sum(amt) as rev;\nwith fin as select item_id, sale_id, store_id;\nselect sum(ra.rev) as t subset join fin.item_id = ra.item_id;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1), rb as (select item_id ret_item, sum(ret_amt) r from returns group by 1), fin as (select item_id, sale_id, store_id from sales) select sum(rev) from ra",
    ),
    (
        "uj two fine count",
        "with fin as select item_id, sale_id, store_id;\nwith fr as select ret_item, ret_id;\nselect count(fin.sale_id) as n, count(fr.ret_id) as m union join fr.ret_item = fin.item_id;",
        "select 6, 4",
    ),
    (
        "membership",
        "with rb as select ret_item, sum(ret_amt) as r;\nselect item_name where item_id in rb.ret_item;",
        "select distinct item_name from items where item_id in (select item_id from returns where item_id is not null)",
    ),
    (
        "membership count",
        "with fin as select item_id, sale_id, store_id;\nselect category, count(item_id) as n where item_id in fin.item_id;",
        "select category, count(*) from items where item_id in (select item_id from sales) group by 1",
    ),
    (
        "membership filtered",
        "with fin as select item_id, sale_id, store_id;\nselect item_name, count(item_id) as n where item_id in (fin.item_id ? fin.store_id = 10);",
        "select item_name, count(*) from items where item_id in (select item_id from sales where store_id = 10) group by 1",
    ),
    (
        "agg by rs key with join, outputs key",
        "with ra as select item_id, sum(amt) as rev;\nselect ra.item_id, sum(qty) as t subset join ra.item_id = item_id;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1) select item_id, sum(qty) from sales group by 1",
    ),
    (
        "agg by rs key equal join",
        "with ra as select item_id, sum(amt) as rev;\nselect ra.item_id, ra.rev, sum(qty) as t equal join ra.item_id = item_id;",
        "with ra as (select item_id, sum(amt) rev from sales group by 1) select item_id, rev, sum(qty) from ra join sales using(item_id) group by 1,2",
    ),
    (
        "basic ratio",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect r2.item_id, r2.store_id, r2.a / r2.q as unit;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select item_id, store_id, a/q from r2",
    ),
    (
        "basic ratio alone",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect r2.a / r2.q as unit;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select distinct a/q from r2",
    ),
    (
        "basic ratio sum",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect sum(r2.a / r2.q) as s;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select sum(a/q) from r2",
    ),
    (
        "basic ratio by item",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect r2.item_id, sum(r2.a / r2.q) as s;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select item_id, sum(a/q) from r2 group by 1",
    ),
    (
        "basic two renames",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect r2.q as qq, r2.a as aa;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select distinct q, a from r2",
    ),
    (
        "basic two renames + key",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect r2.item_id as ii, r2.q as qq, r2.a as aa;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select distinct item_id, q, a from r2",
    ),
    (
        "renames with agg",
        "with r2 as select item_id, store_id, sum(qty) as q, sum(amt) as a;\nselect r2.item_id as ii, sum(r2.q) as sq, max(r2.a) as ma;",
        "with r2 as (select item_id, store_id, sum(qty) q, sum(amt) a from sales group by 1,2) select item_id, sum(q), max(a) from r2 group by 1",
    ),
    (
        "hidden key name sum",
        "with h as select sale_id, item_name, store_name, qty;\nselect h.item_name, sum(h.qty) as t;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select item_name, sum(qty) from h group by 1",
    ),
    (
        "hidden key name count",
        "with h as select sale_id, item_name, store_name, qty;\nselect h.item_name, count(h.sale_id) as t;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select item_name, count(*) from h group by 1",
    ),
    (
        "hidden key name x store",
        "with h as select sale_id, item_name, store_name, qty;\nselect h.item_name, h.store_name, sum(h.qty) as t;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select item_name, store_name, sum(qty) from h group by 1,2",
    ),
    (
        "hidden key count name",
        "with h as select sale_id, item_name, store_name, qty;\nselect count(h.item_name) as t;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select count(item_name) from h",
    ),
    (
        "hidden key cd name",
        "with h as select sale_id, item_name, store_name, qty;\nselect count_distinct(h.item_name) as t;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select count(distinct item_name) from h",
    ),
    (
        "hidden key project names",
        "with h as select sale_id, item_name, store_name, qty;\nselect h.item_name, h.store_name;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select distinct item_name, store_name from h",
    ),
    (
        "grainless h2 sum",
        "with h2 as select item_name, store_name, sum(qty) as q;\nselect h2.item_name, sum(h2.q) as t;",
        "with h2 as (select item_name, store_name, sum(qty) q from sales join items using(item_id) join stores using(store_id) group by 1,2) select item_name, sum(q) from h2 group by 1",
    ),
    (
        "grainless h2 total",
        "with h2 as select item_name, store_name, sum(qty) as q;\nselect sum(h2.q) as t;",
        "with h2 as (select item_name, store_name, sum(qty) q from sales join items using(item_id) join stores using(store_id) group by 1,2) select sum(q) from h2",
    ),
    (
        "grainless h2 count name",
        "with h2 as select item_name, store_name, sum(qty) as q;\nselect count(h2.item_name) as t;",
        "with h2 as (select item_name, store_name, sum(qty) q from sales join items using(item_id) join stores using(store_id) group by 1,2) select count(item_name) from h2",
    ),
    (
        "grainless h2 by store",
        "with h2 as select item_name, store_name, sum(qty) as q;\nselect h2.store_name, count(h2.item_name) as n, sum(h2.q) as t;",
        "with h2 as (select item_name, store_name, sum(qty) q from sales join items using(item_id) join stores using(store_id) group by 1,2) select store_name, count(item_name), sum(q) from h2 group by 1",
    ),
    (
        "h3 renamed props",
        "with h3 as select item_name as nm, category as cat;\nselect h3.cat, count(h3.nm) as n;",
        "select category, count(item_name) from (select distinct item_name, category from items) group by 1",
    ),
    (
        "h3 renamed props alone",
        "with h3 as select item_name as nm, category as cat;\nselect h3.nm;",
        "select distinct item_name from items",
    ),
    (
        "h3 renamed concat",
        "with h3 as select item_name as nm, category as cat;\nselect concat(h3.nm, coalesce(h3.cat,'-')) as c;",
        "select distinct concat(item_name, coalesce(category,'-')) from items",
    ),
    (
        "h3 renamed count grain",
        "with h3 as select item_name as nm, category as cat;\nselect count(grain(h3.nm, h3.cat)) as n;",
        "select count(*) from (select distinct item_name, category from items)",
    ),
    (
        "hidden key with dim join",
        "with h as select sale_id, item_name, store_name, qty;\nselect h.store_name, sum(h.qty) as t, count(h.sale_id) as c;",
        "with h as (select sale_id, item_name, store_name, qty from sales join items using(item_id) join stores using(store_id)) select store_name, sum(qty), count(*) from h group by 1",
    ),
    (
        "rs over rs",
        "with h2 as select item_name, store_name, sum(qty) as q;\nwith h4 as select h2.item_name, sum(h2.q) as tq;\nselect sum(h4.tq) as t, count(h4.item_name) as n;",
        "with h2 as (select item_name, store_name, sum(qty) q from sales join items using(item_id) join stores using(store_id) group by 1,2) select sum(q), count(distinct item_name) from h2",
    ),
    (
        "rs over rs avg",
        "with h2 as select item_name, store_name, sum(qty) as q;\nwith h4 as select h2.store_name, avg(h2.q) as aq;\nselect h4.store_name, h4.aq;",
        "with h2 as (select item_name, store_name, sum(qty) q from sales join items using(item_id) join stores using(store_id) group by 1,2) select store_name, avg(q) from h2 group by 1",
    ),
    (
        "sj fine sum measure notnull",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, sum(fq.qty) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(qty) from items join fq using(item_id) group by 1",
    ),
    (
        "sj fine sum amt total",
        "with fq as select sale_id, item_id, qty, amt;\nselect sum(fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select sum(amt) from fq",
    ),
    (
        "expr qty*amt by name",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, sum(fq.qty * fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(qty*amt) from items join fq using(item_id) group by 1",
    ),
    (
        "expr qty*amt total",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select sum(fq.qty * fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select sum(qty*amt) from fq",
    ),
    (
        "expr case by name",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, sum(case when fq.qty = 1 then fq.amt else 0 end) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(case when qty=1 then amt else 0 end) from items join fq using(item_id) group by 1",
    ),
    (
        "expr qty+0",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, sum(fq.qty + 0) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(qty) from items join fq using(item_id) group by 1",
    ),
    (
        "bare amt",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, sum(fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(amt) from items join fq using(item_id) group by 1",
    ),
    (
        "bare amt & qty",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, sum(fq.amt) as t, sum(fq.qty) as q subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(amt), sum(qty) from items join fq using(item_id) group by 1",
    ),
    (
        "count amt",
        "with fq as select sale_id, item_id, qty, amt;\nwhere fq.item_id is not null select item_name, count(fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, count(amt) from items join fq using(item_id) group by 1",
    ),
    (
        "no join expr",
        "with fq as select sale_id, item_id, qty, amt;\nselect sum(fq.qty * fq.amt) as t;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select sum(qty*amt) from fq",
    ),
    (
        "no join expr by item",
        "with fq as select sale_id, item_id, qty, amt;\nselect fq.item_id, sum(fq.qty * fq.amt) as t;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_id, sum(qty*amt) from fq group by 1",
    ),
    (
        "expr qty*amt by name 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(fq.qty * fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(qty*amt) from fq join items using(item_id) group by 1",
    ),
    (
        "expr qty*amt total 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect sum(fq.qty * fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select sum(qty*amt) from fq",
    ),
    (
        "expr case by name 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(case when fq.qty = 1 then fq.amt else 0 end) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(case when qty=1 then amt else 0 end) from fq join items using(item_id) group by 1",
    ),
    (
        "expr qty+0 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(fq.qty + 0) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(qty) from fq join items using(item_id) group by 1",
    ),
    (
        "bare amt 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(amt) from fq join items using(item_id) group by 1",
    ),
    (
        "bare amt & qty 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(fq.amt) as t, sum(fq.qty) as q subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, sum(amt), sum(qty) from fq join items using(item_id) group by 1",
    ),
    (
        "count amt 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, count(fq.amt) as t subset join fq.item_id = item_id;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_name, count(amt) from fq join items using(item_id) group by 1",
    ),
    (
        "no join expr 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect sum(fq.qty * fq.amt) as t;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select sum(qty*amt) from fq",
    ),
    (
        "no join expr by item 2",
        "with fq as select sale_id, item_id, qty, amt;\nselect fq.item_id, sum(fq.qty * fq.amt) as t;",
        "with fq as (select sale_id, item_id, qty, amt from sales) select item_id, sum(qty*amt) from fq group by 1",
    ),
    (
        "inline product under subset join",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(fq.qty * fq.amt) as t subset join fq.item_id = item_id;",
        "select item_name, sum(qty * amt) from items join sales using (item_id) group by 1",
    ),
    (
        "inline product total under subset join",
        "with fq as select sale_id, item_id, qty, amt;\nselect sum(fq.qty * fq.amt) as t subset join fq.item_id = item_id;",
        "select sum(qty * amt) from sales",
    ),
    (
        "inline case under subset join",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(case when fq.qty = 1 then fq.amt else 0 end) as t subset join fq.item_id = item_id;",
        "select item_name, sum(case when qty = 1 then amt else 0 end) from items join sales using (item_id) group by 1",
    ),
    (
        "inline plus zero under subset join",
        "with fq as select sale_id, item_id, qty, amt;\nselect item_name, sum(fq.qty + 0) as t subset join fq.item_id = item_id;",
        "select item_name, sum(qty) from items join sales using (item_id) group by 1",
    ),
    (
        "inline product rowset target",
        "with fq as select sale_id, item_id, qty, amt;\nwith w as select item_id, item_name;\nselect w.item_name, sum(fq.qty * fq.amt) as t subset join fq.item_id = w.item_id;",
        "select item_name, sum(qty * amt) from items join sales using (item_id) group by 1",
    ),
]


@pytest.fixture(scope="module")
def oracle():
    con = duckdb.connect()
    con.execute(f"create table items as {ITEMS}")
    con.execute(f"create table stores as {STORES}")
    con.execute(f"create table sales as {SALES}")
    con.execute(f"create table returns as {RETURNS}")
    yield con
    con.close()


def _norm(rows):
    out = [
        tuple(
            (
                round(float(v), 4)
                if hasattr(v, "is_integer") or type(v).__name__ == "Decimal"
                else v
            )
            for v in r
        )
        for r in rows
    ]
    return sorted(out, key=lambda r: tuple((v is None, str(v)) for v in r))


@pytest.mark.parametrize("name,query,expected_sql", CASES, ids=[c[0] for c in CASES])
def test_matches_hand_written_sql(oracle, name, query, expected_sql):
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    got = executor.execute_query(query).fetchall()
    assert _norm(got) == _norm(oracle.execute(expected_sql).fetchall()), name
