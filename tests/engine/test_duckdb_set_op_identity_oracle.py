"""Set-op rowsets (union/except/intersect), checked against hand-written DuckDB
SQL: projections are distinct tuples, aggregates range over the stacked rows,
nested set ops, joins and memberships over them."""

import duckdb
import pytest

from trilogy import Dialects, Environment

LINES = "select * from (values\n(1,1,'a',2001,10,1),(2,1,'a',2001,10,2),(3,2,NULL,2001,5,1),(4,3,'b',2001,7,1),\n(5,1,'a',2002,10,1),(6,2,NULL,2002,5,2),(7,4,'c',2003,3,2),(8,1,'a',2011,2,1),\n(9,2,'b',2011,4,1),(10,1,'b',2012,6,2),(11,2,'a',2012,5,1)) as t(line_id,item_id,cat,yr,val,store_id)"
ITEMS = "select * from (values (1,'apple',1.5),(2,'pear',2.0),(3,'fig',3.0),(4,'kiwi',4.0),(5,'plum',5.0)) as t(item_id,name,price)"
STORES = "select * from (values (1,'TX'),(2,'GA'),(3,'NY')) as t(store_id,state)"

MODEL = "\nkey item_id int;\nproperty item_id.item_name string;\nproperty item_id.price float;\nkey store_id int;\nproperty store_id.state string;\nkey line_id int;\nproperty line_id.cat string?;\nproperty line_id.yr int;\nproperty line_id.val int;\n\ndatasource items (item_id: item_id, name: item_name, price: price) grain (item_id)\nquery '''select * from (values (1,'apple',1.5),(2,'pear',2.0),(3,'fig',3.0),(4,'kiwi',4.0),(5,'plum',5.0)) as t(item_id,name,price)''';\ndatasource stores (store_id: store_id, state: state) grain (store_id)\nquery '''select * from (values (1,'TX'),(2,'GA'),(3,'NY')) as t(store_id,state)''';\ndatasource lines (line_id: line_id, item_id: item_id, cat: cat, yr: yr, val: val, store_id: store_id)\ngrain (line_id)\nquery '''select * from (values\n(1,1,'a',2001,10,1),(2,1,'a',2001,10,2),(3,2,NULL,2001,5,1),(4,3,'b',2001,7,1),\n(5,1,'a',2002,10,1),(6,2,NULL,2002,5,2),(7,4,'c',2003,3,2),(8,1,'a',2011,2,1),\n(9,2,'b',2011,4,1),(10,1,'b',2012,6,2),(11,2,'a',2012,5,1)) as t(line_id,item_id,cat,yr,val,store_id)''';\n"

CASES = [
    (
        "proj_distinct",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect c.k;",
        "select distinct k from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "sum_stack",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect sum(c.k) as s;",
        "select sum(k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "count_key_distinct",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect count(c.k) as n;",
        "select count(distinct k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "min_max",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect min(c.k) as a, max(c.k) as b;",
        "select min(k), max(k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "avg_stack",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect avg(c.k) as a;",
        "select avg(k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "count_distinct_fn",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect count_distinct(c.k) as a;",
        "select count(distinct k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "kv_proj",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.k, c.v;",
        "select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "kv_group_sum",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.k, sum(c.v) as s;",
        "select k, sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by k",
    ),
    (
        "kv_total_sum",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect sum(c.v) as s;",
        "select sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "kv_count_v",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect count(c.v) as s;",
        "select count(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "kv_group_count_v",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.k, count(c.v) as s;",
        "select k, count(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by k",
    ),
    (
        "kv_v_only",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.v;",
        "select distinct v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "kv_v_group_count_k",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.v, count(c.k) as n;",
        "select v, count(distinct k) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by v",
    ),
    (
        "kv_v_group_sum_k",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.v, sum(c.k) as n;",
        "select v, sum(k) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by v",
    ),
    (
        "kv_where",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwhere c.v > 5 select c.k, sum(c.v) as s;",
        "select k, sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) where v>5 group by k",
    ),
    (
        "kv_having",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.k, sum(c.v) as s having s > 15;",
        "select k, sum(v) s from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by k having s>15",
    ),
    (
        "two_col_named",
        "with c as union(\n (where yr = 2001 select item_id, cat -> cc),\n (where yr = 2002 select item_id, cat -> cc)\n) -> (iid, cc);\nselect c.iid, c.cc;",
        "select distinct item_id, cat from lines where yr in (2001,2002)",
    ),
    (
        "prop_only_sum",
        "with c as union(\n (where yr = 2001 select val -> v),\n (where yr = 2002 select val -> v)\n) -> (v);\nselect sum(c.v) as s;",
        "select sum(v) from ((select distinct val v from lines where yr=2001) union all (select distinct val v from lines where yr=2002))",
    ),
    (
        "prop_only_count",
        "with c as union(\n (where yr = 2001 select val -> v),\n (where yr = 2002 select val -> v)\n) -> (v);\nselect count(c.v) as s;",
        "select count(distinct v) from ((select distinct val v from lines where yr=2001) union all (select distinct val v from lines where yr=2002))",
    ),
    (
        "const_tag_proj",
        "with c as union(\n (where yr = 2001 select item_id -> k, 'x' -> tag),\n (where yr = 2002 select item_id -> k, 'y' -> tag)\n) -> (k, tag);\nselect c.tag, count(c.k) as n;",
        "select tag, count(distinct k) from (select k, 'x' tag from (select distinct item_id k from lines where yr=2001) union all select k, 'y' from (select distinct item_id k from lines where yr=2002)) group by tag",
    ),
    (
        "const_tag_sum",
        "with c as union(\n (where yr = 2001 select item_id -> k, 'x' -> tag),\n (where yr = 2002 select item_id -> k, 'y' -> tag)\n) -> (k, tag);\nselect c.tag, sum(c.k) as n;",
        "select tag, sum(k) from (select k, 'x' tag from (select distinct item_id k from lines where yr=2001) union all select k, 'y' from (select distinct item_id k from lines where yr=2002)) group by tag",
    ),
    (
        "const_one_sum",
        "with c as union(\n (where yr = 2001 select item_id -> k, 1 -> one),\n (where yr = 2002 select item_id -> k, 1 -> one)\n) -> (k, one);\nselect sum(c.one) as n;",
        "select count(*) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "const_one_by_k",
        "with c as union(\n (where yr = 2001 select item_id -> k, 1 -> one),\n (where yr = 2002 select item_id -> k, 1 -> one)\n) -> (k, one);\nselect c.k, sum(c.one) as n;",
        "select k, count(*) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002)) group by k",
    ),
    (
        "three_arm_sum",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nselect sum(c.k) as s;",
        "select sum(k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002) union all (select distinct item_id k from lines where yr=2011))",
    ),
    (
        "three_arm_proj",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nselect c.k;",
        "select distinct k from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002) union all (select distinct item_id k from lines where yr=2011))",
    ),
    (
        "inline_top_proj",
        "from union((where yr = 2001 select item_id -> k),(where yr = 2002 select item_id -> k)) -> (k) select k;",
        "select distinct k from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "inline_top_sum",
        "from union((where yr = 2001 select item_id -> k),(where yr = 2002 select item_id -> k)) -> (k) select sum(k) -> s;",
        "select sum(k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "inline_kv",
        "from union((where yr = 2001 select item_id -> k, sum(val) -> v),(where yr = 2002 select item_id -> k, sum(val) -> v)) -> (k, v) select k, v;",
        "select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "rowset_cols",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.k, c.v;",
        "select distinct k,v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "union_over_union_sum",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nwith outer_u as union(\n (select inner_u.k -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nselect sum(outer_u.k) as s;",
        "select sum(k) from ((select distinct k from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))) union all (select distinct item_id k from lines where yr=2011))",
    ),
    (
        "union_over_union_proj",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nwith outer_u as union(\n (select inner_u.k -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nselect outer_u.k;",
        "select distinct k from ((select distinct k from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002))) union all (select distinct item_id k from lines where yr=2011))",
    ),
    (
        "union_over_union_inner_agg",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nwith outer_u as union(\n (select inner_u.k -> k, sum(inner_u.k) -> s),\n (where yr = 2011 select item_id -> k, sum(item_id) -> s)\n) -> (k, s);\nselect outer_u.k, sum(outer_u.s) as t;",
        "select k, sum(s) from ((select k, sum(k) s from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002)) group by k) union all (select item_id k, sum(item_id) s from (select distinct line_id, item_id from lines where yr=2011) group by 1)) group by k",
    ),
    (
        "union_over_intersect_sum",
        "with inter as intersect(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nwith u as union(\n (select inter.k -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nselect sum(u.k) as s;",
        "select sum(k) from (((select distinct item_id k from lines where yr=2001) intersect (select distinct item_id k from lines where yr=2002)) union all (select distinct item_id k from lines where yr=2011))",
    ),
    (
        "union_over_intersect_cnt",
        "with inter as intersect(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nwith u as union(\n (select inter.k -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nselect count(u.k) as s;",
        "select count(distinct k) from (((select distinct item_id k from lines where yr=2001) intersect (select distinct item_id k from lines where yr=2002)) union all (select distinct item_id k from lines where yr=2011))",
    ),
    (
        "except_over_union",
        "with u as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nwith ex as except(\n (select u.k -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect ex.k;",
        "select k from (((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2011)) except (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "except_over_union_sum",
        "with u as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nwith ex as except(\n (select u.k -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect sum(ex.k) as s;",
        "select sum(k) from (((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2011)) except (select distinct item_id k from lines where yr=2002))",
    ),
    (
        "intersect_two_col",
        "with x as intersect(\n (where yr = 2001 select item_id -> k, cat -> c),\n (where yr = 2002 select item_id -> k, cat -> c)\n) -> (k, c);\nselect x.k, x.c;",
        "select distinct item_id, cat from lines where yr=2001 intersect select distinct item_id, cat from lines where yr=2002",
    ),
    (
        "intersect_two_col_count",
        "with x as intersect(\n (where yr = 2001 select item_id -> k, cat -> c),\n (where yr = 2002 select item_id -> k, cat -> c)\n) -> (k, c);\nselect count(x.k) as n;",
        "select count(distinct item_id) from (select distinct item_id, cat from lines where yr=2001 intersect select distinct item_id, cat from lines where yr=2002)",
    ),
    (
        "except_kv_sum_v",
        "with x as except(\n (where yr = 2001 select item_id -> k, val -> v),\n (where yr = 2002 select item_id -> k, val -> v)\n) -> (k, v);\nselect sum(x.v) as s;",
        "select sum(v) from (select distinct item_id k, val v from lines where yr=2001 except select distinct item_id, val from lines where yr=2002)",
    ),
    (
        "membership_union_rowset",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select item_id -> k)\n) -> (k);\nselect item_name where item_id in c.k;",
        "select distinct name from items where item_id in (select k from ((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2002)))",
    ),
    (
        "keys_two_models_sum",
        "with m as union(\n (select item_id -> k),\n (select store_id -> k)\n) -> (k);\nselect sum(m.k) as s;",
        "select sum(k) from (select item_id k from items union all select store_id from stores)",
    ),
    (
        "keys_two_models_proj",
        "with m as union(\n (select item_id -> k),\n (select store_id -> k)\n) -> (k);\nselect m.k;",
        "select distinct k from (select item_id k from items union all select store_id from stores)",
    ),
    (
        "explicit_metric_purpose_grp",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v metric int);\nselect c.k, sum(c.v) as s;",
        "select k, sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by k",
    ),
    (
        "explicit_metric_purpose_tot",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v metric int);\nselect sum(c.v) as s;",
        "select sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "explicit_key_purpose_tot",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v key int);\nselect sum(c.v) as s;",
        "select sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "explicit_key_purpose_cnt",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v key int);\nselect count(c.v) as s;",
        "select count(distinct v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "mixed_key_nonkey_sum",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select val -> k)\n) -> (k);\nselect sum(c.k) as s;",
        "select sum(k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct val from lines where yr=2002))",
    ),
    (
        "mixed_key_nonkey_cnt",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select val -> k)\n) -> (k);\nselect count(c.k) as s;",
        "select count(distinct k) from ((select distinct item_id k from lines where yr=2001) union all (select distinct val from lines where yr=2002))",
    ),
    (
        "mixed_key_nonkey_proj",
        "with c as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2002 select val -> k)\n) -> (k);\nselect c.k;",
        "select distinct k from ((select distinct item_id k from lines where yr=2001) union all (select distinct val from lines where yr=2002))",
    ),
    (
        "two_models_kv_proj",
        "with c as union(\n (select item_id -> k, item_name -> nm),\n (select store_id -> k, state -> nm)\n) -> (k, nm);\nselect c.k, c.nm;",
        "select distinct k, nm from ((select item_id k, name nm from items) union all (select store_id, state from stores))",
    ),
    (
        "two_models_nm_count",
        "with c as union(\n (select item_id -> k, item_name -> nm),\n (select store_id -> k, state -> nm)\n) -> (k, nm);\nselect c.nm, count(c.k) as n;",
        "select nm, count(distinct k) from ((select item_id k, name nm from items) union all (select store_id, state from stores)) group by nm",
    ),
    (
        "two_models_k_group",
        "with c as union(\n (select item_id -> k, item_name -> nm),\n (select store_id -> k, state -> nm)\n) -> (k, nm);\nselect c.k, count(c.nm) as n;",
        "select k, count(nm) from ((select item_id k, name nm from items) union all (select store_id, state from stores)) group by k",
    ),
    (
        "grain_not_in_output_sum",
        "with c as union(\n (where yr = 2001 select cat -> c1, val -> v),\n (where yr = 2002 select cat -> c1, val -> v)\n) -> (c1, v);\nselect sum(c.v) as s;",
        "select sum(v) from ((select distinct cat c1, val v from lines where yr=2001) union all (select distinct cat, val from lines where yr=2002))",
    ),
    (
        "grain_not_in_output_group",
        "with c as union(\n (where yr = 2001 select cat -> c1, val -> v),\n (where yr = 2002 select cat -> c1, val -> v)\n) -> (c1, v);\nselect c.c1, sum(c.v) as s;",
        "select c1, sum(v) from ((select distinct cat c1, val v from lines where yr=2001) union all (select distinct cat, val from lines where yr=2002)) group by c1",
    ),
    (
        "grain_not_in_output_proj",
        "with c as union(\n (where yr = 2001 select cat -> c1, val -> v),\n (where yr = 2002 select cat -> c1, val -> v)\n) -> (c1, v);\nselect c.c1, c.v;",
        "select distinct c1, v from ((select distinct cat c1, val v from lines where yr=2001) union all (select distinct cat, val from lines where yr=2002))",
    ),
    (
        "same_arm_twice_sum",
        "with c as union(\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v),\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v)\n) -> (lid, k, v);\nselect sum(c.v) as s;",
        "select sum(v) from ((select line_id, item_id k, val v from lines where yr=2001) union all (select line_id, item_id k, val v from lines where yr=2001))",
    ),
    (
        "same_arm_twice_k_sum",
        "with c as union(\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v),\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v)\n) -> (lid, k, v);\nselect c.k, sum(c.v) as s;",
        "select k, sum(v) from ((select line_id, item_id k, val v from lines where yr=2001) union all (select line_id, item_id k, val v from lines where yr=2001)) group by k",
    ),
    (
        "same_arm_twice_cnt_lid",
        "with c as union(\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v),\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v)\n) -> (lid, k, v);\nselect count(c.lid) as s;",
        "select count(distinct line_id) from ((select line_id, item_id k, val v from lines where yr=2001) union all (select line_id, item_id k, val v from lines where yr=2001))",
    ),
    (
        "same_arm_twice_k_cnt_lid",
        "with c as union(\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v),\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v)\n) -> (lid, k, v);\nselect c.k, count(c.lid) as s;",
        "select k, count(distinct line_id) from ((select line_id, item_id k, val v from lines where yr=2001) union all (select line_id, item_id k, val v from lines where yr=2001)) group by k",
    ),
    (
        "same_arm_twice_proj_kv",
        "with c as union(\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v),\n (where yr = 2001 select line_id -> lid, item_id -> k, val -> v)\n) -> (lid, k, v);\nselect c.k, c.v;",
        "select distinct k, v from ((select line_id, item_id k, val v from lines where yr=2001) union all (select line_id, item_id k, val v from lines where yr=2001))",
    ),
    (
        "hidden_output_sum_v",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (--k, v);\nselect sum(c.v) as s;",
        "select sum(v) from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))",
    ),
    (
        "having_arms_sum",
        "with c as union(\n (where yr <= 2002 select item_id -> k, sum(val) -> v having v > 10),\n (where yr > 2002 select item_id -> k, sum(val) -> v having v > 3)\n) -> (k, v);\nselect c.k, sum(c.v) as s;",
        "select k, sum(v) from ((select item_id k, sum(val) v from lines where yr<=2002 group by 1 having sum(val)>10) union all (select item_id, sum(val) from lines where yr>2002 group by 1 having sum(val)>3)) group by k",
    ),
    (
        "union_k_join_dim",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect item_name, sum(c.v) as s subset join c.k = item_id;",
        "select name, sum(v) from items left join ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) s on s.k = items.item_id group by name",
    ),
    (
        "order_limit",
        "with c as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect c.k, sum(c.v) as s order by s desc limit 2;",
        "select k, sum(v) s from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by k order by s desc limit 2",
    ),
    (
        "uu_prop_sum",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwith outer_u as union(\n (select inner_u.k -> k, inner_u.v -> v),\n (where yr = 2011 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect sum(outer_u.v) as s;",
        "select sum(v) from ((select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))) union all (select item_id k, sum(val) v from lines where yr=2011 group by 1))",
    ),
    (
        "uu_prop_k_sum",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwith outer_u as union(\n (select inner_u.k -> k, inner_u.v -> v),\n (where yr = 2011 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect outer_u.k, sum(outer_u.v) as s;",
        "select k, sum(v) from ((select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))) union all (select item_id k, sum(val) v from lines where yr=2011 group by 1)) group by k",
    ),
    (
        "uu_proj",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwith outer_u as union(\n (select inner_u.k -> k, inner_u.v -> v),\n (where yr = 2011 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect outer_u.k, outer_u.v;",
        "select distinct k, v from ((select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))) union all (select item_id k, sum(val) v from lines where yr=2011 group by 1))",
    ),
    (
        "uu_cnt_k",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwith outer_u as union(\n (select inner_u.k -> k, inner_u.v -> v),\n (where yr = 2011 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect count(outer_u.k) as s;",
        "select count(distinct k) from ((select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))) union all (select item_id k, sum(val) v from lines where yr=2011 group by 1))",
    ),
    (
        "uu_inner_bag_agg",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwith outer_u as union(\n (select inner_u.k -> k, sum(inner_u.v) -> v),\n (where yr = 2011 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect outer_u.k, sum(outer_u.v) as s;",
        "select k, sum(v) from ((select k, sum(v) v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1)) group by k) union all (select item_id k, sum(val) v from lines where yr=2011 group by 1)) group by k",
    ),
    (
        "intersect_of_unions",
        "with u1 as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nwith u2 as union(\n (where yr = 2002 select item_id -> k),\n (where yr = 2003 select item_id -> k)\n) -> (k);\nwith x as intersect((select u1.k -> k), (select u2.k -> k)) -> (k);\nselect x.k;",
        "select k from (((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2011)) intersect ((select distinct item_id k from lines where yr=2002) union all (select distinct item_id k from lines where yr=2003)))",
    ),
    (
        "intersect_of_unions_join_name",
        "with u1 as union(\n (where yr = 2001 select item_id -> k),\n (where yr = 2011 select item_id -> k)\n) -> (k);\nwith u2 as union(\n (where yr = 2002 select item_id -> k),\n (where yr = 2003 select item_id -> k)\n) -> (k);\nwith x as intersect((select u1.k -> k), (select u2.k -> k)) -> (k);\nselect item_name subset join x.k = item_id where x.k is not null;",
        "select distinct name from items where item_id in (select k from (((select distinct item_id k from lines where yr=2001) union all (select distinct item_id k from lines where yr=2011)) intersect ((select distinct item_id k from lines where yr=2002) union all (select distinct item_id k from lines where yr=2003))))",
    ),
    (
        "union_arm_sum_bag_k_by_dim",
        "with inner_u as union(\n (where yr = 2001 select item_id -> k, sum(val) -> v),\n (where yr = 2002 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nwith outer_u as union(\n (select inner_u.k -> k, inner_u.v -> v),\n (where yr = 2011 select item_id -> k, sum(val) -> v)\n) -> (k, v);\nselect item_name, sum(outer_u.v) as s subset join outer_u.k = item_id;",
        "select name, sum(v) from items left join ((select distinct k, v from ((select item_id k, sum(val) v from lines where yr=2001 group by 1) union all (select item_id k, sum(val) v from lines where yr=2002 group by 1))) union all (select item_id k, sum(val) v from lines where yr=2011 group by 1)) o on o.k = items.item_id group by name",
    ),
]


@pytest.fixture(scope="module")
def oracle():
    con = duckdb.connect()
    con.execute(f"create table lines as {LINES}")
    con.execute(f"create table items as {ITEMS}")
    con.execute(f"create table stores as {STORES}")
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
