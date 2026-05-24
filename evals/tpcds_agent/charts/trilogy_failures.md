# Trilogy failure analysis — 20260524-200150

- Run `20260524-200150` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 1313 | failed: 146 (11%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 76 | 52% |
| `syntax-parse` | 21 | 14% |
| `undefined-concept` | 17 | 12% |
| `file-not-found` | 10 | 7% |
| `syntax-missing-alias` | 8 | 5% |
| `cli-misuse` | 8 | 5% |
| `join-resolution` | 4 | 3% |
| `type-error` | 2 | 1% |

## Detail

### `other`

- `trilogy run query01.preql`
  - HAVING references 'store_returns.store.store_sk', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_returns.s…
- `trilogy run query01.preql`
  - (_duckdb.BinderException) Binder Error: Referenced column "total_returns_at_store" not found in FROM clause! Candidate bindings: "sr_return_ship_cost", "s_rec_start_date", "d_quarter_name", "s_store_name", "sr_net_loss" …
- `trilogy run query01.preql`
  - HAVING references 'store_returns.store.store_sk', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_returns.s…
- `trilogy run query01.preql`
  - HAVING references 'store_returns.store.store_sk', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_returns.s…
- `trilogy run query01.preql`
  - (_duckdb.BinderException) Binder Error: Referenced column "total_returns_at_store" not found in FROM clause! Candidate bindings: "sr_return_ship_cost", "s_rec_start_date", "d_quarter_name", "s_store_name", "sr_net_loss" …
- `trilogy run query01.preql`
  - WHERE clause aggregates at multiple grains are not allowed: `ref:store_returns.store.state = TN and ref:store_returns.date_dim.year = 2000 and sum(ref:store_returns.return_amt)<['ref:store_returns.customer.customer_id', …
- `trilogy run query01.preql`
  - HAVING references 'store_returns.return_amt', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_returns.retur…
- `trilogy --debug run query01.preql`
  - HAVING references 'store_returns.return_amt', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_returns.retur…
- `trilogy run query01.preql`
  - (_duckdb.BinderException) Binder Error: aggregate function calls cannot be nested LINE 17: "total_returns_at_store" > 1.2 * avg(sum("store_returns_store_returns"."sr_return_amt")) ^ [SQL: SELECT "store_returns_customer_c…
- `trilogy `
  - Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 33 (char 32). Re-issue the call with valid JSON arguments.
- `trilogy run query11.preql duckdb tpcds.duckdb`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.web_total', 'local.store_total'} out of with found {'store_sales.customer.last_name', 'store_sales.customer.first_name', 'store_sales.customer.…
- `trilogy run query11.preql duckdb tpcds.duckdb`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.store_2001', 'local.web_2001', 'local.web_2002'} out of with found {'local.store_2002', 'store_sales.customer.last_name', 'store_sales.customer…
- `trilogy run --debug query11.preql duckdb tpcds.duckdb`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.web_2001', 'local.store_2001', 'local.web_2002'} out of with found {'local.store_2001', 'store_sales.customer.first_name', 'store_sales.custome…
- `trilogy run query11.preql duckdb tpcds.duckdb`
  - WHERE clause aggregate `sum(subtract(ref:store_sales.ext_list_price,<Filter: ref:store_sales.ext_discount_amt where ref:store_sales.date_dim.year = 2001>))` is also computed in the SELECT (as `store_2001`); aggregate fil…
- `trilogy run --debug query11.preql duckdb tpcds.duckdb`
  - WHERE clause aggregate `sum(subtract(ref:store_sales.ext_list_price,<Filter: ref:store_sales.ext_discount_amt where ref:store_sales.date_dim.year = 2001>))` is also computed in the SELECT (as `store_2001`); aggregate fil…
- `trilogy run query16.preql`
  - Multiple where clauses are not supported --- stderr ---
- `trilogy run query30.preql`
  - HAVING references 'web_returns.return_amt', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --web_returns.return_am…
- `trilogy run query30.preql`
  - WHERE clause aggregates at multiple grains are not allowed: `ref:web_returns.date_dim.year = 2002 and ref:web_returns.returning_customer.customer_address.state = GA and sum(ref:web_returns.return_amt)<['ref:web_returns.r…
- `trilogy run query30.preql`
  - HAVING references 'web_returns.return_amt', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --web_returns.return_am…
- `trilogy run query30.preql`
  - HAVING references 'web_returns.returning_addr.state', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --web_returns…
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb tpcds.duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb tpcds.duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb tpcds.duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - --import raw/catalog_sales:catalog_sales`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr --- [guidance] You have issued this identical call 3 times in a row with the same result — it is not making progress. Stop repeating it and take a different actio…
- `trilogy run --import raw/store_sales.preql:ss --import raw/web_sales.preql:ws merge ss.customer.customer_sk into ws.bill_cust…`
  - Duplicate select output for ws.bill_customer.customer_sk; Line: 3 --- stderr ---
- `trilogy run --import raw/store_sales.preql:store_sales - duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run --import raw/store_sales.preql:store_sales - duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run - duckdb`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run -`
  - exit_code: 2 --- stdout --- No input on stdin. --- stderr ---
- `trilogy run query40.preql duckdb`
  - ORDER BY references 'cs.warehouse.state', which is not in the SELECT projection (line 7). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --cs.warehouse.state order …
- `trilogy run query40.preql duckdb`
  - ORDER BY references 'local.item_id', which is not in the SELECT projection (line 7). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --local.item_id order by local.i…
- `trilogy explore query40.preql`
  - exit_code: 1 --- stdout --- Failed to parse query40.preql: ORDER BY references 'local.item_id', which is not in the SELECT projection (line 7). Add it to SELECT to sort by it — prefix with `--` to keep it out of the outp…
- `trilogy run query42.preql`
  - ORDER BY references 'store_sales.date_dim.year', which is not in the SELECT projection (line 3). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.date_d…
- `trilogy run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy run query44.preql`
  - WHERE clause aggregate `avg(store_sales.net_profit)` is also computed in the SELECT (as `avg_net_profit`); aggregate filters must use the HAVING clause - e.g. `having avg_net_profit > ...`; Line: 9 --- stderr ---
- `trilogy run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy --debug run query44.preql`
  - HAVING references 'store_sales.net_profit', which is not in the SELECT projection (line 9). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.net_profi…
- `trilogy run query47.preql`
  - HAVING references 'store_sales.date_dim.year', which is not in the SELECT projection (line 3). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales.date_d…
- `trilogy run query51.preql`
  - (_duckdb.BinderException) Binder Error: Referenced table "store_sales_date_dim_date_dim" not found! Candidate tables: "web_sales_sold_date_date_dim", "web_sales_item_item", "web_sales_web_sales" LINE 50: coalesce("cheerf…
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/catalog_sales.preql:catalog_sales --import raw/web_sales.…`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.ss_sum', 'local.cs_sum', 'local.ws_sum'} out of with found {'local.ss_sum', 'store_sales.item.item_id'} --- stderr ---
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/catalog_sales.preql:catalog_sales select store_sales.item…`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.cs_sum', 'local.ss_sum'} out of with found {'store_sales.item.item_id', 'local.ss_sum'} --- stderr ---
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/catalog_sales.preql:catalog_sales select store_sales.item…`
  - Cannot resolve query. No remaining priority concepts, have attempted {'local.ss_sum', 'local.cs_sum'} out of with found {'store_sales.item.item_id', 'local.ss_sum'} --- stderr ---
- `trilogy run -e select store_sales.date_dim.week_seq, store_sales.date_dim.dow, store_sales.date_dim.day_name, store_sales.dat…`
  - exit_code: 1 --- stdout --- Environment variable must be in KEY=VALUE format or be a path to an existing env file: select store_sales.date_dim.week_seq, store_sales.date_dim.dow, store_sales.date_dim.day_name, store_sale…
- `trilogy run -e import raw.store_sales as store_sales; select store_sales.date_dim.week_seq, store_sales.date_dim.dow, store_s…`
  - exit_code: 1 --- stdout --- Environment variable must be in KEY=VALUE format or be a path to an existing env file: import raw.store_sales as store_sales; select store_sales.date_dim.week_seq, store_sales.date_dim.dow, st…
- `trilogy run -e`
  - Option '-e' requires an argument.
- `trilogy run -e 1 + 1`
  - exit_code: 1 --- stdout --- Environment variable must be in KEY=VALUE format or be a path to an existing env file: 1 + 1 --- stderr ---
- `trilogy --debug run -e 1+1`
  - exit_code: 1 --- stdout --- ┌────── Debug ───────┐ │ Debug mode enabled │ └────────────────────┘ Environment variable must be in KEY=VALUE format or be a path to an existing env file: 1+1 --- stderr ---
- `trilogy --debug run -e import raw.store_sales as store_sales; select store_sales.date_dim.week_seq, store_sales.date_dim.dow,…`
  - exit_code: 1 --- stdout --- ┌────── Debug ───────┐ │ Debug mode enabled │ └────────────────────┘ Environment variable must be in KEY=VALUE format or be a path to an existing env file: import raw.store_sales as store_sale…
- `trilogy run query61.preql`
  - Value 'Y' is not valid for enum field 'store_sales.promotion.channel_email'. Allowed values: 'N'. --- stderr ---
- `trilogy run query61.preql`
  - Value 'Y' is not valid for enum field 'store_sales.promotion.channel_email'. Allowed values: 'N'. --- stderr ---
- `trilogy `
  - Tool call 'trilogy' rejected: invalid tool arguments: Unterminated string starting at: line 1 column 56 (char 55). Re-issue the call with valid JSON arguments.
- `trilogy run query69.preql`
  - HAVING references 'store_sales.customer.customer_sk', which is not in the SELECT projection (line 8). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sales…
- `trilogy run --import raw/store_sales:store_sales select store_sales.store.state as st, store_sales.store.county as cnty, sum(…`
  - Recursion error building concept local.tp with grain Grain<local.county_rank,local.state_rank,store_sales.store.county,store_sales.s tore.state> and lineage sum(ref:store_sales.net_profit)<['ref:store_sales.store.state',…
- `trilogy run --import raw/store_sales:store_sales select store_sales.store.state as st, sum(store_sales.net_profit) as tp, ran…`
  - Recursion error building concept local.tp with grain Grain<local.state_rank,store_sales.store.state> and lineage sum(ref:store_sales.net_profit)<['ref:store_sales.store.state', 'ref:local.state_rank']>. This is likely du…
- `trilogy run --import raw/store_sales:store_sales select store_sales.store.state as st, sum(store_sales.net_profit) as tp, ran…`
  - Recursion error building concept local.tp with grain Grain<local.sr,store_sales.store.state> and lineage sum(ref:store_sales.net_profit)<['ref:store_sales.store.state', 'ref:local.sr']>. This is likely due to a circular …
- `trilogy run --import raw/store_sales:store_sales where store_sales.date_dim.year = 2000 select store_sales.store.state as st,…`
  - Could not find store_sales.store.state in store_sales.date_dim.date_dim_at_store_sales_date_dim_date_sk_join_store_sales. store.store_at_store_sales_store_store_sk_join_store_sales.store_sales_at_store _sales_item_item_s…
- `trilogy run --import raw/store_sales:store_sales where store_sales.date_dim.year = 2000 select store_sales.store.state as st,…`
  - Could not find store_sales.store.state in store_sales.date_dim.date_dim_at_store_sales_date_dim_date_sk_join_store_sales. store.store_at_store_sales_store_store_sk_join_store_sales.store_sales_at_store _sales_item_item_s…
- `trilogy run query73.preql`
  - ORDER BY references 'store_sales.customer.customer_id', which is not in the SELECT projection (line 14). Add it to SELECT to sort by it — prefix with `--` to keep it out of the output rows, e.g. `select ..., --store_sale…
- `trilogy run query77.preql`
  - (_duckdb.Error) Parameter not supported in ORDER BY clause [SQL: WITH uneven as ( SELECT "store_sales_store_sales"."ss_item_sk" as "store_sales_item_item_sk", "store_sales_store_sales"."ss_sold_date_sk" as "store_sales_d…
- `trilogy run query77.preql --debug`
  - (_duckdb.Error) Parameter not supported in ORDER BY clause [SQL: WITH uneven as ( SELECT "store_sales_store_sales"."ss_item_sk" as "store_sales_item_item_sk", "store_sales_store_sales"."ss_sold_date_sk" as "store_sales_d…
- `trilogy run query77.preql`
  - (_duckdb.Error) Parameter not supported in ORDER BY clause [SQL: WITH uneven as ( SELECT "store_sales_store_sales"."ss_item_sk" as "store_sales_item_item_sk", "store_sales_store_sales"."ss_sold_date_sk" as "store_sales_d…
- `trilogy run query81.preql`
  - Duplicate select output for catalog_returns.returning_customer.customer_id; Line: 3 --- stderr ---
- `trilogy run query87.preql duckdb tpcds.duckdb`
  - HAVING references 'catalog_sales.order_number', which is not in the SELECT projection (line 14). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --catalog_sales.or…
- `trilogy run query87.preql duckdb tpcds.duckdb`
  - HAVING references 'catalog_sales.order_number', which is not in the SELECT projection (line 13). Fix one of: (a) add it to SELECT — prefix with `--` to keep it out of the output rows, e.g. `select ..., --catalog_sales.or…
- `trilogy run test_case.preql`
  - 'int' object has no attribute 'concept_arguments' --- stderr ---

### `syntax-parse`

- `trilogy run -`
  - --> 1:31 | 1 | import raw.store_sales as ss; ss.date_dim.moy; | ^--- | = expected EOI, block, or show_statement Location: import raw.store_sales as ss; ??? ss.date_dim.moy; --- stderr ---
- `trilogy run --import raw/inventory:inventory select inventory.quantity_on_hand as qoh by inventory.item.product_name limit 3;…`
  - --> 2:42 | 2 | select inventory.quantity_on_hand as qoh by inventory.item.product_name limit 3; | ^--- | = expected metadata, limit, order_by, where, or having Location: ...ntory.quantity_on_hand as qoh ??? by inventory.…
- `trilogy run --import raw/store_sales.preql:store_sales stdin duckdb`
  - --> 2:1 | 2 | stdin; | ^--- | = expected EOI, block, or show_statement Location: ...w.store_sales as store_sales; ??? stdin; --- stderr ---
- `trilogy run --import raw/catalog_sales:cs select cs.item.item_id, sum(cs.sales_price) ? cs.sold_date.date < '2000-03-11'::dat…`
  - --> 2:102 | 2 | select cs.item.item_id, sum(cs.sales_price) ? cs.sold_date.date < '2000-03-11'::date as before_total group by cs.item.item_id limit 5; | ^--- | = expected metadata, limit, order_by, where, or having Locat…
- `trilogy run --import raw/item.preql:item select count(item.item_sk) from item where item.manufact_id between 738 and 778;`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...m; select count(item.item_sk) ??? from item where item.manufact_... --- stderr ---
- `trilogy run query51.preql`
  - --> 9:86 | 9 | auto store_running_total <- sum(store_sales.sales_price) by store_sales.item.item_id order by store_sales.date_dim.date asc; | ^--- | = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PL…
- `trilogy run query51.preql`
  - --> 9:113 | 9 | auto store_running_total <- sum(store_sales.sales_price) by store_sales.item.item_id, store_sales.date_dim.date order by store_sales.date_dim.date asc; | ^--- | = expected dot_tail, bracket_tail, dcolon_t…
- `trilogy run query51.preql`
  - --> 9:6 | 9 | auto store_daily_sales := sum(store_sales.sales_price) by store_sales.item.item_id, store_sales.date_dim.date; | ^--- | = expected prop_ident or prop_ident_wildcard Location: ...e totals for store_sales aut…
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/catalog_sales.preql:catalog_sales --import raw/web_sales.…`
  - --> 4:145 | 4 | merge store_sales.item.item_id into catalog_sales.item.item_id; merge store_sales.item.item_id into web_sales.item.item_id; with target_date as (select date_dim.date from date_dim where date_dim.date = '2…
- `trilogy run --import raw/store_sales.preql:store_sales --import raw/catalog_sales.preql:catalog_sales --import raw/web_sales.…`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...e_dim.date, date_dim.week_seq ??? from date_dim where date_dim.d... --- stderr ---
- `trilogy run --import raw/web_sales:ws --import raw/catalog_sales:cs select ws.warehouse.warehouse_name, count(ws.sold_date.mo…`
  - --> 3:223 | 3 | select ws.warehouse.warehouse_name, count(ws.sold_date.moy) by ws.warehouse.warehouse_name, ws.sold_date.moy as cnt_mths, sum(ws.ext_sales_price * ws.quantity) by ws.warehouse.warehouse_name, ws.sold_date…
- `trilogy --debug run --import raw/store_sales:store_sales --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_…`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ....customer.customer_sk) as cnt ??? from store_sales where store_s... Full traceback: Traceback (most…
- `trilogy --debug run --import raw/store_sales:store_sales --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_…`
  - --> 4:369 | 4 | merge web_sales.ship_customer.customer_sk into ~store_sales.customer.customer_sk; merge catalog_sales.ship_customer.customer_sk into ~store_sales.customer.customer_sk; where store_sales.customer.customer_…
- `trilogy --debug run --import raw/store_sales:store_sales --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_…`
  - --> 4:524 | 4 | merge web_sales.ship_customer.customer_sk into ~store_sales.customer.customer_sk; merge catalog_sales.ship_customer.customer_sk into ~store_sales.customer.customer_sk; where store_sales.customer.customer_…
- `trilogy --debug run --import raw/store_sales:store_sales --import raw/web_sales:web_sales --import raw/catalog_sales:catalog_…`
  - --> 4:477 | 4 | merge web_sales.ship_customer.customer_sk into ~store_sales.customer.customer_sk; merge catalog_sales.ship_customer.customer_sk into ~store_sales.customer.customer_sk; where store_sales.customer.customer_…
- `trilogy --debug run --import raw/store_sales:store_sales --import raw/store_returns:store_returns - duckdb tpcds.duckdb`
  - --> 9:1 | 9 | group by store_sales.date_dim.year, store_sales.item.item_sk, store_sales.customer.customer_sk | ^--- | = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS…
- `trilogy --debug run --import raw/store_sales:store_sales --import raw/store_returns:store_returns --import raw/web_sales:web_…`
  - --> 7:1 | 7 | > select | ^--- | = expected EOI, block, or show_statement Location: ...g_returns as catalog_returns; ??? > select store_sales.custome... Full traceback: Traceback (most recent call last): File "C:\Users\et…
- `trilogy run --import raw/web_sales:web_sales select web_sales.bill_cdemo.marital_status, web_sales.ship_cdemo.marital_status …`
  - Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic). Location: ...les.ship_cdemo.marital_status ??? from web_sales limit 5; --- stderr ---
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.sold_date.year, count(catalog_sales.order_number) a…`
  - --> 2:121 | 2 | select catalog_sales.sold_date.year, count(catalog_sales.order_number) as cnt where catalog_sales.sold_date.year = 2000 group by catalog_sales.sold_date.year; | ^--- | = expected limit, order_by, having, …
- `trilogy run query93.preql`
  - --> 36:9 | 36 | * store_sales.sales_price | ^--- | = unexpected MULTIPLY_DIVIDE_PERCENT Location: ....reason.desc is null) ??? * store_sales.sales_price --- stderr ---
- `trilogy run --import raw/store_sales:store_sales select simple_case(true, 1, 0) as x;`
  - --> 2:19 | 2 | select simple_case(true, 1, 0) as x; | ^--- | = expected limit, order_by, where, having, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT Location: ...tor…

### `undefined-concept`

- `trilogy run query03.preql`
  - (UndefinedConceptException(...), 'Undefined concept: year.') --- stderr ---
- `trilogy run query03.preql`
  - (UndefinedConceptException(...), 'Undefined concept: brand_id.') --- stderr ---
- `trilogy run query03.preql`
  - (UndefinedConceptException(...), 'Undefined concept: brand_id.') --- stderr ---
- `trilogy run query20.preql`
  - (UndefinedConceptException(...), 'Undefined concept: category.') --- stderr ---
- `trilogy run --import raw/store_sales.preql:ss --import raw/store_returns.preql:sr --import raw/catalog_sales.preql:cs select …`
  - (UndefinedConceptException(...), "line: 4: Undefined concept: ss.local.quantity. Suggestions: ['ss.quantity', 'cs.quantity']") --- stderr ---
- `trilogy run query36_test.preql`
  - (UndefinedConceptException(...), "line: 3: Undefined concept: store_sales.local.net_profit. Suggestions: ['store_sales.net_profit', 'store_sales.net_paid', 'store_sales.sales_price']") --- stderr ---
- `trilogy run query52.preql`
  - (UndefinedConceptException(...), 'Undefined concept: year.') --- stderr ---
- `trilogy --debug run query52.preql`
  - (UndefinedConceptException(...), 'Undefined concept: year.') Full traceback: Traceback (most recent call last): File "C:\Users\ethan\coding_projects\pytrilogy_two\trilogy\scripts\parallel_executio n.py", line 577, in run…
- `trilogy run query75.preql`
  - (UndefinedConceptException(...), "Undefined concept: catalog_returns.local.order_number. Suggestions: ['catalog_returns.order_number', 'catalog_sales.order_number', 'catalog_returns.call_center.suite_number']") --- stder…
- `trilogy run query75.preql`
  - (UndefinedConceptException(...), "Undefined concept: catalog_returns.local.order_number. Suggestions: ['catalog_returns.order_number', 'catalog_sales.order_number', 'catalog_returns.call_center.suite_number']") --- stder…
- `trilogy run query77.preql`
  - (UndefinedConceptException(...), "Undefined concept: store_sales.sold_date.date. Suggestions: ['web_sales.sold_date.date', 'catalog_sales.sold_date.date', 'web_sales.sold_date.date_sk']") --- stderr ---
- `trilogy run query79.preql`
  - (UndefinedConceptException(...), 'Undefined concept: last_name.') --- stderr ---
- `trilogy run query79.preql`
  - (UndefinedConceptException(...), 'Undefined concept: last_name.') --- stderr ---
- `trilogy run query80.preql`
  - (UndefinedConceptException(...), "Undefined concept: web_returns.web_page. Suggestions: ['web_returns.web_page.url', 'web_returns.web_page.type', 'web_returns.fee']") --- stderr ---
- `trilogy run query97.preql`
  - (UndefinedConceptException(...), "Undefined concept: catalog_sales.customer.customer_id. Suggestions: ['catalog_sales.ship_customer.customer_id', 'catalog_sales.bill_customer.customer_id', 'store_sales.customer.customer_…
- `trilogy run query97.preql`
  - (UndefinedConceptException(...), "Undefined concept: catalog_sales.customer.customer_id. Suggestions: ['catalog_sales.ship_customer.customer_id', 'catalog_sales.bill_customer.customer_id', 'store_sales.customer.customer_…
- `trilogy run query97.preql`
  - (UndefinedConceptException(...), "Undefined concept: catalog_sales.customer.customer_id. Suggestions: ['catalog_sales.ship_customer.customer_id', 'catalog_sales.bill_customer.customer_id', 'store_sales.customer.customer_…

### `file-not-found`

- `trilogy run --import raw/store_sales.preql:store_sales select store_sales.item.category, store_sales.item.class, sum(store_sa…`
  - exit_code: 2 --- stdout --- Input 'select store_sales.item.category, store_sales.item.class, sum(store_sales.local.net_profit) / nullif(sum(store_sales.local.ext_sales_price), 0) * 1.0 as gross_margin from store_sales wh…
- `trilogy run --import raw/store_sales.preql:store_sales - duckdb`
  - exit_code: 2 --- stdout --- Input 'select store_sales.item.category, store_sales.item.class, sum(store_sales.local.net_profit) / nullif(sum(store_sales.local.ext_sales_price), 0) * 1.0 as gross_margin where store_sales.d…
- `trilogy run --import raw/store_sales.preql:store_sales - duckdb`
  - exit_code: 2 --- stdout --- Input 'select store_sales.item.category, store_sales.item.class, sum(store_sales.local.net_profit) / nullif(sum(store_sales.local.ext_sales_price), 0) * 1.0 as gross_margin where store_sales.d…
- `trilogy run query36_test2.preql`
  - exit_code: 2 --- stdout --- Input 'query36_test2.preql' does not exist. --- stderr ---
- `trilogy run query36_test2.preql`
  - exit_code: 2 --- stdout --- Input 'query36_test2.preql' does not exist. --- stderr ---
- `trilogy run query47.preql duck_db --config trilogy.toml`
  - exit_code: 2 --- stdout --- Input 'query47.preql' does not exist. --- stderr ---
- `trilogy run query52.preql`
  - exit_code: 2 --- stdout --- Input 'query52.preql' does not exist. --- stderr ---
- `trilogy run query69.preql`
  - exit_code: 2 --- stdout --- Input 'query69.preql' does not exist. --- stderr ---
- `trilogy --debug run query72.preql`
  - exit_code: 2 --- stdout --- ┌────── Debug ───────┐ │ Debug mode enabled │ └────────────────────┘ Input 'query72.preql' does not exist. --- stderr ---
- `trilogy run --import raw/date_dim:date_dim select date_dim.date, date_dim.week_seq from raw/date_dim as date_dim where date_d…`
  - exit_code: 2 --- stdout --- Input 'select date_dim.date, date_dim.week_seq from raw/date_dim as date_dim where date_dim.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date);' does not exist. --- stderr --…

### `syntax-missing-alias`

- `trilogy run --import raw/store_sales:store_sales select distinct store_sales.item.manufact_id from store_sales; duckdb`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: store_sales; select distinct ??? store_sales.item.manufact_id f... --- stderr ---
- `trilogy run --import raw/inventory:inventory select inventory.quantity_on_hand by inventory.item.product_name limit 3; duckdb`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...ct inventory.quantity_on_hand ??? by inventory.item.product_name... --- stderr ---
- `trilogy run --import raw/catalog_sales:cs select cs.item.item_id, cs.order_number, cs.sold_date.date, coalesce(cs.sales_price…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ..., coalesce(cs.sales_price, 0) ??? limit 5; --- stderr ---
- `trilogy run --import raw/item.preql:item select count(item.item_sk) where item.manufact_id between 738 and 778;`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...m; select count(item.item_sk) ??? where item.manufact_id between... --- stderr ---
- `trilogy run query47.preql duck_db --config trilogy.toml`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...tore_sales.store.company_name ??? order by store_sales.date_dim.... --- stderr ---
- `trilogy run --import raw/store_sales:store_sales select store_sales.item.category, sum(store_sales.sales_price) as monthly_to…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: by store_sales.item.category ??? order by store_sales.date_dim.... --- stderr ---
- `trilogy run --import raw/store_sales:store_sales select store_sales.store.state as st, count(store_sales.ticket_number) as cn…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...) as cnt, rank st by cnt desc ??? limit 20; --- stderr ---
- `trilogy run --import raw/web_sales:web_sales --import raw/web_returns:web_returns select count(web_returns.refunded_cash) lim…`
  - Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Location: ...nt(web_returns.refunded_cash) ??? limit 5; --- stderr ---

### `cli-misuse`

- `trilogy --import raw/store_sales.preql:store_sales run - duckdb`
  - No such option: --import
- `trilogy help`
  - No such command 'help'.
- `trilogy see raw/store_sales.preql`
  - No such command 'see'.
- `trilogy --debug run -i raw/store_sales:store_sales -i raw/web_sales:web_sales -i raw/catalog_sales:catalog_sales select store…`
  - 'raw/store_sales:store_sales' looks like a file path, not a dialect. The dialect argument comes AFTER the input file. Try: trilogy run raw/store_sales:store_sales <dialect>
- `trilogy info --agent`
  - No such command 'info'.
- `trilogy shell -q -c select count(*) from store_returns sr join customer c on sr.sr_customer_sk = c.c_customer_sk join custome…`
  - No such command 'shell'.
- `trilogy duckdb query SELECT count(*) FROM store_returns sr JOIN customer c ON sr.sr_customer_sk = c.c_customer_sk JOIN custom…`
  - No such command 'duckdb'.
- `trilogy eval case(true, 1, 0);`
  - No such command 'eval'.

### `join-resolution`

- `trilogy run --import raw/store_sales.preql:ss --import raw/web_sales.preql:ws select ss.customer.customer_sk, ws.bill_custome…`
  - Could not resolve connections for query with output ['ss.customer.customer_sk<Purpose.KEY>Derivation.ROOT>', 'ws.bill_customer.customer_sk<Purpose.KEY>Derivation.ROOT>'] from current model. --- stderr ---
- `trilogy run --import raw/catalog_sales:cs --import raw/catalog_returns:cr select cs.warehouse.state, cs.item.item_id, cs.sold…`
  - Could not resolve connections for query with output ['cs.warehouse.state<Purpose.PROPERTY>Derivation.ROOT>', 'cs.item.item_id<Purpose.PROPERTY>Derivation.ROOT>', 'cs.sold_date.date<Purpose.PROPERTY>Derivation.ROOT>', 'cs…
- `trilogy run --import raw/catalog_returns:cr --import raw/catalog_sales:cs select cs.sold_date.date, cs.sales_price, cr.refund…`
  - Could not resolve connections for query with output ['cs.sold_date.date<Purpose.PROPERTY>Derivation.ROOT>', 'cs.sales_price<Purpose.PROPERTY>Derivation.ROOT>', 'cr.refunded_cash<Purpose.PROPERTY>Derivation.ROOT>', 'cs.or…
- `trilogy run --import raw/web_sales:web_sales --import raw/web_returns:web_returns select substring(web_returns.reason.desc, 1…`
  - Could not resolve connections for query with output ['local.r_reason_desc<Purpose.PROPERTY>Derivation.BASIC>', 'local.cnt<Purpose.METRIC>Derivation.AGGREGATE>'] from current model. --- stderr ---

### `type-error`

- `trilogy run query13.preql`
  - Invalid argument type 'BIGINT' passed into YEAR function in position 1 from concept: store_sales.date_dim.date_sk. Valid: 'DATE', 'DATETIME', 'TIMESTAMP'. --- stderr ---
- `trilogy run query38.preql`
  - Invalid argument type 'DATE' passed into CONCAT function in position 5 from concept: store_sales.date_dim.date. Valid: 'STRING'. --- stderr ---
