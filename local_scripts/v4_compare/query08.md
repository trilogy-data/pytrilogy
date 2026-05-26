# Query 08

**Status:** `gen_fail`

| Stage | Result |
| --- | --- |
| v4 SQL generation | FAILED |
| reference execution | FAILED |

## Result comparison

_at least one side did not produce rows._

## SQL size + execution time

| Source | Chars | Lines | Exec (min of 4) |
| --- | --- | --- | --- |
| v4 | 0 | 0 | — |
| reference | 2419 | 61 | — |

## Preql

```
import store_sales as store_sales;
import customer as customer;

const zips_pre <- unnest(
    [
        '24128',
        '76232',
        '65084',
        '87816',
        '83926',
        '77556',
        '20548',
        '26231',
        '43848',
        '15126',
        '91137',
        '61265',
        '98294',
        '25782',
        '17920',
        '18426',
        '98235',
        '40081',
        '84093',
        '28577',
        '55565',
        '17183',
        '54601',
        '67897',
        '22752',
        '86284',
        '18376',
        '38607',
        '45200',
        '21756',
        '29741',
        '96765',
        '23932',
        '89360',
        '29839',
        '25989',
        '28898',
        '91068',
        '72550',
        '10390',
        '18845',
        '47770',
        '82636',
        '41367',
        '76638',
        '86198',
        '81312',
        '37126',
        '39192',
        '88424',
        '72175',
        '81426',
        '53672',
        '10445',
        '42666',
        '66864',
        '66708',
        '41248',
        '48583',
        '82276',
        '18842',
        '78890',
        '49448',
        '14089',
        '38122',
        '34425',
        '79077',
        '19849',
        '43285',
        '39861',
        '66162',
        '77610',
        '13695',
        '99543',
        '83444',
        '83041',
        '12305',
        '57665',
        '68341',
        '25003',
        '57834',
        '62878',
        '49130',
        '81096',
        '18840',
        '27700',
        '23470',
        '50412',
        '21195',
        '16021',
        '76107',
        '71954',
        '68309',
        '18119',
        '98359',
        '64544',
        '10336',
        '86379',
        '27068',
        '39736',
        '98569',
        '28915',
        '24206',
        '56529',
        '57647',
        '54917',
        '42961',
        '91110',
        '63981',
        '14922',
        '36420',
        '23006',
        '67467',
        '32754',
        '30903',
        '20260',
        '31671',
        '51798',
        '72325',
        '85816',
        '68621',
        '13955',
        '36446',
        '41766',
        '68806',
        '16725',
        '15146',
        '22744',
        '35850',
        '88086',
        '51649',
        '18270',
        '52867',
        '39972',
        '96976',
        '63792',
        '11376',
        '94898',
        '13595',
        '10516',
        '90225',
        '58943',
        '39371',
        '94945',
        '28587',
        '96576',
        '57855',
        '28488',
        '26105',
        '83933',
        '25858',
        '34322',
        '44438',
        '73171',
        '30122',
        '34102',
        '22685',
        '71256',
        '78451',
        '54364',
        '13354',
        '45375',
        '40558',
        '56458',
        '28286',
        '45266',
        '47305',
        '69399',
        '83921',
        '26233',
        '11101',
        '15371',
        '69913',
        '35942',
        '15882',
        '25631',
        '24610',
        '44165',
        '99076',
        '33786',
        '70738',
        '26653',
        '14328',
        '72305',
        '62496',
        '22152',
        '10144',
        '64147',
        '48425',
        '14663',
        '21076',
        '18799',
        '30450',
        '63089',
        '81019',
        '68893',
        '24996',
        '51200',
        '51211',
        '45692',
        '92712',
        '70466',
        '79994',
        '22437',
        '25280',
        '38935',
        '71791',
        '73134',
        '56571',
        '14060',
        '19505',
        '72425',
        '56575',
        '74351',
        '68786',
        '51650',
        '20004',
        '18383',
        '76614',
        '11634',
        '18906',
        '15765',
        '41368',
        '73241',
        '76698',
        '78567',
        '97189',
        '28545',
        '76231',
        '75691',
        '22246',
        '51061',
        '90578',
        '56691',
        '68014',
        '51103',
        '94167',
        '57047',
        '14867',
        '73520',
        '15734',
        '63435',
        '25733',
        '35474',
        '24676',
        '94627',
        '53535',
        '17879',
        '15559',
        '53268',
        '59166',
        '11928',
        '59402',
        '33282',
        '45721',
        '43933',
        '68101',
        '33515',
        '36634',
        '71286',
        '19736',
        '58058',
        '55253',
        '67473',
        '41918',
        '19515',
        '36495',
        '19430',
        '22351',
        '77191',
        '91393',
        '49156',
        '50298',
        '87501',
        '18652',
        '53179',
        '18767',
        '63193',
        '23968',
        '65164',
        '68880',
        '21286',
        '72823',
        '58470',
        '67301',
        '13394',
        '31016',
        '70372',
        '67030',
        '40604',
        '24317',
        '45748',
        '39127',
        '26065',
        '77721',
        '31029',
        '31880',
        '60576',
        '24671',
        '45549',
        '13376',
        '50016',
        '33123',
        '19769',
        '22927',
        '97789',
        '46081',
        '72151',
        '15723',
        '46136',
        '51949',
        '68100',
        '96888',
        '64528',
        '14171',
        '79777',
        '28709',
        '11489',
        '25103',
        '32213',
        '78668',
        '22245',
        '15798',
        '27156',
        '37930',
        '62971',
        '21337',
        '51622',
        '67853',
        '10567',
        '38415',
        '15455',
        '58263',
        '42029',
        '60279',
        '37125',
        '56240',
        '88190',
        '50308',
        '26859',
        '64457',
        '89091',
        '82136',
        '62377',
        '36233',
        '63837',
        '58078',
        '17043',
        '30010',
        '60099',
        '28810',
        '98025',
        '29178',
        '87343',
        '73273',
        '30469',
        '64034',
        '39516',
        '86057',
        '21309',
        '90257',
        '67875',
        '40162',
        '11356',
        '73650',
        '61810',
        '72013',
        '30431',
        '22461',
        '19512',
        '13375',
        '55307',
        '30625',
        '83849',
        '68908',
        '26689',
        '96451',
        '38193',
        '46820',
        '88885',
        '84935',
        '69035',
        '83144',
        '47537',
        '56616',
        '94983',
        '48033',
        '69952',
        '25486',
        '61547',
        '27385',
        '61860',
        '58048',
        '56910',
        '16807',
        '17871',
        '35258',
        '31387',
        '35458',
        '35576'
    ]
);
auto zip_p_count <- count(customer.id ? customer.preferred_cust_flag = 'Y') by customer.address.zip;
auto zips <- substring(zips_pre::string, 1, 5);
auto p_cust_zip <- customer.address.zip ? zip_p_count > 10;
auto final_zips <- substring(zips ? zips in substring(p_cust_zip, 1, 5), 1, 2);

where
    store_sales.date.quarter = 2
    and store_sales.date.year = 1998
    and substring(store_sales.store.zip, 1, 2) in final_zips
select
    store_sales.store.name,
    sum(store_sales.net_profit) as store_net_profit,
order by
    store_sales.store.name asc
limit 100
;
```

## v4 generated SQL

_v4 did not produce SQL._

## Reference SQL (zquery log)

```sql
WITH 
cheerful as (
SELECT
    "customer_address_customer_address"."CA_ZIP" as "customer_address_zip",
    count("customer_customers"."C_CUSTOMER_SK") as "zip_p_count"
FROM
    "memory"."customer_address" as "customer_address_customer_address"
    INNER JOIN "memory"."customer" as "customer_customers" on "customer_address_customer_address"."CA_ADDRESS_SK" = "customer_customers"."C_CURRENT_ADDR_SK"
WHERE
    "customer_customers"."C_PREFERRED_CUST_FLAG" = 'Y'

GROUP BY
    1),
quizzical as (
SELECT
    unnest(:_virt_7180871482901048) as "zips_pre"
),
cooperative as (
SELECT
    SUBSTRING(CASE WHEN "cheerful"."zip_p_count" > 10 THEN "customer_address_customer_address"."CA_ZIP" ELSE NULL END,1,5) as "_virt_func_substring_4293448550966409"
FROM
    "cheerful"
    INNER JOIN "memory"."customer_address" as "customer_address_customer_address" on "cheerful"."customer_address_zip" = "customer_address_customer_address"."CA_ZIP"
GROUP BY
    1),
yummy as (
SELECT
    SUBSTRING(SUBSTRING(cast("quizzical"."zips_pre" as string),1,5),1,2) as "final_zips"
FROM
    "quizzical"
WHERE
    SUBSTRING(cast("quizzical"."zips_pre" as string),1,5) in (select cooperative."_virt_func_substring_4293448550966409" from cooperative where cooperative."_virt_func_substring_4293448550966409" is not null)

GROUP BY
    1),
abhorrent as (
SELECT
    "store_sales_store_sales"."SS_NET_PROFIT" as "store_sales_net_profit",
    "store_sales_store_store"."S_STORE_NAME" as "store_sales_store_name"
FROM
    "memory"."store_sales" as "store_sales_store_sales"
    INNER JOIN "memory"."date_dim" as "store_sales_date_date" on "store_sales_store_sales"."SS_SOLD_DATE_SK" = "store_sales_date_date"."D_DATE_SK"
    INNER JOIN "memory"."store" as "store_sales_store_store" on "store_sales_store_sales"."SS_STORE_SK" = "store_sales_store_store"."S_STORE_SK"
WHERE
    "store_sales_date_date"."D_QOY" = 2 and "store_sales_date_date"."D_YEAR" = 1998 and SUBSTRING("store_sales_store_store"."S_ZIP",1,2) in (select yummy."final_zips" from yummy where yummy."final_zips" is not null)

GROUP BY
    1,
    2,
    "store_sales_store_sales"."SS_ITEM_SK",
    "store_sales_store_sales"."SS_TICKET_NUMBER")
SELECT
    "abhorrent"."store_sales_store_name" as "store_sales_store_name",
    sum("abhorrent"."store_sales_net_profit") as "store_net_profit"
FROM
    "abhorrent"
GROUP BY
    1
ORDER BY 
    "abhorrent"."store_sales_store_name" asc
LIMIT (100)
```

## v4 generation error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 132, in generate_v4_sql
    info, build_env, _, build_stmt = run_tpcds_query(query_id)
                                     ~~~~~~~~~~~~~~~^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4.py", line 469, in run_tpcds_query
    info = search_concepts(
        mandatory_list=list(build_stmt.output_components),
    ...<4 lines>...
        conditions=[conditions] if conditions else [],
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 92, in search_concepts
    result = _search_concepts(
        mandatory_list,
    ...<5 lines>...
        conditions=conditions,
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\concept_strategies_v4.py", line 58, in _search_concepts
    strategy_node = build_strategy_node(
        group_graph, mandatory_list, environment, g, history
    )
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 386, in build_strategy_node
    for gid in _topological_order(group_graph):
               ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\v4_helper\strategy_builder.py", line 223, in _topological_order
    return list(nx.topological_sort(lineage_only))
  File "C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\algorithms\dag.py", line 308, in topological_sort
    for generation in nx.topological_generations(G):
                      ~~~~~~~~~~~~~~~~~~~~~~~~~~^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\algorithms\dag.py", line 238, in topological_generations
    raise nx.NetworkXUnfeasible(
        "Graph contains a cycle or graph changed during iteration"
    )
networkx.exception.NetworkXUnfeasible: Graph contains a cycle or graph changed during iteration
```

## reference execution error

```
Traceback (most recent call last):
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 187, in run_one
    result.ref_exec_seconds, result.ref_rows = _time(
                                               ~~~~~^
        lambda: execute(con, ref_sql)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 45, in _time
    value = fn()
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 188, in <lambda>
    lambda: execute(con, ref_sql)
            ~~~~~~~^^^^^^^^^^^^^^
  File "C:\Users\ethan\coding_projects\pytrilogy\local_scripts\discovery_v4_compare.py", line 120, in execute
    cursor = con.execute(sql)
_duckdb.ParserException: Parser Error: syntax error at or near ":"

LINE 16:     unnest(:_virt_7180871482901048) as "zips_pre"
                    ^
```
