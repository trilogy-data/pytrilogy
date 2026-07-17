# Trilogy failure analysis — 20260717-023244

- Run `20260717-023235_ingest` | `deepseek/deepseek-chat` | sf=1
- `trilogy` calls: 1619 | failed: 230 (14%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 147 | 64% |
| `syntax-parse` | 74 | 32% |
| `cli-misuse` | 5 | 2% |
| `syntax-missing-alias` | 2 | 1% |
| `type-error` | 1 | 0% |
| `file-not-found` | 1 | 0% |

## Detail

### `other`

- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1858999935.preql`

  ```text
  Resolution error in answer_1858999935.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 6). The requested concepts split into 2 disconnected subgraphs: {_daily_sales_cs_sales}; {_daily_sales_ws_sales, ws.sold_date.dow, ws.sold_date.week_seq}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Undefined concept: date_dim.year. Suggestions: ['store.date_dim.year', 'store.store.date_dim.year', 'catalog.call_center.date_dim.year', 'web.ship_date.year', 'web.sold_date.year', 'catalog.ship_date.year']
  ```
- `trilogy run answer_3863442186.preql`

  ```text
  Syntax error in answer_3863442186.preql: Nothing was executed: parsed 4 definition statement(s) (3 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy run answer_3705756794.preql`

  ```text
  Syntax error in answer_3705756794.preql: Missing source map entry for ws.order_number with pseudonyms set(), have map: {'wr.date_dim.date_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.time_dim.time_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.item.item_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.refunded_customer.customer_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.refunded_cdemo.demo_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.refunded_hdemo.demo_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.refunded_addr.address_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.returning_customer.customer_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.returning_cdemo.demo_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.returning_hdemo.demo_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.returning_addr.address_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.web_page.web_page_sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.reason.sk': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.order_number': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.return_quantity': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.return_amt': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.return_tax': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.return_amt_inc_tax': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.fee': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.return_ship_cost': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.refunded_cash': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.reversed_charge': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.account_credit': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}, 'wr.net_loss': {BuildDatasource(name='web_returns', columns=[BuildColumnAssignment(alias='wr_returned_date_sk', concept=wr.date_dim.date_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returned_time_sk', concept=wr.time_dim.time_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_item_sk', concept=wr.item.item_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_customer_sk', concept=wr.refunded_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cdemo_sk', concept=wr.refunded_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_hdemo_sk', concept=wr.refunded_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_addr_sk', concept=wr.refunded_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_customer_sk', concept=wr.returning_customer.customer_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_cdemo_sk', concept=wr.returning_cdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_hdemo_sk', concept=wr.returning_hdemo.demo_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_returning_addr_sk', concept=wr.returning_addr.address_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>, <Modifier.PARTIAL: 'Partial'>}, origin_address=None), BuildColumnAssignment(alias='wr_web_page_sk', concept=wr.web_page.web_page_sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reason_sk', concept=wr.reason.sk@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_order_number', concept=wr.order_number@Grain<wr.item.item_sk,wr.order_number>, modifiers=set(), origin_address=None), BuildColumnAssignment(alias='wr_return_quantity', concept=wr.return_quantity@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt', concept=wr.return_amt@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_tax', concept=wr.return_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_amt_inc_tax', concept=wr.return_amt_inc_tax@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_fee', concept=wr.fee@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_return_ship_cost', concept=wr.return_ship_cost@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_refunded_cash', concept=wr.refunded_cash@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_reversed_charge', concept=wr.reversed_charge@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_account_credit', concept=wr.account_credit@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None), BuildColumnAssignment(alias='wr_net_loss', concept=wr.net_loss@Grain<wr.item.item_sk,wr.order_number>, modifiers={<Modifier.NULLABLE: 'Nullable'>}, origin_address=None)], address=Address(location='web_returns', write_location=None, quoted=False, exists=True, type=<AddressType.TABLE: 'table'>, partition_columns=[], additional_locations=[]), grain=BuildGrain(components={'wr.order_number', 'wr.item.item_sk'}, where_clause=None, _str='Grain<wr.item.item_sk,wr.order_number>', _str_no_condition='Grain<wr.item.item_sk,wr.order_number>', abstract=False), namespace='wr', metadata=DatasourceMetadata(freshness_concept=None, partition_fields=[], line_no=None), where=None, non_partial_for=None, column_level_partial_addresses={'wr.refunded_customer.customer_sk', 'wr.time_dim.time_sk', 'wr.refunded_addr.address_sk', 'wr.item.item_sk', 'wr.date_dim.date_sk', 'wr.returning_addr.address_sk', 'wr.returning_cdemo.demo_sk', 'wr.returning_customer.customer_sk', 'wr.refunded_cdemo.demo_sk'})}}
  ```
- `trilogy file read raw/customer.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_689898357.preql`

  ```text
  Syntax error in answer_689898357.preql: Comparison `ss.promotion.channel_email = 'N'` matches every value of nullable enum field 'ss.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'ss.promotion.channel_email is not null'.
  ```
- `trilogy run --import raw.store_sales:store_sales select qualifying_zips.qzip; --param zips=24128,76232,65084,87816,83926,77556,20548,26231,43848,15126,91137,…26689,96451,38193,46820,88885,84935,69035,83144,47537,56616,94983,48033,69952,25486,61547,27385,61860,58048,56910,16807,17871,35258,31387,35458,35576`

  ```text
  Syntax error in stdin: Undefined concept: qualifying_zips.qzip (line 2, col 8, in SELECT).
  ```
- `trilogy run --import raw.store_sales:store_sales auto arr <- split('31904,35709', ','); where '31904' in arr select 1 as ok;`

  ```text
  Unexpected error in stdin: Could not render the query: Missing source reference to local.arr. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):
  SELECT
      1 as "ok"

  WHERE
      exists (select 1 from (select unnest(INVALID_REFERENCE_BUG<Missing source reference to local.arr>."arr") as unnest_member from INVALID_REFERENCE_BUG<Missing source reference to local.arr>) as unnest_members where unnest_member is not distinct from '31904')
  ```
- `trilogy run --import raw.store_sales:store_sales auto param_zip_set <- unnest(split('31904,35709,12345', ',')); where '31904' in param_zip_set select 1 as ok;`

  ```text
  Unexpected error in stdin: Could not render the query: Missing source reference to local.param_zip_set. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):
  SELECT
      1 as "ok"

  WHERE
      exists (select 1 from INVALID_REFERENCE_BUG<Missing source reference to local.param_zip_set> where INVALID_REFERENCE_BUG<Missing source reference to local.param_zip_set>."param_zip_set" is not distinct from '31904')
  ```
- `trilogy run --import raw.store_sales:store_sales with param_zips_rs as select unnest(split('31904,35709,12345', ',')) as pz; where '31904' in param_zips_rs.pz select 1 as ok;`

  ```text
  Unexpected error in stdin: Could not render the query: Missing source reference to param_zips_rs.pz. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):
  SELECT
      1 as "ok"

  WHERE
      exists (select 1 from INVALID_REFERENCE_BUG<Missing source reference to param_zips_rs.pz> where INVALID_REFERENCE_BUG<Missing source reference to param_zips_rs.pz>."param_zips_rs_pz" is not distinct from '31904')
  ```
- `trilogy run --import raw.store_sales:store_sales auto param_zips <- split('31904,35709,12345', ','); with test_rs as where '31904' in param_zips select 1 as ok; select test_rs.ok;`

  ```text
  Unexpected error in stdin: Could not render the query: Missing source reference to local.param_zips. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  quizzical as (
  SELECT
      1 as "_test_rs_ok"

  WHERE
      exists (select 1 from (select unnest(INVALID_REFERENCE_BUG<Missing source reference to local.param_zips>."param_zips") as unnest_member from INVALID_REFERENCE_BUG<Missing source reference to local.param_zips>) as unnest_members where unnest_member is not distinct from '31904')
  )
  SELECT
      "quizzical"."_test_rs_ok" as "test_rs_ok"
  FROM
      "quizzical"
  ```
- `trilogy file read answer_3697706765.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Comparison `cs.call_center.county = 'Williamson County'` matches every value of enum field 'cs.call_center.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: ORDER BY contains aggregate `count(sales_base.cs.order_number)` (line 33), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --count(sales_base.cs.order_number) as g order by g desc`.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: Nothing was executed: parsed 3 definition statement(s) (2 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_2524943990.preql`

  ```text
  Syntax error in answer_2524943990.preql: ORDER BY contains aggregate `count(local.order_num)` (line 33), which is not a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order by the alias, e.g. `select ..., --count(local.order_num) as g order by g desc`.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.year, count(grain(item.item_sk, ticket_number)) as cnt limit 5;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - item.item_sk (line 2, in SELECT); did you mean: store_sales.item.item_sk, store_sales.promotion.item.item_sk?
    - local.ticket_number (line 2, in SELECT); did you mean: store_sales.ticket_number?
  ```
- `trilogy run --import raw/catalog_sales:catalog_sales select catalog_sales.sold_date.year, count(grain(item.item_sk, order_number)) as cnt limit 5;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - item.item_sk (line 2, in SELECT); did you mean: catalog_sales.item.item_sk, catalog_sales.promotion.item.item_sk?
    - local.order_number (line 2, in SELECT); did you mean: catalog_sales.order_number?
  ```
- `trilogy run --import raw/store_sales:store_sales --import raw/store_returns:store_returns select store_sales.date_dim.year, store_returns.date_dim.year, count(grain(store_sales.item.item_sk, store_sales.ticket_number)) as cnt limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {cnt, store_sales.date_dim.year}; {store_returns.date_dim.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 34 column 13 (char 1805). Re-issue the call with valid JSON arguments.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 33 column 12 (char 1725). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read answer_1835050598.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2928586490.preql`

  ```text
  Resolution error in answer_2928586490.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 60). The requested concepts split into 2 disconnected subgraphs: {combined_sales.customer_sk, combined_sales.first_name, combined_sales.last_name, combined_sales.line_total}; {customer_all_time.all_time_total}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_2844519538.preql`

  ```text
  Resolution error in answer_2844519538.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {all_agg.all_total, all_agg.stage1.ss.customer.first_name, all_agg.stage1.ss.customer.last_name, all_agg.stage1.store_name, peach_agg.stage1.ss.customer.first_name, peach_agg.stage1.ss.customer.last_name, peach_agg.stage1.store_name}
  ```
- `trilogy run answer_2844519538.preql`

  ```text
  Resolution error in answer_2844519538.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {all_agg.all_total, all_agg.stage1.ss.customer.first_name, all_agg.stage1.ss.customer.last_name, all_agg.stage1.store_name, peach_agg.stage1.ss.customer.first_name, peach_agg.stage1.ss.customer.last_name, peach_agg.stage1.store_name}
  ```
- `trilogy run answer_2844519538.preql`

  ```text
  Resolution error in answer_2844519538.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced individually but not joinable from model: {all_level.all_total, all_level.stage1.ss.customer.first_name, all_level.stage1.ss.customer.last_name, all_level.stage1.store_name, peach_level.stage1.ss.customer.first_name, peach_level.stage1.ss.customer.last_name, peach_level.stage1.store_name}
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3825713089.preql`

  ```text
  Resolution error in answer_3825713089.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 7). The requested concepts split into 3 disconnected subgraphs: {cs.bill_customer.customer_sk, cs.item.item_sk, cs.sold_date.moy, cs.sold_date.year, catalog_sale_net_profit}; {item_code, item_description, store_code, store_name, store_sale_net_profit, ss.customer.customer_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.item_sk, ss.ticket_number}; {store_return_net_loss, sr.customer.customer_sk, sr.date_dim.moy, sr.date_dim.year, sr.item.item_sk, sr.ticket_number}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_4254732537.preql`

  ```text
  Syntax error in answer_4254732537.preql: Comparison `cs.promotion.channel_email = 'N'` matches every value of nullable enum field 'cs.promotion.channel_email', which contains only these values: 'N'. It only excludes nulls; simplify it to 'cs.promotion.channel_email is not null'.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1798498862.preql`

  ```text
  Resolution error in answer_1798498862.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 10). The requested concepts split into 3 disconnected subgraphs: {cs.bill_customer.customer_sk, cs.item.item_sk, cs.sold_date.year, total_catalog_sale_ticket_quantity}; {item_code, item_description, store_code, store_name, total_store_sale_ticket_quantity, ss.customer.customer_sk, ss.date_dim.moy, ss.date_dim.year, ss.item.item_sk}; {total_store_return_quantity, sr.customer.customer_sk, sr.date_dim.moy, sr.date_dim.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3530408544.preql`

  ```text
  Syntax error in answer_3530408544.preql: Comparison `ss.store.county = 'Williamson County'` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy file read answer_3809267817.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 1813 (char 1812). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_142117636.preql`

  ```text
  Resolution error in answer_142117636.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 8). The requested concepts split into 4 disconnected subgraphs: {dd.year, _monthly_stats_month_of_year}; {_monthly_stats_cv, _monthly_stats_mean_qoh, _monthly_stats_stddev_qoh}; {_monthly_stats_it_sk}; {_monthly_stats_wh_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy file read raw/inventory.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_630074061.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3079393690.preql`

  ```text
  Resolution error in answer_3079393690.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 2 disconnected subgraphs: {friday, monday, saturday, sunday, thursday, tuesday, wednesday, ss.date_dim.year}; {store_code, store_name, st.gmt_offset}.
    - `local.store_name` is disconnected — did you mean `ss.store.store_name`? (connected to the other concepts)
  These look like separately-imported copies of models already reachable through a connected import; chain through that path (e.g. `ss.store.store_name`) instead of importing a second, disconnected copy.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.item:it select ss.item.product_name, avg(ss.net_profit) as avg_np, rank(ss.item.item_sk) over (order by ….net_profit) > 0.9 * (select avg(ss2.net_profit) where ss2.store.store_sk = 1 and ss2.customer_address.address_sk is null) order by rnk_asc limit 20;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - ss2.net_profit (line 3, in SELECT); did you mean: ss.net_profit, ss.net_paid, net_profit_avg?
    - ss2.store.store_sk (line 3, col 303, in WHERE); did you mean: ss2.customer_address.address_sk, ss.store.store_sk, ss.store.store_id, ss.store.store_name?
    - ss2.customer_address.address_sk (line 3, col 330, in WHERE); did you mean: ss2.store.store_sk, ss.customer_address.address_sk, ss.customer.customer_address.address_sk, ss.customer_address.address_id?
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.product_name, avg(ss.net_profit) as avg_np, rank(ss.item.item_sk) over (order by avg(ss.net_profit) as…t_profit) > 0.9 * (select avg(ss2.net_profit) where ss2.store.store_sk = 1 and ss2.customer_address.address_sk is null) order by avg_np asc limit 12;`

  ```text
  Syntax error in stdin: 3 undefined concept references; fix all before re-running:
    - ss2.net_profit (line 2, in SELECT); did you mean: ss.net_profit, ss.net_paid, net_profit_avg?
    - ss2.store.store_sk (line 2, col 303, in WHERE); did you mean: ss2.customer_address.address_sk, ss.store.store_sk, ss.store.store_id, ss.store.store_name?
    - ss2.customer_address.address_sk (line 2, col 330, in WHERE); did you mean: ss2.store.store_sk, ss.customer_address.address_sk, ss.customer.customer_address.address_sk, ss.customer_address.address_id?
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/web_sales:ws --import raw/web_returns:wr select ws.item.item_sk, ws.order_number, ws.quantity, ws.net_paid, ws.net_profit, wr.return_quantity, wr.return_amt, ws.sold_date.moy, ws.sold_date.year limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {wr.return_amt, wr.return_quantity}; {ws.item.item_sk, ws.net_paid, ws.net_profit, ws.order_number, ws.quantity, ws.sold_date.moy, ws.sold_date.year}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Syntax error in answer_2940558602.preql: 2 undefined concept references; fix all before re-running:
    - local.channel (line 80, col 10, in ORDER BY); did you mean: combined.channel?
    - item_sk (line 80, col 57, in ORDER BY); did you mean: ws.item.item_sk, wr.item.item_sk, cs.item.item_sk, cr.item.item_sk, ss.item.item_sk, sr.item.item_sk?
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Resolution error in answer_2940558602.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['wr.item.item_sk', 'wr.return_amt', 'wr.return_quantity', 'ws.net_paid', 'ws.net_profit', 'ws.quantity', 'ws.sold_date.moy', 'ws.sold_date.year', 'ws.sold_date.date_sk'], have {'wr.item.item_sk'} or {'wr.order_number'}. Different paths are is: [{'wr.item.item_sk'}, {'wr.order_number'}]")
  ```
- `trilogy run answer_2940558602.preql`

  ```text
  Resolution error in answer_2940558602.preql: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['wr.item.item_sk', 'wr.return_amt', 'wr.return_quantity', 'ws.net_paid', 'ws.net_profit', 'ws.quantity', 'ws.sold_date.moy', 'ws.sold_date.year', 'ws.sold_date.date_sk'], have {'wr.item.item_sk'} or {'wr.order_number'}. Different paths are is: [{'wr.item.item_sk'}, {'wr.order_number'}]")
  ```
- `trilogy file read answer_2940558602.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Syntax error in answer_3560698360.preql: Undefined concept: store_sales.date_dim.year.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3560698360.preql`

  ```text
  Resolution error in answer_3560698360.preql: Discovery error: couldn't source all these concepts into one query; you may need a join or merge to relate them across models. Sourced: {store_chan.dt, store_chan.running_total, store_chan.sk, web_chan.dt, web_chan.running_total, web_chan.sk}; still unresolved: {local._combined_store_running_max, local._combined_web_running_max}
  ```
- `trilogy run answer_3553309440.preql`

  ```text
  Syntax error in answer_3553309440.preql: Nothing was executed: parsed 7 definition statement(s) (4 rowsets, 3 imports) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read answer_3553309440.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read answer_145690531.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1484301313.preql`

  ```text
  Syntax error in answer_1484301313.preql: Comparison `store_sales.promotion.channel_email = 'Y'` can never match enum field 'store_sales.promotion.channel_email', which contains only these values: 'N'. It is always false and should be removed.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 1 column 3726 (char 3725). Re-issue the call with valid JSON arguments.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Nothing was executed: parsed 8 definition statement(s) (4 imports, 2 concepts, 2 rowsets) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Syntax error in answer_3544057080.preql: Conflicting join types (full, left outer) on keys joined into one group: a FULL/UNION join cannot be mixed with another type on the same key (it is ambiguous whether the key is required or one-sided). Make the whole group one type (e.g. `UNION JOIN a = b = c`), or use a distinct key. (line 28, column 1)
  ```
- `trilogy run answer_3544057080.preql`

  ```text
  Unexpected error in answer_3544057080.preql: Could not render the query: Missing source reference to cs.item.item_sk. A planned reference has no backing source CTE -- typically an unsupported cross-rowset or membership shape the planner could not wire. Review the rowset/join structure (or file an issue if the query looks valid).

  Full SQL with sentinel(s):

  WITH
  thoughtful as (
  SELECT
      "cs_catalog_sales"."cs_item_sk" as "cr_item_item_sk",
      sum("cs_catalog_sales"."cs_ext_list_price") as "cs_ext_list_by_item"
  FROM
      "catalog_sales" as "cs_catalog_sales"
  GROUP BY
      1),
  wakeful as (
  SELECT
      "cr_catalog_returns"."cr_refunded_cash" as "cr_refunded_cash",
      "cr_catalog_returns"."cr_reversed_charge" as "cr_reversed_charge",
      "cr_catalog_returns"."cr_store_credit" as "cr_store_credit",
      coalesce("cr_catalog_returns"."cr_item_sk","cs_catalog_sales"."cs_item_sk") as "cr_item_item_sk",
      coalesce("cr_catalog_returns"."cr_order_number","cs_catalog_sales"."cs_order_number") as "cr_order_number"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      FULL JOIN "catalog_returns" as "cr_catalog_returns" on "cs_catalog_sales"."cs_item_sk" = "cr_catalog_returns"."cr_item_sk" AND "cs_catalog_sales"."cs_order_number" = "cr_catalog_returns"."cr_order_number"),
  friendly as (
  SELECT
      "ss_customer_customer_address_customer_address"."ca_city" as "_ss_sr_c_city",
      "ss_customer_customer_address_customer_address"."ca_street_name" as "_ss_sr_c_street_name",
      "ss_customer_customer_address_customer_address"."ca_street_number" as "_ss_sr_c_street_num",
      "ss_customer_customer_address_customer_address"."ca_zip" as "_ss_sr_c_zip",
      "ss_customer_first_sales_date_date_dim"."d_year" as "_ss_sr_first_sales_yr",
      "ss_customer_first_shipto_date_date_dim"."d_year" as "_ss_sr_first_ship_yr",
      "ss_date_dim_date_dim"."d_year" as "_ss_sr_sale_year",
      "ss_item_item"."i_product_name" as "_ss_sr_product_name",
      "ss_store_sales"."ss_coupon_amt" as "_ss_sr_ca",
      "ss_store_sales"."ss_list_price" as "_ss_sr_lp",
      "ss_store_sales"."ss_wholesale_cost" as "_ss_sr_wc",
      "ss_store_store"."s_city" as "_ss_sr_s_city",
      "ss_store_store"."s_state" as "_ss_sr_s_state",
      "ss_store_store"."s_store_name" as "_ss_sr_store_name",
      "ss_store_store"."s_street_name" as "_ss_sr_s_street_name",
      "ss_store_store"."s_street_number" as "_ss_sr_s_street_num",
      "ss_store_store"."s_zip" as "_ss_sr_store_zip",
      coalesce("sr_store_returns"."sr_item_sk","ss_item_item"."i_item_sk","ss_store_sales"."ss_item_sk") as "_ss_sr_item_sk",
      coalesce("sr_store_returns"."sr_ticket_number","ss_store_sales"."ss_ticket_number") as "_ss_sr_tn"
  FROM
      "store_returns" as "sr_store_returns"
      INNER JOIN "store_sales" as "ss_store_sales" on "sr_store_returns"."sr_item_sk" = "ss_store_sales"."ss_item_sk" AND "sr_store_returns"."sr_ticket_number" = "ss_store_sales"."ss_ticket_number"
      INNER JOIN "item" as "ss_item_item" on "sr_store_returns"."sr_item_sk" = "ss_item_item"."i_item_sk"
      INNER JOIN "customer" as "ss_customer_customer" on "ss_store_sales"."ss_customer_sk" = "ss_customer_customer"."c_customer_sk"
      INNER JOIN "customer_demographics" as "ss_customer_demographics_customer_demographics" on "ss_store_sales"."ss_cdemo_sk" = "ss_customer_demographics_customer_demographics"."cd_demo_sk"
      INNER JOIN "customer_address" as "ss_customer_customer_address_customer_address" on "ss_customer_customer"."c_current_addr_sk" = "ss_customer_customer_address_customer_address"."ca_address_sk"
      INNER JOIN "customer_demographics" as "ss_customer_customer_demographics_customer_demographics" on "ss_customer_customer"."c_current_cdemo_sk" = "ss_customer_customer_demographics_customer_demographics"."cd_demo_sk"
      LEFT OUTER JOIN "store" as "ss_store_store" on "ss_store_sales"."ss_store_sk" = "ss_store_store"."s_store_sk"
      LEFT OUTER JOIN "date_dim" as "ss_date_dim_date_dim" on "ss_store_sales"."ss_sold_date_sk" = "ss_date_dim_date_dim"."d_date_sk"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_sales_date_date_dim" on "ss_customer_customer"."c_first_sales_date_sk" = "ss_customer_first_sales_date_date_dim"."d_date_sk"
      LEFT OUTER JOIN "date_dim" as "ss_customer_first_shipto_date_date_dim" on "ss_customer_customer"."c_first_shipto_date_sk" = "ss_customer_first_shipto_date_date_dim"."d_date_sk"
  WHERE
      ("ss_item_item"."i_color" is not null and "ss_item_item"."i_color" in ('purple','burlywood','indian','spring','floral','medium')) and "ss_item_item"."i_current_price" >= 65 and "ss_item_item"."i_current_price" <= 74 and "ss_customer_demographics_customer_demographics"."cd_marital_status" != "ss_customer_customer_demographics_customer_demographics"."cd_marital_status"

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19),
  cooperative as (
  SELECT
      "cs_catalog_sales"."cs_order_number" as "cr_order_number",
      "thoughtful"."cs_ext_list_by_item" as "cs_ext_list_by_item",
      coalesce("cs_catalog_sales"."cs_item_sk","thoughtful"."cr_item_item_sk") as "cr_item_item_sk"
  FROM
      "catalog_sales" as "cs_catalog_sales"
      RIGHT OUTER JOIN "thoughtful" on "cs_catalog_sales"."cs_item_sk" = "thoughtful"."cr_item_item_sk"),
  cheerful as (
  SELECT
      "wakeful"."cr_item_item_sk" as "cr_item_item_sk",
      "wakeful"."cr_order_number" as "cr_order_number",
      ( coalesce("wakeful"."cr_refunded_cash",0) + coalesce("wakeful"."cr_reversed_charge",0) ) + coalesce("wakeful"."cr_store_credit",0) as "cr_refund_by_item_order"
  FROM
      "wakeful"),
  questionable as (
  SELECT
      INVALID_REFERENCE_BUG<Missing source reference to cs.item.item_sk> as "qual_cat_item_sk"
  FROM
      "cooperative"
      INNER JOIN "cheerful" on "cooperative"."cr_item_item_sk" is not distinct from "cheerful"."cr_item_item_sk" AND "cooperative"."cr_order_number" is not distinct from "cheerful"."cr_order_number"
  WHERE
      "cooperative"."cs_ext_list_by_item" > 2 * "cheerful"."cr_refund_by_item_order"

  GROUP BY
      1),
  kaput as (
  SELECT
      "friendly"."_ss_sr_c_city" as "_fin_c_city",
      "friendly"."_ss_sr_c_street_name" as "_fin_c_street_name",
      "friendly"."_ss_sr_c_street_num" as "_fin_c_street_num",
      "friendly"."_ss_sr_c_zip" as "_fin_c_zip",
      "friendly"."_ss_sr_ca" as "_fin_ca",
      "friendly"."_ss_sr_first_sales_yr" as "_fin_first_sales_yr",
      "friendly"."_ss_sr_first_ship_yr" as "_fin_first_ship_yr",
      "friendly"."_ss_sr_item_sk" as "_fin_item_sk",
      "friendly"."_ss_sr_lp" as "_fin_lp",
      "friendly"."_ss_sr_product_name" as "_fin_product_name",
      "friendly"."_ss_sr_s_city" as "_fin_s_city",
      "friendly"."_ss_sr_s_state" as "_fin_s_state",
      "friendly"."_ss_sr_s_street_name" as "_fin_s_street_name",
      "friendly"."_ss_sr_s_street_num" as "_fin_s_street_num",
      "friendly"."_ss_sr_sale_year" as "_fin_sale_year",
      "friendly"."_ss_sr_store_name" as "_fin_store_name",
      "friendly"."_ss_sr_store_zip" as "_fin_store_zip",
      "friendly"."_ss_sr_tn" as "_fin_tn",
      "friendly"."_ss_sr_wc" as "_fin_wc"
  FROM
      "friendly"
  WHERE
      exists (select 1 from questionable where questionable."qual_cat_item_sk" is not distinct from "friendly"."_ss_sr_item_sk")

  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19),
  busy as (
  SELECT
      "kaput"."_fin_c_city" as "fin_c_city",
      "kaput"."_fin_c_street_name" as "fin_c_street_name",
      "kaput"."_fin_c_street_num" as "fin_c_street_num",
      "kaput"."_fin_c_zip" as "fin_c_zip",
      "kaput"."_fin_ca" as "fin_ca",
      "kaput"."_fin_first_sales_yr" as "fin_first_sales_yr",
      "kaput"."_fin_first_ship_yr" as "fin_first_ship_yr",
      "kaput"."_fin_item_sk" as "fin_item_sk",
      "kaput"."_fin_lp" as "fin_lp",
      "kaput"."_fin_product_name" as "fin_product_name",
      "kaput"."_fin_s_city" as "fin_s_city",
      "kaput"."_fin_s_state" as "fin_s_state",
      "kaput"."_fin_s_street_name" as "fin_s_street_name",
      "kaput"."_fin_s_street_num" as "fin_s_street_num",
      "kaput"."_fin_sale_year" as "fin_sale_year",
      "kaput"."_fin_store_name" as "fin_store_name",
      "kaput"."_fin_store_zip" as "fin_store_zip",
      "kaput"."_fin_tn" as "fin_tn",
      "kaput"."_fin_wc" as "fin_wc"
  FROM
      "kaput"),
  premium as (
  SELECT
      "busy"."fin_item_sk" as "fin_item_sk",
      "busy"."fin_product_name" as "fin_product_name"
  FROM
      "busy"
  GROUP BY
      1,
      2),
  charming as (
  SELECT
      "busy"."fin_c_city" as "fin_c_city",
      "busy"."fin_c_street_name" as "fin_c_street_name",
      "busy"."fin_c_street_num" as "fin_c_street_num",
      "busy"."fin_c_zip" as "fin_c_zip",
      "busy"."fin_ca" as "fin_ca",
      "busy"."fin_first_sales_yr" as "fin_first_sales_yr",
      "busy"."fin_first_ship_yr" as "fin_first_ship_yr",
      "busy"."fin_item_sk" as "fin_item_sk",
      "busy"."fin_lp" as "fin_lp",
      "busy"."fin_s_city" as "fin_s_city",
      "busy"."fin_s_state" as "fin_s_state",
      "busy"."fin_s_street_name" as "fin_s_street_name",
      "busy"."fin_s_street_num" as "fin_s_street_num",
      "busy"."fin_sale_year" as "fin_sale_year",
      "busy"."fin_store_name" as "fin_store_name",
      "busy"."fin_store_zip" as "fin_store_zip",
      "busy"."fin_tn" as "fin_tn",
      "busy"."fin_wc" as "fin_wc"
  FROM
      "busy"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18),
  protective as (
  SELECT
      "charming"."fin_c_city" as "fin_c_city",
      "charming"."fin_c_street_name" as "fin_c_street_name",
      "charming"."fin_c_street_num" as "fin_c_street_num",
      "charming"."fin_c_zip" as "fin_c_zip",
      "charming"."fin_first_sales_yr" as "fin_first_sales_yr",
      "charming"."fin_first_ship_yr" as "fin_first_ship_yr",
      "charming"."fin_item_sk" as "fin_item_sk",
      "charming"."fin_s_city" as "fin_s_city",
      "charming"."fin_s_state" as "fin_s_state",
      "charming"."fin_s_street_name" as "fin_s_street_name",
      "charming"."fin_s_street_num" as "fin_s_street_num",
      "charming"."fin_sale_year" as "fin_sale_year",
      "charming"."fin_store_name" as "fin_store_name",
      "charming"."fin_store_zip" as "fin_store_zip",
      count(md5(CONCAT_WS('', coalesce(cast("charming"."fin_item_sk" as string),'
  '), coalesce(cast("charming"."fin_tn" as string),'
  ')))) as "_agg_sale_lines",
      sum("charming"."fin_ca") as "_agg_sum_coupon",
      sum("charming"."fin_lp") as "_agg_sum_lprice",
      sum("charming"."fin_wc") as "_agg_sum_wcost"
  FROM
      "charming"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14),
  puzzled as (
  SELECT
      "premium"."fin_product_name" as "_agg_product_name",
      "protective"."_agg_sale_lines" as "_agg_sale_lines",
      "protective"."_agg_sum_coupon" as "_agg_sum_coupon",
      "protective"."_agg_sum_lprice" as "_agg_sum_lprice",
      "protective"."_agg_sum_wcost" as "_agg_sum_wcost",
      "protective"."fin_c_city" as "_agg_c_city",
      "protective"."fin_c_street_name" as "_agg_c_street_name",
      "protective"."fin_c_street_num" as "_agg_c_street_num",
      "protective"."fin_c_zip" as "_agg_c_zip",
      "protective"."fin_first_sales_yr" as "_agg_first_sales_yr",
      "protective"."fin_first_ship_yr" as "_agg_first_ship_yr",
      "protective"."fin_item_sk" as "_agg_item",
      "protective"."fin_s_city" as "_agg_s_city",
      "protective"."fin_s_state" as "_agg_s_state",
      "protective"."fin_s_street_name" as "_agg_s_street_name",
      "protective"."fin_s_street_num" as "_agg_s_street_num",
      "protective"."fin_sale_year" as "_agg_sale_year",
      "protective"."fin_store_name" as "_agg_store_name",
      "protective"."fin_store_zip" as "_agg_store_zip"
  FROM
      "protective"
      INNER JOIN "premium" on "protective"."fin_item_sk" = "premium"."fin_item_sk"),
  waggish as (
  SELECT
      "puzzled"."_agg_c_city" as "agg_c_city",
      "puzzled"."_agg_c_street_name" as "agg_c_street_name",
      "puzzled"."_agg_c_street_num" as "agg_c_street_num",
      "puzzled"."_agg_c_zip" as "agg_c_zip",
      "puzzled"."_agg_first_sales_yr" as "agg_first_sales_yr",
      "puzzled"."_agg_first_ship_yr" as "agg_first_ship_yr",
      "puzzled"."_agg_item" as "agg_item",
      "puzzled"."_agg_product_name" as "agg_product_name",
      "puzzled"."_agg_s_city" as "agg_s_city",
      "puzzled"."_agg_s_state" as "agg_s_state",
      "puzzled"."_agg_s_street_name" as "agg_s_street_name",
      "puzzled"."_agg_s_street_num" as "agg_s_street_num",
      "puzzled"."_agg_sale_lines" as "agg_sale_lines",
      "puzzled"."_agg_sale_year" as "agg_sale_year",
      "puzzled"."_agg_store_name" as "agg_store_name",
      "puzzled"."_agg_store_zip" as "agg_store_zip",
      "puzzled"."_agg_sum_coupon" as "agg_sum_coupon",
      "puzzled"."_agg_sum_lprice" as "agg_sum_lprice",
      "puzzled"."_agg_sum_wcost" as "agg_sum_wcost"
  FROM
      "puzzled")
  SELECT
      "waggish"."agg_product_name" as "agg_product_name",
      "waggish"."agg_item" as "agg_item",
      "waggish"."agg_store_name" as "agg_store_name",
      "waggish"."agg_store_zip" as "agg_store_zip",
      "waggish"."agg_s_street_num" as "agg_s_street_num",
      "waggish"."agg_s_street_name" as "agg_s_street_name",
      "waggish"."agg_s_city" as "agg_s_city",
      "waggish"."agg_s_state" as "agg_s_state",
      "waggish"."agg_c_street_num" as "agg_c_street_num",
      "waggish"."agg_c_street_name" as "agg_c_street_name",
      "waggish"."agg_c_city" as "agg_c_city",
      "waggish"."agg_c_zip" as "agg_c_zip",
      "waggish"."agg_sale_year" as "agg_sale_year",
      "waggish"."agg_first_sales_yr" as "agg_first_sales_yr",
      "waggish"."agg_first_ship_yr" as "agg_first_ship_yr",
      "waggish"."agg_sale_lines" as "agg_sale_lines",
      "waggish"."agg_sum_wcost" as "agg_sum_wcost",
      "waggish"."agg_sum_lprice" as "agg_sum_lprice",
      "waggish"."agg_sum_coupon" as "agg_sum_coupon"
  FROM
      "waggish"
  GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19
  LIMIT (10)
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe catalog_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy database describe web_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Comparison `ss.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy run answer_1772060640.preql`

  ```text
  Syntax error in answer_1772060640.preql: Comparison `ss.store.county in ('Orange County', 'Bronx County', 'Franklin Parish', 'Williamson County')` matches every value of enum field 'ss.store.county', which contains only these values: 'Williamson County'. It is always true and should be removed.
  ```
- `trilogy file read answer_3840922834.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.sold_date.year, count(grain(store_sales.ticket_number, store_sales.item.item_sk)) by store_sales.sold_date.year order by store_sales.sold_date.year;`

  ```text
  Syntax error in stdin: 2 undefined concept references; fix all before re-running:
    - store_sales.sold_date.year (line 2, col 8, in SELECT); did you mean: store_sales.date_dim.year, store_sales.store.date_dim.year, store_sales.promotion.end_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.last_review_date.year, store_sales.promotion.start_date.year?
    - store_sales.sold_date.year (line 2, col 141, in ORDER BY); did you mean: store_sales.date_dim.year, store_sales.store.date_dim.year, store_sales.promotion.end_date.year, store_sales.customer.first_sales_date.year, store_sales.customer.last_review_date.year, store_sales.promotion.start_date.year?
  ```
- `trilogy run answer_3046445280.preql`

  ```text
  Syntax error in answer_3046445280.preql: Nothing was executed: parsed 7 definition statement(s) (6 imports, 1 rowset) but none produce output. A rowset/with/concept file does nothing on its own — add a final `select` that consumes them.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/web_sales:ws --import raw/catalog_sales:cs select ws.ship_customer.customer_sk, ws.order_number, ws.item.item_sk, cs.ship_addr.address_sk, cs.order_number, cs.item.item_sk limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 4). The requested concepts split into 2 disconnected subgraphs: {cs.item.item_sk, cs.order_number, cs.ship_addr.address_sk}; {ws.item.item_sk, ws.order_number, ws.ship_customer.customer_sk}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run --import raw/store_sales:ss select ss.item.item_sk, ss.ticket_number, ss.sold_date.date_sk, ss.item.category limit 5;`

  ```text
  Syntax error in stdin: Undefined concept: ss.sold_date.date_sk (line 2, col 43, in SELECT). Suggestions: ['ss.date_dim.date_sk', 'ss.store.date_dim.date_sk', 'ss.promotion.end_date.date_sk', 'ss.customer.first_sales_date.date_sk', 'ss.customer.last_review_date.date_sk', 'ss.promotion.start_date.date_sk']
  ```
- `trilogy run answer_747155668.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy file read answer_747155668.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_3210116865.preql`

  ```text
  Syntax error in answer_3210116865.preql: Undefined concept: outlet (line 113, col 35, in ORDER BY). Suggestions: ['combined.outlet', 'outlet_sk']
  ```
- `trilogy run answer_1226264875.preql`

  ```text
  Syntax error in answer_1226264875.preql: Undefined concept: st.item_sk. Suggestions: ['st.cust_sk', 'st.st_sp', 'cat.item_sk', 'ss.item.item_sk', 'sr.item.item_sk', 'cs.item.item_sk']
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read answer_2910545909.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database describe store_sales`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under raw/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((select 'web channel' as c, sum(web_sales.ext_sales_price) as s limit 10)) -> (c, s);
select test_union.c, test_union.s limit 5;`

  ```text
  Syntax error in stdin: union(...) requires at least two relational arms.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((select 'web channel' as c, sum(web_sales.ext_sale….order_number = web_returns.order_number limit 10), (select 'other' as c, sum(0) as s limit 1)) -> (c, s);
select test_union.c, test_union.s limit 5;`

  ```text
  Resolution error in stdin: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['web_returns.item.item_sk', 'web_returns.order_number', 'web_sales.ext_sales_price'], have {'web_returns.item.item_sk'} or {'web_returns.order_number'}. Different paths are is: [{'web_returns.item.item_sk'}, {'web_returns.order_number'}]")
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((select 'web channel' as c, sum(web_sales.ext_sale….order_number = web_returns.order_number limit 10), (select 'other' as c, sum(0) as s limit 1)) -> (c, s); select test_union.c, test_union.s limit 5;`

  ```text
  Resolution error in stdin: (AmbiguousRelationshipResolutionException(...), "Multiple possible concept additions (intermediate join keys) found to resolve ['web_returns.item.item_sk', 'web_returns.order_number', 'web_sales.ext_sales_price'], have {'web_returns.item.item_sk'} or {'web_returns.order_number'}. Different paths are is: [{'web_returns.item.item_sk'}, {'web_returns.order_number'}]")
  ```
- `trilogy run answer_943796012.preql`

  ```text
  Syntax error in answer_943796012.preql: Comparison `store_sales.promotion.channel_tv = 'N'` matches every value of nullable enum field 'store_sales.promotion.channel_tv', which contains only these values: 'N'. It only excludes nulls; simplify it to 'store_sales.promotion.channel_tv is not null'.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/catalog_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run test_4111870542.preql`

  ```text
  Syntax error in test_4111870542.preql: Undefined concept: date_dim.date. Suggestions: ['sr.date_dim.date', 'cr.date_dim.date', 'wr.date_dim.date', 'sr.store.date_dim.date', 'cr.call_center.date_dim.date', 'wr.web_page.access_date.date']
  ```
- `trilogy run test_4111870542.preql`

  ```text
  Syntax error in test_4111870542.preql: Undefined concept: date_dim.date. Suggestions: ['sr.date_dim.date', 'cr.date_dim.date', 'wr.date_dim.date', 'sr.store.date_dim.date', 'cr.call_center.date_dim.date', 'wr.web_page.access_date.date']
  ```
- `trilogy run test_4111870542.preql`

  ```text
  Resolution error in test_4111870542.preql: Discovery error: cannot merge all concepts into one connected query (statement at line 5). The requested concepts split into 3 disconnected subgraphs: {cr_date, cr_week}; {sr_date, sr_week}; {wr_date, wr_week}. Are you missing a join or merge statement to relate them?
  ```
- `trilogy run answer_2852230229.preql`

  ```text
  Syntax error in answer_2852230229.preql: All case expressions must have the same output datatype, got {<DataType.INTEGER: 'int'>, <DataType.BIGINT: 'bigint'>} from {'cast(1,BIGINT)': <DataType.BIGINT: 'bigint'>, 'ref:local.cat_subtotal_rank': <DataType.INTEGER: 'int'>, 'ref:local.detail_rank': <DataType.INTEGER: 'int'>}
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_32504671.preql`

  ```text
  Syntax error in answer_32504671.preql: Comparison `ss.household_demographics.vehicle_count <= 6` matches every value of enum field 'ss.household_demographics.vehicle_count', which contains only these values: -1, 0, 1, 2, 3, 4. It is always true and should be removed.
  ```
- `trilogy `

  ```text
  Tool call 'trilogy' rejected: invalid tool arguments: Expecting ',' delimiter: line 17 column 3 (char 594). Re-issue the call with valid JSON arguments.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1197120511.preql`

  ```text
  Syntax error in answer_1197120511.preql: Undefined concept: sold_date.date. Suggestions: ['ws.sold_date.date', 'ws.ship_date.date', 'ws.promotion.end_date.date', 'ws.web_site.open_date.date', 'ws.web_site.close_date.date', 'ws.promotion.start_date.date']
  ```
- `trilogy file read raw/store_returns.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy run answer_1827331030.preql`

  ```text
  trilogy error: subprocess timed out after 600s.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr select ss.customer.customer_sk, ss.quantity, ss.sales_price where (ss.ticket_number, ss.item.item_sk) in (sr.ticket_number, sr.item.item_sk) and sr.reason.desc = 'reason 28' limit 5;`

  ```text
  Resolution error in stdin: Discovery error: cannot merge all concepts into one connected query (statement at line 3). The requested concepts split into 2 disconnected subgraphs: {sr.reason.desc}; {ss.customer.customer_sk, ss.item.item_sk, ss.quantity, ss.sales_price, ss.ticket_number}. Are you missing a join or merge statement to relate them?
  Note: the membership predicate(s) `(ss.ticket_number, ss.item.item_sk) in (sr.ticket_number, sr.item.item_sk)` span these subgraphs, but membership only filters rows on its left side — it does not join the two sides, so it cannot relate them for outputs or grouping. To combine values from both sides, author a query-scoped join or a merge on shared keys.
  ```
- `trilogy file read answer_1827331030.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/web_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy file read raw/store_sales.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```

### `syntax-parse`

- `trilogy run --import raw/web_sales:ws select ws.sold_date.dow, ws.sold_date.week_seq, sum(ws.net_paid) as sales where ws.sold_date.year = 2000 group by ws.sold_date.dow, ws.sold_date.week_seq order by ws.sold_date.week_seq, ws.sold_date.dow limit 20;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...here ws.sold_date.year = 2000 ??? group by ws.sold_date.dow, ws....
  ```
- `trilogy file write answer_1858999935.preql --content # Combined web + catalog sales per (week_seq, dow)
with all_sales as
union(
    (import raw.web_sales as… ? ratio_data.dow = 6) / nullif(lead(sum(ratio_data.curr_sales ? ratio_data.dow = 6), 53) over (order by ratio_data.week_seq), 0), 2) as sat_ratio
;
`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:6
    |
  4 |     (import raw.web_sales as ws
    |      ^---
    |
    = expected select_statement
  Location:
  ...with all_sales as union(     ( ??? import raw.web_sales as ws

  Write stats: received 2261 chars / 2261 bytes; tail: …' over (order by ratio_data.week_seq), 0), 2) as sat_ratio\\n;\\n'.
  ```
- `trilogy file write answer_1858999935.preql --content import raw.web_sales as ws;
import raw.catalog_sales as cs;

# Stack web and catalog sales per (week_seq…eq where ws.sold_date.year = 2001
        union join cs.sold_date.week_seq where cs.sold_date.year = 2001
    )
order by
    ratio_src.week_seq asc
;`

  ```text
  refused to write 'answer_1858999935.preql': not syntactically valid Trilogy.

  Parse error:
    --> 41:9
     |
  41 |         union join cs.sold_date.week_seq where cs.sold_date.year = 2001
     |         ^---
     |
     = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
  ...sold_date.year = 2001         ??? union join cs.sold_date.week_s...

  Write stats: received 1774 chars / 1774 bytes; tail: …'date.year = 2001\\n    )\\norder by\\n    ratio_src.week_seq asc\\n;'.
  ```
- `trilogy run --import raw/date_dim:d select distinct d.week_seq where d.year = 2001 order by d.week_seq;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...ort raw.date_dim as d; select ??? distinct d.week_seq where d.ye...
  ```
- `trilogy file write test_union.preql -c import raw.store_sales as ss;
import raw.store_returns as sr;

with combined as union(
    (
        where ss.date_dim…eturns,
    coalesce(sum(combined.sales) - sum(combined.net_loss), 0) as net_profit
order by combined.channel, combined.entity nulls first
limit 100;`

  ```text
  refused to write 'test_union.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...ity, sales, returns, net_loss) ???  select     combined.channel,...

  Write stats: received 1145 chars / 1145 bytes; tail: …' by combined.channel, combined.entity nulls first\\nlimit 100;'.
  ```
- `trilogy file write test_join.preql -c import raw.web_sales as ws;
import raw.web_returns as wr;

subset join ws.order_number = wr.order_number;
subset join w…06'::date
  and ws.web_site.site_id is not null
  and wr.date_dim.date between '2000-08-23'::date and '2000-09-06'::date
order by ws_entity
limit 10;`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...mport raw.web_returns as wr;  ??? subset join ws.order_number =

  Write stats: received 514 chars / 514 bytes; tail: …"3'::date and '2000-09-06'::date\\norder by ws_entity\\nlimit 10;".
  ```
- `trilogy run --import raw.item:item select item.category, avg(item.current_price) as cat_avg, count(item.item_sk) as cnt by item.category limit 11;`

  ```text
  Syntax error in stdin:  --> 2:86
    |
  2 | select item.category, avg(item.current_price) as cat_avg, count(item.item_sk) as cnt by item.category limit 11;
    |                                                                                      ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...g, count(item.item_sk) as cnt ??? by item.category limit 11;
  ```
- `trilogy file write answer_3697706765.preql --content import raw.store_sales as store_sales;

parameter zips string;

# Set A: ZIPs from the parameter list (a…s
    )
select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'answer_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
    --> 29:39
     |
  29 |         substring(pref_zips.zp, 1, 2) where pref_zips.zp in param_zips
     |                                       ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...substring(pref_zips.zp, 1, 2) ??? where pref_zips.zp in param_zi...

  Write stats: received 1149 chars / 1149 bytes; tail: …'ofit) as total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3697706765.preql --content import raw.store_sales as store_sales;

parameter zips string;

# ZIPs where >10 preferred customers hav…)
    )
select
    store_sales.store.store_name as store_name,
    sum(store_sales.net_profit) as total_net_profit
order by
    store_name
limit 100;`

  ```text
  refused to write 'answer_3697706765.preql': not syntactically valid Trilogy.

  Parse error:
    --> 27:45
     |
  27 |         substring(pref_zips.pref_zip, 1, 2) where pref_zips.pref_zip in (
     |                                             ^---
     |
     = expected dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, or MULTIPLY_DIVIDE_PERCENT
  Location:
  ...ing(pref_zips.pref_zip, 1, 2) ??? where pref_zips.pref_zip in (

  Write stats: received 1240 chars / 1240 bytes; tail: …'ofit) as total_net_profit\\norder by\\n    store_name\\nlimit 100;'.
  ```
- `trilogy file write answer_3263796749.preql --content import raw.store_sales as store_sales;

with bucket_1_20 as
select
    count(grain(store_sales.ticket_nu…et_61_80,
    case when bucket_81_100.line_count > 165306 then bucket_81_100.avg_discount else bucket_81_100.avg_net_paid end as bucket_81_100
by *;
`

  ```text
  refused to write 'answer_3263796749.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...net_paid end as bucket_81_100 ??? by *;

  Write stats: received 2004 chars / 2004 bytes; tail: …' else bucket_81_100.avg_net_paid end as bucket_81_100\\nby *;\\n'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss
import raw.store_returns as sr
import raw.catalog_sales as cs

where ss.date…item.item_sk
union join ss.customer.customer_sk = cs.bill_customer.customer_sk
order by ss.item.item_id, ss.item.item_desc, ss.store.state
limit 100
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw.store_sales as ss
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw.store_sales as ss import r...

  Write stats: received 1056 chars / 1056 bytes; tail: …'s.item.item_id, ss.item.item_desc, ss.store.state\\nlimit 100\\n'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where ss.d…item.item_sk
union join ss.customer.customer_sk = cs.bill_customer.customer_sk
order by ss.item.item_id, ss.item.item_desc, ss.store.state
limit 100
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tem.item_sk = cs.item.item_sk ??? union join ss.customer.custome...

  Write stats: received 1059 chars / 1059 bytes; tail: …'s.item.item_id, ss.item.item_desc, ss.store.state\\nlimit 100\\n'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where ss.d…k = cs.item.item_sk and ss.customer.customer_sk = cs.bill_customer.customer_sk
order by ss.item.item_id, ss.item.item_desc, ss.store.state
limit 100
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ket_number = sr.ticket_number ??? union join ss.item.item_sk = c...

  Write stats: received 1045 chars / 1045 bytes; tail: …'s.item.item_id, ss.item.item_desc, ss.store.state\\nlimit 100\\n'.
  ```
- `trilogy file write answer_765177085.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

where ss.d…m_sk = cs.item.item_sk, ss.customer.customer_sk = cs.bill_customer.customer_sk
order by ss.item.item_id, ss.item.item_desc, ss.store.state
limit 100
`

  ```text
  refused to write 'answer_765177085.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...) / avg(cs.quantity) as cs_cv ??? union join ss.item.item_sk = s...

  Write stats: received 1039 chars / 1039 bytes; tail: …'s.item.item_id, ss.item.item_desc, ss.store.state\\nlimit 100\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select
    count(ss.quantity) as cnt
union join ss.item.item_sk = sr.item.item_sk, ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
      count(ss.quantity) as cnt ??? union join ss.item.item_sk = s...

  Write stats: received 220 chars / 220 bytes; tail: …'r.item.item_sk, ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select count(ss.quantity) as cnt union join ss.item.item_sk = sr.item.item_sk union join ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tem.item_sk = sr.item.item_sk ??? union join ss.ticket_number =

  Write stats: received 226 chars / 226 bytes; tail: …'m_sk union join ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select count(ss.quantity) as cnt union join ss.item.item_sk = sr.item.item_sk and ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ect count(ss.quantity) as cnt ??? union join ss.item.item_sk = s...

  Write stats: received 219 chars / 219 bytes; tail: …'tem.item_sk and ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select
    count(ss.quantity) as cnt
union join ss.item.item_sk = sr.item.item_sk and ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
      count(ss.quantity) as cnt ??? union join ss.item.item_sk = s...

  Write stats: received 223 chars / 223 bytes; tail: …'tem.item_sk and ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select count(ss.quantity) as cnt union join ss.item.item_sk = sr.item.item_sk and ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ect count(ss.quantity) as cnt ??? union join ss.item.item_sk = s...

  Write stats: received 219 chars / 219 bytes; tail: …'tem.item_sk and ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select
    count(ss.quantity) as cnt
subset join ss.item.item_sk = sr.item.item_sk and ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
      count(ss.quantity) as cnt ??? subset join ss.item.item_sk =

  Write stats: received 224 chars / 224 bytes; tail: …'tem.item_sk and ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select
    count(ss.quantity) as cnt,
union join ss.item.item_sk = sr.item.item_sk and ss.ticket_number = sr.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(ss.quantity) as cnt, ??? union join ss.item.item_sk = s...

  Write stats: received 224 chars / 224 bytes; tail: …'tem.item_sk and ss.ticket_number = sr.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
union join ss.item.item_sk = sr.item.item_sk and ss.ticket_number = sr.ticket_number
select count(ss.quantity) as cnt
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | limit 5
    | ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ect count(ss.quantity) as cnt ??? limit 5

  Write stats: received 219 chars / 219 bytes; tail: …'= sr.ticket_number\\nselect count(ss.quantity) as cnt\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select
    count(ss.quantity) as cnt,
subset join ss.item.item_sk = sr.item.item_sk
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
     count(ss.quantity) as cnt, ??? subset join ss.item.item_sk =

  Write stats: received 185 chars / 185 bytes; tail: …' cnt,\\nsubset join ss.item.item_sk = sr.item.item_sk\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select count(ss.quantity) as cnt subset join ss.item.item_sk = sr.item.item_sk
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ect count(ss.quantity) as cnt ??? subset join ss.item.item_sk =

  Write stats: received 180 chars / 180 bytes; tail: …'s cnt subset join ss.item.item_sk = sr.item.item_sk\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
select count(ss.quantity) as cnt
subset join ss.item.item_sk = sr.item.item_sk
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...ect count(ss.quantity) as cnt ??? subset join ss.item.item_sk =

  Write stats: received 180 chars / 180 bytes; tail: …'s cnt\\nsubset join ss.item.item_sk = sr.item.item_sk\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

with ss_2001 as
where ss.date_dim.year = 2001
sel…    sr_2001_2002.qty as rqty
union join ss_2001.item_sk = sr_2001_2002.item_sk
union join ss_2001.ticket_number = sr_2001_2002.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...tem_sk = sr_2001_2002.item_sk ??? union join ss_2001.ticket_numb...

  Write stats: received 565 chars / 565 bytes; tail: …' ss_2001.ticket_number = sr_2001_2002.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_join.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

with ss_2001 as
where ss.date_dim.year = 2001
sel… sqty,
    sr_2001_2002.qty as rqty
union join ss_2001.item_sk = sr_2001_2002.item_sk and ss_2001.ticket_number = sr_2001_2002.ticket_number
limit 5
`

  ```text
  refused to write 'test_join.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
       sr_2001_2002.qty as rqty ??? union join ss_2001.item_sk = s...

  Write stats: received 558 chars / 558 bytes; tail: …' ss_2001.ticket_number = sr_2001_2002.ticket_number\\nlimit 5\\n'.
  ```
- `trilogy file write test_mem.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

where ss.date_dim.year = 2001
  and ss.ticket_number in sr.ticket_number
select count(ss.quantity) as cnt
limit 5
`

  ```text
  refused to write 'test_mem.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | limit 5
    | ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ect count(ss.quantity) as cnt ??? limit 5

  Write stats: received 177 chars / 177 bytes; tail: …'n sr.ticket_number\\nselect count(ss.quantity) as cnt\\nlimit 5\\n'.
  ```
- `trilogy file write test_min.preql --content import raw.store_sales as ss;
where ss.date_dim.year = 2001
select count(ss.quantity) as cnt
limit 5
`

  ```text
  refused to write 'test_min.preql': not syntactically valid Trilogy.

  Parse error:
   --> 4:1
    |
  4 | limit 5
    | ^---
    |
    = expected metadata, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...ect count(ss.quantity) as cnt ??? limit 5

  Write stats: received 101 chars / 101 bytes; tail: …'te_dim.year = 2001\\nselect count(ss.quantity) as cnt\\nlimit 5\\n'.
  ```
- `trilogy file write answer_2928586490.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

# Frequent it…ales
having sum(combined_sales.line_total) > 0
order by last_name asc nulls first, first_name asc nulls first, total_sales asc nulls first
limit 100;`

  ```text
  refused to write 'answer_2928586490.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ricted with best_customers as ??? union join customer_all_time.c...

  Write stats: received 2668 chars / 2668 bytes; tail: …'name asc nulls first, total_sales asc nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2844519538.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;

# Filtered matched sales base
auto base_s…r.first_name,
    ss.store.store_name,
    peach_sub_total
order by
    ss.customer.last_name,
    ss.customer.first_name,
    ss.store.store_name
;
`

  ```text
  refused to write 'answer_2844519538.preql': not syntactically valid Trilogy.

  Parse error:
    --> 15:1
     |
  15 | union join ss.ticket_number = sr.ticket_number
     | ^---
     |
     = expected LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, COMPARISON_OPERATOR, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, window_sql_over, or OVER_COMPONENT_REF
  Location:
    ss.item.units, ss.item.size ??? union join ss.ticket_number =

  Write stats: received 1321 chars / 1321 bytes; tail: …'name,\\n    ss.customer.first_name,\\n    ss.store.store_name\\n;\\n'.
  ```
- `trilogy run --import raw.store_sales:ss --import raw.store_returns:sr select ss.customer.last_name, ss.customer.first_name, ss.store.store_name, sum(ss.net_p…tomer_address.country) and sr.ticket_number is not null union join ss.ticket_number = sr.ticket_number and ss.item.item_sk = sr.item.item_sk limit 5;`

  ```text
  Syntax error in stdin:  --> 3:304
    |
  3 | select ss.customer.last_name, ss.customer.first_name, ss.store.store_name, sum(ss.net_paid) as sub_total where ss.store.market_id = 8 and ss.customer.customer_address.zip = ss.store.zip and lower(ss.customer.birth_country) != lower(ss.customer.customer_address.country) and sr.ticket_number is not null union join ss.ticket_number = sr.ticket_number and ss.item.item_sk = sr.item.item_sk limit 5;
    |                                                                                                                                                                                                                                                                                                                ^---
    |
    = expected limit, order_by, having, LOGICAL_OR, LOGICAL_AND, dot_tail, bracket_tail, dcolon_tail, PLUS_OR_MINUS, MULTIPLY_DIVIDE_PERCENT, or select_grouping
  Location:
   sr.ticket_number is not null ??? union join ss.ticket_number =
  ```
- `trilogy file write answer_3825713089.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;

# Store s…_return_net_loss,
    sum(cs_filtered.net_profit) as catalog_sale_net_profit
order by item_code, item_description, store_code, store_name
limit 100;
`

  ```text
  refused to write 'answer_3825713089.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...r = sr_filtered.ticket_number ??? union join ss_april.item_sk =

  Write stats: received 1822 chars / 1822 bytes; tail: …'m_code, item_description, store_code, store_name\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_751385098.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales as…),
    (catalog_combos),
    (web_combos)
) -> (last_name, first_name, sale_date);

select
    count(intersection.sale_date) as count_val
limit 100;
`

  ```text
  refused to write 'answer_751385098.preql': not syntactically valid Trilogy.

  Parse error:
    --> 40:6
     |
  40 |     (store_combos),
     |      ^---
     |
     = expected select_statement
  Location:
  ...tersection as intersect(     ( ??? store_combos),     (catalog_co...

  Write stats: received 1481 chars / 1481 bytes; tail: …'t\\n    count(intersection.sale_date) as count_val\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_142117636.preql --content import raw.inventory as inv;

# Step 1: Compute per (warehouse_sk, item_sk, moy) for year 2001
with month…_data.it_sk,
    jan_data.month_of_year,
    jan_data.mean_qoh,
    jan_data.cv,
    feb_data.month_of_year,
    feb_data.mean_qoh,
    feb_data.cv
;`

  ```text
  refused to write 'answer_142117636.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...n_data.wh_sk = feb_data.wh_sk ??? union join jan_data.it_sk = fe...

  Write stats: received 1881 chars / 1881 bytes; tail: …'data.month_of_year,\\n    feb_data.mean_qoh,\\n    feb_data.cv\\n;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.item.item_sk, ss.item.product_name, avg(ss.net_profit) as avg_np where ss.store.store_sk = 1 group by 1,2 order by avg_np desc limit 100;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...p where ss.store.store_sk = 1 ??? group by 1,2 order by avg_np d...
  ```
- `trilogy file write answer_3560698360.preql --content # Compute store daily totals per item
def store_daily as
where store_sales.date_dim.year = 2000
  and st…s store_running_total,
    combined.web_running_max,
    combined.store_running_max
order by combined.item_sk asc, combined.sale_date asc
limit 100
;`

  ```text
  refused to write 'answer_3560698360.preql': not syntactically valid Trilogy.

  Parse error:
   --> 2:1
    |
  2 | def store_daily as
    | ^---
    |
    = expected EOI, block, or show_statement
  Location:
  ...e store daily totals per item ??? def store_daily as where store...

  Write stats: received 1940 chars / 1940 bytes; tail: …' by combined.item_sk asc, combined.sale_date asc\\nlimit 100\\n;'.
  ```
- `trilogy run --import raw.item:item select distinct item.class where item.category = 'Women' limit 20;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.class where item...
  ```
- `trilogy run --import raw.store_sales:ss select ss.customer.customer_sk, ss.date_dim.month_seq, ss.store.county, ss.store.state, ss.customer.customer_address.…ustomer_sk, ss.date_dim.month_seq, ss.store.county, ss.store.state, ss.customer.customer_address.county, ss.customer.customer_address.state limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...nth_seq between 1188 and 1190 ??? group by ss.customer.customer_...
  ```
- `trilogy file write answer_3247656127.preql --content import raw.store_sales as store_sales;
import raw.catalog_sales as catalog_sales;
import raw.web_sales a…ode as item_code,
    sum(all_channels.total_sales) as total_sales
order by
    total_sales asc nulls first,
    item_code asc nulls first
limit 100;`

  ```text
  refused to write 'answer_3247656127.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:6
    |
  7 |     (store_sales):
    |      ^---
    |
    = expected select_statement
  Location:
  ...h all_channels as union(     ( ??? store_sales):     where

  Write stats: received 1577 chars / 1577 bytes; tail: …'es asc nulls first,\\n    item_code asc nulls first\\nlimit 100;'.
  ```
- `trilogy run --import raw/catalog_sales:cs select cs.item.category, cs.item.brand, cs.call_center.name, cs.sold_date.year, cs.sold_date.moy, sum(cs.sales_price) as monthly_total where cs.sold_date.year = 1999 group by 1,2,3,4,5 limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...here cs.sold_date.year = 1999 ??? group by 1,2,3,4,5 limit 10;
  ```
- `trilogy file write answer_145690531.preql -e -c import raw/store_sales as store;
import raw/catalog_sales as catalog;
import raw/web_sales as web;
import raw…otals.catalog_total <= 1.1 * web_totals.web_total
order by
    store_totals.item_id nulls first,
    store_totals.store_total nulls first
limit 100;
`

  ```text
  refused to write 'answer_145690531.preql': not syntactically valid Trilogy.

  Parse error:
   --> 1:8
    |
  1 | import raw/store_sales as store;
    |        ^---
    |
    = expected IMPORT_DOT
  Location:
  import ??? raw/store_sales as store; impo...

  Write stats: received 2437 chars / 2437 bytes; tail: …' first,\\n    store_totals.store_total nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_1623435181.preql --content import raw.store_sales as ss;

# Define a macro for pivoted day sums
def day_sum(d) -> sum(ss.sales_pric…at_01 / year2002.sat_02) as sat_ratio
order by store_name asc nulls first, store_code asc nulls first, this_year_week_seq asc nulls first
limit 100;
`

  ```text
  refused to write 'answer_1623435181.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...1 + 52 = year2002.week_seq_02 ??? union join year2001.store_code...

  Write stats: received 2052 chars / 2052 bytes; tail: …' nulls first, this_year_week_seq asc nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_345752060.preql --content import raw.item as item;

# Music-category item codes
with music_items as
where item.category = 'Music'
s…ct
    all_sales.item_code,
    sum(all_sales.channel_total) as total
order by
    all_sales.item_code nulls first,
    total nulls first
limit 100;
`

  ```text
  refused to write 'answer_345752060.preql': not syntactically valid Trilogy.

  Parse error:
    --> 12:6
     |
  12 |     (import raw.store_sales as ss
     |      ^---
     |
     = expected select_statement
  Location:
  ...with all_sales as union(     ( ??? import raw.store_sales as ss

  Write stats: received 1309 chars / 1309 bytes; tail: …'les.item_code nulls first,\\n    total nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_3544057080.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.s…as sale_lines,
    sum(pre_agg.wc) as sum_wcost,
    sum(pre_agg.lp) as sum_lprice,
    sum(pre_agg.ca) as sum_coupon
;

select * from agg limit 10
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
    --> 80:8
     |
  80 | select * from agg limit 10
     |        ^---
     |
     = expected select_item
  Location:
  ...g.ca) as sum_coupon ;  select ??? * from agg limit 10 ;

  Write stats: received 2998 chars / 2998 bytes; tail: …'um(pre_agg.ca) as sum_coupon\\n;\\n\\nselect * from agg limit 10\\n;'.
  ```
- `trilogy run --import raw/web_sales:ws select ws.ship_mode.carrier, count(grain(ws.item.item_sk,ws.order_number)) group by ws.ship_mode.carrier order by ws.ship_mode.carrier;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tem.item_sk,ws.order_number)) ??? group by ws.ship_mode.carrier
  ```
- `trilogy run --import raw/web_sales:ws select distinct ws.time_dim.time limit 10;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...t raw.web_sales as ws; select ??? distinct ws.time_dim.time limi...
  ```
- `trilogy run --import raw/store_sales:ss select ss.store.state, count(ss.ticket_number) as cnt, sum(ss.net_profit) as total_profit where year(ss.date_dim.date) = 2000 and ss.store.store_sk is not null group by ss.store.state limit 50`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ss.store.store_sk is not null ??? group by ss.store.state limit
  ```
- `trilogy file write answer_42596196.preql --content import raw.catalog_sales as cs;
import raw.store_sales as ss;
import raw.web_sales as ws;

# Combine exten…rice
order by
  total_ext_sales_price desc nulls first,
  combined.brand_id nulls first,
  combined.hour nulls first,
  combined.minute nulls first
;`

  ```text
  refused to write 'answer_42596196.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...rand, hour, minute, ext_price) ???  select   combined.brand_id,

  Write stats: received 1661 chars / 1661 bytes; tail: …'  combined.hour nulls first,\\n  combined.minute nulls first\\n;'.
  ```
- `trilogy file write answer_2091535883.preql --content import raw.catalog_sales as cs;
import raw.inventory as inv;

# Filter catalog sales to 1999 with requir…tched.warehouse_sk)) as total_count
order by
    total_count desc,
    matched.item_desc,
    matched.warehouse_name,
    matched.week_seq
limit 100;`

  ```text
  refused to write 'answer_2091535883.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [226]: Misplaced `subset|union join`. The key looks fine - the join is in the wrong PLACE. A query-scoped join is part of a `select` statement, not a standalone statement and not a pre-`where` clause. Put it right after the select list (preferred, SQL-like): `where <filters> select <cols> subset join a.key = b.key`. The clause order is `where` -> `select` <cols> -> join(s) -> `having` -> `order by` -> `limit`; a join may also sit between `where` and `select`, but never before `where` and never on its own. Full reference: `trilogy agent-info syntax example query-structure`.
  Location:
  ...ed.item_sk = inv.item.item_sk ??? union join cs_filtered.week_se...

  Write stats: received 1892 chars / 1892 bytes; tail: …'\\n    matched.warehouse_name,\\n    matched.week_seq\\nlimit 100;'.
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.year, count(grain(store_sales.ticket_number, store_sales.item.item_sk)) as cnt by store_sales.date_dim.year having cnt > 1000 order by store_sales.date_dim.year;`

  ```text
  Syntax error in stdin:  --> 2:108
    |
  2 | select store_sales.date_dim.year, count(grain(store_sales.ticket_number, store_sales.item.item_sk)) as cnt by store_sales.date_dim.year having cnt > 1000 order by store_sales.date_dim.year;
    |                                                                                                            ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...e_sales.item.item_sk)) as cnt ??? by store_sales.date_dim.year h...
  ```
- `trilogy run --import raw/store_sales:store_sales select store_sales.date_dim.year, count(grain(store_sales.ticket_number, store_sales.item.item_sk)) as cnt by store_sales.date_dim.year order by store_sales.date_dim.year limit 10;`

  ```text
  Syntax error in stdin:  --> 2:108
    |
  2 | select store_sales.date_dim.year, count(grain(store_sales.ticket_number, store_sales.item.item_sk)) as cnt by store_sales.date_dim.year order by store_sales.date_dim.year limit 10;
    |                                                                                                            ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...e_sales.item.item_sk)) as cnt ??? by store_sales.date_dim.year o...
  ```
- `trilogy file write answer_3046445280.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sal…ly.total_qty as prev_qty,
    yearly.total_amt as prev_amt
union join yearly.year = 2002
having
    yearly.year = 2002
select
    2002 as curr_year;
`

  ```text
  refused to write 'answer_3046445280.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   yearly.total_amt as prev_amt ??? union join yearly.year = 2002

  Write stats: received 2503 chars / 2503 bytes; tail: …'having\\n    yearly.year = 2002\\nselect\\n    2002 as curr_year;\\n'.
  ```
- `trilogy run --import raw/item:item select distinct item.category, item.category_id order by item.category_id;`

  ```text
  Syntax error in stdin: Syntax [224]: Using `SELECT DISTINCT`? Trilogy has no DISTINCT keyword - a select is already grouped by its non-aggregate columns, so listing the columns you want already returns distinct rows. Remove `distinct`: write `select s.channel, s.channel_dim_text_id` (not `select distinct s.channel, ...`).
  Location:
  ...port raw.item as item; select ??? distinct item.category, item.c...
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …bined.missing_ref nulls first,
  combined.sale_year nulls first,
  combined.sale_quarter nulls first,
  combined.item_category nulls first
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:5
    |
  7 |     auto missing_flag <- 1 ? ss.store.store_sk is null;
    |     ^---
    |
    = expected select_statement
  Location:
  ...th combined as union(   (     ??? auto missing_flag <- 1 ? ss.st...

  Write stats: received 2349 chars / 2349 bytes; tail: …'nulls first,\\n  combined.item_category nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_747155668.preql --content import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;

with combined …tal_ext_price
order by
  combined.channel,
  combined.missing_ref,
  combined.sale_year,
  combined.sale_quarter,
  combined.item_category
limit 100;`

  ```text
  refused to write 'answer_747155668.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ..., line_count, total_ext_price) ???  select   combined.channel,

  Write stats: received 1968 chars / 1968 bytes; tail: …'  combined.sale_quarter,\\n  combined.item_category\\nlimit 100;'.
  ```
- `trilogy run --import raw.store_sales:ss select ss.store.store_sk, sum(ss.ext_sales_price) as sales_amt where ss.date_dim.date between '2000-08-23'::date and '2000-09-22'::date and ss.store.store_sk is not null group by ss.store.store_sk limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...ss.store.store_sk is not null ??? group by ss.store.store_sk lim...
  ```
- `trilogy file write answer_3210116865.preql --content import raw.store_sales as ss;
import raw.store_returns as sr;
import raw.catalog_sales as cs;
import raw…l_profit
by rollup (combined.channel_label, combined.outlet)
order by channel asc nulls first, outlet asc nulls first, total_returns desc
limit 100;
`

  ```text
  refused to write 'answer_3210116865.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...utlet, sales, returns, profit) ??? select     combined.channel_l...

  Write stats: received 4234 chars / 4234 bytes; tail: …'irst, outlet asc nulls first, total_returns desc\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sale….profit) as profit
by rollup (combined.channel, combined.outlet_id)
order by combined.channel nulls first, combined.outlet_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   web_returns.item.item_sk     ??? subset join web_sales.order_nu...

  Write stats: received 2747 chars / 2747 bytes; tail: …'nnel nulls first, combined.outlet_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sale….profit) as profit
by rollup (combined.channel, combined.outlet_id)
order by combined.channel nulls first, combined.outlet_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   web_returns.item.item_sk     ??? subset join web_sales.order_nu...

  Write stats: received 2747 chars / 2747 bytes; tail: …'nnel nulls first, combined.outlet_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sale….profit) as profit
by rollup (combined.channel, combined.outlet_id)
order by combined.channel nulls first, combined.outlet_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
   web_returns.item.item_sk     ??? subset join web_sales.order_nu...

  Write stats: received 2687 chars / 2687 bytes; tail: …'nnel nulls first, combined.outlet_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((where web_sales.sold_date.date between '2000-08-2…eb_returns.item.item_sk subset join web_sales.order_number = web_returns.order_number limit 10)) -> (c, s) select test_union.c, test_union.s limit 5;`

  ```text
  Syntax error in stdin: Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...sk = web_returns.item.item_sk ??? subset join web_sales.order_nu...
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((where web_sales.sold_date.date between '2000-08-2…m_sk = web_returns.item.item_sk and web_sales.order_number = web_returns.order_number limit 10)) -> (c, s) select test_union.c, test_union.s limit 5;`

  ```text
  Syntax error in stdin: Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...b_sales.ext_sales_price) as s ??? subset join web_sales.item.ite...
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((select 'web channel' as c, sum(web_sales.ext_sale…em_sk = web_returns.item.item_sk subset join web_sales.order_number = web_returns.order_number)) -> (c, s) select test_union.c, test_union.s limit 5;`

  ```text
  Syntax error in stdin: Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...sk = web_returns.item.item_sk ??? subset join web_sales.order_nu...
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((select 'web channel' as c, sum(web_sales.ext_sale….item.item_sk = web_returns.item.item_sk and web_sales.order_number = web_returns.order_number)) -> (c, s) select test_union.c, test_union.s limit 5;`

  ```text
  Syntax error in stdin: Syntax [225]: Expected a join condition. A query-scoped `subset|union join` needs a key equality - write `subset join a.key = b.key` (or `union join a.key = b.key`). Chain more keys for a composite grain with `= c.key`, and separate independent joins with `and` (`a.k1 = b.k1 and a.k2 = b.k2`). Both sides must be real fields or expressions - `...` is not a placeholder.
  Location:
  ...b_sales.ext_sales_price) as s ??? subset join web_sales.item.ite...
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with test_union as union((select 'web channel' as c, sum(web_sales.ext_sales_price) as s limit 10)) -> (c, s) select test_union.c, test_union.s limit 5;`

  ```text
  Syntax error in stdin: Syntax [222]: Missing `;` - a named definition must be terminated with a semicolon before the next statement. Terminate the `union(...) -> (...)` (or `with NAME as ... ` / `rowset NAME <- ...`) definition with a `;` after its `-> (...)` output signature, then start the consuming `select` on the next line. Example: `with u as union(...) -> (channel, np); select ...`.
  Location:
  ...ice) as s limit 10)) -> (c, s) ??? select test_union.c, test_uni...
  ```
- `trilogy run --import raw.web_sales:web_sales --import raw.web_returns:web_returns with web_data as (select web_sales.web_site.site_id as o, sum(web_sales.ext…em.item_sk and web_sales.order_number = web_returns.order_number group by o limit 10);
select web_data.o, web_data.s, web_data.r, web_data.p limit 5;`

  ```text
  Syntax error in stdin:  --> 3:18
    |
  3 | with web_data as (select web_sales.web_site.site_id as o, sum(web_sales.ext_sales_price) as s, coalesce(sum(web_returns.return_amt),0) as r, sum(web_sales.net_profit) - coalesce(sum(web_returns.net_loss),0) as p subset join web_sales.item.item_sk = web_returns.item.item_sk and web_sales.order_number = web_returns.order_number group by o limit 10);
    |                  ^---
    |
    = expected select_statement, tvf_union_invocation, tvf_except_invocation, or tvf_intersect_invocation
  Location:
  ...web_returns; with web_data as ??? (select web_sales.web_site.sit...
  ```
- `trilogy file write answer_943796012.preql --content import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
import raw.catalog_sale….profit) as profit
by rollup (combined.channel, combined.outlet_id)
order by combined.channel nulls first, combined.outlet_id nulls first
limit 100;
`

  ```text
  refused to write 'answer_943796012.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ...e_ch.returns, store_ch.profit ??? from store_ch),   (select cata...

  Write stats: received 2892 chars / 2892 bytes; tail: …'nnel nulls first, combined.outlet_id nulls first\\nlimit 100;\\n'.
  ```
- `trilogy file write answer_4111870542.preql --content import raw.store_returns as sr;
import raw.catalog_returns as cr;
import raw.web_returns as wr;
import r…ws > 0
    and catalog.catalog_rows > 0
    and web.web_rows > 0
order by
    store.item_code nulls first,
    store.store_qty nulls first
limit 100;`

  ```text
  refused to write 'answer_4111870542.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [223]: `*` is not a valid argument - Trilogy has no `*` row-marker, so `count(*)` / `sum(*)` don't parse. To count rows at the query grain, count a NON-NULL GRAIN KEY: `count(<key>)` (counts are already distinct) - e.g. `count(store_sales.id)`; to count a related dimension's rows, count its key (`count(customer.id)`). It MUST be a key, and one that is not nullable: `count(x)` skips rows where `x` is NULL, so counting a nullable property (a name, a date, any optional field) silently undercounts. When the grain takes SEVERAL keys, name them with `grain(...)`: `count(grain(order_id, item.id))` counts order+item combinations, and `count_distinct(grain(first_name, last_name, sale_date))` counts distinct combinations - `grain()` is never NULL, so combinations with a missing member still count. For any other aggregate, pass the column you mean, e.g. `sum(store_sales.ext_sales_price)`.
  Location:
  ...antity) as store_records,     ??? count(*) as store_rows ;  # Co...

  Write stats: received 1353 chars / 1353 bytes; tail: …'code nulls first,\\n    store.store_qty nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Find customers in Edgewood whose current de…name,
    # hidden grain fields
    sr.ticket_number as #ticket_number,
    sr.item.item_sk as #item_sk
order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 23:21
     |
  23 |     sr.item.item_sk as #item_sk
     |                     ^---
     |
     = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...t_number,     sr.item.item_sk ??? as #item_sk order by customer_...

  Write stats: received 971 chars / 971 bytes; tail: …'sk as #item_sk\\norder by customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;

# Find customers in Edgewood whose current de…irst_name, '')) as full_name,
    sr.ticket_number as --ticket_number,
    sr.item.item_sk as --item_sk
order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
    --> 21:25
     |
  21 |     sr.ticket_number as --ticket_number,
     |                         ^---
     |
     = expected IDENTIFIER
  Location:
  ...name,     sr.ticket_number as ??? --ticket_number,     sr.item.i...

  Write stats: received 947 chars / 947 bytes; tail: …'k as --item_sk\\norder by customer_code nulls first\\nlimit 100;'.
  ```
- `trilogy file write answer_210925266.preql --content import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.web_sales as ws;

with store_com…_customer.first_name,
         ws.sold_date.date)
) -> (last_name, first_name, sale_date);

select
    count(only_store.sale_date) as unique_combos;
`

  ```text
  refused to write 'answer_210925266.preql': not syntactically valid Trilogy.

  Parse error:
    --> 30:6
     |
  30 |     (import raw.store_sales as ss;
     |      ^---
     |
     = expected select_statement
  Location:
  ...th only_store as except(     ( ??? import raw.store_sales as ss;

  Write stats: received 1604 chars / 1604 bytes; tail: …';\\n\\nselect\\n    count(only_store.sale_date) as unique_combos;\\n'.
  ```
- `trilogy run --import raw/store_sales:ss --import raw/store_returns:sr with reason28_returns as where sr.reason.desc = 'reason 28' select sr.ticket_number, sr…son28_returns.ticket_number = ss.ticket_number subset join reason28_returns.item.item_sk = ss.item.item_sk group by ss.customer.customer_sk limit 10;`

  ```text
  Syntax error in stdin: Syntax [103]: Using a GROUP BY clause? Trilogy has no GROUP BY - remove it. Grouping is automatic by the non-aggregated fields in your SELECT. To aggregate at a different grain than the select, write `agg(x) by dim1, dim2` inline (e.g. `sum(sales.amount) by sales.store.id`).
  Location:
  ...tem.item_sk = ss.item.item_sk ??? group by ss.customer.customer_...
  ```
- `trilogy run --import raw.store_sales:store_sales select count(grain(store_sales.ticket_number, store_sales.item.item_sk)) as line_item_count from store_sales…se' and store_sales.time_dim.hour = 20 and store_sales.time_dim.minute >= 30 and store_sales.customer.household_demographics.dep_count = 7 limit 100;`

  ```text
  Syntax error in stdin: Syntax [101]: Using FROM keyword? Trilogy does not have a FROM clause (Datasource resolution is automatic).
  Location:
  ....item_sk)) as line_item_count ??? from store_sales where store_s...
  ```

### `cli-misuse`

- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy run --import raw/web_sales:ws --import raw/web_returns:wr with big_returns as where wr.return_amt > 10000 select wr.item.item_sk as ret_item, wr.orde…ofit, big_returns.ret_qty, big_returns.ret_amt union join ws.item.item_sk = big_returns.ret_item and ws.order_number = big_returns.ret_order limit 5;`

  ```text
  'where ws.sold_date.year = 2001 and ws.sold_date.moy = 12 and ws.net_profit > 1 and ws.net_paid > 0 and ws.quantity > 0 and big_returns.ret_amt > 10000' is not a valid dialect. Choose one of: bigquery, sql_server, duck_db, sqlite, presto, trino, postgres, snowflake, dataframe, clickhouse.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```
- `trilogy explore .`

  ```text
  Invalid value for 'PATH': File '.' is a directory.
  ```
- `trilogy explore raw/`

  ```text
  Invalid value for 'PATH': File 'raw/' is a directory.
  ```

### `syntax-missing-alias`

- `trilogy file write answer_3544057080.preql -e -c import raw.store_sales as ss;
import raw.catalog_sales as cs;
import raw.catalog_returns as cr;
import raw.s…)) as sale_lines,
    sum(joined.wc) as sum_wcost,
    sum(joined.lp) as sum_lprice,
    sum(joined.ca) as sum_coupon
;

select * from agg limit 10
;`

  ```text
  refused to write 'answer_3544057080.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `ss_with_sr.*
  union join ss_with_sr.item_sk = qual_cat.item_sk as ss_with_sr_union_join_ss_with_sr_item_sk`
  Location:
  ...select     ss_with_sr.* union ??? join ss_with_sr.item_sk = qual...

  Write stats: received 3060 chars / 3060 bytes; tail: …'sum(joined.ca) as sum_coupon\\n;\\n\\nselect * from agg limit 10\\n;'.
  ```
- `trilogy file write answer_2869182220.preql --content import raw.store_returns as sr;
import raw.customer as c;
import raw.customer_address as ca;
import raw.…e,
    -- hidden grain fields
    sr.ticket_number as --ticket_number,
    sr.item.item_sk as --item_sk
order by customer_code nulls first
limit 100;`

  ```text
  refused to write 'answer_2869182220.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [201]: Missing alias? Alias must be specified with "AS" - e.g. `SELECT x+1 AS y` Here: `hidden grain fields
      sr.ticket_number as hidden_grain_fields_sr_ticket_number`
  Location:
  ...) as full_name,     -- hidden ??? grain fields     sr.ticket_num...

  Write stats: received 1060 chars / 1060 bytes; tail: …'k as --item_sk\\norder by customer_code nulls first\\nlimit 100;'.
  ```

### `type-error`

- `trilogy run answer_219203908.preql`

  ```text
  Type error in answer_219203908.preql: Invalid argument type 'BIGINT' passed into CONCAT_STRICT function in position 1 from concept: store.ticket_number. Valid: 'STRING'.
  ```

### `file-not-found`

- `trilogy run --import raw/store_sales:ss raw/store_returns:sr select ss.quantity, sr.return_quantity, case when ss.quantity is null then null::bigint else coa…ce(sr.return_quantity, 0::bigint) end as net_qty union join ss.ticket_number = sr.ticket_number union join ss.item.item_sk = sr.item.item_sk limit 5;`

  ```text
  Input 'raw/store_returns:sr' does not exist.
  ```
