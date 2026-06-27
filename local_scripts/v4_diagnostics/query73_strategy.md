# V4 Strategy Diagnostics

- strategy nodes: 11

## Tree

- MergeNode source=merge
  outputs: local.ticket_cnt, store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.ticket_number
  inputs: local.ticket_cnt, store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.ticket_number
  - GroupNode source=group
    outputs: local.ticket_cnt, store_sales.customer.id, store_sales.ticket_number
    inputs: local._virt_filter_id_4484877027926973, store_sales.customer.id, store_sales.ticket_number
    - FilterNode source=filter
      outputs: local._virt_filter_id_4484877027926973, store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.store.id, store_sales.ticket_number
      inputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.store.id, store_sales.ticket_number
      - MergeNode source=merge
        outputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.store.id, store_sales.ticket_number
        inputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.store.id, store_sales.ticket_number
        conditions: store_sales.customer.id is not MagicConstants.NULL
        - SelectNode source=select datasource=store_sales.customer.customers
          outputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation
          inputs: store_sales.customer.address.id, store_sales.customer.birth_country, store_sales.customer.birth_day, store_sales.customer.birth_month, store_sales.customer.birth_year, store_sales.customer.demographics.id, store_sales.customer.email_address, store_sales.customer.first_name, store_sales.customer.first_sales_date.id, store_sales.customer.first_shipto_date.id, store_sales.customer.household_demographic.id, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.last_review_date, store_sales.customer.login, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.customer.text_id
        - SelectNode source=select datasource=store_sales.date.date
          outputs: store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year
          inputs: store_sales.date._date_string, store_sales.date.date, store_sales.date.day_name, store_sales.date.day_of_month, store_sales.date.day_of_week, store_sales.date.id, store_sales.date.month_of_year, store_sales.date.month_seq, store_sales.date.quarter, store_sales.date.quarter_name, store_sales.date.text_id, store_sales.date.week_seq, store_sales.date.year
        - SelectNode source=select datasource=store_sales.household_demographic.household_demographics
          outputs: store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count
          inputs: store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.income_band.id, store_sales.household_demographic.income_band_id, store_sales.household_demographic.vehicle_count
        - SelectNode source=select datasource=store_sales.store.store
          outputs: store_sales.store.county, store_sales.store.id
          inputs: store_sales.store.city, store_sales.store.company_id, store_sales.store.company_name, store_sales.store.county, store_sales.store.date.id, store_sales.store.employees, store_sales.store.floor_space, store_sales.store.geography_class, store_sales.store.gmt_offset, store_sales.store.hours, store_sales.store.id, store_sales.store.market, store_sales.store.market_manager, store_sales.store.name, store_sales.store.state, store_sales.store.store_manager, store_sales.store.street_name, store_sales.store.street_number, store_sales.store.street_type, store_sales.store.suite_number, store_sales.store.tax_percentage, store_sales.store.text_id, store_sales.store.zip
        - SelectNode source=select datasource=store_sales.store_sales
          outputs: store_sales.customer.id, store_sales.date.id, store_sales.household_demographic.id, store_sales.item.id, store_sales.store.id, store_sales.ticket_number
          inputs: store_sales.coupon_amt, store_sales.customer.id, store_sales.customer_demographic.id, store_sales.date.id, store_sales.ext_discount_amount, store_sales.ext_list_price, store_sales.ext_sales_price, store_sales.ext_tax, store_sales.ext_wholesale_cost, store_sales.household_demographic.id, store_sales.item.id, store_sales.list_price, store_sales.net_paid, store_sales.net_profit, store_sales.promotion.id, store_sales.quantity, store_sales.row_counter, store_sales.sale_address.id, store_sales.sales_price, store_sales.store.id, store_sales.ticket_number, store_sales.time.id, store_sales.wholesale_cost
  - GroupNode source=group
    outputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation
    inputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation
    - FilterNode source=filter
      outputs: local._virt_filter_id_4484877027926973, store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.store.id, store_sales.ticket_number
      inputs: store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.id, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.id, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.store.id, store_sales.ticket_number
      - MergeNode source=merge (reused)

## Records

```json
[
  {
    "conditions": null,
    "datasource": "store_sales.customer.customers",
    "existence": [],
    "force_group": false,
    "grain": "Grain<store_sales.customer.id>",
    "hidden": [],
    "id": "n0",
    "inputs": [
      "store_sales.customer.id",
      "store_sales.customer.text_id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.address.id",
      "store_sales.customer.demographics.id",
      "store_sales.customer.household_demographic.id",
      "store_sales.customer.first_sales_date.id",
      "store_sales.customer.first_shipto_date.id",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.birth_country",
      "store_sales.customer.salutation",
      "store_sales.customer.email_address",
      "store_sales.customer.birth_day",
      "store_sales.customer.birth_month",
      "store_sales.customer.birth_year",
      "store_sales.customer.login",
      "store_sales.customer.last_review_date"
    ],
    "nullable": [],
    "outputs": [
      "store_sales.customer.first_name",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "store_sales.customer.first_name",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation"
    ]
  },
  {
    "conditions": null,
    "datasource": "store_sales.date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<store_sales.date.id>",
    "hidden": [],
    "id": "n1",
    "inputs": [
      "store_sales.date.id",
      "store_sales.date.text_id",
      "store_sales.date._date_string",
      "store_sales.date.date",
      "store_sales.date.day_of_week",
      "store_sales.date.day_of_month",
      "store_sales.date.day_name",
      "store_sales.date.week_seq",
      "store_sales.date.month_of_year",
      "store_sales.date.month_seq",
      "store_sales.date.quarter",
      "store_sales.date.quarter_name",
      "store_sales.date.year"
    ],
    "nullable": [
      "store_sales.date.day_of_month"
    ],
    "outputs": [
      "store_sales.date.day_of_month",
      "store_sales.date.id",
      "store_sales.date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "store_sales.date.day_of_month",
      "store_sales.date.id",
      "store_sales.date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "store_sales.household_demographic.household_demographics",
    "existence": [],
    "force_group": false,
    "grain": "Grain<store_sales.household_demographic.id>",
    "hidden": [],
    "id": "n2",
    "inputs": [
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.income_band_id",
      "store_sales.household_demographic.income_band.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count"
    ],
    "nullable": [],
    "outputs": [
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.vehicle_count"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.vehicle_count"
    ]
  },
  {
    "conditions": null,
    "datasource": "store_sales.store.store",
    "existence": [],
    "force_group": false,
    "grain": "Grain<store_sales.store.id>",
    "hidden": [],
    "id": "n3",
    "inputs": [
      "store_sales.store.id",
      "store_sales.store.text_id",
      "store_sales.store.date.id",
      "store_sales.store.name",
      "store_sales.store.employees",
      "store_sales.store.floor_space",
      "store_sales.store.hours",
      "store_sales.store.store_manager",
      "store_sales.store.geography_class",
      "store_sales.store.market_manager",
      "store_sales.store.tax_percentage",
      "store_sales.store.city",
      "store_sales.store.state",
      "store_sales.store.county",
      "store_sales.store.zip",
      "store_sales.store.market",
      "store_sales.store.gmt_offset",
      "store_sales.store.company_name",
      "store_sales.store.company_id",
      "store_sales.store.street_number",
      "store_sales.store.street_name",
      "store_sales.store.street_type",
      "store_sales.store.suite_number"
    ],
    "nullable": [],
    "outputs": [
      "store_sales.store.county",
      "store_sales.store.id"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "store_sales.store.county",
      "store_sales.store.id"
    ]
  },
  {
    "conditions": null,
    "datasource": "store_sales.store_sales",
    "existence": [],
    "force_group": false,
    "grain": "Grain<store_sales.item.id,store_sales.ticket_number>",
    "hidden": [],
    "id": "n4",
    "inputs": [
      "store_sales.row_counter",
      "store_sales.date.id",
      "store_sales.time.id",
      "store_sales.customer.id",
      "store_sales.customer_demographic.id",
      "store_sales.household_demographic.id",
      "store_sales.ticket_number",
      "store_sales.item.id",
      "store_sales.sales_price",
      "store_sales.list_price",
      "store_sales.ext_sales_price",
      "store_sales.ext_list_price",
      "store_sales.ext_wholesale_cost",
      "store_sales.ext_discount_amount",
      "store_sales.net_profit",
      "store_sales.promotion.id",
      "store_sales.quantity",
      "store_sales.coupon_amt",
      "store_sales.store.id",
      "store_sales.net_paid",
      "store_sales.ext_tax",
      "store_sales.wholesale_cost",
      "store_sales.sale_address.id"
    ],
    "nullable": [
      "store_sales.customer.id",
      "store_sales.date.id",
      "store_sales.household_demographic.id",
      "store_sales.store.id"
    ],
    "outputs": [
      "store_sales.customer.id",
      "store_sales.date.id",
      "store_sales.household_demographic.id",
      "store_sales.item.id",
      "store_sales.store.id",
      "store_sales.ticket_number"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "store_sales.customer.id",
      "store_sales.date.id",
      "store_sales.household_demographic.id",
      "store_sales.item.id",
      "store_sales.store.id",
      "store_sales.ticket_number"
    ]
  },
  {
    "conditions": "store_sales.customer.id is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n5",
    "inputs": [
      "store_sales.customer.first_name",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.date.day_of_month",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.county",
      "store_sales.store.id",
      "store_sales.item.id",
      "store_sales.ticket_number"
    ],
    "nullable": [
      "store_sales.date.id",
      "store_sales.date.day_of_month",
      "store_sales.household_demographic.id",
      "store_sales.store.id"
    ],
    "outputs": [
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ],
    "parents": [
      "n0",
      "n1",
      "n2",
      "n3",
      "n4"
    ],
    "partials": [],
    "preexisting_conditions": "store_sales.customer.id is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n6",
    "inputs": [
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ],
    "nullable": [
      "store_sales.date.id",
      "store_sales.date.day_of_month",
      "store_sales.household_demographic.id",
      "store_sales.store.id"
    ],
    "outputs": [
      "local._virt_filter_id_4484877027926973",
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ],
    "parents": [
      "n5"
    ],
    "partials": [],
    "preexisting_conditions": "store_sales.customer.id is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "filter",
    "type": "FilterNode",
    "usable_outputs": [
      "local._virt_filter_id_4484877027926973",
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n7",
    "inputs": [
      "local._virt_filter_id_4484877027926973",
      "store_sales.customer.id",
      "store_sales.ticket_number"
    ],
    "nullable": [],
    "outputs": [
      "local.ticket_cnt",
      "store_sales.customer.id",
      "store_sales.ticket_number"
    ],
    "parents": [
      "n6"
    ],
    "partials": [],
    "preexisting_conditions": "store_sales.customer.id is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local.ticket_cnt",
      "store_sales.customer.id",
      "store_sales.ticket_number"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n8",
    "inputs": [
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ],
    "nullable": [
      "store_sales.date.id",
      "store_sales.date.day_of_month",
      "store_sales.household_demographic.id",
      "store_sales.store.id"
    ],
    "outputs": [
      "local._virt_filter_id_4484877027926973",
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ],
    "parents": [
      "n5"
    ],
    "partials": [],
    "preexisting_conditions": "store_sales.customer.id is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "filter",
    "type": "FilterNode",
    "usable_outputs": [
      "local._virt_filter_id_4484877027926973",
      "store_sales.item.id",
      "store_sales.date.id",
      "store_sales.date.year",
      "store_sales.date.day_of_month",
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.household_demographic.id",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.id",
      "store_sales.store.county",
      "store_sales.ticket_number"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n9",
    "inputs": [
      "store_sales.customer.first_name",
      "store_sales.customer.last_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.customer.id"
    ],
    "nullable": [],
    "outputs": [
      "store_sales.customer.first_name",
      "store_sales.customer.last_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.customer.id"
    ],
    "parents": [
      "n8"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "store_sales.customer.first_name",
      "store_sales.customer.last_name",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.customer.salutation",
      "store_sales.customer.id"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": "Grain<store_sales.customer.id,store_sales.ticket_number>",
    "hidden": [],
    "id": "n10",
    "inputs": [
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.salutation",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.ticket_number",
      "local.ticket_cnt"
    ],
    "nullable": [],
    "outputs": [
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.salutation",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.ticket_number",
      "local.ticket_cnt"
    ],
    "parents": [
      "n7",
      "n9"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "store_sales.customer.id",
      "store_sales.customer.last_name",
      "store_sales.customer.first_name",
      "store_sales.customer.salutation",
      "store_sales.customer.preferred_cust_flag",
      "store_sales.ticket_number",
      "local.ticket_cnt"
    ]
  }
]
```
