# V4 Strategy Diagnostics

- strategy nodes: 29

## Tree

- MergeNode source=merge
  outputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id, local.customer_state
  inputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id, local.customer_state
  - GroupNode source=group
    outputs: cr.billing_customer.id, cr.return_address.state, local.customer_state
    inputs: cr.billing_customer.id, cr.date.year, cr.return_address.state, cr.return_amt_inc_tax, local.customer_state
    - SelectNode source=select
      outputs: cr.billing_customer.address.id, cr.billing_customer.address.state, cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local.customer_state, local.scaled_state
      inputs: cr.billing_customer.address.id, cr.billing_customer.address.state, cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local.customer_state, local.scaled_state
      conditions: local.customer_state > local.scaled_state
      - MergeNode source=merge
        outputs: cr.billing_customer.address.id, cr.billing_customer.address.state, cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local.customer_state, local.scaled_state
        inputs: cr.billing_customer.address.id, cr.billing_customer.address.state, cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local.customer_state, local.scaled_state
        - MergeNode source=merge
          outputs: cr.billing_customer.address.id, cr.billing_customer.address.state, cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
          inputs: cr.billing_customer.address.id, cr.billing_customer.address.state, cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
          conditions: cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL
          - SelectNode source=select datasource=cr.billing_customer.address.customer_address
            outputs: cr.billing_customer.address.id, cr.billing_customer.address.state
            inputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.text_id, cr.billing_customer.address.zip
          - SelectNode source=select datasource=cr.billing_customer.customers
            outputs: cr.billing_customer.address.id, cr.billing_customer.id
            inputs: cr.billing_customer.address.id, cr.billing_customer.birth_country, cr.billing_customer.birth_day, cr.billing_customer.birth_month, cr.billing_customer.birth_year, cr.billing_customer.demographics.id, cr.billing_customer.email_address, cr.billing_customer.first_name, cr.billing_customer.first_sales_date.id, cr.billing_customer.first_shipto_date.id, cr.billing_customer.household_demographic.id, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.last_review_date, cr.billing_customer.login, cr.billing_customer.preferred_cust_flag, cr.billing_customer.salutation, cr.billing_customer.text_id
          - SelectNode source=select datasource=cr.catalog_returns
            outputs: cr.billing_customer.id, cr.date.id, cr.item.id, cr.order_number, cr.return_address.id, cr.return_amt_inc_tax
            inputs: cr.billing_customer.id, cr.call_center.id, cr.date.id, cr.item.id, cr.net_loss, cr.order_number, cr.reason.id, cr.refunded_address.id, cr.refunded_cash, cr.refunded_customer.id, cr.return_address.id, cr.return_amount, cr.return_amt_inc_tax, cr.return_quantity, cr.reversed_charge, cr.sales.order_number, cr.store_credit, cr.time.id
          - SelectNode source=select datasource=cr.date.date
            outputs: cr.date.id, cr.date.year
            inputs: cr.date._date_string, cr.date.date, cr.date.day_name, cr.date.day_of_month, cr.date.day_of_week, cr.date.id, cr.date.month_of_year, cr.date.month_seq, cr.date.quarter, cr.date.quarter_name, cr.date.text_id, cr.date.week_seq, cr.date.year
          - SelectNode source=select datasource=cr.return_address.customer_address
            outputs: cr.return_address.id, cr.return_address.state
            inputs: cr.return_address.city, cr.return_address.country, cr.return_address.county, cr.return_address.gmt_offset, cr.return_address.id, cr.return_address.location_type, cr.return_address.state, cr.return_address.street_name, cr.return_address.street_number, cr.return_address.street_type, cr.return_address.suite_number, cr.return_address.text_id, cr.return_address.zip
        - GroupNode source=group
          outputs: cr.billing_customer.id, cr.return_address.state, local.customer_state
          inputs: cr.billing_customer.id, cr.return_address.state, local._virt_filter_return_amt_inc_tax_2184255153361204
          - MergeNode source=merge
            outputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local._virt_filter_return_amt_inc_tax_2184255153361204
            inputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local._virt_filter_return_amt_inc_tax_2184255153361204
            - FilterNode source=filter
              outputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local._virt_filter_return_amt_inc_tax_2184255153361204
              inputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
              conditions: cr.return_address.state is not MagicConstants.NULL
              - MergeNode source=merge
                outputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
                inputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
                - SelectNode source=select datasource=cr.catalog_returns
                  outputs: cr.billing_customer.id, cr.date.id, cr.item.id, cr.order_number, cr.return_address.id, cr.return_amt_inc_tax
                  inputs: cr.billing_customer.id, cr.call_center.id, cr.date.id, cr.item.id, cr.net_loss, cr.order_number, cr.reason.id, cr.refunded_address.id, cr.refunded_cash, cr.refunded_customer.id, cr.return_address.id, cr.return_amount, cr.return_amt_inc_tax, cr.return_quantity, cr.reversed_charge, cr.sales.order_number, cr.store_credit, cr.time.id
                - SelectNode source=select datasource=cr.date.date
                  outputs: cr.date.id, cr.date.year
                  inputs: cr.date._date_string, cr.date.date, cr.date.day_name, cr.date.day_of_month, cr.date.day_of_week, cr.date.id, cr.date.month_of_year, cr.date.month_seq, cr.date.quarter, cr.date.quarter_name, cr.date.text_id, cr.date.week_seq, cr.date.year
                - SelectNode source=select datasource=cr.return_address.customer_address
                  outputs: cr.return_address.id, cr.return_address.state
                  inputs: cr.return_address.city, cr.return_address.country, cr.return_address.county, cr.return_address.gmt_offset, cr.return_address.id, cr.return_address.location_type, cr.return_address.state, cr.return_address.street_name, cr.return_address.street_number, cr.return_address.street_type, cr.return_address.suite_number, cr.return_address.text_id, cr.return_address.zip
            - MergeNode source=merge
              outputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
              inputs: cr.billing_customer.id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
              - SelectNode source=select datasource=cr.catalog_returns (reused)
              - SelectNode source=select datasource=cr.date.date (reused)
              - SelectNode source=select datasource=cr.return_address.customer_address (reused)
        - SelectNode source=select
          outputs: cr.return_address.state, local.scaled_state
          inputs: cr.return_address.state, local._virt_agg_avg_7052944147524274
          - GroupNode source=group
            outputs: cr.return_address.state, local._virt_agg_avg_7052944147524274
            inputs: cr.return_address.state, local.customer_state
            - GroupNode source=group
              outputs: cr.billing_customer.id, cr.return_address.state, local.customer_state
              inputs: cr.billing_customer.id, cr.return_address.state, local.customer_state
              - GroupNode source=group
                outputs: cr.billing_customer.id, cr.return_address.state, local.customer_state
                inputs: cr.billing_customer.id, cr.return_address.state, local._virt_filter_return_amt_inc_tax_2184255153361204
                - MergeNode source=merge (reused)
  - FilterNode source=filter
    outputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax, local._virt_filter_return_amt_inc_tax_2184255153361204
    inputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
    - MergeNode source=merge
      outputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
      inputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id, cr.date.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.id, cr.return_address.state, cr.return_amt_inc_tax
      conditions: cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL
      - SelectNode source=select datasource=cr.billing_customer.address.customer_address
        outputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip
        inputs: cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.id, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.text_id, cr.billing_customer.address.zip
      - SelectNode source=select datasource=cr.billing_customer.customers
        outputs: cr.billing_customer.address.id, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id
        inputs: cr.billing_customer.address.id, cr.billing_customer.birth_country, cr.billing_customer.birth_day, cr.billing_customer.birth_month, cr.billing_customer.birth_year, cr.billing_customer.demographics.id, cr.billing_customer.email_address, cr.billing_customer.first_name, cr.billing_customer.first_sales_date.id, cr.billing_customer.first_shipto_date.id, cr.billing_customer.household_demographic.id, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.last_review_date, cr.billing_customer.login, cr.billing_customer.preferred_cust_flag, cr.billing_customer.salutation, cr.billing_customer.text_id
      - SelectNode source=select datasource=cr.catalog_returns
        outputs: cr.billing_customer.id, cr.date.id, cr.item.id, cr.order_number, cr.return_address.id, cr.return_amt_inc_tax
        inputs: cr.billing_customer.id, cr.call_center.id, cr.date.id, cr.item.id, cr.net_loss, cr.order_number, cr.reason.id, cr.refunded_address.id, cr.refunded_cash, cr.refunded_customer.id, cr.return_address.id, cr.return_amount, cr.return_amt_inc_tax, cr.return_quantity, cr.reversed_charge, cr.sales.order_number, cr.store_credit, cr.time.id
      - SelectNode source=select datasource=cr.date.date
        outputs: cr.date.id, cr.date.year
        inputs: cr.date._date_string, cr.date.date, cr.date.day_name, cr.date.day_of_month, cr.date.day_of_week, cr.date.id, cr.date.month_of_year, cr.date.month_seq, cr.date.quarter, cr.date.quarter_name, cr.date.text_id, cr.date.week_seq, cr.date.year
      - SelectNode source=select datasource=cr.return_address.customer_address
        outputs: cr.return_address.id, cr.return_address.state
        inputs: cr.return_address.city, cr.return_address.country, cr.return_address.county, cr.return_address.gmt_offset, cr.return_address.id, cr.return_address.location_type, cr.return_address.state, cr.return_address.street_name, cr.return_address.street_number, cr.return_address.street_type, cr.return_address.suite_number, cr.return_address.text_id, cr.return_address.zip

## Records

```json
[
  {
    "conditions": null,
    "datasource": "cr.billing_customer.address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.billing_customer.address.id>",
    "hidden": [],
    "id": "n0",
    "inputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.text_id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type"
    ],
    "nullable": [
      "cr.billing_customer.address.state"
    ],
    "outputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.billing_customer.customers",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.billing_customer.id>",
    "hidden": [],
    "id": "n1",
    "inputs": [
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.first_name",
      "cr.billing_customer.address.id",
      "cr.billing_customer.demographics.id",
      "cr.billing_customer.household_demographic.id",
      "cr.billing_customer.first_sales_date.id",
      "cr.billing_customer.first_shipto_date.id",
      "cr.billing_customer.preferred_cust_flag",
      "cr.billing_customer.birth_country",
      "cr.billing_customer.salutation",
      "cr.billing_customer.email_address",
      "cr.billing_customer.birth_day",
      "cr.billing_customer.birth_month",
      "cr.billing_customer.birth_year",
      "cr.billing_customer.login",
      "cr.billing_customer.last_review_date"
    ],
    "nullable": [],
    "outputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.id"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.id"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.catalog_returns",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.item.id,cr.order_number>",
    "hidden": [],
    "id": "n2",
    "inputs": [
      "cr.date.id",
      "cr.time.id",
      "cr.item.id",
      "cr.sales.order_number",
      "cr.order_number",
      "cr.billing_customer.id",
      "cr.refunded_customer.id",
      "cr.return_address.id",
      "cr.refunded_address.id",
      "cr.call_center.id",
      "cr.reason.id",
      "cr.net_loss",
      "cr.refunded_cash",
      "cr.reversed_charge",
      "cr.store_credit",
      "cr.return_amount",
      "cr.return_amt_inc_tax",
      "cr.return_quantity"
    ],
    "nullable": [
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.date.id>",
    "hidden": [],
    "id": "n3",
    "inputs": [
      "cr.date.id",
      "cr.date.text_id",
      "cr.date._date_string",
      "cr.date.date",
      "cr.date.day_of_week",
      "cr.date.day_of_month",
      "cr.date.day_name",
      "cr.date.week_seq",
      "cr.date.month_of_year",
      "cr.date.month_seq",
      "cr.date.quarter",
      "cr.date.quarter_name",
      "cr.date.year"
    ],
    "nullable": [],
    "outputs": [
      "cr.date.id",
      "cr.date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.date.id",
      "cr.date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.return_address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.return_address.id>",
    "hidden": [],
    "id": "n4",
    "inputs": [
      "cr.return_address.id",
      "cr.return_address.text_id",
      "cr.return_address.street_number",
      "cr.return_address.street_name",
      "cr.return_address.street_type",
      "cr.return_address.suite_number",
      "cr.return_address.city",
      "cr.return_address.state",
      "cr.return_address.zip",
      "cr.return_address.county",
      "cr.return_address.country",
      "cr.return_address.gmt_offset",
      "cr.return_address.location_type"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.return_address.id",
      "cr.return_address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.return_address.id",
      "cr.return_address.state"
    ]
  },
  {
    "conditions": "cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n5",
    "inputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax",
      "cr.date.year",
      "cr.return_address.state"
    ],
    "nullable": [
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n0",
      "n1",
      "n2",
      "n3",
      "n4"
    ],
    "partials": [],
    "preexisting_conditions": "cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.catalog_returns",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.item.id,cr.order_number>",
    "hidden": [],
    "id": "n6",
    "inputs": [
      "cr.date.id",
      "cr.time.id",
      "cr.item.id",
      "cr.sales.order_number",
      "cr.order_number",
      "cr.billing_customer.id",
      "cr.refunded_customer.id",
      "cr.return_address.id",
      "cr.refunded_address.id",
      "cr.call_center.id",
      "cr.reason.id",
      "cr.net_loss",
      "cr.refunded_cash",
      "cr.reversed_charge",
      "cr.store_credit",
      "cr.return_amount",
      "cr.return_amt_inc_tax",
      "cr.return_quantity"
    ],
    "nullable": [
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.date.id>",
    "hidden": [],
    "id": "n7",
    "inputs": [
      "cr.date.id",
      "cr.date.text_id",
      "cr.date._date_string",
      "cr.date.date",
      "cr.date.day_of_week",
      "cr.date.day_of_month",
      "cr.date.day_name",
      "cr.date.week_seq",
      "cr.date.month_of_year",
      "cr.date.month_seq",
      "cr.date.quarter",
      "cr.date.quarter_name",
      "cr.date.year"
    ],
    "nullable": [],
    "outputs": [
      "cr.date.id",
      "cr.date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.date.id",
      "cr.date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.return_address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.return_address.id>",
    "hidden": [],
    "id": "n8",
    "inputs": [
      "cr.return_address.id",
      "cr.return_address.text_id",
      "cr.return_address.street_number",
      "cr.return_address.street_name",
      "cr.return_address.street_type",
      "cr.return_address.suite_number",
      "cr.return_address.city",
      "cr.return_address.state",
      "cr.return_address.zip",
      "cr.return_address.county",
      "cr.return_address.country",
      "cr.return_address.gmt_offset",
      "cr.return_address.location_type"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.return_address.id",
      "cr.return_address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.return_address.id",
      "cr.return_address.state"
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
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax",
      "cr.date.year",
      "cr.return_address.state"
    ],
    "nullable": [
      "cr.return_address.state",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n6",
      "n7",
      "n8"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": "cr.return_address.state is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n10",
    "inputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ],
    "nullable": [
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n9"
    ],
    "partials": [],
    "preexisting_conditions": "cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "filter",
    "type": "FilterNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n11",
    "inputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax",
      "cr.date.year",
      "cr.return_address.state"
    ],
    "nullable": [
      "cr.return_address.state",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n6",
      "n7",
      "n8"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n12",
    "inputs": [
      "cr.billing_customer.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "nullable": [
      "cr.return_address.state",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n10",
      "n11"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n13",
    "inputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ],
    "parents": [
      "n12"
    ],
    "partials": [],
    "preexisting_conditions": "cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n14",
    "inputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ],
    "parents": [
      "n12"
    ],
    "partials": [],
    "preexisting_conditions": "cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n15",
    "inputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ],
    "parents": [
      "n14"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n16",
    "inputs": [
      "cr.return_address.state",
      "local.customer_state"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.return_address.state",
      "local._virt_agg_avg_7052944147524274"
    ],
    "parents": [
      "n15"
    ],
    "partials": [],
    "preexisting_conditions": "cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "cr.return_address.state",
      "local._virt_agg_avg_7052944147524274"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n17",
    "inputs": [
      "cr.return_address.state",
      "local._virt_agg_avg_7052944147524274"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.return_address.state",
      "local.scaled_state"
    ],
    "parents": [
      "n16"
    ],
    "partials": [],
    "preexisting_conditions": "cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.return_address.state",
      "local.scaled_state"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n18",
    "inputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax",
      "local.customer_state",
      "local.scaled_state"
    ],
    "nullable": [
      "cr.return_address.state",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax",
      "local.customer_state",
      "local.scaled_state"
    ],
    "parents": [
      "n5",
      "n13",
      "n17"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax",
      "local.customer_state",
      "local.scaled_state"
    ]
  },
  {
    "conditions": "local.customer_state > local.scaled_state",
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n19",
    "inputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax",
      "local.customer_state",
      "local.scaled_state"
    ],
    "nullable": [
      "cr.return_address.state",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax",
      "local.customer_state",
      "local.scaled_state"
    ],
    "parents": [
      "n18"
    ],
    "partials": [],
    "preexisting_conditions": "local.customer_state > local.scaled_state",
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.state",
      "cr.billing_customer.id",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax",
      "local.customer_state",
      "local.scaled_state"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n20",
    "inputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state",
      "cr.date.year",
      "cr.return_amt_inc_tax"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ],
    "parents": [
      "n19"
    ],
    "partials": [],
    "preexisting_conditions": "cr.return_address.state is not MagicConstants.NULL and cr.billing_customer.address.state = GA",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.return_address.state",
      "local.customer_state"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.billing_customer.address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.billing_customer.address.id>",
    "hidden": [],
    "id": "n21",
    "inputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.text_id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type"
    ],
    "nullable": [
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number"
    ],
    "outputs": [
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.zip"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.zip"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.billing_customer.customers",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.billing_customer.id>",
    "hidden": [],
    "id": "n22",
    "inputs": [
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.first_name",
      "cr.billing_customer.address.id",
      "cr.billing_customer.demographics.id",
      "cr.billing_customer.household_demographic.id",
      "cr.billing_customer.first_sales_date.id",
      "cr.billing_customer.first_shipto_date.id",
      "cr.billing_customer.preferred_cust_flag",
      "cr.billing_customer.birth_country",
      "cr.billing_customer.salutation",
      "cr.billing_customer.email_address",
      "cr.billing_customer.birth_day",
      "cr.billing_customer.birth_month",
      "cr.billing_customer.birth_year",
      "cr.billing_customer.login",
      "cr.billing_customer.last_review_date"
    ],
    "nullable": [],
    "outputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.first_name",
      "cr.billing_customer.id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.salutation",
      "cr.billing_customer.text_id"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.address.id",
      "cr.billing_customer.first_name",
      "cr.billing_customer.id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.salutation",
      "cr.billing_customer.text_id"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.catalog_returns",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.item.id,cr.order_number>",
    "hidden": [],
    "id": "n23",
    "inputs": [
      "cr.date.id",
      "cr.time.id",
      "cr.item.id",
      "cr.sales.order_number",
      "cr.order_number",
      "cr.billing_customer.id",
      "cr.refunded_customer.id",
      "cr.return_address.id",
      "cr.refunded_address.id",
      "cr.call_center.id",
      "cr.reason.id",
      "cr.net_loss",
      "cr.refunded_cash",
      "cr.reversed_charge",
      "cr.store_credit",
      "cr.return_amount",
      "cr.return_amt_inc_tax",
      "cr.return_quantity"
    ],
    "nullable": [
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.date.id>",
    "hidden": [],
    "id": "n24",
    "inputs": [
      "cr.date.id",
      "cr.date.text_id",
      "cr.date._date_string",
      "cr.date.date",
      "cr.date.day_of_week",
      "cr.date.day_of_month",
      "cr.date.day_name",
      "cr.date.week_seq",
      "cr.date.month_of_year",
      "cr.date.month_seq",
      "cr.date.quarter",
      "cr.date.quarter_name",
      "cr.date.year"
    ],
    "nullable": [],
    "outputs": [
      "cr.date.id",
      "cr.date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.date.id",
      "cr.date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "cr.return_address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<cr.return_address.id>",
    "hidden": [],
    "id": "n25",
    "inputs": [
      "cr.return_address.id",
      "cr.return_address.text_id",
      "cr.return_address.street_number",
      "cr.return_address.street_name",
      "cr.return_address.street_type",
      "cr.return_address.suite_number",
      "cr.return_address.city",
      "cr.return_address.state",
      "cr.return_address.zip",
      "cr.return_address.county",
      "cr.return_address.country",
      "cr.return_address.gmt_offset",
      "cr.return_address.location_type"
    ],
    "nullable": [
      "cr.return_address.state"
    ],
    "outputs": [
      "cr.return_address.id",
      "cr.return_address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "cr.return_address.id",
      "cr.return_address.state"
    ]
  },
  {
    "conditions": "cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n26",
    "inputs": [
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.first_name",
      "cr.billing_customer.id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.salutation",
      "cr.billing_customer.text_id",
      "cr.date.id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.id",
      "cr.return_amt_inc_tax",
      "cr.date.year",
      "cr.return_address.state"
    ],
    "nullable": [
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.location_type",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.first_name",
      "cr.billing_customer.salutation",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n21",
      "n22",
      "n23",
      "n24",
      "n25"
    ],
    "partials": [],
    "preexisting_conditions": "cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.first_name",
      "cr.billing_customer.salutation",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n27",
    "inputs": [
      "cr.item.id",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.first_name",
      "cr.billing_customer.salutation",
      "cr.return_address.id",
      "cr.return_address.state",
      "cr.order_number",
      "cr.return_amt_inc_tax"
    ],
    "nullable": [
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.location_type",
      "cr.return_amt_inc_tax"
    ],
    "outputs": [
      "cr.billing_customer.first_name",
      "cr.billing_customer.id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.salutation",
      "cr.billing_customer.text_id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ],
    "parents": [
      "n26"
    ],
    "partials": [],
    "preexisting_conditions": "cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "filter",
    "type": "FilterNode",
    "usable_outputs": [
      "cr.billing_customer.first_name",
      "cr.billing_customer.id",
      "cr.billing_customer.last_name",
      "cr.billing_customer.salutation",
      "cr.billing_customer.text_id",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.date.id",
      "cr.date.year",
      "cr.billing_customer.address.id",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "cr.return_address.id",
      "cr.return_amt_inc_tax"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": "Grain<cr.billing_customer.id,cr.return_address.state>",
    "hidden": [],
    "id": "n28",
    "inputs": [
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.salutation",
      "cr.billing_customer.first_name",
      "cr.billing_customer.last_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "local.customer_state"
    ],
    "nullable": [
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.location_type"
    ],
    "outputs": [
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.salutation",
      "cr.billing_customer.first_name",
      "cr.billing_customer.last_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "local.customer_state"
    ],
    "parents": [
      "n20",
      "n27"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "cr.billing_customer.id",
      "cr.billing_customer.text_id",
      "cr.billing_customer.salutation",
      "cr.billing_customer.first_name",
      "cr.billing_customer.last_name",
      "cr.billing_customer.address.street_number",
      "cr.billing_customer.address.street_name",
      "cr.billing_customer.address.street_type",
      "cr.billing_customer.address.suite_number",
      "cr.billing_customer.address.city",
      "cr.billing_customer.address.county",
      "cr.billing_customer.address.state",
      "cr.billing_customer.address.zip",
      "cr.billing_customer.address.country",
      "cr.billing_customer.address.gmt_offset",
      "cr.billing_customer.address.location_type",
      "local.customer_state"
    ]
  }
]
```
