# V4 Strategy Diagnostics

- strategy nodes: 30

## Tree

- MergeNode source=merge
  outputs: local.customer_state_returns_2002, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id
  inputs: local.customer_state_returns_2002, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id
  - GroupNode source=group
    outputs: local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state
    inputs: local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year
    - SelectNode source=select
      outputs: local.customer_state_returns_2002, local.scaled_state_returns_2002, web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
      inputs: local.customer_state_returns_2002, local.scaled_state_returns_2002, web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
      conditions: local.customer_state_returns_2002 > local.scaled_state_returns_2002
      - MergeNode source=merge
        outputs: local.customer_state_returns_2002, local.scaled_state_returns_2002, web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
        inputs: local.customer_state_returns_2002, local.scaled_state_returns_2002, web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
        - MergeNode source=merge
          outputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
          inputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
          conditions: web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL
          - SelectNode source=select datasource=web_returns.billing_customer.address.customer_address
            outputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state
            inputs: web_returns.billing_customer.address.city, web_returns.billing_customer.address.country, web_returns.billing_customer.address.county, web_returns.billing_customer.address.gmt_offset, web_returns.billing_customer.address.id, web_returns.billing_customer.address.location_type, web_returns.billing_customer.address.state, web_returns.billing_customer.address.street_name, web_returns.billing_customer.address.street_number, web_returns.billing_customer.address.street_type, web_returns.billing_customer.address.suite_number, web_returns.billing_customer.address.text_id, web_returns.billing_customer.address.zip
          - SelectNode source=select datasource=web_returns.billing_customer.customers
            outputs: web_returns.billing_customer.address.id, web_returns.billing_customer.id
            inputs: web_returns.billing_customer.address.id, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.demographics.id, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.first_sales_date.id, web_returns.billing_customer.first_shipto_date.id, web_returns.billing_customer.household_demographic.id, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id
          - SelectNode source=select datasource=web_returns.return_address.customer_address
            outputs: web_returns.return_address.id, web_returns.return_address.state
            inputs: web_returns.return_address.city, web_returns.return_address.country, web_returns.return_address.county, web_returns.return_address.gmt_offset, web_returns.return_address.id, web_returns.return_address.location_type, web_returns.return_address.state, web_returns.return_address.street_name, web_returns.return_address.street_number, web_returns.return_address.street_type, web_returns.return_address.suite_number, web_returns.return_address.text_id, web_returns.return_address.zip
          - SelectNode source=select datasource=web_returns.return_date.date
            outputs: web_returns.return_date.id, web_returns.return_date.year
            inputs: web_returns.return_date._date_string, web_returns.return_date.date, web_returns.return_date.day_name, web_returns.return_date.day_of_month, web_returns.return_date.day_of_week, web_returns.return_date.id, web_returns.return_date.month_of_year, web_returns.return_date.month_seq, web_returns.return_date.quarter, web_returns.return_date.quarter_name, web_returns.return_date.text_id, web_returns.return_date.week_seq, web_returns.return_date.year
          - SelectNode source=select datasource=web_returns.web_returns
            outputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_amount, web_returns.return_date.id, web_returns.web_sales.item.id, web_returns.web_sales.order_number
            inputs: web_returns.billing_customer.id, web_returns.fee, web_returns.net_loss, web_returns.reason.id, web_returns.refunded_address.id, web_returns.refunded_cash, web_returns.refunded_customer.id, web_returns.refunded_demographic.id, web_returns.return_address.id, web_returns.return_amount, web_returns.return_date.id, web_returns.return_quantity, web_returns.returning_demographic.id, web_returns.store.id, web_returns.time.id, web_returns.web_page.id, web_returns.web_sales.item.id, web_returns.web_sales.order_number
        - GroupNode source=group
          outputs: local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state
          inputs: local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.state
          - MergeNode source=merge
            outputs: local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
            inputs: local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
            - FilterNode source=filter
              outputs: local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
              inputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
              conditions: web_returns.return_address.state is not MagicConstants.NULL
              - MergeNode source=merge
                outputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
                inputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
                - SelectNode source=select datasource=web_returns.return_address.customer_address
                  outputs: web_returns.return_address.id, web_returns.return_address.state
                  inputs: web_returns.return_address.city, web_returns.return_address.country, web_returns.return_address.county, web_returns.return_address.gmt_offset, web_returns.return_address.id, web_returns.return_address.location_type, web_returns.return_address.state, web_returns.return_address.street_name, web_returns.return_address.street_number, web_returns.return_address.street_type, web_returns.return_address.suite_number, web_returns.return_address.text_id, web_returns.return_address.zip
                - SelectNode source=select datasource=web_returns.return_date.date
                  outputs: web_returns.return_date.id, web_returns.return_date.year
                  inputs: web_returns.return_date._date_string, web_returns.return_date.date, web_returns.return_date.day_name, web_returns.return_date.day_of_month, web_returns.return_date.day_of_week, web_returns.return_date.id, web_returns.return_date.month_of_year, web_returns.return_date.month_seq, web_returns.return_date.quarter, web_returns.return_date.quarter_name, web_returns.return_date.text_id, web_returns.return_date.week_seq, web_returns.return_date.year
                - SelectNode source=select datasource=web_returns.web_returns
                  outputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_amount, web_returns.return_date.id, web_returns.web_sales.item.id, web_returns.web_sales.order_number
                  inputs: web_returns.billing_customer.id, web_returns.fee, web_returns.net_loss, web_returns.reason.id, web_returns.refunded_address.id, web_returns.refunded_cash, web_returns.refunded_customer.id, web_returns.refunded_demographic.id, web_returns.return_address.id, web_returns.return_amount, web_returns.return_date.id, web_returns.return_quantity, web_returns.returning_demographic.id, web_returns.store.id, web_returns.time.id, web_returns.web_page.id, web_returns.web_sales.item.id, web_returns.web_sales.order_number
            - MergeNode source=merge
              outputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
              inputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
              - SelectNode source=select datasource=web_returns.return_address.customer_address (reused)
              - SelectNode source=select datasource=web_returns.return_date.date (reused)
              - SelectNode source=select datasource=web_returns.web_returns (reused)
        - SelectNode source=select
          outputs: local.scaled_state_returns_2002, web_returns.return_address.state
          inputs: local._virt_agg_avg_3885168128306444, web_returns.return_address.state
          - GroupNode source=group
            outputs: local._virt_agg_avg_3885168128306444, web_returns.return_address.state
            inputs: local.customer_state_returns_2002, web_returns.return_address.state
            - GroupNode source=group
              outputs: local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state
              inputs: local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state
              - GroupNode source=group
                outputs: local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state
                inputs: local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.state
                - MergeNode source=merge (reused)
  - GroupNode source=group
    outputs: web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.state
    inputs: web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.state
    - FilterNode source=filter
      outputs: local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
      inputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
      - MergeNode source=merge
        outputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
        inputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.id, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number
        conditions: web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL
        - SelectNode source=select datasource=web_returns.billing_customer.address.customer_address
          outputs: web_returns.billing_customer.address.id, web_returns.billing_customer.address.state
          inputs: web_returns.billing_customer.address.city, web_returns.billing_customer.address.country, web_returns.billing_customer.address.county, web_returns.billing_customer.address.gmt_offset, web_returns.billing_customer.address.id, web_returns.billing_customer.address.location_type, web_returns.billing_customer.address.state, web_returns.billing_customer.address.street_name, web_returns.billing_customer.address.street_number, web_returns.billing_customer.address.street_type, web_returns.billing_customer.address.suite_number, web_returns.billing_customer.address.text_id, web_returns.billing_customer.address.zip
        - SelectNode source=select datasource=web_returns.billing_customer.customers
          outputs: web_returns.billing_customer.address.id, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id
          inputs: web_returns.billing_customer.address.id, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.demographics.id, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.first_sales_date.id, web_returns.billing_customer.first_shipto_date.id, web_returns.billing_customer.household_demographic.id, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id
        - SelectNode source=select datasource=web_returns.return_address.customer_address
          outputs: web_returns.return_address.id, web_returns.return_address.state
          inputs: web_returns.return_address.city, web_returns.return_address.country, web_returns.return_address.county, web_returns.return_address.gmt_offset, web_returns.return_address.id, web_returns.return_address.location_type, web_returns.return_address.state, web_returns.return_address.street_name, web_returns.return_address.street_number, web_returns.return_address.street_type, web_returns.return_address.suite_number, web_returns.return_address.text_id, web_returns.return_address.zip
        - SelectNode source=select datasource=web_returns.return_date.date
          outputs: web_returns.return_date.id, web_returns.return_date.year
          inputs: web_returns.return_date._date_string, web_returns.return_date.date, web_returns.return_date.day_name, web_returns.return_date.day_of_month, web_returns.return_date.day_of_week, web_returns.return_date.id, web_returns.return_date.month_of_year, web_returns.return_date.month_seq, web_returns.return_date.quarter, web_returns.return_date.quarter_name, web_returns.return_date.text_id, web_returns.return_date.week_seq, web_returns.return_date.year
        - SelectNode source=select datasource=web_returns.web_returns
          outputs: web_returns.billing_customer.id, web_returns.return_address.id, web_returns.return_amount, web_returns.return_date.id, web_returns.web_sales.item.id, web_returns.web_sales.order_number
          inputs: web_returns.billing_customer.id, web_returns.fee, web_returns.net_loss, web_returns.reason.id, web_returns.refunded_address.id, web_returns.refunded_cash, web_returns.refunded_customer.id, web_returns.refunded_demographic.id, web_returns.return_address.id, web_returns.return_amount, web_returns.return_date.id, web_returns.return_quantity, web_returns.returning_demographic.id, web_returns.store.id, web_returns.time.id, web_returns.web_page.id, web_returns.web_sales.item.id, web_returns.web_sales.order_number

## Records

```json
[
  {
    "conditions": null,
    "datasource": "web_returns.billing_customer.address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.billing_customer.address.id>",
    "hidden": [],
    "id": "n0",
    "inputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.text_id",
      "web_returns.billing_customer.address.street_number",
      "web_returns.billing_customer.address.street_name",
      "web_returns.billing_customer.address.street_type",
      "web_returns.billing_customer.address.suite_number",
      "web_returns.billing_customer.address.city",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.address.zip",
      "web_returns.billing_customer.address.county",
      "web_returns.billing_customer.address.country",
      "web_returns.billing_customer.address.gmt_offset",
      "web_returns.billing_customer.address.location_type"
    ],
    "nullable": [
      "web_returns.billing_customer.address.state"
    ],
    "outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.billing_customer.customers",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.billing_customer.id>",
    "hidden": [],
    "id": "n1",
    "inputs": [
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.demographics.id",
      "web_returns.billing_customer.household_demographic.id",
      "web_returns.billing_customer.first_sales_date.id",
      "web_returns.billing_customer.first_shipto_date.id",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.last_review_date"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.id"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.id"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.return_address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.return_address.id>",
    "hidden": [],
    "id": "n2",
    "inputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.text_id",
      "web_returns.return_address.street_number",
      "web_returns.return_address.street_name",
      "web_returns.return_address.street_type",
      "web_returns.return_address.suite_number",
      "web_returns.return_address.city",
      "web_returns.return_address.state",
      "web_returns.return_address.zip",
      "web_returns.return_address.county",
      "web_returns.return_address.country",
      "web_returns.return_address.gmt_offset",
      "web_returns.return_address.location_type"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.return_date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.return_date.id>",
    "hidden": [],
    "id": "n3",
    "inputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.text_id",
      "web_returns.return_date._date_string",
      "web_returns.return_date.date",
      "web_returns.return_date.day_of_week",
      "web_returns.return_date.day_of_month",
      "web_returns.return_date.day_name",
      "web_returns.return_date.week_seq",
      "web_returns.return_date.month_of_year",
      "web_returns.return_date.month_seq",
      "web_returns.return_date.quarter",
      "web_returns.return_date.quarter_name",
      "web_returns.return_date.year"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.web_returns",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.web_sales.item.id,web_returns.web_sales.order_number>",
    "hidden": [],
    "id": "n4",
    "inputs": [
      "web_returns.return_date.id",
      "web_returns.time.id",
      "web_returns.web_sales.item.id",
      "web_returns.billing_customer.id",
      "web_returns.refunded_customer.id",
      "web_returns.returning_demographic.id",
      "web_returns.refunded_demographic.id",
      "web_returns.return_address.id",
      "web_returns.refunded_address.id",
      "web_returns.reason.id",
      "web_returns.web_page.id",
      "web_returns.return_amount",
      "web_returns.return_quantity",
      "web_returns.refunded_cash",
      "web_returns.fee",
      "web_returns.net_loss",
      "web_returns.web_sales.order_number",
      "web_returns.store.id"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_amount",
      "web_returns.return_date.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_amount",
      "web_returns.return_date.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ]
  },
  {
    "conditions": "web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n5",
    "inputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_amount",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
    ],
    "parents": [
      "n0",
      "n1",
      "n2",
      "n3",
      "n4"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.return_address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.return_address.id>",
    "hidden": [],
    "id": "n6",
    "inputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.text_id",
      "web_returns.return_address.street_number",
      "web_returns.return_address.street_name",
      "web_returns.return_address.street_type",
      "web_returns.return_address.suite_number",
      "web_returns.return_address.city",
      "web_returns.return_address.state",
      "web_returns.return_address.zip",
      "web_returns.return_address.county",
      "web_returns.return_address.country",
      "web_returns.return_address.gmt_offset",
      "web_returns.return_address.location_type"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.return_date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.return_date.id>",
    "hidden": [],
    "id": "n7",
    "inputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.text_id",
      "web_returns.return_date._date_string",
      "web_returns.return_date.date",
      "web_returns.return_date.day_of_week",
      "web_returns.return_date.day_of_month",
      "web_returns.return_date.day_name",
      "web_returns.return_date.week_seq",
      "web_returns.return_date.month_of_year",
      "web_returns.return_date.month_seq",
      "web_returns.return_date.quarter",
      "web_returns.return_date.quarter_name",
      "web_returns.return_date.year"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.web_returns",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.web_sales.item.id,web_returns.web_sales.order_number>",
    "hidden": [],
    "id": "n8",
    "inputs": [
      "web_returns.return_date.id",
      "web_returns.time.id",
      "web_returns.web_sales.item.id",
      "web_returns.billing_customer.id",
      "web_returns.refunded_customer.id",
      "web_returns.returning_demographic.id",
      "web_returns.refunded_demographic.id",
      "web_returns.return_address.id",
      "web_returns.refunded_address.id",
      "web_returns.reason.id",
      "web_returns.web_page.id",
      "web_returns.return_amount",
      "web_returns.return_quantity",
      "web_returns.refunded_cash",
      "web_returns.fee",
      "web_returns.net_loss",
      "web_returns.web_sales.order_number",
      "web_returns.store.id"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_amount",
      "web_returns.return_date.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_amount",
      "web_returns.return_date.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
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
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_amount",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
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
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
    ]
  },
  {
    "conditions": "web_returns.return_address.state is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n10",
    "inputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
    ],
    "nullable": [],
    "outputs": [
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_address.id",
      "web_returns.return_amount"
    ],
    "parents": [
      "n9"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "filter",
    "type": "FilterNode",
    "usable_outputs": [
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_address.id",
      "web_returns.return_amount"
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
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_amount",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
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
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
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
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_address.id",
      "web_returns.return_amount"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_address.id",
      "web_returns.return_amount"
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
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_address.id",
      "web_returns.return_amount"
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
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "parents": [
      "n12"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
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
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "parents": [
      "n12"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
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
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
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
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
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
      "local.customer_state_returns_2002",
      "web_returns.return_address.state"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local._virt_agg_avg_3885168128306444",
      "web_returns.return_address.state"
    ],
    "parents": [
      "n15"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local._virt_agg_avg_3885168128306444",
      "web_returns.return_address.state"
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
      "local._virt_agg_avg_3885168128306444",
      "web_returns.return_address.state"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local.scaled_state_returns_2002",
      "web_returns.return_address.state"
    ],
    "parents": [
      "n16"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "local.scaled_state_returns_2002",
      "web_returns.return_address.state"
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
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount",
      "local.customer_state_returns_2002",
      "local.scaled_state_returns_2002"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount",
      "local.customer_state_returns_2002",
      "local.scaled_state_returns_2002"
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
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount",
      "local.customer_state_returns_2002",
      "local.scaled_state_returns_2002"
    ]
  },
  {
    "conditions": "local.customer_state_returns_2002 > local.scaled_state_returns_2002",
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n19",
    "inputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount",
      "local.customer_state_returns_2002",
      "local.scaled_state_returns_2002"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount",
      "local.customer_state_returns_2002",
      "local.scaled_state_returns_2002"
    ],
    "parents": [
      "n18"
    ],
    "partials": [],
    "preexisting_conditions": "local.customer_state_returns_2002 > local.scaled_state_returns_2002",
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount",
      "local.customer_state_returns_2002",
      "local.scaled_state_returns_2002"
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
      "web_returns.billing_customer.id",
      "web_returns.return_address.state",
      "local.customer_state_returns_2002",
      "web_returns.return_date.year",
      "web_returns.return_amount"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "parents": [
      "n19"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.billing_customer.address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.billing_customer.address.id>",
    "hidden": [],
    "id": "n21",
    "inputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.text_id",
      "web_returns.billing_customer.address.street_number",
      "web_returns.billing_customer.address.street_name",
      "web_returns.billing_customer.address.street_type",
      "web_returns.billing_customer.address.suite_number",
      "web_returns.billing_customer.address.city",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.address.zip",
      "web_returns.billing_customer.address.county",
      "web_returns.billing_customer.address.country",
      "web_returns.billing_customer.address.gmt_offset",
      "web_returns.billing_customer.address.location_type"
    ],
    "nullable": [
      "web_returns.billing_customer.address.state"
    ],
    "outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.billing_customer.customers",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.billing_customer.id>",
    "hidden": [],
    "id": "n22",
    "inputs": [
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.demographics.id",
      "web_returns.billing_customer.household_demographic.id",
      "web_returns.billing_customer.first_sales_date.id",
      "web_returns.billing_customer.first_shipto_date.id",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.last_review_date"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.return_address.customer_address",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.return_address.id>",
    "hidden": [],
    "id": "n23",
    "inputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.text_id",
      "web_returns.return_address.street_number",
      "web_returns.return_address.street_name",
      "web_returns.return_address.street_type",
      "web_returns.return_address.suite_number",
      "web_returns.return_address.city",
      "web_returns.return_address.state",
      "web_returns.return_address.zip",
      "web_returns.return_address.county",
      "web_returns.return_address.country",
      "web_returns.return_address.gmt_offset",
      "web_returns.return_address.location_type"
    ],
    "nullable": [
      "web_returns.return_address.state"
    ],
    "outputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.state"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.return_address.id",
      "web_returns.return_address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.return_date.date",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.return_date.id>",
    "hidden": [],
    "id": "n24",
    "inputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.text_id",
      "web_returns.return_date._date_string",
      "web_returns.return_date.date",
      "web_returns.return_date.day_of_week",
      "web_returns.return_date.day_of_month",
      "web_returns.return_date.day_name",
      "web_returns.return_date.week_seq",
      "web_returns.return_date.month_of_year",
      "web_returns.return_date.month_seq",
      "web_returns.return_date.quarter",
      "web_returns.return_date.quarter_name",
      "web_returns.return_date.year"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.year"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.return_date.id",
      "web_returns.return_date.year"
    ]
  },
  {
    "conditions": null,
    "datasource": "web_returns.web_returns",
    "existence": [],
    "force_group": false,
    "grain": "Grain<web_returns.web_sales.item.id,web_returns.web_sales.order_number>",
    "hidden": [],
    "id": "n25",
    "inputs": [
      "web_returns.return_date.id",
      "web_returns.time.id",
      "web_returns.web_sales.item.id",
      "web_returns.billing_customer.id",
      "web_returns.refunded_customer.id",
      "web_returns.returning_demographic.id",
      "web_returns.refunded_demographic.id",
      "web_returns.return_address.id",
      "web_returns.refunded_address.id",
      "web_returns.reason.id",
      "web_returns.web_page.id",
      "web_returns.return_amount",
      "web_returns.return_quantity",
      "web_returns.refunded_cash",
      "web_returns.fee",
      "web_returns.net_loss",
      "web_returns.web_sales.order_number",
      "web_returns.store.id"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_amount",
      "web_returns.return_date.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.id",
      "web_returns.return_amount",
      "web_returns.return_date.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ]
  },
  {
    "conditions": "web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n26",
    "inputs": [
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.return_amount",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.last_review_date",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
    ],
    "parents": [
      "n21",
      "n22",
      "n23",
      "n24",
      "n25"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.last_review_date",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
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
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.last_review_date",
      "web_returns.return_address.id",
      "web_returns.return_address.state",
      "web_returns.return_amount"
    ],
    "nullable": [],
    "outputs": [
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.return_address.id",
      "web_returns.return_amount"
    ],
    "parents": [
      "n26"
    ],
    "partials": [],
    "preexisting_conditions": "web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL",
    "rollups": [],
    "source_type": "filter",
    "type": "FilterNode",
    "usable_outputs": [
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.state",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_date.id",
      "web_returns.return_date.year",
      "web_returns.billing_customer.address.id",
      "web_returns.billing_customer.address.state",
      "web_returns.return_address.id",
      "web_returns.return_amount"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n28",
    "inputs": [
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.state"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.state"
    ],
    "parents": [
      "n27"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.state"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": "Grain<web_returns.billing_customer.id,web_returns.return_address.state>",
    "hidden": [],
    "id": "n29",
    "inputs": [
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.last_review_date",
      "local.customer_state_returns_2002"
    ],
    "nullable": [],
    "outputs": [
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.last_review_date",
      "local.customer_state_returns_2002"
    ],
    "parents": [
      "n20",
      "n28"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.text_id",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.first_name",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month",
      "web_returns.billing_customer.birth_year",
      "web_returns.billing_customer.birth_country",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.last_review_date",
      "local.customer_state_returns_2002"
    ]
  }
]
```
