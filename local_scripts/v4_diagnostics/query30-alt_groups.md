# V4 Group Diagnostics

- groups: 9
- edges: 19

## __final__

- derivation: `final`
- depth: `final`
- grain: `-`
- aggregate input grain: `-`
- primary members: `-`
- secondary members: `-`
- outputs: `-`
- inputs: `-`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:merge, grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state:merge, grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e:merge, grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec:merge, grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:merge, grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec:merge, grp:root:root:∅:merge, grp:root:root_d1:∅:merge`
- successors: `-`
- conditions: `local.customer_state_returns_2002 > local.scaled_state_returns_2002 and web_returns.billing_customer.address.state = GA and web_returns.return_address.state is not MagicConstants.NULL`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
      "output_addresses": [
        "local.customer_state_returns_2002",
        "web_returns.billing_customer.id",
        "web_returns.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "web_returns.billing_customer.id",
        "web_returns.return_address.state"
      ]
    },
    {
      "group_id": "grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state",
      "output_addresses": [
        "local._virt_agg_avg_3885168128306444",
        "web_returns.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "web_returns.return_address.state"
      ]
    },
    {
      "group_id": "grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e",
      "output_addresses": [
        "local.scaled_state_returns_2002",
        "web_returns.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec",
      "output_addresses": [
        "web_returns.billing_customer.id",
        "local._virt_filter_return_amount_7190501181391118",
        "web_returns.web_sales.item.id",
        "web_returns.web_sales.order_number",
        "web_returns.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
      "output_addresses": [
        "local.customer_state_returns_2002",
        "web_returns.billing_customer.id",
        "web_returns.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "web_returns.billing_customer.id",
        "web_returns.return_address.state"
      ]
    },
    {
      "group_id": "grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec",
      "output_addresses": [
        "web_returns.billing_customer.preferred_cust_flag",
        "web_returns.billing_customer.id",
        "web_returns.billing_customer.login",
        "local._virt_filter_return_amount_7190501181391118",
        "web_returns.billing_customer.last_review_date",
        "web_returns.billing_customer.first_name",
        "web_returns.web_sales.item.id",
        "web_returns.billing_customer.email_address",
        "web_returns.billing_customer.last_name",
        "web_returns.billing_customer.birth_country",
        "web_returns.web_sales.order_number",
        "web_returns.billing_customer.salutation",
        "web_returns.billing_customer.birth_year",
        "web_returns.billing_customer.text_id",
        "web_returns.return_address.state",
        "web_returns.billing_customer.birth_day",
        "web_returns.billing_customer.birth_month"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "web_returns.billing_customer.preferred_cust_flag",
        "web_returns.billing_customer.id",
        "web_returns.billing_customer.login",
        "web_returns.billing_customer.last_review_date",
        "web_returns.billing_customer.first_name",
        "web_returns.web_sales.item.id",
        "web_returns.billing_customer.email_address",
        "web_returns.billing_customer.last_name",
        "web_returns.billing_customer.birth_country",
        "web_returns.web_sales.order_number",
        "web_returns.billing_customer.salutation",
        "web_returns.billing_customer.birth_year",
        "web_returns.return_amount",
        "web_returns.billing_customer.text_id",
        "web_returns.return_address.state",
        "web_returns.return_date.year",
        "web_returns.billing_customer.birth_day",
        "web_returns.billing_customer.birth_month"
      ],
      "preserve_keys": [
        "web_returns.billing_customer.id",
        "web_returns.return_address.state"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root_d1:\u2205",
      "output_addresses": [
        "web_returns.billing_customer.id",
        "web_returns.web_sales.item.id",
        "web_returns.web_sales.order_number",
        "web_returns.return_amount",
        "web_returns.return_address.state",
        "web_returns.return_date.year"
      ],
      "preserve_keys": [
        "web_returns.billing_customer.id",
        "web_returns.return_address.state"
      ],
      "projection_grain": []
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [
    "web_returns.billing_customer.id",
    "web_returns.return_address.state"
  ],
  "output_addresses": [
    "web_returns.billing_customer.preferred_cust_flag",
    "web_returns.billing_customer.id",
    "web_returns.billing_customer.login",
    "web_returns.billing_customer.last_review_date",
    "web_returns.billing_customer.first_name",
    "local.customer_state_returns_2002",
    "web_returns.billing_customer.last_name",
    "web_returns.billing_customer.email_address",
    "web_returns.billing_customer.birth_country",
    "web_returns.billing_customer.salutation",
    "web_returns.billing_customer.birth_year",
    "web_returns.billing_customer.text_id",
    "web_returns.billing_customer.birth_day",
    "web_returns.billing_customer.birth_month"
  ],
  "required_grain": [
    "local.customer_state_returns_2002",
    "web_returns.billing_customer.id",
    "web_returns.billing_customer.text_id"
  ]
}
```

## grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number

- derivation: `aggregate`
- depth: `d1`
- grain: `web_returns.billing_customer.id, web_returns.return_address.state`
- aggregate input grain: `web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- primary members: `local.customer_state_returns_2002`
- secondary members: `web_returns.billing_customer.id, web_returns.return_address.state`
- outputs: `local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state`
- inputs: `local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.state, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- hidden: `-`
- predecessors: `grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec:lineage, grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state:lineage, grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:constraint`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "web_returns.billing_customer.id",
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_address.state"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.billing_customer.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_address.state"
    ]
  }
]
```

## grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state

- derivation: `aggregate`
- depth: `d1`
- grain: `web_returns.return_address.state`
- aggregate input grain: `web_returns.billing_customer.id, web_returns.return_address.state`
- primary members: `local._virt_agg_avg_3885168128306444`
- secondary members: `web_returns.return_address.state`
- outputs: `local._virt_agg_avg_3885168128306444, web_returns.return_address.state`
- inputs: `local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:lineage, grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "local.customer_state_returns_2002",
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ]
  }
]
```

## grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e

- derivation: `basic`
- depth: `d1`
- grain: `web_returns.return_address.state`
- aggregate input grain: `-`
- primary members: `local.scaled_state_returns_2002`
- secondary members: `-`
- outputs: `local.scaled_state_returns_2002, web_returns.return_address.state`
- inputs: `local._virt_agg_avg_3885168128306444, web_returns.return_address.state`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state:lineage`
- successors: `__final__:merge, grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:constraint`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state",
    "preserve_keys": [
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "local._virt_agg_avg_3885168128306444",
      "web_returns.return_address.state"
    ]
  }
]
```

## grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec

- derivation: `filter`
- depth: `d1`
- grain: `web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- aggregate input grain: `-`
- primary members: `local._virt_filter_return_amount_7190501181391118`
- secondary members: `-`
- outputs: `local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.id, web_returns.return_address.state, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- inputs: `web_returns.billing_customer.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- hidden: `-`
- predecessors: `grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:lineage`
- conditions: `web_returns.return_address.state is not MagicConstants.NULL`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "required_grain": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "required_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_amount",
      "web_returns.return_address.state",
      "web_returns.return_date.year"
    ]
  }
]
```

## grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number

- derivation: `aggregate`
- depth: `d0`
- grain: `web_returns.billing_customer.id, web_returns.return_address.state`
- aggregate input grain: `web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- primary members: `local.customer_state_returns_2002`
- secondary members: `web_returns.billing_customer.id, web_returns.return_address.state`
- outputs: `local.customer_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state`
- inputs: `local._virt_filter_return_amount_7190501181391118, local.scaled_state_returns_2002, web_returns.billing_customer.id, web_returns.return_address.state, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:constraint, grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e:constraint, grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec:lineage, grp:root:root:∅:lineage`
- successors: `__final__:merge`
- conditions: `local.customer_state_returns_2002 > local.scaled_state_returns_2002`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "local.scaled_state_returns_2002",
      "web_returns.return_address.state"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "web_returns.billing_customer.id",
      "local._virt_filter_return_amount_7190501181391118",
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_address.state"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_grain": [
      "web_returns.billing_customer.id",
      "web_returns.return_address.state"
    ],
    "required_outputs": [
      "web_returns.web_sales.item.id",
      "web_returns.billing_customer.id",
      "web_returns.web_sales.order_number",
      "web_returns.return_address.state"
    ]
  }
]
```

## grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec

- derivation: `filter`
- depth: `d*`
- grain: `web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- aggregate input grain: `-`
- primary members: `local._virt_filter_return_amount_7190501181391118`
- secondary members: `-`
- outputs: `local._virt_filter_return_amount_7190501181391118, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.state, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- inputs: `web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- hidden: `-`
- predecessors: `grp:root:root:∅:lineage`
- successors: `__final__:merge, grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "required_grain": [
      "web_returns.web_sales.item.id",
      "web_returns.web_sales.order_number"
    ],
    "required_outputs": [
      "web_returns.billing_customer.preferred_cust_flag",
      "web_returns.billing_customer.id",
      "web_returns.billing_customer.login",
      "web_returns.billing_customer.last_review_date",
      "web_returns.billing_customer.first_name",
      "web_returns.web_sales.item.id",
      "web_returns.billing_customer.email_address",
      "web_returns.billing_customer.last_name",
      "web_returns.billing_customer.birth_country",
      "web_returns.web_sales.order_number",
      "web_returns.billing_customer.salutation",
      "web_returns.billing_customer.birth_year",
      "web_returns.return_amount",
      "web_returns.billing_customer.text_id",
      "web_returns.return_address.state",
      "web_returns.return_date.year",
      "web_returns.billing_customer.birth_day",
      "web_returns.billing_customer.birth_month"
    ]
  }
]
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `web_returns.billing_customer.address.state, web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- secondary members: `-`
- outputs: `web_returns.billing_customer.birth_country, web_returns.billing_customer.birth_day, web_returns.billing_customer.birth_month, web_returns.billing_customer.birth_year, web_returns.billing_customer.email_address, web_returns.billing_customer.first_name, web_returns.billing_customer.id, web_returns.billing_customer.last_name, web_returns.billing_customer.last_review_date, web_returns.billing_customer.login, web_returns.billing_customer.preferred_cust_flag, web_returns.billing_customer.salutation, web_returns.billing_customer.text_id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:lineage, grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec:lineage`
- conditions: `web_returns.billing_customer.address.state = GA; web_returns.return_address.state is not MagicConstants.NULL`

## grp:root:root_d1:∅

- derivation: `root`
- depth: `root_d1`
- grain: `-`
- aggregate input grain: `-`
- primary members: `web_returns.billing_customer.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- secondary members: `-`
- outputs: `web_returns.billing_customer.id, web_returns.return_address.state, web_returns.return_amount, web_returns.return_date.year, web_returns.web_sales.item.id, web_returns.web_sales.order_number`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number:lineage, grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state:lineage, grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec:lineage`

# Edges

- `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` -> `grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state` kind=lineage phase=post_condition
- `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` -> `grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` kind=constraint phase=post_condition
- `grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state` -> `grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e` kind=lineage phase=post_condition
- `grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]basic:d1:web_returns.return_address.state:sig:f5912e` -> `grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` kind=constraint phase=post_condition
- `grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec` -> `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` kind=lineage phase=post_condition
- `grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` -> `__final__` kind=merge phase=post_condition
- `grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec` -> `__final__` kind=merge phase=post_condition
- `grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec` -> `grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `__final__` kind=merge phase=post_condition
- `grp:root:root:∅` -> `grp:aggregate:d0:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `grp:filter:d*:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec` kind=lineage phase=post_condition
- `grp:root:root_d1:∅` -> `__final__` kind=merge phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]aggregate:d1:web_returns.billing_customer.id|web_returns.return_address.state:input:web_returns.web_sales.item.id|web_returns.web_sales.order_number` kind=lineage phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]aggregate:d1:web_returns.return_address.state:input:web_returns.billing_customer.id|web_returns.return_address.state` kind=lineage phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]filter:d1:web_returns.web_sales.item.id|web_returns.web_sales.order_number:sig:8f68ec` kind=lineage phase=pre_condition
