# V4 Group Diagnostics

- groups: 10
- edges: 20

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
- predecessors: `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:merge, grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state:merge, grp:[@condition]basic:d1:cr.return_address.state:sig:b60650:merge, grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f:merge, grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:merge, grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f:merge, grp:root:root:∅:dim:cr.billing_customer.id:merge, grp:root:root:∅:merge, grp:root:root_d1:∅:merge`
- successors: `-`
- conditions: `local.customer_state > local.scaled_state and cr.billing_customer.address.state = GA and cr.return_address.state is not MagicConstants.NULL`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
      "output_addresses": [
        "local.customer_state",
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "cr.return_address.state",
        "cr.billing_customer.id"
      ]
    },
    {
      "group_id": "grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state",
      "output_addresses": [
        "local._virt_agg_avg_7052944147524274",
        "cr.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "cr.return_address.state"
      ]
    },
    {
      "group_id": "grp:[@condition]basic:d1:cr.return_address.state:sig:b60650",
      "output_addresses": [
        "local.scaled_state",
        "cr.return_address.state"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f",
      "output_addresses": [
        "local._virt_filter_return_amt_inc_tax_2184255153361204",
        "cr.item.id",
        "cr.order_number",
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
      "output_addresses": [
        "local.customer_state",
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "cr.return_address.state",
        "cr.billing_customer.id"
      ]
    },
    {
      "group_id": "grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f",
      "output_addresses": [
        "local._virt_filter_return_amt_inc_tax_2184255153361204",
        "cr.item.id",
        "cr.order_number",
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "cr.return_amt_inc_tax",
        "cr.item.id",
        "cr.order_number",
        "cr.return_address.state",
        "cr.billing_customer.id",
        "cr.date.year"
      ],
      "preserve_keys": [
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root:\u2205:dim:cr.billing_customer.id",
      "output_addresses": [
        "cr.billing_customer.address.location_type",
        "cr.billing_customer.salutation",
        "cr.billing_customer.address.zip",
        "cr.billing_customer.first_name",
        "cr.billing_customer.address.gmt_offset",
        "cr.billing_customer.last_name",
        "cr.billing_customer.text_id",
        "cr.billing_customer.address.city",
        "cr.billing_customer.address.suite_number",
        "cr.billing_customer.address.street_name",
        "cr.billing_customer.id",
        "cr.billing_customer.address.street_number",
        "cr.billing_customer.address.street_type",
        "cr.billing_customer.address.state",
        "cr.billing_customer.address.country",
        "cr.billing_customer.address.county"
      ],
      "preserve_keys": [
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root_d1:\u2205",
      "output_addresses": [
        "cr.return_amt_inc_tax",
        "cr.item.id",
        "cr.order_number",
        "cr.return_address.state",
        "cr.billing_customer.id",
        "cr.date.year"
      ],
      "preserve_keys": [
        "cr.return_address.state",
        "cr.billing_customer.id"
      ],
      "projection_grain": []
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [
    "cr.return_address.state",
    "cr.billing_customer.id"
  ],
  "output_addresses": [
    "cr.billing_customer.address.location_type",
    "cr.billing_customer.salutation",
    "cr.billing_customer.last_name",
    "cr.billing_customer.address.zip",
    "cr.billing_customer.address.country",
    "cr.billing_customer.address.gmt_offset",
    "cr.billing_customer.text_id",
    "local.customer_state",
    "cr.billing_customer.address.city",
    "cr.billing_customer.address.suite_number",
    "cr.billing_customer.address.street_name",
    "cr.billing_customer.id",
    "cr.billing_customer.address.street_number",
    "cr.billing_customer.address.street_type",
    "cr.billing_customer.address.state",
    "cr.billing_customer.first_name",
    "cr.billing_customer.address.county"
  ],
  "required_grain": [
    "cr.billing_customer.address.location_type",
    "cr.billing_customer.address.zip",
    "cr.billing_customer.address.gmt_offset",
    "cr.billing_customer.text_id",
    "local.customer_state",
    "cr.billing_customer.address.city",
    "cr.billing_customer.address.suite_number",
    "cr.billing_customer.address.street_name",
    "cr.billing_customer.id",
    "cr.billing_customer.address.street_number",
    "cr.billing_customer.address.street_type",
    "cr.billing_customer.address.state",
    "cr.billing_customer.address.country",
    "cr.billing_customer.address.county"
  ]
}
```

## grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number

- derivation: `aggregate`
- depth: `d1`
- grain: `cr.billing_customer.id, cr.return_address.state`
- aggregate input grain: `cr.item.id, cr.order_number`
- primary members: `local.customer_state`
- secondary members: `cr.billing_customer.id, cr.return_address.state`
- outputs: `cr.billing_customer.id, cr.return_address.state, local.customer_state`
- inputs: `cr.billing_customer.id, cr.item.id, cr.order_number, cr.return_address.state, local._virt_filter_return_amt_inc_tax_2184255153361204`
- hidden: `-`
- predecessors: `grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f:lineage, grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state:lineage, grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:constraint`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "cr.order_number",
      "cr.item.id",
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  }
]
```

## grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state

- derivation: `aggregate`
- depth: `d1`
- grain: `cr.return_address.state`
- aggregate input grain: `cr.billing_customer.id, cr.return_address.state`
- primary members: `local._virt_agg_avg_7052944147524274`
- secondary members: `cr.return_address.state`
- outputs: `cr.return_address.state, local._virt_agg_avg_7052944147524274`
- inputs: `cr.billing_customer.id, cr.return_address.state, local.customer_state`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:lineage, grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]basic:d1:cr.return_address.state:sig:b60650:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "local.customer_state",
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  }
]
```

## grp:[@condition]basic:d1:cr.return_address.state:sig:b60650

- derivation: `basic`
- depth: `d1`
- grain: `cr.return_address.state`
- aggregate input grain: `-`
- primary members: `local.scaled_state`
- secondary members: `-`
- outputs: `cr.return_address.state, local.scaled_state`
- inputs: `cr.return_address.state, local._virt_agg_avg_7052944147524274`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state:lineage`
- successors: `__final__:merge, grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:constraint`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]basic:d1:cr.return_address.state:sig:b60650",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state",
    "preserve_keys": [
      "cr.return_address.state"
    ],
    "required_grain": [
      "cr.return_address.state"
    ],
    "required_outputs": [
      "local._virt_agg_avg_7052944147524274",
      "cr.return_address.state"
    ]
  }
]
```

## grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f

- derivation: `filter`
- depth: `d1`
- grain: `cr.item.id, cr.order_number`
- aggregate input grain: `-`
- primary members: `local._virt_filter_return_amt_inc_tax_2184255153361204`
- secondary members: `-`
- outputs: `cr.billing_customer.id, cr.item.id, cr.order_number, cr.return_address.state, local._virt_filter_return_amt_inc_tax_2184255153361204`
- inputs: `cr.billing_customer.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.state, cr.return_amt_inc_tax`
- hidden: `-`
- predecessors: `grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:lineage`
- conditions: `cr.return_address.state is not MagicConstants.NULL`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "cr.order_number",
      "cr.item.id"
    ],
    "required_grain": [
      "cr.order_number",
      "cr.item.id"
    ],
    "required_outputs": [
      "cr.return_amt_inc_tax",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "cr.billing_customer.id",
      "cr.date.year"
    ]
  }
]
```

## grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number

- derivation: `aggregate`
- depth: `d0`
- grain: `cr.billing_customer.id, cr.return_address.state`
- aggregate input grain: `cr.item.id, cr.order_number`
- primary members: `local.customer_state`
- secondary members: `cr.billing_customer.id, cr.return_address.state`
- outputs: `cr.billing_customer.id, cr.return_address.state, local.customer_state`
- inputs: `cr.billing_customer.id, cr.item.id, cr.order_number, cr.return_address.state, local._virt_filter_return_amt_inc_tax_2184255153361204, local.scaled_state`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:constraint, grp:[@condition]basic:d1:cr.return_address.state:sig:b60650:constraint, grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f:lineage, grp:root:root:∅:lineage`
- successors: `__final__:merge`
- conditions: `local.customer_state > local.scaled_state`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:[@condition]basic:d1:cr.return_address.state:sig:b60650",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "local.scaled_state",
      "cr.return_address.state"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "local._virt_filter_return_amt_inc_tax_2184255153361204",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_grain": [
      "cr.return_address.state",
      "cr.billing_customer.id"
    ],
    "required_outputs": [
      "cr.order_number",
      "cr.item.id",
      "cr.return_address.state",
      "cr.billing_customer.id"
    ]
  }
]
```

## grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f

- derivation: `filter`
- depth: `d*`
- grain: `cr.item.id, cr.order_number`
- aggregate input grain: `-`
- primary members: `local._virt_filter_return_amt_inc_tax_2184255153361204`
- secondary members: `-`
- outputs: `cr.billing_customer.id, cr.item.id, cr.order_number, cr.return_address.state, local._virt_filter_return_amt_inc_tax_2184255153361204`
- inputs: `cr.billing_customer.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.state, cr.return_amt_inc_tax`
- hidden: `-`
- predecessors: `grp:root:root:∅:lineage`
- successors: `__final__:merge, grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "cr.order_number",
      "cr.item.id"
    ],
    "required_grain": [
      "cr.order_number",
      "cr.item.id"
    ],
    "required_outputs": [
      "cr.return_amt_inc_tax",
      "cr.item.id",
      "cr.order_number",
      "cr.return_address.state",
      "cr.billing_customer.id",
      "cr.date.year"
    ]
  }
]
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `cr.billing_customer.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.state, cr.return_amt_inc_tax`
- secondary members: `-`
- outputs: `cr.billing_customer.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.state, cr.return_amt_inc_tax`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:lineage, grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f:lineage`
- conditions: `cr.return_address.state is not MagicConstants.NULL`

## grp:root:root:∅:dim:cr.billing_customer.id

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id`
- secondary members: `-`
- outputs: `cr.billing_customer.address.city, cr.billing_customer.address.country, cr.billing_customer.address.county, cr.billing_customer.address.gmt_offset, cr.billing_customer.address.location_type, cr.billing_customer.address.state, cr.billing_customer.address.street_name, cr.billing_customer.address.street_number, cr.billing_customer.address.street_type, cr.billing_customer.address.suite_number, cr.billing_customer.address.zip, cr.billing_customer.first_name, cr.billing_customer.id, cr.billing_customer.last_name, cr.billing_customer.salutation, cr.billing_customer.text_id`
- inputs: `cr.billing_customer.id`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge`
- conditions: `cr.billing_customer.address.state = GA`

## grp:root:root_d1:∅

- derivation: `root`
- depth: `root_d1`
- grain: `-`
- aggregate input grain: `-`
- primary members: `cr.billing_customer.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.state, cr.return_amt_inc_tax`
- secondary members: `-`
- outputs: `cr.billing_customer.id, cr.date.year, cr.item.id, cr.order_number, cr.return_address.state, cr.return_amt_inc_tax`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number:lineage, grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state:lineage, grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f:lineage`

# Edges

- `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` -> `grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state` kind=lineage phase=post_condition
- `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` -> `grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` kind=constraint phase=post_condition
- `grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state` -> `grp:[@condition]basic:d1:cr.return_address.state:sig:b60650` kind=lineage phase=post_condition
- `grp:[@condition]basic:d1:cr.return_address.state:sig:b60650` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]basic:d1:cr.return_address.state:sig:b60650` -> `grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` kind=constraint phase=post_condition
- `grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f` -> `__final__` kind=merge phase=post_condition
- `grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f` -> `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` kind=lineage phase=post_condition
- `grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` -> `__final__` kind=merge phase=post_condition
- `grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f` -> `__final__` kind=merge phase=post_condition
- `grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f` -> `grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `__final__` kind=merge phase=post_condition
- `grp:root:root:∅` -> `grp:aggregate:d0:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `grp:filter:d*:cr.item.id|cr.order_number:sig:87f58f` kind=lineage phase=post_condition
- `grp:root:root:∅:dim:cr.billing_customer.id` -> `__final__` kind=merge phase=post_condition
- `grp:root:root_d1:∅` -> `__final__` kind=merge phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]aggregate:d1:cr.billing_customer.id|cr.return_address.state:input:cr.item.id|cr.order_number` kind=lineage phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]aggregate:d1:cr.return_address.state:input:cr.billing_customer.id|cr.return_address.state` kind=lineage phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]filter:d1:cr.item.id|cr.order_number:sig:87f58f` kind=lineage phase=pre_condition
