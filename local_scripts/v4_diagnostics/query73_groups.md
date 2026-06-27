# V4 Group Diagnostics

- groups: 4
- edges: 6

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
- predecessors: `grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number:merge, grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095:merge, grp:root:root:∅:merge`
- successors: `-`
- conditions: `store_sales.customer.id is not MagicConstants.NULL`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number",
      "output_addresses": [
        "store_sales.ticket_number",
        "store_sales.customer.id",
        "local.ticket_cnt"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "store_sales.ticket_number",
        "store_sales.customer.id"
      ]
    },
    {
      "group_id": "grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095",
      "output_addresses": [
        "store_sales.item.id",
        "local._virt_filter_id_4484877027926973"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "store_sales.item.id",
        "store_sales.customer.last_name",
        "store_sales.date.year",
        "store_sales.customer.first_name",
        "store_sales.household_demographic.vehicle_count",
        "store_sales.store.county",
        "store_sales.ticket_number",
        "store_sales.household_demographic.buy_potential",
        "store_sales.household_demographic.dependent_count",
        "store_sales.customer.id",
        "store_sales.date.day_of_month",
        "store_sales.customer.preferred_cust_flag",
        "store_sales.customer.salutation"
      ],
      "preserve_keys": [
        "store_sales.ticket_number",
        "store_sales.customer.id"
      ],
      "projection_grain": []
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [
    "store_sales.ticket_number",
    "store_sales.customer.id"
  ],
  "output_addresses": [
    "store_sales.customer.last_name",
    "store_sales.customer.first_name",
    "store_sales.ticket_number",
    "store_sales.customer.id",
    "local.ticket_cnt",
    "store_sales.customer.preferred_cust_flag",
    "store_sales.customer.salutation"
  ],
  "required_grain": [
    "store_sales.ticket_number",
    "store_sales.customer.id"
  ]
}
```

## grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number

- derivation: `aggregate`
- depth: `d0`
- grain: `store_sales.customer.id, store_sales.ticket_number`
- aggregate input grain: `store_sales.item.id, store_sales.ticket_number`
- primary members: `local.ticket_cnt`
- secondary members: `store_sales.customer.id, store_sales.ticket_number`
- outputs: `local.ticket_cnt, store_sales.customer.id, store_sales.ticket_number`
- inputs: `local._virt_filter_id_4484877027926973, store_sales.customer.id, store_sales.item.id, store_sales.ticket_number`
- hidden: `-`
- predecessors: `grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095:lineage, grp:root:root:∅:lineage`
- successors: `__final__:merge`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095",
    "preserve_keys": [
      "store_sales.ticket_number",
      "store_sales.customer.id"
    ],
    "required_grain": [
      "store_sales.ticket_number",
      "store_sales.customer.id"
    ],
    "required_outputs": [
      "store_sales.item.id",
      "local._virt_filter_id_4484877027926973"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "store_sales.ticket_number",
      "store_sales.customer.id"
    ],
    "required_grain": [
      "store_sales.ticket_number",
      "store_sales.customer.id"
    ],
    "required_outputs": [
      "store_sales.ticket_number",
      "store_sales.item.id",
      "store_sales.customer.id"
    ]
  }
]
```

## grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095

- derivation: `filter`
- depth: `d*`
- grain: `store_sales.customer.id, store_sales.ticket_number`
- aggregate input grain: `-`
- primary members: `local._virt_filter_id_4484877027926973`
- secondary members: `-`
- outputs: `local._virt_filter_id_4484877027926973, store_sales.item.id`
- inputs: `store_sales.date.day_of_month, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county`
- hidden: `-`
- predecessors: `grp:root:root:∅:lineage`
- successors: `__final__:merge, grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "store_sales.ticket_number",
      "store_sales.customer.id"
    ],
    "required_grain": [
      "store_sales.ticket_number",
      "store_sales.customer.id"
    ],
    "required_outputs": [
      "store_sales.item.id",
      "store_sales.date.year",
      "store_sales.household_demographic.vehicle_count",
      "store_sales.store.county",
      "store_sales.household_demographic.buy_potential",
      "store_sales.household_demographic.dependent_count",
      "store_sales.date.day_of_month"
    ]
  }
]
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.ticket_number`
- secondary members: `-`
- outputs: `store_sales.customer.first_name, store_sales.customer.id, store_sales.customer.last_name, store_sales.customer.preferred_cust_flag, store_sales.customer.salutation, store_sales.date.day_of_month, store_sales.date.year, store_sales.household_demographic.buy_potential, store_sales.household_demographic.dependent_count, store_sales.household_demographic.vehicle_count, store_sales.item.id, store_sales.store.county, store_sales.ticket_number`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number:lineage, grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095:lineage`
- conditions: `store_sales.customer.id is not MagicConstants.NULL`

# Edges

- `grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number` -> `__final__` kind=merge phase=post_condition
- `grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095` -> `__final__` kind=merge phase=post_condition
- `grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095` -> `grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `__final__` kind=merge phase=post_condition
- `grp:root:root:∅` -> `grp:aggregate:d0:store_sales.customer.id|store_sales.ticket_number:input:store_sales.item.id|store_sales.ticket_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `grp:filter:d*:store_sales.customer.id|store_sales.ticket_number:sig:650095` kind=lineage phase=post_condition
