# V4 Group Diagnostics

- groups: 5
- edges: 7

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
- predecessors: `grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id:merge, grp:[@condition]basic:d1:∅:sig:1fc31a:merge, grp:root:root:∅:merge, grp:root:root_d1:∅:merge`
- successors: `-`
- conditions: `local.val > local.half`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id",
      "output_addresses": [
        "local.mx"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "__preql_internal.all_rows"
      ]
    },
    {
      "group_id": "grp:[@condition]basic:d1:\u2205:sig:1fc31a",
      "output_addresses": [
        "local.half"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "local.id"
      ],
      "preserve_keys": [
        "__preql_internal.all_rows"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root_d1:\u2205",
      "output_addresses": [
        "local.id",
        "local.val"
      ],
      "preserve_keys": [
        "__preql_internal.all_rows"
      ],
      "projection_grain": []
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [
    "__preql_internal.all_rows"
  ],
  "output_addresses": [
    "local.id"
  ],
  "required_grain": [
    "local.id"
  ]
}
```

## grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id

- derivation: `aggregate`
- depth: `d1`
- grain: `__preql_internal.all_rows`
- aggregate input grain: `__preql_internal.all_rows, local.id`
- primary members: `local.mx`
- secondary members: `-`
- outputs: `local.mx`
- inputs: `local.id, local.val`
- hidden: `-`
- predecessors: `grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:[@condition]basic:d1:∅:sig:1fc31a:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "__preql_internal.all_rows"
    ],
    "required_grain": [
      "__preql_internal.all_rows"
    ],
    "required_outputs": [
      "local.id",
      "local.val"
    ]
  }
]
```

## grp:[@condition]basic:d1:∅:sig:1fc31a

- derivation: `basic`
- depth: `d1`
- grain: `-`
- aggregate input grain: `-`
- primary members: `local.half`
- secondary members: `-`
- outputs: `local.half`
- inputs: `local.mx`
- hidden: `-`
- predecessors: `grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id:lineage`
- successors: `__final__:merge, grp:root:root:∅:constraint`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]basic:d1:\u2205:sig:1fc31a",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id",
    "preserve_keys": [
      "__preql_internal.all_rows"
    ],
    "required_grain": [
      "__preql_internal.all_rows"
    ],
    "required_outputs": [
      "local.mx"
    ]
  }
]
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `local.id, local.val`
- secondary members: `-`
- outputs: `local.id`
- inputs: `local.half`
- hidden: `-`
- predecessors: `grp:[@condition]basic:d1:∅:sig:1fc31a:constraint`
- successors: `__final__:merge`
- conditions: `local.val > local.half`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:root:root:\u2205",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]basic:d1:\u2205:sig:1fc31a",
    "preserve_keys": [],
    "required_grain": [],
    "required_outputs": [
      "local.half"
    ]
  }
]
```

## grp:root:root_d1:∅

- derivation: `root`
- depth: `root_d1`
- grain: `-`
- aggregate input grain: `-`
- primary members: `local.id, local.val`
- secondary members: `-`
- outputs: `local.id, local.val`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id:lineage`

# Edges

- `grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id` -> `__final__` kind=merge phase=pre_condition
- `grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id` -> `grp:[@condition]basic:d1:∅:sig:1fc31a` kind=lineage phase=pre_condition
- `grp:[@condition]basic:d1:∅:sig:1fc31a` -> `__final__` kind=merge phase=pre_condition
- `grp:[@condition]basic:d1:∅:sig:1fc31a` -> `grp:root:root:∅` kind=constraint phase=pre_condition
- `grp:root:root:∅` -> `__final__` kind=merge phase=post_condition
- `grp:root:root_d1:∅` -> `__final__` kind=merge phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]aggregate:d1:__preql_internal.all_rows:input:__preql_internal.all_rows|local.id` kind=lineage phase=pre_condition
