# V4 Group Diagnostics

- groups: 3
- edges: 3

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
- predecessors: `grp:root:root:∅:merge, grp:rowset:d1:∅:rowset:_subquery_12_23:merge`
- successors: `-`
- conditions: `local.val > _subquery_12_23.half`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "local.id"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:rowset:d1:\u2205:rowset:_subquery_12_23",
      "output_addresses": [
        "_subquery_12_23.half"
      ],
      "preserve_keys": [],
      "projection_grain": []
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [],
  "output_addresses": [
    "local.id"
  ],
  "required_grain": [
    "local.id"
  ]
}
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `local.id, local.val`
- secondary members: `-`
- outputs: `local.id`
- inputs: `_subquery_12_23.half`
- hidden: `-`
- predecessors: `grp:rowset:d1:∅:rowset:_subquery_12_23:constraint`
- successors: `__final__:merge`
- conditions: `local.val > _subquery_12_23.half`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:root:root:\u2205",
    "may_project_dimension": false,
    "parent_group_id": "grp:rowset:d1:\u2205:rowset:_subquery_12_23",
    "preserve_keys": [],
    "required_grain": [],
    "required_outputs": [
      "_subquery_12_23.half"
    ]
  }
]
```

## grp:rowset:d1:∅:rowset:_subquery_12_23

- derivation: `rowset`
- depth: `d1`
- grain: `-`
- aggregate input grain: `-`
- primary members: `_subquery_12_23.half`
- secondary members: `-`
- outputs: `_subquery_12_23.half`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:root:root:∅:constraint`

# Edges

- `grp:root:root:∅` -> `__final__` kind=merge phase=post_condition
- `grp:rowset:d1:∅:rowset:_subquery_12_23` -> `__final__` kind=merge phase=pre_condition
- `grp:rowset:d1:∅:rowset:_subquery_12_23` -> `grp:root:root:∅` kind=constraint phase=pre_condition
