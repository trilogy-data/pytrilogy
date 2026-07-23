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
- predecessors: `grp:basic:d*:∅:sig:11c1f0:merge, grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs:merge`
- successors: `-`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:basic:d*:\u2205:sig:11c1f0",
      "output_addresses": [
        "local.present",
        "local.cross_pair_absent"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs",
      "output_addresses": [
        "pairs.val",
        "pairs.cat"
      ],
      "preserve_keys": [],
      "projection_grain": []
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [],
  "output_addresses": [
    "local.present",
    "local.cross_pair_absent"
  ],
  "required_grain": [
    "local.present",
    "local.cross_pair_absent"
  ]
}
```

## grp:basic:d*:∅:sig:11c1f0

- derivation: `basic`
- depth: `d*`
- grain: `-`
- aggregate input grain: `-`
- primary members: `local.cross_pair_absent, local.present`
- secondary members: `-`
- outputs: `local.cross_pair_absent, local.present`
- inputs: `pairs.cat, pairs.val`
- hidden: `-`
- predecessors: `grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs:lineage`
- successors: `__final__:merge`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:\u2205:sig:11c1f0",
    "may_project_dimension": true,
    "parent_group_id": "grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs",
    "preserve_keys": [
      "pairs.val",
      "pairs.cat"
    ],
    "required_grain": [
      "pairs.val",
      "pairs.cat"
    ],
    "required_outputs": [
      "pairs.val",
      "pairs.cat"
    ]
  }
]
```

## grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs

- derivation: `rowset`
- depth: `d0`
- grain: `pairs.cat, pairs.val`
- aggregate input grain: `-`
- primary members: `pairs.cat, pairs.val`
- secondary members: `-`
- outputs: `pairs.cat, pairs.val`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:basic:d*:∅:sig:11c1f0:lineage`

# Edges

- `grp:basic:d*:∅:sig:11c1f0` -> `__final__` kind=merge
- `grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs` -> `__final__` kind=merge
- `grp:rowset:d0:pairs.cat|pairs.val:rowset:pairs` -> `grp:basic:d*:∅:sig:11c1f0` kind=lineage
