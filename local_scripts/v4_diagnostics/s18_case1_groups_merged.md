# V4 Group Diagnostics

- groups: 7
- edges: 13

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
- predecessors: `grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period:merge, grp:aggregate:d0:agg.s.period:merge, grp:basic:d*:agg.s.period|fut.s.period:sig:50d021:merge, grp:basic:d*:∅:sig:fddbe9:merge, grp:rowset:d0:agg.s.period:rowset:agg:merge, grp:rowset:d0:fut.s.period:rowset:fut:merge`
- successors: `-`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:aggregate:d0:agg.s.period",
      "output_addresses": [
        "agg.s.period",
        "local._virt_agg_sum_4149934679595063"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "agg.s.period"
      ]
    },
    {
      "group_id": "grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period",
      "output_addresses": [
        "local._virt_agg_sum_9228488784072447"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "agg.s.period"
      ]
    },
    {
      "group_id": "grp:basic:d*:agg.s.period|fut.s.period:sig:50d021",
      "output_addresses": [
        "agg.s.period",
        "local.r"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "agg.s.period",
        "s.period"
      ]
    },
    {
      "group_id": "grp:basic:d*:\u2205:sig:fddbe9",
      "output_addresses": [
        "local._virt_func_add_8165249766077962",
        "fut.s.period",
        "fut.tot"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "local._virt_func_add_8165249766077962"
      ]
    },
    {
      "group_id": "grp:rowset:d0:agg.s.period:rowset:agg",
      "output_addresses": [
        "agg.s.period",
        "agg.tot"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "local._virt_func_add_8165249766077962",
        "agg.s.period",
        "s.period"
      ]
    },
    {
      "group_id": "grp:rowset:d0:fut.s.period:rowset:fut",
      "output_addresses": [
        "fut.s.period",
        "fut.tot"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "local._virt_func_add_8165249766077962",
        "agg.s.period",
        "s.period"
      ]
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [
    "local._virt_func_add_8165249766077962",
    "agg.s.period",
    "s.period"
  ],
  "output_addresses": [
    "agg.s.period",
    "local.r"
  ],
  "required_grain": [
    "agg.s.period"
  ]
}
```

## grp:aggregate:d0:agg.s.period

- derivation: `aggregate`
- depth: `d0`
- grain: `agg.s.period`
- aggregate input grain: `agg.s.period`
- primary members: `local._virt_agg_sum_4149934679595063`
- secondary members: `agg.s.period`
- outputs: `agg.s.period, local._virt_agg_sum_4149934679595063`
- inputs: `agg.s.period, agg.tot, local._virt_func_add_8165249766077962`
- hidden: `-`
- predecessors: `grp:basic:d*:∅:sig:fddbe9:lineage, grp:rowset:d0:agg.s.period:rowset:agg:lineage`
- successors: `__final__:merge, grp:basic:d*:agg.s.period|fut.s.period:sig:50d021:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:agg.s.period",
    "may_project_dimension": true,
    "parent_group_id": "grp:basic:d*:\u2205:sig:fddbe9",
    "preserve_keys": [
      "agg.s.period"
    ],
    "required_grain": [
      "agg.s.period"
    ],
    "required_outputs": [
      "local._virt_func_add_8165249766077962"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:agg.s.period",
    "may_project_dimension": true,
    "parent_group_id": "grp:rowset:d0:agg.s.period:rowset:agg",
    "preserve_keys": [
      "agg.s.period"
    ],
    "required_grain": [
      "agg.s.period"
    ],
    "required_outputs": [
      "agg.s.period",
      "agg.tot"
    ]
  }
]
```

## grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period

- derivation: `aggregate`
- depth: `d0`
- grain: `agg.s.period`
- aggregate input grain: `agg.s.period, fut.s.period`
- primary members: `local._virt_agg_sum_9228488784072447`
- secondary members: `agg.s.period`
- outputs: `local._virt_agg_sum_9228488784072447`
- inputs: `fut.s.period, fut.tot, local._virt_func_add_8165249766077962`
- hidden: `-`
- predecessors: `grp:basic:d*:∅:sig:fddbe9:lineage, grp:rowset:d0:fut.s.period:rowset:fut:lineage`
- successors: `__final__:merge, grp:basic:d*:agg.s.period|fut.s.period:sig:50d021:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period",
    "may_project_dimension": true,
    "parent_group_id": "grp:basic:d*:\u2205:sig:fddbe9",
    "preserve_keys": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_grain": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_outputs": [
      "fut.tot",
      "fut.s.period",
      "local._virt_func_add_8165249766077962"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period",
    "may_project_dimension": true,
    "parent_group_id": "grp:rowset:d0:fut.s.period:rowset:fut",
    "preserve_keys": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_grain": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_outputs": [
      "fut.s.period",
      "fut.tot"
    ]
  }
]
```

## grp:basic:d*:agg.s.period|fut.s.period:sig:50d021

- derivation: `basic`
- depth: `d*`
- grain: `agg.s.period, fut.s.period`
- aggregate input grain: `-`
- primary members: `local.r`
- secondary members: `-`
- outputs: `agg.s.period, local.r`
- inputs: `agg.s.period, local._virt_agg_sum_4149934679595063, local._virt_agg_sum_9228488784072447`
- hidden: `-`
- predecessors: `grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period:lineage, grp:aggregate:d0:agg.s.period:lineage`
- successors: `__final__:merge`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:agg.s.period|fut.s.period:sig:50d021",
    "may_project_dimension": false,
    "parent_group_id": "grp:aggregate:d0:agg.s.period",
    "preserve_keys": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_grain": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:agg.s.period|fut.s.period:sig:50d021",
    "may_project_dimension": false,
    "parent_group_id": "grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period",
    "preserve_keys": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_grain": [
      "agg.s.period",
      "fut.s.period"
    ],
    "required_outputs": [
      "local._virt_agg_sum_9228488784072447"
    ]
  }
]
```

## grp:basic:d*:∅:sig:fddbe9

- derivation: `basic`
- depth: `d*`
- grain: `-`
- aggregate input grain: `-`
- primary members: `local._virt_func_add_8165249766077962`
- secondary members: `-`
- outputs: `fut.s.period, fut.tot, local._virt_func_add_8165249766077962`
- inputs: `fut.s.period, fut.tot`
- hidden: `-`
- predecessors: `grp:rowset:d0:fut.s.period:rowset:fut:lineage`
- successors: `__final__:merge, grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period:lineage, grp:aggregate:d0:agg.s.period:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:\u2205:sig:fddbe9",
    "may_project_dimension": true,
    "parent_group_id": "grp:rowset:d0:fut.s.period:rowset:fut",
    "preserve_keys": [
      "fut.s.period"
    ],
    "required_grain": [
      "fut.s.period"
    ],
    "required_outputs": [
      "fut.s.period",
      "fut.tot"
    ]
  }
]
```

## grp:rowset:d0:agg.s.period:rowset:agg

- derivation: `rowset`
- depth: `d0`
- grain: `agg.s.period`
- aggregate input grain: `-`
- primary members: `agg.s.period, agg.tot`
- secondary members: `-`
- outputs: `agg.s.period, agg.tot`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:aggregate:d0:agg.s.period:lineage`

## grp:rowset:d0:fut.s.period:rowset:fut

- derivation: `rowset`
- depth: `d0`
- grain: `fut.s.period`
- aggregate input grain: `-`
- primary members: `fut.s.period, fut.tot`
- secondary members: `-`
- outputs: `fut.s.period, fut.tot`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period:lineage, grp:basic:d*:∅:sig:fddbe9:lineage`

# Edges

- `grp:aggregate:d0:agg.s.period` -> `__final__` kind=merge
- `grp:aggregate:d0:agg.s.period` -> `grp:basic:d*:agg.s.period|fut.s.period:sig:50d021` kind=lineage
- `grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period` -> `__final__` kind=merge
- `grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period` -> `grp:basic:d*:agg.s.period|fut.s.period:sig:50d021` kind=lineage
- `grp:basic:d*:agg.s.period|fut.s.period:sig:50d021` -> `__final__` kind=merge
- `grp:basic:d*:∅:sig:fddbe9` -> `__final__` kind=merge
- `grp:basic:d*:∅:sig:fddbe9` -> `grp:aggregate:d0:agg.s.period` kind=lineage
- `grp:basic:d*:∅:sig:fddbe9` -> `grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period` kind=lineage
- `grp:rowset:d0:agg.s.period:rowset:agg` -> `__final__` kind=merge
- `grp:rowset:d0:agg.s.period:rowset:agg` -> `grp:aggregate:d0:agg.s.period` kind=lineage
- `grp:rowset:d0:fut.s.period:rowset:fut` -> `__final__` kind=merge
- `grp:rowset:d0:fut.s.period:rowset:fut` -> `grp:aggregate:d0:agg.s.period:input:agg.s.period|fut.s.period` kind=lineage
- `grp:rowset:d0:fut.s.period:rowset:fut` -> `grp:basic:d*:∅:sig:fddbe9` kind=lineage
