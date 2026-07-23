# V4 Strategy Diagnostics

- strategy nodes: 8

## Tree

- MergeNode source=merge
  outputs: local.id
  inputs: _subquery_12_23.half, local.id, local.val
  conditions: local.val > _subquery_12_23.half
  - SelectNode source=select datasource=t
    outputs: local.id
    inputs: local.cat, local.id, local.val
  - MergeNode source=merge
    outputs: _subquery_12_23.half, local.id, local.val
    inputs: _subquery_12_23.half, local.id, local.val
    - SelectNode source=select
      outputs: _subquery_12_23.half, local.__subquery_12_23_half
      inputs: local.__subquery_12_23_half
      - SelectNode source=select
        outputs: local.__subquery_12_23_half
        inputs: local._virt_agg_max_4462979558645779
        - GroupNode source=group
          outputs: local._virt_agg_max_4462979558645779
          inputs: local.val
          - SelectNode source=select datasource=t
            outputs: local.id, local.val
            inputs: local.cat, local.id, local.val
    - SelectNode source=select datasource=t
      outputs: local.__subquery_12_23_half, local.id, local.val
      inputs: local.cat, local.id, local.val

## Records

```json
[
  {
    "conditions": null,
    "datasource": "t",
    "existence": [],
    "force_group": false,
    "grain": "Grain<local.id>",
    "hidden": [],
    "id": "n0",
    "inputs": [
      "local.id",
      "local.val",
      "local.cat"
    ],
    "nullable": [],
    "outputs": [
      "local.id"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "local.id"
    ]
  },
  {
    "conditions": null,
    "datasource": "t",
    "existence": [],
    "force_group": false,
    "grain": "Grain<local.id>",
    "hidden": [],
    "id": "n1",
    "inputs": [
      "local.id",
      "local.val",
      "local.cat"
    ],
    "nullable": [],
    "outputs": [
      "local.id",
      "local.val"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "local.id",
      "local.val"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n2",
    "inputs": [
      "local.val"
    ],
    "nullable": [],
    "outputs": [
      "local._virt_agg_max_4462979558645779"
    ],
    "parents": [
      "n1"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local._virt_agg_max_4462979558645779"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n3",
    "inputs": [
      "local._virt_agg_max_4462979558645779"
    ],
    "nullable": [],
    "outputs": [
      "local.__subquery_12_23_half"
    ],
    "parents": [
      "n2"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "local.__subquery_12_23_half"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": "Grain<Abstract>",
    "hidden": [],
    "id": "n4",
    "inputs": [
      "local.__subquery_12_23_half"
    ],
    "nullable": [],
    "outputs": [
      "_subquery_12_23.half",
      "local.__subquery_12_23_half"
    ],
    "parents": [
      "n3"
    ],
    "partials": [
      "local.__subquery_12_23_half"
    ],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "_subquery_12_23.half",
      "local.__subquery_12_23_half"
    ]
  },
  {
    "conditions": null,
    "datasource": "t",
    "existence": [],
    "force_group": false,
    "grain": "Grain<local.id>",
    "hidden": [],
    "id": "n5",
    "inputs": [
      "local.id",
      "local.val",
      "local.cat"
    ],
    "nullable": [],
    "outputs": [
      "local.id",
      "local.val",
      "local.__subquery_12_23_half"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "local.id",
      "local.val",
      "local.__subquery_12_23_half"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": "Grain<Abstract>",
    "hidden": [],
    "id": "n6",
    "inputs": [
      "_subquery_12_23.half",
      "local.id",
      "local.val"
    ],
    "nullable": [
      "_subquery_12_23.half",
      "local.id",
      "local.val"
    ],
    "outputs": [
      "_subquery_12_23.half",
      "local.id",
      "local.val"
    ],
    "parents": [
      "n4",
      "n5"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "_subquery_12_23.half",
      "local.id",
      "local.val"
    ]
  },
  {
    "conditions": "local.val > _subquery_12_23.half",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n7",
    "inputs": [
      "local.id",
      "local.val",
      "_subquery_12_23.half"
    ],
    "nullable": [
      "local.id"
    ],
    "outputs": [
      "local.id"
    ],
    "parents": [
      "n0",
      "n6"
    ],
    "partials": [],
    "preexisting_conditions": "local.val > _subquery_12_23.half",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "local.id"
    ]
  }
]
```
